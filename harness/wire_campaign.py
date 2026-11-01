"""End-to-end faults through MCP HTTP -> real paho -> a TLS MQTT test peer.

Unlike callback fakes, this catches reconnection/queue behavior in the pinned paho,
serialization over MCP, and process shutdown. Run only inside run.py's namespace.
Seeds are recorded and replayable. No production credentials or printer address.
"""
import asyncio
import json
import os
from pathlib import Path
import random
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time

if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

ROOT = Path(__file__).resolve().parents[1]

def packet(kind, data):
    n = len(data)
    length = bytearray()
    while True:
        b, n = n % 128, n // 128
        length.append(b | (128 if n else 0))
        if not n:
            break
    return bytes([kind]) + length + data

def exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n-len(data))
        if not chunk:
            raise EOFError()
        data += chunk
    return data

class Broker:
    def __init__(self, tmp):
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(tmp/"key.pem"), "-out", str(tmp/"cert.pem"),
                        "-days", "1", "-subj", "/CN=localhost"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.ctx.load_cert_chain(tmp/"cert.pem", tmp/"key.pem")
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(.2)
        self.lock = threading.Lock()
        self.peers = set()
        self.received = []
        self.connections = 0
        self.maximum = 0
        self.reports = True
        self.running = False
        self.block_reads = False
        self.stop = threading.Event()
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while not self.stop.is_set():
            try:
                raw, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.serve, args=(raw,), daemon=True).start()

    def send(self, peer, data):
        with self.lock:
            peer.sendall(data)

    def report(self, peer, data=None):
        if data is None:
            data = {"print": {"command": "push_status", "gcode_state": "RUNNING" if self.running else "IDLE",
                    "nozzle_temper": 240., "bed_temper": 75., "mc_percent": 3 if self.running else 0,
                    "gcode_file": "Metadata/plate_1.gcode", "subtask_name": "fixture-job",
                    "task_id": "77" if self.running else "0", "layer_num": 2, "total_layer_num": 115}}
        topic = b"device/01P00TESTONLY/report"
        body = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.send(peer, packet(0x30, len(topic).to_bytes(2, "big")+topic+body))

    def broadcast(self, data=None):
        for peer in list(self.peers):
            try:
                self.report(peer, data)
            except OSError:
                pass

    def serve(self, raw):
        peer = None
        try:
            raw.settimeout(3)
            peer = self.ctx.wrap_socket(raw, server_side=True)
            peer.settimeout(10)
            self.peers.add(peer)
            self.connections += 1
            self.maximum = max(self.maximum, len(self.peers))
            def read(n):
                data = b""
                while len(data)<n:
                    while self.block_reads and not self.stop.is_set():
                        time.sleep(.01)
                    chunk = peer.recv(min(16384,n-len(data)))
                    if not chunk:
                        raise EOFError()
                    data += chunk
                return data
            while not self.stop.is_set():
                header = read(1)[0]
                length, scale = 0, 1
                for _ in range(4):
                    b = read(1)[0]
                    length += (b & 127) * scale
                    if not b & 128:
                        break
                    scale *= 128
                body = read(length)
                typ = header >> 4
                if typ == 1:
                    self.send(peer, b"\x20\x02\x00\x00")
                elif typ == 8:
                    self.send(peer, packet(0x90, body[:2]+b"\x00"))
                    if self.reports:
                        self.report(peer)
                elif typ == 3:
                    size = int.from_bytes(body[:2], "big")
                    msg = json.loads(body[2+size:])
                    self.received.append(msg)
                    if self.reports and msg.get("pushing", {}).get("command") == "pushall":
                        self.report(peer)
                elif typ == 12:
                    self.send(peer, b"\xd0\x00")
                elif typ == 14:
                    break
        except (OSError, EOFError, ValueError):
            pass
        finally:
            if peer:
                self.peers.discard(peer)
                peer.close()
            else:
                raw.close()

    def mutations(self):
        return [m for m in self.received if not (
            m.get("pushing", {}).get("command") == "pushall" or
            m.get("info", {}).get("command") == "get_version")]

    def drop(self):
        for peer in list(self.peers):
            self.peers.discard(peer)
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            peer.close()

    def close(self):
        self.stop.set()
        self.drop()
        self.listener.close()

def payload(r):
    if r.structuredContent is not None:
        value = r.structuredContent
        if isinstance(value, dict) and set(value) == {"result"}:
            value = value["result"]
        if not isinstance(value,str):
            return value
        text = value
    else:
        text = "\n".join(c.text for c in r.content if c.type == "text")
    try:
        return json.loads(text)
    except ValueError:
        return text

async def campaign(seed, cycles):
    rng = random.Random(seed)
    events = []
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        broker = Broker(tmp)
        port = 25099  # private network namespace; production's registered port range
        env = os.environ.copy()
        env.update(HOME=str(tmp/"home"), BAMBU_MCP_PORT=str(port), BAMBU_MCP_DISABLE_REST="1",
                   HARNESS_MQTT_PORT=str(broker.port), BAMBU_MCP_ALLOW_CLIENT_CONFIG_WRITE="0")
        Path(env["HOME"]).mkdir()
        log = (tmp/"server.log").open("w+")
        proc = subprocess.Popen([sys.executable, str(ROOT/"harness/fixture_server.py")],
                                env=env, cwd=ROOT, stdout=log, stderr=log)
        try:
            for _ in range(100):
                if proc.poll() is not None:
                    raise AssertionError(f"fixture exited {proc.returncode}")
                try:
                    probe = socket.create_connection(("127.0.0.1", port), .1)
                    probe.close(); break
                except OSError:
                    await asyncio.sleep(.1)
            async with streamablehttp_client(f"http://127.0.0.1:{port}/mcp") as (rd, wr, _):
                async with ClientSession(rd, wr) as c:
                    await c.initialize()
                    names = {t.name for t in (await c.list_tools()).tools}
                    assert "get_printer_connection_status" in names
                    async def call(tool, **args):
                        return payload(await asyncio.wait_for(c.call_tool(tool, {"name":"fixture", **args}), 6))
                    async def until_live():
                        start = time.monotonic()
                        for _ in range(140):
                            h = await call("get_printer_connection_status")
                            if h.get("telemetry", {}).get("live"):
                                return time.monotonic()-start
                            await asyncio.sleep(.1)
                        raise AssertionError(f"not live: {h}")
                    await until_live()
                    from watchdog import probe as watchdog_probe
                    assert (await watchdog_probe(f"http://127.0.0.1:{port}/mcp", "fixture"))["live"]
                    # One authorized test mutation must arrive once over the real wire.
                    sent = await call("set_bed_temp", temp=0, user_permission=True)
                    assert "acceptance is not confirmed" in str(sent), sent
                    await asyncio.sleep(.15)
                    assert len(broker.mutations()) == 1, broker.mutations()
                    for i in range(cycles):
                        fault = ["drop", "silence", "malformed", "job-ftps-reset"][i % 4]
                        before = len(broker.mutations())
                        connections = broker.connections
                        start = time.monotonic()
                        if fault == "drop":
                            broker.reports = False
                            broker.drop()
                            await asyncio.sleep(.1+rng.random()*.1)
                        elif fault == "silence":
                            broker.reports = False
                            await asyncio.sleep(1.5+rng.random()*.1)
                        elif fault == "malformed":
                            broker.reports = False
                            broker.broadcast(b"{broken-json")
                            await asyncio.sleep(.15)
                        else:
                            broker.running = not broker.running
                            broker.broadcast()
                            await asyncio.sleep(.2)
                            progress = await call("get_print_progress")
                            assert progress.get("gcode_state") == ("RUNNING" if broker.running else "IDLE"), progress
                        if fault != "job-ftps-reset":
                            state = await call("get_print_progress")
                            assert state.get("error") == "telemetry_not_live", state
                            refusal = await call("set_chamber_light", on=False, user_permission=True)
                            # Existing string-returning tools keep their public output schema;
                            # refusal must carry its explicit cause, not a schema-validation error.
                            assert ((isinstance(refusal,dict) and refusal.get("error")=="telemetry_not_live") or
                                    (isinstance(refusal,str) and "Error (telemetry_not_live):" in refusal)), refusal
                        broker.reports = True
                        broker.broadcast()
                        await until_live()
                        await asyncio.sleep(.1)
                        assert len(broker.mutations()) == before, broker.mutations()
                        assert broker.maximum == 1, broker.maximum
                        # A drop needs a new connection; the others must be survived by the same
                        # paho loop (a parse error or FTPS reset that killed it would reconnect).
                        assert (broker.connections > connections if fault == "drop"
                                else broker.connections == connections), (fault, connections, broker.connections)
                        events.append({"fault":fault,"recovery_s":round(time.monotonic()-start,3),"replayed":0})
                        broker.broadcast()
                    # Read callers share one session and remain usable together.
                    result = await asyncio.gather(*(call("get_print_progress") for _ in range(8)))
                    assert all("error" not in x for x in result), result
                    assert broker.maximum == 1
                    # Fill the real TCP send buffer. The command cannot complete in the
                    # socket deadline; after disconnect it must not be replayed by paho.
                    broker.running = False
                    broker.broadcast()
                    await asyncio.sleep(.15)
                    await until_live()
                    before = len(broker.mutations())
                    broker.block_reads = True
                    unknown = await call("send_gcode", gcode=";HARNESS-ONLY-"+"x"*(8*1024*1024), user_permission=True)
                    # send_gcode catches the exception itself; the guard must still say unknown/not retried.
                    text = unknown if isinstance(unknown, str) else json.dumps(unknown)
                    assert ("publish_unconfirmed" in text and '"sent": "unknown"' in text
                            and '"retried": false' in text), text[:1000]
                    broker.drop()
                    broker.block_reads = False
                    await until_live()
                    await asyncio.sleep(.3)
                    assert len(broker.mutations()) == before, "unconfirmed packet was replayed"
                    events.append({"fault":"socket-backpressure-disconnect","replayed":0})
                    # Bytes without usable telemetry (malformed frames) must not hold the
                    # connection open: the supervisor drops it and real paho reconnects once.
                    before, connections = len(broker.mutations()), broker.connections
                    broker.reports = False
                    start = time.monotonic()
                    while broker.connections == connections and time.monotonic() - start < 10:
                        broker.broadcast(b"{flood")
                        await asyncio.sleep(.05)
                    assert broker.connections > connections, "flood of unusable frames held off reconnect"
                    broker.reports = True
                    broker.broadcast()
                    await until_live()
                    assert len(broker.mutations()) == before and broker.maximum == 1, broker.mutations()
                    events.append({"fault":"unusable-frame-flood","recovery_s":round(time.monotonic()-start,3),
                                   "replayed":0})
                    # A dead HTTP process must not leave the existing client holding an
                    # invalid server session ID. Keep this exact ClientSession open.
                    before = len(broker.mutations())
                    old_pid = proc.pid
                    start = time.monotonic()
                    proc.kill(); proc.wait(timeout=5)
                    for _ in range(100):
                        if not broker.peers:
                            break
                        await asyncio.sleep(.02)
                    assert not broker.peers, "dead server still owns MQTT socket"
                    proc = subprocess.Popen([sys.executable,str(ROOT/"harness/fixture_server.py")],
                                            env=env,cwd=ROOT,stdout=log,stderr=log)
                    for _ in range(100):
                        assert proc.poll() is None, "replacement fixture died"
                        try:
                            probe = socket.create_connection(("127.0.0.1",port),.1)
                            probe.close(); break
                        except OSError:
                            await asyncio.sleep(.1)
                    await until_live()  # no initialize, no replacement MCP client
                    recovered = await call("get_print_progress")
                    assert recovered.get("gcode_state")=="IDLE", recovered
                    assert len(broker.mutations())==before and broker.maximum==1
                    assert proc.pid != old_pid
                    events.append({"fault":"server-SIGKILL-same-MCP-client",
                                   "recovery_s":round(time.monotonic()-start,3),"replayed":0})
            proc.terminate()
            proc.wait(timeout=14)
            assert proc.returncode in (0,143,-15), proc.returncode
            return {"seed":seed,"cycles":cycles,"events":events,"max_mqtt_clients":broker.maximum,
                    "mqtt_connections":broker.connections,"physical_test_commands":len(broker.mutations()),
                    "transport":"MCP HTTP + paho TLS MQTT", "passed":True}
        except BaseException:
            log.flush(); log.seek(0)
            print(log.read()[-10000:], file=sys.stderr)
            raise
        finally:
            if proc.poll() is None:
                proc.kill(); proc.wait(timeout=5)
            broker.close()
            log.close()

if __name__ == "__main__":
    print(json.dumps(asyncio.run(campaign(int(sys.argv[1]), int(sys.argv[2])))))
