"""E2E workflows and faults through MCP HTTP and TLS MQTT/FTPS/camera peers.

Exercises the installed libraries, MCP serialization and process lifecycle.
Run only inside run.py's namespace.
Seeds are recorded and replayable. No production credentials or printer address.
"""
import asyncio
import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time

if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from printer_peers import FTPSPeer, CameraPeer

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

def held_mqtt_sockets():
    """Client-side TCP sockets to the MQTT port that are still open in this network namespace.

    ESTABLISHED, or CLOSE_WAIT: the peer closed but the client never closed its socket (the
    2026-11-01 leak). The broker's own ends (local port 8883) are not counted.
    """
    held = 0
    with open("/proc/net/tcp") as f:
        next(f)
        for line in f:
            _, local, remote, state = line.split()[:4]
            if int(remote.rsplit(":", 1)[1], 16) == 8883 and int(local.rsplit(":", 1)[1], 16) != 8883 \
                    and state in ("01", "08"):
                held += 1
    return held

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
        # No TLS 1.3 session tickets: bpm ends an FTPS upload with a bare TCP shutdown, and a
        # ticket the server is still writing then resets the data channel before it is read.
        self.ctx.num_tickets = 0
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 8883))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(.2)
        self.lock = threading.Lock()
        self.peers = set()
        self.received = []
        self.connections = 0
        self.maximum = 0
        self.reports = False
        self.running = False
        self.block_reads = False
        # normal | refuse (TCP reset at accept: printer off or unreachable) |
        # unauthorized (CONNACK 5: wrong LAN access code)
        self.mode = "normal"
        # What this simulated printer is: its serial (model prefix 01P = P1S) names the MQTT topic,
        # and its status reports carry the installed nozzle as a P1S does (None: not reported).
        self.serial = "01P00TESTONLY"
        self.nozzle = "0.4"
        self.attempts = 0
        self.refusals = 0
        # Peers whose link went dead: never read from or written to again, no PINGRESP.
        self.blackholed = set()
        # A printer that answers requests (get_version replies) while it sends no status report
        # and ignores full-state requests: the live P1S on 2026-11-03.
        self.answering = False
        # SUBACK failure (0x80) for the report topic; such a peer is sent nothing on that topic.
        self.refuse_subscribe = False
        self.unsubscribed = set()
        # Status reports flow only while a stream request ({"pushing": {"command": "start"}}) is at
        # most lease_s old: the request Bambu Studio sends after 20 s without a message
        # (DeviceCore/DevManager.cpp check_pushing) and ha-bambulab after 60 s. Each report advances
        # the layer. This is the vendor clients' renewal, not behaviour shown on the live firmware.
        self.lease_s = None
        self.lease_until = 0.
        self.layer = 2
        self.held_max = 0
        self.stop = threading.Event()
        threading.Thread(target=self.accept, daemon=True).start()
        threading.Thread(target=self.stream, daemon=True).start()

    def stream(self):
        while not self.stop.is_set():
            if self.lease_s and time.monotonic() < self.lease_until:
                self.broadcast(self.delta())
            time.sleep(.2)

    def delta(self):
        self.layer += 1
        return {"print": {"command": "push_status", "msg": 1, "layer_num": self.layer,
                          "mc_percent": min(99, self.layer), "nozzle_temper": 240., "bed_temper": 75.}}

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
            if self.nozzle is not None:
                data["print"]["nozzle_diameter"] = self.nozzle
        topic = f"device/{self.serial}/report".encode()
        body = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.send(peer, packet(0x30, len(topic).to_bytes(2, "big")+topic+body))

    def broadcast(self, data=None):
        for peer in list(self.peers - self.unsubscribed):
            try:
                self.report(peer, data)
            except OSError:
                pass

    def sample_held(self):
        self.held_max = max(self.held_max, held_mqtt_sockets())
        return self.held_max

    def blackhole(self):
        """The current link dies silently: no FIN/RST, no data and no PINGRESP from the printer."""
        with self.lock:
            dead = set(self.peers)
            self.blackholed |= dead
            self.peers -= dead

    def serve(self, raw):
        peer = None
        self.attempts += 1
        if self.mode == "refuse":
            self.refusals += 1
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            raw.close()
            return
        try:
            raw.settimeout(3)
            peer = self.ctx.wrap_socket(raw, server_side=True)
            # Faults below own disconnect timing. A ten-second idle-read timeout here could
            # close an otherwise healthy broker during slow FTPS/admission checks, before the
            # intended socket-backpressure start, turning "unknown delivery" into "not sent".
            # shutdown() in drop()/close() wakes this blocking read for fault injection/cleanup.
            peer.settimeout(None)
            self.peers.add(peer)
            self.connections += 1
            self.maximum = max(self.maximum, len(self.peers))
            self.sample_held()
            def read(n):
                data = b""
                while len(data)<n:
                    while (self.block_reads or peer in self.blackholed) and not self.stop.is_set():
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
                if typ == 1 and self.mode == "unauthorized":
                    self.refusals += 1
                    self.send(peer, b"\x20\x02\x00\x05")  # CONNACK: not authorized
                    break
                if typ == 1:
                    self.send(peer, b"\x20\x02\x00\x00")
                elif typ == 8:
                    if self.refuse_subscribe:
                        self.unsubscribed.add(peer)
                        self.send(peer, packet(0x90, body[:2]+b"\x80"))
                        continue
                    self.unsubscribed.discard(peer)
                    self.send(peer, packet(0x90, body[:2]+b"\x00"))
                    if self.reports:
                        self.report(peer)
                elif typ == 3:
                    size = int.from_bytes(body[:2], "big")
                    msg = json.loads(body[2+size:])
                    self.received.append(msg)
                    if peer in self.unsubscribed:
                        continue
                    if self.lease_s and msg.get("pushing", {}).get("command") == "start":
                        self.lease_until = time.monotonic() + self.lease_s
                    if self.reports and msg.get("pushing", {}).get("command") == "pushall":
                        self.report(peer)
                    elif self.answering and msg.get("info", {}).get("command") == "get_version":
                        self.report(peer, {"info": {"command": "get_version", "sequence_id": "0", "module": [
                            {"name": "ota", "sw_ver": "01.09.00.00", "product_name": "P1S"}]}})
                elif typ == 12:
                    self.send(peer, b"\xd0\x00")
                elif typ == 14:
                    break
        except (OSError, EOFError, ValueError):
            pass
        finally:
            if peer:
                self.peers.discard(peer)
                self.blackholed.discard(peer)
                self.unsubscribed.discard(peer)
                peer.close()
            else:
                raw.close()

    def mutations(self):
        return [m for m in self.received if not (
            m.get("pushing", {}).get("command") in ("pushall", "start") or
            m.get("info", {}).get("command") == "get_version")]

    def full_state_requests(self):
        return sum(1 for m in self.received if m.get("pushing", {}).get("command") == "pushall")

    def drop(self):
        # Shut down only; the peer's serve thread closes its own socket. Closing it here, while
        # that thread is inside SSL_read, freed the fd number while OpenSSL still read from it: the
        # next HTTP connection reused it and lost its first 5 bytes ("HTTP/") as a TLS record
        # header ("illegal status line: 1.1 200 OK", also nightly 2026-11-02 seed 309119290).
        for peer in list(self.peers):
            self.peers.discard(peer)
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.stop.set()
        self.drop()
        self.listener.close()

def payload(r):
    if r.structuredContent is not None:
        value = r.structuredContent
        if isinstance(value, dict) and set(value) == {"result"}:
            value = value["result"]
        text = value
    else:
        text = "\n".join(c.text for c in r.content if c.type == "text")
    if isinstance(text, str):
        try:
            text = json.loads(text)
        except ValueError:
            pass
    if isinstance(text, dict) and text.get("encoding") == "gzip+base64":
        return json.loads(gzip.decompress(base64.b64decode(text["data"])))
    return text

async def campaign(seed, cycles):
    rng = random.Random(seed)
    events = []
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        broker = Broker(tmp)
        broker.mode = "refuse"  # the printer is off when the server starts
        ftps = FTPSPeer(broker.ctx)
        camera = CameraPeer(broker.ctx)
        studio = tmp / "BambuStudio.conf"
        # A P1S (01P) and an A1 (039) on the same synthetic access code; the A1 is used once.
        studio.write_text(json.dumps({"user_access_code":{"01P00TESTONLY":"TESTONLY", "03900TESTONLY":"TESTONLY"}}))
        port = 25099  # private network namespace; production's registered port range
        env = os.environ.copy()
        env.update(HOME=str(tmp/"home"), BAMBU_MCP_PORT=str(port), BAMBU_MCP_DISABLE_REST="1",
                   BAMBU_MCP_ALLOW_CLIENT_CONFIG_WRITE="0", BAMBU_STUDIO_CONFIG=str(studio),
                   BAMBU_PRINTER_NAME="fixture", BAMBU_PRINTER_SERIAL="01P00TESTONLY",
                   BAMBU_PRINTER_HOST="127.0.0.1", BAMBU_MCP_FTPS_TIMEOUT="2",
                   # The server runs this fixed process as its preparer, as it runs the real
                   # Soyrack preflight in production; see fixture_preparer.py for what it is not.
                   BAMBU_PREFLIGHT_COMMAND_JSON=json.dumps([sys.executable, str(ROOT/"harness/fixture_preparer.py")]),
                   BAMBU_PREFLIGHT_TIMEOUT_S="3")
        store = tmp/"home"/".bambu-mcp"/"preflight"
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
            url = f"http://127.0.0.1:{port}/mcp"
            from watchdog import probe as watchdog_probe
            async with streamablehttp_client(url) as (rd, wr, _):
                async with ClientSession(rd, wr) as c:
                    await c.initialize()
                    names = {t.name for t in (await c.list_tools()).tools}
                    assert "get_printer_connection_status" in names
                    async def call(tool, **args):
                        return payload(await asyncio.wait_for(c.call_tool(tool, {"name":"fixture", **args}), 10))
                    async def until_live():
                        start = time.monotonic()
                        for _ in range(140):
                            h = await call("get_printer_connection_status")
                            if h.get("telemetry", {}).get("live"):
                                return time.monotonic()-start
                            await asyncio.sleep(.1)
                        raise AssertionError(f"not live: {h}")
                    async def health_until(predicate, what, timeout=10.):
                        start = time.monotonic()
                        while True:
                            h = (await call("get_printer_connection_status"))["telemetry"]
                            if predicate(h):
                                return h
                            if time.monotonic() - start > timeout:
                                raise AssertionError(f"{what}: {h}")
                            await asyncio.sleep(.1)
                    async def released_dead_socket():
                        # A refused reconnect can be ESTABLISHED for an instant; a leaked socket stays.
                        for _ in range(40):
                            if held_mqtt_sockets() == 0:
                                return
                            await asyncio.sleep(.05)
                        raise AssertionError(f"MCP still holds {held_mqtt_sockets()} MQTT socket(s) to a dead link")
                    async def refuses_writes():
                        refused = await call("set_bed_temp", temp=0, user_permission=True)
                        assert "telemetry_not_live" in str(refused), refused
                    # Printer off or unreachable when the server starts. The status must name the
                    # cause, writes are refused, reconnects back off instead of storming, and the
                    # MCP process itself answers the external watchdog as healthy.
                    offline = await health_until(lambda h: "cannot connect" in str(h.get("reason")),
                                                 "offline cause not reported")
                    assert not offline["live"] and not offline["writable"], offline
                    attempts, t0 = broker.attempts, time.monotonic()
                    await asyncio.sleep(2)
                    retries = broker.attempts - attempts
                    assert 1 <= retries <= 12, f"{retries} connection attempts in {time.monotonic()-t0:.1f}s"
                    assert (await watchdog_probe(url, "fixture"))["live"] is False
                    await refuses_writes()
                    events.append({"contract":"printer offline at server start: cause reported, writes "
                                   "refused, bounded retries, MCP healthy", "retries_in_2s":retries, "sent":0})
                    # Reachable, but the LAN access code is rejected (CONNACK 5).
                    broker.mode = "unauthorized"
                    denied = await health_until(lambda h: "not authorized" in str(h.get("reason")).lower(),
                                                "access-code refusal not reported")
                    assert not denied["writable"], denied
                    await refuses_writes()
                    assert broker.refusals and not broker.mutations(), broker.mutations()
                    events.append({"contract":"rejected access code reported as such, writes refused", "sent":0})
                    broker.mode = "normal"
                    starting = await call("get_print_progress")
                    assert starting.get("error") == "telemetry_not_live", starting
                    refused = await call("set_bed_temp", temp=0, user_permission=True)
                    assert "telemetry_not_live" in str(refused), refused
                    assert not broker.mutations(), broker.mutations()
                    events.append({"contract":"startup without telemetry refuses writes", "sent":0})
                    broker.reports = True
                    broker.broadcast()
                    await until_live()
                    assert (await watchdog_probe(url, "fixture"))["live"]
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
                            for _ in range(20):
                                if ftps.resets:
                                    break
                                await asyncio.sleep(.1)
                            assert ftps.resets, "job transition never reached the FTPS reset peer"
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
                        assert broker.sample_held() <= 1, ("MQTT sockets held", broker.held_max)
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
                    # 2026-11-03, live P1S printing from SD: MQTT stayed connected and the printer answered
                    # get_version and pings, but sent one partial report and then nothing, ignoring every
                    # full-state request. The supervisor dropped the connection 120 s after each connect
                    # (paho names our own disconnect() "Normal disconnection"), 39 times without effect,
                    # and health said "starting" throughout. A printer that answers is not a dead link:
                    # state stays unknown and writes refused, health says why, full-state requests keep a
                    # fixed cadence, the connection is replaced only on a widening schedule, and a late
                    # report is used on the same connection.
                    before = len(broker.mutations())
                    broker.reports = False
                    broker.answering = True
                    broker.drop()
                    for _ in range(60):
                        if broker.peers:
                            break
                        await asyncio.sleep(.05)
                    assert broker.peers, "paho never reconnected after the drop"
                    connections, requests, start = broker.connections, broker.full_state_requests(), time.monotonic()
                    restarts = (await call("get_printer_connection_status"))["telemetry"]["restarts"]
                    broker.broadcast({"print": {"command": "push_status", "mc_remaining_time": 210,
                                                "wifi_signal": "-61dBm", "sequence_id": "1"}})
                    while time.monotonic() - start < 4.:
                        assert broker.connections == connections, (
                            f"connection replaced {time.monotonic()-start:.1f}s after connect although the "
                            "printer answered requests", broker.connections - connections)
                        await asyncio.sleep(.1)
                    quiet = (await call("get_printer_connection_status"))["telemetry"]
                    assert quiet["status"] == "stale" and not quiet["live"] and not quiet["writable"], quiet
                    assert "no full status report" in quiet["reason"] and "answers" in quiet["reason"], quiet
                    assert quiet["full_state"]["unanswered"] >= 2, quiet
                    state = await call("get_print_progress")
                    assert state.get("error") == "telemetry_not_live", state
                    await refuses_writes()
                    sent = broker.full_state_requests() - requests
                    assert 4 <= sent <= 10, f"{sent} full-state requests in 4s at a 0.5s cadence"
                    assert any(m.get("pushing", {}).get("command") == "start" for m in broker.received), \
                        "report streaming was never requested"
                    # One replacement on the widening schedule, then none for a while.
                    while broker.connections == connections and time.monotonic() - start < 9.:
                        await asyncio.sleep(.1)
                    replaced = time.monotonic() - start
                    assert broker.connections == connections + 1 and replaced >= 4.5, (replaced, broker.connections)
                    await asyncio.sleep(3.)
                    assert broker.connections == connections + 1, "replacement connections did not widen"
                    again = (await call("get_printer_connection_status"))["telemetry"]
                    assert again["restarts"] == restarts + 1 and not again["writable"], again
                    # The next replacement reaches the configured cap (7s), rather than doubling
                    # indefinitely. The existing first-replacement check does not own saturation.
                    second_start = time.monotonic()
                    while broker.connections == connections + 1 and time.monotonic() - start < 16.:
                        await asyncio.sleep(.1)
                    second_at = time.monotonic() - start
                    assert broker.connections == connections + 2 and 11. <= second_at <= 14.5, (second_at, broker.connections)
                    connections += 1  # recovery below must keep this second replacement connection
                    # The printer reports again: the next full-state request is answered on the
                    # connection that exists, with no further reconnect.
                    broker.reports = True
                    await until_live()
                    assert broker.connections == connections + 1 and broker.maximum == 1, broker.connections
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"fault":"answering-printer-without-reports","replaced_after_s":round(replaced,1),
                                   "full_state_requests_4s":sent,"recovery_s":round(time.monotonic()-start,3),
                                   "replayed":0})
                    # Remaining 2026-11-03 failure after the fix above: one full report, then the printer went
                    # quiet again while still answering get_version. A stream request was due only with the
                    # supervisor's own full-state request, and bpm's watchdog, asking on the same 30 s cadence,
                    # usually took that slot first with a plain pushall (request times 69/99/159 s live).
                    # Whenever reports stop, the stream must be asked to resume whoever holds the slot, so
                    # reporting continues with advancing progress on the same single connection.
                    before, connections = len(broker.mutations()), broker.connections
                    broker.reports = False
                    broker.answering = True
                    broker.lease_s = 1.
                    broker.broadcast()  # the one full frame
                    await until_live()
                    first = (await call("get_print_progress"))["current_layer"]
                    start, lapses = time.monotonic(), []
                    while time.monotonic() - start < 6.:
                        h = (await call("get_printer_connection_status"))["telemetry"]
                        if not h["live"]:
                            lapses.append((round(time.monotonic() - start, 1), h["status"], h["reason"]))
                        await asyncio.sleep(.1)
                    progress = await call("get_print_progress")
                    assert not lapses, f"reporting lapsed in {len(lapses)} samples over 6s: {lapses[:3]}"
                    assert progress["current_layer"] >= first + 12, (first, progress)
                    # While reports stream, full state is still re-requested on the vendor keep-alive cadence.
                    broker.lease_s = None
                    pushalls, start = broker.full_state_requests(), time.monotonic()
                    while time.monotonic() - start < 2.5:
                        broker.broadcast(broker.delta())
                        await asyncio.sleep(.2)
                    kept = broker.full_state_requests() - pushalls
                    assert 1 <= kept <= 2, f"{kept} full-state requests in 2.5s of continuous reports"
                    assert broker.connections == connections and broker.maximum == 1, broker.connections
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    await health_until(lambda h: not h["live"], "stopped reports still read as live", timeout=5)
                    await refuses_writes()
                    broker.reports = True
                    events.append({"fault":"one-full-report-then-quiet","layers_streamed":progress["current_layer"]-first,
                                   "keepalive_requests_2_5s":kept,"replayed":0})
                    # The printer refuses the report subscription (SUBACK 0x80): nothing can arrive, so
                    # health names it and the connection is replaced until a subscription is granted.
                    connections = broker.connections
                    broker.refuse_subscribe = True
                    broker.drop()
                    refused = await health_until(lambda h: "subscription" in str(h.get("reason")),
                                                 "refused report subscription not reported", timeout=5)
                    assert not refused["live"] and not refused["writable"], refused
                    broker.refuse_subscribe = False
                    await until_live()
                    assert broker.connections >= connections + 2 and broker.maximum == 1, broker.connections
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    broker.answering = False
                    events.append({"fault":"report-subscription-refused","replayed":0})
                    # The link dies silently (no FIN/RST, no data, no PINGRESP) and the printer then
                    # stays unreachable: what the live P1S did on 2026-11-02. Telemetry must go
                    # not-live, writes are refused, the dead socket is released rather than left
                    # open, the MCP stays healthy, and the session returns by itself afterwards.
                    before, connections = len(broker.mutations()), broker.connections
                    broker.reports = False
                    start = time.monotonic()
                    broker.blackhole()
                    broker.mode = "refuse"
                    await asyncio.sleep(1.5)
                    state = await call("get_print_progress")
                    assert state.get("error") == "telemetry_not_live", state
                    await refuses_writes()
                    await health_until(lambda h: "cannot" in str(h.get("reason")),
                                       "dead link never abandoned", timeout=15)
                    await released_dead_socket()
                    assert (await watchdog_probe(url, "fixture"))["live"] is False
                    broker.blackholed.clear()
                    broker.mode = "normal"
                    broker.reports = True
                    await until_live()
                    await asyncio.sleep(.2)
                    assert broker.connections > connections, "session never came back"
                    assert len(broker.mutations()) == before and broker.maximum == 1, broker.mutations()
                    assert broker.sample_held() <= 1, broker.held_max
                    events.append({"fault":"silent-link-loss-then-unreachable",
                                   "recovery_s":round(time.monotonic()-start,3),"replayed":0})
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
                    # Public workflows use the actual parser and FTPS client, with a real
                    # archive downloaded over TLS. No tool/library method is replaced.
                    ftps.reset = False
                    project = await call("get_project_info", file_path="fixture-job.gcode.3mf")
                    assert project.get("plate_num") == 1, project
                    assert "PETG" in json.dumps(project) and "coupon_post.stl" in json.dumps(project), project
                    assert "fixture-job.gcode.3mf" in ftps.downloads, ftps.downloads
                    events.append({"contract":"previewless 3MF downloads and parses over FTPS"})
                    before = len(broker.mutations())
                    denied = await call("load_filament", unit_id=0, slot_id=254)
                    assert "user_permission" in str(denied), denied
                    wrong_holder = await call("load_filament", unit_id=0, slot_id=255, user_permission=True)
                    assert "dual-nozzle" in str(wrong_holder), wrong_holder
                    downloads = len(ftps.downloads)
                    # A valid sliced job already on the card (the file this campaign used to print)
                    # is not admitted, so it is refused before any FTPS read or MQTT command.
                    for filename, plate in [("broken.gcode.3mf",1),("fixture-job.gcode.3mf",2),
                                            ("fixture-job.gcode.3mf",1)]:
                        refused = await call("print_file", file_path=filename, plate_num=plate,
                                             use_ams=False, user_permission=True)
                        assert refused.get("error") == "print_not_admitted", refused
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    assert len(ftps.downloads) == downloads, ftps.downloads[downloads:]
                    events.append({"contract":"permission, invalid holder and unadmitted SD files refuse before sending", "sent":0})
                    loaded = await call("load_filament", unit_id=0, slot_id=254, user_permission=True)
                    assert "Load filament command sent" in str(loaded), loaded
                    commands = broker.mutations()[before:]
                    assert len(commands) == 1, commands
                    load = commands[0]["print"]
                    assert (load["command"],load["target"],load["ams_id"],load["slot_id"]) == (
                        "ams_change_filament",254,254,0), load
                    events.append({"contract":"external holder loads without AMS", "sent":1})
                    await until_live()
                    # ── Print admission. Only the server's own preparer run can admit a job; the
                    # peer counts STOR transfers and received MQTT packets, not method calls.
                    async def prepare(part):
                        return payload(await asyncio.wait_for(
                            c.call_tool("prepare_print_job", {"part": part}), 15))
                    async def upload(local, remote):
                        return await call("upload_file", local_path=str(local), remote_path=remote,
                                          user_permission=True)
                    async def start(remote, **args):
                        return await call("print_file", file_path=remote, use_ams=False,
                                          user_permission=True, **args)
                    def runs():
                        return sorted(p.name for p in store.iterdir() if p.is_dir()) if store.is_dir() else []
                    async def replace_server(old, server_env):
                        old.kill(); old.wait(timeout=5)
                        for _ in range(100):
                            if not broker.peers:
                                break
                            await asyncio.sleep(.02)
                        assert not broker.peers, "dead server still owns MQTT socket"
                        new = subprocess.Popen([sys.executable, str(ROOT/"harness/fixture_server.py")],
                                               env=server_env, cwd=ROOT, stdout=log, stderr=log)
                        for _ in range(100):
                            assert new.poll() is None, "replacement fixture died"
                            try:
                                probe = socket.create_connection(("127.0.0.1", port), .1)
                                probe.close(); break
                            except OSError:
                                await asyncio.sleep(.1)
                        await until_live()
                        assert broker.maximum == 1, broker.maximum
                        return new
                    # A connected printer reports on its own; the many write calls below must not land in
                    # the stale window between this fixture's on-request reports.
                    reporting = asyncio.Event()
                    async def keep_reporting():
                        while not reporting.is_set():
                            broker.broadcast()
                            await asyncio.sleep(.3)
                    reporter = asyncio.create_task(keep_reporting())
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    seen = runs()
                    for bad in ("../rim", "-rf", "Drive Tray", "x"*65, ""):
                        refused = await prepare(bad)
                        assert refused.get("error") == "invalid_part", (bad, refused)
                    assert runs() == seen, "an invalid part name reached the preparer"
                    # Outcomes a preparer can produce that must not admit anything. A passing-looking
                    # report never stands in for a successful run, and the job left behind stays
                    # unprintable.
                    for part, code in {"fail_exit":"preflight_failed", "hang":"preflight_timeout",
                                       "no_report":"preflight_rejected", "forged_hash":"preflight_rejected",
                                       "geometry_fail":"preflight_rejected", "problems":"preflight_rejected",
                                       "old_schema":"preflight_rejected", "running":"preflight_rejected",
                                       "extra_part":"preflight_rejected", "symlink_job":"preflight_rejected",
                                       "no_target":"preflight_rejected", "target_x1c":"preflight_rejected",
                                       "job_target_mismatch":"preflight_rejected"}.items():
                        refused = await prepare(part)
                        assert refused.get("error") == code and "receipt_id" not in refused, (part, refused)
                        left = [p for p in store.glob(f"*/{part}/{part}.gcode.3mf")]
                        assert left, (part, "fixture wrote no job")
                        refused = await upload(left[-1], f"/{part}.gcode.3mf")
                        assert refused.get("error") == "job_not_prepared", (part, refused)
                    assert "rim at z 12 mm" in json.dumps((await prepare("problems")).get("report")), "problems not reported"
                    events.append({"contract":"failed, hung, forged, incomplete, untargeted, unqualified and "
                                   "target-contradicting preparer runs admit nothing", "preparer_outcomes":13, "sent":0})
                    # A caller's own PASS report and job, and an exact copy of a prepared job, are not
                    # receipts. A job edited after preparation is refused at upload.
                    from printer_peers import project_archive
                    forged = tmp/"forged"/"rim_ok"
                    forged.mkdir(parents=True)
                    (forged/"rim_ok.gcode.3mf").write_bytes(project_archive(marker="rim_ok"))
                    digest = hashlib.sha256((forged/"rim_ok.gcode.3mf").read_bytes()).hexdigest()
                    (forged.parent/"preflight.json").write_text(json.dumps({
                        "schema_version":2, "geometry_policy_version":1, "pass":True, "state":"complete",
                        "parts":{"rim_ok":{"problems":[], "geometry":{"pass":True}, "job":digest}}}))
                    prep = await prepare("rim_ok")
                    assert prep.get("success") and prep["plates"] == [1], prep
                    assert prep["target"] == {"printer_model":"Bambu Lab P1S", "nozzle_diameter":["0.4"],
                                              "plate_type":"Textured PEI Plate", "bed_type":"textured_plate"}, prep
                    job = Path(prep["job_path"]).read_bytes()
                    assert hashlib.sha256(job).hexdigest() == prep["sha256"], prep
                    (tmp/"copy.gcode.3mf").write_bytes(job)
                    for local in (forged/"rim_ok.gcode.3mf", tmp/"copy.gcode.3mf"):
                        refused = await upload(local, "/forged.gcode.3mf")
                        assert refused.get("error") == "job_not_prepared", refused
                    edited = await prepare("rim_ok")
                    Path(edited["job_path"]).write_bytes(job + b"\0")
                    refused = await upload(edited["job_path"], "/edited.gcode.3mf")
                    assert refused.get("error") == "job_modified", refused
                    refused = await upload(prep["job_path"], "/admitted.bin")
                    assert refused.get("error") == "remote_not_project", refused
                    assert len(ftps.stores) == stores, ftps.stores[stores:]
                    # A non-job file still transfers, but cannot be renamed into a printable job, and a
                    # download cannot write into the prepared-job store.
                    (tmp/"notes.txt").write_text("diagnostics")
                    sent = await upload(tmp/"notes.txt", "/notes.txt")
                    assert sent.get("success") and "admission" not in sent and ftps.stores[-1] == "notes.txt", sent
                    renames = sum(1 for verb, _ in ftps.commands if verb == "RNFR")
                    refused = await call("rename_sdcard_file", src_path="/notes.txt",
                                         dest_path="/notes.gcode.3mf", user_permission=True)
                    assert refused.get("error") == "rename_creates_job", refused
                    assert sum(1 for verb, _ in ftps.commands if verb == "RNFR") == renames
                    refused = await call("download_file", remote_path="/notes.txt",
                                         local_path=str(Path(prep["job_path"])), user_permission=True)
                    assert refused.get("error") == "local_path_reserved", refused
                    assert Path(prep["job_path"]).read_bytes() == job
                    events.append({"contract":"caller reports, copies, edited jobs and renamed files are not admitted",
                                   "uploads":len(ftps.stores)-stores})
                    # Raw tools cannot start a file, write the SD card or extrude: refused by the raw
                    # allowlist, or on the MQTT publish path itself (send_gcode has no own check).
                    before = len(broker.mutations())
                    for raw in ({"print":{"command":"project_file", "url":"file:///sdcard/admitted.gcode.3mf",
                                          "file":"/admitted.gcode.3mf", "param":"Metadata/plate_1.gcode",
                                          "sequence_id":"0"}},
                                {"print":{"command":"gcode_file", "param":"/admitted.gcode", "sequence_id":"0"}},
                                {"print":{"command":"gcode_line", "param":"M23 /a.gcode\nM24\n", "sequence_id":"0"}},
                                {"print":{"command":"pause", "sequence_id":"0"}, "pushing":{"command":"pushall"}},
                                {"upgrade":{"command":"start", "url":"http://x/fw.bin", "sequence_id":"0"}}):
                        refused = await call("send_mqtt_command", command_json=json.dumps(raw), user_permission=True)
                        assert refused.get("error") == "raw_command_refused", (raw, refused)
                    for gcode in ("M23 /admitted.gcode.3mf\nM24", "M32 /admitted.gcode.3mf", "G1 X10 E5",
                                  "g1e5", "G28\nG1 X5 Y5 E0.4 F600", "M28 job.gcode", "M810 G1 E5", "T1"):
                        refused = await call("send_gcode", gcode=gcode, user_permission=True)
                        assert "publish_rejected" in str(refused) and "not allowed" in str(refused), (gcode, refused)
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    off = await call("send_gcode", gcode="M104 S0\nM140 S0 ; heaters off", user_permission=True)
                    assert "G-code sent" in str(off), off
                    paused = await call("send_mqtt_command", user_permission=True,
                                        command_json=json.dumps({"print":{"command":"pause", "sequence_id":"0"}}))
                    assert paused.get("success"), paused
                    await asyncio.sleep(.1)
                    commands = broker.mutations()[before:]
                    assert [m["print"]["command"] for m in commands] == ["gcode_line", "pause"], commands
                    events.append({"contract":"raw MQTT and G-code cannot start, write SD or extrude; maintenance still sent",
                                   "refused":13, "sent":2})
                    # The admitted path: plate and options are checked before the admission is used, the
                    # card is re-read before the start, and exactly one bound start reaches the wire.
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    up = await upload(prep["job_path"], "admitted.gcode.3mf")
                    assert up.get("admission", {}).get("sha256") == prep["sha256"] and up["remote_path"] == "/admitted.gcode.3mf", up
                    assert ftps.files["admitted.gcode.3mf"] == job and ftps.stores[stores:] == ["admitted.gcode.3mf"]
                    for wrong, code in (({"plate_num":2}, "plate_not_admitted"), ({"bed_type":"lava_plate"}, "invalid_bed_type"),
                                        ({"bed_type":"cool_plate"}, "bed_type_mismatch")):
                        refused = await start("/admitted.gcode.3mf", **wrong)
                        assert refused.get("error") == code, (wrong, refused)
                    # The printer must match the prepared target as it reports itself now: a 0.6 mm nozzle
                    # is refused, and a 0.4 report from before a reconnect does not count for the new link.
                    async def nozzle_reported(value):
                        await health_until(lambda h: h.get("reported_nozzle_diameter") == value,
                                           f"nozzle {value!r} never reported", timeout=5)
                    broker.nozzle = "0.6"
                    await nozzle_reported("0.6")
                    refused = await start("/admitted.gcode.3mf")
                    assert refused.get("error") == "nozzle_mismatch", refused
                    # Numeric parsing alone is insufficient: NaN compares unequal to no diameter,
                    # so a naïve absolute-difference guard can accidentally authorize a start.
                    for invalid in ("NaN", "Infinity", "0"):
                        broker.nozzle = invalid
                        await nozzle_reported(invalid)
                        refused = await start("/admitted.gcode.3mf")
                        assert refused.get("error") == "nozzle_unknown", (invalid, refused)
                    broker.nozzle = None
                    broker.drop()
                    await until_live()
                    await nozzle_reported(None)
                    refused = await start("/admitted.gcode.3mf")
                    assert refused.get("error") == "nozzle_unknown", refused
                    broker.nozzle = "0.4"
                    await nozzle_reported("0.4")
                    ftps.files["admitted.gcode.3mf"] = job[:-1] + bytes([job[-1] ^ 1])  # replaced on the card
                    refused = await start("/admitted.gcode.3mf")
                    assert refused.get("error") == "remote_file_mismatch", refused
                    ftps.files["admitted.gcode.3mf"] = job
                    refused = await start("/admitted.gcode.3mf")
                    assert refused.get("error") == "print_not_admitted", refused
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    await upload(prep["job_path"], "/admitted.gcode.3mf")
                    downloads = len(ftps.downloads)
                    printed = await start("/admitted.gcode.3mf")
                    assert "error" not in printed and printed["admission"]["consumed"] is True, printed
                    assert "admitted.gcode.3mf" in ftps.downloads[downloads:], "card not re-read before the start"
                    await asyncio.sleep(.1)
                    commands = broker.mutations()[before:]
                    assert len(commands) == 1, commands
                    command = commands[0]["print"]
                    assert (command["command"], command["file"], command["url"], command["param"]) == (
                        "project_file", "/admitted.gcode.3mf", "file:///sdcard/admitted.gcode.3mf",
                        "Metadata/plate_1.gcode"), command
                    assert command["use_ams"] is False and command["ams_mapping"] == [-1], command
                    assert command["bed_type"] == "textured_plate", command  # bed_type auto -> the prepared plate
                    assert command["md5"] == hashlib.md5(job).hexdigest().upper(), command
                    again = await start("/admitted.gcode.3mf")
                    assert again.get("error") == "print_not_admitted", again
                    assert len(broker.mutations()) == before + 1, broker.mutations()[before:]
                    events.append({"contract":"prepared job: wrong plate, bed type, nozzle, unreported nozzle and "
                                   "remote tamper refused; verified start names the prepared plate and MD5, "
                                   "reaches the wire once; repeat refused", "sent":1})
                    # Upload, rename and delete drop the admissions they could invalidate, first. The card
                    # is case-insensitive, so a rename onto another spelling of the path counts.
                    before = len(broker.mutations())
                    for remote, change in (("/rename-me.gcode.3mf", ("rename_sdcard_file",
                                            {"src_path":"/rename-me.gcode.3mf", "dest_path":"/renamed.gcode.3mf"})),
                                           ("/target.gcode.3mf", ("rename_sdcard_file",
                                            {"src_path":"/broken.gcode.3mf", "dest_path":"/TARGET.gcode.3mf"})),
                                           ("/delete-me.gcode.3mf", ("delete_file", {"remote_path":"/delete-me.gcode.3mf"}))):
                        assert (await upload(prep["job_path"], remote)).get("admission"), remote
                        done = await call(change[0], user_permission=True, **change[1])
                        assert done.get("success"), (change, done)
                        for path in {remote, change[1].get("dest_path", remote)}:
                            refused = await start(path)
                            assert refused.get("error") == "print_not_admitted", (change, path, refused)
                    assert (await upload(prep["job_path"], "/vanish.gcode.3mf")).get("admission")
                    del ftps.files["vanish.gcode.3mf"]  # removed from the card by another client
                    refused = await start("/vanish.gcode.3mf")
                    assert refused.get("error") == "remote_file_missing", refused
                    refused = await start("/vanish.gcode.3mf")
                    assert refused.get("error") == "print_not_admitted", refused
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"contract":"rename, overwrite-by-rename, delete and external removal revoke "
                                   "admission", "sent":0})
                    # Unknown delivery of an admitted start: the MQTT socket is full, so the start cannot be
                    # written within the publish bound. The admission is already consumed; the outcome is
                    # unknown, nothing is retried, and calling print_file again is refused.
                    assert (await upload(prep["job_path"], "/uncertain.gcode.3mf")).get("admission")
                    before = len(broker.mutations())
                    broker.block_reads = True  # the reporter keeps telemetry live meanwhile
                    filler = await call("send_gcode", gcode=";HARNESS-ONLY-"+"x"*(8*1024*1024), user_permission=True)
                    assert "publish_unconfirmed" in str(filler), str(filler)[:500]
                    uncertain = await start("/uncertain.gcode.3mf")
                    assert (uncertain.get("error"), uncertain.get("sent"), uncertain.get("retried")) == (
                        "publish_unconfirmed", "unknown", False), uncertain
                    assert uncertain["tool_result"].get("admission") == "consumed", uncertain
                    broker.drop()
                    broker.block_reads = False
                    await until_live()
                    await asyncio.sleep(.3)
                    again = await start("/uncertain.gcode.3mf")
                    assert again.get("error") == "print_not_admitted", again
                    assert len(broker.mutations()) == before, "an unconfirmed start was replayed"
                    events.append({"contract":"unknown start delivery consumes admission; never replayed", "replayed":0})
                    # Admission lives in server memory: a restarted server admits nothing it did not prepare
                    # itself. The replacement serves an A1 (serial 039...): a P1S job prepared and uploaded
                    # there is refused as the wrong model before any card read or command.
                    assert (await upload(prep["job_path"], "/restart.gcode.3mf")).get("admission")
                    before, downloads = len(broker.mutations()), len(ftps.downloads)
                    broker.serial = "03900TESTONLY"
                    proc = await replace_server(proc, {**env, "BAMBU_PRINTER_SERIAL":"03900TESTONLY"})
                    refused = await start("/restart.gcode.3mf")
                    assert refused.get("error") == "print_not_admitted", refused
                    refused = await upload(prep["job_path"], "/restart.gcode.3mf")
                    assert refused.get("error") == "job_not_prepared", refused
                    other = await prepare("rim_ok")
                    assert other.get("success"), other
                    assert (await upload(other["job_path"], "/other-model.gcode.3mf")).get("admission")
                    refused = await start("/other-model.gcode.3mf")
                    assert refused.get("error") == "printer_model_mismatch" and "A1" in refused["detail"], refused
                    assert "other-model.gcode.3mf" not in ftps.downloads[downloads:], ftps.downloads[downloads:]
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"contract":"restart drops every admission; a P1S job on an A1 is refused", "sent":0})
                    # A replacement with an untrusted preparer (world-writable directory) refuses without
                    # running it.
                    untrusted = tmp/"world-writable"
                    untrusted.mkdir()
                    os.chmod(untrusted, 0o777)
                    (untrusted/"prep.py").write_text(f"open({str(tmp/'ran')!r}, 'w').close()\n")
                    broker.serial = "01P00TESTONLY"
                    proc = await replace_server(proc, {**env, "BAMBU_PREFLIGHT_COMMAND_JSON":
                                                       json.dumps([sys.executable, str(untrusted/"prep.py")])})
                    refused = await prepare("rim_ok")
                    assert refused.get("error") == "preflight_command_untrusted", refused
                    assert not (tmp/"ran").exists(), "untrusted preparer was run"
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    proc = await replace_server(proc, env)
                    events.append({"contract":"untrusted preparer refused without running it", "sent":0})
                    reporting.set()
                    await reporter
                    broker.running = True
                    broker.broadcast()
                    for _ in range(20):
                        if (await call("get_print_progress")).get("gcode_state") == "RUNNING":
                            break
                        await asyncio.sleep(.1)
                    else:
                        raise AssertionError("RUNNING telemetry not delivered")
                    before = len(broker.mutations())
                    for tool, args in [
                        ("load_filament", {"unit_id":0,"slot_id":254}),
                        ("print_file", {"file_path":"fixture-job.gcode.3mf","use_ams":False}),
                        ("send_gcode", {"gcode":"G1 X20"}),
                    ]:
                        refused = await call(tool, user_permission=True, **args)
                        assert "Blocked" in str(refused), (tool, refused)
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"contract":"active print refuses load, new print and G-code", "sent":0})
                    for invalid in (False, True, False):
                        camera.invalid = invalid
                        result = await asyncio.wait_for(c.call_tool("get_camera_image", {"name":"fixture"}), 10)
                        if invalid:
                            assert payload(result).get("error") == "camera_capture_failed", result
                        else:
                            frames = [x for x in result.content if x.type == "image"]
                            assert len(frames) == 1 and base64.b64decode(frames[0].data) == camera.frames[1], result
                    # Active-job monitoring may capture extra frames concurrently. The
                    # three public responses above own freshness/error/recovery checks;
                    # the peer only requires that those authenticated connections exist.
                    assert camera.authenticated >= 3, camera.authenticated
                    events.append({"contract":"camera returns fresh second frame and recovers after invalid header"})
                    # A second process must fail at the shared singleton before opening MQTT.
                    before = broker.connections
                    duplicate = await asyncio.create_subprocess_exec(
                        sys.executable, str(ROOT/"server.py"), "--transport", "stdio",
                        env=env, cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    stdout, stderr = await asyncio.wait_for(duplicate.communicate(), 10)
                    assert duplicate.returncode != 0 and b"already" in stderr.lower(), (stdout, stderr)
                    assert broker.connections == before and broker.maximum == 1
                    events.append({"contract":"second HTTP/stdio owner refused before MQTT"})
            proc.terminate()
            proc.wait(timeout=14)
            assert proc.returncode in (0,143,-15), proc.returncode
            return {"seed":seed,"cycles":cycles,"events":events,"max_mqtt_clients":broker.maximum,
                    "mqtt_connections":broker.connections,"max_held_mqtt_sockets":broker.held_max,
                    "connection_attempts":broker.attempts,"simulated_printer_commands":len(broker.mutations()),
                    "ftps_downloads":ftps.downloads, "camera_captures":camera.captures,
                    "transport":"MCP HTTP + TLS MQTT/FTPS/camera", "passed":True}
        except BaseException:
            log.flush(); log.seek(0)
            print(log.read()[-10000:], file=sys.stderr)
            raise
        finally:
            if proc.poll() is None:
                proc.kill(); proc.wait(timeout=5)
            broker.close()
            ftps.close()
            camera.close()
            log.close()

if __name__ == "__main__":
    print(json.dumps(asyncio.run(campaign(int(sys.argv[1]), int(sys.argv[2])))))
