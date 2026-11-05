"""E2E workflows and faults through MCP HTTP and TLS MQTT/FTPS/camera peers.

Exercises the installed libraries, MCP serialization and process lifecycle.
Run only inside run.py's namespace.
Seeds are recorded and replayable. No production credentials or printer address.
"""
import asyncio
import base64
import collections
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import re
import select
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zipfile

if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from printer_peers import CARD_RECORD, FTPSPeer, CameraPeer, card_record

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
        # SD card state in each status report, as a P1 sends it: the sdcard bool and home_flag
        # bits 8-9 (Bambu Studio DevStorage). None: neither field is sent.
        self.sd = "normal"
        # Chamber light mode in each status report as lights_report, as a P1 sends it (None: not
        # reported). A ledctrl for chamber_light changes it and the printer reports the change at
        # once; ignore_light models a printer that does not act on the command.
        self.light = None
        self.ignore_light = False
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
        # Why each peer connection ended and any write that failed on it, printed on failure: the
        # MCP side only sees paho's generic "Unspecified error".
        self.wire_events = []
        # Each peer's TLS socket is read and written only by its own serve thread (one SSL object
        # used from two threads corrupts records). Others queue packets here: peer -> (deque, wake).
        self.outbox = {}
        self.kill = set()
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

    def note(self, peer, what):
        self.wire_events.append(f"{time.strftime('%H:%M:%S')}.{int(time.time()*1000)%1000:03d} "
                                f"peer {id(peer) % 10000:04d} {what}")

    def send(self, peer, data):
        """Queue a packet for the peer's serve thread, the only writer of its TLS socket."""
        box = self.outbox.get(peer)
        if box is None:
            raise ConnectionError("peer connection has ended")
        box[0].append(data)
        try:
            box[1].send(b"\0")
        except OSError:
            pass

    def wake(self, peer):
        box = self.outbox.get(peer)
        if box is not None:
            try:
                box[1].send(b"\0")
            except OSError:
                pass

    def report(self, peer, data=None):
        if data is None:
            data = {"print": {"command": "push_status", "gcode_state": "RUNNING" if self.running else "IDLE",
                    "nozzle_temper": 240., "bed_temper": 75., "mc_percent": 3 if self.running else 0,
                    "gcode_file": "Metadata/plate_1.gcode", "subtask_name": "fixture-job",
                    "task_id": "77" if self.running else "0", "layer_num": 2, "total_layer_num": 115}}
            if self.nozzle is not None:
                data["print"]["nozzle_diameter"] = self.nozzle
            if self.sd is not None:
                present, code = {"absent": (False, 0), "normal": (True, 1), "abnormal": (True, 2),
                                 "read_only": (True, 3)}[self.sd]
                data["print"].update(sdcard=present, home_flag=code << 8)
            if self.light is not None:
                data["print"]["lights_report"] = self.lights()
        topic = f"device/{self.serial}/report".encode()
        body = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.send(peer, packet(0x30, len(topic).to_bytes(2, "big")+topic+body))

    def lights(self):
        return [{"node": "chamber_light", "mode": self.light}, {"node": "work_light", "mode": "flashing"}]

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

    @staticmethod
    def take_packet(buf):
        """One complete MQTT packet (type, body) removed from the front of buf, or None."""
        length, scale, i = 0, 1, 1
        while True:
            if i >= len(buf):
                return None
            b = buf[i]
            length += (b & 127) * scale
            i += 1
            if not b & 128:
                break
            scale *= 128
            if i > 4:
                raise ValueError("malformed remaining length")
        if len(buf) < i + length:
            return None
        typ, body = buf[0] >> 4, bytes(buf[i:i+length])
        del buf[:i+length]
        return typ, body

    def handle(self, peer, typ, body):
        """React to one packet from the client; True when the connection must end after the replies."""
        if typ == 1 and self.mode == "unauthorized":
            self.refusals += 1
            self.send(peer, b"\x20\x02\x00\x05")  # CONNACK: not authorized
            return True
        if typ == 1:
            self.send(peer, b"\x20\x02\x00\x00")
        elif typ == 8:
            if self.refuse_subscribe:
                self.unsubscribed.add(peer)
                self.send(peer, packet(0x90, body[:2]+b"\x80"))
                return False
            self.unsubscribed.discard(peer)
            self.send(peer, packet(0x90, body[:2]+b"\x00"))
            if self.reports:
                self.report(peer)
        elif typ == 3:
            size = int.from_bytes(body[:2], "big")
            msg = json.loads(body[2+size:])
            self.received.append(msg)
            if peer in self.unsubscribed:
                return False
            led = msg.get("system", {})
            if led.get("command") == "ledctrl" and led.get("led_node") == "chamber_light" and not self.ignore_light:
                self.light = led.get("led_mode")
                self.report(peer, {"print": {"command": "push_status", "lights_report": self.lights()}})
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
            raise EOFError()  # DISCONNECT
        return False

    def serve(self, raw):
        peer = None
        self.attempts += 1
        if self.mode == "refuse":
            self.refusals += 1
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            raw.close()
            return
        wake_r, wake_w = socket.socketpair()
        wake_r.setblocking(False)
        out = collections.deque()
        try:
            raw.settimeout(3)
            peer = self.ctx.wrap_socket(raw, server_side=True)
            # From here this thread alone reads and writes the socket, without blocking: replies,
            # reports and broadcasts from other threads wait in out. block_reads stops reading only
            # (socket backpressure), a blackholed link neither reads nor writes, and drop() asks
            # this thread to shut the connection down.
            peer.setblocking(False)
            self.outbox[peer] = (out, wake_w)
            self.peers.add(peer)
            self.connections += 1
            self.maximum = max(self.maximum, len(self.peers))
            self.sample_held()
            inbuf, pending, ending = bytearray(), b"", False
            while not self.stop.is_set():
                if peer in self.kill:
                    self.note(peer, "ended: dropped by the fault")
                    socket.socket.shutdown(peer, socket.SHUT_RDWR)
                    break
                dead = peer in self.blackholed
                reading = not (self.block_reads or dead or ending)
                if not pending and out and not dead:
                    pending = out.popleft()
                if reading and peer.pending():
                    readable, writable = [peer], []
                else:
                    readable, writable, _ = select.select([wake_r] + ([peer] if reading else []),
                                                          [peer] if pending and not dead else [], [], .05)
                if wake_r in readable:
                    try:
                        wake_r.recv(4096)
                    except BlockingIOError:
                        pass
                if peer in writable:
                    try:
                        pending = pending[peer.send(pending):]
                    except (ssl.SSLWantWriteError, ssl.SSLWantReadError, BlockingIOError):
                        pass
                if ending and not pending and not out:
                    break
                if peer in readable:
                    try:
                        chunk = peer.recv(65536)
                    except (ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError):
                        continue
                    if not chunk:
                        raise EOFError()
                    inbuf += chunk
                    while not ending and (parsed := self.take_packet(inbuf)) is not None:
                        ending = self.handle(peer, *parsed)
        except EOFError:
            self.note(peer, "ended: client closed (EOF or DISCONNECT)")
        except (OSError, ValueError) as exc:
            self.note(peer, f"ended: {type(exc).__name__}: {exc}")
        finally:
            if peer:
                self.outbox.pop(peer, None)
                self.peers.discard(peer)
                self.blackholed.discard(peer)
                self.unsubscribed.discard(peer)
                self.kill.discard(peer)
                peer.close()
            else:
                raw.close()
            wake_r.close()
            wake_w.close()

    def mutations(self):
        return [m for m in self.received if not (
            m.get("pushing", {}).get("command") in ("pushall", "start") or
            m.get("info", {}).get("command") == "get_version")]

    def full_state_requests(self):
        return sum(1 for m in self.received if m.get("pushing", {}).get("command") == "pushall")

    def drop(self):
        # The peer's serve thread shuts its own connection down. Closing it here, while that thread
        # was inside SSL_read, freed the fd number while OpenSSL still read from it: the next HTTP
        # connection reused it and lost its first 5 bytes ("HTTP/") as a TLS record header
        # ("illegal status line: 1.1 200 OK", also nightly 2026-11-02 seed 309119290).
        dropped = list(self.peers)
        for peer in dropped:
            self.peers.discard(peer)
            self.kill.add(peer)
            self.wake(peer)
        # Return once every dropped connection is shut down (its owner thread has ended), as when
        # this closed them directly, so a fault takes effect before the next check.
        deadline = time.monotonic() + 3
        while any(peer in self.outbox for peer in dropped) and time.monotonic() < deadline:
            time.sleep(.01)

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
                   BAMBU_PREFLIGHT_TIMEOUT_S="3", BAMBU_HARNESS_CLOCK_FILE=str(tmp/"clock"))
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
                    async def replacement_connected(connections):
                        # A drop is seen by the server asynchronously; until the replacement connection
                        # exists, health can still describe the dropped one.
                        for _ in range(200):
                            if broker.connections > connections:
                                return
                            await asyncio.sleep(.05)
                        raise AssertionError("no replacement MQTT connection after the drop")
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
                        fault = ["drop", "silence", "malformed", "job-no-ftps"][i % 4]
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
                            # 2026-11-03: the P1S printing from SD sent no status report while our
                            # 3MF download ran. The job start used to trigger a project lookup over
                            # FTPS within 2 s; no connection may reach the card during a job.
                            ftps_before = ftps.connections
                            broker.running = not broker.running
                            broker.broadcast()
                            await asyncio.sleep(.2)
                            progress = await call("get_print_progress")
                            assert progress.get("gcode_state") == ("RUNNING" if broker.running else "IDLE"), progress
                            if broker.running:
                                listing = await call("list_sdcard_files")
                                assert "SD card access is refused while the printer reports gcode_state RUNNING" \
                                    in str(listing), listing
                            await asyncio.sleep(1.5)
                            assert ftps.connections == ftps_before, ("FTPS during a job transition",
                                                                     ftps.connections - ftps_before)
                        if fault != "job-no-ftps":
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
                    connections = broker.connections
                    broker.drop()
                    broker.block_reads = False
                    await replacement_connected(connections)
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
                    # Quiet hours for the chamber light (night_light.py), driven by the server's own timer
                    # and the printer's reports. The simulated printer reports lights_report and acts on
                    # ledctrl; the server's wall clock is the epoch written to the clock file. Every
                    # command counted here is a received MQTT packet. Auckland is UTC+13 in November
                    # (NZDT) and UTC+12 in June (NZST): a fixed offset gets one of the two wrong.
                    def clock(when):
                        (tmp/"clock").write_text(str(dt.datetime.fromisoformat(when).timestamp()))
                    def light_cmds():
                        return [m["system"]["led_mode"] for m in broker.mutations()
                                if m.get("system", {}).get("command") == "ledctrl"
                                and m["system"].get("led_node") == "chamber_light"]
                    async def eventually(check, what, timeout=5.):
                        start = time.monotonic()
                        while not check():
                            if time.monotonic() - start > timeout:
                                raise AssertionError(f"{what}: light={broker.light} cmds={light_cmds()}")
                            await asyncio.sleep(.05)
                    async def status():
                        return await call("get_night_light_policy")
                    async def reported(mode):
                        for _ in range(60):
                            if (await status())["light"]["reported"] == mode:
                                return
                            await asyncio.sleep(.05)
                        raise AssertionError(f"light not reported {mode}: {await status()}")
                    async def lease_closed():  # closed once the server has seen the light reported off
                        for _ in range(60):
                            if (await status())["lease"] is None:
                                return
                            await asyncio.sleep(.05)
                        raise AssertionError(f"lease still open: {await status()}")
                    pacing = {"on": True, "done": False}
                    async def pace():  # a connected printer keeps reporting, as above
                        while not pacing["done"]:
                            if pacing["on"]:
                                broker.broadcast()
                            await asyncio.sleep(.25)
                    pacer = asyncio.create_task(pace())
                    expected = light_cmds()
                    clock("2026-11-05T09:30:00+00:00")  # 22:30 NZDT
                    st = await status()
                    assert st["policy"] is None and st["light"]["reported"] == "unknown", st
                    refused = await call("set_night_light_policy", enabled=True, timezone="Pacific/Auckland")
                    assert "user_permission" in str(refused.get("error")), refused
                    for bad in ({"timezone": "Mars/Olympus_Mons"}, {"timezone": ""},
                                {"timezone": "Pacific/Auckland", "start": "24:00"},
                                {"timezone": "Pacific/Auckland", "start": "7:00", "end": "22:00"},
                                {"timezone": "Pacific/Auckland", "start": "07:00", "end": "07:00"}):
                        refused = await call("set_night_light_policy", enabled=True, user_permission=True, **bad)
                        assert "nothing stored" in str(refused.get("error")), (bad, refused)
                    assert (await status())["policy"] is None
                    st = await call("set_night_light_policy", enabled=True, timezone="Pacific/Auckland",
                                    start="22:00", end="07:00", user_permission=True)
                    assert st["policy"]["enabled"] and st["quiet_now"], st
                    # Inside quiet hours with the light never reported: unknown, not off, nothing sent.
                    await asyncio.sleep(1)
                    st = await status()
                    assert st["light"]["reported"] == "unknown" and st["last_action"] is None, st
                    assert light_cmds() == expected, light_cmds()
                    # A light switched on by hand in the evening stays on, in either season.
                    broker.light = "on"
                    clock("2026-11-05T08:59:50+00:00")  # 21:59:50 NZDT
                    await reported("on")
                    await asyncio.sleep(1)
                    clock("2026-06-15T09:30:00+00:00")  # 21:30 NZST; 22:30 at a fixed +13
                    await asyncio.sleep(1)
                    st = await status()
                    assert not st["quiet_now"] and st["light"]["reported"] == "on", st
                    assert light_cmds() == expected and broker.light == "on", light_cmds()
                    # The boundary: exactly one OFF, then nothing more for a light reported off.
                    clock("2026-11-05T09:00:00+00:00")  # 22:00 NZDT
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off", "no OFF at 22:00")
                    await asyncio.sleep(1.5)
                    st = await status()
                    assert light_cmds() == expected and st["light"]["reported"] == "off", (light_cmds(), st)
                    assert st["last_action"]["reason"] == "quiet hours" and st["last_action"]["result"] == "sent", st
                    events.append({"contract":"quiet hours switch the light off at the Auckland boundary (NZDT, "
                                   "not NZST), never act on an unreported light, then send nothing", "sent":1})
                    # Switched on during the night (firmware, a person, another tool): off again, once.
                    broker.light = "on"
                    broker.broadcast()
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off", "night ON kept")
                    await asyncio.sleep(1)
                    assert light_cmds() == expected, light_cmds()
                    # A printer that ignores the command, then goes stale: nothing is sent while stale,
                    # and fresh ON reports get repeats only on the doubling back-off.
                    broker.ignore_light = True
                    broker.light = "on"
                    broker.broadcast()
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected, "no OFF for an ignored command")
                    pacing["on"], broker.reports = False, False
                    await health_until(lambda h: not h["live"], "telemetry stayed live without reports")
                    await asyncio.sleep(2.5)
                    st = await status()
                    assert light_cmds() == expected and not st["telemetry"]["writable"], (light_cmds(), st)
                    pacing["on"], broker.reports = True, True
                    broker.broadcast()
                    await until_live()
                    await asyncio.sleep(3)
                    repeats = len(light_cmds()) - len(expected)
                    # ~12 fresh ON reports arrived in that window; the back-off (0.5, 1, 2 s) allows 1-4 OFFs.
                    assert 1 <= repeats <= 4 and set(light_cmds()[len(expected):]) == {"off"}, light_cmds()
                    broker.ignore_light = False
                    await eventually(lambda: broker.light == "off", "OFF not repeated after the back-off", 6)
                    await asyncio.sleep(.5)
                    expected = light_cmds()
                    await asyncio.sleep(1)
                    assert light_cmds() == expected, light_cmds()
                    events.append({"contract":"night ON switched off again; stale telemetry sends nothing; an "
                                   "ignored OFF repeats only on back-off", "repeats":repeats})
                    # The server dies; the light is switched on while it is down. The stored policy is
                    # restored at start and the light goes off after the reconnect.
                    broker.light = "on"
                    proc = await replace_server(proc, env)
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off",
                                     "no catch-up OFF after restart")
                    st = await status()
                    assert st["policy"] == {"enabled": True, "timezone": "Pacific/Auckland",
                                            "start": "22:00", "end": "07:00"}, st
                    events.append({"contract":"policy survives a SIGKILL; light switched on while the server was "
                                   "down goes off after reconnect", "sent":1})
                    # Morning: nothing is switched on at the end of quiet hours.
                    clock("2026-11-05T18:00:00+00:00")  # 07:00 NZDT
                    await asyncio.sleep(1)
                    assert light_cmds() == expected and broker.light == "off" and not (await status())["quiet_now"]
                    # Inspection lease. Refused without permission, out of range and in quiet hours.
                    clock("2026-11-05T09:30:00+00:00")
                    refused = await call("light_inspection_lease", seconds=60, user_permission=True)
                    assert refused.get("error") == "quiet_hours" and refused["sent"] is False, refused
                    clock("2026-11-05T20:00:00+00:00")  # 09:00 NZDT
                    refused = await call("light_inspection_lease", seconds=60)
                    assert "user_permission" in str(refused.get("error")), refused
                    refused = await call("light_inspection_lease", seconds=901, user_permission=True)
                    assert "nothing sent" in str(refused.get("error")), refused
                    assert light_cmds() == expected, light_cmds()
                    # The caller lights the chamber and never ends the lease (its capture failed).
                    lease = await call("light_inspection_lease", seconds=30, user_permission=True)
                    assert lease["sent"] is True and lease["seconds"] == 30, lease
                    expected = expected + ["on"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "on", "lease ON")
                    await asyncio.sleep(1)
                    assert light_cmds() == expected, "light switched off before the deadline"
                    clock("2026-11-05T20:00:31+00:00")
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off", "lease not ended")
                    await lease_closed()
                    # Again, and the server is SIGKILLed during the lease; it restarts past the deadline.
                    lease = await call("light_inspection_lease", seconds=120, user_permission=True)
                    expected = expected + ["on"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "on", "lease ON")
                    old = proc
                    old.kill(); old.wait(timeout=5)
                    clock("2026-11-05T20:03:00+00:00")
                    proc = await replace_server(old, env)
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off",
                                     "lease deadline not enforced after restart")
                    await lease_closed()
                    # A light already on by hand gets no lease and is not switched off later.
                    broker.light = "on"
                    broker.broadcast()
                    await reported("on")
                    kept = await call("light_inspection_lease", seconds=60, user_permission=True)
                    assert kept["lease"] is None and kept["sent"] is False, kept
                    clock("2026-11-05T20:30:00+00:00")
                    await asyncio.sleep(1)
                    assert light_cmds() == expected and broker.light == "on", light_cmds()
                    # A lease granted just before quiet hours ends at their start; quiet hours win.
                    broker.light = "off"
                    broker.broadcast()
                    clock("2026-11-05T08:59:00+00:00")  # 21:59 NZDT
                    await reported("off")
                    lease = await call("light_inspection_lease", seconds=900, user_permission=True)
                    assert lease["seconds"] == 60 and lease["lease_until"].startswith("2026-11-05T22:00:00+13:00"), lease
                    expected = expected + ["on"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "on", "lease ON")
                    clock("2026-11-05T09:00:30+00:00")
                    expected = expected + ["off"]
                    await eventually(lambda: light_cmds() == expected and broker.light == "off", "lease outlived night")
                    await lease_closed()
                    events.append({"contract":"inspection lease returns the light off at its deadline, after a "
                                   "failed client or a restart; manual ON kept; quiet hours win", "sent":6})
                    # Disabled: quiet hours no longer act.
                    st = await call("set_night_light_policy", enabled=False, user_permission=True)
                    assert st["policy"]["enabled"] is False and st["policy"]["timezone"] == "Pacific/Auckland", st
                    broker.light = "on"
                    broker.broadcast()
                    await asyncio.sleep(1)
                    assert light_cmds() == expected and broker.light == "on", light_cmds()
                    assert (await status())["lease"] is None
                    pacing["done"] = True
                    await pacer
                    (tmp/"clock").unlink()
                    events.append({"contract":"disabled quiet hours send nothing", "sent":0})
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
                    # SD card space (docs/storage-space.md). 2026-11-03: an admitted 1.4 MB job was uploaded,
                    # verified and started on a card with 38 free clusters; the P1S could not unpack its 6.9 MB
                    # plate G-code (PREPARE -> IDLE, HMS 0500-4003). The fixture card is attested at 1 GiB; a
                    # listed-only filler stands for the dashcam videos. big_gcode is a ~0.1 MB job whose G-code
                    # unpacks to 24 MiB, so the job alone fits where its unpacked G-code does not.
                    MiB = 1 << 20
                    def card_available(mib):
                        ftps.sizes["MOVE-dashcam.mp4"] = (1024 - mib) * MiB
                        ftps.files.setdefault("MOVE-dashcam.mp4", b"")
                    async def space_check():
                        return (await call("get_printer_connection_status"))["telemetry"]["storage"]["last_space_check"]
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    big = await prepare("big_gcode")
                    with zipfile.ZipFile(big["job_path"]) as z:
                        unpacked = z.getinfo("Metadata/plate_1.gcode").file_size  # what the printer writes to /cache
                    assert big.get("success") and unpacked > 24 * MiB > 10 * big["size"], big
                    card_available(76)
                    refused = await upload(big["job_path"], "/big.gcode.3mf")
                    assert refused.get("error") == "storage_insufficient", refused
                    space = refused["storage"]
                    assert space["available_bytes"] < space["required_bytes"], space
                    assert space["available_bytes"] >= space["required"]["job"] + space["required"]["reserve"], space
                    assert space["required"]["unpacked_gcode"] >= unpacked == big["plate_gcode_bytes"]["1"], space
                    assert (await space_check())["verdict"] == "storage_insufficient"
                    assert len(ftps.stores) == stores and "big.gcode.3mf" not in ftps.files, ftps.stores[stores:]
                    assert (await start("/big.gcode.3mf")).get("error") == "print_not_admitted"
                    # Room for the upload and its G-code; then the card fills (another writer) before the start.
                    card_available(100)
                    up = await upload(big["job_path"], "/big.gcode.3mf")
                    assert up.get("admission") and up["storage"]["available_bytes"] >= up["storage"]["required_bytes"], up
                    assert (await space_check())["verdict"] == "ok"
                    card_available(80)
                    refused = await start("/big.gcode.3mf")
                    assert refused.get("error") == "storage_insufficient" and "admission is kept" in refused["detail"], refused
                    assert refused["storage"]["required"]["unpacked_gcode"] >= unpacked, refused
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    card_available(200)
                    printed = await start("/big.gcode.3mf")
                    assert "error" not in printed and printed["storage"]["card_id"] == "0" * 32, printed
                    await asyncio.sleep(.1)
                    commands = broker.mutations()[before:]
                    assert len(commands) == 1 and commands[0]["print"]["command"] == "project_file", commands
                    assert commands[0]["print"]["md5"] == hashlib.md5(Path(big["job_path"]).read_bytes()).hexdigest().upper()
                    events.append({"contract":"a job whose unpacked G-code does not fit is not uploaded; space is "
                                   "measured again before the start (admission kept); a start with room reaches "
                                   "the wire once", "sent":1})
                    # Unknown is never treated as room: no card record, a card the printer reports read-only,
                    # an SD state not reported on this connection, and another card at the start.
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    record = ftps.files.pop(CARD_RECORD)
                    refused = await upload(prep["job_path"], "/space.gcode.3mf")
                    assert refused.get("error") == "storage_unattested", refused
                    ftps.files[CARD_RECORD] = record
                    broker.sd = "read_only"
                    await health_until(lambda h: h["storage"]["sd_card"]["state"] == "read_only", "read-only card", 5)
                    refused = await upload(prep["job_path"], "/space.gcode.3mf")
                    assert refused.get("error") == "storage_card_read_only", refused
                    assert len(ftps.stores) == stores, ftps.stores[stores:]
                    broker.sd = "normal"
                    await health_until(lambda h: h["storage"]["sd_card"]["state"] == "normal", "SD state not reported", 5)
                    assert (await upload(prep["job_path"], "/swap.gcode.3mf")).get("admission")
                    ftps.files[CARD_RECORD] = card_record(card_id="1" * 32)  # another card holding the same file
                    refused = await start("/swap.gcode.3mf")
                    assert refused.get("error") == "storage_card_changed", refused
                    assert (await start("/swap.gcode.3mf")).get("error") == "print_not_admitted"
                    ftps.files[CARD_RECORD] = record
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"contract":"unattested card, read-only card and a replaced card refuse before "
                                   "any write or start", "sent":0})
                    # The listing must be complete: bpm reads a folder the printer refuses (550) as empty and
                    # skips rows it does not parse, and either can hide any amount of used space. A folder's
                    # files count (here dated "Nov  3", a form bpm skips); an empty folder is fine; a refused
                    # folder, a link and an unparsed row refuse the upload and the start before STOR or project_file.
                    ftps.root_rows = ["drwxr-xr-x 1 test test 0 Jan 01 2026 timelapse"]
                    ftps.folders["timelapse"] = []
                    up = await upload(prep["job_path"], "/opaque.gcode.3mf")
                    assert up.get("admission") and up["storage"]["folders"] == 2, up
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    ftps.folders["timelapse"] = [f"-rw-r--r-- 1 test test {150 * MiB} Nov  3 18:19 video.mp4"]
                    refused = await upload(prep["job_path"], "/opaque.gcode.3mf")
                    assert refused.get("error") == "storage_insufficient", refused
                    assert refused["storage"]["available_bytes"] <= up["storage"]["available_bytes"] - 150 * MiB, refused
                    assert refused["storage"]["available_bytes"] < refused["storage"]["required_bytes"], refused
                    for case, rows in (("refused folder", None), ("link", []), ("unparsed row", [])):
                        ftps.folders["timelapse"] = rows
                        ftps.root_rows = ["drwxr-xr-x 1 test test 0 Jan 01 2026 timelapse",
                                          *{"link": ["lrwxrwxrwx 1 test test 9 Jan 01 2026 video -> /x"],
                                            "unparsed row": ["-rw-r--r-- 1 test test Jan 01 2026 video.mp4"]}.get(case, [])]
                        refused = await upload(prep["job_path"], "/opaque.gcode.3mf")
                        assert refused.get("error") == "storage_unknown" and "incomplete" in refused["detail"], (case, refused)
                        refused = await start("/opaque.gcode.3mf")
                        assert refused.get("error") == "storage_unknown" and "admission is kept" in refused["detail"], (case, refused)
                    assert refused["folder"] == "/", refused
                    ftps.root_rows, ftps.folders["timelapse"] = ftps.root_rows[:1], None
                    refused = await upload(prep["job_path"], "/opaque.gcode.3mf")
                    assert refused.get("folder") == "/timelapse" and "550" in refused["detail"], refused
                    assert len(ftps.stores) == stores and len(broker.mutations()) == before, ftps.stores[stores:]
                    ftps.root_rows, ftps.folders = [], {}
                    assert (await upload(prep["job_path"], "/opaque.gcode.3mf")).get("admission")
                    events.append({"contract":"a folder's files count; a folder the printer refuses, a link or an "
                                   "unparsed listing row refuses the upload and the start", "sent":0})
                    # The card record states the card's size, so a caller cannot write one: not by upload
                    # (any spelling, or FAT's 8.3 alias) and not by renaming another file onto it.
                    forged = tmp / "card.json"
                    forged.write_bytes(card_record(card_id="2" * 32, data_bytes=1 << 40))
                    stores, renames = len(ftps.stores), sum(1 for verb, _ in ftps.commands if verb == "RNFR")
                    for remote in ("/bambu-mcp-card.json", "/BAMBU-MCP-CARD.JSON.", "/BAMBU-~1.JSO"):
                        refused = await upload(forged, remote)
                        assert refused.get("error") == "storage_record_protected", (remote, refused)
                    refused = await call("rename_sdcard_file", user_permission=True, src_path="/notes.txt",
                                         dest_path="/Bambu-MCP-Card.json")
                    assert refused.get("error") == "storage_record_protected", refused
                    assert len(ftps.stores) == stores and ftps.files[CARD_RECORD] == card_record(), ftps.stores[stores:]
                    assert sum(1 for verb, _ in ftps.commands if verb == "RNFR") == renames
                    events.append({"contract":"the card record cannot be uploaded or renamed onto by a caller", "sent":0})
                    # A card that stays in the printer (2026-11-04): formatted and checked on a computer (clean
                    # fsck.fat -n -v), put back without a record. ops/register_sd_card.py, a separate process,
                    # takes the saved fsck output and has the server write the record over its own FTPS
                    # session. Real FAT32 images with 32 KiB clusters, as on the P1S card, give the logs.
                    def fat_image(path, mib):
                        subprocess.run(["/usr/sbin/mkfs.fat", "-F", "32", "-s", "64", "-S", "512", "-n", "BAMBU_P1S",
                                        "-C", str(path), str(mib * 1024)], check=True, capture_output=True)
                    def fsck_log(image, name, *flags):
                        run = subprocess.run(["/usr/sbin/fsck.fat", "-n", *flags, str(image)], capture_output=True, text=True)
                        (tmp / name).write_text(run.stdout + run.stderr)
                        return str(tmp / name)
                    card_img, other_img = tmp / "card.img", tmp / "other.img"
                    fat_image(card_img, 4096)
                    fat_image(other_img, 3072)
                    verbose, final = fsck_log(card_img, "fsck-v.log", "-v"), fsck_log(card_img, "fsck.log")
                    mismatched = fsck_log(other_img, "fsck-other.log")
                    with open(card_img, "r+b") as fh:  # a wrong free-cluster summary, as the dashcam card had
                        fh.seek(512 + 488)
                        fh.write(struct.pack("<I", 12345))
                    dirty = fsck_log(card_img, "fsck-dirty.log", "-v")
                    assert "Free cluster summary wrong" in Path(dirty).read_text(), Path(dirty).read_text()
                    data_bytes = int(re.search(r"data clusters \((\d+) bytes\)", Path(verbose).read_text())[1])
                    registry = tmp / "home" / ".bambu-mcp" / "sd-card"
                    custody = "E2E: formatted and checked on a computer, put back into the fixture printer"
                    async def register(*logs):
                        cmd = [sys.executable, str(ROOT / "ops/register_sd_card.py"), "--printer", "fixture",
                               "--mcp", url, "--custody", custody, "--evidence", str(tmp / "notes.txt")]
                        for log_path in logs:
                            cmd += ["--fsck-log", log_path]
                        run = await asyncio.to_thread(subprocess.run, cmd, env=env, cwd=ROOT, capture_output=True,
                                                      text=True, timeout=60)
                        try:
                            return run.returncode, json.loads(run.stdout)
                        except ValueError:
                            raise AssertionError((run.returncode, run.stdout, run.stderr)) from None
                    record, dashcam = ftps.files.pop(CARD_RECORD), ftps.sizes["MOVE-dashcam.mp4"]
                    before, stores = len(broker.mutations()), len(ftps.stores)
                    # Logs without the geometry, of two filesystems, or of an unclean one: the CLI refuses
                    # and writes nothing.
                    for logs, why in (((final,), "no log shows the geometry"),
                                      ((verbose, mismatched), "not of one filesystem"),
                                      ((dirty,), "Free cluster summary wrong")):
                        code, out = await register(*logs)
                        assert code == 1 and out.get("registered") is False and why in out["error"], (why, out)
                    assert not registry.exists(), list(registry.iterdir())
                    # An MCP caller cannot write a registration (download_file refuses the store) or apply
                    # one that the operator did not make; the tool takes no size.
                    forged_id = "3" * 32
                    refused = await call("download_file", remote_path="/notes.txt", user_permission=True,
                                         local_path=str(registry / f"{forged_id}.json"))
                    assert refused.get("error") == "local_path_reserved", refused
                    refused = await call("register_sd_card", registration_id=forged_id, user_permission=True)
                    assert refused.get("error") == "registration_missing", refused
                    # A card listing more than the logs' data area is not that card; an incomplete listing
                    # is unknown. Each registration is used up by its one call.
                    ftps.sizes["MOVE-dashcam.mp4"] = 5 << 30
                    code, out = await register(verbose, final)
                    assert code == 1 and out.get("error") == "registration_inconsistent", out
                    ftps.sizes["MOVE-dashcam.mp4"] = dashcam
                    ftps.root_rows = ["lrwxrwxrwx 1 test test 9 Jan 01 2026 video -> /x"]
                    code, out = await register(verbose, final)
                    assert code == 1 and out.get("error") == "storage_unknown" and "incomplete" in out["detail"], out
                    ftps.root_rows = []
                    assert len(ftps.stores) == stores and CARD_RECORD not in ftps.files, ftps.stores[stores:]
                    assert not list(registry.glob("????????????????????????????????.json")), list(registry.iterdir())
                    code, out = await register(verbose, final)
                    assert code == 0 and out.get("registered") is True, out
                    reg_id, written = out["registration_id"], json.loads(ftps.files[CARD_RECORD])
                    assert ftps.stores[stores:] == [CARD_RECORD], ftps.stores[stores:]
                    assert (written["card_id"], written["data_bytes"], written["cluster_bytes"]) == (reg_id, data_bytes, 32768)
                    assert "not checked since" in written["basis"] and custody in written["basis"], written
                    assert out["storage"]["card_id"] == reg_id and out["storage"]["available_bytes"] == \
                        data_bytes - out["storage"]["listed_allocated_bytes"], out
                    assert json.loads(Path(out["receipt"]).read_text())["answer"]["registered"] is True
                    # Used once; a card that has a record keeps it.
                    refused = await call("register_sd_card", registration_id=reg_id, user_permission=True)
                    assert refused.get("error") == "registration_missing", refused
                    code, out = await register(verbose, final)
                    assert code == 1 and out.get("error") == "storage_already_attested", out
                    assert ftps.stores[stores:] == [CARD_RECORD] and json.loads(ftps.files[CARD_RECORD]) == written
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    events.append({"contract":"a card checked on a computer is registered in the printer from its fsck "
                                   "logs by the operator CLI; unclean, inconsistent and mismatched logs, an incomplete "
                                   "listing, a caller's registration and replays write nothing", "stores":1})
                    # The registered card admits like an attested one: the upload binds it, the start measures
                    # the unpacked G-code and reserve again, and one start reaches the wire.
                    up = await upload(prep["job_path"], "/registered.gcode.3mf")
                    assert up.get("admission") and up["storage"]["card_id"] == reg_id, up
                    assert up["storage"]["data_bytes"] == data_bytes and "operator registration" in up["storage"]["basis"], up
                    ftps.sizes["MOVE-dashcam.mp4"] = data_bytes - 40 * MiB  # another writer filled it
                    refused = await start("/registered.gcode.3mf")
                    assert refused.get("error") == "storage_insufficient" and "admission is kept" in refused["detail"], refused
                    ftps.sizes["MOVE-dashcam.mp4"] = dashcam
                    assert len(broker.mutations()) == before, broker.mutations()[before:]
                    printed = await start("/registered.gcode.3mf")
                    assert "error" not in printed and printed["storage"]["card_id"] == reg_id, printed
                    await asyncio.sleep(.1)
                    commands = broker.mutations()[before:]
                    assert len(commands) == 1 and commands[0]["print"]["command"] == "project_file", commands
                    assert commands[0]["print"]["md5"] == hashlib.md5(job).hexdigest().upper(), commands
                    assert (await start("/registered.gcode.3mf")).get("error") == "print_not_admitted"
                    ftps.files[CARD_RECORD] = record
                    events.append({"contract":"a registered card admits an upload and one start, measured again "
                                   "before the start", "sent":1})
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
                    # Neither the nozzle nor the SD card state reported before a reconnect counts for the new link.
                    broker.nozzle, broker.sd = None, None
                    broker.drop()
                    await until_live()
                    await nozzle_reported(None)
                    refused = await start("/admitted.gcode.3mf")
                    assert refused.get("error") == "nozzle_unknown", refused
                    await health_until(lambda h: h["storage"]["sd_card"]["state"] is None, "pre-reconnect SD state kept", 5)
                    stores = len(ftps.stores)
                    refused = await upload(prep["job_path"], "/space.gcode.3mf")
                    assert refused.get("error") == "storage_state_unknown" and len(ftps.stores) == stores, refused
                    broker.nozzle, broker.sd = "0.4", "normal"
                    await nozzle_reported("0.4")
                    await health_until(lambda h: h["storage"]["sd_card"]["state"] == "normal", "SD state not reported", 5)
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
                    events.append({"contract":"prepared job: wrong plate, bed type, nozzle, unreported nozzle or SD "
                                   "state and remote tamper refused; verified start names the prepared plate and MD5, "
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
                    connections = broker.connections
                    filler = await call("send_gcode", gcode=";HARNESS-ONLY-"+"x"*(8*1024*1024), user_permission=True)
                    assert "publish_unconfirmed" in str(filler), str(filler)[:500]
                    # Other threads publish while that write is still pending in OpenSSL (supervisor timers,
                    # bpm's watchdog, other tools). Written from their own threads, OpenSSL refused the
                    # retry ("bad length") and the link dropped before the start (CI 2026-11-03, seeds
                    # 3092584084, 1748876678). The link must stay up, so the start meets real backpressure.
                    async def keep_refreshing(until):
                        calls = 0
                        while time.monotonic() < until:
                            done = await call("trigger_printer_refresh", user_permission=True)
                            assert "Refresh triggered" in str(done), done
                            calls += 1
                        return calls
                    refreshes = sum(await asyncio.gather(*(keep_refreshing(time.monotonic() + 1.5)
                                                           for _ in range(4))))
                    assert broker.connections == connections and broker.peers, (
                        f"link dropped under backpressure during {refreshes} concurrent refreshes")
                    uncertain = await start("/uncertain.gcode.3mf")
                    assert (uncertain.get("error"), uncertain.get("sent"), uncertain.get("retried")) == (
                        "publish_unconfirmed", "unknown", False), uncertain
                    assert uncertain["tool_result"].get("admission") == "consumed", uncertain
                    connections = broker.connections
                    broker.drop()
                    broker.block_reads = False
                    await replacement_connected(connections)
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
                    ftps_before = ftps.connections
                    for tool, args in [("list_sdcard_files", {}),
                                       ("get_project_info", {"file_path":"fixture-job.gcode.3mf"}),
                                       ("upload_file", {"local_path":str(tmp/"notes.txt"), "user_permission":True,
                                                        "remote_path":"/during-print.txt"})]:
                        refused = await call(tool, **args)
                        assert "SD card access is refused" in str(refused), (tool, refused)
                    code, out = await register(verbose, final)  # no card write while a job runs
                    assert code == 1 and "SD card access is refused" in out.get("detail", ""), out
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
                    # The job monitor's frame capture above runs with its plate-image fetch; neither
                    # it nor the refused tools may open FTPS while the printer reports a job.
                    assert ftps.connections == ftps_before, ("FTPS during a print", ftps.connections - ftps_before)
                    events.append({"contract":"no SD card access while the printer reports a job"})
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
            print("broker wire events:", *broker.wire_events[-30:], sep="\n  ", file=sys.stderr)
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
