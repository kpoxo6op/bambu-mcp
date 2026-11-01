"""Offline failure tests for reliability.py and its session_manager / tool-registry integration.

Real failure being reproduced: on 2026-11-01 22:44:52, bpm's ``_on_message`` listed the SD card
over FTPS on paho's network thread at a job start. The P1S reset the handshake, the exception
killed ``loop_forever``, the socket sat in CLOSE-WAIT, ``recent_update`` stayed True, and every
state tool kept serving the pre-job IDLE frame. ``test_control_plain_bpm_*`` shows these fakes
reproduce that with stock bpm; the rest prove the fixed contracts.

Nothing touches a printer or the network. paho's Client is replaced by FakeClient, which
drives bpm's real callbacks and parser from an in-memory inbox. FTPS is replaced by a client
that raises ConnectionResetError, like the P1S did. HOME is a temporary directory.

Run directly (no pytest needed):  .venv/bin/python3 test_reliability.py
"""

import os
import sys
import tempfile

_TMP_HOME = tempfile.mkdtemp(prefix="bambu-rel-test-")
os.environ["HOME"] = _TMP_HOME
for _k in ("BAMBU_STUDIO_CONFIG", "BAMBU_MCP_WATCHDOG_EXIT"):
    os.environ.pop(_k, None)

import json  # noqa: E402
import logging  # noqa: E402
import queue  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
import types  # noqa: E402
from pathlib import Path  # noqa: E402

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
logging.basicConfig(level=logging.CRITICAL)

import auth  # noqa: E402
import reliability  # noqa: E402
import session_manager as sm_mod  # noqa: E402
from bpm import bambuprinter  # noqa: E402
from bpm.bambuconfig import BambuConfig  # noqa: E402
from bpm.bambutools import ServiceState  # noqa: E402
from tools import _guards  # noqa: E402

for _name, _value in dict(STALE_AFTER_S=0.6, PUSHALL_EVERY_S=0.2, FORCE_RECONNECT_AFTER_S=1.5,
                          STUCK_CALLBACK_S=0.4, STUCK_EXIT_AFTER_S=0.8, QUIT_TIMEOUT_S=0.8,
                          PUBLISH_TIMEOUT_S=0.3, BACKOFF_MIN_S=0.2, BACKOFF_MAX_S=0.8,
                          SUPERVISOR_TICK_S=0.05, SUPERVISOR_DEAD_AFTER_S=2.0).items():
    setattr(reliability, _name, _value)

NAME = "fake-p1s"
CREDS = {"ip": "192.0.2.1", "access_code": "12345678", "serial": "TESTSERIAL0001"}
FULL_IDLE = {"print": {"command": "push_status", "sequence_id": "1", "gcode_state": "IDLE",
                       "nozzle_temper": 25.0, "bed_temper": 24.0, "mc_percent": 0}}
JOB_START = {"print": {"command": "push_status", "sequence_id": "2", "gcode_state": "RUNNING",
                       "gcode_file": "/data/Metadata/plate_1.gcode", "subtask_name": "Soyrack v3",
                       "task_id": "777", "mc_percent": 1, "layer_num": 1, "total_layer_num": 115}}


# ── fakes ────────────────────────────────────────────────────────────────────

class _RC:
    is_failure = False

    def __str__(self):
        return "Success"


class FakeInfo:
    def __init__(self, rc, published):
        self.rc, self._published = rc, published

    def is_published(self):
        return self._published

    def wait_for_publish(self, timeout=None):
        if self.rc:
            raise RuntimeError("no connection")
        if not self._published:
            time.sleep(timeout or 0)


class FakeClient:
    instances: list = []
    connects: list = []  # every connect() attempt; bpm also builds throwaway clients it never connects
    refuse_connect = False
    write_mode = "ok"  # "ok" or "hang" (never confirmed written)

    def __init__(self, *args, **kwargs):
        FakeClient.instances.append(self)
        self.inbox: queue.Queue = queue.Queue()
        self.connected = False
        self.sock_open = False
        self.sock_closed = False
        self.loop_running = False
        self.published: list[tuple[str, str]] = []
        self.on_connect = self.on_disconnect = self.on_message = None
        self.connect_timeout = None

    def tls_set(self, **k): pass
    def tls_insecure_set(self, v): pass
    def reconnect_delay_set(self, **k): self.reconnect_delay = k
    def username_pw_set(self, *a, **k): pass
    def user_data_set(self, *a): pass
    def subscribe(self, *a, **k): pass
    def unsubscribe(self, *a, **k): pass

    def connect(self, host, port):
        FakeClient.connects.append(self)
        if FakeClient.refuse_connect:
            raise ConnectionRefusedError("fake printer refuses")
        self.sock_open = True

    def _do_connect(self):
        self.connected = True
        self.on_connect(self, None, {}, _RC(), None)

    def loop_forever(self, retry_first_connection=False):
        self.loop_running = True
        try:
            self._do_connect()
            while True:
                ev = self.inbox.get()
                if ev[0] == "msg":
                    self.on_message(self, None, types.SimpleNamespace(payload=ev[1].encode()))
                elif ev[0] == "drop":  # connection lost; paho keeps the thread and reconnects later
                    self.connected = False
                    self.on_disconnect(self, None, None, "connection lost", None)
                elif ev[0] == "reconnect":
                    self._do_connect()
                elif ev[0] == "raise":
                    raise ev[1]
                elif ev[0] == "stop":
                    return
        finally:
            self.loop_running = False

    def is_connected(self):
        return self.connected

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        self.published.append((threading.current_thread().name, payload))
        if not self.sock_open:
            return FakeInfo(4, False)
        return FakeInfo(0, FakeClient.write_mode == "ok")

    def disconnect(self):
        self.connected = False
        self.inbox.put(("stop",))

    def _sock_close(self):
        self.sock_open, self.sock_closed = False, True

    def send(self, payload):
        self.inbox.put(("msg", json.dumps(payload) if isinstance(payload, dict) else payload))

    def mutations(self):
        return [p for _, p in self.published if not reliability.is_read_only_request(p)]


FTPS_THREADS: list[str] = []


class ResetFTPS:
    """The P1S at a job start: the FTPS TLS handshake is reset."""

    def __init__(self, *a, **k):
        FTPS_THREADS.append(threading.current_thread().name)
        raise ConnectionResetError(104, "Connection reset by peer")


# ── harness ──────────────────────────────────────────────────────────────────

def wait_until(pred, timeout=3.0, step=0.01):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(step)
    return pred()


def setup():
    bambuprinter.mqtt.Client = FakeClient
    bambuprinter.IoTFTPSClient = ResetFTPS
    FakeClient.instances.clear()
    FakeClient.connects.clear()
    FakeClient.refuse_connect = False
    FakeClient.write_mode = "ok"
    FTPS_THREADS.clear()
    os.environ.pop("BAMBU_MCP_WATCHDOG_EXIT", None)
    reliability.supervisor = reliability._Supervisor()
    reliability._hard_exit = lambda code: EXITS.append(code)
    EXITS.clear()
    auth.get_printer_credentials = lambda name: dict(CREDS)
    auth.get_configured_printer_names = lambda: [NAME]
    sm_mod._ensure_imports()
    return sm_mod.SessionManager()


def teardown(manager):
    reliability.supervisor.stop()
    thread = reliability.supervisor._thread
    assert thread is None or not thread.is_alive(), "supervisor thread outlived stop()"
    for name in list(manager._printers):
        manager.stop_printer(name)
    for c in FakeClient.instances:
        c.inbox.put(("stop",))


EXITS: list[int] = []
_ORIG_CLIENT = bambuprinter.mqtt.Client


def start(manager, supervise=False):
    if supervise:
        reliability.supervisor.start(manager)
        reliability.supervisor.want(NAME)
    else:
        # health() needs a heartbeat to call a session writable; a supervisor without wants is inert.
        reliability.supervisor.start(manager)
    manager._start_printer(NAME)
    p = manager.get_printer(NAME)
    assert wait_until(lambda: p._client is not None and p._client.connected)
    return p, p._client


def live(name=NAME):
    return reliability.health(name)["status"] == "live"


# ── tests ────────────────────────────────────────────────────────────────────

def test_control_plain_bpm_dies_on_ftps_reset_at_job_start():
    """Control: stock bpm with these fakes reproduces the incident, so the tests below are meaningful."""
    setup()
    p = bambuprinter.BambuPrinter(BambuConfig(hostname="192.0.2.1", access_code="12345678",
                                              serial_number="TESTSERIAL0001",
                                              bpm_cache_path=Path(_TMP_HOME) / "bpm"))
    p.start_session()
    client = FakeClient.connects[-1]
    client.send(FULL_IDLE)
    client.send(JOB_START)
    assert wait_until(lambda: not p._mqtt_client_thread.is_alive()), "stock bpm survived (fakes do not reproduce)"
    assert p.service_state == ServiceState.QUIT
    assert p.printer_state.gcode_state == "IDLE", "the RUNNING frame was never applied"
    assert FTPS_THREADS == ["bambuprinter-session"], FTPS_THREADS
    p.quit()


def test_job_start_ftps_reset_keeps_mqtt_alive_and_state_current():
    """Incident fix: FTPS never runs on the MQTT thread; parsing continues; the lookup is deferred."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        client.send(JOB_START)
        assert wait_until(lambda: p.printer_state.gcode_state == "RUNNING"), p.printer_state.gcode_state
        assert p._mqtt_client_thread.is_alive()
        assert live(), reliability.health(NAME)
        assert wait_until(lambda: FTPS_THREADS), "deferred lookup never ran"
        assert "bambuprinter-session" not in FTPS_THREADS, FTPS_THREADS
        assert all(t.startswith("bambu-callbacks-") for t in FTPS_THREADS), FTPS_THREADS
        time.sleep(0.3)
        assert len(FTPS_THREADS) <= reliability.PROJECT_LOOKUP_ATTEMPTS
        client.send({"print": {"command": "push_status", "mc_percent": 2, "nozzle_temper": 240.0}})
        assert wait_until(lambda: p.active_job_info.print_percentage == 2)
    finally:
        teardown(m)


def test_parse_failure_marks_incomplete_blocks_writes_then_recovers():
    """An exception while parsing must not kill the loop; state is not live until re-baselined."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        reads_before = len(client.published)
        client.send("{this is not json")
        assert wait_until(lambda: reliability.health(NAME)["status"] == "incomplete"), reliability.health(NAME)
        assert p._mqtt_client_thread.is_alive()
        assert reliability.mutation_block(NAME) is not None
        try:
            p.pause_printing()
            raise AssertionError("pause was not refused while incomplete")
        except reliability.PublishRejected:
            pass
        reliability.supervisor.want(NAME)  # let the supervisor request a pushall (read-only)
        assert wait_until(lambda: len(client.published) > reads_before)
        assert client.mutations() == [], client.mutations()
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
    finally:
        teardown(m)


def test_startup_state_unknown_until_full_report_and_state_tools_say_so():
    m = setup()
    try:
        p, client = start(m)
        h = reliability.health(NAME)
        assert h["status"] == "starting" and not h["writable"], h

        def get_temperatures(name):
            return {"nozzle": 25.0}

        out = _guards.run_guarded(get_temperatures, {"name": NAME})
        assert out["error"] == "telemetry_not_live" and out["last_known"] == {"nozzle": 25.0}, out
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert _guards.run_guarded(get_temperatures, {"name": NAME}) == {"nozzle": 25.0}
    finally:
        teardown(m)


def test_silence_goes_stale_reads_flagged_writes_refused_camera_still_works():
    m = setup()
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert wait_until(lambda: reliability.health(NAME)["status"] == "stale", timeout=2), reliability.health(NAME)
        called = []

        def get_print_progress(name):
            return {"gcode_state": "IDLE"}

        def get_camera_image(name):
            return "jpeg"

        def pause_print(name, user_permission=False):
            called.append(name)
            return "Pause command sent"

        assert _guards.run_guarded(get_print_progress, {"name": NAME})["error"] == "telemetry_not_live"
        assert _guards.run_guarded(get_camera_image, {"name": NAME}) == "jpeg"
        out = _guards.run_guarded(pause_print, {"name": NAME, "user_permission": True})
        assert out["error"] == "telemetry_not_live" and called == [], out
        # Without permission the tool's own guard still answers (unchanged behaviour).
        assert _guards.run_guarded(pause_print, {"name": NAME, "user_permission": False}) == "Pause command sent"
        assert wait_until(lambda: any(reliability.is_read_only_request(x) for _, x in client.published[2:]))
        assert client.mutations() == []
    finally:
        teardown(m)


def test_writes_refused_on_dead_or_dropped_session_and_never_replayed():
    """The incident socket: loop thread dead or reconnecting → nothing sent, nothing sent later."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        client.inbox.put(("drop",))
        assert wait_until(lambda: not client.connected)
        for attempt in (p.pause_printing, lambda: p.send_gcode("M400")):
            try:
                attempt()
                raise AssertionError("write was not refused while disconnected")
            except reliability.PublishRejected:
                pass
        client.inbox.put(("reconnect",))
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        time.sleep(0.2)
        assert client.mutations() == [], "a refused write was replayed after reconnect"
        p.pause_printing()
        assert len(client.mutations()) == 1
        # Loop thread dies with the socket still open (exactly the CLOSE-WAIT case).
        client.inbox.put(("raise", RuntimeError("boom")))
        assert wait_until(lambda: not p._mqtt_client_thread.is_alive())
        try:
            p.pause_printing()
            raise AssertionError("write into a dead loop's socket was not refused")
        except reliability.PublishRejected as exc:
            assert "loop thread" in str(exc) or "not connected" in str(exc), exc
        assert len(client.mutations()) == 1
    finally:
        teardown(m)


def test_unconfirmed_write_is_bounded_reported_and_not_retried():
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)

        def pause_print(name, user_permission=False):
            m.get_printer(name).pause_printing()
            return "Pause command sent"

        assert _guards.run_guarded(pause_print, {"name": NAME, "user_permission": True}).endswith(
            f"[delivery: {_guards._DELIVERY_NOTE}]")
        FakeClient.write_mode = "hang"
        t0 = time.monotonic()
        out = _guards.run_guarded(pause_print, {"name": NAME, "user_permission": True})
        assert time.monotonic() - t0 < reliability.PUBLISH_TIMEOUT_S + 0.5
        assert out["error"] == "publish_unconfirmed" and out["retried"] is False, out
        time.sleep(0.3)
        assert len(client.mutations()) == 2, client.mutations()  # one sent, one unconfirmed; no retry
    finally:
        teardown(m)


def test_dead_session_restarted_with_bounded_backoff_one_client_at_a_time():
    m = setup()
    peak = [0]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            peak[0] = max(peak[0], sum(1 for c in FakeClient.instances if c.loop_running))
            time.sleep(0.002)

    threading.Thread(target=sample, daemon=True).start()
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        FakeClient.refuse_connect = True
        client.inbox.put(("raise", RuntimeError("loop died")))
        assert wait_until(lambda: client.sock_closed, timeout=2), "dead loop's socket was left open"
        assert wait_until(lambda: reliability.supervisor.info(NAME).get("restarts", 0) >= 3, timeout=4)
        attempts = len(FakeClient.connects)
        time.sleep(1.0)
        assert len(FakeClient.connects) - attempts <= 2, "restarts not backed off"  # ≤ 1 per 0.8s cap
        FakeClient.refuse_connect = False
        assert wait_until(lambda: m.get_printer(NAME) is not None and m.get_printer(NAME)._client is not None
                          and m.get_printer(NAME)._client.connected, timeout=3)
        m.get_printer(NAME)._client.send(FULL_IDLE)
        assert wait_until(live, timeout=3), reliability.health(NAME)
        assert peak[0] == 1, f"{peak[0]} MQTT loops ran at once"
    finally:
        stop.set()
        teardown(m)


def test_connected_but_silent_session_is_dropped_and_restarted_once():
    """A socket that stays 'connected' but delivers nothing (half-open) is replaced, not trusted."""
    m = setup()
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert wait_until(lambda: len(FakeClient.connects) >= 2, timeout=4), "silent session never replaced"
        assert wait_until(lambda: not client.loop_running)  # the old loop ended before/while the new one runs
        assert wait_until(lambda: m.get_printer(NAME) is not None and m.get_printer(NAME)._client is not client
                          and m.get_printer(NAME)._client.connected)
        new = m.get_printer(NAME)._client
        new.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert EXITS == [] and reliability.health(NAME)["restarts"] >= 1
        assert client.mutations() == [] and new.mutations() == []
    finally:
        teardown(m)


def test_stuck_callback_blocks_writes_never_replaced_then_supervised_exit():
    m = setup()
    release = threading.Event()
    orig = bambuprinter.BambuState.fromJson

    def blocking_from_json(msg, printer):
        if "STUCK" in json.dumps(msg):
            release.wait(10)
        return orig(msg, printer)

    bambuprinter.BambuState.fromJson = staticmethod(blocking_from_json)
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        client.send({"print": {"command": "push_status", "STUCK": 1}})
        assert wait_until(lambda: reliability.health(NAME)["status"] == "stuck", timeout=2), reliability.health(NAME)
        assert reliability.mutation_block(NAME) is not None
        time.sleep(0.5)
        assert len(FakeClient.connects) == 1, "a replacement session was started next to a stuck one"
        assert EXITS == [], "exit without permission"
        os.environ["BAMBU_MCP_WATCHDOG_EXIT"] = "1"
        assert wait_until(lambda: EXITS, timeout=2) and EXITS[0] == reliability.EXIT_CODE_STUCK
        # In-process: quit is bounded, the stuck session becomes a zombie, and no restart is allowed.
        t0 = time.monotonic()
        m.stop_printer(NAME)
        assert time.monotonic() - t0 < reliability.QUIT_TIMEOUT_S + 1.0
        assert reliability.health(NAME)["status"] == "stuck"
        try:
            m._start_printer(NAME)
            raise AssertionError("replacement started while old threads survive")
        except RuntimeError:
            pass
        assert len(FakeClient.connects) == 1
        release.set()
        assert wait_until(lambda: reliability.supervisor.zombie_since(NAME) is None, timeout=3)
    finally:
        release.set()
        bambuprinter.BambuState.fromJson = orig
        teardown(m)


def test_intentional_stop_and_pause_are_not_restarted():
    m = setup()
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        m.pause_session(NAME)
        time.sleep(0.4)
        assert reliability.health(NAME)["status"] == "paused" and len(FakeClient.connects) == 1
        m.stop_printer(NAME)
        time.sleep(0.6)
        assert m.get_printer(NAME) is None and len(FakeClient.connects) == 1
        assert reliability.health(NAME)["status"] == "not_connected"
    finally:
        teardown(m)


def test_slow_update_callbacks_do_not_delay_parsing():
    m = setup()
    calls = []
    m.register_update_callback(lambda n: (calls.append(n), time.sleep(0.5)))
    try:
        p, client = start(m)
        t0 = time.monotonic()
        for i in range(20):
            client.send({"print": {"command": "push_status", "gcode_state": "IDLE", "nozzle_temper": 25.0 + i,
                                   "bed_temper": 24.0, "sequence_id": str(i)}})
        assert wait_until(lambda: p._rel_processed >= 20, timeout=2)
        assert time.monotonic() - t0 < 0.5, "parsing waited for on_update callbacks"
        assert p.printer_state.active_nozzle_temp == 44.0
        time.sleep(1.2)
        assert 1 <= len(calls) <= 4, f"{len(calls)} callbacks: not coalesced"
    finally:
        teardown(m)


def test_tools_run_off_the_event_loop():
    """A slow sync tool must not freeze other tool calls on the shared server."""
    import anyio
    from mcp.server.fastmcp import FastMCP

    def slow_tool(name: str) -> str:
        """slow"""
        time.sleep(1.0)
        return "slow"

    def fast_tool(name: str) -> str:
        """fast"""
        return "fast"

    def get_printer_connection_status(name: str) -> dict:
        """diag"""
        return {"live": False}

    mcp = FastMCP("t")
    mcp.add_tool(_guards.guard_tool(slow_tool))
    mcp.add_tool(_guards.guard_tool(fast_tool))
    mcp.add_tool(_guards.guard_tool(get_printer_connection_status))
    done = {}

    async def call(tool):
        await mcp.call_tool(tool, {"name": "x"})
        done[tool] = time.monotonic()

    async def main():

        t0 = time.monotonic()
        async with anyio.create_task_group() as tg:
            for _ in range(_guards.TOOL_THREADS):  # saturate the main pool
                tg.start_soon(call, "slow_tool")
            await anyio.sleep(0.05)
            tg.start_soon(call, "get_printer_connection_status")
        return t0

    t0 = anyio.run(main)
    assert done["get_printer_connection_status"] - t0 < 0.5, "health waited behind a saturated pool"
    assert done["slow_tool"] - t0 >= 1.0

    done.clear()

    async def main2():
        t0 = time.monotonic()
        async with anyio.create_task_group() as tg:
            tg.start_soon(call, "slow_tool")
            await anyio.sleep(0.05)
            tg.start_soon(call, "fast_tool")
        return t0

    t0 = anyio.run(main2)
    assert done["fast_tool"] - t0 < 0.5, "fast tool waited for the slow one"


def test_real_tools_through_registry_fail_closed_then_work():
    """The registered MCP tools (not stubs): unknown state refuses writes and flags reads."""
    import anyio
    from mcp.server.fastmcp import FastMCP
    from tools import _registry

    setup()
    manager = sm_mod.session_manager  # the singleton the real tools use
    mcp = FastMCP("t")
    _registry.register_tools(mcp)

    def call(tool, args):
        async def go():
            return await mcp.call_tool(tool, args)
        out = anyio.run(go)
        blocks = out[0] if isinstance(out, tuple) else out
        text = "".join(b.text for b in blocks if b.type == "text")
        try:
            return json.loads(text)
        except ValueError:
            return text

    try:
        p, client = start(manager)
        temps = call("get_temperatures", {"name": NAME})
        assert temps["error"] == "telemetry_not_live" and "last_known" in temps, temps
        refused = call("pause_print", {"name": NAME, "user_permission": True})  # declared -> str
        assert refused.startswith("Error (telemetry_not_live)") and "No command was sent" in refused, refused
        assert client.mutations() == []
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        temps = call("get_temperatures", {"name": NAME})
        assert "error" not in temps, temps
        status = call("get_printer_connection_status", {"name": NAME})
        assert status["live"] is True and status["telemetry"]["status"] == "live", status
        sent = call("pause_print", {"name": NAME, "user_permission": True})
        assert "acceptance is not confirmed" in sent, sent
        assert len(client.mutations()) == 1
    finally:
        teardown(manager)


def test_swallowed_publish_failure_is_reported_and_partial_is_never_sent_false():
    """Most tools catch every exception and return an error string; the delivery outcome must
    still reach the caller machine-readably, and a half-sent sequence must not read as unsent."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)

        def send_gcode(name, gcode="", user_permission=False, drop_after_first=False):  # like tools/print_control
            try:
                for i, line in enumerate(gcode.split("\n")):
                    if i and drop_after_first:
                        client.sock_open = False  # the socket is lost between two writes
                    m.get_printer(name).send_gcode(line)
                return "G-code sent"
            except Exception as e:
                return f"Error sending G-code to '{name}': {e}"

        FakeClient.write_mode = "hang"
        out = _guards.run_guarded(send_gcode, {"name": NAME, "gcode": "M400", "user_permission": True}, as_text=True)
        assert out.startswith("Error (publish_unconfirmed)"), out
        assert '"sent": "unknown"' in out and '"retried": false' in out and "Error sending G-code" in out, out
        FakeClient.write_mode = "ok"
        out = _guards.run_guarded(send_gcode, {"name": NAME, "gcode": "M17\nM18", "user_permission": True,
                                               "drop_after_first": True})
        assert out["error"] == "publish_partial" and out["sent"] == "partial" and out["retried"] is False, out
        assert out["publishes"] == {"sent": 1, "unconfirmed": 0, "rejected": 1}, out
        client.sock_open = True
        out = _guards.run_guarded(send_gcode, {"name": NAME, "gcode": "M17", "user_permission": True})
        assert out == f"G-code sent [delivery: {_guards._DELIVERY_NOTE}]", out
        # unconfirmed, M17, M18 (refused by paho rc, recorded by the fake, never written), M17: no retry
        assert len(client.mutations()) == 4, client.mutations()
    finally:
        teardown(m)


def test_dead_supervisor_refuses_direct_writes_but_not_refresh():
    """REST routes and internal code call bpm directly; the last mile must apply the same
    supervisor requirement the tool guard does, while read-only refreshes still go out."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        reliability.supervisor.stop()
        assert wait_until(lambda: not reliability.supervisor.alive())
        try:
            p.pause_printing()
            raise AssertionError("write sent with no supervisor")
        except reliability.PublishRejected as exc:
            assert "supervisor" in str(exc), exc
        before = len(client.published)
        p.refresh()
        assert len(client.published) == before + 2 and client.mutations() == [], client.published
    finally:
        teardown(m)


def test_start_failure_before_registration_is_retried_without_racing_the_first_start():
    m = setup()
    calls, overlap = [], []

    def creds(name):
        if calls and calls[-1] == "running":
            overlap.append(name)  # a second start while the first was still in flight
        calls.append("running")
        try:
            if len(calls) == 1:
                time.sleep(0.3)  # several supervisor ticks while the first start is building
                raise OSError("config temporarily unreadable")
            return dict(CREDS)
        finally:
            calls[-1] = "done"

    auth.get_printer_credentials = creds
    try:
        reliability.supervisor.start(m)
        reliability.supervisor.want(NAME)
        try:
            m._start_printer(NAME)
            raise AssertionError("first start should fail")
        except OSError:
            pass
        assert overlap == [], "supervisor started a second session while the first start was in flight"
        assert wait_until(lambda: m.get_printer(NAME) is not None and m.get_printer(NAME)._client is not None
                          and m.get_printer(NAME)._client.connected, timeout=3), reliability.health(NAME)
        m.get_printer(NAME)._client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert reliability.health(NAME)["restarts"] == 1, reliability.health(NAME)
    finally:
        teardown(m)


def test_start_queued_behind_an_abandoned_session_does_not_start_beside_it():
    m = setup()
    try:
        p, client = start(m)
        m._quit_printer = lambda name, printer: time.sleep(0.2) or False  # old threads will not stop
        results = []

        def restart():
            try:
                m._start_printer(NAME)
                results.append("started")
            except RuntimeError as exc:
                results.append(str(exc))

        first = threading.Thread(target=restart)
        first.start()
        time.sleep(0.05)  # the second start queues on the name lock before the zombie is recorded
        second = threading.Thread(target=restart)
        second.start()
        first.join(3)
        second.join(3)
        assert len(results) == 2 and "started" not in results, results
        assert sum(1 for c in FakeClient.instances if c.loop_running) == 1
    finally:
        teardown(m)


def test_flood_of_unusable_frames_still_forces_reconnect():
    """Bytes arriving is not telemetry: malformed frames and command replies must not hold off
    the forced reconnect of a session that never delivers usable state."""
    m = setup()
    stop = threading.Event()
    try:
        p, client = start(m, supervise=True)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)

        def flood():
            while not stop.is_set():
                client.send("{broken")
                client.send({"print": {"command": "gcode_line", "result": "success"}})
                time.sleep(0.02)

        threading.Thread(target=flood, daemon=True).start()
        assert wait_until(lambda: len(FakeClient.connects) >= 2, timeout=reliability.FORCE_RECONNECT_AFTER_S + 3), \
            reliability.health(NAME)
        stop.set()
        assert wait_until(lambda: m.get_printer(NAME)._client is not client and m.get_printer(NAME)._client.connected)
        m.get_printer(NAME)._client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        assert client.mutations() == [] and m.get_printer(NAME)._client.mutations() == []
    finally:
        stop.set()
        teardown(m)


def test_transport_error_after_publish_attempt_is_unknown_not_unsent():
    """paho can return CONN_LOST after a partial socket write. Existing timeout tests
    own delayed completion; this owns immediate transport failure after submission."""
    m = setup()
    try:
        p, client = start(m)
        client.send(FULL_IDLE)
        assert wait_until(live), reliability.health(NAME)
        attempts = []
        def pause_print(name, user_permission=False):
            try:
                p.pause_printing()
                return "sent"
            except Exception as exc:
                return str(exc)  # actual tools catch broadly too
        def connection_lost(*args, **kwargs):
            attempts.append(1)
            return FakeInfo(7,False)
        def transport_raised(*args, **kwargs):
            attempts.append(1)
            raise OSError("socket failed after partial write")
        for transport in (connection_lost,transport_raised):
            client.publish = transport
            out = _guards.run_guarded(pause_print,{"name":NAME,"user_permission":True})
            assert out["error"] == "publish_unconfirmed" and out["sent"] == "unknown", out
            assert out["retried"] is False
        assert len(attempts)==2
    finally:
        teardown(m)


def test_every_write_tool_is_classified():
    from tools import _registry
    _guards.check_tool_classification(_registry.ANNOTATIONS)
    for tool in ("pause_print", "stop_print", "send_gcode", "print_file", "load_filament", "unload_filament",
                 "set_nozzle_temp", "send_mqtt_command", "upload_file"):
        assert tool in _guards.PRINTER_WRITE_TOOLS, tool
    for tool in ("force_state_refresh", "trigger_printer_refresh", "disconnect_printer", "start_printer"):
        assert tool in _guards.NON_PRINTER_WRITE_TOOLS, tool


if __name__ == "__main__":
    import traceback

    failed = 0
    for tname, fn in list(globals().items()):
        if tname.startswith("test_") and callable(fn):
            t0 = time.monotonic()
            try:
                fn()
                print(f"ok   {tname} ({time.monotonic() - t0:.1f}s)")
            except Exception:
                failed += 1
                print(f"FAIL {tname}")
                traceback.print_exc()
    bambuprinter.mqtt.Client = _ORIG_CLIENT
    print(f"{failed} failed")
    sys.stdout.flush()
    os._exit(1 if failed else 0)  # fake loop threads are non-daemon by bpm's design
