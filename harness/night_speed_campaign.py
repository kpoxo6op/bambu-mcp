"""ops/night_speed.py as its own process against real FastMCP HTTP, paho and a TLS printer peer.

Every speed command counted here is a received MQTT packet. Run only inside run.py's namespace.
"""
import asyncio
import configparser
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from printer_peers import FTPSPeer, CameraPeer
from wire_campaign import Broker, payload

ROOT = Path(__file__).resolve().parents[1]
JOB = "soyrack-fixture-20261106"
PORT = 25099
URL = f"http://127.0.0.1:{PORT}/mcp"


class Printer(Broker):
    """Reports one job's state, HMS and spd_lvl as a P1S does, and acts on print_speed."""
    def __init__(self, tmp):
        self.gcode_state, self.job, self.job_layer, self.print_error, self.hms = "RUNNING", JOB, 30, 0, []
        self.speed = 2
        super().__init__(tmp)

    def report(self, peer, data=None):
        if data is None:
            data = {"print": {"command": "push_status", "gcode_state": self.gcode_state, "nozzle_temper": 240.,
                              "bed_temper": 75., "mc_percent": self.job_layer // 5, "gcode_file": "Metadata/plate_1.gcode",
                              "subtask_name": self.job, "task_id": "77", "layer_num": self.job_layer,
                              "total_layer_num": 480, "print_error": self.print_error, "hms": self.hms,
                              "spd_lvl": self.speed, "nozzle_diameter": "0.4", "sdcard": True, "home_flag": 1 << 8}}
        super().report(peer, data)

    def handle(self, peer, typ, body):
        before = len(self.received)
        ended = super().handle(peer, typ, body)
        if len(self.received) > before and self.received[-1].get("print", {}).get("command") == "print_speed":
            self.speed = int(self.received[-1]["print"]["param"])
        return ended


def local(when):
    return dt.datetime.fromisoformat(when).timestamp()


def listening():
    with open("/proc/net/tcp") as f:
        next(f)
        return {int(row.split()[1].rsplit(":", 1)[1], 16) for row in f if row.split()[3] == "0A"}


async def campaign():
    events, expected = [], []
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        broker = Printer(tmp)
        broker.reports = True
        ftps = FTPSPeer(broker.ctx)
        camera = CameraPeer(broker.ctx)
        studio = tmp/"BambuStudio.conf"
        studio.write_text(json.dumps({"user_access_code":{"01P00TESTONLY":"TESTONLY"}}))
        env = os.environ.copy()
        env.update(HOME=str(tmp/"home"), BAMBU_MCP_PORT=str(PORT), BAMBU_MCP_DISABLE_REST="1",
                   BAMBU_MCP_ALLOW_CLIENT_CONFIG_WRITE="0", BAMBU_STUDIO_CONFIG=str(studio),
                   BAMBU_PRINTER_NAME="fixture", BAMBU_PRINTER_SERIAL="01P00TESTONLY",
                   BAMBU_PRINTER_HOST="127.0.0.1", BAMBU_MCP_FTPS_TIMEOUT="2")
        Path(env["HOME"]).mkdir()
        log = (tmp/"server.log").open("w+")
        proc = subprocess.Popen([sys.executable, str(ROOT/"harness/fixture_server.py")],
                                env=env, cwd=ROOT, stdout=log, stderr=log)
        clock_file = tmp/"clock"
        helper_env = {**os.environ, "HOME": env["HOME"], "BAMBU_HARNESS_CLOCK_FILE": str(clock_file)}
        pacing = {"on": True}
        async def pace():  # a connected printer keeps reporting
            while True:
                if pacing["on"]:
                    broker.broadcast()
                await asyncio.sleep(.25)
        pacer = asyncio.create_task(pace())
        try:
            for _ in range(100):
                assert proc.poll() is None, f"fixture exited {proc.returncode}"
                try:
                    socket.create_connection(("127.0.0.1", PORT), .1).close()
                    break
                except OSError:
                    await asyncio.sleep(.1)

            serial = [0]
            def policy(**over):
                serial[0] += 1
                path = tmp/f"policy-{serial[0]}.json"
                body = {"schema_version": 1, "enabled": True, "printer": "fixture", "mcp_url": URL,
                        "timezone": "Pacific/Auckland", "start": "22:30", "end": "08:30", "quiet_mode": "quiet",
                        "day_mode": "standard", "expected_job": JOB, "gate_settle_s": 0,
                        "reviewed": {"at": "2026-11-06T04:00:00Z", "by": "harness"},
                        "authorization": {"at": "2026-11-06T03:47:41Z", "source": "harness",
                                          "verbatim": "Quiet 22:30-08:30, then Standard"}, **over}
                path.write_text(json.dumps({k: v for k, v in body.items() if v is not None}))
                path.chmod(0o600)
                return path
            good = policy()
            state = tmp/"state"
            async def helper(command, config=good, state_dir=state, mode=None, at=None, fault=None, real=False):
                """The actual helper as a process; with real=True the plain file on the real clock."""
                entry = ["-I", str(ROOT/"ops/night_speed.py")] if real else [str(ROOT/"harness/fixture_night_speed.py")]
                cmd = [sys.executable, *entry, command, "--config", str(config)]
                if command != "check":
                    cmd += ["--state-dir", str(state_dir)]
                if mode:
                    cmd += ["--mode", mode]
                if at and command == "dry-run":
                    cmd += ["--at", at]
                else:
                    if at:
                        clock_file.write_text(str(local(at)))
                penv = {**helper_env, "BAMBU_NIGHT_SPEED_FAULT": fault or ""}
                p = await asyncio.create_subprocess_exec(*cmd, env=penv, cwd=tmp, stdout=asyncio.subprocess.PIPE,
                                                         stderr=asyncio.subprocess.PIPE)
                out, err = await asyncio.wait_for(p.communicate(), 60)
                try:
                    answer = json.loads(out)
                except ValueError:
                    answer = {"unparsed": out.decode()[-500:], "stderr": err.decode()[-2000:]}
                return p.returncode, answer
            def speeds():
                return [m["print"]["param"] for m in broker.mutations() if m.get("print", {}).get("command") == "print_speed"]
            def on_wire(*also):
                expected.extend(also)
                assert speeds() == expected, (speeds(), expected)
                others = [m for m in broker.mutations() if m != {"print": {"sequence_id": "0", "command": "print_speed",
                                                                           "param": m.get("print", {}).get("param")}}]
                assert not others, others
            async def wire_settled(*also):
                expected.extend(also)
                start = time.monotonic()
                while speeds() != expected and time.monotonic() - start < 3:
                    await asyncio.sleep(.05)
                await asyncio.sleep(.2)
                on_wire()
            async def boundary(mode, at, want, reason=None, code=None, **kw):
                got, out = await helper("boundary", mode=mode, at=at, **kw)
                assert out.get("result") == want and (reason is None or out.get("reason") == reason), (mode, at, got, out)
                if code is not None:
                    assert got == code, (got, out)
                return out

            async with streamablehttp_client(URL) as (rd, wr, _):
                async with ClientSession(rd, wr) as c:
                    await c.initialize()
                    async def call(tool, **args):
                        return payload(await asyncio.wait_for(c.call_tool(tool, {"name":"fixture", **args}), 10))
                    async def reported(gcode_state, job=JOB, timeout=8.):
                        start = time.monotonic()
                        while True:
                            p = await call("get_print_progress")
                            if p.get("gcode_state") == gcode_state and p.get("subtask_name") == job:
                                return
                            assert time.monotonic() - start < timeout, (gcode_state, job, p)
                            await asyncio.sleep(.05)
                    async def live(want=True, timeout=12.):
                        start = time.monotonic()
                        while True:
                            h = (await call("get_printer_connection_status"))["telemetry"]
                            if bool(h.get("live")) == want:
                                return
                            assert time.monotonic() - start < timeout, h
                            await asyncio.sleep(.05)
                    await live()
                    await reported("RUNNING")
                    connections, ftps_before = broker.connections, ftps.connections

                    # A policy that is not valid, reviewed and enabled refuses before any connection.
                    bads = {"unknown_key": {"enable": True}, "sport": {"quiet_mode": "sport"}, "swapped": {"day_mode": "quiet"},
                            "remote": {"mcp_url": "http://192.0.2.1:25099/mcp"}, "https": {"mcp_url": "https://127.0.0.1:25099/mcp"},
                            "zone": {"timezone": "Mars/Olympus_Mons"}, "hours": {"start": "7:00"}, "hour24": {"end": "24:00"},
                            "equal": {"end": "22:30"}, "not_overnight": {"start": "08:30", "end": "22:30"},
                            "no_job": {"expected_job": ""}, "unreviewed": {"reviewed": None}, "unauthorized": {"authorization": None},
                            "settle": {"gate_settle_s": 100000}, "version": {"schema_version": 2}}
                    for label, over in bads.items():
                        got, out = await helper("check", config=policy(**over))
                        assert got == 1 and out["reason"] == "policy_invalid", (label, out)
                        await boundary("quiet", "2026-11-06T22:30:00+13:00", "refused", "policy_invalid",
                                       code=1, config=policy(**over))
                    shared = policy(); shared.chmod(0o664)
                    await boundary("quiet", "2026-11-06T22:30:00+13:00", "refused", "policy_invalid", config=shared)
                    link = tmp/"policy-link.json"; link.symlink_to(good)
                    await boundary("quiet", "2026-11-06T22:30:00+13:00", "refused", "policy_invalid", config=link)
                    disabled = policy(enabled=False, reviewed=None)
                    assert (await helper("check", config=disabled))[0] == 0
                    await boundary("quiet", "2026-11-06T22:30:00+13:00", "refused", "disabled", config=disabled)
                    assert not state.exists(), "a refused policy created state"
                    on_wire()
                    events.append({"contract": "invalid, unreviewed, shared, linked or disabled policy refuses", "cases": len(bads)+3, "sent": 0})

                    # The dry run reads the same gates and writes nothing anywhere, also for a simulated boundary.
                    got, out = await helper("dry-run", at="2026-11-06T22:30:00+13:00", mode="quiet")
                    assert got == 0 and out["result"] == "would_send" and out["window"] == "quiet-2026-11-06", out
                    assert out["gates"]["job"]["job"] == JOB and out["sent"] == 0, out
                    got, out = await helper("dry-run", config=disabled, at="2026-11-06T22:30:00+13:00", mode="quiet")
                    assert got == 1 and out["reason"] == "disabled" and out["gates"]["job"]["gcode_state"] == "RUNNING", out
                    got, out = await helper("dry-run", at="2026-11-07T08:30:00+13:00", mode="standard")
                    assert got == 0 and out["result"] == "not_needed", out
                    got, out = await helper("dry-run", at="2026-11-06T16:50:00+13:00")
                    assert got == 0 and out["mode"] == "standard" and out["result"] == "not_needed", out
                    assert not state.exists(), "a dry run created state"
                    await wire_settled()
                    events.append({"contract": "dry runs read the gates, create no record and send nothing", "sent": 0})

                    # Time gates, at the second, in the zone's real offset on both sides of its DST changes.
                    for mode, at in (("quiet", "2026-11-06T22:29:59+13:00"), ("standard", "2026-11-06T22:30:00+13:00"),
                                     ("quiet", "2026-11-07T08:30:00+13:00"), ("standard", "2026-11-07T08:29:59+13:00"),
                                     ("quiet", "2026-06-15T09:30:00+00:00"),   # 21:30 NZST; 22:30 at a fixed +13
                                     ("standard", "2026-09-26T19:29:59+00:00"),  # 08:29:59 NZDT, the morning clocks went forward
                                     ("standard", "2027-04-03T19:30:00+00:00")):  # 07:30 NZST, the morning clocks went back
                        await boundary(mode, at, "refused", "wrong_window", code=1)
                    for mode, at in (("quiet", "2026-06-15T10:30:00+00:00"), ("quiet", "2026-09-26T19:29:59+00:00"),
                                     ("quiet", "2027-04-03T19:30:00+00:00")):
                        got, out = await helper("dry-run", at=at, mode=mode)
                        assert out["result"] == "would_send", (at, out)
                    await boundary("standard", "2027-04-03T20:30:00+00:00", "not_needed", code=0)
                    await wire_settled()
                    events.append({"contract": "a stale or early event refuses the wrong mode, NZDT and NZST, DST mornings", "sent": 0})

                    # Unsafe printer state refuses with no intent; a later event in the window can still act.
                    night = "2026-11-06T22:30:00+13:00"
                    gates = tmp/"gates"
                    broker.job = "unrelated-job"
                    await reported("RUNNING", "unrelated-job")
                    await boundary("quiet", night, "refused", "job_mismatch", code=1, state_dir=gates)
                    broker.job = JOB
                    for gcode_state in ("PAUSE", "PREPARE", "IDLE"):
                        broker.gcode_state = gcode_state
                        await reported(gcode_state)
                        await boundary("quiet", night, "refused", "job_not_running", code=1, state_dir=gates)
                    broker.gcode_state = "RUNNING"
                    await reported("RUNNING")
                    broker.print_error = 0x05004003
                    broker.hms = [{"attr": 0x05000000, "code": 0x00040003}]
                    for _ in range(60):
                        if (await call("get_hms_errors")).get("print_error"):
                            break
                        await asyncio.sleep(.05)
                    await boundary("quiet", night, "refused", "active_hms", code=1, state_dir=gates)
                    broker.print_error = 0  # entries remain, Historical once print_error clears: not a fault
                    for _ in range(60):
                        if not (await call("get_hms_errors")).get("print_error"):
                            break
                        await asyncio.sleep(.05)
                    got, out = await helper("dry-run", at=night, mode="quiet", state_dir=gates)
                    assert out["result"] == "would_send", out
                    broker.hms = []
                    await boundary("quiet", night, "refused", "mcp_unreachable", state_dir=gates,
                                   config=policy(mcp_url="http://127.0.0.1:1/mcp"))
                    pacing["on"], broker.reports = False, False
                    await live(False)
                    await boundary("quiet", night, "refused", "telemetry_not_live", code=1, state_dir=gates)
                    assert not list(gates.glob("*.intent.json")), "a refusal consumed the window"
                    records = gates/"events.jsonl"
                    assert len(records.read_text().splitlines()) == 7, records.read_text()
                    await wire_settled()
                    # A bounded wait outlasts a moment without telemetry; the command then goes once.
                    listeners = listening()
                    settle = asyncio.create_task(helper("boundary", mode="quiet", at=night, state_dir=gates,
                                                        config=policy(gate_settle_s=6)))
                    await asyncio.sleep(1)
                    assert listening() <= listeners, "the helper opened a listener"
                    pacing["on"], broker.reports = True, True
                    await live()
                    got, out = await settle
                    assert got == 0 and out["result"] == "sent_unconfirmed", out
                    await wire_settled("1")
                    broker.speed = 2
                    events.append({"contract": "unrelated job, PAUSE, PREPARE, IDLE, active HMS, unreachable MCP and stale "
                                   "telemetry refuse with no intent and no resume; a bounded wait outlasts a short gap",
                                   "sent": 1, "resumes": 0})

                    # One exact command at the boundary, one record pair, sent but not called confirmed.
                    out = await boundary("quiet", night, "sent_unconfirmed", code=0)
                    await wire_settled("1")
                    assert broker.speed == 1
                    assert out["hardware_confirmed"] is False and "Not confirmed" in out["readback"], out
                    assert out["window"] == "quiet-2026-11-06" and out["recorded_at_utc"].endswith("+00:00"), out
                    assert stat.S_IMODE(state.stat().st_mode) == 0o700
                    for f in state.iterdir():
                        assert stat.S_IMODE(f.stat().st_mode) == 0o600, f
                    intent = json.loads((state/"quiet-2026-11-06.intent.json").read_text())
                    assert intent["intended_at_utc"] == "2026-11-06T09:30:00+00:00", intent
                    assert intent["gates"]["job"]["job"] == JOB and intent["gates"]["job"]["wall_start_time"] > 0, intent
                    # The printer reported spd_lvl=1 here, and no tool that stays on this connection shows it.
                    await asyncio.sleep(.5)
                    for tool in ("get_printer_state", "get_print_progress", "get_job_info", "get_printer_connection_status",
                                 "get_printer_info", "get_session_status"):
                        text = json.dumps(await call(tool))
                        assert "spd_lvl" not in text and "speed_level" not in text, (tool, "a readback now exists")
                    pref = await call("get_user_pref", key="speed_level")
                    assert pref.get("value") is None, pref
                    events.append({"contract": "Quiet at 22:30 NZDT: one exact print_speed param 1, recorded owner-only, "
                                   "sent_unconfirmed; no tool exposes spd_lvl; no preference stored", "sent": 1})

                    # The same window never sends twice, from any later process.
                    await boundary("quiet", "2026-11-06T22:30:01+13:00", "duplicate_noop", code=0)
                    await boundary("quiet", "2026-11-07T08:29:59+13:00", "duplicate_noop", code=0)
                    # Morning: Standard once to the same job, and never by a stale event.
                    await boundary("standard", "2026-11-07T08:29:59+13:00", "refused", "wrong_window")
                    await wire_settled()
                    await boundary("standard", "2026-11-07T08:30:00+13:00", "sent_unconfirmed", code=0)
                    await wire_settled("2")
                    await boundary("standard", "2026-11-07T14:00:00+13:00", "duplicate_noop", code=0)
                    await boundary("standard", "2026-11-07T22:30:00+13:00", "refused", "wrong_window")
                    await wire_settled()
                    events.append({"contract": "Standard at 08:30 once to the same job; duplicates and stale events send nothing", "sent": 1})

                    # Concurrent events: one process holds the window, exactly one packet arrives.
                    async def together(mode, at, want):
                        runs = await asyncio.gather(*(helper("boundary", mode=mode, at=at) for _ in range(4)))
                        assert sum(1 for _, o in runs if o["result"] == "sent_unconfirmed") == 1, runs
                        assert all(o["result"] in ("sent_unconfirmed", "duplicate_noop") or o.get("reason") == "concurrent_run"
                                   for _, o in runs), runs
                        await wire_settled(want)
                    await together("quiet", "2026-11-07T22:30:00+13:00", "1")
                    await together("standard", "2026-11-08T08:30:00+13:00", "2")
                    events.append({"contract": "four concurrent runs, Quiet and Standard: one packet each", "sent": 2})

                    # A replacement in the morning gets nothing: another name, or the same name restarted.
                    await boundary("quiet", "2026-11-08T22:30:00+13:00", "sent_unconfirmed")
                    await wire_settled("1")
                    broker.job = "replacement-job"
                    await reported("RUNNING", "replacement-job")
                    await boundary("standard", "2026-11-09T08:30:00+13:00", "refused", "job_mismatch")
                    broker.job, broker.gcode_state = JOB, "FINISH"
                    await reported("FINISH")
                    broker.gcode_state, broker.job_layer = "RUNNING", 2
                    await reported("RUNNING")
                    await boundary("standard", "2026-11-09T08:30:00+13:00", "refused", "replacement_job")
                    await wire_settled()
                    # The new run of that job (a fresh policy review would name it): its own night, then a paused morning.
                    await boundary("quiet", "2026-11-09T22:30:00+13:00", "sent_unconfirmed")
                    await wire_settled("1")
                    broker.gcode_state = "PAUSE"
                    await reported("PAUSE")
                    await boundary("standard", "2026-11-10T08:30:00+13:00", "refused", "job_not_running", code=1)
                    await wire_settled()
                    broker.gcode_state = "RUNNING"  # a person resumed it; this helper sends no resume
                    await reported("RUNNING")
                    await boundary("standard", "2026-11-10T08:31:00+13:00", "sent_unconfirmed")
                    await wire_settled("2")
                    await boundary("standard", "2026-11-11T08:30:00+13:00", "not_needed", code=0)
                    await wire_settled()
                    events.append({"contract": "replacement job (other name; same name restarted) and paused job get no "
                                   "Standard, nothing resumed; no Quiet that night means no Standard", "sent": 2})

                    # Unknown outcomes: killed before the call, killed after it, and an answer that never came.
                    night2 = "2026-11-11T22:30:00+13:00"
                    got, _ = await helper("boundary", mode="quiet", at=night2, fault="crash_before")
                    assert got == 137, got
                    await wire_settled()
                    assert (state/"quiet-2026-11-11.intent.json").exists() and not (state/"quiet-2026-11-11.outcome.json").exists()
                    await boundary("quiet", "2026-11-11T23:00:00+13:00", "unknown_no_replay", code=2)
                    await wire_settled()
                    await boundary("standard", "2026-11-12T08:30:00+13:00", "sent_unconfirmed")  # that Quiet may have arrived
                    await wire_settled("2")
                    got, _ = await helper("boundary", mode="quiet", at="2026-11-12T22:30:00+13:00", fault="crash_after")
                    assert got == 137, got
                    await wire_settled("1")
                    await boundary("quiet", "2026-11-12T22:31:00+13:00", "unknown_no_replay", code=2)
                    await wire_settled()
                    await boundary("standard", "2026-11-13T08:30:00+13:00", "sent_unconfirmed")
                    await wire_settled("2")
                    out = await boundary("quiet", "2026-11-13T22:30:00+13:00", "unknown", code=2, fault="lost_answer")
                    assert "never arrived" in out["answer"] and out["hardware_confirmed"] is False, out
                    await wire_settled("1")
                    await boundary("quiet", "2026-11-13T22:31:00+13:00", "unknown_no_replay", code=2)
                    await boundary("standard", "2026-11-14T08:30:00+13:00", "sent_unconfirmed")
                    await wire_settled("2")
                    events.append({"contract": "kill before the call, kill after it and a lost answer are unknown and never "
                                   "replayed; Standard still follows a Quiet that may have arrived", "sent": 4})

                    # The server refuses at the last moment: telemetry dies after the intent. Not sent, one attempt.
                    release = tmp/"release"
                    held = asyncio.create_task(helper("boundary", mode="quiet", at="2026-11-14T22:30:00+13:00",
                                                      fault=f"hold:{release}"))
                    while not (state/"quiet-2026-11-14.intent.json").exists():
                        await asyncio.sleep(.05)
                    pacing["on"], broker.reports = False, False
                    await live(False)
                    release.touch()
                    got, out = await held
                    assert got == 1 and out["result"] == "not_sent" and "telemetry_not_live" in out["answer"], out
                    pacing["on"], broker.reports = True, True
                    await live()
                    await wire_settled()
                    await boundary("quiet", "2026-11-14T22:31:00+13:00", "not_sent", code=1)
                    await boundary("standard", "2026-11-15T08:30:00+13:00", "not_needed", code=0)
                    await wire_settled()
                    events.append({"contract": "a server refusal after the intent is not_sent and uses the window", "sent": 0})

                    # Real backpressure: the MQTT socket is full, delivery is unknown, the socket's end loses it.
                    broker.gcode_state = "IDLE"
                    await reported("IDLE")
                    broker.block_reads = True
                    filler = await call("send_gcode", gcode=";HARNESS-ONLY-"+"x"*(8*1024*1024), user_permission=True)
                    assert "publish_unconfirmed" in str(filler), str(filler)[:500]
                    broker.gcode_state = "RUNNING"
                    await reported("RUNNING")
                    out = await boundary("quiet", "2026-11-15T22:30:00+13:00", "unknown", code=2)
                    assert "publish_unconfirmed" in out["answer"], out
                    await boundary("quiet", "2026-11-15T22:31:00+13:00", "unknown_no_replay", code=2)
                    before = broker.connections
                    broker.drop()
                    broker.block_reads = False
                    for _ in range(200):
                        if broker.connections > before:
                            break
                        await asyncio.sleep(.05)
                    await live()
                    await reported("RUNNING")
                    await boundary("quiet", "2026-11-15T22:32:00+13:00", "unknown_no_replay", code=2)
                    await wire_settled()
                    events.append({"contract": "a full MQTT socket gives unknown delivery, no replay after reconnect", "replayed": 0})

                    # The plain file, as the unit runs it (python -I, nothing patched), on the real clock.
                    now = dt.datetime.now(dt.timezone.utc)
                    zone = "Pacific/Auckland" if now.astimezone(ZoneInfo("Pacific/Auckland")).strftime("%H:%M") != "00:00" else "Etc/UTC"
                    real_state = tmp/"real-state"
                    all_night = policy(timezone=zone, start="00:01", end="00:00")
                    got, out = await helper("dry-run", config=all_night, state_dir=real_state, mode="quiet", real=True)
                    assert got == 0 and out["result"] == "would_send", out
                    assert not real_state.exists()
                    got, out = await helper("boundary", config=all_night, state_dir=real_state, mode="quiet", real=True)
                    assert got == 0 and out["result"] == "sent_unconfirmed", out
                    await wire_settled("1")
                    got, out = await helper("boundary", config=all_night, state_dir=real_state, mode="quiet", real=True)
                    assert got == 0 and out["result"] == "duplicate_noop", out
                    await wire_settled()
                    events.append({"contract": "ops/night_speed.py run as the unit runs it, real clock: dry run, one send, no repeat", "sent": 1})

                    # Whole campaign: speed packets only, on the one connection owner, no card access.
                    on_wire()
                    assert broker.maximum == 1, broker.maximum
                    assert ftps.connections == ftps_before, "FTPS reached the printer"
                    assert set(expected) == {"1", "2"} and len(expected) == len(speeds())
                    await asyncio.sleep(.1)

            # The templates the operator installs, as systemd reads them.
            units = ROOT/"ops/night-speed"
            def unit(name):
                parser = configparser.ConfigParser(strict=False, interpolation=None)
                parser.optionxform = str
                parser.read(units/name)
                return parser
            for mode, hours in (("quiet", "22:30"), ("standard", "08:30")):
                timer = unit(f"bambu-night-speed-{mode}.timer")["Timer"]
                assert timer["OnCalendar"] == f"*-*-* {hours}:00 Pacific/Auckland" and timer["Unit"] == f"bambu-night-speed@{mode}.service", dict(timer)
                assert timer["Persistent"] == "true"
                if shutil.which("systemd-analyze"):
                    parsed = subprocess.run(["systemd-analyze", "calendar", timer["OnCalendar"]], capture_output=True, text=True)
                    assert parsed.returncode == 0 and f"{hours}:00 Pacific/Auckland" in parsed.stdout, parsed
            service = unit("bambu-night-speed@.service")
            assert not {"Requires", "Wants", "BindsTo", "PartOf"} & set(service["Unit"]), dict(service["Unit"])
            start = service["Service"]["ExecStart"]
            assert " -I " in start and "boundary --mode %i" in start and int(service["Service"]["TimeoutStartSec"]) > 240 + 30, start
            events.append({"contract": "units: 22:30 and 08:30 Pacific/Auckland, nothing starts the server"})
            proc.terminate()
            proc.wait(timeout=14)
            return {"events": events, "speed_packets": expected, "mqtt_connections_during_helper_runs": broker.connections - connections,
                    "max_mqtt_clients": broker.maximum, "ftps_connections": ftps.connections - ftps_before,
                    "transport": "ops/night_speed.py process -> MCP HTTP -> TLS MQTT", "passed": True}
        except BaseException:
            log.flush(); log.seek(0)
            print(log.read()[-6000:], file=sys.stderr)
            raise
        finally:
            pacer.cancel()
            if proc.poll() is None:
                proc.kill(); proc.wait(timeout=5)
            broker.close(); ftps.close(); camera.close(); log.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(campaign())))
