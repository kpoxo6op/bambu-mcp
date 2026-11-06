#!/usr/bin/env python3
"""Quiet at night, Standard in the morning, for one reviewed print. See ops/night-speed/README.md.

Run by two systemd calendar timers, as its own short-lived process, against the running server's
HTTP MCP endpoint. It opens no MQTT, FTPS or camera connection, listens on nothing, needs no model
or agent, and imports nothing from the server. The only command it can send is
set_print_speed(name, 'quiet' or 'standard', user_permission=True), at most once per window.

    night_speed.py check    --config night-speed.json
    night_speed.py dry-run  --config night-speed.json --state-dir DIR [--mode quiet|standard] [--at ISO]
    night_speed.py boundary --config night-speed.json --state-dir DIR --mode quiet|standard

A window is the stretch between two boundaries in the policy's IANA zone: quiet-<date of its
start> or standard-<date>. boundary acts only when --mode is the mode of the current window, so a
late or caught-up event cannot apply the wrong one; a missed event applies later only inside its
own window. Refusals consume nothing. Just before the command, the window's intent is created
exclusively and synced: an intent exists at most once, a second run in the window sends nothing,
and an intent without an outcome (crash, kill, lost answer) stays unknown and is never replayed.
Standard is sent only when that night's Quiet may have reached the printer, and only to the same
job. Nothing here pauses, resumes, stops or clears anything.

Readback: no MCP tool reports the printer's speed profile, so "sent_unconfirmed" means the command
reached the MQTT socket (QoS 0), not that the printer changed speed. dry-run only reads.

Exit status: 0 sent, already sent, not needed, or a dry run that would send; 1 refused or
not sent; 2 delivery unknown.
"""
import argparse
import asyncio
import base64
import datetime as dt
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

SCHEMA_VERSION = 1
MAX_POLICY_BYTES = 64 * 1024
POLICY_KEYS = {"schema_version", "enabled", "printer", "mcp_url", "timezone", "start", "end", "quiet_mode",
               "day_mode", "expected_job", "gate_settle_s", "reviewed", "authorization", "operator_plan", "status"}
REQUIRED_KEYS = POLICY_KEYS - {"gate_settle_s", "reviewed", "operator_plan", "status"}
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
# Telemetry can be away for a moment at the boundary (a reconnect); wait that out, not a refusal
# that needs a person. The whole run stays inside the unit's TimeoutStartSec.
DEFAULT_SETTLE_S = 180
MAX_SETTLE_S = 240
SETTLE_RETRY_S = 20.
ATTEMPT_TIMEOUT_S = 30.
READBACK = ("Sent to the printer's MQTT socket (QoS 0). Not confirmed: no MCP tool reports spd_lvl, and "
            "the setter does not change the server's own value. Check the printer's screen.")
NEVER_REPLAYED = ("An intent exists for this window without a known delivery. The command may have reached the "
                  "printer. It is not sent again; check the printer's screen.")


def _wall_clock() -> float:  # the harness reads the campaign's clock instead
    return time.time()


def _utc(epoch: float) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat()


class Refused(Exception):
    def __init__(self, reason: str, detail: str, retriable: bool = False):
        super().__init__(detail)
        self.reason, self.detail, self.retriable = reason, detail, retriable


# ── Policy ────────────────────────────────────────────────────────────────────────────────────────

def _hours(value, key: str) -> dt.time:
    if not isinstance(value, str) or not re.fullmatch(r"([01][0-9]|2[0-3]):[0-5][0-9]", value):
        raise Refused("policy_invalid", f"{key} must be HH:MM (00:00-23:59), got {value!r}")
    return dt.time(int(value[:2]), int(value[3:]))


def load_policy(path: Path) -> dict:
    """The reviewed private config, or Refused. Writes nothing and connects to nothing."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise Refused("policy_invalid", f"{path}: {exc.strerror}")
    with os.fdopen(fd, "rb") as fh:
        st = os.fstat(fh.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise Refused("policy_invalid", f"{path} must be a regular file of this user, mode 0600 or stricter "
                                            f"(mode {stat.S_IMODE(st.st_mode):04o})")
        raw = fh.read(MAX_POLICY_BYTES + 1)
    if len(raw) > MAX_POLICY_BYTES:
        raise Refused("policy_invalid", f"{path} is over {MAX_POLICY_BYTES} bytes")
    try:
        policy = json.loads(raw)
    except ValueError as exc:
        raise Refused("policy_invalid", f"{path} is not JSON: {exc}")
    if not isinstance(policy, dict):
        raise Refused("policy_invalid", "the policy must be a JSON object")
    missing, unknown = sorted(REQUIRED_KEYS - policy.keys()), sorted(policy.keys() - POLICY_KEYS)
    if missing or unknown:
        raise Refused("policy_invalid", f"missing keys {missing}, unknown keys {unknown}")
    if policy["schema_version"] != SCHEMA_VERSION or policy["enabled"] not in (True, False):
        raise Refused("policy_invalid", f"schema_version must be {SCHEMA_VERSION} and enabled true or false")
    if policy["quiet_mode"] != "quiet" or policy["day_mode"] != "standard":
        raise Refused("policy_invalid", "quiet_mode must be 'quiet' and day_mode 'standard'; this helper sends no other")
    for key in ("printer", "expected_job"):
        value = policy[key]
        if not isinstance(value, str) or not value or value != value.strip() or len(value) > 200:
            raise Refused("policy_invalid", f"{key} must be an exact non-empty name")
    url = urlsplit(policy["mcp_url"]) if isinstance(policy["mcp_url"], str) else None
    if (url is None or url.scheme != "http" or url.hostname not in LOOPBACK or url.port is None or url.path != "/mcp"
            or url.username or url.query or url.fragment):
        raise Refused("policy_invalid", "mcp_url must be http://<loopback>:<port>/mcp")
    try:
        policy["_zone"] = ZoneInfo(policy["timezone"])
    except Exception:
        raise Refused("policy_invalid", f"unknown timezone {policy['timezone']!r}")
    policy["_start"], policy["_end"] = _hours(policy["start"], "start"), _hours(policy["end"], "end")
    if policy["_start"] <= policy["_end"]:
        raise Refused("policy_invalid", "the quiet window must cross midnight (start later than end)")
    settle = policy.get("gate_settle_s", DEFAULT_SETTLE_S)
    if isinstance(settle, bool) or not isinstance(settle, int) or not 0 <= settle <= MAX_SETTLE_S:
        raise Refused("policy_invalid", f"gate_settle_s must be 0 to {MAX_SETTLE_S}")
    policy["_settle"] = settle
    auth = policy["authorization"]
    if not isinstance(auth, dict) or not all(isinstance(auth.get(k), str) and auth[k].strip()
                                             for k in ("at", "source", "verbatim")):
        raise Refused("policy_invalid", "authorization must carry at, source and verbatim")
    reviewed = policy.get("reviewed")
    if policy["enabled"] and not (isinstance(reviewed, dict) and all(
            isinstance(reviewed.get(k), str) and reviewed[k].strip() for k in ("at", "by"))):
        raise Refused("policy_invalid", "an enabled policy must carry reviewed: {at, by}")
    policy["_sha256"] = hashlib.sha256(raw).hexdigest()
    return policy


def window_at(policy: dict, epoch: float) -> dict:
    """The window holding an instant, by wall clock in the policy's zone (DST moves the UTC instants)."""
    local = dt.datetime.fromtimestamp(epoch, policy["_zone"])
    today, now = local.date(), local.time()
    if now >= policy["_start"]:
        mode, date = "quiet", today
    elif now < policy["_end"]:
        mode, date = "quiet", today - dt.timedelta(days=1)
    else:
        mode, date = "standard", today
    return {"id": f"{mode}-{date.isoformat()}", "mode": mode, "local": local.isoformat(),
            "paired_quiet": f"quiet-{(today - dt.timedelta(days=1)).isoformat()}" if mode == "standard" else None}


# ── Records ───────────────────────────────────────────────────────────────────────────────────────

def _check_private_dir(path: Path) -> None:
    st = os.lstat(path)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise Refused("state_invalid", f"{path} must be a real directory of this user, mode 0700")


def prepare_state(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        _check_private_dir(path)
    except OSError as exc:
        raise Refused("state_invalid", f"{path}: {exc.strerror}")


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def create_once(path: Path, record: dict) -> bool:
    """Create a complete owner-only record, durably, unless it exists. False when it exists."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(record, fh, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, path)  # fails on an existing name; the rename a replace would do does not
        except FileExistsError:
            return False
    finally:
        tmp.unlink(missing_ok=True)
    _sync_dir(path.parent)
    return True


def window_record(state: Path, window_id: str) -> dict | None:
    """What this window has already done: None, or {"status": sent_unconfirmed | not_sent | unknown, ...}."""
    try:
        intent = json.loads((state / f"{window_id}.intent.json").read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        return {"status": "unknown", "detail": f"unreadable intent: {exc}", "intent": None}
    try:
        outcome = json.loads((state / f"{window_id}.outcome.json").read_text())
    except (OSError, ValueError):
        return {"status": "unknown", "detail": NEVER_REPLAYED, "intent": intent}
    status = outcome.get("status")
    return {"status": status if status in ("sent_unconfirmed", "not_sent") else "unknown",
            "detail": outcome.get("answer"), "intent": intent}


def append_event(state: Path, event: dict) -> None:
    fd = os.open(state / "events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "a") as fh:
        fh.write(json.dumps(event) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


# ── Gates over the shared MCP server ──────────────────────────────────────────────────────────────

def _decode(result):
    """A tool's value as MCP delivers it, including the server's compression envelope."""
    value = result.structuredContent
    if isinstance(value, dict) and set(value) == {"result"}:
        value = value["result"]
    if value is None:
        value = "\n".join(c.text for c in result.content if c.type == "text")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            pass
    if isinstance(value, dict) and value.get("encoding") == "gzip+base64":
        value = json.loads(gzip.decompress(base64.b64decode(value["data"])))
    return value


async def read_gates(session, policy: dict, paired: dict | None):
    """Every gate, read-only, in one pass. Returns (Refused or None, what was seen)."""
    printer, seen = policy["printer"], {}
    tools = {t.name: t for t in (await session.list_tools()).tools}
    speed = tools.get("set_print_speed")
    if speed is None or not {"name", "speed_level", "user_permission"} <= set(speed.inputSchema.get("properties", {})):
        return Refused("tool_missing", "this server has no set_print_speed(name, speed_level, user_permission)"), seen

    async def read(tool: str):
        result = await session.call_tool(tool, {"name": printer})
        return {"error": "tool_error", "detail": str(result.content)[:500]} if result.isError else _decode(result)

    link = await read("get_printer_connection_status")
    telemetry = link.get("telemetry") or {} if isinstance(link, dict) else {}
    seen["telemetry"] = {k: telemetry.get(k) for k in ("status", "reason", "live", "writable", "session_generation")}
    seen["telemetry"]["connected"] = isinstance(link, dict) and link.get("connected")
    if not (isinstance(link, dict) and link.get("connected") is True and link.get("live") is True
            and telemetry.get("status") == "live" and telemetry.get("writable") is True):
        return Refused("telemetry_not_live", f"'{printer}' is not connected with live, writable telemetry: "
                       f"{seen['telemetry']}", retriable=True), seen
    progress, job, hms = await read("get_print_progress"), await read("get_job_info"), await read("get_hms_errors")
    for name, value in (("get_print_progress", progress), ("get_job_info", job), ("get_hms_errors", hms)):
        if not isinstance(value, dict) or "error" in value:
            detail = value.get("error") if isinstance(value, dict) else str(value)[:200]
            return Refused("telemetry_not_live", f"{name}: {detail}", retriable=True), seen
    identity = {"job": progress.get("subtask_name"), "wall_start_time": job.get("wall_start_time"),
                "total_layers": progress.get("total_layers")}
    seen["job"] = {**identity, "gcode_state": progress.get("gcode_state"), "layer": progress.get("current_layer"),
                   "percent": progress.get("print_percentage"), "stage": progress.get("stage_name")}
    if identity["job"] != policy["expected_job"] or job.get("subtask_name") != policy["expected_job"]:
        return Refused("job_mismatch", f"the printer reports job {identity['job']!r}, not the reviewed "
                       f"{policy['expected_job']!r}"), seen
    if progress.get("gcode_state") != "RUNNING":
        return Refused("job_not_running", f"the job is {progress.get('gcode_state')!r}, not RUNNING. Nothing is sent, "
                       "resumed or cleared."), seen
    active = [e for e in hms.get("hms_errors", ["unreadable"]) if not isinstance(e, dict)
              or e.get("severity") != "Historical"]
    seen["hms"] = {"print_error": hms.get("print_error"), "active": active}
    if hms.get("print_error") != 0 or active:
        return Refused("active_hms", f"print_error={hms.get('print_error')!r}, active HMS {active}"), seen
    if not (isinstance(identity["wall_start_time"], (int, float)) and identity["wall_start_time"] > 0
            and isinstance(identity["total_layers"], int) and identity["total_layers"] > 0):
        return Refused("job_identity_unknown", f"the job has no start time or layer count yet: {identity}"), seen
    if paired is not None:
        night = (paired.get("gates") or {}).get("job") or {}
        same = all(night.get(k) == identity[k] for k in identity)
        progressing = (isinstance(night.get("layer"), int) and isinstance(seen["job"]["layer"], int)
                       and seen["job"]["layer"] >= night["layer"])
        if not (same and progressing):
            return Refused("replacement_job", "this is not the job that got Quiet: "
                           f"night {night}, now {seen['job']}"), seen
    return None, seen


async def _send_speed(session, printer: str, mode: str) -> str:
    result = await session.call_tool("set_print_speed", {"name": printer, "speed_level": mode, "user_permission": True})
    text = "\n".join(c.text for c in result.content if c.type == "text")
    return f"tool error: {text}" if result.isError else text


def classify(answer: str, mode: str, printer: str) -> str:
    """Only the exact success with its delivery note is sent; only the two guard refusals are not."""
    if answer.startswith(f"Speed level set to '{mode}' on '{printer}'.") and "[delivery:" in answer:
        return "sent_unconfirmed"
    if answer.startswith(("Error (telemetry_not_live)", "Error (publish_rejected)")):
        return "not_sent"
    return "unknown"


async def attempt(policy: dict, mode: str, window: dict, paired: dict | None, state: Path | None) -> dict:
    """One connection to the shared server. state None (dry run) ends after the reads."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    out = {"refusal": None, "gates": {}, "intent": False, "answer": None, "error": None, "conflict": False}
    try:
        async with asyncio.timeout(ATTEMPT_TIMEOUT_S):
            # Nothing is raised in here on purpose: anyio's task groups would wrap it.
            async with streamablehttp_client(policy["mcp_url"]) as (rd, wr, _):
                async with ClientSession(rd, wr) as session:
                    await session.initialize()
                    out["refusal"], out["gates"] = await read_gates(session, policy, paired)
                    if out["refusal"] is not None or state is None:
                        return out
                    now = _wall_clock()
                    if window_at(policy, now)["id"] != window["id"]:
                        out["refusal"] = Refused("wrong_window", f"the window {window['id']} ended during the checks")
                        return out
                    try:
                        out["intent"] = create_once(state / f"{window['id']}.intent.json", {
                            "window": window["id"], "mode": mode, "printer": policy["printer"],
                            "expected_job": policy["expected_job"], "policy_sha256": policy["_sha256"],
                            "intended_at_utc": _utc(now), "local": dt.datetime.fromtimestamp(now, policy["_zone"]).isoformat(),
                            "gates": out["gates"]})
                    except OSError as exc:  # no durable intent, no command
                        out["refusal"] = Refused("state_invalid", f"the intent could not be recorded: {exc}")
                        return out
                    if not out["intent"]:
                        out["conflict"] = True
                        return out
                    out["answer"] = await _send_speed(session, policy["printer"], mode)
    except (Exception, BaseExceptionGroup) as exc:
        while isinstance(exc, BaseExceptionGroup):
            exc = exc.exceptions[0]
        out["error"] = f"{type(exc).__name__}: {exc}"[:500]
    return out


def before_connecting(policy: dict, mode: str, epoch: float, state_dir: Path) -> tuple[dict, dict | None, dict | None]:
    """Local answers. Returns (window, final result or None, the paired quiet intent for Standard)."""
    window = window_at(policy, epoch)
    if window["mode"] != mode:
        raise Refused("wrong_window", f"{window['local']} is in {window['id']}; {mode} does not apply now")
    done = window_record(state_dir, window["id"])
    if done is not None:
        if done["status"] == "sent_unconfirmed":
            return window, {"result": "duplicate_noop", "detail": "this window's command was already sent"}, None
        if done["status"] == "not_sent":
            return window, {"result": "not_sent", "detail": "this window's one attempt was refused by the server; "
                            "the record stands"}, None
        return window, {"result": "unknown_no_replay", "detail": NEVER_REPLAYED}, None
    if mode != "standard":
        return window, None, None
    night = window_record(state_dir, window["paired_quiet"])
    if night is None or night["status"] == "not_sent":
        return window, {"result": "not_needed", "detail": f"{window['paired_quiet']} sent no Quiet to undo"}, None
    if night["intent"] is None:
        raise Refused("record_unreadable", f"{window['paired_quiet']}: {night['detail']}")
    return window, None, night["intent"]


EXIT_CODES = {"sent_unconfirmed": 0, "duplicate_noop": 0, "not_needed": 0, "would_send": 0,
              "refused": 1, "not_sent": 1, "unknown": 2, "unknown_no_replay": 2}


def finish(result: dict, state: Path | None = None) -> int:
    if state is not None:
        try:
            append_event(state, result)
        except OSError as exc:
            result = {**result, "event_log_error": str(exc)}
    print(json.dumps(result, indent=1), flush=True)
    return EXIT_CODES[result["result"]]


def refused(error: Refused, **extra) -> dict:
    return {"result": "refused", "reason": error.reason, "detail": error.detail, "sent": 0, **extra}


def run_boundary(args) -> int:
    os.umask(0o077)
    base = {"mode": args.mode, "at_utc": _utc(_wall_clock())}
    try:
        policy = load_policy(args.config)
        if not policy["enabled"]:
            raise Refused("disabled", "the policy is not enabled")
        prepare_state(args.state_dir)
    except Refused as error:
        return finish(refused(error, **base))
    state = args.state_dir
    base.update(printer=policy["printer"], expected_job=policy["expected_job"])
    lock = os.open(state / "lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return finish(refused(Refused("concurrent_run", "another run holds this state directory; it decides"), **base), state)
    deadline = time.monotonic() + policy["_settle"]
    while True:
        try:
            epoch = _wall_clock()
            base["at_utc"] = _utc(epoch)
            window, final, paired = before_connecting(policy, args.mode, epoch, state)
        except Refused as error:
            return finish(refused(error, **base), state)
        base["window"] = window["id"]
        if final is not None:
            return finish({**final, **base}, state)
        out = asyncio.run(attempt(policy, args.mode, window, paired, state))
        if out["conflict"]:
            continue  # the record decides on the next pass
        if out["intent"]:
            answer = out["answer"]
            status = classify(answer, args.mode, policy["printer"]) if answer is not None else "unknown"
            outcome = {"window": window["id"], "mode": args.mode, "status": status,
                       "answer": (answer if answer is not None else out["error"] or "no answer")[:2000],
                       "recorded_at_utc": _utc(_wall_clock()), "hardware_confirmed": False,
                       "readback": READBACK if status == "sent_unconfirmed" else None}
            try:
                create_once(state / f"{window['id']}.outcome.json", outcome)
            except OSError as exc:  # the intent stands: later runs read this window as unknown
                outcome["outcome_record_error"] = str(exc)
            return finish({"result": status, **outcome, **base, "gates": out["gates"]}, state)
        error = out["refusal"] or Refused("mcp_unreachable", f"{policy['mcp_url']}: {out['error']}", retriable=True)
        if not error.retriable or time.monotonic() + SETTLE_RETRY_S > deadline:
            return finish(refused(error, **base, gates=out["gates"]), state)
        time.sleep(SETTLE_RETRY_S)


def run_dry(args) -> int:
    """The same gates, reads only: no record, lock or directory is created and no command can be sent."""
    base = {"dry_run": True, "sent": 0}
    try:
        policy = load_policy(args.config)
        if args.state_dir.exists():
            _check_private_dir(args.state_dir)
        epoch = _wall_clock()
        if args.at:
            when = dt.datetime.fromisoformat(args.at)
            if when.tzinfo is None:
                raise Refused("bad_time", "--at needs a UTC offset")
            epoch = when.timestamp()
        mode = args.mode or window_at(policy, epoch)["mode"]
        base.update(mode=mode, at_utc=_utc(epoch), enabled=policy["enabled"], simulated_time=bool(args.at),
                    printer=policy["printer"], expected_job=policy["expected_job"])
        window, final, paired = before_connecting(policy, mode, epoch, args.state_dir)
    except Refused as error:
        return finish(refused(error, **base))
    base["window"] = window["id"]
    if final is not None:
        return finish({**final, **base})
    out = asyncio.run(attempt(policy, mode, window, paired, None))
    error = out["refusal"] or (Refused("mcp_unreachable", f"{policy['mcp_url']}: {out['error']}") if out["error"] else None)
    if error is None and not policy["enabled"]:
        error = Refused("disabled", "every gate passed, but the policy is not enabled")
    if error is not None:
        return finish(refused(error, **base, gates=out["gates"]))
    return finish({"result": "would_send", "command": f"set_print_speed({policy['printer']}, {mode})",
                   "gates": out["gates"], **base})


def run_check(args) -> int:
    try:
        policy = load_policy(args.config)
    except Refused as error:
        return finish(refused(error))
    print(json.dumps({"valid": True, "enabled": policy["enabled"], "printer": policy["printer"],
                      "expected_job": policy["expected_job"], "timezone": policy["timezone"],
                      "quiet": [policy["start"], policy["end"]], "gate_settle_s": policy["_settle"],
                      "policy_sha256": policy["_sha256"]}, indent=1))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="validate the policy; no connection")
    check.add_argument("--config", type=Path, required=True)
    dry = sub.add_parser("dry-run", help="read every gate, optionally for a simulated time; never writes")
    dry.add_argument("--config", type=Path, required=True)
    dry.add_argument("--state-dir", type=Path, required=True)
    dry.add_argument("--mode", choices=("quiet", "standard"))
    dry.add_argument("--at", help="ISO time with offset to evaluate the time gate for (real time is not changed)")
    boundary = sub.add_parser("boundary", help="what the timers run: at most one command per window")
    boundary.add_argument("--config", type=Path, required=True)
    boundary.add_argument("--state-dir", type=Path, required=True)
    boundary.add_argument("--mode", choices=("quiet", "standard"), required=True)
    args = parser.parse_args()
    return {"check": run_check, "dry-run": run_dry, "boundary": run_boundary}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
