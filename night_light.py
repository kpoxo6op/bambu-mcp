"""
night_light.py — Per-printer quiet hours for the chamber light, and bounded inspection leases.

Without this, a light switched on for a camera check (or by the firmware, or by hand) stayed on
all night until someone asked an agent to switch it off. The daemon that already owns the one
MQTT session now does it from an ordinary timer and the session's update notifications: no
agent, no extra connection, no polling of the camera or the job.

Policy (opt-in, per printer, ``~/.bambu-mcp/night_light_<name>.json``, written atomically):
``{"enabled", "timezone" (IANA), "start", "end" ("HH:MM", local wall clock)}``. The window may
cross midnight; local time comes from zoneinfo, so daylight-saving changes move it correctly.

Rules, evaluated every TICK_S and when the printer reports:

* During quiet hours a chamber light the printer reports ON on this connection is switched OFF
  through the guarded publish path (``printer.light_state = False``), the same command as
  ``set_chamber_light``. That covers the boundary itself, a daemon start or reconnect inside the
  window and a light switched on during it by the firmware, a person or another tool.
* Nothing is sent unless telemetry is writable (live, supervisor alive) and the printer has
  reported its light on this connection. An unreported light is unknown, never taken as off,
  and is never switched blind. After an OFF command, another is sent only when a newer report
  still says ON, and no sooner than OFF_BACKOFF_MIN_S, doubling to OFF_BACKOFF_MAX_S until the
  light is reported OFF. A light already OFF gets no command.
* The light is never switched ON automatically, and nothing else (print, heaters, settings)
  is ever changed. A running print is not touched.

Inspection lease (``grant_lease``): switches a light reported OFF to ON for at most
LEASE_MAX_S and persists the deadline first. At the deadline the lease sends its own OFF
whether or not the printer has reported the ON (it need not: QoS 0, and status deltas may omit
lights_report), through the same guard, so only while telemetry is writable; a printer offline
at the deadline gets it once it is writable again, also after a daemon restart. The lease
closes only on an OFF report newer than that OFF; until then the OFF repeats on the back-off.
A light already ON is left alone and gets no lease, so an ordinary manual ON is never switched
off. An expired lease never proven off is dropped LEASE_GRACE_S after its deadline.

Precedence: quiet hours win. A lease is refused inside quiet hours and is cut short at the next
quiet-hours start; inside quiet hours the light goes OFF whatever any lease says.
"""

from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import os
import re
import tempfile
import threading
import time
import zoneinfo
from pathlib import Path

log = logging.getLogger(__name__)

TICK_S = 30.0               # re-evaluation without any report
MIN_GAP_S = 1.0             # at most one evaluation pass this often, however often reports arrive
OFF_BACKOFF_MIN_S = 60.0    # gap before a repeated OFF for a light still reported ON
OFF_BACKOFF_MAX_S = 1800.0
LEASE_MAX_S = 900
LEASE_GRACE_S = 6 * 3600.0

_STATE_DIR = Path.home() / ".bambu-mcp"
_HHMM = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


def _wall_clock() -> float:  # replaced by the E2E fixture to place the server in a chosen hour
    return time.time()


@functools.cache
def _zones() -> frozenset[str]:
    return frozenset(zoneinfo.available_timezones())


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def validate_policy(enabled: bool, timezone: str, start: str, end: str) -> dict:
    """The stored policy for these arguments. Raises ValueError naming the bad argument."""
    if not isinstance(enabled, bool):
        raise ValueError("enabled must be true or false")
    if not isinstance(timezone, str) or timezone not in _zones():
        raise ValueError(f"timezone {timezone!r} is not an IANA zone name such as 'Europe/Berlin'")
    for label, value in (("start", start), ("end", end)):
        if not isinstance(value, str) or not _HHMM.match(value):
            raise ValueError(f"{label} {value!r} must be HH:MM, 24-hour (00:00-23:59)")
    if start == end:
        raise ValueError("start and end must differ")
    return {"enabled": enabled, "timezone": timezone, "start": start, "end": end}


def quiet_now(policy: dict | None, now: float) -> bool:
    if not policy or not policy.get("enabled"):
        return False
    local = dt.datetime.fromtimestamp(now, zoneinfo.ZoneInfo(policy["timezone"]))
    m, s, e = local.hour * 60 + local.minute, _minutes(policy["start"]), _minutes(policy["end"])
    return s <= m < e if s < e else (m >= s or m < e)


def _next_local(policy: dict, hhmm: str, now: float) -> float:
    """The first instant after ``now`` whose local wall clock reads ``hhmm``."""
    zone = zoneinfo.ZoneInfo(policy["timezone"])
    today = dt.datetime.fromtimestamp(now, zone).date()
    h, m = (int(x) for x in hhmm.split(":"))
    for days in range(3):
        at = dt.datetime.combine(today + dt.timedelta(days=days), dt.time(h, m), tzinfo=zone).timestamp()
        if at > now:
            return at
    raise AssertionError("unreachable")


def _iso(epoch: float | None, policy: dict | None = None) -> str | None:
    if epoch is None:
        return None
    zone = zoneinfo.ZoneInfo(policy["timezone"]) if policy else dt.timezone.utc
    return dt.datetime.fromtimestamp(epoch, zone).isoformat(timespec="seconds")


def _path(name: str) -> Path:
    return _STATE_DIR / f"night_light_{name}.json"


def _configured(name: str) -> bool:
    import auth
    return name in auth.get_configured_printer_names()


class _Controller:
    def __init__(self):
        # Serialises evaluation, lease changes and policy writes, so the controller never acts on
        # a record a tool is replacing (a lease is persisted before its ON is sent).
        self._lock = threading.RLock()
        self._records: dict[str, dict] = {}
        self._runtime: dict[str, dict] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._manager = None

    # ── persistence ──────────────────────────────────────────────────────────

    def _record(self, name: str) -> dict:
        rec = self._records.get(name)
        if rec is None:
            rec = {"policy": None, "lease": None}
            try:
                data = json.loads(_path(name).read_text(encoding="utf-8"))
                policy, lease = data.get("policy"), data.get("lease")
                if policy is not None:
                    policy = validate_policy(policy.get("enabled"), policy.get("timezone"),
                                             policy.get("start"), policy.get("end"))
                if lease is not None:
                    lease = {"until": float(lease["until"]), "granted_at": float(lease["granted_at"]),
                             "restore": "off"}
                rec = {"policy": policy, "lease": lease}
            except FileNotFoundError:
                pass
            except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
                # Operator intent is unknown: act on nothing and say so in the status.
                rec["error"] = f"unreadable {_path(name).name}: {type(exc).__name__}: {exc}"
                log.warning("night_light: %s; no policy applied for %s", rec["error"], name)
            self._records[name] = rec
        return rec

    def _save(self, name: str, policy: dict | None, lease: dict | None) -> None:
        """Write the record (write, fsync, rename) and only then adopt it in memory."""
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=_STATE_DIR, prefix=".night_light_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"version": 1, "policy": policy, "lease": lease}, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, _path(name))
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._records[name] = {"policy": policy, "lease": lease}

    def _rt(self, name: str) -> dict:
        return self._runtime.setdefault(name, {"last_off": None, "off_sends": 0, "last_action": None})

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self, manager) -> None:
        self._manager = manager
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="night-light", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def wake(self, _name: str | None = None) -> None:
        """Update callback: evaluate soon, on the controller's own thread (never the report worker's)."""
        self._wake.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(TICK_S)
            self._wake.clear()
            if self._stop.is_set():
                return
            try:
                names = self._manager.list_connected() if self._manager else []
            except Exception:
                names = []
            for name in names:
                try:
                    self.evaluate(name)
                except Exception as exc:
                    log.warning("night_light: evaluation failed for %s: %s", name, exc, exc_info=True)
            self._stop.wait(MIN_GAP_S)

    # ── the rule ─────────────────────────────────────────────────────────────

    def evaluate(self, name: str) -> None:
        import reliability

        with self._lock:
            rec = self._record(name)
            policy, lease = rec["policy"], rec["lease"]
            now = _wall_clock()
            night = quiet_now(policy, now)
            lease_due = lease is not None and now >= lease["until"]
            if lease_due and now - lease["until"] > LEASE_GRACE_S:
                log.warning("night_light: %s lease ended %.0fs ago with no light report; dropped",
                            name, now - lease["until"])
                self._save(name, policy, None)
                lease, lease_due = None, False
            if not (night or lease_due):
                return
            printer = self._manager.get_printer(name) if self._manager else None
            reported = getattr(printer, "reported_light", None)
            if printer is None or not callable(reported) or not reliability.health(name).get("writable"):
                return  # stale, disconnected or starting: nothing is sent
            # None: not reported on this connection; "unknown": unrecognised mode. Neither is off.
            mode, seen = reported() or ("unknown", None)
            rt = self._rt(name)
            # A lease closes only on an OFF report newer than its own OFF: the OFF cached from before
            # its ON (the printer need not report the ON) proves nothing.
            off_proof = rt["last_off"] is not None and seen is not None and seen > rt["last_off"]
            if mode == "off" and (off_proof or not lease_due):
                rt["off_sends"] = 0
                if lease_due:
                    self._save(name, policy, None)
                    log.info("night_light: %s light reported off after the lease's OFF; lease closed", name)
                return
            if lease_due and rt["last_off"] is None:
                pass  # the lease's own OFF, whatever the (possibly lagging) report says
            elif mode != "on" and not lease_due:
                return  # quiet hours act only on a light reported on
            elif rt["last_off"] is not None:
                if not lease_due and seen <= rt["last_off"]:
                    return  # no report since our last OFF; wait for one
                # Repeats: a newer ON report, or a lease with no OFF report since its OFF.
                wait = reliability._bounded_backoff(OFF_BACKOFF_MIN_S, OFF_BACKOFF_MAX_S,
                                                    max(0, rt["off_sends"] - 1))
                if rt["off_sends"] and time.monotonic() - rt["last_off"] < wait:
                    return
            reason = "quiet hours" if night and mode == "on" else "inspection lease ended"
            rt["last_off"] = time.monotonic()
            rt["off_sends"] += 1
            try:
                printer.light_state = False
                result = "sent"
            except reliability.PublishRejected as exc:
                result = f"rejected: {exc}"
            except reliability.PublishUnconfirmed as exc:
                result = f"unknown: {exc}"
            except Exception as exc:
                result = f"error: {type(exc).__name__}: {exc}"
            rt["last_action"] = {"at": _iso(now, policy), "command": "off", "reason": reason, "result": result}
            log.info("night_light: %s light reported on (%s); OFF %s", name, reason, result)

    # ── tool API ─────────────────────────────────────────────────────────────

    def set_policy(self, name: str, enabled: bool, timezone: str, start: str, end: str) -> dict:
        if not _configured(name):
            raise ValueError(f"'{name}' is not a configured printer")
        with self._lock:
            old = self._record(name).get("policy") or {}
            if enabled:
                policy = validate_policy(enabled, timezone, start, end)
            else:
                # Disabling keeps the stored hours for the status; any given value must still be valid.
                policy = validate_policy(False, timezone or old.get("timezone", "UTC"),
                                         start or old.get("start", "22:00"), end or old.get("end", "07:00"))
            self._save(name, policy, self._record(name)["lease"])
            self._runtime.pop(name, None)
        self._wake.set()
        return self.status(name)

    def grant_lease(self, name: str, seconds: int) -> dict:
        """Switch a light reported OFF to ON until a persisted deadline; 0 ends a lease now."""
        if isinstance(seconds, bool) or not isinstance(seconds, int) or not 0 <= seconds <= LEASE_MAX_S:
            raise ValueError(f"seconds must be an integer from 0 to {LEASE_MAX_S}")
        with self._lock:
            rec = self._record(name)
            if rec.get("error"):
                raise ValueError(rec["error"])
            policy, lease = rec["policy"], rec["lease"]
            now = _wall_clock()
            if seconds == 0:
                if lease is None:
                    return {"ended": False, "detail": "no inspection lease to end; nothing sent"}
                self._save(name, policy, {**lease, "until": min(lease["until"], now)})
                self._wake.set()
                return {"ended": True, "detail": "the light is switched off once reported on; "
                                                 "no command was sent by this call"}
            if quiet_now(policy, now):
                return {"error": "quiet_hours", "sent": False,
                        "detail": f"quiet hours ({policy['start']}-{policy['end']} {policy['timezone']}) "
                                  "take precedence; nothing sent"}
            printer = self._manager.get_printer(name) if self._manager else None
            if printer is None:
                return {"error": "not_connected", "sent": False, "detail": f"Printer '{name}' not connected"}
            obs = printer.reported_light()
            if obs is None or obs[0] not in ("on", "off"):
                return {"error": "light_unknown", "sent": False,
                        "detail": "the printer has not reported its light as on or off on this "
                                  "connection; nothing sent"}
            until = now + seconds
            if policy and policy["enabled"]:
                until = min(until, _next_local(policy, policy["start"], now))
            held = lease is not None  # running, or past its deadline and not yet resolved: ours
            if obs[0] == "on" and not held:
                return {"lease": None, "sent": False, "light": "on",
                        "detail": "the light is already on; no lease, and it will not be switched off"}
            new = {"until": max(until, lease["until"]) if held else until,
                   "granted_at": lease["granted_at"] if held else now, "restore": "off"}
            self._save(name, policy, new)  # the deadline is durable before anything is sent
            result = {"lease_until": _iso(new["until"], policy), "seconds": round(new["until"] - now),
                      "restore": "off at the deadline, also after a restart or failed capture",
                      "sent": False}
            # Earlier OFFs are not this lease's: its deadline sends its own and waits for a newer report.
            self._rt(name).update(off_sends=0, last_off=None)
            if obs[0] != "on":
                # A failure propagates to the tool guard, which reports what was sent. The lease
                # stays either way: its OFF is idempotent, and it closes only once the light is
                # reported off after that OFF.
                printer.light_state = True
                result["sent"] = True
            return result

    def status(self, name: str) -> dict:
        import reliability

        with self._lock:
            rec = self._record(name)
            policy, lease = rec["policy"], rec["lease"]
            now = _wall_clock()
            printer = self._manager.get_printer(name) if self._manager else None
            reported = getattr(printer, "reported_light", None)
            obs = reported() if callable(reported) else None
            h = reliability.health(name)
            out = {
                "policy": policy,
                "quiet_now": quiet_now(policy, now),
                "local_time": _iso(now, policy),
                "next_quiet_start": _iso(_next_local(policy, policy["start"], now), policy)
                if policy and policy["enabled"] else None,
                "next_quiet_end": _iso(_next_local(policy, policy["end"], now), policy)
                if policy and policy["enabled"] else None,
                "light": {"reported": obs[0] if obs else "unknown",
                          "age_s": round(time.monotonic() - obs[1], 1) if obs else None},
                "telemetry": {"status": h.get("status"), "writable": bool(h.get("writable"))},
                "lease": None if lease is None else {
                    "until": _iso(lease["until"], policy), "due": now >= lease["until"], "restore": "off"},
                "last_action": self._rt(name)["last_action"],
                "off_commands_since_reported_off": self._rt(name)["off_sends"],
            }
            if rec.get("error"):
                out["error"] = rec["error"]
            return out


controller = _Controller()
