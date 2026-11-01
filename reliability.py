"""
reliability.py — Telemetry freshness, last-mile write protection and session supervision.

Why this exists (incident 2026-11-01 22:44:52, bambu-mcp.log): bpm parses every MQTT message on
paho's network thread. At a job start, bpm ``_on_message`` (bambuprinter.py:2649) listed the SD
card over FTPS from that thread. The P1S reset the FTPS handshake, the ConnectionResetError
escaped paho's on_message, ``loop_forever`` died and bpm set QUIT. Its watchdog exited, the
socket was never closed (CLOSE-WAIT), ``recent_update`` stayed True, and printer_state kept the
pre-job IDLE frame forever. Tools served that frame as current, and nothing restarted the
session. A write sent then would have been written into a dead socket and reported as success.

What this module adds, without editing bpm (``ReliablePrinter`` subclasses ``BambuPrinter``):

* The MQTT thread only parses. FTPS that bpm would run inside a message callback fails fast
  there; a bounded per-printer worker does the job's project lookup afterwards. ``on_update``
  fan-out (notifications, data collector, job monitor) is coalesced onto that worker.
  An exception while parsing a message is caught, counted and marks state incomplete. It
  never kills the loop.
* Monotonic receive/processed timestamps per session. A new session or reconnect is
  "starting" until gcode_state, bed and nozzle values have been processed again. ``health()``
  derives one status from those facts and never trusts bpm's ``recent_update`` or
  ``service_state`` alone.
* Last-mile publish guard. A request other than version/pushall is refused unless the loop
  thread is alive, paho is connected and telemetry is live. The paho return code is checked,
  and completion of the socket write is bounded. Nothing is retried or queued for later:
  paho 2.1 drops unsent QoS 0 packets on reconnect, and a refused write raises.
* Supervisor thread. It restarts a dead session with capped exponential backoff, requests
  pushall (read-only) when telemetry is stale or incomplete, forces a reconnect when a live
  socket stays silent, and never starts a replacement while the old loop thread is alive.
  If that thread cannot be stopped, writes stay blocked. In daemon mode (or with
  BAMBU_MCP_WATCHDOG_EXIT=1) the process exits with code 70 so systemd restarts it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time

from bpm.bambucommands import ANNOUNCE_PUSH, ANNOUNCE_VERSION
from bpm.bambuprinter import BambuPrinter
from bpm.bambuproject import get_3mf_entry_by_name, get_project_info
from bpm.bambutools import ServiceState

log = logging.getLogger(__name__)

# Thresholds (seconds). Module-level so offline tests can shrink them.
STALE_AFTER_S = 45.0          # no processed print telemetry for this long → stale (bpm asks pushall at 30)
PUSHALL_EVERY_S = 30.0        # min gap between supervisor pushall requests while stale/incomplete
FORCE_RECONNECT_AFTER_S = 120.0  # connected but no usable telemetry this long → disconnect so the session restarts
STUCK_CALLBACK_S = 60.0       # one message being processed this long → loop thread stuck
STUCK_EXIT_AFTER_S = 120.0    # stuck this long (and exit allowed) → process exit for supervised restart
QUIT_TIMEOUT_S = 15.0         # bpm quit() joins threads with no timeout; we bound it
PUBLISH_TIMEOUT_S = 5.0       # bound on a write reaching the socket
BACKOFF_MIN_S = 2.0
BACKOFF_MAX_S = 60.0
BACKOFF_RESET_AFTER_LIVE_S = 120.0
SUPERVISOR_TICK_S = 1.0
SUPERVISOR_DEAD_AFTER_S = 15.0
PROJECT_LOOKUP_ATTEMPTS = 3
EXIT_CODE_STUCK = 70

_ACTIVE_JOB_STATES = ("PREPARE", "RUNNING", "PAUSE")
_READ_ONLY_REQUESTS = (
    json.dumps(ANNOUNCE_PUSH, sort_keys=True),
    json.dumps(ANNOUNCE_VERSION, sort_keys=True),
)


class PublishRejected(RuntimeError):
    """A printer write was refused before anything was sent."""


class PublishUnconfirmed(RuntimeError):
    """A printer write was handed to paho but not confirmed written to the socket. Not retried."""


class DeferredFtps(OSError):
    """FTPS requested from the MQTT thread; deferred to the worker instead of blocking parsing."""


_tls = threading.local()


def _in_mqtt_callback() -> bool:
    return bool(getattr(_tls, "in_message", False))


class capture_publishes:
    """Collect the outcomes of printer writes made by this thread (used by the tool wrapper)."""

    def __enter__(self):
        self.outcomes: list[dict] = []
        self._prev = getattr(_tls, "outcomes", None)
        _tls.outcomes = self.outcomes
        return self.outcomes

    def __exit__(self, *exc):
        _tls.outcomes = self._prev
        return False


def _record_outcome(outcome: dict) -> None:
    outcomes = getattr(_tls, "outcomes", None)
    if outcomes is not None:
        outcomes.append(outcome)


def is_read_only_request(payload) -> bool:
    """True only for bpm's version and pushall requests; any other payload is a printer write."""
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        return json.dumps(json.loads(payload), sort_keys=True) in _READ_ONLY_REQUESTS
    except Exception:
        return False


class _NotSent:
    """A paho-like MQTTMessageInfo for a read request skipped because there is no connection."""

    rc = 4  # MQTT_ERR_NO_CONN

    def is_published(self) -> bool:
        return False

    def wait_for_publish(self, timeout=None) -> None:
        return None


class _GuardedClient:
    """Proxy for the paho client bpm sees as ``self.client``; everything but publish delegates."""

    def __init__(self, printer: "ReliablePrinter", real):
        object.__setattr__(self, "_printer", printer)
        object.__setattr__(self, "_real", real)

    def __bool__(self) -> bool:
        return True

    def __getattr__(self, attr):
        return getattr(self._real, attr)

    def __setattr__(self, attr, value):
        if attr == "on_connect" and value is not None:
            value = self._printer._wrap_on_connect(value)
        elif attr == "on_disconnect" and value is not None:
            value = self._printer._wrap_on_disconnect(value)
        setattr(self._real, attr, value)

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        p = self._printer
        real = self._real
        if is_read_only_request(payload):
            if real is not p._client or not real.is_connected():
                return _NotSent()
            return real.publish(topic, payload, qos, retain, properties)
        # A printer write: refuse unless this exact session is demonstrably live.
        reason = p._write_refusal(real)
        if reason:
            _record_outcome({"status": "rejected", "reason": reason})
            raise PublishRejected(f"printer write not sent: {reason}")
        try:
            info = real.publish(topic, payload, qos, retain, properties)
        except Exception as exc:
            # Once the transport was invoked an exception need not mean zero bytes
            # reached the peer. Never tell a caller it is safe to retry automatically.
            _record_outcome({"status": "unconfirmed", "reason": f"publish raised {type(exc).__name__}: {exc}"})
            raise PublishUnconfirmed(f"printer write outcome unknown ({exc}); not retried") from None
        rc = int(getattr(info, "rc", 0) or 0)
        if rc != 0:
            if rc == 4:  # paho MQTT_ERR_NO_CONN: no socket for this publish
                _record_outcome({"status": "rejected", "reason": f"paho rc={rc}"})
                raise PublishRejected(f"printer write not sent: paho rc={rc}")
            # MQTT_ERR_CONN_LOST can follow a partial socket write in _packet_write.
            _record_outcome({"status": "unconfirmed", "reason": f"paho rc={rc}"})
            raise PublishUnconfirmed(f"printer write outcome unknown: paho rc={rc}; not retried")
        try:
            info.wait_for_publish(timeout=PUBLISH_TIMEOUT_S)
        except Exception as exc:
            _record_outcome({"status": "unconfirmed", "reason": str(exc)})
            raise PublishUnconfirmed(f"printer write outcome unknown ({exc}); not retried") from None
        if not info.is_published() or int(getattr(info, "rc", 0) or 0) != 0:
            _record_outcome({"status": "unconfirmed", "reason": "not written within bound"})
            raise PublishUnconfirmed(
                f"printer write not confirmed written within {PUBLISH_TIMEOUT_S:g}s; outcome unknown; not retried"
            )
        _record_outcome({"status": "sent"})
        return info


class ReliablePrinter(BambuPrinter):
    """BambuPrinter with fault-isolated parsing, freshness tracking and a guarded publish path."""

    def __init__(self, config=None):
        super().__init__(config)
        self._rel_lock = threading.Lock()
        self._rel_reset(generation=0)
        self._rel_worker_thread: threading.Thread | None = None
        self._rel_worker_event = threading.Event()
        self._rel_worker_stop = threading.Event()
        self._rel_update_pending = False
        self._rel_project_pending = False
        self._rel_project_key = None
        self._rel_project_attempts = 0
        self._rel_project_next = 0.0
        self._rel_proxy = None
        self._rel_quit_ok = True

    # ── tracking ────────────────────────────────────────────────────────────

    def _rel_reset(self, generation: int) -> None:
        self._rel_generation = generation
        self._rel_session_started = time.monotonic()
        self._rel_connected_at: float | None = None
        self._rel_disconnected_at: float | None = None
        self._rel_last_received: float | None = None
        self._rel_last_telemetry: float | None = None
        # Last usable telemetry frame processed while state was complete. Recovery keys off this,
        # not _rel_last_received: a flood of invalid or non-telemetry frames is not a working session.
        self._rel_last_usable: float | None = None
        self._rel_baseline_at: float | None = None
        self._rel_covered: set[str] = set()
        self._rel_in_message_since: float | None = None
        self._rel_received = 0
        self._rel_processed = 0
        self._rel_failed = 0
        self._rel_last_error: str | None = None
        self._rel_last_pushall_req = 0.0

    def _rel_mark_incomplete(self, why: str) -> None:
        with self._rel_lock:
            self._rel_covered = set()
            self._rel_baseline_at = None
            self._rel_last_error = why

    def _rel_note_coverage(self, status: dict) -> None:
        keys = set()
        if "gcode_state" in status:
            keys.add("gcode_state")
        if "bed_temper" in status:
            keys.add("bed")
        if "nozzle_temper" in status or "device" in status:
            keys.add("nozzle")
        with self._rel_lock:
            self._rel_covered |= keys
            if self._rel_baseline_at is None and {"gcode_state", "bed", "nozzle"} <= self._rel_covered:
                self._rel_baseline_at = time.monotonic()

    # ── MQTT thread ──────────────────────────────────────────────────────────

    def _on_message(self, msg: str):
        now = time.monotonic()
        with self._rel_lock:
            self._rel_received += 1
            self._rel_last_received = now
            self._rel_in_message_since = now
        _tls.in_message = True
        try:
            super()._on_message(msg)
        except Exception as exc:  # never let a parse/side-effect failure kill paho's loop
            with self._rel_lock:
                self._rel_failed += 1
            self._rel_mark_incomplete(f"message processing failed: {type(exc).__name__}: {exc}")
            log.warning("ReliablePrinter: message processing failed (%s: %s); state marked incomplete",
                        type(exc).__name__, exc)
            return
        finally:
            _tls.in_message = False
            with self._rel_lock:
                self._rel_in_message_since = None
        try:
            status = json.loads(msg).get("print")
        except Exception:
            status = None
        # Usable telemetry: a status push or a frame carrying state keys, not a command reply.
        usable = isinstance(status, dict) and (status.get("command") == "push_status"
                                               or bool({"gcode_state", "bed_temper", "nozzle_temper"} & status.keys()))
        with self._rel_lock:
            self._rel_processed += 1
            if usable:
                self._rel_last_telemetry = time.monotonic()
        if usable:
            self._rel_note_coverage(status)
            with self._rel_lock:
                if self._rel_baseline_at is not None:
                    self._rel_last_usable = self._rel_last_telemetry
        if self._rel_project_pending:
            self._rel_worker_event.set()

    def _notify_update(self):
        # Coalesced: the worker delivers at most one pending on_update at a time.
        self._rel_update_pending = True
        self._rel_worker_event.set()
        if self._rel_worker_thread is None or not self._rel_worker_thread.is_alive():
            # No worker (session not started or already quit): deliver inline, as bpm would.
            self._rel_update_pending = False
            super()._notify_update()

    # FTPS from inside a message callback fails fast; the worker repeats the lookup.
    def ftp_connection(self):
        if _in_mqtt_callback():
            self._rel_project_pending = True
            raise DeferredFtps("FTPS deferred off the MQTT thread")
        return super().ftp_connection()

    def get_sdcard_contents(self):
        if _in_mqtt_callback():
            self._rel_project_pending = True
            return None
        return super().get_sdcard_contents()

    def get_sdcard_3mf_files(self):
        if _in_mqtt_callback():
            self._rel_project_pending = True
            return None
        return super().get_sdcard_3mf_files()

    def _wrap_on_connect(self, original):
        def on_connect(client, userdata, flags, reason_code, properties):
            original(client, userdata, flags, reason_code, properties)
            if getattr(reason_code, "is_failure", False):
                return
            with self._rel_lock:
                self._rel_connected_at = time.monotonic()
                self._rel_disconnected_at = None
                self._rel_covered = set()
                self._rel_baseline_at = None
            if self.service_state != ServiceState.PAUSED:
                self._request_pushall(client)
        return on_connect

    def _wrap_on_disconnect(self, original):
        def on_disconnect(client, userdata, flags, reason_code, properties):
            with self._rel_lock:
                self._rel_connected_at = None
                self._rel_disconnected_at = time.monotonic()
                self._rel_covered = set()
                self._rel_baseline_at = None
                self._rel_last_error = f"mqtt disconnected: {reason_code}"
            original(client, userdata, flags, reason_code, properties)
        return on_disconnect

    def _request_pushall(self, client=None) -> bool:
        """Read-only full-state request. Safe to repeat; never a printer write."""
        client = client or self._client
        if client is None:
            return False
        try:
            if not client.is_connected():
                return False
            self._rel_last_pushall_req = time.monotonic()
            client.publish(f"device/{self.config.serial_number}/request", json.dumps(ANNOUNCE_PUSH))
            return True
        except Exception as exc:
            log.debug("ReliablePrinter: pushall request failed: %s", exc)
            return False

    # ── paho client proxy ────────────────────────────────────────────────────

    @property
    def client(self):
        real = self._client
        if real is None:
            return BambuPrinter.client.fget(self)
        proxy = self._rel_proxy
        if proxy is None or object.__getattribute__(proxy, "_real") is not real:
            proxy = self._rel_proxy = _GuardedClient(self, real)
        return proxy

    @client.setter
    def client(self, value):
        if isinstance(value, _GuardedClient):
            value = object.__getattribute__(value, "_real")
        self._client = value
        self._rel_proxy = None

    def _write_refusal(self, real) -> str | None:
        # Every printer write reaches here, whether from a guarded tool, a REST route or internal
        # code, so the supervisor requirement health() applies to tools holds for all of them.
        if real is not self._client:
            return "not the current session's client"
        if not supervisor.alive():
            return "session supervisor is not running (nothing would recover a failed session)"
        if self.service_state == ServiceState.PAUSED:
            return "session paused"
        thread = self._mqtt_client_thread
        if thread is None or not thread.is_alive():
            return "MQTT loop thread is not running"
        if not real.is_connected():
            return "MQTT not connected"
        h = self.health_snapshot()
        if not h["live"]:
            return f"telemetry {h['status']}: {h['reason']}"
        return None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def _close_dead_socket(self) -> None:
        """Close the socket of a client whose loop thread died (it never closes it itself)."""
        thread = self._mqtt_client_thread
        if self._client is not None and thread is not None and thread.ident is not None and not thread.is_alive():
            sock_close = getattr(self._client, "_sock_close", None)
            if callable(sock_close):
                try:
                    sock_close()
                except Exception:
                    pass

    def start_session(self):
        # Restarting this same object (session_manager.resume_session) must not leak the old socket.
        self._close_dead_socket()
        self._rel_reset(generation=self._rel_generation + 1)
        self._rel_quit_ok = True
        self._rel_worker_stop.clear()
        if self._rel_worker_thread is None or not self._rel_worker_thread.is_alive():
            self._rel_worker_thread = threading.Thread(
                target=self._rel_worker, name=f"bambu-callbacks-{self.config.serial_number[-4:]}", daemon=True)
            self._rel_worker_thread.start()
        super().start_session()
        real = self._client
        if real is not None:
            try:
                # paho backs off exponentially between these bounds (bpm pins 1..1).
                real.reconnect_delay_set(min_delay=1, max_delay=30)
            except Exception:
                pass

    def quit(self, timeout: float | None = None) -> bool:
        """bpm quit() bounded. Returns False when bpm's threads did not stop in time."""
        timeout = QUIT_TIMEOUT_S if timeout is None else timeout
        done = threading.Event()

        def _run():
            try:
                BambuPrinter.quit(self)
            except Exception as exc:
                log.warning("ReliablePrinter.quit: %s", exc)
            finally:
                done.set()

        threading.Thread(target=_run, name="bambu-quit", daemon=True).start()
        ok = done.wait(timeout)
        self._rel_worker_stop.set()
        self._rel_worker_event.set()
        thread = self._mqtt_client_thread
        # A loop that died on an exception never closed its socket (the CLOSE-WAIT leak).
        self._close_dead_socket()
        self._rel_quit_ok = ok and not (thread is not None and thread.is_alive())
        if not self._rel_quit_ok:
            log.error("ReliablePrinter.quit: MQTT threads did not stop within %.0fs", timeout)
        return self._rel_quit_ok

    # ── worker: on_update fan-out and deferred project lookup ────────────────

    def _rel_worker(self) -> None:
        while True:
            self._rel_worker_event.wait(timeout=5.0)
            self._rel_worker_event.clear()
            if self._rel_update_pending:  # delivered even when stopping: quit()'s QUIT notification
                self._rel_update_pending = False
                try:
                    BambuPrinter._notify_update(self)
                except Exception as exc:
                    log.warning("ReliablePrinter: on_update callback failed: %s", exc)
            if self._rel_worker_stop.is_set():
                break
            if self._rel_project_pending:
                try:
                    self._deferred_project_lookup()
                except Exception as exc:
                    log.warning("ReliablePrinter: deferred project lookup failed: %s", exc)

    def _deferred_project_lookup(self) -> None:
        info = self._active_job_info
        if (self.printer_state.gcode_state or "") not in _ACTIVE_JOB_STATES:
            self._rel_project_pending = False
            return
        if info.project_info and info.project_info.id:
            self._rel_project_pending = False
            return
        key = (self._task_id, info.subtask_name, info.gcode_file)
        if key != self._rel_project_key:
            self._rel_project_key, self._rel_project_attempts, self._rel_project_next = key, 0, 0.0
        if self._rel_project_attempts >= PROJECT_LOOKUP_ATTEMPTS:
            self._rel_project_pending = False
            return
        if time.monotonic() < self._rel_project_next:
            return
        self._rel_project_attempts += 1
        self._rel_project_next = time.monotonic() + 20.0 * self._rel_project_attempts
        match = re.search(r"plate_(\d{1,2})", info.gcode_file or "")
        plate = int(match.group(1)) if match else (info.plate_num if info.plate_num > 0 else 1)
        url = (info.project_file_command or {}).get("print", {}).get("url", "")
        subtask = info.subtask_name or ""
        names = [n for n in (url.rsplit("/", 1)[-1] if "://" in url else "",
                             subtask if subtask.endswith(".3mf") else "",
                             f"{subtask}.gcode.3mf" if subtask else "",
                             f"{subtask}.3mf" if subtask else "")
                 if n and n != "project_file.gcode.3mf"]
        remote = self.get_sdcard_3mf_files()
        entry = None
        for n in names if remote else []:
            entry = get_3mf_entry_by_name(remote, n)
            if entry:
                break
        if not entry:
            log.info("deferred project lookup: no SD entry for %s (attempt %d)", names, self._rel_project_attempts)
            return
        project = get_project_info(entry["id"], self, plate_num=plate)
        if project and project.id and key == (self._task_id, info.subtask_name, info.gcode_file):
            with self._state_lock:
                info.project_info = project
                info.project_info_fetch_attempted = True
            self._persist_job_record(project.id, project.md5 or None, project.plate_num)
            self._rel_project_pending = False

    # ── health ───────────────────────────────────────────────────────────────

    def session_dead(self) -> bool:
        """True once bpm gave up (QUIT) or its loop thread ran and exited. A session that is still
        inside start_session (registered, no thread yet) is not dead and must not be replaced."""
        thread = self._mqtt_client_thread
        if thread is not None and thread.ident is not None:
            return not thread.is_alive()
        # No thread yet, or assigned but not yet started (bpm assigns before .start()).
        return self.service_state == ServiceState.QUIT

    def health_snapshot(self) -> dict:
        now = time.monotonic()

        def age(t):
            return None if t is None else round(now - t, 1)

        with self._rel_lock:
            connected_at = self._rel_connected_at
            last_tel = self._rel_last_telemetry
            baseline = self._rel_baseline_at
            in_msg = self._rel_in_message_since
            snap = {
                "age_s": {
                    "last_received": age(self._rel_last_received),
                    "last_telemetry": age(last_tel),
                    "last_usable": age(self._rel_last_usable),
                    "baseline": age(baseline),
                    "connected_for": age(connected_at),
                    "in_callback_for": age(in_msg),
                },
                "messages": {"received": self._rel_received, "processed": self._rel_processed,
                             "failed": self._rel_failed},
                "last_error": self._rel_last_error,
                "session_generation": self._rel_generation,
            }
        thread = self._mqtt_client_thread
        alive = bool(thread and thread.is_alive())
        real = self._client
        try:
            mqtt_connected = bool(real is not None and real.is_connected())
        except Exception:
            mqtt_connected = False
        state = self.service_state
        snap.update({"session_thread_alive": alive, "mqtt_connected": mqtt_connected,
                     "service_state": getattr(state, "name", str(state))})

        if not self._rel_quit_ok:
            status, reason = "stuck", "previous MQTT threads did not stop"
        elif in_msg is not None and now - in_msg > STUCK_CALLBACK_S:
            status, reason = "stuck", f"one message has been processing for {now - in_msg:.0f}s"
        elif state == ServiceState.PAUSED:
            status, reason = "paused", "MQTT session paused by operator"
        elif not alive and not self.session_dead():
            status, reason = "starting", "session starting; state unknown"
        elif not alive:
            status, reason = "disconnected", self._rel_last_error or "MQTT loop thread is not running"
        elif not mqtt_connected or connected_at is None:
            if self._rel_received == 0 and self._rel_disconnected_at is None:
                status, reason = "starting", "connecting; state unknown"
            else:
                status, reason = "disconnected", "MQTT reconnecting"
        elif baseline is None:
            if self._rel_failed and self._rel_last_error and "processing failed" in self._rel_last_error:
                status, reason = "incomplete", self._rel_last_error
            else:
                status, reason = "starting", "waiting for a full state report since (re)connect"
        elif last_tel is None or now - last_tel > STALE_AFTER_S:
            status, reason = "stale", f"no telemetry processed for {age(last_tel)}s"
        else:
            status, reason = "live", "telemetry current"
        snap.update({"status": status, "reason": reason, "live": status == "live"})
        return snap


# ── Supervisor ───────────────────────────────────────────────────────────────

def _exit_allowed() -> bool:
    env = os.environ.get("BAMBU_MCP_WATCHDOG_EXIT")
    if env is not None:
        return env == "1"
    try:
        import daemon_port
        return daemon_port.is_daemon_mode()
    except Exception:
        return False


def _hard_exit(code: int) -> None:  # replaced in tests
    logging.shutdown()
    os._exit(code)


class _Supervisor:
    def __init__(self):
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._manager = None
        self._wanted: set[str] = set()
        self._info: dict[str, dict] = {}
        # name -> [(printer, since)] whose quit() timed out: their loop threads may still hold a
        # connection, so no replacement may start and writes stay blocked until they are gone.
        self._zombies: dict[str, list] = {}
        self.heartbeat: float | None = None
        self.started = False

    # wiring
    def start(self, manager) -> None:
        with self._lock:
            self._manager = manager
            self._stop.clear()
            self.started = True
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, name="bambu-supervisor", daemon=True)
                self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)

    def want(self, name: str) -> None:
        with self._lock:
            self._wanted.add(name)

    def unwant(self, name: str) -> None:
        with self._lock:
            self._wanted.discard(name)
            self._info.pop(name, None)

    def alive(self) -> bool:
        if not self.started:
            return False
        hb = self.heartbeat
        return (self._thread is not None and self._thread.is_alive() and hb is not None
                and time.monotonic() - hb < SUPERVISOR_DEAD_AFTER_S)

    def info(self, name: str) -> dict:
        with self._lock:
            return dict(self._info.get(name, {}))

    def add_zombie(self, name: str, printer) -> None:
        with self._lock:
            self._zombies.setdefault(name, []).append((printer, time.monotonic()))
        log.error("supervisor: %s previous MQTT session threads survived quit(); writes and restarts blocked", name)

    def zombie_since(self, name: str) -> float | None:
        """When the oldest still-running abandoned session of ``name`` was abandoned, else None."""
        with self._lock:
            live = [(p, t) for p, t in self._zombies.get(name, [])
                    if getattr(p, "_mqtt_client_thread", None) is not None and p._mqtt_client_thread.is_alive()]
            if live:
                self._zombies[name] = live
            else:
                self._zombies.pop(name, None)
            return min(t for _, t in live) if live else None

    # loop
    def _run(self) -> None:
        while not self._stop.is_set():
            self.heartbeat = time.monotonic()
            try:
                self.tick()
            except Exception:
                log.exception("supervisor: tick failed")
            self._stop.wait(SUPERVISOR_TICK_S)

    def tick(self) -> None:
        manager = self._manager
        if manager is None or self._stop.is_set():
            return
        with self._lock:
            wanted = set(self._wanted)
        with self._lock:
            names = sorted(wanted | set(self._zombies))
        for name in names:
            if self._stop.is_set():
                return
            try:
                since = self.zombie_since(name)
                if since is not None:
                    if _exit_allowed() and time.monotonic() - since > STUCK_EXIT_AFTER_S - STUCK_CALLBACK_S:
                        log.critical("supervisor: %s abandoned MQTT threads still alive; exiting %d for supervised restart",
                                     name, EXIT_CODE_STUCK)
                        _hard_exit(EXIT_CODE_STUCK)
                    continue
                if name in wanted:
                    self._check(manager, name)
            except Exception:
                log.exception("supervisor: check failed for %s", name)

    def _entry(self, name: str) -> dict:
        with self._lock:
            return self._info.setdefault(name, {"failures": 0, "restarts": 0, "next_restart": 0.0,
                                                "stuck_since": None, "reconnect_requested": None,
                                                "live_since": None, "last_restart_reason": None})

    def _check(self, manager, name: str) -> None:
        now = time.monotonic()
        e = self._entry(name)
        p = manager.get_printer(name)
        if p is None:
            # Wanted but unregistered: a start failed (credentials, construction, connect) or a
            # restart is pending. A start still in flight registers when it finishes; leave it be.
            in_flight = getattr(manager, "start_in_flight", None)
            if not (callable(in_flight) and in_flight(name)):
                self._maybe_restart(manager, name, e, now, "no session registered")
            return
        if not isinstance(p, ReliablePrinter):
            return
        h = p.health_snapshot()
        status = h["status"]
        if status == "live":
            e["stuck_since"] = None
            e["reconnect_requested"] = None
            if e["live_since"] is None:
                e["live_since"] = now
            elif e["failures"] and now - e["live_since"] > BACKOFF_RESET_AFTER_LIVE_S:
                e["failures"] = 0
            return
        e["live_since"] = None
        if status == "paused":
            return
        if status == "stuck":
            if e["stuck_since"] is None:
                e["stuck_since"] = now
                log.error("supervisor: %s MQTT worker stuck (%s); writes blocked, no replacement session", name, h["reason"])
            if _exit_allowed() and now - e["stuck_since"] > STUCK_EXIT_AFTER_S - STUCK_CALLBACK_S:
                log.critical("supervisor: %s stuck worker cannot be stopped in-process; exiting %d for supervised restart",
                             name, EXIT_CODE_STUCK)
                _hard_exit(EXIT_CODE_STUCK)
            return
        if not h["session_thread_alive"]:
            if p.session_dead():
                self._maybe_restart(manager, name, e, now, h["reason"])
            return
        if status == "disconnected":
            return  # paho is reconnecting on its own (bounded 1..30s backoff)
        # connected: starting / incomplete / stale
        if now - p._rel_last_pushall_req > PUSHALL_EVERY_S:
            p._request_pushall()
        # Quiet = no usable, complete telemetry this connection. Received-but-useless frames
        # (malformed, partial, command replies) do not count, or a flood would hold it off forever.
        usable_for = h["age_s"]["last_usable"]
        connected_for = h["age_s"]["connected_for"] or 0.0
        silent = connected_for if usable_for is None else min(usable_for, connected_for)
        if silent > FORCE_RECONNECT_AFTER_S and connected_for > FORCE_RECONNECT_AFTER_S:
            requested = e["reconnect_requested"]
            if requested is None or requested[0] is not p:  # per session, never inherited
                e["reconnect_requested"] = (p, now)
                log.warning("supervisor: %s connected without usable telemetry for %.0fs; dropping the connection",
                            name, silent)
                try:
                    p._client.disconnect()  # loop_forever returns; the dead thread is then restarted
                except Exception as exc:
                    log.warning("supervisor: disconnect failed for %s: %s", name, exc)
            elif now - requested[1] > QUIT_TIMEOUT_S:
                # This session's loop did not exit after disconnect: treat as stuck.
                p._rel_quit_ok = False

    def _maybe_restart(self, manager, name: str, e: dict, now: float, reason: str) -> None:
        if now < e["next_restart"]:
            return
        old = manager.get_printer(name)
        if old is not None and not (getattr(old, "session_dead", None) and old.session_dead()):
            return  # never start a replacement while the old loop thread survives or is starting
        delay = min(BACKOFF_MAX_S, BACKOFF_MIN_S * (2 ** e["failures"]))
        e["failures"] += 1
        e["next_restart"] = now + delay
        log.warning("supervisor: restarting session for %s (%s); next attempt no sooner than %.0fs", name, reason, delay)
        try:
            # replace_live=False: rechecked under the per-name lock, so a session another caller
            # started meanwhile (or is starting) is kept, never replaced.
            started = manager._start_printer(name, replace_live=False)
        except Exception as exc:
            log.warning("supervisor: restart of %s failed: %s", name, exc)
            started = None
        if started is False:
            e["failures"] -= 1
            e["next_restart"] = 0.0
            return
        e["restarts"] += 1
        e["last_restart_reason"] = reason


supervisor = _Supervisor()


# ── Public API ───────────────────────────────────────────────────────────────

def health(name: str) -> dict:
    """Freshness and connection health for ``name``. Never raises, never does I/O."""
    try:
        manager = supervisor._manager
        if manager is None:
            from session_manager import session_manager as manager
        p = manager.get_printer(name)
    except Exception as exc:
        p, err = None, str(exc)
    else:
        err = None
    info = supervisor.info(name)
    base = {"name": name, "restarts": info.get("restarts", 0),
            "last_restart_reason": info.get("last_restart_reason"),
            "supervisor_alive": supervisor.alive()}
    nr = info.get("next_restart")
    base["next_restart_in_s"] = round(max(0.0, nr - time.monotonic()), 1) if nr else None
    zombie = supervisor.zombie_since(name)
    if zombie is not None:
        base.update({"status": "stuck", "live": False, "writable": False,
                     "reason": f"an abandoned MQTT session has not stopped for {time.monotonic() - zombie:.0f}s; "
                               "restart the bambu-mcp service"})
        return base
    if p is None:
        recovering = name in supervisor._wanted
        base.update({"status": "recovering" if recovering else "not_connected",
                     "reason": err or ("session restarting" if recovering else "no session (stopped or never started)"),
                     "live": False, "writable": False})
        return base
    if not isinstance(p, ReliablePrinter):
        base.update({"status": "unknown", "reason": "session without reliability tracking",
                     "live": False, "writable": False})
        return base
    try:
        snap = p.health_snapshot()
    except Exception as exc:
        base.update({"status": "unknown", "reason": f"health failed: {exc}", "live": False, "writable": False})
        return base
    base.update(snap)
    base["writable"] = bool(snap["live"] and base["supervisor_alive"])
    if snap["live"] and not base["supervisor_alive"]:
        base["reason"] = "telemetry current, but the session supervisor is not running"
    return base


def mutation_block(name: str) -> dict | None:
    """None when a printer write may be attempted; otherwise an error dict. Fails closed."""
    h = health(name)
    if h.get("writable"):
        return None
    return {
        "error": "telemetry_not_live",
        "detail": (f"Refused: printer '{name}' telemetry is {h.get('status')} ({h.get('reason')}). "
                   "No command was sent and none will be sent later. Check get_printer_connection_status, "
                   "the camera or the printer screen."),
        "telemetry": h,
    }
