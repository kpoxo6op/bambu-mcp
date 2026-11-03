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
  there (bpm then uses its metadata cache). ``on_update`` fan-out (notifications, data
  collector, job monitor) is coalesced onto a per-printer worker.
  An exception while parsing a message is caught, counted and marks state incomplete. It
  never kills the loop.
* No FTPS while a job runs. On 2026-11-03 the P1S, printing from SD, served our 3MF downloads
  (job monitor plate images, the job's project lookup) at about 1 KB/s for tens of minutes.
  While one ran it streamed no report and left most full-state requests unanswered, but still
  answered get_version: the MQTT link itself was fine (its TCP byte counts were the version
  replies alone). The first full report after a service restart came before any download began;
  reports stopped once one did. With the same process and code, the previous evening's print
  reported throughout: its lookups failed at once ("not found") and nothing was downloaded.
  ftp_connection(), which every bpm SD-card operation goes through, is refused unless the last
  reported gcode_state is idle (IDLE, FINISH, FAILED), so no caller can read or write the card
  during a print.
* Monotonic receive/processed timestamps per session. A new session or reconnect is
  "starting" until gcode_state, bed and nozzle values have been processed again. ``health()``
  derives one status from those facts and never trusts bpm's ``recent_update`` or
  ``service_state`` alone.
* paho's loop thread alone reads and writes the TLS socket (_loop_owns_socket_io). bpm runs
  loop_forever on its own thread, so publish() used to write from the calling thread; with a large
  write pending and other threads publishing, OpenSSL refused the retry ("bad length") and the link
  dropped with writes in flight (E2E, CI 2026-11-03).
* Last-mile publish guard. A request other than version/pushall is refused unless the loop
  thread is alive, paho is connected and telemetry is live. The paho return code is checked,
  and completion of the socket write is bounded. Nothing is retried or queued for later:
  paho 2.1 drops unsent QoS 0 packets on reconnect, and a refused write raises.
  Every write first passes print_admission.publish_refusal(): a print start needs the one-shot
  ticket of an admitted print_file call, and file starts, SD-card or extruding G-code are
  refused whichever tool, route or internal caller published them.
* Supervisor thread. It restarts a dead session with capped exponential backoff, requests
  full state (read-only, backing off while unanswered) when telemetry is stale or incomplete,
  forces a reconnect when a connected socket delivers nothing well-formed, and never starts a
  replacement while the old loop thread is alive.
* A printer that answers requests but sends no status report is not a dead link (incident
  2026-11-03, live P1S printing from SD): MQTT stayed connected and get_version was answered, but
  no report arrived and full-state requests were ignored. The supervisor then dropped the
  connection 120 s after every connect, 39 times; paho labels that client-side disconnect()
  "Normal disconnection", and no new connection brought a report. Such a connection is now
  replaced only on a doubling schedule, and health reports the printer as answering but not reporting.
* Requests that keep reports flowing follow the vendor client (Bambu Studio DeviceCore/DevManager.cpp
  keep_alive/check_pushing): a stream request after PUSH_START_AFTER_S without a status report,
  repeated at most every PUSH_START_EVERY_S, on its own timer; a full-state request at a fixed
  PUSHALL_EVERY_S while telemetry is not live (bpm's own watchdog cadence) and every
  PUSHALL_KEEPALIVE_S while it is. On 2026-11-03 the stream request rode on the supervisor's
  full-state request only, and bpm's watchdog, asking on the same cadence, usually took that
  shared slot first with a plain pushall, so after one full report the printer went quiet again.
  If that thread cannot be stopped, writes stay blocked. In daemon mode (or with
  BAMBU_MCP_WATCHDOG_EXIT=1) the process exits with code 70 so systemd restarts it.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

import paho.mqtt.client as paho_client
from bpm.bambucommands import ANNOUNCE_PUSH, ANNOUNCE_VERSION
from bpm.bambuprinter import BambuPrinter
from bpm.bambutools import ServiceState

import print_admission

log = logging.getLogger(__name__)

# Thresholds (seconds). Module-level so offline tests can shrink them.
STALE_AFTER_S = 45.0          # no processed print telemetry for this long → stale (bpm asks pushall at 30)
PUSHALL_EVERY_S = 30.0        # gap after an unanswered full-state request (bpm's watchdog cadence); version probe gap
PUSHALL_KEEPALIVE_S = 300.0   # full-state request while live, as Bambu Studio's keep_alive (TIMEOUT_FOR_KEEPALIVE)
PUSH_START_AFTER_S = 20.0     # no status report this long → stream request (Studio TIMEOUT_FOR_STRAT)
PUSH_START_EVERY_S = 15.0     # at most this often (Studio REQUEST_START_MIN_TIME)
FORCE_RECONNECT_AFTER_S = 120.0  # connected, no usable telemetry and nothing well-formed received this long → disconnect
NO_REPORT_RECONNECT_S = 300.0    # the printer answers but sends no usable report: first replacement after this long,
NO_REPORT_RECONNECT_MAX_S = 3600.0  # doubling per replacement (per printer, across sessions) up to this
STUCK_CALLBACK_S = 60.0       # one message being processed this long → loop thread stuck
STUCK_EXIT_AFTER_S = 120.0    # stuck this long (and exit allowed) → process exit for supervised restart
QUIT_TIMEOUT_S = 15.0         # bpm quit() joins threads with no timeout; we bound it
PUBLISH_TIMEOUT_S = 5.0       # bound on a write reaching the socket
BACKOFF_MIN_S = 2.0
BACKOFF_MAX_S = 60.0
BACKOFF_RESET_AFTER_LIVE_S = 120.0
SUPERVISOR_TICK_S = 1.0
SUPERVISOR_DEAD_AFTER_S = 15.0
EXIT_CODE_STUCK = 70

# The only gcode_states in which this server opens FTPS to the printer (see SdCardBusy).
_SD_IDLE_STATES = ("IDLE", "FINISH", "FAILED")
# Asks the printer to (re)start streaming status reports. Bambu Studio sends exactly this after 20 s
# without a message to printers whose profile lacks support_mqtt_alive (MachineObject::command_pushing,
# DevManager::check_pushing); ha-bambulab sends it after 60 s without data. It changes no printer state.
REQUEST_PUSH_START = {"pushing": {"command": "start", "sequence_id": "0"}}
_READ_ONLY_REQUESTS = (
    json.dumps(ANNOUNCE_PUSH, sort_keys=True),
    json.dumps(ANNOUNCE_VERSION, sort_keys=True),
    json.dumps(REQUEST_PUSH_START, sort_keys=True),
)
_FULL_STATE_REQUEST = json.dumps(ANNOUNCE_PUSH, sort_keys=True)


class PublishRejected(RuntimeError):
    """A printer write was refused before anything was sent."""


class PublishUnconfirmed(RuntimeError):
    """A printer write was handed to paho but not confirmed written to the socket. Not retried."""


class DeferredFtps(OSError):
    """FTPS requested from the MQTT thread; refused there instead of blocking parsing."""


class SdCardBusy(OSError):
    """FTPS refused: the printer last reported a job that may be running (2026-11-03)."""


_tls = threading.local()


def _sd_card_state(status: dict) -> tuple[int, str] | None:
    """SD card state as Bambu Studio decodes it (DevStorage::SdcardState: 0 none, 1 normal,
    2 abnormal, 3 read-only): print.aux bits 12-13, else home_flag bits 8-9, else the legacy
    sdcard bool. sdcard false always reads as no card."""
    if status.get("sdcard") is False:
        return 0, "sdcard"
    try:
        if isinstance(status.get("aux"), str) and status["aux"]:
            return (int(status["aux"], 16) >> 12) & 3, "aux"
        if "home_flag" in status:
            return (int(status["home_flag"]) >> 8) & 3, "home_flag"
    except (TypeError, ValueError):
        return None
    if status.get("sdcard") is True:
        return 1, "sdcard"
    return None


def _external_free_bytes(status: dict) -> int | None:
    """tl_external_free_kb (KiB, Bambu Studio DevStorage) from device.cam or ipcam, as bytes."""
    for section in ((status.get("device") or {}).get("cam"), status.get("ipcam")):
        value = section.get("tl_external_free_kb") if isinstance(section, dict) else None
        if type(value) is int and value >= 0:
            return value * 1024
    return None


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


def _canonical(payload) -> str | None:
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        return json.dumps(json.loads(payload), sort_keys=True)
    except Exception:
        return None


def _bounded_backoff(initial: float, cap: float, count: int) -> float:
    # Saturate before calculating another power: long outages must not overflow a float.
    value = min(initial, cap)
    while count > 0 and value < cap:
        value = min(cap, value * 2)
        count -= 1
    return value


def is_read_only_request(payload) -> bool:
    """True only for the version, pushall and push-start requests; any other payload is a printer write."""
    return _canonical(payload) in _READ_ONLY_REQUESTS


def _loop_owns_socket_io(client, userdata, sock) -> None:
    """paho on_socket_register_write: only the loop thread may touch the TLS socket.

    bpm runs paho's loop_forever on its own thread, so paho's _thread is None and publish() wrote
    the socket from whichever thread called it (paho _packet_queue -> loop_write), while the loop
    thread read and wrote the same SSL object. Tools, the supervisor's timers and bpm's watchdog
    publish from their own threads, and OpenSSL does not support one SSL object used from two
    threads at once: the peer received records with a bad MAC and paho dropped the link as
    "Unspecified error" (harness, 2026-11-03). With this callback set, publish() only queues the
    packet and wakes the loop (ReliablePrinter.client adds the wake-up pair), and loop_forever
    writes it when the socket is writable. Nothing is needed here; its presence is the switch.
    """


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
            # Installed with on_connect, before connect(): the SUBACK cannot arrive unobserved.
            self._real.on_subscribe = self._printer._on_subscribe
        elif attr == "on_disconnect" and value is not None:
            value = self._printer._wrap_on_disconnect(value)
        setattr(self._real, attr, value)

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):
        p = self._printer
        real = self._real
        if is_read_only_request(payload):
            if real is not p._client or not real.is_connected():
                return _NotSent()
            # bpm's watchdog and refresh() ask for full state too; all of them share one back-off.
            if _canonical(payload) == _FULL_STATE_REQUEST and not p._rel_full_state_slot():
                return _NotSent()
            return real.publish(topic, payload, qos, retain, properties)
        # A printer write. Admission policy first (it also consumes a print_file start ticket),
        # then refuse unless this exact session is demonstrably live.
        reason = print_admission.publish_refusal(payload) or p._write_refusal(real)
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
        # Why the latest connection attempt failed (refused, unreachable, not authorized).
        # Kept apart from last_error, which the disconnect that follows a refusal overwrites.
        self._rel_connect_error: str | None = None
        self._rel_last_pushall_req = 0.0
        self._rel_last_version_req = 0.0
        self._rel_last_start_req = 0.0
        self._rel_push_starts = 0
        # Last message parsed without error, of any kind (replies included): the printer is answering.
        self._rel_last_valid: float | None = None
        # Full-state requests on this connection, and how many were sent since the last full report.
        self._rel_full_requests = 0
        self._rel_full_unanswered = 0
        self._rel_last_full_report: float | None = None
        # "pending" until the report subscription's SUBACK, then "granted" or "refused (<code>)".
        self._rel_subscription: str | None = None
        # nozzle_diameter exactly as the printer reported it since this connection began; print
        # admission matches a job to it. Never carried across a reconnect (the nozzle may change).
        self._rel_nozzle: str | None = None
        # SD card state (code, field, monotonic time) and any firmware free-space figure (bytes),
        # as reported on this connection; never carried across a reconnect (the card may change).
        self._rel_sd: tuple[int, str, float] | None = None
        self._rel_ext_free: int | None = None

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
        sd, free = _sd_card_state(status), _external_free_bytes(status)
        with self._rel_lock:
            if "nozzle_diameter" in status:
                self._rel_nozzle = str(status["nozzle_diameter"])
            if sd is not None:
                self._rel_sd = (*sd, time.monotonic())
            if free is not None:
                self._rel_ext_free = free
            if {"gcode_state", "bed", "nozzle"} <= keys:  # one frame with the full state: a full report
                self._rel_full_unanswered = 0
                self._rel_last_full_report = time.monotonic()
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
            self._rel_last_valid = time.monotonic()
            if usable:
                self._rel_last_telemetry = time.monotonic()
        if usable:
            self._rel_note_coverage(status)
            with self._rel_lock:
                if self._rel_baseline_at is not None:
                    self._rel_last_usable = self._rel_last_telemetry

    def _notify_update(self):
        # Coalesced: the worker delivers at most one pending on_update at a time.
        self._rel_update_pending = True
        self._rel_worker_event.set()
        if self._rel_worker_thread is None or not self._rel_worker_thread.is_alive():
            # No worker (session not started or already quit): deliver inline, as bpm would.
            self._rel_update_pending = False
            super()._notify_update()

    def sd_card_refusal(self) -> str | None:
        """Why FTPS to this printer is refused now, or None when the last report says it is idle."""
        state = self.printer_state.gcode_state or ""
        if state in _SD_IDLE_STATES:
            return None
        shown = f"reports gcode_state {state}" if state else "has not reported its job state on this session"
        return (f"SD card access is refused while the printer {shown}: during a print from SD the P1S "
                "stopped sending status reports for as long as a file transfer ran (2026-11-03). "
                "Retry once it reports IDLE, FINISH or FAILED. Nothing was read or changed.")

    # Every bpm SD-card operation opens its FTPS session here. From inside a message callback it
    # fails fast (bpm falls back to its cache); during a job it is refused.
    def ftp_connection(self):
        if _in_mqtt_callback():
            raise DeferredFtps("FTPS refused on the MQTT thread")
        refusal = self.sd_card_refusal()
        if refusal:
            raise SdCardBusy(refusal)
        return super().ftp_connection()

    def get_sdcard_contents(self):
        if _in_mqtt_callback():
            return None
        return super().get_sdcard_contents()

    def get_sdcard_3mf_files(self):
        if _in_mqtt_callback():
            return None
        return super().get_sdcard_3mf_files()

    def _wrap_on_connect(self, original):
        def on_connect(client, userdata, flags, reason_code, properties):
            original(client, userdata, flags, reason_code, properties)
            if getattr(reason_code, "is_failure", False):
                # bpm labels the session CONNECTED even on a refused CONNACK; health does not.
                # Credentials are read when a session is built, so a changed code needs a restart.
                self._rel_set_connect_error(
                    f"printer refused the MQTT connection: {reason_code} (if the LAN access code "
                    "changed, update it in Bambu Studio, then restart bambu-mcp.service)")
                return
            with self._rel_lock:
                down_since = self._rel_disconnected_at
                self._rel_connect_error = None
                self._rel_connected_at = time.monotonic()
                self._rel_disconnected_at = None
                self._rel_covered = set()
                self._rel_baseline_at = None
                self._rel_nozzle = None
                self._rel_sd = self._rel_ext_free = None
                # A new connection may be answered where the last one was not.
                self._rel_full_unanswered = 0
                self._rel_last_pushall_req = 0.0
                self._rel_subscription = "pending"
            if down_since is not None:
                log.warning("ReliablePrinter: MQTT reconnected after %.0fs", time.monotonic() - down_since)
            if self.service_state != ServiceState.PAUSED:
                self._request_pushall(client)
        return on_connect

    def _wrap_on_disconnect(self, original):
        def on_disconnect(client, userdata, flags, reason_code, properties):
            with self._rel_lock:
                was_connected = self._rel_connected_at is not None
                self._rel_connected_at = None
                self._rel_disconnected_at = time.monotonic()
                self._rel_covered = set()
                self._rel_baseline_at = None
                self._rel_nozzle = None
                self._rel_sd = self._rel_ext_free = None
                self._rel_last_error = f"mqtt disconnected: {reason_code}"
            if was_connected:  # once per lost connection, not per failed retry
                log.warning("ReliablePrinter: MQTT disconnected (%s); telemetry not live until it reconnects",
                            reason_code)
            original(client, userdata, flags, reason_code, properties)
        return on_disconnect

    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        refused = [rc for rc in reason_code_list or () if getattr(rc, "is_failure", False)]
        with self._rel_lock:
            self._rel_subscription = f"refused ({refused[0]})" if refused else "granted"
        if refused:
            log.warning("ReliablePrinter: printer refused the report subscription (%s); no report can arrive",
                        refused[0])

    def _rel_set_connect_error(self, message: str) -> None:
        with self._rel_lock:
            changed = message != self._rel_connect_error
            self._rel_connect_error = message
        if changed:  # each distinct cause once, not every reconnect attempt
            log.warning("ReliablePrinter: %s", message)

    def reported_nozzle_diameter(self) -> str | None:
        """The nozzle_diameter the printer reported on this connection, or None (not yet / reconnected)."""
        with self._rel_lock:
            return self._rel_nozzle

    def reported_sd_card(self) -> tuple[int, str, float] | None:
        """(SdcardState code, field it came from, monotonic time) reported on this connection, or None."""
        with self._rel_lock:
            return self._rel_sd

    def reported_external_free_bytes(self) -> int | None:
        """Free bytes of external storage as the firmware reported them on this connection, or None."""
        with self._rel_lock:
            return self._rel_ext_free

    def _rel_full_state_wait(self, now: float) -> float:
        """Seconds until another full-state request may be sent (caller holds _rel_lock)."""
        gap = PUSHALL_EVERY_S if self._rel_full_unanswered else 0.0
        return max(0.0, self._rel_last_pushall_req + gap - now)

    def _rel_full_state_slot(self) -> bool:
        """Claim the next full-state request. The first on a connection, or the first after a full
        report, goes at once; while one is unanswered the next waits PUSHALL_EVERY_S."""
        now = time.monotonic()
        with self._rel_lock:
            if self._rel_full_state_wait(now) > 0:
                return False
            self._rel_last_pushall_req = now
            self._rel_full_requests += 1
            self._rel_full_unanswered += 1
            return True

    def _request_pushall(self, client=None) -> bool:
        """Read-only full-state request, within the slot shared with bpm's watchdog and refresh().
        Never a printer write."""
        client = client or self._client
        if client is None:
            return False
        try:
            if not client.is_connected():
                return False
            if not self._rel_full_state_slot():
                return False
            client.publish(f"device/{self.config.serial_number}/request", json.dumps(ANNOUNCE_PUSH))
            return True
        except Exception as exc:
            log.debug("ReliablePrinter: pushall request failed: %s", exc)
            return False

    def _request_push_start(self) -> bool:
        """Read-only stream request once no status report arrived for PUSH_START_AFTER_S on this
        connection, at most every PUSH_START_EVERY_S. Its own timer: no other request source can
        take its turn."""
        client = self._client
        now = time.monotonic()
        with self._rel_lock:
            connected_at, last_tel = self._rel_connected_at, self._rel_last_telemetry
            quiet_since = connected_at if last_tel is None or (connected_at is not None and last_tel < connected_at) \
                else last_tel
            if (connected_at is None or now - quiet_since < PUSH_START_AFTER_S
                    or now - self._rel_last_start_req < PUSH_START_EVERY_S):
                return False
            self._rel_last_start_req = now
        try:
            if client is None or not client.is_connected():
                return False
            client.publish(f"device/{self.config.serial_number}/request", json.dumps(REQUEST_PUSH_START))
            with self._rel_lock:
                self._rel_push_starts += 1
            return True
        except Exception as exc:
            log.debug("ReliablePrinter: stream request failed: %s", exc)
            return False

    def _request_version(self) -> bool:
        """Read-only get_version, at most every PUSHALL_EVERY_S. Its reply shows the printer still
        answers on this connection when it sends no report."""
        client = self._client
        now = time.monotonic()
        try:
            if client is None or not client.is_connected() or now - self._rel_last_version_req < PUSHALL_EVERY_S:
                return False
            self._rel_last_version_req = now
            client.publish(f"device/{self.config.serial_number}/request", json.dumps(ANNOUNCE_VERSION))
            return True
        except Exception as exc:
            log.debug("ReliablePrinter: version request failed: %s", exc)
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
        if value is not None:
            # Set before bpm calls connect(), so no packet is ever written from another thread.
            value.on_socket_register_write = _loop_owns_socket_io
            if getattr(value, "_sockpairW", None) is None:
                # The wake-up pair paho's loop()/loop_start() create and loop_forever() does not:
                # without it a queued publish waits for loop_forever's 1 s select timeout. paho
                # (pinned 2.1.0) writes one byte to it per queued packet and closes it in __del__.
                value._sockpairR, value._sockpairW = paho_client._socketpair_compat()
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
        if self.service_state == ServiceState.QUIT and self._internalException is not None:
            # bpm's synchronous first connect failed (printer off, unreachable, TLS reset).
            exc = self._internalException
            self._rel_set_connect_error(f"cannot connect to the printer: {type(exc).__name__}: {exc}")
        real = self._client
        if real is not None:
            try:
                # paho backs off exponentially between these bounds (bpm pins 1..1).
                real.reconnect_delay_set(min_delay=1, max_delay=30)
            except Exception:
                pass

            def on_connect_fail(client, userdata):
                # paho's own reconnect raised OSError (no route, refused, TLS reset, timeout).
                self._rel_set_connect_error("cannot reach the printer (reconnect attempt failed)")
            real.on_connect_fail = on_connect_fail

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

    # ── worker: on_update fan-out ────────────────────────────────────────────

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

    # ── health ───────────────────────────────────────────────────────────────

    def session_dead(self) -> bool:
        """True once bpm gave up (QUIT) or its loop thread ran and exited. A session that is still
        inside start_session (registered, no thread yet) is not dead and must not be replaced."""
        thread = self._mqtt_client_thread
        if thread is not None and thread.ident is not None:
            return not thread.is_alive()
        # No thread yet, or assigned but not yet started (bpm assigns before .start()).
        return self.service_state == ServiceState.QUIT

    @staticmethod
    def _quiet_detail(answering: bool, last_valid: float | None, now: float, unanswered: int) -> str:
        who = (f"the printer answers requests (last message {now - last_valid:.0f}s ago) but sends no status report"
               if answering else "nothing well-formed received from the printer on this connection")
        return f"{who}; {unanswered} full-state request(s) unanswered"

    def health_snapshot(self) -> dict:
        now = time.monotonic()

        def age(t):
            return None if t is None else round(now - t, 1)

        with self._rel_lock:
            connected_at = self._rel_connected_at
            last_tel = self._rel_last_telemetry
            baseline = self._rel_baseline_at
            in_msg = self._rel_in_message_since
            last_valid = self._rel_last_valid
            subscription = self._rel_subscription
            unanswered = self._rel_full_unanswered
            # Answering: something well-formed arrived on this connection within the dead-link window.
            answering = (connected_at is not None and last_valid is not None and last_valid >= connected_at
                         and now - last_valid <= FORCE_RECONNECT_AFTER_S)
            snap = {
                "age_s": {
                    "last_received": age(self._rel_last_received),
                    "last_valid": age(last_valid),
                    "last_telemetry": age(last_tel),
                    "last_usable": age(self._rel_last_usable),
                    "baseline": age(baseline),
                    "connected_for": age(connected_at),
                    "in_callback_for": age(in_msg),
                },
                "messages": {"received": self._rel_received, "processed": self._rel_processed,
                             "failed": self._rel_failed},
                "printer_answering": answering,
                "report_subscription": subscription,
                "stream_requests": self._rel_push_starts,
                "full_state": {"requests": self._rel_full_requests, "unanswered": unanswered,
                               "last_request": age(self._rel_last_pushall_req or None),
                               "last_full_report": age(self._rel_last_full_report),
                               "next_request_in_s": round(self._rel_full_state_wait(now), 1)},
                "last_error": self._rel_last_error,
                "connect_error": self._rel_connect_error,
                "session_generation": self._rel_generation,
                "reported_nozzle_diameter": self._rel_nozzle,
            }
            connect_error = self._rel_connect_error
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
            status, reason = "disconnected", (connect_error or self._rel_last_error
                                              or "MQTT loop thread is not running")
        elif not mqtt_connected or connected_at is None:
            if self._rel_received == 0 and self._rel_disconnected_at is None and not connect_error:
                status, reason = "starting", "connecting; state unknown"
            else:
                status, reason = "disconnected", "MQTT reconnecting" + (
                    f"; last attempt: {connect_error}" if connect_error
                    else f"; {self._rel_last_error}" if self._rel_last_error else "")
        elif subscription and subscription.startswith("refused"):
            status, reason = "stale", f"report subscription {subscription}; no report can arrive on this connection"
        elif baseline is None:
            if self._rel_failed and self._rel_last_error and "processing failed" in self._rel_last_error:
                status, reason = "incomplete", self._rel_last_error
            elif now - connected_at <= STALE_AFTER_S:
                status, reason = "starting", "waiting for a full state report since (re)connect"
            else:
                status, reason = "stale", (f"MQTT connected for {now - connected_at:.0f}s with no full status "
                                           "report since (re)connect; " + self._quiet_detail(answering, last_valid, now, unanswered))
        elif last_tel is None or now - last_tel > STALE_AFTER_S:
            status, reason = "stale", (f"no telemetry processed for {age(last_tel)}s; "
                                       + self._quiet_detail(answering, last_valid, now, unanswered))
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
                                                "live_since": None, "last_restart_reason": None,
                                                "quiet_replacements": 0})

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
        if h["mqtt_connected"] and h["session_thread_alive"] and status not in ("paused", "stuck"):
            # Read-only, on its own timer: reports quiet for PUSH_START_AFTER_S, even while still live.
            p._request_push_start()
        if status == "live":
            if now - p._rel_last_pushall_req > PUSHALL_KEEPALIVE_S:
                p._request_pushall()  # vendor keep-alive while reports flow
            e["stuck_since"] = None
            e["reconnect_requested"] = None
            if e["live_since"] is None:
                e["live_since"] = now
            elif now - e["live_since"] > BACKOFF_RESET_AFTER_LIVE_S:
                e["failures"] = 0
                e["quiet_replacements"] = 0
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
        # connected: starting / incomplete / stale. Both requests are read-only and rate-limited by
        # ReliablePrinter; the version reply shows whether the printer still answers.
        p._request_version()
        p._request_pushall()
        # Quiet = no usable, complete telemetry this connection. Received-but-useless frames
        # (malformed, partial, command replies) do not count, or a flood would hold it off forever.
        usable_for = h["age_s"]["last_usable"]
        connected_for = h["age_s"]["connected_for"] or 0.0
        silent = connected_for if usable_for is None else min(usable_for, connected_for)
        # Nothing well-formed arrives: a dead or corrupted link, replaced after FORCE_RECONNECT_AFTER_S.
        # The printer answers but does not report: a new connection was not seen to help (2026-11-03,
        # 39 of them), so replace it only on a doubling schedule kept per printer across sessions.
        answering = h.get("printer_answering")
        limit = (_bounded_backoff(NO_REPORT_RECONNECT_S, NO_REPORT_RECONNECT_MAX_S, e["quiet_replacements"])
                 if answering else FORCE_RECONNECT_AFTER_S)
        if silent > limit and connected_for > limit:
            requested = e["reconnect_requested"]
            if requested is None or requested[0] is not p:  # per session, never inherited
                e["reconnect_requested"] = (p, now)
                if answering:
                    e["quiet_replacements"] += 1
                    log.warning("supervisor: %s answers but sent no usable telemetry for %.0fs; replacing the "
                                "connection (replacement %d; next no sooner than %.0fs)", name, silent,
                                e["quiet_replacements"],
                                _bounded_backoff(NO_REPORT_RECONNECT_S, NO_REPORT_RECONNECT_MAX_S, e["quiet_replacements"]))
                else:
                    log.warning("supervisor: %s connected without usable telemetry or any well-formed message "
                                "for %.0fs; dropping the connection", name, silent)
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
        delay = _bounded_backoff(BACKOFF_MIN_S, BACKOFF_MAX_S, e["failures"])
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
    try:
        import storage_space
        base["storage"] = {"sd_card": storage_space.sd_state(p),
                           "timelapse_storage_free_bytes": p.reported_external_free_bytes(),
                           "last_space_check": storage_space.last_check(name)}
    except Exception as exc:
        base["storage"] = {"error": f"{type(exc).__name__}: {exc}"}
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
