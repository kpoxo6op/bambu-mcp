"""tools/_guards.py — Shared safety guards for MCP tools and HTTP routes."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def check_active_print_guard(printer, name: str, operation: str) -> dict | None:
    """Return an error dict if the printer is actively printing; None if safe.

    Blocks when gcode_state is RUNNING or PREPARE — these states indicate the
    printer is executing toolpath moves or preparing to do so.  Sending
    disruptive commands (GCode injection, tool swaps, filament changes, new
    print jobs) during these states risks toolhead crashes, failed prints, or
    hardware damage.

    Args:
        printer: BambuPrinter instance (already resolved from session_manager).
        name: Human-readable printer name (for error messages).
        operation: Short description of the blocked operation (for error messages).

    Returns:
        dict with "error" key if blocked, or None if the operation may proceed.
    """
    try:
        state = printer.printer_state
        gcode_state = getattr(state, "gcode_state", None) or ""
    except Exception:
        gcode_state = ""

    if gcode_state.upper() in ("RUNNING", "PREPARE"):
        log.warning(
            "active_print_guard: BLOCKED %s on '%s' — gcode_state=%s",
            operation, name, gcode_state,
        )
        return {
            "error": (
                f"Blocked: '{name}' is currently {gcode_state}. "
                f"{operation} is not safe while a print is active. "
                "Wait for the print to finish, pause it first, or cancel it."
            ),
        }
    return None


# ── Telemetry guard and off-loop execution for every registered tool ─────────────────────────────
#
# Incident 2026-11-01: a dead MQTT session kept serving the pre-job IDLE frame with
# recent_update=True, and nothing stopped a write from going into the dead socket. FastMCP 1.26
# also runs sync tools on the event-loop thread, so one blocking tool (FTPS, camera, a quit()
# joining a stuck thread) froze every client of the shared server. guard_tool() fixes both at
# registration time: tools run on a bounded thread pool, printer writes are refused unless
# reliability.health() says writable, and live-state reads that are not current come back as
# an error carrying the last known values instead of passing as current.

import functools
import inspect

# Tools that make the printer act or change (MQTT commands, SD-card writes). Refused while
# telemetry is not live. Their own user_permission and active-print checks still run first.
PRINTER_WRITE_TOOLS: frozenset[str] = frozenset({
    "calibrate_ams_remaining", "clear_print_error", "create_folder", "delete_file", "load_filament",
    "pause_print", "print_file", "refresh_nozzles", "register_sd_card", "rename_printer", "rename_sdcard_file",
    "resume_print", "select_extrusion_calibration", "send_ams_control_command", "send_gcode",
    "send_mqtt_command", "set_air_printing_detection", "set_ams_filament_setting",
    "set_ams_user_setting", "set_bed_temp", "set_buildplate_marker_detection", "set_chamber_light",
    "set_chamber_temp", "set_fan_speed", "set_first_layer_inspection", "set_nozzle_clumping_detection",
    "set_nozzle_config", "set_nozzle_temp", "set_print_option", "set_print_options", "set_print_speed",
    "set_purge_chute_detection", "set_spaghetti_detection", "skip_objects", "start_ams_dryer",
    "stop_ams_dryer", "stop_print", "swap_tool", "unload_filament", "upload_file",
})

# Non-read-only tools that send no printer write: local session/config management, SD reads,
# the local print preparer, and the version/pushall refresh requests (allowed while stale: they
# are how state recovers).
NON_PRINTER_WRITE_TOOLS: frozenset[str] = frozenset({
    "add_printer", "disconnect_printer", "download_file", "force_state_refresh", "pause_mqtt_session",
    "prepare_print_job", "remove_printer", "resume_mqtt_session", "set_user_pref", "start_printer",
    "start_stream", "stop_stream", "trigger_printer_refresh", "truncate_log", "update_printer_credentials",
})

# Read tools whose answer is the printer's current MQTT-reported state.
LIVE_STATE_TOOLS: frozenset[str] = frozenset({
    "get_ams_status", "get_ams_units", "get_chamber_light", "get_climate", "get_detector_settings",
    "get_external_spool", "get_fan_speeds", "get_hms_errors", "get_job_info", "get_nozzle_info",
    "get_print_progress", "get_printer_state", "get_spool_info", "get_temperatures", "get_wifi_signal",
})

# Cheap, I/O-free diagnostics get their own lane so health stays answerable even when slow
# tools (camera/FTPS timeouts) fill the main pool.
DIAGNOSTIC_TOOLS: frozenset[str] = frozenset({
    "get_printer_connection_status", "get_configured_printers", "get_server_info",
})

TOOL_THREADS = 8
DIAGNOSTIC_THREADS = 2
_limiters: dict = {}

_DELIVERY_NOTE = ("sent to the printer's MQTT socket (QoS 0); the printer's acceptance is not "
                  "confirmed. Verify with telemetry or the camera.")


def check_tool_classification(annotations: dict) -> None:
    """Every non-read-only tool must say whether it writes to the printer. Raises otherwise."""
    writes = {n for n, a in annotations.items() if not a[1]}
    unclassified = sorted(writes - PRINTER_WRITE_TOOLS - NON_PRINTER_WRITE_TOOLS)
    both = sorted(PRINTER_WRITE_TOOLS & NON_PRINTER_WRITE_TOOLS)
    unknown = sorted((PRINTER_WRITE_TOOLS | NON_PRINTER_WRITE_TOOLS | LIVE_STATE_TOOLS) - annotations.keys())
    if unclassified or both or unknown:
        raise RuntimeError(f"tool guard classification out of step: unclassified={unclassified} "
                           f"both={both} unknown={unknown}")


def _annotate_delivery(result, outcomes: list):
    if not any(o.get("status") == "sent" for o in outcomes):
        return result
    if isinstance(result, dict) and "delivery" not in result:
        return {**result, "delivery": _DELIVERY_NOTE}
    if isinstance(result, str):
        return f"{result} [delivery: {_DELIVERY_NOTE}]"
    return result


def _delivery_failure(outcomes: list, tool_result) -> dict | None:
    """The machine-readable delivery outcome when any printer write in this call was not sent.

    Taken from the publish guard's own records, so it holds even when the tool caught
    PublishRejected/PublishUnconfirmed and returned a plain error string (most tools do).
    ``sent``: False only when nothing was written; "unknown" when a write may have reached the
    printer; "partial" when earlier writes went out before a later one failed. Never retried.
    """
    counts = {k: sum(1 for o in outcomes if o.get("status") == k) for k in ("sent", "unconfirmed", "rejected")}
    if not counts["unconfirmed"] and not counts["rejected"]:
        return None
    reasons = "; ".join(dict.fromkeys(o.get("reason", "") for o in outcomes if o.get("status") != "sent"))
    if counts["sent"]:
        error, sent = "publish_partial", "partial"
        detail = (f"{counts['sent']} printer write(s) were sent before a later one failed ({reasons}). "
                  "The printer may be part way through this operation. Nothing was retried; "
                  "check telemetry or the camera before repeating any of it.")
    elif counts["unconfirmed"]:
        error, sent = "publish_unconfirmed", "unknown"
        detail = (f"The printer write may or may not have reached the printer ({reasons}). It was not "
                  "retried. Delivery remains unknown; check telemetry or the camera before repeating it.")
    else:
        error, sent = "publish_rejected", False
        detail = f"No command was sent and none will be sent later ({reasons})."
    return {"error": error, "detail": detail, "sent": sent, "retried": False, "publishes": counts,
            "tool_result": tool_result}


def _as_text(result: dict) -> str:
    """A guard result for a tool declared ``-> str`` (FastMCP validates the declared type)."""
    import json

    tel = result.get("telemetry") or {}
    brief = {k: tel.get(k) for k in ("status", "reason", "age_s", "writable") if k in tel}
    extra = {k: v for k, v in result.items() if k not in ("error", "detail", "telemetry")}
    text = f"Error ({result.get('error')}): {result.get('detail', '')}"
    if brief:
        text += f" telemetry={json.dumps(brief, default=str)}"
    if extra:
        text += f" {json.dumps(extra, default=str)}"
    return text


def run_guarded(fn, kwargs: dict, as_text: bool = False):
    """Run one tool call with the telemetry guards; synchronous (call it off the event loop).

    ``as_text``: the tool is declared ``-> str``, so guard results are rendered as text.
    """
    result = _run_guarded(fn, kwargs)
    if as_text and isinstance(result, dict):
        return _as_text(result)
    return result


def _run_guarded(fn, kwargs: dict):
    import reliability

    tool = fn.__name__
    printer = kwargs.get("name", "")
    if tool in PRINTER_WRITE_TOOLS and kwargs.get("user_permission"):
        blocked = reliability.mutation_block(printer)
        if blocked is not None:
            log.warning("guard: refused %s on %s: %s", tool, printer, blocked["telemetry"].get("status"))
            return blocked
        with reliability.capture_publishes() as outcomes:
            try:
                result = fn(**kwargs)
            except Exception as exc:
                if not outcomes:
                    raise  # nothing was published: the tool's own error stands unchanged
                failure = _delivery_failure(outcomes, f"{type(exc).__name__}: {exc}")
                if failure is None:  # every write was sent, then the tool failed
                    failure = {"error": "tool_failed_after_send", "detail": f"{type(exc).__name__}: {exc}",
                               "sent": True, "retried": False, "delivery": _DELIVERY_NOTE}
                return failure
        failure = _delivery_failure(outcomes, result)
        if failure is not None:
            log.warning("guard: %s on %s: %s %s", tool, printer, failure["error"], failure["publishes"])
            return failure
        return _annotate_delivery(result, outcomes)
    result = fn(**kwargs)
    if tool in LIVE_STATE_TOOLS:
        health = reliability.health(printer)
        if not health.get("live"):
            return {
                "error": "telemetry_not_live",
                "detail": (f"'{printer}' telemetry is {health.get('status')} ({health.get('reason')}); "
                           "last_known is NOT current printer state."),
                "telemetry": health,
                "last_known": result,
            }
    return result


def guard_tool(fn):
    """Async wrapper for FastMCP: same name, docstring and schema; runs fn on a bounded pool."""
    if inspect.iscoroutinefunction(fn):
        return fn  # none today; an async tool would need its own guard
    signature = inspect.signature(fn, eval_str=True)
    as_text = signature.return_annotation is str
    lane = "diagnostic" if fn.__name__ in DIAGNOSTIC_TOOLS else "tools"

    @functools.wraps(fn)
    async def wrapper(**kwargs):
        import anyio

        limiter = _limiters.get(lane)
        if limiter is None:
            limiter = _limiters[lane] = anyio.CapacityLimiter(
                DIAGNOSTIC_THREADS if lane == "diagnostic" else TOOL_THREADS)
        return await anyio.to_thread.run_sync(run_guarded, fn, kwargs, as_text, limiter=limiter)

    # FastMCP resolves string annotations with the wrapper's globals; give it resolved ones.
    wrapper.__signature__ = signature
    return wrapper
