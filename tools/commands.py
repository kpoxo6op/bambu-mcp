"""
tools/commands.py — Raw MQTT command tool for Bambu Lab printers.

Raw commands are limited to an allowlist of non-start maintenance commands
(print_admission.RAW_MQTT_ALLOWED). They cannot start or name a file, and G-code goes through
send_gcode. Every publish still passes the telemetry guard and the print-admission policy in
reliability.py. Use only when no dedicated tool covers the parameters you need.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)

from session_manager import session_manager


def _permission_denied(consequence: str) -> str:
    return f"Error: user_permission must be True to perform this action. {consequence}"


def send_mqtt_command(
    name: str,
    command_json: str,
    user_permission: bool = False,
) -> dict:
    """
    Send one allowlisted, non-start MQTT command JSON to the printer's request topic.

    WHEN to use: as a LAST RESORT, only when no dedicated tool covers the parameters you need
    (dedicated tools include ``swap_tool``, ``set_print_speed``, ``set_nozzle_temp``,
    ``set_bed_temp``, ``set_fan_speed``, ``load_filament``, ``set_print_option`` and
    ``send_gcode``). Wrong parameters can still fault the hardware.

    WRITE GUARD: publishes the command to the physical printer's MQTT request topic
    (device/{serial}/request), so what it asks the firmware to do happens on the hardware and
    may not be reversible. ``user_permission`` must be explicitly True (mandatory
    write-protection gate). With it False (the default) the tool changes nothing, contacts no
    printer, and returns the refusal ``{"error": "Error: user_permission must be True to
    perform this action. <what this would do>"}``. Like every printer write, it is refused
    while telemetry is not live.

    ALLOWLIST: the JSON must be one object with exactly one section and a string
    ``command`` from this list; anything else is refused with nothing sent (no override):
      print: pause, resume, stop, print_speed, skip_objects, clean_print_error, print_option,
        ams_change_filament, ams_control, ams_filament_setting, ams_user_setting,
        ams_filament_drying, ams_get_rfid, unload_filament, manual_ams_bind,
        extrusion_cali_sel, extrusion_cali_set, extrusion_cali_get, extrusion_cali_get_result,
        flowrate_get_result, select_extruder, set_nozzle, refresh_nozzle, set_ctt, set_airduct
      system: ledctrl, uiop, set_accessories, get_accessories
      xcam: xcam_control_set;  info: get_version;  pushing: pushall
    It can never start a print: ``project_file``, ``gcode_file`` and any command carrying a
    ``url`` or ``file`` field are refused (prints start only through ``prepare_print_job`` ->
    ``upload_file`` -> ``print_file``), ``gcode_line`` is refused (use ``send_gcode``), and
    firmware upgrade or other unlisted commands are refused.

    Sibling disambiguation: ``send_mqtt_command`` publishes one allowlisted MQTT JSON command
    and is NOT gated by the active-print guard; ``send_gcode`` sends maintenance G-code and is
    blocked while a print is active (gcode_state RUNNING/PREPARE). Prefer the dedicated tool
    whenever one exists.

    Args:
        name: Configured printer name; the printer serial number is looked up automatically
            from the stored credentials for it, so you do not supply the serial.
        command_json: A JSON string matching the Bambu Lab MQTT command schema (see
            kb_get('bambu-mqtt-commands') for field details), e.g.
            ``{"print": {"command": "pause", "sequence_id": "0"}}``. It is parsed, checked
            against the allowlist and re-serialised before publishing to
            device/{serial}/request via printer.send_anything().
        user_permission: Must be explicitly True to send; defaults to False (refuses).

    Returns:
        On success: ``{"success": True, "message": "Command sent to '<name>'.", "command":
        <the parsed command>, "delivery": <note>}``: the command was written to the MQTT socket
        (QoS 0); the firmware's acceptance is not confirmed. On failure: ``{"error": str}``
        with the permission refusal, ``"Printer '<name>' not connected"`` (no session under
        that name), ``"Invalid JSON: <detail>"`` or ``"Error sending command to '<name>':
        <detail>"``; ``{"error": "raw_command_refused", "detail": str}`` for a command outside
        the allowlist; or the guard's ``telemetry_not_live`` / ``publish_rejected`` /
        ``publish_unconfirmed`` result.

    Notes:
        Nothing is queued or retried. A refused write was not sent and will not be sent later;
        ``publish_unconfirmed`` means the write may or may not have reached the printer and was
        not retried, so check ``get_printer_state`` and ``get_printer_connection_status``
        before repeating it.

        CAUTION: this tool is not gated by the active-print guard, so that pause, stop and the
        like always work. Commands such as select_extruder or ams_change_filament sent while a
        print is active (gcode_state RUNNING/PREPARE) are EXTREMELY DANGEROUS: firmware does NOT
        reject tool swaps mid-print. The agent MUST NOT use this tool to circumvent the
        active-print guard on swap_tool(), load_filament() or any other gated tool.
    """
    log.debug("send_mqtt_command: called for %s user_permission=%s", name, user_permission)
    if not user_permission:
        log.debug("send_mqtt_command: permission denied for %s", name)
        return {
            "error": _permission_denied(
                "This would publish an allowlisted raw MQTT command to the printer's request topic; "
                "it is not subject to the active-print guard."
            )
        }
    printer = session_manager.get_printer(name)
    if printer is None:
        log.warning("send_mqtt_command: printer not connected: %s", name)
        return {"error": f"Printer '{name}' not connected"}
    log.debug("send_mqtt_command: validating JSON, length=%d", len(command_json))
    try:
        command = json.loads(command_json)
    except json.JSONDecodeError as e:
        log.debug("send_mqtt_command: JSON parse failed for %s: %s", name, e)
        return {"error": f"Invalid JSON: {e}"}
    import print_admission
    refusal = print_admission.raw_command_refusal(command)
    if refusal:
        log.warning("send_mqtt_command: refused for %s: %s", name, refusal)
        return {"error": "raw_command_refused", "detail": f"{refusal} No command was sent."}
    try:
        log.debug("send_mqtt_command: sending command to %s: %s", name, command_json)
        printer.send_anything(command_json)
        log.info("send_mqtt_command: command sent to %s", name)
        log.debug("send_mqtt_command: → success, command=%s", command_json[:80])
        return {
            "success": True,
            "message": f"Command sent to '{name}'.",
            "command": command,
        }
    except Exception as e:
        log.error("send_mqtt_command: error for %s: %s", name, e, exc_info=True)
        return {"error": f"Error sending command to '{name}': {e}"}
