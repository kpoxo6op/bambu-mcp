"""
auth.py — Printer credential resolution from secrets_store.

Credentials are stored per-printer under keys:
  bambu-{name}_ip, bambu-{name}_access_code, bambu-{name}_serial

Configured printer names are stored as a JSON list under key "_printer_names".
"""

from __future__ import annotations

import logging
import json
import os
from pathlib import Path

log = logging.getLogger(__name__)

import secrets_store


def _studio_printer() -> tuple[str, dict] | None:
    """Resolve one explicitly selected printer from existing Bambu Studio credentials.

    The access code stays in the existing Studio file and process memory. It is
    never copied into MCP client configuration, arguments, or a second vault.
    """
    config_path = os.environ.get("BAMBU_STUDIO_CONFIG")
    if not config_path:
        return None
    name = os.environ.get("BAMBU_PRINTER_NAME", "p1s")
    serial = os.environ.get("BAMBU_PRINTER_SERIAL", "")
    host = os.environ.get("BAMBU_PRINTER_HOST", "")
    if not serial or not host:
        raise ValueError("Studio credential source requires an explicit printer serial and host")
    try:
        # raw_decode: Studio may append a "# MD5 checksum" line after the JSON object.
        data, _ = json.JSONDecoder().raw_decode(Path(config_path).read_text().lstrip())
        code = data.get("user_access_code", {}).get(serial) or data.get("access_code", {}).get(serial)
    except (OSError, ValueError, TypeError, AttributeError):
        raise ValueError("Cannot read the configured Bambu Studio credential source") from None
    if not isinstance(code, str) or len(code) != 8 or not code.isascii() or not code.isalnum():
        raise ValueError("No valid LAN access code for the configured printer serial")
    return name, {"ip": host, "access_code": code, "serial": serial}


def get_printer_credentials(name: str) -> dict:
    """
    Return {ip, access_code, serial} for a configured printer.
    Raises KeyError if the printer has no stored credentials.
    """
    studio = _studio_printer()
    if studio is not None:
        if name != studio[0]:
            raise KeyError("Printer is not the configured Studio printer")
        return studio[1]
    log.debug("get_printer_credentials: called for name=%s", name)
    prefix = f"bambu-{name}"
    ip = secrets_store.get(f"{prefix}_ip")
    access_code = secrets_store.get(f"{prefix}_access_code")
    serial = secrets_store.get(f"{prefix}_serial")
    if not ip or not access_code or not serial:
        log.warning("get_printer_credentials: printer '%s' not fully configured (ip=%s serial=%s)", name, bool(ip), bool(serial))
        raise KeyError(
            f"Printer '{name}' not fully configured. "
            f"Use add_printer('{name}', ip, serial, access_code, user_permission=True) to set it up."
        )
    log.debug("get_printer_credentials: returning ip=%s serial=%s access_code=<redacted>", ip, serial)
    return {"ip": ip, "access_code": access_code, "serial": serial}


def save_printer_credentials(name: str, ip: str, access_code: str, serial: str) -> None:
    """Save credentials for a printer and add it to the configured printer list."""
    log.debug("save_printer_credentials: called for name=%s ip=%s serial=%s access_code=<redacted>", name, ip, serial)
    prefix = f"bambu-{name}"
    secrets_store.set(f"{prefix}_ip", ip)
    secrets_store.set(f"{prefix}_access_code", access_code)
    secrets_store.set(f"{prefix}_serial", serial)
    log.debug("save_printer_credentials: credentials saved for '%s'", name)
    names = secrets_store.get("_printer_names", default=[])
    if name not in names:
        names.append(name)
        secrets_store.set("_printer_names", names)
        log.debug("save_printer_credentials: updated printer names list, count=%d", len(names))
    log.debug("save_printer_credentials: → done for name=%s", name)


def delete_printer_credentials(name: str) -> None:
    """Remove all credentials for a printer."""
    log.debug("delete_printer_credentials: called for name=%s", name)
    prefix = f"bambu-{name}"
    for suffix in ("_ip", "_access_code", "_serial"):
        secrets_store.delete(f"{prefix}{suffix}")
    log.debug("delete_printer_credentials: deleted credentials for '%s'", name)
    names = secrets_store.get("_printer_names", default=[])
    if name in names:
        names.remove(name)
        secrets_store.set("_printer_names", names)
        log.debug("delete_printer_credentials: updated printer names list, count=%d", len(names))
    log.debug("delete_printer_credentials: → done for name=%s", name)


def get_configured_printer_names() -> list[str]:
    """Return the list of configured printer names."""
    studio = _studio_printer()
    if studio is not None:
        return [studio[0]]
    log.debug("get_configured_printer_names: called")
    names = secrets_store.get("_printer_names", default=[])
    log.debug("get_configured_printer_names: → %s", names)
    return names
