"""
storage_space.py — A prepared job is uploaded and started only when the SD card is shown to have
room for it and for the G-code the printer unpacks from it.

Why (2026-11-03): an admitted 1,407,822-byte .gcode.3mf was uploaded, verified and started on the
P1S; the printer went PREPARE -> IDLE with HMS 0500-4003 because it could not unpack the
6,904,233-byte Metadata/plate_1.gcode (the firmware writes it to /cache/<job>_plate_1.gcode). Read
on a computer, the card (a reused dashcam card) had 38 free 32 KiB clusters, while its FAT32 free
cluster summary claimed 945,077. Nothing in the server knew how full the card was.

What the printer offers (docs/storage-space.md has the sources):
  MQTT   SD card state only: home_flag bits 8-9, aux bits 12-13, the legacy `sdcard` bool (none,
         normal, abnormal, read-only). Bambu Studio's only byte figures, device.cam
         tl_external_free_kb / tl_external_total_kb, are timelapse storage on newer models with
         built-in storage; a P1 sends no such field, and nothing shows the figure is the card a job
         goes to, so it is shown in telemetry and not used here.
  FTPS   names and sizes (LIST). No free-space or capacity command is documented for the
         printer's FTPS dialect, so none is sent.

So the card's capacity comes from the card itself: ops/attest_sd_card.py, run on a computer with
the card inserted, requires a clean `fsck.fat -n`, reads the data-area size and cluster size from
the filesystem and writes /bambu-mcp-card.json with a random card id. The server reads that file
and lists the whole card (both over FTPS, only while the printer reports no job). The listing
must be complete: a folder the printer refuses to list (550) or a listing row that is not a
recognised file or folder refuses, because what it hides can be any size. Then:

  available = data bytes - every listed file rounded up to whole clusters - folder clusters
  upload    needs the job (rounded) + its largest plate G-code (rounded) + RESERVE_BYTES
  start     needs the selected plate G-code (rounded) + RESERVE_BYTES (the job is on the card)

A file being overwritten and an earlier unpacked copy in /cache are not credited. available is an
upper bound on the free space, not a guarantee: RESERVE_BYTES covers what a listing cannot show
(clusters lost to corruption since the attestation; the 2026-11-03 card had 17 lost clusters and
two over-long chains, under 1 MiB together; folder clusters still held by deleted entries), folder
growth and firmware logs.

Unknown is refused: no card marker, a marker that does not parse, a folder that cannot be listed,
a listing row that is not a file or folder, an SD state not reported on this connection, or a
state other than normal. Nothing here deletes, formats or
writes the card, and there is no override.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import threading
import time
from pathlib import Path

import print_admission as pa

MARKER = "/bambu-mcp-card.json"
MARKER_SCHEMA = "bambu-mcp-sd-card/1"
MAX_MARKER_BYTES = 4096
RESERVE_BYTES = 64 * 1024 * 1024
SD_STATES = {0: "absent", 1: "normal", 2: "abnormal", 3: "read_only"}

_ATTEST = ("Run ops/attest_sd_card.py with the card in a computer (see docs/storage-space.md); "
           "it checks the filesystem and records the card's size on it.")

_lock = threading.Lock()
_last: dict[str, dict] = {}


def _mib(n: int) -> str:
    return f"{n / 1048576:.1f} MiB"


def _clusters(size: int, cluster: int) -> int:
    return -(-max(0, int(size)) // cluster) * cluster


def _record(name: str, entry: dict) -> None:
    with _lock:
        _last[name] = {**entry, "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "_mono": time.monotonic()}


def last_check(name: str) -> dict | None:
    """The latest space check for name (no I/O), with its age."""
    with _lock:
        entry = dict(_last[name]) if name in _last else None
    if entry is None:
        return None
    entry["age_s"] = round(time.monotonic() - entry.pop("_mono"), 1)
    return entry


def sd_state(printer) -> dict:
    """The SD card state this MQTT connection reported: {state, source, age_s}; state None if not yet."""
    reported = getattr(printer, "reported_sd_card", None)
    value = reported() if callable(reported) else None
    if not value:
        return {"state": None, "source": None, "age_s": None}
    code, source, at = value
    return {"state": SD_STATES.get(code, f"unknown ({code})"), "source": source,
            "age_s": round(time.monotonic() - at, 1)}


def check_sd_state(printer) -> dict:
    """Refuse unless the printer reported a normal SD card on this connection. MQTT only, no I/O."""
    sd = sd_state(printer)
    state = sd["state"]
    if state == "normal":
        return sd
    why = {None: ("storage_state_unknown", "The printer has not reported its SD card state since it (re)connected"),
           "absent": ("storage_no_card", "The printer reports no SD card"),
           "abnormal": ("storage_card_abnormal", "The printer reports its SD card as abnormal"),
           "read_only": ("storage_card_read_only", "The printer reports its SD card as read-only")}
    code, text = why.get(state, ("storage_state_unknown", f"The printer reports SD card state {state}"))
    raise pa.Refused(code, f"{text}. Nothing was written to the card and no command was sent.", sd_card=sd)


# The record's own name and the 8.3 alias FAT gives it (BAMBU-~1.JSO); FAT ignores case and
# trailing dots and spaces.
_RECORD_NAMES = re.compile(r"/(bambu-mcp-card\.json|bambu\S*~\d+\.jso)", re.I)


def protect_record(remote: str) -> None:
    """Refuse a server write onto the card record: the record is the card's capacity, so only
    ops/attest_sd_card.py writes it, on a computer, after fsck. A caller's upload would be a claim."""
    if _RECORD_NAMES.fullmatch(remote.rstrip(" ./")):
        raise pa.Refused("storage_record_protected",
                         f"{remote} is the SD card's record, which states the card's size; this server does not "
                         f"write it. Nothing was written. {_ATTEST}")


def _read_marker(ftps, tree: dict) -> dict:
    """The card record named in the listing, read on the same FTPS session and validated."""
    entry = next((c for c in tree["children"] if "children" not in c and c["name"].casefold() == MARKER[1:]), None)
    if entry is None:
        raise pa.Refused("storage_unattested",
                         f"The SD card has no {MARKER}, so its size is unknown: the printer reports neither capacity nor "
                         f"free space over MQTT or FTPS. Nothing was written and no command was sent. {_ATTEST}")
    if not 0 < (entry.get("size") or 0) <= MAX_MARKER_BYTES:
        raise pa.Refused("storage_unattested", f"{MARKER} on the card is not a card record ({entry.get('size')} bytes). "
                         f"Nothing was written and no command was sent. {_ATTEST}")
    tmpdir = tempfile.mkdtemp(prefix=".card-", dir=pa._ensure_root())
    local = os.path.join(tmpdir, "card.json")
    try:
        try:
            ftps.download_file("/" + entry["name"], local)
        except Exception as exc:
            raise pa.Refused("storage_unknown", f"{MARKER} could not be read from the card ({type(exc).__name__}: "
                             f"{exc}). Nothing was written and no command was sent.") from None
        try:
            card = json.loads(pa._read_regular(Path(local), MAX_MARKER_BYTES))
        except (OSError, ValueError) as exc:
            raise pa.Refused("storage_unattested", f"{MARKER} on the card is not a card record ({exc}). Nothing "
                             f"was written and no command was sent. {_ATTEST}") from None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    ok = (isinstance(card, dict) and card.get("schema") == MARKER_SCHEMA
          and isinstance(card.get("card_id"), str) and re.fullmatch(r"[0-9a-f]{32}", card["card_id"])
          and type(card.get("cluster_bytes")) is int and type(card.get("data_bytes")) is int)
    cluster = card.get("cluster_bytes") if ok else 0
    ok = ok and 512 <= cluster <= 1 << 21 and cluster & (cluster - 1) == 0 \
        and 0 < card["data_bytes"] <= 1 << 42 and card["data_bytes"] % cluster == 0
    if not ok:
        raise pa.Refused("storage_unattested", f"{MARKER} on the card is not a valid {MARKER_SCHEMA} record. Nothing "
                         f"was written and no command was sent. {_ATTEST}")
    return card


# One row of the printer's LIST: a file or folder, ls -l style. Anything else (a link, a device,
# a row this does not parse) refuses rather than being skipped.
_ROW = re.compile(r"(?P<type>[d-])[rwxsStT-]{9}\s+\d+\s+\S+\s+\S+\s+(?P<size>\d+)\s+"
                  r"(?P<month>[A-Z][a-z]{2})\s+(?P<day>\d{1,2})\s+(?:\d{4}|\d{1,2}:\d{2})\s+(?P<name>.+)")
_MONTHS = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
_TOTAL = re.compile(r"total \d+")  # an ls -l header line; it names no entry
MAX_DEPTH = 32


def _unlisted(path: str, why: str) -> pa.Refused:
    return pa.Refused("storage_unknown", f"The SD card's listing is incomplete: {path} {why}, so what it holds and "
                      "the card's free space are unknown. Nothing was written and no command was sent.", folder=path)


def _list_card(ftps) -> dict:
    """The whole card as {"name", "children": [...]}, read with LIST on the FTPS session ftps
    already holds. Every folder must list and every row must be a file or a folder: a refused
    folder (bpm reads a 550 as empty) or a skipped row could hide any amount of used space."""
    session = ftps.ftps_session
    root = {"name": "/", "children": []}
    stack = [("/", root, 0)]
    while stack:
        path, node, depth = stack.pop()
        if depth > MAX_DEPTH:
            raise _unlisted(path, f"is nested deeper than {MAX_DEPTH} folders")
        rows: list[str] = []
        try:
            session.retrlines(f"LIST {path}", rows.append)
        except Exception as exc:
            raise _unlisted(path, f"could not be listed ({type(exc).__name__}: {str(exc).strip()})") from None
        for i, row in enumerate(rows):
            if not row.strip() or (i == 0 and _TOTAL.fullmatch(row.strip())):
                continue
            m = _ROW.fullmatch(row)
            if not m or m["month"] not in _MONTHS or not 1 <= int(m["day"]) <= 31:
                raise _unlisted(path, f"lists an entry that is not a file or folder: {row[:120]!r}")
            name = m["name"]
            if name in (".", "..") or "/" in name:
                raise _unlisted(path, f"lists an entry this cannot place: {row[:120]!r}")
            child = {"name": name}
            if m["type"] == "d":
                child["children"] = []
                stack.append((f"{path.rstrip('/')}/{name}", child, depth + 1))
            else:
                child["size"] = int(m["size"])
            node["children"].append(child)
    return root


def _entry_bytes(name: str) -> int:
    """Directory bytes FAT uses for name: one 32-byte short entry and one long-name entry per 13
    UTF-16 units (up to 20 for a 255-unit name), counted even when the name would fit 8.3."""
    return 32 * (1 + -(-len(name.encode("utf-16-le")) // 26))


def _usage(tree: dict, cluster: int) -> tuple[int, int, int]:
    """(allocated bytes, files, folders) for a listed card tree, the root folder included. A
    folder counts its entries' actual name lengths plus its "." and ".." entries, at least one
    cluster; slots held by deleted entries are not visible here and fall to the reserve."""
    allocated = files = folders = 0
    stack = [tree]
    while stack:
        node = stack.pop()
        children = node["children"]
        folders += 1
        allocated += _clusters(max(1, 64 + sum(_entry_bytes(c["name"]) for c in children)), cluster)
        for child in children:
            if "children" in child:
                stack.append(child)
            else:
                files += 1
                allocated += _clusters(child["size"], cluster)
    return allocated, files, folders


def measure(printer) -> dict:
    """List the whole card and read its card record on one FTPS session from the printer
    session's ftp_connection (refused while the printer reports a job). The caller holds pa.file_lock."""
    started = time.monotonic()
    try:
        with printer.ftp_connection() as ftps:
            tree = _list_card(ftps)
            listing_s = round(time.monotonic() - started, 2)
            card = _read_marker(ftps, tree)
    except pa.Refused:
        raise
    except Exception as exc:
        raise pa.Refused("storage_unknown", f"The SD card could not be read ({type(exc).__name__}: {exc}), so its "
                         "free space is unknown. Nothing was written and no command was sent.") from None
    cluster = card["cluster_bytes"]
    used, files, folders = _usage(tree, cluster)
    return {"card_id": card["card_id"], "attested_at": card.get("attested_at"), "data_bytes": card["data_bytes"],
            "cluster_bytes": cluster, "listed_allocated_bytes": used, "files": files, "folders": folders,
            "available_bytes": card["data_bytes"] - used, "listing_s": listing_s}


def require(name: str, printer, purpose: str, needs, card_id: str | None = None) -> dict:
    """Measure the card and refuse unless it holds needs(cluster_bytes) + RESERVE_BYTES; return the evidence.

    The caller holds pa.file_lock(name), so no other upload, rename or delete of this server runs
    between this measurement and the transfer or start it guards.
    """
    try:
        sd = check_sd_state(printer)
        evidence = measure(printer)
    except pa.Refused as r:
        _record(name, {"purpose": purpose, "verdict": r.code, "sd_card": sd_state(printer)})
        raise
    breakdown = {**needs(evidence["cluster_bytes"]), "reserve": RESERVE_BYTES}
    required = sum(breakdown.values())
    available = evidence["available_bytes"]
    evidence = {**evidence, "required_bytes": required, "required": breakdown, "sd_card": sd}
    if card_id is not None and evidence["card_id"] != card_id:
        _record(name, {"purpose": purpose, "verdict": "storage_card_changed", **evidence})
        raise pa.Refused("storage_card_changed", "The SD card is not the one the job was uploaded to (its card record "
                         "differs). No command was sent.", storage=evidence)
    if available < required:
        _record(name, {"purpose": purpose, "verdict": "storage_insufficient", **evidence})
        parts = ", ".join(f"{k} {_mib(v)}" for k, v in breakdown.items())
        source = (f"attested data area {_mib(evidence['data_bytes'])} minus {_mib(evidence['listed_allocated_bytes'])} "
                  f"in {evidence['files']} listed files and {evidence['folders']} folders, measured just now")
        raise pa.Refused("storage_insufficient",
                         f"The SD card has {_mib(available)} available ({source}) but the {purpose} needs "
                         f"{_mib(required)} ({parts}). Nothing was written and no command was sent. Free space on "
                         "the card yourself (this server deletes nothing), then try again.", storage=evidence)
    _record(name, {"purpose": purpose, "verdict": "ok", **evidence})
    return evidence


def require_for_upload(name: str, printer, receipt) -> dict:
    """Upload check: the job, its largest plate G-code once unpacked, and the reserve. An existing
    file at the destination is not credited."""
    gcode = dict(receipt.plate_gcode_bytes)
    return require(name, printer, "upload", lambda cluster: {
        "job": _clusters(receipt.size, cluster),
        "unpacked_gcode": max(_clusters(v, cluster) for v in gcode.values())})


def require_for_start(name: str, printer, admitted, plate: int) -> dict:
    """Start check, on the card the job was uploaded to: the selected plate's G-code once unpacked
    and the reserve (the job itself is on the card and counted). An earlier unpacked copy is not credited."""
    gcode = dict(admitted.plate_gcode_bytes)[plate]
    return require(name, printer, "start", lambda cluster: {"unpacked_gcode": _clusters(gcode, cluster)},
                   card_id=admitted.card_id)
