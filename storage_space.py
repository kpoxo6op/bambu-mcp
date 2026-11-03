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

So the card's capacity comes from the card itself: /bambu-mcp-card.json, a record of the data-area
size and cluster size taken from a clean `fsck.fat -n` of that card, with a random card id. Two
operator paths write it, never a caller's upload or rename (protect_record):
  ops/attest_sd_card.py      the card in a computer: fsck, write the record, fsck again.
  ops/register_sd_card.py    the card stays in the printer: the operator hands over fsck logs of
                             this card already taken on a computer and a custody statement; the CLI
                             writes a one-shot registration into a host store no MCP file tool
                             writes (registry_root), and register_sd_card has this server re-parse
                             the logs and write the record over its own FTPS session.
The server reads the record and lists the whole card (both over FTPS, only while the printer
reports no job). The listing
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
import stat
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

_ATTEST = ("Record the card's size with ops/attest_sd_card.py (card in a computer) or "
           "ops/register_sd_card.py (card in the printer, from fsck logs of this card); see docs/storage-space.md.")

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
    """Refuse a caller's write onto the card record: the record is the card's capacity, so only the
    operator paths write it (attest on a computer; register from fsck logs). An upload would be a claim."""
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
    return {"card_id": card["card_id"], "attested_at": card.get("attested_at"), "basis": card.get("basis"),
            "data_bytes": card["data_bytes"],
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


# ── Operator registration: a card that stays in the printer ────────────────────────────────────
#
# 2026-11-04: the P1S's card was formatted and checked on a computer (clean fsck.fat -n -v) and
# went back into the printer without a record; it stays there. ops/register_sd_card.py takes the
# saved fsck output of that check and an operator's custody statement, writes a one-shot
# registration into registry_root() (download_file refuses destinations there, so no MCP caller
# writes one), and calls register_sd_card. This server parses the logs again, lists the card and
# writes the record over its own FTPS session. No size is taken from anyone: it is the fsck output.

REGISTRATION_SCHEMA = "bambu-mcp-card-registration/1"
REGISTRATION_MAX_AGE_S = 900
MAX_REGISTRATION_BYTES = 256 * 1024
MAX_FSCK_LOG_BYTES = 64 * 1024
MAX_CUSTODY_CHARS = 1000
# fsck.fat output meaning the filesystem is not clean: the words ops/attest_sd_card.py refuses.
FSCK_PROBLEMS = re.compile(r"wrong|differ|truncat|reclaim|correct|bad |invalid|lost|free cluster summary|"
                           r"unable|error|has no|cluster chain|duplicate|dirty", re.I)
_FSCK_SUMMARY = re.compile(r"(?P<device>/\S+): (?P<files>\d+) files, (?P<used>\d+)/(?P<clusters>\d+) clusters")
_ID = re.compile(r"[0-9a-f]{32}")


def registry_root() -> Path:
    """Host store for operator card registrations (ops/register_sd_card.py writes, this server reads)."""
    return Path.home() / ".bambu-mcp" / "sd-card"


def registry_dir(create: bool = False) -> Path:
    """registry_root(), required to be a directory only the service user can use."""
    root = registry_root()
    if create:
        root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            root.mkdir(mode=0o700)
        except FileExistsError:
            pass
    try:
        st = os.lstat(root)
    except FileNotFoundError:
        raise pa.Refused("registration_missing", f"There is no registration store ({root}); run "
                         "ops/register_sd_card.py. Nothing was written.") from None
    if (stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid()
            or st.st_mode & 0o077):
        raise pa.Refused("registration_store_untrusted",
                         f"{root} must be a directory owned by the service user with mode 0700. Nothing was written.")
    return root


def parse_fsck_log(text: str) -> dict:
    """One saved `fsck.fat -n [-v]` output of a clean FAT filesystem: its summary line and, from -v,
    the data area and cluster size. ValueError for anything else: a problem line, a check that did
    not reach its summary, or geometry that does not add up."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("fsck.fat "):
        raise ValueError("not fsck.fat output (its first line names fsck.fat)")
    problems = [line.strip() for line in lines
                if not line.startswith(("fsck.fat", "Checking", "Boot sector")) and FSCK_PROBLEMS.search(line)]
    if problems:
        raise ValueError(f"fsck reported problems: {problems[:3]}")
    summary = _FSCK_SUMMARY.fullmatch(lines[-1].strip())
    if not summary:
        raise ValueError("its last line is not fsck's '<device>: N files, used/total clusters' summary")
    out = {"device": summary["device"], "files": int(summary["files"]),
           "used_clusters": int(summary["used"]), "clusters": int(summary["clusters"])}
    cluster = re.search(r"^\s*(\d+) bytes per cluster\s*$", text, re.M)
    data = re.search(r"^\s*(\d+) data clusters \((\d+) bytes\)\s*$", text, re.M)
    if cluster or data:
        if not (cluster and data):
            raise ValueError("it shows only part of the filesystem geometry")
        size, count, data_bytes = int(cluster[1]), int(data[1]), int(data[2])
        if count * size != data_bytes or count != out["clusters"]:
            raise ValueError("its geometry does not add up")
        out.update(cluster_bytes=size, data_bytes=data_bytes)
    if not 0 < out["used_clusters"] <= out["clusters"]:
        raise ValueError("its cluster counts are impossible")
    return out


def card_geometry(logs: list[str]) -> dict:
    """The data area and cluster size of one filesystem from its saved clean fsck logs, oldest
    first. At least one must be verbose (-v); all must name the same device and cluster count."""
    parsed = [parse_fsck_log(text) for text in logs]
    geo = next((p for p in parsed if "data_bytes" in p), None)
    if geo is None:
        raise ValueError("no log shows the geometry; include the `fsck.fat -n -v` output")
    for p in parsed:
        if (p["device"], p["clusters"], p.get("cluster_bytes", geo["cluster_bytes"])) != \
                (geo["device"], geo["clusters"], geo["cluster_bytes"]):
            raise ValueError("the logs are not of one filesystem (device, cluster count or cluster size differ)")
    size = geo["cluster_bytes"]
    if not (512 <= size <= 1 << 21 and size & (size - 1) == 0 and 0 < geo["data_bytes"] <= 1 << 42):
        raise ValueError("the geometry is outside what a card record accepts")
    return {"device": geo["device"], "cluster_bytes": size, "data_bytes": geo["data_bytes"],
            "clusters": geo["clusters"], "used_clusters": parsed[-1]["used_clusters"], "logs": len(parsed)}


def _take_registration(name: str, registration_id) -> tuple[dict, dict]:
    """Claim the pending registration (renamed before it is read, so it is used once whatever
    follows) and validate it: (registration, geometry parsed again from its fsck logs)."""
    if not isinstance(registration_id, str) or not _ID.fullmatch(registration_id):
        raise pa.Refused("registration_invalid", "registration_id must be the 32-hex id ops/register_sd_card.py "
                         "printed. Nothing was written.")
    root = registry_dir()
    taken = root / f"{registration_id}.used.json"
    try:
        os.rename(root / f"{registration_id}.json", taken)
    except FileNotFoundError:
        raise pa.Refused("registration_missing", f"No pending registration {registration_id}: each is used once, "
                         "by this call. Run ops/register_sd_card.py for a new one. Nothing was written.") from None
    try:
        reg = json.loads(pa._read_regular(taken, MAX_REGISTRATION_BYTES))
        if not isinstance(reg, dict) or reg.get("schema") != REGISTRATION_SCHEMA \
                or reg.get("registration_id") != registration_id:
            raise ValueError(f"not a {REGISTRATION_SCHEMA} record with this id")
        if reg.get("printer") != name:
            raise ValueError(f"it is for printer {reg.get('printer')!r}, not {name!r}")
        age = time.time() - float(reg.get("created_at"))
        if not -60 <= age <= REGISTRATION_MAX_AGE_S:
            raise ValueError(f"it was made {age:.0f} s ago; it must be used within {REGISTRATION_MAX_AGE_S} s")
        custody, measured = reg.get("custody"), reg.get("measured_at")
        if not isinstance(custody, str) or not 10 <= len(custody.strip()) <= MAX_CUSTODY_CHARS:
            raise ValueError(f"its custody statement must be 10 to {MAX_CUSTODY_CHARS} characters")
        if not isinstance(measured, str) or not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", measured):
            raise ValueError("measured_at is not a UTC time")
        logs = reg.get("fsck_logs")
        if not isinstance(logs, list) or not logs or not all(isinstance(entry, dict) for entry in logs):
            raise ValueError("it has no fsck logs")
        texts = [entry.get("text") for entry in logs]
        if not all(isinstance(t, str) and len(t) <= MAX_FSCK_LOG_BYTES for t in texts):
            raise ValueError("an fsck log is missing or too long")
        geo = card_geometry(texts)
    except (OSError, ValueError, TypeError) as exc:
        raise pa.Refused("registration_invalid", f"Registration {registration_id} is not usable ({exc}); it is now "
                         "used up. Nothing was written.") from None
    return reg, geo


def register(name: str, printer, registration_id) -> dict:
    """Write the card record to the card in the printer from an operator registration, over one
    FTPS session of the printer session (refused while it reports a job), then read it back
    through measure(). Refuses a card that already has a record, a listing that is incomplete
    and a card that lists more than the registered data area."""
    with pa.file_lock(name):
        reg, geo = _take_registration(name, registration_id)
        try:
            sd = check_sd_state(printer)
        except pa.Refused as r:
            _record(name, {"purpose": "register", "verdict": r.code, "sd_card": sd_state(printer)})
            raise
        record = {"schema": MARKER_SCHEMA, "card_id": registration_id, "data_bytes": geo["data_bytes"],
                  "cluster_bytes": geo["cluster_bytes"], "attested_at": reg["measured_at"],
                  "registered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "basis": f"operator registration from {geo['logs']} clean fsck.fat -n log(s) taken on a computer, "
                           f"newest {reg['measured_at']}; not checked since the card went into the printer. "
                           f"Custody: {reg['custody'].strip()}"}
        data = json.dumps(record, separators=(",", ":")).encode()
        if len(data) > MAX_MARKER_BYTES:
            raise pa.Refused("registration_invalid", "The card record would be too long. Nothing was written.")
        tmpdir = tempfile.mkdtemp(prefix=".card-", dir=pa._ensure_root())
        stored = False
        try:
            local, back = os.path.join(tmpdir, "card.json"), os.path.join(tmpdir, "back.json")
            Path(local).write_bytes(data)
            with printer.ftp_connection() as ftps:
                tree = _list_card(ftps)
                if any("children" not in c and c["name"].casefold() == MARKER[1:] for c in tree["children"]):
                    raise pa.Refused("storage_already_attested", f"The card already has {MARKER}; it is not "
                                     "replaced. Nothing was written.")
                used, files, folders = _usage(tree, geo["cluster_bytes"])
                if used > geo["data_bytes"]:
                    raise pa.Refused("registration_inconsistent",
                                     f"The card in the printer lists {_mib(used)} in {files} files and {folders} folders, "
                                     f"more than the {_mib(geo['data_bytes'])} data area the fsck logs show: it is not "
                                     "the card they describe. Nothing was written.")
                stored = True
                ftps.upload_file(local, MARKER)
                ftps.download_file(MARKER, back)
                if Path(back).read_bytes() != data:
                    ftps.delete_file(MARKER)
                    stored = False
                    raise pa.Refused("registration_failed", f"{MARKER} read back from the card differs from what "
                                     "was written; it was deleted again. The card is not registered.")
        except pa.Refused:
            raise
        except Exception as exc:
            raise pa.Refused("registration_failed" if stored else "storage_unknown",
                             f"The card could not be {'written' if stored else 'read'} ({type(exc).__name__}: {exc}). "
                             + (f"It may hold a partial {MARKER}, which is refused unless it validates; "
                                "list the card before trying again." if stored else "Nothing was written.")) from None
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        try:
            evidence = measure(printer)  # the record as every upload and start will read it
        except pa.Refused as r:
            raise pa.Refused("registration_failed", f"{MARKER} was written and read back, but measuring the card "
                             f"then failed ({r.code}: {r.detail}). The card holds the record; an upload measures it "
                             "again.", storage_error=r.code) from None
        if evidence["card_id"] != registration_id:
            raise pa.Refused("registration_failed", f"After writing, the card's record reads as {evidence['card_id']}, "
                             f"not {registration_id}. The card is not registered.")
        _record(name, {"purpose": "register", "verdict": "registered", **evidence, "sd_card": sd})
        return {"registered": True, "registration_id": registration_id, "device": geo["device"],
                "fsck_used_clusters": geo["used_clusters"], "sd_card": sd, "storage": evidence}
