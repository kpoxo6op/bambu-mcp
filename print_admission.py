"""
print_admission.py — A print starts only from bytes this server itself prepared and verified.

Why (2026-11-02): Soyrack shelf rims were printed without the support they needed. "Run the
geometry preflight first" was an instruction, not enforcement: upload_file and print_file
accepted any .3mf, and send_mqtt_command / send_gcode could start an SD file or extrude directly.
Nothing in the server knew whether a job had passed a check.

Chain of custody, kept in this process's memory only (a restart forgets it: fail closed):

  prepare(part)   runs the operator's preparer (BAMBU_PREFLIGHT_COMMAND_JSON) itself, with a
                  new output directory, and keeps a receipt for the exact job bytes when the
                  process exited 0 and its report says the part passed for a qualified target
                  (printer model, one nozzle, plate) that the job's own G-code also declares.
                  A report, PASS or path supplied by a caller is never consulted.
  upload          a job-like file (.3mf/.gcode) is accepted only from a receipt's job path; a
                  private snapshot must hash to the receipt; (printer, remote path) is bound.
                  Upload, rename and delete drop the bindings they could invalidate, first.
  print           the printer must be the target (model from its serial, nozzle reported on this
                  connection, the prepared plate); the remote file is downloaded and must hash to
                  the binding; the binding is consumed before publishing; a one-shot ticket
                  describes the start packet, including the verified MD5.
  publish         reliability._GuardedClient.publish() asks publish_refusal() about every printer
                  write: project_file needs that ticket; any other write naming a file, SD-card or
                  extruding G-code and G-code outside a maintenance allowlist are refused, for
                  every caller (tools, REST routes, internal code).

The preparer judges geometry, supports and profiles. This module only proves that a job is the
byte string the configured preparer produced and declared passing in this server's lifetime.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import secrets
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time
import zipfile
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

SCHEMA_VERSION = 2
GEOMETRY_POLICY_VERSION = 1
# Printers a geometry policy is qualified for: Bambu Studio printer_model -> bpm PrinterModel
# name. Policy 1 was qualified on a single-nozzle P1S only; anything else is refused.
QUALIFIED_MODELS = {1: {"Bambu Lab P1S": "P1S"}}
# Bambu Studio curr_bed_type -> bpm PlateType name (the project_file bed_type, lower-cased).
PLATE_TYPES = {"Cool Plate": "COOL_PLATE", "Engineering Plate": "ENG_PLATE",
               "High Temp Plate": "HOT_PLATE", "Textured PEI Plate": "TEXTURED_PLATE"}
PART_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
DEFAULT_TIMEOUT_S = 1800
MAX_REPORT_BYTES = 8 * 1024 * 1024
MAX_JOB_BYTES = 512 * 1024 * 1024
MAX_RECEIPTS = 64
KEEP_RUNS = 100            # newest preparer runs kept on disk; older unreferenced runs are pruned
FILE_LOCK_TIMEOUT_S = 300.0
_RUN_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{12}")
_JOB_SUFFIXES = (".3mf", ".gcode")

_NEXT_STEP = ("Run prepare_print_job(part), upload the returned job_path with upload_file, then call "
              "print_file on that remote path.")


class Refused(Exception):
    """An admission refusal. Nothing was sent to the printer because of the refused step."""

    def __init__(self, code: str, detail: str, **extra):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail, self.extra = code, detail, extra

    def as_dict(self) -> dict:
        return {"error": self.code, "detail": self.detail, **self.extra}


# ── paths ──────────────────────────────────────────────────────────────────────

def output_root() -> Path:
    """Server-owned preparer output root. Inside HOME, where the Flatpak slicer may write."""
    return Path.home() / ".bambu-mcp" / "preflight"


def _ensure_root() -> Path:
    root = output_root()
    root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        pass
    st = os.lstat(root)
    if (stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid()
            or st.st_mode & 0o022):
        raise Refused("preflight_store_untrusted",
                      f"{root} must be a directory owned by the service user and not writable by others.")
    # Resolved, so job paths compare equal to their realpath even when HOME sits behind a symlink.
    return Path(os.path.realpath(root))


def inside_store(local_path: str) -> bool:
    """True when local_path (existing or not) resolves inside the preparer output root."""
    try:
        target = Path(os.path.realpath(os.path.expanduser(str(local_path))))
        root = Path(os.path.realpath(output_root()))
    except (OSError, ValueError):
        return True
    return target == root or root in target.parents


def is_job_name(path) -> bool:
    """A name the printer can start (.3mf project or .gcode), ignoring case and trailing dots."""
    name = str(path or "").rstrip("/").rsplit("/", 1)[-1].rstrip(". ").lower()
    return name.endswith(_JOB_SUFFIXES)


def normalize_remote(path) -> str:
    """Absolute SD-card path with single separators; a trailing '/' (folder) is kept."""
    if (not isinstance(path, str) or not path or path != path.strip()
            or any(c in path for c in "\x00\r\n\\")):
        raise Refused("invalid_remote_path", f"Unusable SD card path {path!r}. Nothing was changed or sent.")
    segments = [s for s in path.split("/") if s]
    if any(s in (".", "..") for s in segments):
        raise Refused("invalid_remote_path", f"'.' and '..' are not allowed in SD card paths: {path!r}.")
    if not segments:
        return "/"
    return "/" + "/".join(segments) + ("/" if path.endswith("/") else "")


# ── preparer command (operator configuration) ─────────────────────────────────

def _overflow_uid() -> int:
    try:
        return int(Path("/proc/sys/kernel/overflowuid").read_text())
    except (OSError, ValueError):
        return 65534


def _check_trusted(path: str, executable: bool) -> str:
    """Resolve path; it and both directories (as given and resolved) must not be open to others.

    Owner must be root or the service user. Inside a user namespace root appears as the
    overflow uid, so that uid is accepted as root. World-writable files or directories fail.
    """
    real = os.path.realpath(path)
    try:
        st = os.stat(real)
    except OSError as exc:
        raise Refused("preflight_config_invalid", f"Preparer path {path} is unusable: {exc.strerror}.") from None
    if not stat.S_ISREG(st.st_mode):
        raise Refused("preflight_config_invalid", f"Preparer path {path} is not a regular file.")
    if executable and not os.access(real, os.X_OK):
        raise Refused("preflight_config_invalid", f"Preparer program {path} is not executable.")
    owners = {0, os.getuid(), _overflow_uid()}
    for target in dict.fromkeys((real, os.path.dirname(real), os.path.dirname(os.path.normpath(path)))):
        s = os.stat(target)
        if s.st_uid not in owners or s.st_mode & 0o002:
            raise Refused("preflight_command_untrusted",
                          f"{target} is world-writable or owned by another user; the preparer is not run.")
    return real


def command() -> tuple[list[str], float]:
    """The configured preparer argv and timeout. Raises Refused when missing or unusable."""
    raw = os.environ.get("BAMBU_PREFLIGHT_COMMAND_JSON", "")
    if not raw.strip():
        raise Refused("preflight_unconfigured",
                      "BAMBU_PREFLIGHT_COMMAND_JSON is not set in the service environment, so no job can be "
                      "prepared and no print can be started. See docs/recovery.md.")
    try:
        argv = json.loads(raw)
    except ValueError:
        raise Refused("preflight_config_invalid", "BAMBU_PREFLIGHT_COMMAND_JSON is not valid JSON.") from None
    if not (isinstance(argv, list) and 1 <= len(argv) <= 16 and all(
            isinstance(a, str) and a and "\x00" not in a and "\n" not in a for a in argv)):
        raise Refused("preflight_config_invalid",
                      "BAMBU_PREFLIGHT_COMMAND_JSON must be a JSON array of 1..16 non-empty strings.")
    if not argv[0].startswith("/"):
        raise Refused("preflight_config_invalid",
                      f"The preparer program must be an absolute path (no PATH lookup), not {argv[0]!r}.")
    for i, arg in enumerate(argv):
        if "/" in arg:
            if not arg.startswith("/"):
                raise Refused("preflight_config_invalid", f"Relative path {arg!r} in the preparer command.")
            _check_trusted(arg, executable=i == 0)
    raw_timeout = os.environ.get("BAMBU_PREFLIGHT_TIMEOUT_S", "").strip()
    try:
        timeout = float(raw_timeout) if raw_timeout else float(DEFAULT_TIMEOUT_S)
    except ValueError:
        timeout = -1.0
    if not 1 <= timeout <= 7200:
        raise Refused("preflight_config_invalid", "BAMBU_PREFLIGHT_TIMEOUT_S must be 1..7200 seconds.")
    return argv, timeout


# ── receipts ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Target:
    """The printer a job was prepared for, as both the report and the job's G-code declare it."""
    printer_model: str            # Bambu Studio name, e.g. "Bambu Lab P1S"
    model: str                    # bpm PrinterModel name, e.g. "P1S"
    nozzle_diameter: tuple[str, ...]
    plate_type: str               # Bambu Studio curr_bed_type, e.g. "Textured PEI Plate"
    plate: str                    # bpm PlateType name, e.g. "TEXTURED_PLATE"

    def public(self) -> dict:
        return {"printer_model": self.printer_model, "nozzle_diameter": list(self.nozzle_diameter),
                "plate_type": self.plate_type, "bed_type": self.plate.lower()}


@dataclass(frozen=True)
class Receipt:
    id: str
    part: str
    job_path: str
    sha256: str
    md5: str
    size: int
    plates: tuple[int, ...]
    # (plate, bytes of Metadata/plate_<N>.gcode): what the printer unpacks to its card to print it.
    plate_gcode_bytes: tuple[tuple[int, int], ...]
    target: Target
    run_dir: str
    report_sha256: str
    created: float
    summary: dict

    def public(self) -> dict:
        return {"receipt_id": self.id, "part": self.part, "job_path": self.job_path, "sha256": self.sha256,
                "md5": self.md5, "size": self.size, "plates": list(self.plates),
                "plate_gcode_bytes": {str(p): n for p, n in self.plate_gcode_bytes}, "target": self.target.public(),
                "run_dir": self.run_dir, "report_sha256": self.report_sha256,
                "prepared_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.created))}


@dataclass(frozen=True)
class Binding:
    printer: str
    path: str
    receipt_id: str
    part: str
    sha256: str
    md5: str
    size: int
    plates: tuple[int, ...]
    plate_gcode_bytes: tuple[tuple[int, int], ...]
    target: Target
    card_id: str                  # the SD card record the upload was measured against
    bound_at: float

    def public(self) -> dict:
        return {"receipt_id": self.receipt_id, "part": self.part, "remote_path": self.path,
                "sha256": self.sha256, "plates": list(self.plates), "target": self.target.public(),
                "card_id": self.card_id}


_lock = threading.Lock()
_receipts: "OrderedDict[str, Receipt]" = OrderedDict()
_bindings: dict[tuple[str, str], Binding] = {}
_prepare_lock = threading.Lock()
_file_locks: dict[str, threading.RLock] = {}


def _read_regular(path: Path, limit: int) -> bytes:
    """Read a regular file without following a final symlink, refusing anything over limit."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as fh:
        st = os.fstat(fh.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_size > limit:
            raise ValueError(f"{path.name} is not a regular file of at most {limit} bytes")
        return fh.read(limit + 1)


def _hash_fileobj(fh) -> tuple[str, str, int]:
    sha, md5, size = hashlib.sha256(), hashlib.md5(), 0
    for chunk in iter(lambda: fh.read(1 << 20), b""):
        sha.update(chunk)
        md5.update(chunk)
        size += len(chunk)
    return sha.hexdigest(), md5.hexdigest().upper(), size


def _hash_path(path: str) -> str:
    with open(path, "rb") as fh:
        return _hash_fileobj(fh)[0]


def _tail(path: Path, limit: int = 4000) -> str:
    try:
        with open(path, "rb") as fh:
            fh.seek(max(0, os.fstat(fh.fileno()).st_size - limit))
            return fh.read().decode("utf-8", "replace")
    except OSError:
        return ""


def _report_problems(out: Path, part: str):
    """Problems the preparer reported for part, for the caller's information only."""
    try:
        report = json.loads(_read_regular(out / "preflight.json", MAX_REPORT_BYTES))
        entry = report.get("parts", {}).get(part, {})
        return {"problems": entry.get("problems"), "geometry": entry.get("geometry"),
                "error": report.get("error")}
    except Exception:
        return None


def _validate(out: Path, part: str) -> tuple[dict, str, str, str, int, tuple[int, ...], tuple, "Target"]:
    """Check the finished run against the admission rules; see docs/print-admission.md."""
    def expect(ok, why):
        if not ok:
            raise Refused("preflight_rejected", f"{why}. The job is not admitted.",
                          report=_report_problems(out, part))

    try:
        for d in (out, out / part):
            expect(stat.S_ISDIR(os.lstat(d).st_mode), f"{d} is not a directory")
        raw = _read_regular(out / "preflight.json", MAX_REPORT_BYTES)
        report = json.loads(raw)
    except Refused:
        raise
    except (OSError, ValueError) as exc:
        raise Refused("preflight_rejected", f"No usable preflight.json from the preparer ({exc}). "
                      "The job is not admitted.") from None
    expect(isinstance(report, dict), "preflight.json is not a JSON object")
    version = report.get("schema_version")
    expect(type(version) is int and version == SCHEMA_VERSION, f"schema_version is {version!r}, not {SCHEMA_VERSION}")
    policy = report.get("geometry_policy_version")
    expect(type(policy) is int and policy == GEOMETRY_POLICY_VERSION,
           f"geometry_policy_version is {policy!r}, not {GEOMETRY_POLICY_VERSION}")
    expect(report.get("pass") is True, "the report does not say pass: true")
    expect(report.get("state") == "complete", f"the report state is {report.get('state')!r}, not 'complete'")
    expect(not report.get("error"), f"the report carries an error: {report.get('error')!r}")
    parts = report.get("parts")
    expect(isinstance(parts, dict) and set(parts) == {part}, f"the report must cover exactly the part {part!r}")
    entry = parts[part]
    expect(isinstance(entry, dict), "the part entry is not an object")
    expect(isinstance(entry.get("problems"), list) and not entry["problems"],
           f"the part has problems: {entry.get('problems')!r}")
    geometry = entry.get("geometry")
    expect(isinstance(geometry, dict) and geometry.get("pass") is True, "geometry.pass is not true")
    expect(geometry.get("policy_version") == GEOMETRY_POLICY_VERSION and type(geometry.get("policy_version")) is int,
           f"the geometry check ran policy {geometry.get('policy_version')!r}, not {GEOMETRY_POLICY_VERSION}")
    target = _target(entry.get("target"), expect)
    claimed = entry.get("job")
    expect(isinstance(claimed, str) and re.fullmatch(r"[0-9a-f]{64}", claimed or ""),
           "the part entry has no job SHA-256")
    job = out / part / f"{part}.gcode.3mf"
    try:
        fd = os.open(job, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise Refused("preflight_rejected", f"Job file {job} is unusable ({exc.strerror}). The job is not admitted.") from None
    with os.fdopen(fd, "rb") as fh:
        st = os.fstat(fh.fileno())
        expect(stat.S_ISREG(st.st_mode) and 0 < st.st_size <= MAX_JOB_BYTES,
               f"{job.name} is not a regular file of 1 byte to {MAX_JOB_BYTES} bytes")
        expect(os.path.realpath(job) == str(job), f"{job} resolves outside the run directory")
        sha, md5, size = _hash_fileobj(fh)
        expect(sha == claimed, "the job's SHA-256 does not match the report")
        fh.seek(0)
        try:
            with zipfile.ZipFile(fh) as zf:
                names = zf.namelist()
                plates = tuple(sorted({int(m.group(1)) for n in names
                                       if (m := re.fullmatch(r"Metadata/plate_(\d{1,3})\.gcode", n))}))
                expect(bool(plates), "the job has no Metadata/plate_<N>.gcode")
                # What the printer unpacks to its card for each plate. Read in full, so the
                # member's CRC proves the size (a corrupt member fails here).
                gcode_bytes = tuple((p, _member_bytes(zf, f"Metadata/plate_{p}.gcode")) for p in plates)
                # The G-code the printer runs must itself say it was sliced for the target.
                for plate in plates:
                    declared = _gcode_config(zf, f"Metadata/plate_{plate}.gcode")
                    why = _job_target_mismatch(declared, target)
                    expect(why is None, f"plate {plate} G-code {why}")
        except Refused:
            raise
        except Exception as exc:  # not a ZIP, corrupt member, unreadable entry
            expect(False, f"the job is not a readable 3MF (ZIP) archive ({type(exc).__name__}: {exc})")
    summary = {k: report.get(k) for k in ("commit", "source", "preflight", "renderer", "dependencies",
                                           "support_checker", "support_dependencies", "profiles", "checkedAt")
               if k in report}
    summary["part"] = {k: entry.get(k) for k in ("stl", "triangles", "slicer", "minutes", "grams", "watch",
                                                  "geometry") if k in entry}
    return summary, hashlib.sha256(raw).hexdigest(), sha, md5, size, plates, gcode_bytes, target


def _target(raw, expect) -> Target:
    """The report's target, required, qualified for this geometry policy and fully known."""
    expect(isinstance(raw, dict), "the part entry has no target (printer_model, nozzle_diameter, plate_type)")
    model = raw.get("printer_model")
    qualified = QUALIFIED_MODELS.get(GEOMETRY_POLICY_VERSION, {})
    expect(isinstance(model, str) and model in qualified,
           f"target printer {model!r} is not qualified for geometry policy {GEOMETRY_POLICY_VERSION} "
           f"(qualified: {sorted(qualified)})")
    nozzles = raw.get("nozzle_diameter")
    expect(isinstance(nozzles, list) and len(nozzles) == 1 and all(isinstance(n, str) for n in nozzles),
           f"target nozzle_diameter {nozzles!r} is not exactly one nozzle (single-nozzle policy)")
    try:
        diameter = float(nozzles[0])
    except ValueError:
        diameter = 0.0
    expect(0 < diameter < 2, f"target nozzle_diameter {nozzles[0]!r} is not a diameter in mm")
    plate_type = raw.get("plate_type")
    expect(isinstance(plate_type, str) and plate_type in PLATE_TYPES,
           f"target plate_type {plate_type!r} is not a known Bambu Studio bed type")
    return Target(printer_model=model, model=qualified[model], nozzle_diameter=tuple(nozzles),
                  plate_type=plate_type, plate=PLATE_TYPES[plate_type])


def _member_bytes(zf: zipfile.ZipFile, name: str) -> int:
    size = 0
    with zf.open(name) as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            size += len(chunk)
    return size


def _gcode_config(zf: zipfile.ZipFile, name: str, limit: int = 4 * 1024 * 1024) -> dict[str, str]:
    """`; key = value` settings of a Bambu G-code CONFIG block, read up to its end (bounded)."""
    found, seen = {}, 0
    with zf.open(name) as fh:
        for raw in fh:
            seen += len(raw)
            line = raw.decode("utf-8", "replace").strip()
            if line == "; CONFIG_BLOCK_END" or seen > limit:
                break
            m = re.fullmatch(r";\s*([a-z0-9_]+)\s*=\s*(.*)", line)
            if m:
                found.setdefault(m.group(1), m.group(2).strip())
    return found


def _job_target_mismatch(declared: dict[str, str], target: Target) -> str | None:
    if declared.get("printer_model") != target.printer_model:
        return f"declares printer_model {declared.get('printer_model')!r}, the target is {target.printer_model!r}"
    if declared.get("curr_bed_type") != target.plate_type:
        return f"declares curr_bed_type {declared.get('curr_bed_type')!r}, the target is {target.plate_type!r}"
    try:
        nozzles = [float(n) for n in declared.get("nozzle_diameter", "").split(",")]
    except ValueError:
        nozzles = []
    if nozzles != [float(n) for n in target.nozzle_diameter]:
        return (f"declares nozzle_diameter {declared.get('nozzle_diameter')!r}, the target is "
                f"{list(target.nozzle_diameter)!r}")
    return None


def _kill_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _prune(root: Path) -> None:
    """Keep the newest KEEP_RUNS preparer runs; never a run a live receipt points into."""
    with _lock:
        live = {Path(r.run_dir).name for r in _receipts.values()}
    runs = sorted(p.name for p in root.iterdir() if _RUN_RE.fullmatch(p.name) and p.is_dir())
    for name in runs[:-KEEP_RUNS]:
        if name not in live:
            shutil.rmtree(root / name, ignore_errors=True)
            (root / f"{name}.log").unlink(missing_ok=True)


def prepare(part: str) -> dict:
    """Run the configured preparer for one part; return the receipt or raise Refused."""
    if not isinstance(part, str) or not PART_RE.fullmatch(part):
        raise Refused("invalid_part", "part must match ^[a-z0-9][a-z0-9_-]{0,63}$ (one part name). "
                      "Nothing was run.")
    argv, timeout = command()
    if not _prepare_lock.acquire(blocking=False):
        raise Refused("preparation_in_progress", "Another job is being prepared; try again when it finishes.")
    try:
        root = _ensure_root()
        _prune(root)
        run = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{secrets.token_hex(6)}"
        out, log_path = root / run, root / f"{run}.log"
        tools = {a: _hash_path(a) for a in argv if a.startswith("/")}
        started = time.monotonic()
        fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as log_fh:
            try:
                proc = subprocess.Popen([*argv, str(out), part], cwd=root, stdin=subprocess.DEVNULL,
                                        stdout=log_fh, stderr=subprocess.STDOUT, start_new_session=True,
                                        close_fds=True)
            except OSError as exc:
                raise Refused("preflight_failed", f"The preparer could not be started: {exc}.") from None
            try:
                code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                _kill_group(proc)
                proc.wait()
                raise Refused("preflight_timeout", f"The preparer ran longer than {timeout:g}s and was killed. "
                              "The job is not admitted.", log=str(log_path), log_tail=_tail(log_path)) from None
            finally:
                # Nothing the preparer started may keep writing into its run after it is judged.
                _kill_group(proc)
        duration = round(time.monotonic() - started, 3)
        if code != 0:
            raise Refused("preflight_failed", f"The preparer exited with status {code}. The job is not admitted.",
                          report=_report_problems(out, part), log=str(log_path), log_tail=_tail(log_path))
        summary, report_sha, sha, md5, size, plates, gcode_bytes, target = _validate(out, part)
        summary.update(command=argv, command_sha256=tools, duration_s=duration, log=str(log_path))
        receipt = Receipt(id=secrets.token_hex(8), part=part, job_path=str(out / part / f"{part}.gcode.3mf"),
                          sha256=sha, md5=md5, size=size, plates=plates, plate_gcode_bytes=gcode_bytes,
                          target=target, run_dir=str(out),
                          report_sha256=report_sha, created=time.time(), summary=summary)
        with _lock:
            _receipts[receipt.id] = receipt
            while len(_receipts) > MAX_RECEIPTS:
                _receipts.popitem(last=False)
        log.info("print_admission: prepared %s receipt=%s sha256=%s plates=%s", part, receipt.id, sha, plates)
        return {"success": True, **receipt.public(), "report": summary, "valid_until": "server restart",
                "next": f"upload_file(name, local_path='{receipt.job_path}', remote_path='/{part}.gcode.3mf', "
                        "user_permission=True), then print_file on that remote path."}
    finally:
        _prepare_lock.release()


def _receipt_for(local_path: str) -> Receipt | None:
    real = os.path.realpath(os.path.expanduser(str(local_path)))
    with _lock:
        for r in _receipts.values():
            if r.job_path == real:
                return r
    return None


def snapshot_for_upload(local_path: str, remote_path: str) -> tuple[Receipt, str, str, str]:
    """(receipt, normalized remote, snapshot file, snapshot dir) for an admitted job upload.

    The snapshot is copied from the receipt's job while hashing, so the bytes uploaded are
    exactly the bytes checked. The caller removes the snapshot dir.
    """
    remote = normalize_remote(remote_path)
    if remote.endswith("/") or not remote.rstrip(". ").lower().endswith(".3mf"):
        raise Refused("remote_not_project",
                      f"A prepared job must be uploaded to an SD card path ending in .3mf, not {remote_path!r}. "
                      "Nothing was uploaded.")
    receipt = _receipt_for(local_path)
    if receipt is None:
        raise Refused("job_not_prepared",
                      f"{local_path} is not a job prepared by prepare_print_job in this server's lifetime, so it "
                      f"cannot be uploaded as a printable file. Nothing was uploaded. {_NEXT_STEP}")
    tmpdir = tempfile.mkdtemp(prefix=".upload-", dir=_ensure_root())
    snapshot = os.path.join(tmpdir, os.path.basename(receipt.job_path))
    try:
        src = os.open(receipt.job_path, os.O_RDONLY | os.O_NOFOLLOW)
        dst = os.open(snapshot, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        sha = hashlib.sha256()
        with os.fdopen(src, "rb") as fin, os.fdopen(dst, "wb") as fout:
            for chunk in iter(lambda: fin.read(1 << 20), b""):
                sha.update(chunk)
                fout.write(chunk)
        if sha.hexdigest() != receipt.sha256:
            raise Refused("job_modified",
                          f"{receipt.job_path} changed after it was prepared (SHA-256 differs from the receipt). "
                          f"Nothing was uploaded. {_NEXT_STEP}")
    except Refused:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
    except OSError as exc:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise Refused("job_unreadable", f"{receipt.job_path} cannot be read ({exc.strerror}). Nothing was uploaded.") from None
    return receipt, remote, snapshot, tmpdir


# ── bindings (printer, SD path) -> receipt ─────────────────────────────────────

def _key(printer: str, path: str) -> tuple[str, str]:
    return printer, path.casefold()  # SD cards are FAT/exFAT: names are case-insensitive


def bind(printer: str, path: str, receipt: Receipt, card_id: str) -> Binding:
    b = Binding(printer=printer, path=path, receipt_id=receipt.id, part=receipt.part, sha256=receipt.sha256,
                md5=receipt.md5, size=receipt.size, plates=receipt.plates,
                plate_gcode_bytes=receipt.plate_gcode_bytes, target=receipt.target, card_id=card_id,
                bound_at=time.time())
    with _lock:
        _bindings[_key(printer, path)] = b
    log.info("print_admission: %s %s bound to receipt %s", printer, path, receipt.id)
    return b


def drop(printer: str, path: str, why: str) -> list[str]:
    """Drop the binding for path, or every binding under it when path ends with '/'."""
    folded = path.casefold()
    with _lock:
        if path.endswith("/"):
            keys = [k for k in _bindings if k[0] == printer and k[1].startswith(folded)]
        else:
            keys = [k for k in (_key(printer, path),) if k in _bindings]
        dropped = [_bindings.pop(k).path for k in keys]
    for p in dropped:
        log.info("print_admission: %s %s admission dropped (%s)", printer, p, why)
    return dropped


def binding(printer: str, path: str) -> Binding | None:
    with _lock:
        return _bindings.get(_key(printer, path))


def consume(printer: str, expected: Binding) -> bool:
    """Remove the binding iff it is still exactly expected. One start per upload."""
    with _lock:
        if _bindings.get(_key(printer, expected.path)) is not expected:
            return False
        del _bindings[_key(printer, expected.path)]
    log.info("print_admission: %s %s admission consumed for a start", printer, expected.path)
    return True


def start_plate(admitted: Binding, bed_type) -> str:
    """The bpm PlateType name the start names: the prepared plate. 'auto' resolves to it; another
    valid plate is refused, so the printer is never told a plate the job was not prepared for."""
    requested = str(bed_type or "auto").strip().upper()
    if requested == "AUTO":
        return admitted.target.plate
    if requested != admitted.target.plate:
        raise Refused("bed_type_mismatch",
                      f"The job was prepared for the {admitted.target.plate_type} (bed_type "
                      f"{admitted.target.plate.lower()}), not {requested.lower()}. No command was sent; the admission "
                      "is kept. Use bed_type auto (or the prepared plate) with that plate on the printer.")
    return requested


def check_printer(printer, admitted: Binding) -> None:
    """Refuse unless the connected printer is the prepared target: its model (from the serial the
    live session answers on), one nozzle, and the nozzle diameter it reported since it connected.
    Unknown values refuse; nothing is inferred from the printer's configured name."""
    target, config = admitted.target, getattr(printer, "config", None)
    model = getattr(getattr(config, "printer_model", None), "name", None)
    if not model or model == "UNKNOWN":
        raise Refused("printer_model_unknown", f"This printer's model is unknown, so a job prepared for "
                      f"{target.printer_model} is not started. No command was sent.")
    if model != target.model:
        raise Refused("printer_model_mismatch", f"This printer is a {model}; the job was prepared for "
                      f"{target.printer_model}. No command was sent.")
    if getattr(getattr(config, "capabilities", None), "has_dual_extruder", True):
        raise Refused("printer_not_qualified", "Only single-nozzle printers are qualified for this geometry "
                      "policy. No command was sent.")
    reported = getattr(printer, "reported_nozzle_diameter", None)
    value = reported() if callable(reported) else None
    try:
        actual = float(value)
    except (TypeError, ValueError):
        raise Refused("nozzle_unknown", f"The printer has not reported a usable nozzle diameter since it "
                      f"(re)connected ({value!r}), so it cannot be matched to the job. No command was sent; "
                      "the admission is kept.") from None
    if not math.isfinite(actual) or actual <= 0:
        raise Refused("nozzle_unknown", f"The reported nozzle diameter {value!r} is not a finite positive "
                      "measurement. No command was sent; the admission is kept.")
    if abs(actual - float(target.nozzle_diameter[0])) > 1e-6:
        raise Refused("nozzle_mismatch", f"The printer reports a {value} mm nozzle; the job was prepared for "
                      f"{target.nozzle_diameter[0]} mm. No command was sent; the admission is kept.")


@contextmanager
def file_lock(printer: str):
    """Serialize this server's SD-card mutations and print verification for one printer."""
    with _lock:
        lock = _file_locks.setdefault(printer, threading.RLock())
    if not lock.acquire(timeout=FILE_LOCK_TIMEOUT_S):
        raise Refused("file_operation_busy", "Another SD card operation on this printer is still running. "
                      "Nothing was changed or sent.")
    try:
        yield
    finally:
        lock.release()


def remote_digest(printer, path: str) -> tuple[str, str, int, str, str]:
    """Download path over the session's FTPS and hash it: (sha256, MD5, size, local copy, temp dir).

    The caller removes the temp dir. Download errors propagate (ftplib.error_perm: missing).
    """
    tmpdir = tempfile.mkdtemp(prefix=".verify-", dir=_ensure_root())
    local = os.path.join(tmpdir, "remote.gcode.3mf")
    try:
        printer.download_sdcard_file(path, local)
        with open(local, "rb") as fh:
            sha, md5, size = _hash_fileobj(fh)
    except BaseException:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise
    return sha, md5, size, local, tmpdir


# ── start ticket and the final publish boundary ────────────────────────────────

_tls = threading.local()


def _flag(value):
    if value is True or (type(value) is int and value == 1):
        return True
    if value is False or (type(value) is int and value == 0):
        return False
    return None


@dataclass(frozen=True)
class StartTicket:
    """The one project_file packet print_file is about to publish for an admitted upload."""
    path: str
    plate: int
    use_ams: bool
    ams_mapping: tuple[int, ...] | None  # None: external spool, bpm encodes -1 per filament
    bed_type: str
    bed_leveling: bool
    flow: bool
    timelapse: bool
    md5: str

    def mismatch(self, body: dict) -> str | None:
        mapping = body.get("ams_mapping")
        if self.ams_mapping is None:
            mapping_ok = isinstance(mapping, list) and bool(mapping) and all(type(x) is int and x == -1 for x in mapping)
        else:
            mapping_ok = isinstance(mapping, list) and tuple(mapping) == self.ams_mapping
        checks = {
            "file": body.get("file") == self.path,
            "url": body.get("url") in (f"file:///sdcard{self.path}", f"ftp://{self.path}"),
            "plate": body.get("param") == f"Metadata/plate_{self.plate}.gcode",
            "use_ams": body.get("use_ams") is self.use_ams,
            "ams_mapping": mapping_ok,
            "bed_type": body.get("bed_type") == self.bed_type,
            "bed_leveling": _flag(body.get("bed_leveling")) is self.bed_leveling,
            "flow_cali": _flag(body.get("flow_cali")) is self.flow,
            "timelapse": _flag(body.get("timelapse")) is self.timelapse,
            "md5": str(body.get("md5") or "").upper() == self.md5,
        }
        bad = [k for k, ok in checks.items() if not ok]
        return ", ".join(bad) if bad else None


@contextmanager
def start_ticket(ticket: StartTicket):
    """Allow exactly one matching project_file publish from this thread inside the block."""
    _tls.ticket = ticket
    try:
        yield
    finally:
        _tls.ticket = None


# Commands that start a file other than through an admitted project_file.
_FILE_START_COMMANDS = frozenset({"gcode_file", "project_prepare"})

# Maintenance G-code a raw caller or dedicated tool may send: heaters, fans, homing, jogging
# without extrusion, motors off, waits and reports. Value = parameter letters allowed.
_GCODE_ALLOWED: dict[str, frozenset[str]] = {
    "G0": frozenset("XYZF"), "G1": frozenset("XYZF"), "G4": frozenset("PS"), "G28": frozenset("XYZ"),
    "G90": frozenset(), "G91": frozenset(),
    "M17": frozenset("XYZE"), "M18": frozenset("XYZES"), "M84": frozenset("XYZES"),
    "M104": frozenset("ST"), "M109": frozenset("ST"), "M140": frozenset("S"), "M190": frozenset("S"),
    "M141": frozenset("S"), "M191": frozenset("S"), "M106": frozenset("PS"), "M107": frozenset("P"),
    "M105": frozenset(), "M114": frozenset(), "M400": frozenset("SPU"),
}
_GCODE_LINE = re.compile(r"(?:[A-Z][-+]?(?:\d+\.?\d*|\.\d+)?\s*)+")
_GCODE_WORD = re.compile(r"([A-Z])([-+]?(?:\d+\.?\d*|\.\d+))?")


def gcode_refusal(text) -> str | None:
    """None when every line of text is allowlisted maintenance G-code; otherwise why not.

    No extrusion (no E on moves), no SD-card file selection/start/write, no macros, tool changes
    or unknown commands. Comments and blank lines are ignored.
    """
    if not isinstance(text, str):
        return "G-code is not allowed: the payload is not text"
    for number, raw in enumerate(text.splitlines(), 1):
        line = re.sub(r"\([^)]*\)", " ", raw.split(";", 1)[0]).strip().upper()
        line = re.sub(r"^N\d+\s*", "", re.sub(r"\*\d+\s*$", "", line)).strip()
        if not line:
            continue
        refused = f"G-code line {number} {raw.strip()[:80]!r} is not allowed"
        head = re.match(r"([GM])(\d+)(?![\d.])", line)
        if head is None:
            return f"{refused}: it does not start with a G or M command (no tool changes or other words)"
        cmd = f"{head.group(1)}{int(head.group(2))}"
        allowed = _GCODE_ALLOWED.get(cmd)
        if allowed is None:
            return (f"{refused}: {cmd} is not a maintenance command (no SD-card file commands, extrusion, macros "
                    "or tool changes; prints start only through print_file)")
        if not _GCODE_LINE.fullmatch(line):
            return f"{refused}: only numeric parameters may follow {cmd}"
        for param, _ in _GCODE_WORD.findall(line)[1:]:
            if param not in allowed:
                return (f"{refused}: parameter {param} on {cmd}"
                        + (" (no extrusion through raw G-code)" if param == "E" else ""))
    return None


def publish_refusal(payload) -> str | None:
    """The final policy for one printer write (called by reliability for every non-read publish)."""
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        msg = json.loads(payload)
    except Exception:
        return "printer write is not a JSON object"
    if not isinstance(msg, dict) or not msg:
        return "printer write is not a JSON object"
    for section, body in msg.items():
        if not isinstance(body, dict):
            return f"printer write section {section!r} is not an object"
        cmd = body.get("command")
        if cmd == "project_file":
            ticket, _tls.ticket = getattr(_tls, "ticket", None), None
            if ticket is None:
                return ("print start refused: only print_file starts a job, and only one prepared by "
                        "prepare_print_job and uploaded by upload_file")
            why = ticket.mismatch(body)
            if why:
                return f"print start refused: the packet differs from the admitted job ({why})"
            continue
        if cmd in _FILE_START_COMMANDS or "url" in body or "file" in body:
            return f"{section}.{cmd} refers to a file; only an admitted print_file start may"
        if cmd == "gcode_line":
            why = gcode_refusal(body.get("param"))
            if why:
                return why
    return None


# ── raw MQTT (send_mqtt_command and POST /api/send_mqtt_command) ──────────────

# Non-start commands a raw caller may publish: the ones dedicated tools send, minus every start
# and G-code (send_gcode). Anything else is refused; there is no override.
RAW_MQTT_ALLOWED: dict[str, frozenset[str]] = {
    "print": frozenset({
        "pause", "resume", "stop", "print_speed", "skip_objects", "clean_print_error", "print_option",
        "ams_change_filament", "ams_control", "ams_filament_setting", "ams_user_setting",
        "ams_filament_drying", "ams_get_rfid", "unload_filament", "manual_ams_bind",
        "extrusion_cali_sel", "extrusion_cali_set", "extrusion_cali_get", "extrusion_cali_get_result",
        "flowrate_get_result", "select_extruder", "set_nozzle", "refresh_nozzle", "set_ctt", "set_airduct",
    }),
    "system": frozenset({"ledctrl", "uiop", "set_accessories", "get_accessories"}),
    "xcam": frozenset({"xcam_control_set"}),
    "info": frozenset({"get_version"}),
    "pushing": frozenset({"pushall"}),
}


def raw_command_refusal(msg) -> str | None:
    """None when msg is one allowlisted non-start command; otherwise why it is refused."""
    if not isinstance(msg, dict) or len(msg) != 1:
        return "A raw command must be a JSON object with exactly one section, e.g. {\"print\": {...}}."
    (section, body), = msg.items()
    if not isinstance(body, dict) or not isinstance(body.get("command"), str):
        return f"Section {section!r} must be an object with a string 'command'."
    cmd = body["command"]
    if cmd == "project_file" or cmd in _FILE_START_COMMANDS or "url" in body or "file" in body:
        return f"Raw MQTT cannot start or reference a file. {_NEXT_STEP}"
    if cmd == "gcode_line":
        return "Raw G-code goes through send_gcode (maintenance G-code only)."
    if cmd not in RAW_MQTT_ALLOWED.get(section, ()):
        return (f"{section}.{cmd} is not on the raw MQTT allowlist (non-start maintenance commands only); "
                "use the dedicated tool for it.")
    return None
