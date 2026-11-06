#!/usr/bin/env python3
"""Fixed preparer process for the E2E campaign: the server-configured peer at the process boundary.

The server runs it as BAMBU_PREFLIGHT_COMMAND_JSON + [<out>, <part>], exactly as it runs the
Soyrack preflight. It renders, slices and checks NOTHING: the part name selects an outcome a
real preparer can produce (a pass, a failed process, a forged or incomplete report, a symlinked
job, a hang), so the campaign proves which of them the server admits. Real CAD, slicer and
support geometry are covered by the Soyrack repository's own preflight E2E.

The geometry policy parts must be judged under their exact names, so the campaign chooses their
outcome in the scenario file (JSON): "real" copies a preparer output from run.py's
--preparer-artifacts; otherwise fixtures/preflight-<anchor>.json, a report the real preparer wrote
at the commit in its "commit" field, gets a fixture job and the scenario's edits.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from printer_peers import project_archive  # noqa: E402

out, part = Path(sys.argv[1]), sys.argv[2]
out.mkdir()  # a new run never reuses a directory; an existing one fails here
(out / part).mkdir()
job = out / part / f"{part}.gcode.3mf"
try:
    scenario = json.loads(Path(os.environ["BAMBU_HARNESS_PREPARER_SCENARIO"]).read_text())
except (KeyError, OSError):
    scenario = {}
if "real" in scenario:
    source = Path(os.environ["BAMBU_HARNESS_PREPARER_ARTIFACTS"]) / scenario["real"]
    shutil.copyfile(source / "preflight.json", out / "preflight.json")
    if (source / part / job.name).exists():
        shutil.copyfile(source / part / job.name, job)
    sys.exit(0)
anchor = Path(__file__).resolve().parent / "fixtures" / f"preflight-{scenario.get('anchor', part)}.json"
if anchor.exists():
    report = json.loads(anchor.read_text())
    (name, entry), = report["parts"].items()
    report["parts"] = {part: entry}
    job.write_bytes(project_archive(marker=part, **scenario.get("job", {})))
    entry["job"] = hashlib.sha256(job.read_bytes()).hexdigest()
    sections = {"report": report, "entry": entry, "target": entry["target"], "geometry": entry["geometry"],
                "interface": entry["geometry"].get("release_interface", {})}
    for section, key, value in scenario.get("set", []):
        sections[section][key] = value
    for section, key in scenario.get("delete", []):
        del sections[section][key]
    (out / "preflight.json").write_text(json.dumps(report))
    sys.exit(0)
# job_target_mismatch: the report names the P1S 0.4 target, but the job's G-code was sliced for 0.6.
# big_gcode: a small deflated job whose plate G-code unpacks to 24 MiB (the 2026-11-03 job was 1.4 MB
# compressed and 6.9 MB unpacked).
job.write_bytes(project_archive(marker=part, nozzle="0.6" if part == "job_target_mismatch" else "0.4",
                                gcode_moves=24 * 1024 * 1024 // 36 if part == "big_gcode" else 0))
entry = {"stl": "0" * 64, "triangles": 12, "problems": [],
         "geometry": {"pass": True, "policy_version": 1, "policy": "fixture"},
         "target": {"printer_model": "Bambu Lab P1S", "nozzle_diameter": ["0.4"], "plate_type": "Textured PEI Plate"},
         "job": hashlib.sha256(job.read_bytes()).hexdigest(), "slicer": "fixture", "minutes": 1, "grams": 1,
         "watch": []}
report = {"schema_version": 2, "geometry_policy_version": 1, "pass": True, "state": "complete",
          "commit": "fixture", "profiles": {}, "parts": {part: entry}}
code = 0
if part == "fail_exit":
    code = 1                                  # passing report, failed process
elif part == "no_report":
    report = None
elif part == "forged_hash":
    entry["job"] = "f" * 64                   # report claims other bytes
elif part == "geometry_fail":
    entry["geometry"]["pass"] = False         # pass: true at the top, geometry failed
elif part == "problems":
    entry["problems"] = ["rim at z 12 mm crosses 9 mm of open air without support"]
elif part == "old_schema":
    report["schema_version"] = 1
elif part == "running":
    report["state"] = "running"
elif part == "extra_part":
    report["parts"]["other_part"] = dict(entry)
elif part == "no_target":
    del entry["target"]
elif part == "target_x1c":
    entry["target"]["printer_model"] = "Bambu Lab X1 Carbon"   # not qualified for policy 1
elif part == "symlink_job":
    elsewhere = out / "elsewhere.bin"
    job.rename(elsewhere)
    job.symlink_to(elsewhere)                 # same bytes and hash, reached through a symlink
if report is not None:
    (out / "preflight.json").write_text(json.dumps(report))
if part == "hang":
    time.sleep(60)                            # a passing report from a process that never finished
sys.exit(code)
