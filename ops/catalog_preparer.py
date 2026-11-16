#!/usr/bin/env python3
"""Catalog preparer: admit any committed model from the print catalog through the same rules as Soyrack.

    catalog_preparer.py <new-out-dir> <part>

The catalog is a git repository (BAMBU_CATALOG, default ~/code/print-catalog):

    profiles/machine.json  profiles/process.json  profiles/<filament>.json   (qualified Bambu Studio profiles)
    parts/<part>/part.json   {"model": "model.stl", "filament": "asa", "process": {...overlay...}, "note": "..."}
    parts/<part>/model.stl   (committed print-ready mesh, oriented as it prints)

Rules (any failure rejects the run and removes the job):
- out must not exist; inputs (part dir, profiles, this script, the support checker) committed and unchanged until the end.
- Bambu Studio 02.08.02.61 slices the committed STL as supplied (one object, same height), no slicer warning, no support.
- Every overlay key appears in the G-code header with its value.
- Brim, if any, only on the first layer. The soyrack layer support checker (policy 1) judges every other toolpath:
  brim moves are passed to it as travel moves (positions kept, extrusion removed), so it sees the part alone.
- preflight.json follows schema 2 / geometry policy 1, which bambu-mcp print admission verifies independently.
"""
import hashlib, json, os, re, shutil, struct, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve()
CATALOG = Path(os.environ.get("BAMBU_CATALOG", Path.home() / "code" / "print-catalog")).resolve()
CHECKER_DIR = Path(os.environ.get("BAMBU_SUPPORT_CHECKER_DIR",
                                  Path.home() / ".local/share/soyrack-preparer/apps/assembly-guide/cad")).resolve()
STUDIO = os.environ.get("BAMBU_STUDIO", "flatpak run com.bambulab.BambuStudio").split()
UV = os.environ.get("UV", "uv")
SLICER = "BambuStudio 02.08.02.61"
PART_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def sha256(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def git(cwd, *args):
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"git {' '.join(args)} in {cwd}: {r.stderr.strip()}")
    return r.stdout.rstrip()


def stl_height(p):
    b = Path(p).read_bytes()
    n = struct.unpack_from("<I", b, 80)[0]
    zs = [struct.unpack_from("<f", b, 84 + 50 * i + 12 + 12 * k + 8)[0] for i in range(n) for k in range(3)]
    return max(zs) - min(zs), n


def neutralise_brim(src, dst, first_layer_z):
    """Copy G-code; brim extrusions become travels. Returns (brim_moves, problem or None)."""
    feature, z, moves, problem = "", 0.0, 0, None
    with open(src) as fi, open(dst, "w") as fo:
        for line in fi:
            if line.startswith("; Z_HEIGHT:"):
                z = float(line[11:])
            elif line.startswith("; FEATURE:"):
                feature = line[10:].strip()
            elif feature == "Brim" and line.startswith(("G1 ", "G2 ", "G3 ")) and " E" in line:
                if z > first_layer_z + 1e-3 and problem is None:
                    problem = f"brim above the first layer (z {z})"
                line = re.sub(r" E-?[\d.]+", "", line)
                moves += 1
            fo.write(line)
    return moves, problem


def main():
    if len(sys.argv) != 3:
        print("usage: catalog_preparer.py <new-out-dir> <part>", file=sys.stderr)
        return 2
    out, part = Path(sys.argv[1]).resolve(), sys.argv[2]
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.mkdir()
    except OSError as e:
        print(f"Use a NEW job directory; existing jobs are never overwritten: {e}", file=sys.stderr)
        return 2
    report = {"schema_version": 2, "geometry_policy_version": 1, "pass": False, "state": "running",
              "checkedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "preparer": "catalog",
              "profiles": {}, "parts": {}}

    def save():
        tmp = out / "preflight.json.tmp"
        tmp.write_text(json.dumps(report, indent=2) + "\n")
        tmp.rename(out / "preflight.json")

    save()
    res = {"problems": []}
    try:
        if not PART_RE.match(part):
            raise RuntimeError(f"bad part name {part!r}")
        pdir = CATALOG / "parts" / part
        spec = json.loads((pdir / "part.json").read_text())
        model = pdir / spec["model"]
        filament = f"{spec.get('filament', 'asa')}.json"
        overlay = {k: str(v) for k, v in (spec.get("process") or {}).items()}
        cat_inputs = [f"parts/{part}/part.json", f"parts/{part}/{spec['model']}",
                      "profiles/machine.json", "profiles/process.json", f"profiles/{filament}"]
        if git(CATALOG, "status", "--porcelain", "--", *cat_inputs):
            raise RuntimeError(f"uncommitted catalog inputs for {part}")
        checker = CHECKER_DIR / "check-support.py"
        lockf = CHECKER_DIR / "check-support.py.lock"
        hashes = {str(p): sha256(p) for p in [*(CATALOG / f for f in cat_inputs), HERE, checker, lockf]}
        report["commit"] = git(CATALOG, "rev-parse", "HEAD")
        report["catalog"] = str(CATALOG)
        report["preflight"] = sha256(HERE)
        report["support_checker"] = sha256(checker)
        report["support_dependencies"] = sha256(lockf)
        for f in ("machine.json", "process.json", filament):
            report["profiles"][f] = sha256(CATALOG / "profiles" / f)

        stl_dir = out / "stl"; stl_dir.mkdir()
        stl = stl_dir / f"{part}.stl"
        shutil.copyfile(model, stl)
        height, tris = stl_height(stl)
        res.update({"stl": sha256(stl), "triangles": tris})
        job_dir = out / part; job_dir.mkdir()
        proc = json.loads((CATALOG / "profiles/process.json").read_text())
        proc.update(overlay)
        (job_dir / "process.json").write_text(json.dumps(proc, indent=1) + "\n")
        if overlay:
            res["process"] = {"overlay": overlay, "sha256": sha256(job_dir / "process.json")}
        with open(job_dir / "slicing.log", "w") as log:
            r = subprocess.run([*STUDIO, "--debug", "2", "--load-settings",
                                f"{CATALOG}/profiles/machine.json;{job_dir}/process.json",
                                "--load-filaments", str(CATALOG / "profiles" / filament),
                                "--arrange", "1", "--orient", "0", "--slice", "0",
                                "--export-3mf", f"{part}.gcode.3mf", "--outputdir", str(job_dir), str(stl)],
                               stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        if r.returncode:
            raise RuntimeError(f"slicer failed ({r.returncode}); see {job_dir}/slicing.log")
        result = json.loads((job_dir / "result.json").read_text())
        plate = (result.get("sliced_plates") or [None])[0]
        if result.get("return_code") != 0 or not plate:
            raise RuntimeError(f"slicing failed: {result.get('error_string')}")
        objs = plate.get("objects") or []
        if len(objs) != 1 or objs[0]["name"] != f"{part}.stl" or abs(objs[0]["bbox"]["height"] - height) > 0.01:
            res["problems"].append("the slicer did not keep this STL as supplied")
        if plate.get("warning_message"):
            res["problems"].append(f"slicer warning: {plate['warning_message']}")
        job, gcode = job_dir / f"{part}.gcode.3mf", job_dir / "plate_1.gcode"
        if not job.exists() or not gcode.exists():
            raise RuntimeError("the slicer wrote no job")
        head = gcode.read_text(errors="replace")[:400000]
        for k, v in overlay.items():
            if f"\n; {k} = {v}\n" not in head:
                res["problems"].append(f"the job was not sliced with {k} = {v}")
        res["slicer"] = (re.search(r"; (BambuStudio [\d.]+)", head[:300]) or [None, None])[1]
        if res["slicer"] != SLICER:
            res["problems"].append(f"unqualified slicer version: {res['slicer']}")
        if re.search(r"^; FEATURE: Support", gcode.read_text(errors="replace"), re.M):
            res["problems"].append("the slicer generated support")
        first = float(proc.get("initial_layer_print_height", "0.2"))
        checked = job_dir / "plate_1.checked.gcode"
        brim_moves, brim_problem = neutralise_brim(gcode, checked, first)
        if brim_problem:
            res["problems"].append(brim_problem)
        res["brim_moves_excluded_from_geometry"] = brim_moves
        gfile = job_dir / "geometry.json"
        g = subprocess.run([UV, "run", "--locked", "--script", str(checker), str(checked), str(stl),
                            str(job_dir / "result.json"), str(gfile)], capture_output=True, text=True)
        if not gfile.exists():
            res["problems"].append(f"support checker failed: {g.stderr[-400:]}")
        else:
            geo = json.loads(gfile.read_text())
            res["geometry"] = {**geo, "failures": (geo.get("failures") or [])[:20],
                               "bridges": (geo.get("bridges") or [])[:20],
                               "failures_count": len(geo.get("failures") or []),
                               "bridges_count": len(geo.get("bridges") or []), "evidence_sha256": sha256(gfile)}
            if g.returncode or geo.get("pass") is not True or geo.get("policy_version") != 1:
                res["problems"].append(f"layer support check failed: {geo.get('error') or str(len(geo.get('failures') or [])) + ' unsupported paths; see geometry.json'}")
        machine = json.loads((CATALOG / "profiles/machine.json").read_text())
        res["target"] = {"printer_model": machine["printer_model"], "nozzle_diameter": machine["nozzle_diameter"],
                         "plate_type": proc["curr_bed_type"]}
        res.update({"job": sha256(job), "minutes": round(plate["total_predication"] / 60),
                    "grams": round(plate["filaments"][0]["total_used_g"])})
        if git(CATALOG, "rev-parse", "HEAD") != report["commit"] or \
                git(CATALOG, "status", "--porcelain", "--", *cat_inputs) or \
                any(sha256(p) != h for p, h in hashes.items()):
            raise RuntimeError("inputs changed during preparation; no job admitted")
    except Exception as e:  # noqa: BLE001 — any failure rejects the run
        report["error"] = str(e)
        print(e, file=sys.stderr)
    report["parts"][part] = res
    report["pass"] = not report.get("error") and not res["problems"] and bool(res.get("job"))
    report["state"] = "complete"
    if not report["pass"]:
        res.pop("job", None)
        if PART_RE.match(part):
            (out / part / f"{part}.gcode.3mf").unlink(missing_ok=True)
    save()
    notes = res["problems"] or [f"{res.get('minutes')} min, {res.get('grams')} g"]
    print(f"{'pass' if report['pass'] else 'FAIL'}  {part}: {'; '.join(notes)}")
    print(f"{'PASS' if report['pass'] else 'FAIL'}: {out / 'preflight.json'}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
