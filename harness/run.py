#!/usr/bin/env python3
"""E2E release gate / nightly soak. Never connects to a real printer.

Every test child runs in a fresh network namespace with read-only host files and
private /tmp + HOME. Bubblewrap failing means the run fails, never falls back.
Reports/logs/seeds survive outside the sandbox; failures make the command fail.
"""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]

FREEZE = """
import hashlib, importlib.metadata as m, json
deps = sorted((d.metadata['Name'], d.version) for d in m.distributions())
prints = {}
for name in ("bambu-printer-manager", "paho-mqtt", "mcp"):
    try:
        d = m.distribution(name)
    except m.PackageNotFoundError:
        prints[name] = None
        continue
    h = hashlib.sha256()
    for f in sorted(d.files or [], key=str):
        if f.suffix == ".py":
            h.update(str(f).encode()); h.update(d.locate_file(f).read_bytes())
    prints[name] = {"version": d.version, "installed_py_sha256": h.hexdigest(),
                    "direct_url": json.loads(d.read_text("direct_url.json") or "null")}
print(json.dumps({"dependencies": deps, "dependency_fingerprints": prints}))
"""

def source_hash():
    h = hashlib.sha256()
    for p in sorted(ROOT.rglob("*.py")):
        if any(x.startswith(".") for x in p.relative_to(ROOT).parts) or "evidence" in p.parts:
            continue
        h.update(str(p.relative_to(ROOT)).encode()); h.update(p.read_bytes())
    return h.hexdigest()

def interpreter_dirs(python):
    """Directories the test interpreter needs that may sit under the hidden home directory."""
    out = subprocess.run([str(python), "-I", "-c", "import sys; print(sys.base_prefix)"],
                         capture_output=True, text=True, check=True, timeout=30).stdout.strip()
    venv_dir = Path(os.path.abspath(python)).parent.parent
    dirs = {Path(out), Path(out).resolve(), Path(python).resolve().parent.parent, venv_dir.resolve()}
    cfg = Path(python).parent.parent / "pyvenv.cfg"
    for line in cfg.read_text().splitlines() if cfg.exists() else []:
        key, _, value = line.partition("=")
        if key.strip() == "home":
            dirs |= {Path(value.strip()).parent, Path(value.strip()).resolve().parent}
    return sorted(d for d in dirs if d.is_dir())

def run_child(args, python, log, timeout):
    env = {"PATH":"/usr/local/bin:/usr/bin:/bin", "LANG":"C.UTF-8",
           "PYTHONDONTWRITEBYTECODE":"1", "PYTHONUNBUFFERED":"1"}
    # The real home directory (Bambu Studio's LAN access code, printer.env, MCP configs) and
    # the private evidence directory are hidden; only the checkout and interpreter come back.
    hide = ["--tmpfs", str(Path.home())]
    for d in [ROOT, *interpreter_dirs(python)]:
        hide += ["--ro-bind", str(d), str(d)]
    if (ROOT/"evidence").is_dir():
        hide += ["--tmpfs", str(ROOT/"evidence")]
    cmd = ["/usr/bin/bwrap", "--unshare-user", "--uid", "0", "--gid", "0",
           "--cap-add", "CAP_NET_BIND_SERVICE", "--unshare-net", "--unshare-pid", "--die-with-parent",
           "--ro-bind", "/", "/", *hide, "--dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp",
           "--dir", "/tmp/home", "--setenv", "HOME", "/tmp/home",
           "--setenv", "XDG_CONFIG_HOME", "/tmp/home/.config",
           "--setenv", "XDG_CACHE_HOME", "/tmp/home/.cache",
           "--setenv", "BAMBU_HARNESS_ISOLATED", "1", "--chdir", str(ROOT),
           str(python), *args]
    start = time.monotonic()
    with log.open("w") as out:
        p = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=5)
            code = 124
    return {"command":args,"exit_code":code,"duration_s":round(time.monotonic()-start,3),
            "passed":code==0,"log":log.name}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--cycles", type=int, default=8)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--python", type=Path, default=ROOT/".venv-candidate/bin/python")
    ap.add_argument("--output", type=Path, default=ROOT/"evidence/harness")
    args = ap.parse_args()
    if not 1 <= args.rounds <= 1000 or not 4 <= args.cycles <= 1000:
        ap.error("rounds must be 1..1000; cycles 4..1000")
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=True)
    lock = (args.output/"runner.lock").open("w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("harness is already running")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = args.output/stamp
    run.mkdir()
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**32)
    report = {"started":stamp,"source_sha256":source_hash(),"python":str(args.python),
              "seed":seed,"network":"isolated loopback-only namespace", "results":[],"passed":False}
    # Preserve exact installed versions alongside each run. The local BPM build and the
    # transport libraries are also fingerprinted by installed file content: a rebuilt local
    # dependency keeps its version string, so the version alone does not identify it.
    freeze = subprocess.run([str(args.python),"-I","-c",FREEZE],
        capture_output=True,text=True,check=True,timeout=30)
    report.update(json.loads(freeze.stdout))
    corpus_file = args.output/"regression-seeds.json"
    try:
        corpus = json.loads(corpus_file.read_text())
    except (OSError,ValueError):
        corpus = []
    seeds = list(dict.fromkeys(corpus+[seed+i for i in range(args.rounds)]))
    jobs = [["harness/wire_campaign.py", str(s), str(args.cycles)] for s in seeds]
    for i, cmd in enumerate(jobs):
        result = run_child(cmd, args.python, run/f"{i:02}.log", max(120, args.cycles*15))
        report["results"].append(result)
        print(json.dumps(result), flush=True)
        (run/"report.json").write_text(json.dumps(report, indent=2))
        # Fail fast leaves the first reproducer and its exact seed. Don't hide regressions
        # behind a retry or continue spending the entire night's budget after a failure.
        if not result["passed"]:
            if cmd[0] == "harness/wire_campaign.py":
                corpus = list(dict.fromkeys(corpus+[int(cmd[1])]))[-50:]
                corpus_file.write_text(json.dumps(corpus,indent=2))
            break
    report["passed"] = len(report["results"]) == len(jobs) and all(r["passed"] for r in report["results"])
    report["finished"] = dt.datetime.now(dt.timezone.utc).isoformat()
    (run/"report.json").write_text(json.dumps(report, indent=2))
    temp = args.output/"latest.tmp"
    temp.write_text(json.dumps(report, indent=2)); temp.replace(args.output/"latest.json")
    # Bound routine soak artifacts; keep the 100 newest reports (including failures).
    # Failing seed identities stay in the regression corpus and run first in later campaigns.
    import shutil
    for old in sorted(args.output.glob("????????T??????Z"))[:-100]:
        if old.is_dir():
            shutil.rmtree(old)
    print(f"REPORT {run/'report.json'}", flush=True)
    return 0 if report["passed"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
