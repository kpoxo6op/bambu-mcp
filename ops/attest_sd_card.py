#!/usr/bin/env python3
"""Record a printer SD card's size on the card, after checking its filesystem.

The printer reports neither the card's capacity nor its free space (docs/storage-space.md), so
bambu-mcp reads /bambu-mcp-card.json from the card and refuses to upload or start a print
without it. Run this on a computer with the card inserted and NOT mounted:

    sudo python3 ops/attest_sd_card.py /dev/mmcblk0p1     # or without sudo if you can read it

It requires a clean `fsck.fat -n -v` (no errors, no wrong free-cluster summary), takes the data
area and cluster size from the filesystem, mounts the card with udisksctl, writes the record
with a new random card id, unmounts, and requires fsck to be clean again. If the card fails after
the record was written (the write, the read-back or the second fsck), the record is deleted again
so the server cannot take it as proof; the output says whether that worked. Otherwise it deletes,
formats and repairs nothing: a card that fails fsck is reported and left as it is. FAT32/FAT16 only.
"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
import time

MARKER = "bambu-mcp-card.json"
SCHEMA = "bambu-mcp-sd-card/1"
FSCK = next((p for p in ("/usr/sbin/fsck.fat", "/sbin/fsck.fat") if os.path.exists(p)), "fsck.fat")
PROBLEMS = re.compile(r"wrong|differ|truncat|reclaim|correct|bad |invalid|lost|free cluster summary|"
                      r"unable|error|has no|cluster chain|duplicate|dirty", re.I)


def fail(message: str) -> None:
    print(json.dumps({"attested": False, "error": message}))
    sys.exit(1)


class Unclean(Exception):
    pass


def fsck(device: str) -> dict:
    run = subprocess.run([FSCK, "-n", "-v", device], capture_output=True, text=True)
    out = run.stdout + run.stderr
    lines = [line for line in out.splitlines() if not line.startswith(("fsck.fat", "Checking", "Boot sector"))]
    problems = [line.strip() for line in lines if PROBLEMS.search(line)]
    if run.returncode != 0 or problems:
        raise Unclean(f"{FSCK} -n exited {run.returncode}; the filesystem is not clean, so the card's free space "
                      f"cannot be trusted: {problems[:5] or out.strip()[-400:]}.")
    cluster = re.search(r"^\s*(\d+) bytes per cluster", out, re.M)
    data = re.search(r"^\s*(\d+) data clusters \((\d+) bytes\)", out, re.M)
    used = re.search(r": (\d+) files, (\d+)/(\d+) clusters\s*$", out, re.M)
    if not (cluster and data and used):
        raise Unclean(f"{FSCK} did not report the filesystem geometry (not FAT?).")
    cluster_bytes, clusters, data_bytes = int(cluster.group(1)), int(data.group(1)), int(data.group(2))
    if clusters * cluster_bytes != data_bytes or int(used.group(3)) != clusters:
        raise Unclean(f"inconsistent geometry from {FSCK}.")
    return {"cluster_bytes": cluster_bytes, "data_bytes": data_bytes, "clusters": clusters,
            "used_clusters": int(used.group(2)), "files": int(used.group(1)),
            "fsck": out.splitlines()[0].strip()}


def udisks(*args: str) -> str | None:
    """udisksctl's output, or None after printing why it failed."""
    run = subprocess.run(["udisksctl", *args, "--no-user-interaction"], capture_output=True, text=True)
    if run.returncode != 0:
        print(f"udisksctl {' '.join(args)} failed: {(run.stderr or run.stdout).strip()}", file=sys.stderr)
        return None
    return run.stdout


def mount(device: str) -> Path | None:
    out = udisks("mount", "-b", device)
    match = re.search(r" at (/.+?)\.?\s*$", (out or "").strip())
    return Path(match.group(1)) if match and os.path.isdir(match.group(1)) else None


def withdraw(device: str, why: str) -> None:
    """The card failed after the record was written: delete it, so it is never taken as proof."""
    mountpoint, removed = mount(device), False
    if mountpoint is not None:
        try:
            for name in (MARKER, f".{MARKER}.tmp"):
                (mountpoint / name).unlink(missing_ok=True)
            dfd = os.open(mountpoint, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            removed = not (mountpoint / MARKER).exists()
        except OSError as exc:
            print(f"deleting the record failed: {exc}", file=sys.stderr)
        removed = udisks("unmount", "-b", device) is not None and removed
    if removed:
        fail(f"{why} The record written to the card was deleted again: the card is NOT attested. Check or "
             "format it, then attest again.")
    fail(f"{why} The record written to the card could NOT be confirmed deleted: delete /{MARKER} from the "
         "card yourself before it goes back in the printer, check or format the card, then attest again.")


def mounted(device: str) -> bool:
    real = os.path.realpath(device)
    with open("/proc/self/mounts") as fh:
        return any(os.path.realpath(line.split()[0]) == real for line in fh if line.startswith("/"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("device", help="the card's FAT partition, e.g. /dev/mmcblk0p1 or /dev/sdb1")
    args = parser.parse_args()
    try:
        if not stat.S_ISBLK(os.stat(args.device).st_mode):
            fail(f"{args.device} is not a block device. Nothing was written.")
    except OSError as exc:
        fail(f"{args.device}: {exc.strerror}. Nothing was written.")
    if not os.access(args.device, os.R_OK | os.W_OK):
        fail(f"{args.device} is not readable and writable by this user; run with sudo. Nothing was written.")
    if mounted(args.device):
        fail(f"{args.device} is mounted; unmount it first so fsck sees the card as written. Nothing was written.")
    try:
        before = fsck(args.device)
    except Unclean as exc:
        fail(f"{exc} Nothing was written.")
    mountpoint = mount(args.device)
    if mountpoint is None:
        udisks("unmount", "-b", args.device)
        fail(f"could not mount {args.device} to write the record. Nothing was written.")
    record = {"schema": SCHEMA, "card_id": secrets.token_hex(16), "data_bytes": before["data_bytes"],
              "cluster_bytes": before["cluster_bytes"],
              "attested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "basis": f"{before['fsck']} -n clean; data area and cluster size from the filesystem",
              "used_clusters_at_attestation": before["used_clusters"]}
    error = None
    try:
        tmp = mountpoint / f".{MARKER}.tmp"
        with open(tmp, "w") as fh:
            json.dump(record, fh, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, mountpoint / MARKER)
        dfd = os.open(mountpoint, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
        if json.loads((mountpoint / MARKER).read_text()) != record:
            error = "The record read back differs from the one written."
    except (OSError, ValueError) as exc:
        error = f"Writing the record failed ({exc})."
    finally:
        if udisks("unmount", "-b", args.device) is None and error is None:
            error = f"{args.device} could not be unmounted after the record was written."
    if error is None:
        try:
            after = fsck(args.device)
            if after["data_bytes"] != before["data_bytes"]:
                error = "The filesystem's data area changed while the record was written."
        except Unclean as exc:
            error = f"After the record was written: {exc}"
    if error is not None:
        withdraw(args.device, error)
    print(json.dumps({"attested": True, "device": args.device, "card_id": record["card_id"],
                      "data_bytes": record["data_bytes"], "cluster_bytes": record["cluster_bytes"],
                      "used_clusters": after["used_clusters"], "free_bytes":
                      (after["clusters"] - after["used_clusters"]) * after["cluster_bytes"],
                      "fsck_after": "clean"}, indent=1))


if __name__ == "__main__":
    main()
