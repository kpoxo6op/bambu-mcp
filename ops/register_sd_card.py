#!/usr/bin/env python3
"""Record the size of the SD card that stays in the printer, from fsck logs of that card.

ops/attest_sd_card.py needs the card in a computer. This is for a card already checked on a
computer (a saved clean `fsck.fat -n -v` output, e.g. right after formatting) that went back into
the printer without a record and stays there. Run it on the bambu-mcp host as the service user,
with the installation's interpreter, while the printer is idle:

    .venv-candidate/bin/python ops/register_sd_card.py --printer p1s \\
        --fsck-log fsck-after-format.log --fsck-log fsck-final.log \\
        --evidence result.json \\
        --custody "Formatted and checked on platt 2026-11-03, put back into the printer by Boris; ..."

It parses the logs (each must be a clean fsck.fat -n output ending in its summary line; at least
one verbose, giving the data area and cluster size; all of one filesystem), writes a one-shot
registration into ~/.bambu-mcp/sd-card/ (mode 0700; download_file refuses that store, so no MCP
caller writes one), and calls the running server's register_sd_card tool over its HTTP MCP
endpoint. The server parses the logs again, requires a complete card listing, no existing record
and no more listed than that data area, writes /bambu-mcp-card.json over its own FTPS session
and reads it back. --evidence files are hashed into the receipt and not interpreted. The custody
statement says how you know the logs are of this card: nothing over the network identifies a card.

Prints the server's answer and saves it as ~/.bambu-mcp/sd-card/<id>.receipt.json. Exit 0 only
when the card is registered. --check validates the logs and writes nothing.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import storage_space  # noqa: E402


def fail(message: str) -> None:
    print(json.dumps({"registered": False, "error": message}, indent=1))
    sys.exit(1)


def read_small(path: str, limit: int) -> bytes:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as fh:
            data = fh.read(limit + 1)
    except OSError as exc:
        fail(f"{path}: {exc.strerror}. Nothing was written.")
    if len(data) > limit:
        fail(f"{path} is over {limit} bytes. Nothing was written.")
    return data


def default_url() -> str:
    try:
        port = int((Path.home() / ".bambu-mcp" / "daemon.port").read_text().strip())
    except (OSError, ValueError):
        port = 25099
    return f"http://127.0.0.1:{port}/mcp"


async def call_tool(url: str, printer: str, registration_id: str) -> dict:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    async with streamablehttp_client(url) as (rd, wr, _):
        async with ClientSession(rd, wr) as session:
            await session.initialize()
            result = await asyncio.wait_for(session.call_tool("register_sd_card", {
                "name": printer, "registration_id": registration_id, "user_permission": True}), 120)
    text = "".join(getattr(c, "text", "") for c in result.content)
    try:
        return json.loads(text)
    except ValueError:
        return {"error": "unexpected_response", "detail": text[:2000]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--printer", required=True, help="configured printer name")
    parser.add_argument("--fsck-log", action="append", required=True, metavar="PATH",
                        help="saved fsck.fat -n [-v] output of the card, oldest first (repeatable)")
    parser.add_argument("--evidence", action="append", default=[], metavar="PATH",
                        help="other saved proof (format log, round-trip result); hashed, not interpreted")
    parser.add_argument("--custody", required=True,
                        help="how the card those logs describe is the card in the printer")
    parser.add_argument("--mcp", default=None, help="the server's MCP URL (default from ~/.bambu-mcp/daemon.port)")
    parser.add_argument("--check", action="store_true", help="validate the logs only; write and call nothing")
    args = parser.parse_args()

    custody = args.custody.strip()
    if not 10 <= len(custody) <= storage_space.MAX_CUSTODY_CHARS:
        fail(f"--custody must be 10 to {storage_space.MAX_CUSTODY_CHARS} characters. Nothing was written.")
    logs = []
    for path in args.fsck_log:
        raw = read_small(path, storage_space.MAX_FSCK_LOG_BYTES)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            fail(f"{path} is not text. Nothing was written.")
        logs.append({"name": Path(path).name, "sha256": hashlib.sha256(raw).hexdigest(), "text": text,
                     "mtime": os.stat(path).st_mtime})
    try:
        geo = storage_space.card_geometry([entry["text"] for entry in logs])
    except ValueError as exc:
        fail(f"The fsck logs do not show one clean filesystem: {exc}. Nothing was written.")
    evidence = []
    for path in args.evidence:
        raw = read_small(path, storage_space.MAX_REGISTRATION_BYTES)
        evidence.append({"name": Path(path).name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
    measured_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(max(entry["mtime"] for entry in logs)))
    if args.check:
        print(json.dumps({"checked": True, "geometry": geo, "measured_at": measured_at}, indent=1))
        return

    registration_id = secrets.token_hex(16)
    registration = {"schema": storage_space.REGISTRATION_SCHEMA, "registration_id": registration_id,
                    "printer": args.printer, "created_at": time.time(), "measured_at": measured_at,
                    "custody": custody, "fsck_logs": [{k: e[k] for k in ("name", "sha256", "text")} for e in logs],
                    "evidence": evidence}
    try:
        store = storage_space.registry_dir(create=True)
    except storage_space.pa.Refused as r:
        fail(f"{r.code}: {r.detail}")
    tmp = store / f".{registration_id}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump(registration, fh, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, store / f"{registration_id}.json")

    url = args.mcp or default_url()
    try:
        answer = asyncio.run(call_tool(url, args.printer, registration_id))
    except Exception as exc:
        # The server may or may not have taken the registration; it is used once either way.
        (store / f"{registration_id}.json").unlink(missing_ok=True)
        answer = {"error": "server_unreachable", "detail": f"{url}: {type(exc).__name__}: {exc}. If the server took "
                  "the registration, the card may be registered: check get_printer_connection_status before "
                  "running this again."}
    receipt = {"registration_id": registration_id, "printer": args.printer, "measured_at": measured_at,
               "geometry": geo, "fsck_logs": registration["fsck_logs"], "evidence": evidence, "custody": custody,
               "server": url, "answer": answer}
    path = store / f"{registration_id}.receipt.json"
    path.write_text(json.dumps(receipt, indent=1))
    print(json.dumps({**answer, "receipt": str(path)}, indent=1))
    sys.exit(0 if answer.get("registered") is True else 1)


if __name__ == "__main__":
    main()
