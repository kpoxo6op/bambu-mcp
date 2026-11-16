#!/usr/bin/env python3
"""Configured preparer entry point: route a part to the catalog preparer or the Soyrack preparer.

    prepare_dispatch.py <new-out-dir> <part>

A part with parts/<part>/part.json in the print catalog (BAMBU_CATALOG, default ~/code/print-catalog)
goes to ops/catalog_preparer.py. Everything else goes to the Soyrack preflight.mjs unchanged.
Both write the same schema-2 preflight.json that bambu-mcp print admission verifies.
"""
import os, re, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CATALOG = Path(os.environ.get("BAMBU_CATALOG", Path.home() / "code" / "print-catalog"))
SOYRACK = Path(os.environ.get("BAMBU_SOYRACK_PREFLIGHT",
                              Path.home() / ".local/share/soyrack-preparer/apps/assembly-guide/cad/preflight.mjs"))

if len(sys.argv) != 3 or not re.match(r"^[a-z0-9][a-z0-9_-]{0,63}$", sys.argv[2]):
    print("usage: prepare_dispatch.py <new-out-dir> <part>", file=sys.stderr)
    sys.exit(2)
out, part = sys.argv[1], sys.argv[2]
if (CATALOG / "parts" / part / "part.json").is_file():
    os.execv(sys.executable, [sys.executable, "-I", str(HERE / "catalog_preparer.py"), out, part])
os.execv("/usr/bin/node", ["/usr/bin/node", str(SOYRACK), out, part])
