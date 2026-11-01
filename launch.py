#!/usr/bin/env python3
"""Shared HTTP / standalone stdio launcher; credentials remain in Bambu Studio."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.umask(0o077)
if sys.argv[1:] not in ([], ["--http"]):
    raise SystemExit("Usage: launch.py [--http]")
transport = "streamable-http" if sys.argv[1:] else "stdio"
env = os.environ.copy()
defaults = {
    "BAMBU_STUDIO_CONFIG": str(Path.home()/".var/app/com.bambulab.BambuStudio/config/BambuStudio/BambuStudio.conf"),
    "BAMBU_PRINTER_NAME": "p1s",
    "BAMBU_MCP_DISABLE_REST": "1",
    "BAMBU_MCP_PORT": "25099",
    "BAMBU_MCP_ALLOW_CLIENT_CONFIG_WRITE": "0",
    "BAMBU_MCP_FTPS_TIMEOUT": "30",
    "PYTHONUNBUFFERED": "1",
}
for key, value in defaults.items():
    env.setdefault(key, value)
missing = [key for key in ("BAMBU_PRINTER_HOST", "BAMBU_PRINTER_SERIAL") if not env.get(key)]
if missing:
    raise SystemExit("Configure " + ", ".join(missing) + "; see docs/recovery.md")
os.chdir(ROOT)
os.execve(str(ROOT / ".venv-candidate/bin/python"),
          [str(ROOT / ".venv-candidate/bin/python"), str(ROOT / "server.py"), "--transport", transport], env)
