#!/usr/bin/env python3
"""Build a fresh runtime from this checkout, including the patched BPM snapshot.

Does not connect to a printer, change MCP clients, install services or replace an
existing environment. Use the same Python interpreter for bootstrap and runtime.
"""
import argparse
from pathlib import Path
import subprocess
import sys
import venv

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venv", type=Path, default=ROOT / ".venv-candidate")
    args = parser.parse_args()
    if sys.version_info < (3, 12):
        parser.error("Python 3.12+ is required; the release is checked with Python 3.14")
    target = args.venv.resolve()
    if target.exists():
        parser.error(f"Refusing to replace an existing environment: {target}")
    venv.EnvBuilder(with_pip=True).create(target)
    python = target / "bin/python"
    install = [str(python), "-m", "pip", "install", "--disable-pip-version-check"]
    subprocess.run(install + ["-r", str(ROOT / "installed-requirements.txt")], check=True)
    subprocess.run(install + ["--no-deps", str(ROOT / "vendor/bambu-printer-manager")], check=True)
    subprocess.run(install + ["--no-deps", "-e", str(ROOT)], check=True)
    subprocess.run([str(python), "-m", "pip", "check"], check=True)
    print(f"Runtime ready: {python}. No services or printer commands were started.")


if __name__ == "__main__":
    main()
