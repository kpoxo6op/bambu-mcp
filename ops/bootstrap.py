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
    parser.add_argument("--reuse", action="store_true",
                        help="Refresh an existing environment in place (after a vendor or pin change). "
                             "Restart bambu-mcp.service afterwards; keep a copy for rollback first.")
    args = parser.parse_args()
    if sys.version_info < (3, 12):
        parser.error("Python 3.12+ is required; the release is checked with Python 3.14")
    target = args.venv.resolve()
    if target.exists() and not args.reuse:
        parser.error(f"Refusing to replace an existing environment: {target} (see --reuse)")
    if not target.exists():
        venv.EnvBuilder(with_pip=True).create(target)
    python = target / "bin/python"
    if subprocess.run([str(python), "-m", "pip", "--version"], capture_output=True).returncode != 0:
        # An environment created by uv (or `venv --without-pip`) has no pip yet.
        subprocess.run([str(python), "-m", "ensurepip", "--upgrade"], check=True)
    install = [str(python), "-m", "pip", "install", "--disable-pip-version-check"]
    subprocess.run(install + ["-r", str(ROOT / "installed-requirements.txt")], check=True)
    # The patched BPM comes only from vendor/; its +bambumcp local version is never on PyPI,
    # so nothing can resolve the MCP's pin to an unpatched upstream build.
    subprocess.run(install + ["--no-deps", "--force-reinstall", str(ROOT / "vendor/bambu-printer-manager")],
                   check=True)
    subprocess.run(install + ["--no-deps", "-e", str(ROOT)], check=True)
    subprocess.run([str(python), "-m", "pip", "check"], check=True)
    print(f"Runtime ready: {python}. No services or printer commands were started.")


if __name__ == "__main__":
    main()
