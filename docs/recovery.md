# Recover on another Linux machine

Everything needed to rebuild the software is in this repository: MCP source,
E2E peers, the patched printer-library source, pinned runtime dependencies and
systemd templates. Python packages are downloaded from PyPI during bootstrap.
The printer's identity and LAN access code are configured separately.

## Rebuild and verify

Install Python 3.14 with venv support, Git, OpenSSL and bubblewrap. Clone to the
standard service path under your own home directory:

```sh
mkdir -p ~/.local/share
git clone https://github.com/kpoxo6op/bambu-mcp.git ~/.local/share/bambu-mcp
cd ~/.local/share/bambu-mcp
python3.14 ops/bootstrap.py
.venv-candidate/bin/python harness/run.py --rounds 3 --cycles 8
```

A passing result is written to `evidence/harness/latest.json`. Every child test
runs in an isolated network namespace. Do not bypass a failed namespace setup.
The server, Studio and another printer client are not needed for this check.

## Configure your printer

Connect Bambu Studio to the printer in LAN mode. Its configuration must contain
the LAN access code for the printer serial. The access code can be read from the
printer's network settings if the old laptop is unavailable.

```sh
install -d -m 700 ~/.config/bambu-mcp
install -m 600 ops/printer.env.example ~/.config/bambu-mcp/printer.env
```

Edit `~/.config/bambu-mcp/printer.env`: set the printer host and serial, and set
`BAMBU_STUDIO_CONFIG` if Studio is not installed with Flatpak. The environment
file contains the credential-source path, not the access code. The launcher
uses the current user's home directory; no `/home/boris` path is required.

Printing needs the job preparer: set `BAMBU_PREFLIGHT_COMMAND_JSON` to the
absolute command that renders, slices and checks a part (for the Soyrack guide,
`/usr/bin/node` and its `preflight.mjs`). Without it the server starts, but
`prepare_print_job` refuses and so does every print. See
[print-admission.md](print-admission.md) for the rules and a check run.

## Start the shared service

For the standard checkout location above:

```sh
mkdir -p ~/.config/systemd/user
cp ops/bambu-mcp.service harness/bambu-mcp-{watchdog,harness}.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now bambu-mcp.service bambu-mcp-watchdog.timer bambu-mcp-harness.timer
```

Connect MCP clients to `http://127.0.0.1:25099/mcp` using Streamable HTTP. They
share the same printer session. The watchdog and nightly jobs run while the user
systemd manager is running; GitHub Actions runs independently of this computer.

Before sending a print, call `get_printer_connection_status`, `get_print_progress`
and `get_camera_image`. Telemetry must be live and the physical bed must be ready.
Software restoration does not itself start heating, loading or printing. Jobs
prepared before a restart are forgotten: run `prepare_print_job` and `upload_file`
again.

## Keep the recoverable copy current

Commit and push each completed change to this fork. A local commit is not an
off-machine copy. GitHub Actions rebuilds a fresh environment from the pushed
checkout and runs E2E. Review its result before deploying a new runtime. Runtime
logs, Studio files, credentials and local evidence are excluded from Git.

Keep a working checkout and environment until the replacement passes E2E. The
bootstrap script refuses to overwrite an existing environment; create a separate
checkout or use `--venv /path/to/new-environment` for a trial rebuild. After a
change to `vendor/` or the pins, refresh the live environment with
`python3.14 ops/bootstrap.py --reuse` (copy `.venv-candidate` first for rollback),
then restart `bambu-mcp.service`. `python make.py` runs the same bootstrap.

The patched printer library is versioned `2.0.0rc2+bambumcp.1`. Local version
labels cannot be published to PyPI, so the MCP's exact pin can only be satisfied
by `vendor/bambu-printer-manager`; a plain `pip install -e .` in a fresh
environment fails instead of silently installing an unpatched upstream build.
