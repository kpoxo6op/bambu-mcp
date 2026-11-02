
>
> **Archetype:** MCP server + REST API daemon
> **Language:** Python 3.12+ · setuptools · `.venv-candidate/` local install
> **Interfaces:** MCP (stdio/SSE) · HTTP REST (49152+) · MJPEG streams

## Build / Validate

- Per-file: `python -m py_compile <file>.py`
- E2E tests only: `.venv-candidate/bin/python harness/run.py --rounds 3 --cycles 8`. See `harness/README.md`. Do not add unit/mock or direct-import smoke tests.
- Rebuild on a new machine: `python3.14 ops/bootstrap.py`; follow `docs/recovery.md`.
- Shared runtime: `bambu-mcp.service`. Use the existing HTTP endpoint; do not create a competing MQTT session.

## Architecture

- 85 MCP tools, 79 HTTP routes, 41 knowledge modules, 1 system prompt
- All printer ops route through BPM library via `session_manager.get_printer(name)`
- BPM is considered stable — do not modify it to solve MCP-layer problems
- No tool may open its own direct FTPS/MQTT/socket/HTTP connection (camera streaming excepted)
- Prints start only through `print_admission.py`: `prepare_print_job` → `upload_file` → `print_file`; see `docs/print-admission.md`, never add an override
- REST API server and MJPEG camera streams draw ports from a shared `PortPool` (default 49152-49251)
- Port discovery: `get_server_info()` MCP tool or `GET /api/server_info` — never hardcode ports
- Camera subsystem: RTSPS (H2D/X1) + TCP-TLS (A1/P1), MJPEG encoding, anomaly detection
- 24 runtime dependencies (Flask, mcp SDK, av, numpy, PIL, zeroconf)

### Dual-Layer Sync

Every HTTP route and MCP tool must stay in sync:
- `api_server.py` route docstring (Swagger/OpenAPI source)
- `knowledge/http_api_*.py` sub-topic file (agent-facing reference)

A change to one layer without the other is incomplete.

### Filesystem Persistence

### Live Printer State Access

- **Bash/scripts**: Use local REST API `http://localhost:49152/api/...` (probe ports 49152-49158)

## Git Policy

Agent-managed — full git lifecycle authorized (stage, commit, push). Descriptive commit messages, push after each logical unit.

- Never update/add/remove dependencies without explicit user approval
- Version in one place: `pyproject.toml` `[project] version`
- After version bump: `pip install -e .` then `python make.py version-sync`

## Credential Registry

All project secrets are stored in the workspace vault (`~/.claude/secrets.vault`).

| Secret | Vault Service | Expires | Notes |
|--------|--------------|---------|-------|
| Container API auth | `bpm_api_auth` | 90d default | Basic auth for BPM container REST API |
