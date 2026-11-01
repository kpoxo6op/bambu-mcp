# Bambu MCP — recovery and E2E fork

[![E2E](https://github.com/kpoxo6op/bambu-mcp/actions/workflows/e2e.yml/badge.svg)](https://github.com/kpoxo6op/bambu-mcp/actions/workflows/e2e.yml)

A maintained fork of [synman/bambu-mcp](https://github.com/synman/bambu-mcp),
based on commit `7001c3b79e53d8a215223026db44fc4f0577fde2`. It controls Bambu Lab
printers over the LAN using the existing upstream tools and printer library.

This fork adds recovery for failures observed with a P1S: an FTPS handshake reset
must not kill the MQTT receiver, stale telemetry cannot authorize a write, and a
lost connection must not replay a physical command. A shared HTTP service and an
OS lock keep one MQTT owner. A separate watchdog can restart a hung MCP process.

The E2E gate exercises the actual MCP server and MQTT/FTPS/camera clients against
network protocol simulators. It checks disconnections, malformed data, socket
backpressure, process crashes, external spool loading, previewless 3MF files and
camera recovery. Only E2E tests are used. GitHub Actions runs on pushes, pull
requests and daily; the laptop also has an optional nightly systemd timer.

## Install from a clean checkout

Linux is the checked platform. Use Python 3.14, Git, OpenSSL and bubblewrap.
The bootstrap command creates a new environment and never overwrites an existing
one. It installs pinned dependencies and the included patched printer library;
no other local checkout is needed.

```sh
git clone https://github.com/kpoxo6op/bambu-mcp.git
cd bambu-mcp
python3.14 ops/bootstrap.py
.venv-candidate/bin/python harness/run.py --rounds 3 --cycles 8
```

Follow [recovery and service setup](docs/recovery.md) to configure a real printer
and connect MCP clients. Bootstrap and E2E testing never start a real print.
The LAN access code stays in the user's Bambu Studio configuration.

## Evidence and scope

- [E2E contracts, failure reports and nightly checks](harness/README.md)
- [Why retain this upstream and what alternatives were checked](docs/alternatives.md)
- [Upstream tool documentation](docs/upstream-readme.md)
- [Dependency provenance](vendor/bambu-printer-manager/PROVENANCE.json)

The initial P1S deployment was checked during a real print, including an MCP
process restart. The simulator does not prove physical extrusion or validate
every printer model. A socket-written MQTT command is not firmware confirmation.
The server binds to loopback; other local processes are trusted.

Upstream authorship and history are preserved. The included printer library
retains its own [license](vendor/bambu-printer-manager/LICENSE). This fork does
not assign a new license to upstream code that did not ship one.
