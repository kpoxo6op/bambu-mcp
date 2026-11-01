# Local P1S MCP

Based on `synman/bambu-mcp` v0.9.0, commit
`7001c3b79e53d8a215223026db44fc4f0577fde2`, branch `main`.
Published fork: https://github.com/kpoxo6op/bambu-mcp.
See `docs/recovery.md` for a rebuild on another machine.

## Runtime

One systemd user service, `bambu-mcp.service`, owns the printer session. Clients
use **http://127.0.0.1:25099/mcp**. Codex enables 37 tools. REST is disabled.
HTTP is stateless, so clients can keep using the endpoint after a process restart;
long-lived resource push subscriptions are not provided by this mode.
An OS file lock shared by HTTP and stdio prevents a second MCP connection owner;
PID/port files only help discovery. The service restarts after a crash. A separate
watchdog checks the actual MCP interface and recovers a hung process after three
failed probes, with a five-minute cooldown. It respects an intentionally stopped
service. Printer-offline status is not itself a reason to restart the service.

`launch.py` reads the existing Bambu Studio credential at runtime, by exact serial.
Credentials are not copied into MCP configuration. The endpoint is loopback only,
with SDK Host/Origin checks; it is not authenticated against other local processes.
Runtime files are private. Logs rotate at 10 MB with three backups in
`~/.bambu-mcp/bambu-mcp.log`; warnings (disconnects, reconnects, connection
causes, supervisor restarts) also reach the systemd journal. Per-request HTTP
access lines are suppressed so the 30 s watchdog does not bury them.

## Incident and repair

On 2026-11-01 the upstream MQTT callback performed an FTPS lookup at job start.
The printer reset the TLS handshake. The exception killed the MQTT loop, leaving
CLOSE-WAIT sockets and a pre-job IDLE snapshot falsely marked `recent_update=true`.

`reliability.ReliablePrinter` moves project lookup and update callbacks off the
MQTT thread, catches parsing failures, tracks monotonic telemetry age and
supervises connection recovery. State tools flag non-live values explicitly.
Every physical write is checked again at publication; refused, unconfirmed and
socket-written results remain distinct. Physical commands are not automatically
replayed after reconnect. Read-only state refreshes may repeat.

The 3MF parser also accepts valid sliced projects without preview PNGs. The patch
is `patches/bpm-optional-thumbnails.patch`, based on BPM commit
`728551cde926e8e3b4506c37dbaf722b30b98a70`. The recoverable source snapshot is in
`vendor/bambu-printer-manager`; `ops/bootstrap.py` installs it before the MCP package. The candidate environment
is `.venv-candidate`; `installed-requirements.txt` records installed versions.
External-spool loading accepts tray254 without requiring an AMS. Print submission
verifies plate metadata and rechecks active printing/filament change before send.
Camera reads have a deadline and simultaneous one-shot captures are serialized.

## Evidence and continuous checks

See [the harness](harness/README.md) for commands and the fault matrix.
`evidence/harness/latest.json` is the latest verdict. Only E2E tests run; the old
unit/mock suites and smoke scripts are removed. Nightly isolated tests run at
04:30 local time, with bounded duration/resources. Failed seeds are saved and
replayed before new ones. A failed run stays failed; it is not retried into green.
The timer records regressions, without automatically changing or deploying code.

Claude Opus 5.5, high effort, implemented and reviewed the reliability work:

- `evidence/reliability-20261101/claude-core.md`: implementation and reproduced incident.
- `evidence/reliability-20261101/claude-final.md`: final findings and corrections.
- `evidence/reliability-20261101/claude-addendum.md`: transport-error and restart review.
- `evidence/reliability-20261101/live-acceptance.json`: post-deployment read-only acceptance.
- `evidence/reliability-20261101/crash-recovery.json`: acceptance after service crash recovery.

The public-path test uses real FastMCP HTTP, paho TLS MQTT, FTPS and camera clients
against a protocol simulator in a private network namespace. No fault-injection test reaches the LAN.
A live check uses `ops/verify_live.py` and the existing HTTP server; it never
creates another MQTT client or sends a printer write.

## Recovery and limits

```sh
systemctl --user status bambu-mcp.service bambu-mcp-watchdog.timer bambu-mcp-harness.timer
journalctl --user -u bambu-mcp.service -u bambu-mcp-watchdog.service
```

A separately built old source/environment is preserved at
`~/.local/share/bambu-mcp-rollback-20261101`. For rollback, stop the watchdog timer,
point the service's ExecStart and WorkingDirectory to that directory, reload
systemd and restart the MCP service. This changes the control connection, not the
printer's independently executing job.

MQTT QoS0 confirms socket delivery, not firmware acceptance or actual extrusion.
Extrusion and adhesion need camera/physical observation. The printer's self-signed
MQTT/camera certificates are accepted on the trusted LAN. Other clients such as
Bambu Studio still consume printer connection slots. Coalesced update callbacks
can merge very short state transitions. The thresholds and offline fault corpus
remain explicit in code and reports and should evolve with observed failures.

Historical private evidence includes configuration backups and raw review transcripts.
Do not publish or commit that directory. No credentials belong in Git.
