# E2E reliability gate

This repository uses E2E tests only. Unit/mock suites and the old direct-import
smoke scripts have been removed. The nightly job uses the same E2E command.

Run from the installation directory:

```sh
.venv-candidate/bin/python harness/run.py --rounds 3 --cycles 8
```

The command exits nonzero on the first failure. `evidence/harness/latest.json` is
the latest verdict. Each dated directory contains source fingerprint, dependency
versions, exact seed, command, durations and logs. A failed wire seed is kept in
`regression-seeds.json` and runs before new seeds on every later campaign.

Replay a failure:

```sh
.venv-candidate/bin/python harness/run.py --rounds 1 --cycles 8 --seed 101
```

## What the gate observes

* A real TCP reset during the job-start FTPS handshake must not stop MQTT state
  updates or force a replacement MQTT connection.
* Stale, malformed, starting and disconnected telemetry must be visibly rejected
  by public state/write tools. Health and camera remain diagnostic tools.
* The pinned **real paho** client and **real FastMCP HTTP** server connect to a
  synthetic TLS MQTT peer. Connection drops, silence, malformed frames and job
  transitions cannot create an extra MQTT owner or replay a write. The peer counts
  received mutation packets, not mock method calls.
* Socket backpressure followed by disconnect produces an explicit unknown
  delivery result and no command replay. Killing the MCP process and starting
  its replacement must leave the same HTTP client usable.
* A sliced 3MF without preview images is downloaded through real FTPS and parsed
  by the installed dependency. Corrupt files and missing plates cannot start a
  print. External-holder loading and print mapping are checked in received MQTT
  packets; requests without permission and writes during a print are refused.
* The camera peer authenticates over TLS and sends two distinct JPEG frames.
  MCP must return the fresh second frame, reject an oversized header and allow
  another capture after that error.
* A second HTTP/stdio process must be refused before opening another MQTT
  connection. The main server must exit within its shutdown deadline.

The server, tools, configuration reader, parser and client libraries run normally;
only recovery deadlines are accelerated in the fixture entrypoint. The simulated
printer implements wire protocols, not Python method mocks. These checks prove
software behavior against that peer; physical extrusion remains a live check.

All child tests use bubblewrap with a **private network namespace**, read-only
host filesystem, private `/tmp` and temporary HOME. An unprivileged user namespace
permits binding the printer's FTPS port 990 inside that private network only.
Only the fixture's loopback peer is reachable. Synthetic credentials enter through
the normal Bambu Studio configuration reader. No real printer credential is passed
to a test. Failure to create the sandbox fails the run; there is no unsandboxed fallback. Production
printer operations are never used as fault-injection tests.

## Over time

`bambu-mcp-harness.timer` runs daily at 04:30 local time, with up to five minutes
of jitter. Twenty seeds, each with twelve rotating faults and the workflow checks
above, plus saved failing seeds, run under CPU/memory/time limits.
Reports retain the latest 100 campaigns; up to
50 failing seeds remain in the replay corpus. A failure is visible as a failed
systemd unit and a failed `latest.json`; it is never retried into a green result.

To improve the implementation: reproduce the saved failure, extend an E2E
scenario at the public MCP/network boundary, fix it, rerun the gate, then deploy.
Do not add direct-import, unit or mock tests. Each scenario must name the observed
contract and credible failure that the existing scenarios do not already cover.
The timer tests and records; it does not silently edit code or deploy changes.

`bambu-mcp-watchdog.timer` checks MCP through its real HTTP tool interface every
30 seconds. Three consecutive timeouts/dead-supervisor responses request a restart
of **bambu-mcp.service only**, with a five-minute cooldown. A printer being offline
is a valid response, and an intentionally stopped service stays stopped. Immediate
process exits are handled by the service's `Restart=always`; an explicit
`systemctl stop` still leaves it stopped.

```sh
systemctl --user status bambu-mcp.service bambu-mcp-watchdog.timer bambu-mcp-harness.timer
journalctl --user -u bambu-mcp-watchdog.service -u bambu-mcp-harness.service
```

## Release and rollback

The live launcher chooses `.venv-candidate` only after the gate passes. The old
source and environment are separately preserved in
`~/.local/share/bambu-mcp-rollback-20261101`, so rollback does not rely on an
editable virtualenv pointing at changed source. Restore the old unit's ExecStart
and WorkingDirectory to that directory, daemon-reload and restart the MCP service.
Stop the watchdog timer while rolling back (the old version lacks its health
schema). The printer continues its own already-started job independently.

The local BPM dependency is based on upstream
`728551cde926e8e3b4506c37dbaf722b30b98a70`; its small source patch makes preview
PNGs optional in otherwise valid sliced 3MF files. The adapter, watchdog, harness
and pinned dependencies must be reviewed together on upgrades. Camera observation
of actual extrusion remains separate from software health.
