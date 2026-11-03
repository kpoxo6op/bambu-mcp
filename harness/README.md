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

* The printer is off when the server starts (TCP reset at connect). Status must
  name the cause, writes are refused, reconnects back off (bounded attempts), and
  the external watchdog probe still reports a healthy MCP: an offline printer is
  not a reason to restart the service.
* The printer rejects the LAN access code (CONNACK 5). Status must say
  "Not authorized" rather than a generic "connecting", and writes are refused.
* A real TCP reset during the job-start FTPS handshake must not stop MQTT state
  updates or force a replacement MQTT connection.
* Stale, malformed, starting and disconnected telemetry must be visibly rejected
  by public state/write tools. Health and camera remain diagnostic tools.
* The pinned **real paho** client and **real FastMCP HTTP** server connect to a
  synthetic TLS MQTT peer. Connection drops, silence, malformed frames and job
  transitions cannot create an extra MQTT owner or replay a write. The peer counts
  received mutation packets, not mock method calls.
* The link dies silently (no FIN/RST, no data, no PINGRESP) and new connections
  are then refused, as when the P1S dropped off Wi-Fi on 2026-11-02. Telemetry
  must go not-live, writes are refused, the dead socket is released, the MCP
  stays healthy and the session returns by itself when the printer does.
* The printer answers requests but sends no status report, as the live P1S did on
  2026-11-03 while printing from SD: get_version answered, one partial report, every
  full-state request ignored. The old supervisor dropped such a connection 120 s after each
  connect (paho logs our own disconnect() as "Normal disconnection") and health said
  "starting". Now health must say stale and that the printer answers, writes are refused,
  full-state requests keep a fixed cadence and a stream request is sent, the connection is
  replaced once at the widening threshold and not again within the doubled interval, and
  the late report makes it live on the existing connection. A refused report subscription
  (SUBACK 0x80) is named in health and the connection is replaced until one is granted.
* After that fix the live P1S sent one full report and went quiet again, still answering.
  The stream request rode on the supervisor's full-state request, and bpm's own watchdog,
  asking on the same 30 s cadence (kept in the fixture), usually took that slot first. The
  peer here streams only while a stream request is recent, as the requests Bambu Studio and
  ha-bambulab send imply; that firmware behaviour is not shown on the live printer. Over 6 s
  every health sample must be live with advancing layers on one connection, a full-state
  keep-alive must still go out while reports stream, and writes are refused once they stop.
* Client-side MQTT sockets are counted from `/proc/net/tcp` in the namespace at
  every accepted connection and after every fault. More than one ESTABLISHED or
  CLOSE_WAIT socket fails the run; CLOSE_WAIT is the 2026-11-01 leak.
* Socket backpressure followed by disconnect produces an explicit unknown
  delivery result and no command replay. Killing the MCP process and starting
  its replacement must leave the same HTTP client usable.
* A sliced 3MF without preview images is downloaded through real FTPS and parsed
  by the installed dependency. External-holder loading is checked in received MQTT
  packets; requests without permission and writes during a print are refused.
* Print admission (`docs/print-admission.md`). The server runs the fixed
  `fixture_preparer.py` as its configured preparer process. Invalid part names never
  reach it; a failed exit, a hang (killed at the timeout), a missing, old-schema,
  still-running, extra-part, problem-carrying or geometry-failed report, a forged
  job hash and a symlinked job admit nothing, and their job files cannot be
  uploaded. A caller's own PASS report, an exact copy of a prepared job, a job edited
  after preparation and a valid sliced file already on the card are refused before
  any STOR or start packet. Raw MQTT starts, `gcode_file`, SD-card G-code (M23/M24,
  M28, M32), extrusion (including `g1e5`), macros and tool changes are refused on the
  publish path; maintenance G-code and an allowlisted pause still arrive. For a
  prepared job, a missing or nonfinite nozzle report, the wrong plate or bed type and a file replaced on the card are
  refused; the card is re-read and exactly one `project_file` arrives with the
  admitted file, plate, external mapping, the prepared bed type (for `auto`) and
  exactly the job's MD5; a repeat is refused. A report without a target, a target
  not qualified for the policy (X1 Carbon) and a job whose G-code header contradicts
  the target admit nothing; a valid but different plate (`cool_plate`), a 0.6 mm
  nozzle report and a nozzle not reported since the reconnect are refused; a P1S job
  uploaded to a server for an A1 serial is refused as the wrong model. The simulated
  printer reports its serial topic and nozzle as a P1S does.
  Rename, overwrite by a differently-cased rename, delete and removal by another
  client (`remote_file_missing`) revoke admission. A
  start whose delivery is unknown (full socket) is not replayed and cannot be
  repeated. A SIGKILLed server forgets every admission, and a replacement with a
  world-writable preparer refuses without running it.
  The fixture renders, slices and checks no geometry: it only produces the outcomes
  a real preparer can, at the same process boundary. The real Soyrack renderer and
  slicer are tested in that repository.
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
host filesystem, private `/tmp` and temporary HOME. The invoking user's real home
directory is replaced by an empty tmpfs (only the checkout and the interpreter are
bound back read-only), and `evidence/` is hidden, so the real Bambu Studio access
code, `printer.env` and private transcripts are not readable by test code. An unprivileged user namespace
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
