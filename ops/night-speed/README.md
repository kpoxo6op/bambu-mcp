# Night speed for one reviewed print

Quiet from 22:30 to 08:30 Pacific/Auckland, then Standard, for the one job a reviewed policy names.
No pause, resume or stop. Boris chose this on 2026-11-06 over a night pause, because movement
becomes quieter without a long stop.

Two systemd calendar timers each start `ops/night_speed.py` once a day. The helper is an ordinary
client of the running server's HTTP MCP endpoint: it opens no MQTT, FTPS or camera connection,
listens on nothing, runs no model and does not change the server. It is copied beside the server,
so installing it needs no restart. Printer-side rules stay the server's: the telemetry guard and
publish path apply to its one command, `set_print_speed(name, level, user_permission=True)`.

## What happens at a boundary

Nothing is sent unless all of this holds, read fresh over MCP:

* The policy is a private (0600) regular file of this user, valid, `enabled`, with `reviewed`.
* The current time, in the policy's zone, is in the window of that mode. A late Quiet still applies
  at 03:00, an 08:30 event replayed at 23:00 refuses. A missed event never crosses into the next
  window. The timers use `Persistent=true`, so a host that was off catches up inside this rule.
* The printer is connected with live, writable telemetry. A gap is waited out for `gate_settle_s`
  (default 180 s, retried every 20 s, with no command); nothing else is retried.
* The job is exactly `expected_job` and `RUNNING`, not `PAUSE`, `PREPARE` or anything else.
* `print_error` is 0 and no HMS entry is active. A lingering `command_error` also refuses: read the
  reason in the dry run before enabling.
* Standard only: that night's Quiet may have reached the printer, and this is the same job: same
  name, `wall_start_time` and layer count, no fewer layers. Otherwise a replacement gets nothing.

Then `<window>.intent.json` is created exclusively and synced, one command goes out, and
`<window>.outcome.json` records the answer. A refusal records no intent, so a later event in the
window can still act. The server's preference store is not touched.

| Result | Meaning | Exit |
|---|---|---|
| `sent_unconfirmed` | Reached the MQTT socket (QoS 0). Not proof the printer changed speed. | 0 |
| `duplicate_noop` | This window's command was already sent. Nothing sent. | 0 |
| `not_needed` | Standard with no Quiet from that night to undo. Nothing sent. | 0 |
| `refused` | A gate failed (`reason`, `detail`). Nothing sent, window still open. | 1 |
| `not_sent` | The server refused after the intent. This window is used; Standard is not needed. | 1 |
| `unknown` | The call failed or delivery is unknown (full socket, lost answer). | 2 |
| `unknown_no_replay` | An intent has no known delivery, for example the helper was killed. Never sent again. | 2 |

After `unknown`, the morning's Standard is still sent, because Quiet may have arrived. Every run
appends one line to `events.jsonl`. A failed unit stays visible in `systemctl --user status`.

## Readback limit

The server has no tool that reports the speed profile. `get_printer_state` carries no speed,
`get_printer_info` is identity, `get_snapshot` and the monitoring tools return URLs that need the
REST listener (`api_port` is 0 on the shared server), `analyze_active_job` opens a camera connection
and does not return speed either, and the setter does not change the server's own value. Incoming
`spd_lvl` updates a value that no tool exposes. The helper therefore records intent and sent, never
"confirmed", and a missing readback never causes a retry. Check the printer's screen for the real
profile. A getter would be a separate server change, deployed while no job is running.

## Policy

`~/.config/bambu-mcp/night-speed.json`, mode 0600. Unknown keys refuse.

```json
{
 "schema_version": 1,
 "enabled": true,
 "printer": "p1s",
 "mcp_url": "http://127.0.0.1:25099/mcp",
 "timezone": "Pacific/Auckland",
 "start": "22:30",
 "end": "08:30",
 "quiet_mode": "quiet",
 "day_mode": "standard",
 "expected_job": "<subtask name the printer reports>",
 "reviewed": {"at": "<UTC time>", "by": "<reviewer>"},
 "authorization": {"at": "<UTC time>", "source": "<who decided>", "verbatim": "<their words>"}
}
```

Optional: `gate_settle_s` (0-240), `operator_plan` and `status` (kept, not interpreted). The timers
hold the hours too: a different schedule means changing both. To use the same schedule for a later
job, review the policy again with the new `expected_job`. Nothing here starts or admits a job.

## Install (copy only)

From a checkout at the reviewed commit, with no server restart:

```sh
install -d -m 700 ~/.local/share/bambu-night-speed ~/.local/state/bambu-mcp/night-speed
install -m 500 ops/night_speed.py ~/.local/share/bambu-night-speed/night_speed.py
install -m 644 ops/night-speed/bambu-night-speed@.service ops/night-speed/bambu-night-speed-quiet.timer \
    ops/night-speed/bambu-night-speed-standard.timer ~/.config/systemd/user/
install -m 600 <reviewed policy> ~/.config/bambu-mcp/night-speed.json
systemctl --user daemon-reload
```

`daemon-reload` restarts nothing. The units use the server's interpreter read-only, for the `mcp`
package. Prove the gates, then enable:

```sh
PY=~/.local/share/bambu-mcp/.venv-candidate/bin/python
HELPER=~/.local/share/bambu-night-speed/night_speed.py
ARGS="--config $HOME/.config/bambu-mcp/night-speed.json --state-dir $HOME/.local/state/bambu-mcp/night-speed"
$PY -I $HELPER check --config ~/.config/bambu-mcp/night-speed.json
timeout 60 $PY -I $HELPER dry-run $ARGS --mode quiet --at 2026-11-06T22:30:00+13:00
timeout 60 $PY -I $HELPER dry-run $ARGS --mode standard --at 2026-11-07T08:30:00+13:00
systemctl --user enable --now bambu-night-speed-quiet.timer bambu-night-speed-standard.timer
systemctl --user list-timers 'bambu-night-speed-*'
```

The dry run sends no command and creates no file, lock or directory. It only calls `list_tools`,
`get_printer_connection_status`, `get_print_progress`, `get_job_info` and `get_hms_errors`; `--at`
moves only the time gate. Enabling sends nothing either. Expect `would_send` for Quiet and
`not_needed` for Standard until a Quiet has been recorded. Do not run `boundary` by hand as a test:
inside its window, with the gates passing, it sends the real command.

Stop: `systemctl --user disable --now bambu-night-speed-quiet.timer bambu-night-speed-standard.timer`.
Nothing is restored when stopping; if a Quiet went out, the printer stays in Quiet until set otherwise.

```sh
cat ~/.local/state/bambu-mcp/night-speed/events.jsonl
journalctl --user -u 'bambu-night-speed@*'
```

When the job has ended, disable the timers: each event otherwise refuses (`job_mismatch` or
`job_not_running`), sends nothing and leaves a failed unit.

## Tests

`harness/night_speed_campaign.py` runs the actual helper against real FastMCP HTTP, paho and a
synthetic TLS printer as part of `harness/run.py`; see `harness/README.md`. It shows wire behaviour,
not that a physical printer changed speed.
