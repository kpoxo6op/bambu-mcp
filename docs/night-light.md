# Chamber light quiet hours and inspection leases

The server keeps a printer's chamber light off during configured quiet hours, and can light the
chamber briefly for an inspection with the OFF guaranteed. Source: `night_light.py`; tools
`set_night_light_policy`, `get_night_light_policy`, `light_inspection_lease` (`tools/climate.py`).

## Why

A light switched on for a camera check, by the firmware or by hand stayed on all night unless an
agent or a person remembered to switch it off. The daemon already owns the printer's only MQTT
session and sees every report, so it applies the rule itself: an ordinary timer (30 s) and the
session's update notifications, no agent, no extra connection, no camera or job polling.

## Behaviour

* Opt-in per printer. Nothing happens until an operator stores a policy. The policy is in
  `~/.bambu-mcp/night_light_<printer>.json` (written atomically) and is restored at start.
* Inside quiet hours, a chamber light the printer reports ON on the current connection is
  switched OFF with the same command as `set_chamber_light` (through the same publish guard).
  That covers the window start, a server start or reconnect inside the window, and a light
  switched on during it.
* Nothing is sent while telemetry is not writable (stale, disconnected, starting) or before the
  printer has reported its light on this connection. An unreported light is `unknown`, never
  taken as off. A light reported OFF gets no command. After an OFF, another is sent only when a
  newer report still says ON, no sooner than 60 s, doubling to 30 min until it is reported OFF.
* The light is never switched ON automatically (not at the end of quiet hours either), and no
  print, heater or setting is touched. A running print continues; only the light changes.
* Hours are local wall-clock times in an IANA zone, so daylight-saving changes move the UTC
  instants. The window may cross midnight.

### Inspection lease

`light_inspection_lease(name, seconds, user_permission=True)` (1-900 s) switches a light reported
OFF to ON after persisting the deadline. At the deadline the server switches it OFF once it is
reported ON, also if the caller crashed or the server restarted in between. `seconds=0` ends a
lease early (the OFF follows from the server). A repeat while a lease runs extends it. A light
that is already ON gets no lease and is never switched off by this mechanism, so an ordinary
manual ON during the day is left alone. A lease still unresolved 6 h after its deadline (printer
unreachable) is dropped.

### Precedence

Quiet hours win: a lease is refused inside them, a lease granted earlier ends at the next
quiet-hours start, and inside them the light is switched off whatever any lease says.

## Operate

Enable (any MCP client of the running server; stores the policy, sends nothing itself):

```text
set_night_light_policy(name="<printer>", enabled=true, timezone="<IANA zone>",
                       start="22:00", end="07:00", user_permission=true)
```

Check: `get_night_light_policy(name="<printer>")` returns the policy, `quiet_now`, the next
start/end, the reported light (`on`/`off`/`unknown`), telemetry writability, any lease and the
last automatic command with its result.

Disable: `set_night_light_policy(name="<printer>", enabled=false, user_permission=true)` keeps the
hours for reference. Removing the policy file while the server is stopped has the same effect.

Inspection: `light_inspection_lease(name, seconds=120, user_permission=true)`, capture, then
`light_inspection_lease(name, seconds=0, user_permission=true)`. If the end call never comes, the
deadline does it.

Rollback: deploy the previous release. It ignores `night_light_*.json`; leaving the files in place
is harmless, and they take effect again on a release that has this feature.

## Limits

* The printer must include `lights_report` in its reports (the H2D full report in
  `docs/captures/` does; bpm parses it for every model). Until it arrives on a connection the
  status says `unknown` and nothing is sent: after deployment, confirm `light.reported` is
  `on` or `off` for each enabled printer.
* OFF commands are QoS 0 like every command; the effect is confirmed only by the next report.
* A light switched on inside quiet hours goes off again: to light the chamber at night, disable
  the policy first.
* The E2E gate (`harness/README.md`) exercises these rules against the simulated printer with a
  controlled wall clock; the physical light remains a live check.
