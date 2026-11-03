# Print admission

A print starts through this server only from a job that the server's own preparer
run produced and declared passing, uploaded byte-for-byte and verified on the card
just before the start. Instructions ("run the preflight first") are not the control;
the code path is. Source: `print_admission.py`, with the final check in
`reliability._GuardedClient.publish`.

```
prepare_print_job(part) -> upload_file(local=<job_path>, remote=/<part>.gcode.3mf) -> print_file(remote)
```

## What is enforced

| Step | Rule |
|---|---|
| `prepare_print_job(part)` | Runs the operator-configured command with a new output directory chosen by the server and `part`. Admits the job only if the process exits 0 and the report and job pass every rule below. No printer action. |
| `upload_file` | A local or remote name ending `.3mf` or `.gcode` is a printable job. It is accepted only from a prepared `job_path` whose bytes still hash to the receipt, copied to a private snapshot while hashing, to a remote path ending `.3mf`, and only when the attested card has room for the job, its largest unpacked plate G-code and a reserve (`docs/storage-space.md`). The remote path is then bound to the job and that card for one start. A previous binding for that path is dropped before the transfer; a failed transfer binds nothing. Other files transfer as before. |
| `rename_sdcard_file`, `delete_file` | Drop bindings for the source, destination and anything under a renamed/deleted folder before the FTPS operation. A non-printable name renamed to a printable one is refused. |
| `download_file` | Refuses destinations inside the prepared-job store. |
| `print_file` | Needs a binding for the path and an admitted plate, and a printer that is the job's target (below). Under a per-printer lock shared with upload/rename/delete it downloads the remote file over FTPS, requires the prepared SHA-256, reads plate metadata from those bytes, requires that bambu will put exactly that MD5 in the start packet, requires room on the same card to unpack the plate's G-code (`docs/storage-space.md`), consumes the binding, then publishes one start. |
| MQTT publish (every caller) | `project_file` passes only with the one-shot ticket of that `print_file` call and only if file, URL, plate, AMS mapping, bed type (the prepared plate), calibration flags and `md5` (exactly the verified bytes' MD5, never empty) match it. `gcode_file`, `project_prepare` and any write with a `url` or `file` field are refused. `gcode_line` must be maintenance G-code (below). |
| `send_mqtt_command` | One section, one allowlisted non-start command (see the tool docstring). |

Receipts and bindings exist only in the server's memory. A restart forgets all of
them, so nothing can be revived after a restart: prepare and upload again. A start
whose delivery is unknown is never repeated; another attempt needs a new upload
(the receipt stays valid; nothing is re-sliced).

There is no force, override, `allow_warning` or caller-supplied report anywhere.

### Report and job rules

All of these must hold for `<out>/preflight.json` and the job, or nothing is admitted:

1. Exit status 0, within `BAMBU_PREFLIGHT_TIMEOUT_S` (the whole process group is
   killed on timeout, and again after exit).
2. `preflight.json` is a regular file (not a symlink), at most 8 MiB, a JSON object.
3. `schema_version == 2` and `geometry_policy_version == 1` (integers).
4. `pass is true`, `state == "complete"`, `error` absent or empty.
5. `parts` has exactly one key, the requested part; its `problems` is `[]`; its
   `geometry.pass is true` and `geometry.policy_version == 1`.
6. `parts[part].target` is `{printer_model, nozzle_diameter, plate_type}`: a printer
   qualified for the geometry policy (policy 1: `Bambu Lab P1S` only), exactly one
   nozzle diameter, and a known Bambu Studio bed type (`Cool Plate`, `Engineering
   Plate`, `High Temp Plate`, `Textured PEI Plate`). Missing or unknown fails.
7. `parts[part].job` is the SHA-256 the server computes for
   `<out>/<part>/<part>.gcode.3mf`, a regular file (no symlink in the run), 1 B to
   512 MiB, a ZIP with at least one `Metadata/plate_<N>.gcode`. Those N are the
   admitted plates, and each plate's G-code header (`; printer_model`,
   `; nozzle_diameter`, `; curr_bed_type`) must declare the same target.

### Printer match at start

The binding carries the target. Before downloading anything and again just before the
start, `print_file` requires:

* model: bambu's `PrinterModel` for the serial number the live MQTT session answers on
  (prefix `01P` is a P1S) equals the target; unknown refuses (`printer_model_unknown`),
  another model refuses (`printer_model_mismatch`). The configured printer name is never
  used.
* one nozzle (`printer_not_qualified` otherwise), and the `nozzle_diameter` the printer
  reported on the current connection equals the target. A value from before a
  reconnect does not count (`nozzle_unknown`); another diameter refuses
  (`nozzle_mismatch`).
* plate: `bed_type` `auto` names the prepared plate in the start packet; the same plate
  named explicitly is accepted; another valid plate refuses (`bed_type_mismatch`).

These refusals keep the admission (fix the printer and call `print_file` again).
Software cannot sense the physical plate or the loaded filament; the start packet
declares the prepared plate type and the camera or a person checks the bed.

The server judges no geometry: supports, rims and profiles are the preparer's job.
The preparer receives `[<out>, <part>]` appended to its command, where `<out>` does
not exist yet (it must create it and refuse an existing one), stdin is `/dev/null`,
output goes to `<store>/<run>.log`, and its environment is the service's own.

### Maintenance G-code allowlist

Per line, after comments: G0/G1 with X Y Z F only, G4, G28, G90, G91, M17, M18, M84,
M104, M109, M140, M190, M141, M191, M106, M107, M105, M114, M400. Extrusion (an `E`
word on a move), SD-card commands (M23/M24/M28/M29/M30/M32...), macros, tool changes
and unknown words are refused, with nothing sent. The dedicated temperature and fan
tools emit only allowlisted G-code.

## Operator setup

Add to `~/.config/bambu-mcp/printer.env` (single quotes keep the JSON intact):

```sh
BAMBU_PREFLIGHT_COMMAND_JSON='["/usr/bin/node","/path/to/apps/assembly-guide/cad/preflight.mjs"]'
# optional, seconds, 1..7200 (default 1800)
BAMBU_PREFLIGHT_TIMEOUT_S=1800
```

Rules checked on every call: element 0 is an absolute path to an executable; every
element containing `/` is an absolute path to an existing regular file; those files
and their directories are owned by root or the service user and are not
world-writable. Point it at a checkout nobody edits casually (a pinned worktree),
because whatever that script does is what admits a job.

Before relying on it, run the preparer once with the service's environment and
restrictions, into a new directory, and read its report:

```sh
systemd-run --user --wait --pipe -p EnvironmentFile="$HOME/.config/bambu-mcp/printer.env" \
  -p UMask=0077 -p NoNewPrivileges=true \
  /usr/bin/node /path/to/preflight.mjs "$HOME/preflight-check-$(date +%s)" <part>
```

Then restart the service (see `docs/operators-guide.md`) and call
`prepare_print_job` for a known-good part.

Prepared runs are kept in `~/.bambu-mcp/preflight/` (newest 100 runs; runs that a
live receipt uses are never pruned).

## Compatibility changes

* `upload_file` refuses arbitrary `.3mf`/`.gcode` files; `print_file` refuses any file
  not uploaded from a prepared job in the current server process, including files
  sliced elsewhere and files already on the card. Start those from the printer or
  Bambu Studio if you must; this server will not.
* `send_mqtt_command` accepts only the allowlist; firmware upgrades, `project_file`,
  `gcode_file` and G-code are refused. `send_gcode` and `POST /api/send_gcode` accept
  only the maintenance allowlist (no manual extrusion: use the filament tools).
* `POST /api/print_3mf` runs the same admission; `POST /api/upload_file_to_printer`
  refuses printable jobs; rename/delete routes drop admissions like the tools.
* Remote paths are normalized (leading `/`, single separators); `.` and `..` are refused.

## Rollback

Redeploy the previous release. Unsetting `BAMBU_PREFLIGHT_COMMAND_JSON` does not
re-open unchecked printing: it only makes `prepare_print_job` refuse, so no job can
be admitted and every start is refused.

## Limits

* Only this server's MQTT session is governed. Bambu Studio, Bambu Handy, the
  printer's screen or another LAN client can still start any file on the card.
* Between the FTPS verification and the printer opening the file, a client outside
  this server could replace it. The start packet carries the verified MD5; whether
  the firmware checks it is not established.
* The printer model comes from the serial prefix the session answers on, not from a
  firmware model report. The physical plate and the loaded material are not sensed:
  AMS mapping still accepts a same-colour spool of a different material ("Color
  Match Only"), and the bed is the camera's or a person's to check.
* E2E uses a fixed fixture preparer at the process boundary. It proves admission,
  binding and the wire packets, not geometry; the Soyrack repository's preflight is
  tested with the real renderer and slicer separately.
