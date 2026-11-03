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

## Tools

<!-- gen_docs:tools:begin -->
107 tools (52 read-only, 55 write) and 87 REST routes. Full reference: [bambu-mcp docs](https://synman.github.io/bambu-printer-manager/bambu-mcp/).

| Category | Tools |
|---|---|
| [Printer State](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/state/) | `get_ams_status`, `get_capabilities`, `get_fan_speeds`, `get_hms_errors`, `get_job_info`, `get_print_progress`, `get_printer_info`, `get_printer_state`, `get_spool_info`, `get_temperatures`, `get_wifi_signal` |
| [Print Control](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/print-control/) | `clear_print_error`, `pause_print`, `resume_print`, `select_extrusion_calibration`, `send_gcode`, `set_print_option`, `set_print_speed`, `skip_objects`, `stop_print` |
| [Climate](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/climate/) | `get_chamber_light`, `get_climate`, `set_bed_temp`, `set_chamber_light`, `set_chamber_temp`, `set_fan_speed`, `set_nozzle_temp` |
| [Filament & AMS](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/filament/) | `calibrate_ams_remaining`, `get_ams_units`, `get_external_spool`, `get_filament_catalog`, `load_filament`, `send_ams_control_command`, `set_ams_filament_setting`, `set_ams_user_setting`, `start_ams_dryer`, `stop_ams_dryer`, `unload_filament` |
| [Nozzles](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/nozzle/) | `get_nozzle_info`, `refresh_nozzles`, `set_nozzle_config`, `swap_tool` |
| [Detectors](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/detectors/) | `get_detector_settings`, `set_air_printing_detection`, `set_buildplate_marker_detection`, `set_first_layer_inspection`, `set_nozzle_clumping_detection`, `set_purge_chute_detection`, `set_spaghetti_detection` |
| [Printer Management](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/management/) | `add_printer`, `disconnect_printer`, `get_configured_printers`, `get_printer_connection_status`, `remove_printer`, `start_printer`, `update_printer_credentials` |
| [Files](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/files/) | `create_folder`, `delete_file`, `download_file`, `get_3mf_entry_by_id`, `get_3mf_entry_by_name`, `get_all_project_info`, `get_current_job_project_info`, `get_file_info`, `get_plate_thumbnail`, `get_plate_topview`, `get_project_info`, `list_sdcard_files`, `open_plate_layout`, `open_plate_viewer`, `prepare_print_job`, `preview_ams_mapping`, `print_file`, `refresh_sdcard`, `register_sd_card`, `rename_sdcard_file`, `upload_file` |
| [System](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/system/) | `dump_log`, `force_state_refresh`, `get_firmware_version`, `get_server_info`, `get_session_status`, `get_user_pref`, `pause_mqtt_session`, `rename_printer`, `resume_mqtt_session`, `set_print_options`, `set_user_pref`, `trigger_printer_refresh`, `truncate_log` |
| [Discovery](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/discovery/) | `discover_printers` |
| [Raw Commands](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/commands/) | `send_mqtt_command` |
| [Camera](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/camera/) | `analyze_active_job`, `get_camera_image`, `get_stream_url`, `open_job_state`, `start_stream`, `stop_stream`, `view_stream` |
| [Alerts](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/notifications/) | `get_pending_alerts` |
| [Snapshots & Monitoring Data](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/snapshots/) | `get_monitoring_data`, `get_monitoring_history`, `get_monitoring_series`, `get_snapshot` |
| [Charts](https://synman.github.io/bambu-printer-manager/bambu-mcp/tools/charts/) | `open_charts`, `render_charts_html`, `render_charts_panels` |
<!-- gen_docs:tools:end -->
