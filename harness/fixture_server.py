"""Real MCP server, real paho/TLS, entirely synthetic printer. Harness-only entrypoint."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# The runner's network namespace has loopback only. Refuse accidental direct invocation.
if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")
import auth
import reliability
import session_manager
from bpm import bambuprinter

NAME = "fixture"
auth.get_configured_printer_names = lambda: [NAME]
auth.get_printer_credentials = lambda name: {
    "ip": "127.0.0.1", "serial": "01P00TESTONLY", "access_code": "TESTONLY"}
session_manager._bpm_config_overrides = lambda: {
    "mqtt_port": int(os.environ["HARNESS_MQTT_PORT"]),
    "mqtt_connection_timeout": 1, "ftps_connection_timeout": 1,
    "bpm_cache_path": Path.home() / "bpm"}

class ResetFTPS:
    def __init__(self, *a, **kw):
        raise ConnectionResetError("injected FTPS handshake reset")

bambuprinter.IoTFTPSClient = ResetFTPS
for key, value in dict(STALE_AFTER_S=1.2, PUSHALL_EVERY_S=.5,
                       FORCE_RECONNECT_AFTER_S=3., QUIT_TIMEOUT_S=1., PUBLISH_TIMEOUT_S=.4,
                       BACKOFF_MIN_S=.2, BACKOFF_MAX_S=1., SUPERVISOR_TICK_S=.1,
                       STUCK_CALLBACK_S=2., STUCK_EXIT_AFTER_S=4.,
                       SUPERVISOR_DEAD_AFTER_S=3.).items():
    setattr(reliability, key, value)
import server

sys.argv = ["server.py", "--transport", "streamable-http"]
server.main()
