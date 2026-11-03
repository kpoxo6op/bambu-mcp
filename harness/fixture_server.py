"""Production server entrypoint with accelerated recovery deadlines for E2E faults.

Credentials/configuration, parser, tools and all transports are real. Only the
time thresholds are shortened. Protocol faults come from the network peers.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# The runner's network namespace has loopback only. Refuse accidental direct invocation.
if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")
import reliability
for key, value in dict(STALE_AFTER_S=1.2, PUSHALL_EVERY_S=.5,
                       FORCE_RECONNECT_AFTER_S=3., NO_REPORT_RECONNECT_S=5., NO_REPORT_RECONNECT_MAX_S=7.,
                       PUSH_START_AFTER_S=.4, PUSH_START_EVERY_S=.3, PUSHALL_KEEPALIVE_S=2.,
                       QUIT_TIMEOUT_S=1., PUBLISH_TIMEOUT_S=.4,
                       BACKOFF_MIN_S=.2, BACKOFF_MAX_S=1., SUPERVISOR_TICK_S=.1,
                       STUCK_CALLBACK_S=2., STUCK_EXIT_AFTER_S=4.,
                       SUPERVISOR_DEAD_AFTER_S=3.).items():
    setattr(reliability, key, value)
# bpm's own watchdog asks for full state after watchdog_timeout seconds without a message: 30 s,
# equal to PUSHALL_EVERY_S in production. Keep that ratio so both request sources compete here
# as they do on the live printer.
import bpm.bambuconfig as bambuconfig
_config_init = bambuconfig.BambuConfig.__init__
def _config_init_scaled(self, *args, **kwargs):
    _config_init(self, *args, **kwargs)
    self.watchdog_timeout = reliability.PUSHALL_EVERY_S
bambuconfig.BambuConfig.__init__ = _config_init_scaled
# paho reports any socket failure to its callbacks as a generic "Unspecified error"; its own error
# log names the actual exception. Diagnostics only: nothing in the server reads it.
import logging
import paho.mqtt.client as paho_client
_paho_init = paho_client.Client.__init__
def _paho_init_logged(self, *args, **kwargs):
    _paho_init(self, *args, **kwargs)
    self.enable_logger(logging.getLogger("paho"))
paho_client.Client.__init__ = _paho_init_logged
import server

sys.argv = ["server.py", "--transport", "streamable-http"]
server.main()
