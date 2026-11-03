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
                       FORCE_RECONNECT_AFTER_S=3., NO_REPORT_RECONNECT_S=5., NO_REPORT_RECONNECT_MAX_S=20.,
                       PUSHALL_MAX_S=4., QUIT_TIMEOUT_S=1., PUBLISH_TIMEOUT_S=.4,
                       BACKOFF_MIN_S=.2, BACKOFF_MAX_S=1., SUPERVISOR_TICK_S=.1,
                       STUCK_CALLBACK_S=2., STUCK_EXIT_AFTER_S=4.,
                       SUPERVISOR_DEAD_AFTER_S=3.).items():
    setattr(reliability, key, value)
import server

sys.argv = ["server.py", "--transport", "streamable-http"]
server.main()
