"""ops/night_speed.py's own main with the campaign's wall clock and shorter waits.

The helper's checks, records and MCP client run unchanged. BAMBU_NIGHT_SPEED_FAULT stops the
process at the send, around the real call: crash_before and crash_after end it as a kill would,
lost_answer loses the answer after the real call, hold:<path> waits for <path> first.
"""
import asyncio
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"ops"))
if os.environ.get("BAMBU_HARNESS_ISOLATED") != "1":
    raise SystemExit("Use harness/run.py; network isolation is required")
import night_speed

_clock_file = Path(os.environ["BAMBU_HARNESS_CLOCK_FILE"])
night_speed._wall_clock = lambda: float(_clock_file.read_text())
night_speed.SETTLE_RETRY_S = .2
fault = os.environ.get("BAMBU_NIGHT_SPEED_FAULT", "")
_send = night_speed._send_speed
async def _send_with_fault(session, printer, mode):
    if fault == "crash_before":
        os._exit(137)
    if fault.startswith("hold:"):
        while not Path(fault[5:]).exists():
            await asyncio.sleep(.05)
    answer = await _send(session, printer, mode)
    if fault == "crash_after":
        os._exit(137)
    if fault == "lost_answer":
        raise ConnectionError("harness: the answer never arrived")
    return answer
night_speed._send_speed = _send_with_fault
sys.exit(night_speed.main())
