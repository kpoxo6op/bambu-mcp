"""Policy contract: repeated API hangs recover, offline printers and user stops don't loop.
Real MCP transport probe is exercised separately in wire_campaign.py.
"""
import unittest
from watchdog import decision

class WatchdogPolicy(unittest.TestCase):
    def test_three_failures_cooldown_and_intentional_stop(self):
        s = {}
        for i in range(3):
            s, restart = decision(s,active="active",pid="10",ok=False,now=1000+i*30)
            self.assertEqual(restart,i==2)
        s["last_restart"] = 1060
        for now in (1090,1200,1359):
            s,restart=decision(s,active="active",pid="10",ok=False,now=now)
            self.assertFalse(restart)
        s,restart=decision(s,active="inactive",pid="0",ok=False,now=1500)
        self.assertFalse(restart); self.assertEqual(s["failures"],0)

    def test_response_clears_failures_and_pid_change_starts_fresh(self):
        s={"failures":2,"pid":"10"}
        s,restart=decision(s,active="active",pid="10",ok=True,now=1000)
        self.assertFalse(restart); self.assertEqual(s["failures"],0)
        s,restart=decision({"failures":99,"pid":"old"},active="active",pid="new",ok=False,now=1000)
        self.assertFalse(restart); self.assertEqual(s["failures"],1)

if __name__ == "__main__":
    unittest.main()
