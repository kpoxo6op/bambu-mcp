#!/usr/bin/env python3
"""External read-only MCP probe; restarts only the MCP user service if it hangs.

No printer write API is called. A disconnected printer is a healthy MCP response.
Three failed probes and a five-minute cooldown prevent restart storms. Intentional
systemctl stop is respected. systemd owns immediate process-crash recovery.
"""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

UNIT = "bambu-mcp.service"
STATE = Path.home()/".bambu-mcp/watchdog.json"

async def probe(url="http://127.0.0.1:25099/mcp", name="p1s"):
    async with asyncio.timeout(8):
        async with streamablehttp_client(url) as (rd, wr, _):
            async with ClientSession(rd, wr) as c:
                await c.initialize()
                r = await c.call_tool("get_printer_connection_status", {"name":name})
                if r.isError:
                    raise RuntimeError("MCP returned an error")
                data = r.structuredContent
                if data is None:
                    data = json.loads(next(x.text for x in r.content if x.type=="text"))
                health = data.get("telemetry", {})
                if not health.get("supervisor_alive"):
                    raise RuntimeError("MCP session supervisor is not alive")
                # Health has its own diagnostic lane. Exercise the ordinary pool too;
                # otherwise eight hung file/camera calls could block every real tool
                # while this watchdog kept declaring the process healthy.
                ordinary = await c.call_tool("get_print_progress", {"name":name})
                if ordinary.isError:
                    raise RuntimeError("ordinary MCP tool lane returned an error")
                # telemetry_not_live is expected if the printer itself is offline.
                return {"status":health.get("status"),"live":health.get("live")}

def decision(previous, *, active, pid, ok, now):
    """Deterministic policy; caller supplies observations, never guesses print state."""
    old = previous if previous.get("pid") == pid else {"last_restart":previous.get("last_restart",0)}
    state = {**old,"pid":pid,"checked_at":now,"active":active,"probe_ok":ok}
    state["failures"] = 0 if ok or active != "active" else old.get("failures",0)+1
    restart = (active=="active" and state["failures"]>=3 and now-old.get("last_restart",0)>=300)
    return state, restart

def main():
    os.umask(0o077)
    status = subprocess.run(["systemctl","--user","show",UNIT,"-p","ActiveState","-p","MainPID"],
                            check=True,capture_output=True,text=True,timeout=4)
    props = dict(x.split("=",1) for x in status.stdout.splitlines() if "=" in x)
    active, pid = props.get("ActiveState"), props.get("MainPID")
    try:
        previous = json.loads(STATE.read_text())
    except (OSError, ValueError):
        previous = {}
    ok, detail = False, {"status":"service not active"}
    if active == "active":
        try:
            detail = asyncio.run(probe()); ok = True
        except Exception as exc:
            detail = {"error":type(exc).__name__,"detail":str(exc)[:250]}
    state, restart = decision(previous,active=active,pid=pid,ok=ok,now=time.time())
    state["observation"] = detail
    if restart:
        result = subprocess.run(["systemctl","--user","--no-block","restart",UNIT],
                                capture_output=True,text=True,timeout=5)
        state["restart_requested"] = result.returncode==0
        if result.returncode == 0:
            state["last_restart"] = time.time(); state["failures"] = 0
        print(json.dumps(state),flush=True)
    elif not ok and active=="active":
        print(json.dumps(state),flush=True)
    STATE.parent.mkdir(exist_ok=True,parents=True,mode=0o700)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state,indent=2)); tmp.replace(STATE)

if __name__ == "__main__":
    main()
