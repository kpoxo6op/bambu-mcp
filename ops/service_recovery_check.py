"""Operator-invoked MCP-process crash check. No printer commands; never in nightly tests."""
import asyncio
import json
from pathlib import Path
import socket
import subprocess
import sys
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

if sys.argv[1:] != ["--execute-service-crash"]:
    raise SystemExit("Requires --execute-service-crash: kills only the bambu-mcp user service process")

def unit():
    out=subprocess.check_output(["systemctl","--user","show","bambu-mcp.service",
        "-p","MainPID","-p","NRestarts","-p","ActiveState"],text=True)
    return dict(x.split("=",1) for x in out.splitlines() if "=" in x)

def payload(r):
    assert not r.isError
    return r.structuredContent or json.loads(next(c.text for c in r.content if c.type=="text"))

async def main():
    async with streamablehttp_client("http://127.0.0.1:25099/mcp") as (rd,wr,_):
        async with ClientSession(rd,wr) as c:
            await c.initialize()
            before=payload(await c.call_tool("get_print_progress",{"name":"p1s"}))
            old=unit()
            started=time.monotonic()
            subprocess.run(["systemctl","--user","kill","--signal=SIGKILL","--kill-whom=main",
                            "bambu-mcp.service"],check=True)
            for _ in range(120):
                new=unit()
                if new.get("MainPID") not in (old["MainPID"],"0") and new.get("ActiveState")=="active":
                    try:
                        sock=socket.create_connection(("127.0.0.1",25099),.1);sock.close();break
                    except OSError:
                        pass
                await asyncio.sleep(.25)
            else:
                raise AssertionError("MCP service did not restart within30s")
            for _ in range(60):
                health=payload(await c.call_tool("get_printer_connection_status",{"name":"p1s"}))
                if health.get("telemetry",{}).get("live"):
                    break
                await asyncio.sleep(.5)
            assert health["telemetry"]["live"] and health["telemetry"]["supervisor_alive"],health
            after=payload(await c.call_tool("get_print_progress",{"name":"p1s"}))
            assert after["subtask_name"]==before["subtask_name"],(before,after)
            assert after["gcode_state"] in ("RUNNING","FINISH"),after
            assert after["current_layer"]>=before["current_layer"],(before,after)
            assert int(new["NRestarts"])>int(old["NRestarts"]),new
            report={"passed":True,"same_mcp_client":True,"printer_write_tools_called":[],
                    "fault":"SIGKILL MCP main process only","before_unit":old,"after_unit":new,
                    "recovery_seconds":round(time.monotonic()-started,2),"before":before,"after":after,
                    "health":health,"time":time.strftime("%Y-%m-%dT%H:%M:%S%z")}
            out=Path(__file__).resolve().parents[1]/"evidence/reliability-20261101/crash-recovery.json"
            out.write_text(json.dumps(report,indent=2))
            print(json.dumps({k:report[k] for k in ("passed","same_mcp_client","recovery_seconds","after")}))

asyncio.run(main())
