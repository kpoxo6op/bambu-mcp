"""Post-deploy read-only acceptance. Uses the shared endpoint, never a new MQTT session."""
import argparse
import asyncio
import base64
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

URL = "http://127.0.0.1:25099/mcp"
ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--name", default="p1s")
parser.add_argument("--host", required=True, help="Printer address, used to count existing sockets only")
parser.add_argument("--project", help="Optional SD-card 3MF to read")
parser.add_argument("--job", help="Optional expected active/finished job name")
parser.add_argument("--output", type=Path, default=ROOT/"evidence/live-acceptance")
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True, mode=0o700)

def payload(r):
    if r.isError:
        raise AssertionError(str(r.content)[:500])
    if r.structuredContent is not None:
        value = r.structuredContent
    else:
        value = json.loads(next(c.text for c in r.content if c.type=="text"))
    if isinstance(value,dict) and value.get("encoding")=="gzip+base64":
        value=json.loads(gzip.decompress(base64.b64decode(value["data"])))
    if isinstance(value,dict) and "error" in value:
        raise AssertionError(value)
    return value

async def client(full=False):
    async with streamablehttp_client(URL) as (rd,wr,_):
        async with ClientSession(rd,wr) as c:
            await c.initialize()
            for _ in range(60):
                h = payload(await c.call_tool("get_printer_connection_status",{"name":args.name}))
                if h.get("telemetry",{}).get("live"):
                    break
                await asyncio.sleep(1)
            assert h.get("telemetry",{}).get("live"), h
            assert h["telemetry"]["supervisor_alive"], h
            result={"connection":h}
            result["progress"]=payload(await c.call_tool("get_print_progress",{"name":args.name}))
            if args.job:
                assert result["progress"]["subtask_name"]==args.job,result["progress"]
                assert result["progress"]["gcode_state"] in ("RUNNING","FINISH"),result["progress"]
            if full:
                result["temperatures"]=payload(await c.call_tool("get_temperatures",{"name":args.name}))
                result["errors"]=payload(await c.call_tool("get_hms_errors",{"name":args.name}))
                if args.project:
                    project=payload(await c.call_tool("get_project_info",{
                        "name":args.name,"file_path":args.project,"plate_num":1}))
                    assert project["plate_num"]==1 and project["metadata"]["filament"],project
                    result["project"]={k:project.get(k) for k in ("name","plate_num","plates")}
                    result["project"]["filament"]=project["metadata"]["filament"]
                    result["project"]["objects"]=project["metadata"].get("map",{}).get("bbox_objects")
                camera=await c.call_tool("get_camera_image",{"name":args.name})
                images=[x for x in camera.content if x.type=="image"]
                assert len(images)==1
                image=base64.b64decode(images[0].data)
                (args.output/"camera.jpg").write_bytes(image)
                result["camera_bytes"]=len(image)
            return result

async def main():
    report={"printer_write_tools_called":[],"time":time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    report["clients"]=await asyncio.gather(client(True),client(),client())
    pid=subprocess.check_output(["systemctl","--user","show","bambu-mcp.service","-p","MainPID","--value"],text=True).strip()
    sockets=subprocess.check_output(["ss","-Htnp"],text=True).splitlines()
    owned=[s for s in sockets if f"pid={pid}," in s and f"{args.host}:8883" in s]
    report["mqtt_socket_count"]=len(owned); report["pid"]=int(pid)
    assert len(owned)==1,owned
    report["passed"]=True
    path=args.output/"report.json"
    path.write_text(json.dumps(report,indent=2))
    print(json.dumps({"passed":True,"clients":3,"mqtt_socket_count":len(owned),"pid":pid,
                      "progress":report["clients"][0]["progress"],"report":str(path)}))

asyncio.run(main())
