"""Network-side FTPS and camera peers for MCP E2E acceptance.

No production classes are imported or mocked. These peers implement the wire
protocols (the FTPS peer lists, sends, stores, deletes and renames files as the
printer's server does) and can reset a handshake or return an invalid camera header.
"""
import io
import json
import socket
import ssl
import struct
import threading
import zipfile

from PIL import Image


def project_archive(marker=None, printer_model="Bambu Lab P1S", nozzle="0.4", plate="Textured PEI Plate",
                    gcode_moves=0):
    """A sliced single-plate 3MF whose G-code header names its printer, nozzle and plate, as
    Bambu Studio writes it. ``marker`` makes a distinct job (different bytes and hash).
    ``gcode_moves`` appends that many move lines: deflated, as Bambu Studio stores the plate
    G-code, the job stays small while the G-code the printer unpacks is large."""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("Metadata/plate_1.json", json.dumps({
            "bbox_objects":[{"name":"coupon_post.stl"}], "filament_ids":[1]}))
        z.writestr("Metadata/slice_info.config", '''<config><plate>
          <metadata key="index" value="1"/><metadata key="filament_maps" value="1"/>
          <object identify_id="83" name="coupon_post.stl"/>
          <filament id="1" type="PETG" color="#000000"/></plate></config>''')
        z.writestr("Metadata/plate_1.gcode", "; CONFIG_BLOCK_START\n; filament_type = PETG\n"
                   f"; printer_model = {printer_model}\n; nozzle_diameter = {nozzle}\n"
                   f"; curr_bed_type = {plate}\n; CONFIG_BLOCK_END\n"
                   + "G1 X128.000 Y128.000 E0.03125 F1800\n" * gcode_moves)
        if marker is not None:
            z.writestr("Metadata/fixture_job.txt", str(marker))
    return out.getvalue()  # intentionally no thumbnail or top image


class Listener:
    def __init__(self, ctx, port):
        self.ctx = ctx
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.stop = threading.Event()
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while not self.stop.is_set():
            try:
                raw, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.serve, args=(raw,), daemon=True).start()

    def close(self):
        self.stop.set()
        self.listener.close()


CARD_RECORD = "bambu-mcp-card.json"


def card_record(card_id="0" * 32, data_bytes=1 << 30, cluster_bytes=32768):
    """The record ops/attest_sd_card.py writes on a checked card (the fixture card is 1 GiB)."""
    return json.dumps({"schema": "bambu-mcp-sd-card/1", "card_id": card_id, "data_bytes": data_bytes,
                       "cluster_bytes": cluster_bytes, "attested_at": "2026-11-03T00:00:00Z"}).encode()


class FTPSPeer(Listener):
    def __init__(self, ctx):
        self.reset = True
        self.resets = 0
        self.connections = 0  # every accepted control connection, reset or served
        self.downloads = []
        self.stores = []
        self.commands = []
        self.files = {"fixture-job.gcode.3mf":project_archive(), "broken.gcode.3mf":b"broken archive",
                      CARD_RECORD:card_record()}
        # Listed size of files whose bytes nobody reads here (a dashcam's videos filling the card).
        self.sizes = {}
        # Folders below the root: path -> LIST rows, or None for a folder the printer refuses (550).
        self.folders = {}
        self.root_rows = []  # extra raw rows in the root listing (folders, rows bpm skips)
        super().__init__(ctx, 990)

    def serve(self, raw):
        passive = None
        rename_from = None
        self.connections += 1
        if self.reset:
            self.resets += 1
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii",1,0))
            raw.close()
            return
        try:
            raw.settimeout(5)
            with self.ctx.wrap_socket(raw, server_side=True) as control:
                stream = control.makefile("rb")
                def reply(line):
                    control.sendall((line+"\r\n").encode())
                reply("220 E2E implicit TLS printer")
                while not self.stop.is_set():
                    line = stream.readline(8192).decode().strip()
                    if not line:
                        break
                    verb, _, arg = line.partition(" ")
                    # Test-only credentials; never record passwords in artifacts.
                    self.commands.append((verb, "<redacted>" if verb=="PASS" else arg))
                    if verb=="USER": reply("331 Password required")
                    elif verb=="PASS": reply("230 Logged in")
                    elif verb in ("PBSZ","PROT","TYPE","NOOP"): reply("200 OK")
                    elif verb=="PWD": reply('257 "/"')
                    elif verb=="CWD": reply("250 Directory changed")
                    elif verb in ("PASV","EPSV"):
                        if passive: passive.close()
                        passive = socket.socket(); passive.bind(("127.0.0.1",0)); passive.listen()
                        passive.settimeout(5); port=passive.getsockname()[1]
                        if verb=="PASV": reply(f"227 Entering Passive Mode (127,0,0,1,{port//256},{port%256})")
                        else: reply(f"229 Entering Extended Passive Mode (|||{port}|)")
                    elif verb in ("LIST","RETR"):
                        if verb=="LIST" and arg.strip("/") in self.folders:
                            rows=self.folders[arg.strip("/")]
                            if rows is None:
                                reply("550 Permission denied"); continue
                            data="".join(f"{r}\r\n" for r in rows).encode()
                        elif verb=="LIST":
                            data="".join([f"-rw-r--r-- 1 test test {self.sizes.get(n, len(v))} Jan 01 2026 {n}\r\n"
                                          for n,v in self.files.items()]+[f"{r}\r\n" for r in self.root_rows]).encode()
                        else:
                            name=arg.lstrip("/")
                            if name not in self.files:
                                reply("550 File not found"); continue
                            data=self.files[name]; self.downloads.append(name)
                        reply("150 Opening data connection")
                        conn,_=passive.accept()
                        with self.ctx.wrap_socket(conn,server_side=True) as data_socket:
                            data_socket.sendall(data)
                            data_socket.unwrap().close()
                        passive.close(); passive=None
                        reply("226 Transfer complete")
                    elif verb=="STOR":
                        # The printer's own STOR: a TLS data channel the client ends without
                        # close_notify (bpm shuts the socket down), then 226.
                        reply("150 Ok to send data")
                        conn,_=passive.accept()
                        chunks=[]
                        with self.ctx.wrap_socket(conn,server_side=True) as data_socket:
                            try:
                                while chunk:=data_socket.recv(65536):
                                    chunks.append(chunk)
                            except (ssl.SSLEOFError, ConnectionResetError):
                                pass  # end of data without TLS close_notify
                        passive.close(); passive=None
                        self.files[arg.lstrip("/")]=b"".join(chunks); self.stores.append(arg.lstrip("/"))
                        reply("226 Transfer complete")
                    elif verb=="DELE":
                        if self.files.pop(arg.lstrip("/"),None) is None: reply("550 File not found")
                        else: reply("250 Delete operation successful")
                    elif verb=="RNFR":
                        rename_from=arg.lstrip("/") if arg.lstrip("/") in self.files else None
                        reply("350 Ready for RNTO" if rename_from else "550 File not found")
                    elif verb=="RNTO" and rename_from:
                        self.files[arg.lstrip("/")]=self.files.pop(rename_from); rename_from=None
                        reply("250 Rename successful")
                    elif verb=="SIZE":
                        data=self.files.get(arg.lstrip("/"))
                        reply("550 File not found" if data is None else f"213 {len(data)}")
                    elif verb=="QUIT": reply("221 Bye"); break
                    else: reply("502 Command not implemented")
        except (OSError,UnicodeError):
            pass
        finally:
            if passive: passive.close()
            raw.close()


class CameraPeer(Listener):
    def __init__(self, ctx):
        self.invalid = False
        self.captures = 0
        self.authenticated = 0
        self.frames = []
        for color in ("red","green"):
            buf=io.BytesIO(); Image.new("RGB",(32,24),color).save(buf,format="JPEG")
            self.frames.append(buf.getvalue())
        super().__init__(ctx,6000)

    def serve(self,raw):
        try:
            raw.settimeout(5)
            with self.ctx.wrap_socket(raw,server_side=True) as peer:
                auth=b""
                while len(auth)<80:
                    part=peer.recv(80-len(auth))
                    if not part: return
                    auth+=part
                if auth[16:48].rstrip(b"\0")!=b"bblp" or auth[48:].rstrip(b"\0")!=b"TESTONLY":
                    return
                self.authenticated+=1
                self.captures+=1
                if self.invalid:
                    peer.sendall(struct.pack("<4I",9_000_000,0,1,0))
                else:
                    for frame in self.frames:
                        peer.sendall(struct.pack("<4I",len(frame),0,1,0)+frame)
        except OSError:
            pass
        finally:
            raw.close()
