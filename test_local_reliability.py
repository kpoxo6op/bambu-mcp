"""Offline contracts missed by existing spool metadata/stdio tests.

A valid headless 3MF must retain printable plate metadata without images; external
loading must not require nonexistent AMS hardware; a second MCP process must not
acquire the same connection owner, including after the previous owner crashes.
No test opens a printer connection. The parser receives a local archive and a
network-free stub; filament commands are captured by a fake printer.
"""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
import zipfile

ROOT=Path(__file__).resolve().parent

class LocalReliability(unittest.TestCase):
    def test_previewless_plate_metadata_and_optional_image(self):
        from bpm.bambuproject import get_project_info
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'headless.3mf'
            xml='''<config><plate><metadata key="index" value="1"/>
            <metadata key="filament_maps" value="1"/>
            <object identify_id="83" name="coupon_post.stl"/>
            <filament id="1" type="PETG" color="#000000"/></plate></config>'''
            stub=types.SimpleNamespace(config=types.SimpleNamespace(
                bpm_cache_path=Path(tmp),serial_number='OFFLINE',verbose=True,
                capabilities=types.SimpleNamespace(has_dual_extruder=False)),
                cached_sd_card_3mf_files=None)
            for with_image in [False,True]:
                with self.subTest(with_image=with_image):
                    with zipfile.ZipFile(p,'w') as z:
                        z.writestr('Metadata/plate_1.json',json.dumps({'bbox_objects':[{'name':'coupon_post.stl'}],'filament_ids':[1]}))
                        z.writestr('Metadata/slice_info.config',xml)
                        z.writestr('Metadata/plate_1.gcode','; CONFIG_BLOCK_START\n; filament_type = PETG\n; CONFIG_BLOCK_END\n')
                        if with_image:z.writestr('Metadata/plate_1.png',b'preview')
                    info=get_project_info('/headless.3mf',stub,local_file=str(p),use_cached_list=True)
                    self.assertIsNotNone(info)
                    self.assertEqual(info.plates,[1])
                    self.assertEqual(info.metadata['filament'],[{'id':1,'type':'PETG','color':'#000000'}])
                    self.assertEqual(info.metadata['map']['bbox_objects'][0]['id'],83)
                    self.assertEqual(info.metadata['external_spool_trays'],[255])
                    self.assertNotIn('topimg',info.metadata)
                    self.assertEqual('thumbnail' in info.metadata,with_image)

    def test_external_loading_has_no_ams_dependency_and_retains_guards(self):
        from tools import filament
        calls=[]
        state=types.SimpleNamespace(gcode_state='IDLE',ams_units=[],extruders=[object()],hms_errors=[])
        printer=types.SimpleNamespace(printer_state=state,load_filament=lambda **kw:calls.append(kw))
        manager=types.SimpleNamespace(get_printer=lambda name:printer,get_state=lambda name:state)
        with patch.object(filament,'session_manager',manager),patch.object(filament,'_await_change_refusal',return_value=None):
            self.assertIn('user_permission',filament.load_filament('offline',0,254))
            self.assertEqual(calls,[])
            result=filament.load_filament('offline',0,254,True)
            self.assertIn('command sent',result)
            self.assertEqual(calls,[{'slot_id':254,'ams_id':254}])
            self.assertIn('requires a dual',filament.load_filament('offline',0,255,True))
            self.assertEqual(len(calls),1)
            state.gcode_state='RUNNING'
            self.assertIn('Blocked',filament.load_filament('offline',0,254,True))
            self.assertEqual(len(calls),1)

    def test_print_requires_verified_metadata_and_rechecks_busy_state(self):
        from tools import files
        calls=[]
        state=types.SimpleNamespace(gcode_state='IDLE',active_tray_state='LOADED')
        printer=types.SimpleNamespace(printer_state=state,print_3mf_file=lambda **kw:calls.append(kw))
        manager=types.SimpleNamespace(get_printer=lambda name:printer)
        plate=types.SimpleNamespace(plate_num=1,metadata={'filament':[{'type':'PETG','id':1}]})
        args=dict(name='offline',file_path='/a.3mf',use_ams=False,user_permission=True)
        with patch.object(files,'session_manager',manager):
            with patch('bpm.bambuproject.get_project_info',return_value=None):
                self.assertEqual(files.print_file(**args)['error'],'print_metadata_unavailable')
            self.assertEqual(calls,[])
            with patch('bpm.bambuproject.get_project_info',return_value=plate):
                state.active_tray_state='LOADING'
                self.assertEqual(files.print_file(**args)['error'],'filament_change_in_progress')
                state.active_tray_state='LOADED'
                self.assertEqual(files.print_file(**args)['status'],'published_unconfirmed')
            self.assertEqual(len(calls),1)
            def busy_during_read(*a,**kw):
                state.gcode_state='RUNNING'
                return plate
            with patch('bpm.bambuproject.get_project_info',side_effect=busy_during_read):
                self.assertIn('Blocked',files.print_file(**args)['error'])
            self.assertEqual(len(calls),1)

    def test_slow_camera_peer_has_overall_deadline(self):
        from camera import tcp_stream
        ticks=iter([0.0,0.4,0.8,1.2])
        class SlowPeer:
            def settimeout(self, seconds):pass
            def recv(self, n):return b"x"
        with patch.object(tcp_stream.time,'monotonic',side_effect=lambda:next(ticks)):
            with self.assertRaises(TimeoutError):
                tcp_stream._read_exactly(SlowPeer(),20,deadline=1.0)

    def test_owner_lock_survives_competition_and_releases_on_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            env={**os.environ,'HOME':tmp,'PYTHONPATH':str(ROOT)}
            p=subprocess.Popen([sys.executable,'-u','-c','import runtime_lock,time; runtime_lock.claim(); print("ready",flush=True); time.sleep(30)'],env=env,stdout=subprocess.PIPE,text=True)
            try:
                self.assertEqual(p.stdout.readline().strip(),'ready')
                second=subprocess.run([sys.executable,'-c','import runtime_lock; runtime_lock.claim()'],env=env,capture_output=True,text=True,timeout=3)
                self.assertNotEqual(second.returncode,0)
                self.assertIn('already owns',second.stderr)
                p.kill();p.wait(timeout=3)
                third=subprocess.run([sys.executable,'-c','import runtime_lock; runtime_lock.claim()'],env=env,capture_output=True,text=True,timeout=3)
                self.assertEqual(third.returncode,0,third.stderr)
            finally:
                if p.poll() is None:p.kill();p.wait()
                p.stdout.close()

if __name__=='__main__':unittest.main()
