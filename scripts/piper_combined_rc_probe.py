import asyncio, json, os, shutil, sqlite3, sys, tempfile, threading
from pathlib import Path
from http.server import ThreadingHTTPServer
root=Path(__file__).resolve().parent.parent
plugin=Path(os.environ['PIPER_TEST_PLUGIN_ROOT']).resolve()
sb_root=Path(os.environ['PIPER_TEST_SB_ROOT']).resolve()
sys.path[:0]=[str(root),str(plugin/'tests')]
from e2e_probe import Model, free_port, http, CHAT, KEY, PREFIX
TOKEN='z'*40
CHARM_TOKEN='c'*40

async def main():
    base=Path(tempfile.mkdtemp(prefix='piper-web-e2e-',dir=os.environ['TMPDIR'])); home=base/'home'; home.mkdir(mode=0o700)
    os.environ.update(HOME=str(base),HERMES_HOME=str(home),OPENAI_API_KEY='loopback-only',HERMES_SHARED_THREAD_MEDIA_V1='1')
    keep={'HOME','HERMES_HOME','OPENAI_API_KEY','HERMES_SHARED_THREAD_MEDIA_V1','TMPDIR','PATH','LANG','TZ'}
    for k in list(os.environ):
        if k not in keep: del os.environ[k]
    shutil.copytree(plugin,home/'plugins/piper-shared-thread',ignore=shutil.ignore_patterns('.git','tests','__pycache__','*.md'))
    shutil.copytree(root/'contrib/piper-turn-outbox',home/'plugins/piper-turn-outbox')
    import importlib.util
    spec=importlib.util.spec_from_file_location('staging_plugin',plugin/'shared_thread.py'); pst=importlib.util.module_from_spec(spec); spec.loader.exec_module(pst)
    pst.TokenStore.add(home/'piper-shared-thread/tokens.json','second-brain-web',TOKEN)
    pst.TokenStore.add(home/'piper-shared-thread/tokens.json','charm',CHARM_TOKEN)
    model=ThreadingHTTPServer(('127.0.0.1',0),Model)
    model.requests=[]; model.blocked=threading.Event(); model.release=threading.Event(); model.blocked2=threading.Event(); model.release2=threading.Event(); model.hold_compress=threading.Event(); model.compress_blocked=threading.Event(); model.compress_release=threading.Event(); model.summaries=0
    thread=threading.Thread(target=model.serve_forever); thread.start(); port=free_port(); ready=asyncio.Queue()
    import websockets
    async def connector(ws):
        json.loads(await ws.recv())
        desc={'contract_version':1,'platform':'telegram','label':'Fixture','max_message_length':4096,'supports_draft_streaming':False,'supports_edit':True,'supports_threads':False,'markdown_dialect':'plain','len_unit':'chars'}
        await ws.send(json.dumps({'type':'descriptor','descriptor':desc})+'\n'); await ready.put(ws)
        async for line in ws:
            frame=json.loads(line)
            if frame['type']=='outbound': await ws.send(json.dumps({'type':'outbound_result','requestId':frame['requestId'],'result':{'success':True,'message_id':'fake'}})+'\n')
            elif frame['type']=='going_idle': await ws.send(json.dumps({'type':'going_idle_ack'})+'\n')
    try:
      async with websockets.serve(connector,'127.0.0.1',0) as relay:
        url=f'http://127.0.0.1:{model.server_port}/v1'
        os.environ.update(OPENAI_BASE_URL=url,GATEWAY_RELAY_URL=f"ws://127.0.0.1:{relay.sockets[0].getsockname()[1]}/relay",GATEWAY_RELAY_PLATFORMS='telegram',GATEWAY_RELAY_BOT_IDS='{"telegram":"fixture-bot"}',GATEWAY_RELAY_ID='fixture-gateway',GATEWAY_RELAY_SECRET='fixture-secret',API_SERVER_ENABLED='true',TELEGRAM_ALLOWED_USERS=CHAT,API_SERVER_HOST='127.0.0.1',API_SERVER_PORT=str(port),API_SERVER_KEY='api-key-not-used-by-shared-thread-0123456789')
        cfg={'gateway':{'multiplex_profiles':False,'optional_settlement_plugins':['piper-turn-outbox']},'model':{'provider':'custom','default':'local-fixture','base_url':url},'streaming':{'enabled':False},'terminal':{'cwd':str(home)},'approvals':{'mode':'off'},'auxiliary':{'title_generation':{'enabled':False}},'plugins':{'isolation':'in_process','enabled':['piper-shared-thread','piper-turn-outbox'],'entries':{'piper-shared-thread':{'allow_gateway_injection':True,'settings':{'session_key':KEY,'command_wait_s':2}},'piper-turn-outbox':{'settings':{'route':KEY}}}}}
        (home/'config.yaml').write_text(json.dumps(cfg))
        from gateway.run import start_gateway
        task=asyncio.create_task(start_gateway(verbosity=None))
        try:
          ws=await asyncio.wait_for(ready.get(),40)
          from tests.gateway.test_normal_runtime_boot import control
          for _ in range(300):
            try:
              if (await asyncio.to_thread(control,home,'identify')).get('state')=='ready': break
            except (OSError,ValueError,AssertionError): pass
            await asyncio.sleep(.1)
          else: raise AssertionError('gateway not ready')
          await ws.send(json.dumps({'type':'inbound','event':{'text':'fixture seed','message_id':'tg-fixture-1','source':{'platform':'telegram','chat_id':CHAT,'chat_type':'dm','user_id':CHAT,'user_name':'Fixture'}}})+'\n')
          for _ in range(400):
            try:
              with sqlite3.connect(f'file:{home / "state.db"}?mode=ro',uri=True) as db:
                done=db.execute("select status from session_admissions where request_id='tg-fixture-1'").fetchone()
              if done and done[0]=='terminal': break
            except sqlite3.Error: pass
            await asyncio.sleep(.1)
          else: raise AssertionError('telegram seed did not settle')
          # The connector redelivering the exact message must not create another event.
          await ws.send(json.dumps({'type':'inbound','event':{'text':'fixture seed','message_id':'tg-fixture-1','source':{'platform':'telegram','chat_id':CHAT,'chat_type':'dm','user_id':CHAT,'user_name':'Fixture'}}})+'\n')
          import subprocess
          st, info = await asyncio.to_thread(http,port,'GET',PREFIX+'/thread',None,TOKEN)
          assert st==200 and info['capabilities']['attachments_v1'] is True,(st,info)
          sb=sb_root
          env={'PATH':os.environ['PATH'],'HOME':str(base),'TMPDIR':os.environ['TMPDIR'],
               'PIPER_FIXTURE_ORIGIN':f'http://127.0.0.1:{port}'}
          result=await asyncio.to_thread(subprocess.run,['node','--import','tsx',str(sb/'scripts/test-piper-attachment-http.ts')],cwd=sb,env=env,capture_output=True,text=True,timeout=45)
          assert result.returncode==0,(result.stdout,result.stderr)
          assert 'PASS real SB handler' in result.stdout,result.stdout
          st,completed=await asyncio.to_thread(http,port,'GET',PREFIX+'/turns/web-0123456789abcdef0123?wait=25',None,TOKEN)
          assert st==200 and completed['state']=='completed',(st,completed)
          paths=list(home.glob('piper-shared-thread/native-media-private/*/*/*_note.txt'))
          assert len(paths)==1 and paths[0].read_bytes()==b'hello',paths
          assert any('note.txt' in json.dumps(req['messages']) for req in model.requests),model.requests
          charm={'turn_id':'charm-turn-0001','text':'fixture charm input'}
          st,reply=await asyncio.to_thread(http,port,'POST',PREFIX+'/turns',charm,CHARM_TOKEN)
          assert st==202,(st,reply)
          st,reply=await asyncio.to_thread(http,port,'GET',PREFIX+'/turns/charm-turn-0001?wait=25',None,CHARM_TOKEN)
          assert st==200 and reply['state']=='completed',(st,reply)
          st,reply=await asyncio.to_thread(http,port,'POST',PREFIX+'/turns',charm,CHARM_TOKEN)
          assert st==200,(st,reply)
          with sqlite3.connect(f'file:{home / "state.db"}?mode=ro',uri=True) as db:
              rows=db.execute('select a.principal_id,a.request_id,o.turn_id,o.state from piper_turn_outbox o join session_admissions a on a.admission_id=o.admission_id').fetchall()
          assert len(rows)==3,rows
          assert sum(p.startswith('messaging:') for p,_,_,_ in rows)==1,rows
          assert sum('charm' in req for _,req,_,_ in rows)==1,rows
          assert sum(t=='web-0123456789abcdef0123' for _,_,t,_ in rows)==1,rows
          assert all(state=='pending' for _,_,_,state in rows),rows
          print('COMBINED_RC_PASS telegram=1 charm=1 web_file=1 replay=no_duplicate bytes=hello',flush=True)
        finally:
          task.cancel()
          try: await task
          except asyncio.CancelledError: pass
    finally:
      model.shutdown(); thread.join(); model.server_close(); print('SCRATCH',base,flush=True)

asyncio.run(main())
