"""Foreground-only disposable gateway smoke test, with a loopback model.

Run with a clean environment and scratch TMPDIR; never point at a real profile:
  env -i PATH="$PATH" TMPDIR=/path/to/scratch python scripts/piper_gateway_normal_probe.py --required
Omit --required to exercise the default-off configuration. No child processes,
real credentials, Firebase, phones, or external services are used.
"""
import asyncio
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ModelServer(ThreadingHTTPServer):
    requests: int = 0


class Model(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.server.requests += 1
        answer = {'role': 'assistant', 'content': 'STAGING_NORMAL_CHAT_ACK'}
        payload = {'id': 'fixture', 'choices': [{'index': 0, 'message': answer, 'finish_reason': 'stop'}],
                   'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}
        if request.get('stream'):
            payload = ('data: ' + json.dumps({'id': 'fixture', 'choices': [
                {'index': 0, 'delta': answer, 'finish_reason': 'stop'}]}) + '\n\ndata: [DONE]\n\n').encode()
            content_type = 'text/event-stream'
        else:
            payload = json.dumps(payload).encode()
            content_type = 'application/json'
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


async def main():
    if set(sys.argv[1:]) - {'--required'} or len(sys.argv) > 2:
        raise SystemExit('usage: piper_gateway_normal_probe.py [--required]')
    required = '--required' in sys.argv
    scratch = os.environ.get('TMPDIR')
    if not scratch or not Path(scratch).is_absolute():
        raise SystemExit('TMPDIR must name an absolute disposable scratch directory')
    base = Path(tempfile.mkdtemp(prefix='piper-gateway-probe-', dir=scratch))
    home = base / 'home'
    home.mkdir(mode=0o700)
    os.environ.update(HOME=str(base), HERMES_HOME=str(home), OPENAI_API_KEY='loopback-only')
    # This script starts a real gateway. Never inherit external platform, model,
    # or profile settings from the caller.
    allowed = {'PATH', 'LANG', 'TZ', 'TMPDIR', 'HOME', 'HERMES_HOME', 'OPENAI_API_KEY'}
    for key in list(os.environ):
        if key not in allowed:
            del os.environ[key]
    model = ModelServer(('127.0.0.1', 0), Model)
    model.requests = 0
    server_thread = threading.Thread(target=model.serve_forever)
    server_thread.start()
    url = f'http://127.0.0.1:{model.server_port}/v1'
    os.environ['OPENAI_BASE_URL'] = url
    config = {'gateway': {'multiplex_profiles': False},
              'model': {'provider': 'custom', 'default': 'loopback-fixture', 'base_url': url},
              'auxiliary': {'title_generation': {'enabled': False}}}
    if required:
        (home / 'plugins').mkdir()
        shutil.copytree(ROOT / 'contrib/piper-turn-outbox', home / 'plugins/piper-turn-outbox')
        config['gateway']['required_settlement_plugins'] = ['piper-turn-outbox']
        config['plugins'] = {'isolation': 'in_process', 'enabled': ['piper-turn-outbox'],
                             'entries': {'piper-turn-outbox': {'settings': {
                                 'route': 'agent:main:telegram:dm:fixture'}}}}
    (home / 'config.yaml').write_text(json.dumps(config))
    try:
        from gateway.run import start_gateway
        from tests.gateway.test_normal_runtime_boot import control
        from websockets.asyncio.client import connect
        task = asyncio.create_task(start_gateway(verbosity=None))
        try:
            descriptor = None
            for _ in range(400):
                if task.done():
                    raise RuntimeError(f'gateway exited: {task.result()!r}')
                try:
                    descriptor = await asyncio.to_thread(control, home, 'identify')
                    if descriptor.get('state') == 'ready':
                        break
                except (OSError, ValueError, AssertionError):
                    pass
                await asyncio.sleep(.1)
            assert descriptor and descriptor.get('state') == 'ready', descriptor
            assert descriptor['served_profiles'] == [{'profile_id': str(home), 'home': str(home)}]
            assert descriptor['api_origin'].startswith('http://127.0.0.1:'), descriptor['api_origin']
            if required:
                from gateway.session_settlement_extensions import required_extensions
                from types import SimpleNamespace
                callbacks = await asyncio.to_thread(
                    required_extensions, SimpleNamespace(db_path=home / 'state.db'), str(home))
                assert len(callbacks) == 1, 'required writer was not registered'
            binding = {'profile_id': str(home), 'instance_id': descriptor['instance_id'], 'purpose': 'interactive'}
            grant = await asyncio.to_thread(control, home, 'session-ticket', binding)
            uri = descriptor['api_origin'].replace('http:', 'ws:', 1) + '/api/ws'
            async with connect(uri, subprotocols=['hermes-gateway-v1',
                                                 'hermes-gateway-ticket.' + grant['ticket']]) as ws:
                async def rpc(method, **params):
                    await ws.send(json.dumps({'jsonrpc': '2.0', 'id': method, 'method': method, 'params': params}))
                    async with asyncio.timeout(30):
                        while True:
                            response = json.loads(await ws.recv())
                            if response.get('id') == method:
                                assert 'result' in response, response
                                return response['result']
                session = await rpc('session.create', request_id='fixture-create', source='cli')
                sid = session['session_id']
                sent = await rpc('prompt.submit', session_id=sid, input_id='fixture-input', text='fixture message')
                assert sent['status'] == 'queued', sent
                for _ in range(300):
                    snapshot = await rpc('session.resume', session_id=sid)
                    if 'STAGING_NORMAL_CHAT_ACK' in json.dumps(snapshot.get('messages', [])):
                        break
                    await asyncio.sleep(.1)
                else:
                    raise AssertionError('model reply did not appear in the transcript')
                rows = []
                for _ in range(100):
                    with sqlite3.connect(f'file:{home / "state.db"}?mode=ro', uri=True) as db:
                        rows = db.execute("SELECT status,outcome FROM session_admissions WHERE request_id='fixture-input'").fetchall()
                    if rows and rows[0][0] == 'terminal':
                        break
                    await asyncio.sleep(.1)
                assert rows == [('terminal', 'completed')], rows
                assert model.requests >= 1, model.requests
                print(f'PASS real gateway normal chat: required_plugin={required}; terminal=completed; model_requests={model.requests}', flush=True)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    finally:
        model.shutdown()
        server_thread.join()
        model.server_close()
        # The gateway's logging threads may still drain at interpreter shutdown.
        # Scratch is automatically pruned; removing the home here races them.
        print(f'Disposable staging home: {base}', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
