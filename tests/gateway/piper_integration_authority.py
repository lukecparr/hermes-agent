"""Disposable authority-to-HTTP exercise, invoked only by piper_integration_receiver.ts."""
import asyncio
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
from types import SimpleNamespace

home = Path(os.environ['HERMES_HOME']).resolve()
repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo))
import hermes_bootstrap  # Activate this checkout's pinned dependencies in the child interpreter.
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, admit_session_input, claim_session_input
from gateway.config import GatewayConfig
from gateway.session import SessionStore
from gateway.session_authority import SessionAuthority, LiveSession
from gateway.session_contract import Principal, SessionRef
from gateway.session_settlement_recovery import commit_and_publish
from gateway.piper_outbox_sender import send_one

route = 'agent:main:telegram:dm:test'
turn = 'web-1234567890123456'
shutil.copytree(repo / 'contrib/piper-turn-outbox', home / 'plugins/piper-turn-outbox')
(home / 'config.yaml').write_text(
    'gateway:\n  required_settlement_plugins: [piper-turn-outbox]\n'
    'plugins:\n  enabled: [piper-turn-outbox]\n  entries:\n    piper-turn-outbox:\n'
    f'      settings:\n        include_preview: true\n        route: "{route}"\n')
path = home / 'state.db'
db = SessionDB(path)
epoch = begin_runtime_epoch(db, instance_id='integration')
session = 'fixture-physical-session'
db.create_session(session, source='telegram')
a = admit_session_input(db, epoch=epoch, principal_id='automation:' + route, session_id=session,
    request_id=json.dumps(['piper-shared-thread', 'second-brain-web', turn]), payload={'text': 'synthetic'})
row = claim_session_input(db, epoch=epoch, session_id=session)
store = SessionStore(config=GatewayConfig(), sessions_dir=home / 'sessions')
store._db = db
runner = SimpleNamespace(_draining=False, session_store=store)
owner = SessionAuthority(runner, profile_id=str(home), instance_id='integration', db=db, epoch=epoch)
runner.session_authority = owner
live = LiveSession(None, route)
owner.sessions[session] = live
captured = {'result': {'final_response': '**Hello** <b>Luke</b> &amp; hi',
                       'messages': [{'role': 'tool', 'content': 'PRIVATE TOOL RESULT'},
                                    {'role': 'assistant', 'reasoning': 'PRIVATE REASONING'}], 'completed': True,
                       'already_sent': True, '_notification_reply_muted': False}, 'usage': {}}
if os.environ.get('PIPER_TEST_RECOVERY') == '1':
    owner.pending_results[row['admission_id']] = captured
    db._execute_write(lambda conn: conn.execute("UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
    async def resolved(_ref, _id, _prepared, _captured, result):
        return result
    owner._resolved_unknown = resolved
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    result = asyncio.run(owner.resolve_unknown(actor, SessionRef(owner.profile_id, session), row['admission_id'], row['generation']))
    assert result['outcome'] == 'completed'
else:
    settlement = {}
    # Streaming already sent the answer; the adapter deliberately returns None.
    commit_and_publish(owner, live, SessionRef(owner.profile_id, session), row, None, 'completed', captured, settlement)
    assert settlement['settled']['outcome'] == 'completed'
with sqlite3.connect(path, isolation_level=None) as conn:
    assert conn.execute('SELECT count(*) FROM piper_turn_outbox').fetchone()[0] == 1
    if os.environ.get('PIPER_TEST_LOCAL_WAKE') == '1':
        from functools import partial
        from urllib.request import Request, urlopen
        from scripts.piper_completion_worker import run
        def wake():
            with urlopen(Request(sys.argv[1] + 'wake', data=b'', method='POST',
                                 headers={'Authorization': 'Bearer ' + 'x' * 32}), timeout=10) as response:
                assert response.status == 204
        assert run(conn, sys.argv[1], 'x' * 32, once=True,
                   send=partial(send_one, include_preview=True), on_sent=wake) == 'sent'
    else:
        assert send_one(conn, sys.argv[1], 'x' * 32, include_preview=True) == 'sent'
print('AUTHORITY_SETTLED', a['admission_id'], flush=True)
db.close()
