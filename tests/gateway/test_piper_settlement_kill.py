"""Kill a real owner process on each side of the transaction commit boundary."""
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys

import pytest

from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, get_session_admission, recover_session_inputs

ROUTE = 'agent:main:telegram:dm:test'
TURN = 'web-1234567890123456'

_CHILD = r'''
import json, os, sys
from pathlib import Path
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, admit_session_input, claim_session_input
from gateway.session_results import finish_result
import gateway.session_settlement_recovery as recovery
import gateway.piper_turn_outbox as outbox
recovery.owner_stop_notice = lambda *_: None
home = Path(os.environ['HERMES_HOME'])
db = SessionDB(home / 'state.db')
epoch = begin_runtime_epoch(db, instance_id='killed-owner')
db.create_session('turn', source='telegram')
row = admit_session_input(db, epoch=epoch, session_id='turn', principal_id='automation:' + sys.argv[1],
    request_id=json.dumps(['piper-shared-thread','second-brain-web',sys.argv[2]]), payload={'text':'synthetic'})
claim = claim_session_input(db, epoch=epoch, session_id='turn')
print('CLAIM ' + row['admission_id'], flush=True)
if sys.argv[3] == 'before':
    insert = outbox.insert_completion
    def stop_before_commit(conn, admission_id, turn_id):
        insert(conn, admission_id, turn_id)
        print('BEFORE_COMMIT', flush=True)
        sys.stdin.read(1)
    outbox.insert_completion = stop_before_commit
captured = {'result': {'completed': True, 'final_response': 'answer', 'messages': []}, 'usage': {}}
if sys.argv[4] == 'captured':
    import asyncio
    from types import SimpleNamespace
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import SessionAuthority, LiveSession
    from gateway.session_contract import Principal, SessionRef
    store = SessionStore(config=GatewayConfig(), sessions_dir=home / 'sessions')
    store._db = db
    runner = SimpleNamespace(_draining=False, session_store=store)
    owner = SessionAuthority(runner, profile_id=str(home), instance_id='killed-owner', db=db, epoch=epoch)
    owner.sessions['turn'] = LiveSession(None, sys.argv[1])
    owner.pending_results[row['admission_id']] = captured
    db._execute_write(lambda conn: conn.execute("UPDATE session_admissions SET status='unknown' WHERE admission_id=?",
                                               (row['admission_id'],)))
    async def resolved(_ref, _id, _prepared, _captured, result): return result
    owner._resolved_unknown = resolved
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    asyncio.run(owner.resolve_unknown(actor, SessionRef(owner.profile_id, 'turn'), row['admission_id'], claim['generation']))
else:
    finish_result(db, epoch=epoch, row=claim, route=sys.argv[1], profile_id=str(home), response='answer',
                  outcome='completed', result=captured)
print('AFTER_COMMIT', flush=True)
sys.stdin.read(1)
'''


@pytest.mark.parametrize('point', ['before', 'after'])
@pytest.mark.parametrize('path', ['normal', 'captured'])
@pytest.mark.platforms('posix')
def test_killed_owner_has_atomic_admission_and_outbox(tmp_path, point, path):
    home = tmp_path / 'home'
    home.mkdir()
    repo = Path(__file__).resolve().parents[2]
    shutil.copytree(repo / 'contrib/piper-turn-outbox', home / 'plugins/piper-turn-outbox')
    (home / 'config.yaml').write_text(
        'gateway:\n  required_settlement_plugins: [piper-turn-outbox]\n'
        'plugins:\n  enabled: [piper-turn-outbox]\n  entries:\n    piper-turn-outbox:\n'
        f'      settings:\n        route: "{ROUTE}"\n')
    env = dict(os.environ, HERMES_HOME=str(home), HOME=str(home), HERMES_RUNTIME_DIR=str(home / 'runtime'))
    child = subprocess.Popen([sys.executable, '-u', '-c', _CHILD, ROUTE, TURN, point, path], cwd=repo,
                             env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    try:
        first = child.stdout.readline().strip()
        assert first.startswith('CLAIM '), (first, child.stderr.read() if child.poll() is not None else '')
        admission = first.split()[1]
        marker = child.stdout.readline().strip()
        assert marker == ('BEFORE_COMMIT' if point == 'before' else 'AFTER_COMMIT'), marker
        child.send_signal(signal.SIGKILL)
        assert child.wait(timeout=10) == -signal.SIGKILL
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream:
                stream.close()
    db = SessionDB(home / 'state.db')
    try:
        row = get_session_admission(db, admission_id=admission)
        with db._read_ctx() as conn:
            table = conn.execute("SELECT 1 FROM sqlite_master WHERE name='piper_turn_outbox'").fetchone()
            events = conn.execute('SELECT admission_id FROM piper_turn_outbox').fetchall() if table else []
        if point == 'before':
            assert row['status'] == ('started' if path == 'normal' else 'unknown')
            assert events == []
            epoch = begin_runtime_epoch(db, instance_id='replacement')
            recover_session_inputs(db, epoch=epoch)
            from gateway.session_unattended_unknown import settle_recovered_unattended
            assert settle_recovered_unattended(db, epoch) == 1
            row = get_session_admission(db, admission_id=admission)
            assert row['outcome'] == 'interrupted'
        else:
            assert row['status'] == 'terminal' and row['outcome'] == 'completed'
            assert [r[0] for r in events] == [admission]
    finally:
        db.close()
