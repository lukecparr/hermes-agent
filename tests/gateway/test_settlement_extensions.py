"""Real plugin discovery and transaction-bound settlement registration."""
import json
import shutil
from pathlib import Path

import pytest

from gateway.session_results import finish_result
from hermes_state_runtime import admit_session_input, claim_session_input, get_session_admission

ROUTE = 'agent:main:telegram:dm:test'
TURN = 'web-1234567890123456'


def setup_config(home, *, install=True, plugin_name='piper-turn-outbox'):
    if install:
        source = Path(__file__).resolve().parents[2] / 'contrib' / 'piper-turn-outbox'
        shutil.copytree(source, home / 'plugins' / 'piper-turn-outbox')
    (home / 'config.yaml').write_text(
        f'gateway:\n  required_settlement_plugins: [{plugin_name}]\n'
        f'plugins:\n  enabled: [piper-turn-outbox]\n  entries:\n'
        f'    piper-turn-outbox:\n      settings:\n        route: "{ROUTE}"\n')


def claim(owner):
    owner.db.create_session(TURN, source='telegram')
    admit_session_input(owner.db, epoch=owner.epoch, principal_id='automation:' + ROUTE,
                        session_id=TURN, request_id=json.dumps(['piper-shared-thread', 'second-brain-web', TURN]),
                        payload={'text': 'question'})
    return claim_session_input(owner.db, epoch=owner.epoch, session_id=TURN)


def finish(owner, row):
    return finish_result(owner.db, epoch=owner.epoch, row=row, response='answer', outcome='completed',
                         result={'result': {'completed': True, 'final_response': 'answer', 'messages': []}},
                         route=ROUTE, profile_id=owner.profile_id)


def test_real_plugin_discovery_writes_with_settlement(owner, tmp_path, monkeypatch):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path)
    row = claim(owner)
    finish(owner, row)
    with owner.db._read_ctx() as conn:
        assert tuple(conn.execute('SELECT admission_id, turn_id FROM piper_turn_outbox').fetchone()) == (
            row['admission_id'], TURN)


def test_callback_uses_database_owner_profile(owner, tmp_path, monkeypatch):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    profile = tmp_path / 'owner-profile'
    profile.mkdir()
    owner.profile_id = str(profile)
    setup_config(profile)
    (profile / 'plugins' / 'piper-turn-outbox' / '__init__.py').write_text(
        'from hermes_constants import get_hermes_home\n'
        'def register(ctx):\n'
        '    def write(*, conn, **kwargs):\n'
        '        conn.execute("CREATE TABLE scope_probe (home TEXT)")\n'
        '        conn.execute("INSERT INTO scope_probe VALUES (?)", (str(get_hermes_home()),))\n'
        '    ctx.register_settlement_extension(write)\n')
    other = tmp_path / 'other-profile'
    other.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(other))
    row = claim(owner)
    finish(owner, row)
    with owner.db._read_ctx() as conn:
        assert Path(conn.execute('SELECT home FROM scope_probe').fetchone()[0]) == profile


def test_host_isolation_cannot_register_required_extension(owner, tmp_path, monkeypatch):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path)
    config = tmp_path / 'config.yaml'
    config.write_text(config.read_text().replace('plugins:\n', 'plugins:\n  isolation: host\n'))
    row = claim(owner)
    with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
        finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'started'


def test_missing_required_plugin_refuses_settlement(owner, tmp_path, monkeypatch):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path, install=False)
    row = claim(owner)
    with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
        finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'started'


@pytest.mark.parametrize('source', [
    'def register(ctx):\n    pass\n',
    'def register(ctx):\n    ctx.register_settlement_extension(lambda **kw: None)\n    raise ValueError("broken")\n',
    'def register(ctx):\n    ctx.register_settlement_extension(None)\n',
])
def test_failed_or_missing_registration_fails_closed(owner, tmp_path, monkeypatch, source):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path)
    (tmp_path / 'plugins' / 'piper-turn-outbox' / '__init__.py').write_text(source)
    row = claim(owner)
    with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
        finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'started'


@pytest.mark.asyncio
async def test_real_plugin_captured_recovery(owner, tmp_path, monkeypatch):
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    setup_config(tmp_path)
    row = claim(owner)
    owner.sessions[TURN] = LiveSession(None, ROUTE)
    owner.pending_results[row['admission_id']] = {
        'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
    owner.db._execute_write(lambda conn: conn.execute(
        "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
    async def resolved(_ref, _id, _prepared, _captured, result):
        return result
    monkeypatch.setattr(owner, '_resolved_unknown', resolved)
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    ref = SessionRef(owner.profile_id, TURN)
    result = await owner.resolve_unknown(actor, ref, row['admission_id'], row['generation'])
    assert result['outcome'] == 'completed'
    with owner.db._read_ctx() as conn:
        assert tuple(conn.execute('SELECT admission_id, turn_id FROM piper_turn_outbox').fetchone()) == (
            row['admission_id'], TURN)


@pytest.mark.parametrize('resolution', ['normal', 'captured'])
@pytest.mark.asyncio
async def test_real_plugin_write_failure_rolls_back(owner, tmp_path, monkeypatch, resolution):
    import gateway.session_settlement_recovery as recovery
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path)
    (tmp_path / 'plugins' / 'piper-turn-outbox' / '__init__.py').write_text(
        'def register(ctx):\n'
        '    def write(*, conn, **kwargs):\n'
        '        conn.execute("CREATE TABLE probe (id INTEGER)")\n'
        '        conn.execute("INSERT INTO probe VALUES (1)")\n'
        '        raise RuntimeError("write failed")\n'
        '    ctx.register_settlement_extension(write)\n')
    row = claim(owner)
    if resolution == 'normal':
        with pytest.raises(RuntimeError, match='write failed'):
            finish(owner, row)
        expected = 'started'
    else:
        owner.sessions[TURN] = LiveSession(None, ROUTE)
        owner.pending_results[row['admission_id']] = {
            'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
        owner.db._execute_write(lambda conn: conn.execute(
            "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
        actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
        with pytest.raises(RuntimeError, match='write failed'):
            await owner.resolve_unknown(actor, SessionRef(owner.profile_id, TURN), row['admission_id'], row['generation'])
        expected = 'unknown'
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == expected
    with owner.db._read_ctx() as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='probe'").fetchone() is None


@pytest.mark.asyncio
async def test_missing_plugin_fences_captured_recovery(owner, tmp_path, monkeypatch):
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    setup_config(tmp_path, install=False)
    row = claim(owner)
    owner.sessions[TURN] = LiveSession(None, ROUTE)
    owner.pending_results[row['admission_id']] = {
        'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
    owner.db._execute_write(lambda conn: conn.execute(
        "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    ref = SessionRef(owner.profile_id, TURN)
    with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
        await owner.resolve_unknown(actor, ref, row['admission_id'], row['generation'])
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'unknown'
