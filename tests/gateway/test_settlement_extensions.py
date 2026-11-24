"""Real plugin discovery and transaction-bound settlement registration."""
import json
import shutil
from pathlib import Path

import pytest

from gateway.session_results import finish_result
from hermes_state_runtime import admit_session_input, claim_session_input, get_session_admission

ROUTE = 'agent:main:telegram:dm:test'
TURN = 'web-1234567890123456'


def setup_config(home, *, install=True, plugin_name='piper-turn-outbox', optional=False):
    if install:
        source = Path(__file__).resolve().parents[2] / 'contrib' / 'piper-turn-outbox'
        shutil.copytree(source, home / 'plugins' / 'piper-turn-outbox')
    (home / 'config.yaml').write_text(
        f'gateway:\n  {"optional" if optional else "required"}_settlement_plugins: [{plugin_name}]\n'
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


def test_piper_outbox_failure_rolls_back_only_notification(owner, tmp_path, monkeypatch, caplog):
    import gateway.piper_turn_outbox as outbox
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    original = outbox.insert_completion

    def fail_after_insert(conn, admission_id, turn_id):
        original(conn, admission_id, turn_id)
        raise RuntimeError('private marker must not enter logs')

    monkeypatch.setattr(outbox, 'insert_completion', fail_after_insert)
    setup_config(tmp_path, optional=True)
    row = claim(owner)
    finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['outcome'] == 'completed'
    with owner.db._read_ctx() as conn:
        assert conn.execute("SELECT count(*) FROM sqlite_master WHERE name='piper_turn_outbox'").fetchone()[0] == 0
    assert 'Optional settlement writer failed; its effects were dropped' in caplog.text
    assert 'private marker' not in caplog.text


def test_missing_optional_writer_does_not_block_settlement(owner, tmp_path, monkeypatch, caplog):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path, install=False, optional=True)
    row = claim(owner)
    finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['outcome'] == 'completed'
    assert 'Optional settlement plugin unavailable' in caplog.text


def test_optional_writer_success_still_commits_event(owner, tmp_path, monkeypatch):
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path, optional=True)
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


def test_reload_refreshes_writer_and_unload_fails_closed(owner, tmp_path, monkeypatch):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from hermes_cli.plugins import get_plugin_manager
    from gateway.session_settlement_extensions import required_extensions, write_extensions
    import gateway.session_settlement_recovery as recovery
    monkeypatch.setattr(recovery, 'owner_stop_notice', lambda *_: None)
    setup_config(tmp_path)
    plugin_file = tmp_path / 'plugins' / 'piper-turn-outbox' / '__init__.py'
    def plugin(version):
        return ('def register(ctx):\n'
                '    def write(*, conn, **kwargs):\n'
                '        conn.execute("CREATE TABLE IF NOT EXISTS reload_probe (version TEXT)")\n'
                f'        conn.execute("INSERT INTO reload_probe VALUES (\'{version}\')")\n'
                '    ctx.register_settlement_extension(write)\n')
    plugin_file.write_text(plugin('first'))
    first = required_extensions(owner.db, str(tmp_path))
    token = set_hermes_home_override(tmp_path)
    try:
        manager = get_plugin_manager()
        # Change the source size as well as the contents so Python cannot reuse timestamped bytecode.
        plugin_file.write_text(plugin('second-version'))
        manager.discover_and_load(force=True)
        second = required_extensions(owner.db, str(tmp_path))
        for callbacks in (first, second):
            owner.db._execute_write(lambda conn: write_extensions(
                owner.db, callbacks, conn, row={}, route=ROUTE, response='answer',
                outcome='completed', result={}, profile_id=str(tmp_path)))
        with owner.db._read_ctx() as conn:
            assert [r[0] for r in conn.execute('SELECT version FROM reload_probe')] == ['first', 'second-version']
        assert manager.unload('piper-turn-outbox')
        with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
            required_extensions(owner.db, str(tmp_path))
    finally:
        reset_hermes_home_override(token)


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


@pytest.mark.asyncio
async def test_startup_missing_plugin_does_not_fence_existing_owner(owner, tmp_path):
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import settle_session_input
    setup_config(tmp_path, install=False)
    row = claim(owner)
    with pytest.raises(RuntimeError, match='required settlement plugin unavailable'):
        await initialize_session_authority(owner.runner, profile_id=str(tmp_path), instance_id='new-owner',
                                           db=owner.db, register=False)
    # The old owner can still complete under its original epoch: startup refused before rotating it.
    settled = settle_session_input(owner.db, epoch=owner.epoch, admission_id=row['admission_id'],
                                   generation=row['generation'], outcome='failed')
    assert settled['outcome'] == 'failed'


@pytest.mark.asyncio
async def test_discard_unknown_invokes_required_writer_with_interrupted_outcome(owner, tmp_path, monkeypatch):
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    setup_config(tmp_path)
    (tmp_path / 'plugins' / 'piper-turn-outbox' / '__init__.py').write_text(
        'def register(ctx):\n'
        '    def write(*, conn, row, route, outcome, result, **kw):\n'
        '        conn.execute("CREATE TABLE audit (admission TEXT, route TEXT, outcome TEXT)")\n'
        '        conn.execute("INSERT INTO audit VALUES (?, ?, ?)", (row["admission_id"], route, outcome))\n'
        '        assert result is None\n'
        '    ctx.register_settlement_extension(write)\n')
    row = claim(owner)
    owner.sessions[TURN] = LiveSession(None, ROUTE)
    owner.db._execute_write(lambda conn: conn.execute(
        "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
    async def resolved(_ref, _id, _prepared, _captured, result):
        return result
    monkeypatch.setattr(owner, '_resolved_unknown', resolved)
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    settled = await owner.resolve_unknown(actor, SessionRef(owner.profile_id, TURN), row['admission_id'], row['generation'])
    assert settled['outcome'] == 'interrupted'
    with owner.db._read_ctx() as conn:
        assert tuple(conn.execute('SELECT * FROM audit').fetchone()) == (row['admission_id'], ROUTE, 'interrupted')


def test_boot_interruption_writer_failure_rolls_back(owner, tmp_path):
    from gateway.session_unattended_unknown import settle_recovered_unattended
    from hermes_state_runtime import begin_runtime_epoch, recover_session_inputs
    setup_config(tmp_path)
    (tmp_path / 'plugins' / 'piper-turn-outbox' / '__init__.py').write_text(
        'def register(ctx):\n'
        '    def write(*, conn, outcome, route, **kw):\n'
        '        assert outcome == "interrupted"\n'
        f'        assert route == "{ROUTE}"\n'
        '        conn.execute("CREATE TABLE audit (id TEXT)")\n'
        '        raise RuntimeError("audit failed")\n'
        '    ctx.register_settlement_extension(write)\n')
    row = claim(owner)
    epoch = begin_runtime_epoch(owner.db, instance_id='restarted')
    recover_session_inputs(owner.db, epoch=epoch)
    with pytest.raises(RuntimeError, match='audit failed'):
        settle_recovered_unattended(owner.db, epoch, profile_id=str(tmp_path))
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'unknown'
    with owner.db._read_ctx() as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='audit'").fetchone() is None


def test_restart_loses_uncommitted_capture_and_does_not_invent_completion(owner, tmp_path):
    from gateway.session_unattended_unknown import settle_recovered_unattended
    from hermes_state_runtime import begin_runtime_epoch, recover_session_inputs
    setup_config(tmp_path)
    row = claim(owner)
    # A captured result is held in the dead owner's memory, not durably stored.
    owner.pending_results[row['admission_id']] = {
        'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
    new_epoch = begin_runtime_epoch(owner.db, instance_id='restarted')
    recover_session_inputs(owner.db, epoch=new_epoch)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'unknown'
    assert settle_recovered_unattended(owner.db, new_epoch) == 1
    recovered = get_session_admission(owner.db, admission_id=row['admission_id'])
    assert recovered['status'] == 'terminal'
    assert recovered['outcome'] == 'interrupted'
    with owner.db._read_ctx() as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='piper_turn_outbox'").fetchone() is None


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
