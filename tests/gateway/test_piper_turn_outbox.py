"""The opt-in Piper outbox commits with the actual authority settlement."""
import json
import sqlite3

import pytest

from gateway.session_results import finish_result
from hermes_state_runtime import admit_session_input, claim_session_input, get_session_admission

ROUTE = 'agent:main:telegram:dm:test'
TURN = 'web-1234567890123456'


@pytest.fixture(autouse=True)
def isolate_stop_notice(monkeypatch):
    # Avoid importing the live install bootstrap through owner_stop_notice.
    import gateway.session_settlement_recovery as settlement
    monkeypatch.setattr(settlement, 'owner_stop_notice', lambda *_args: None)


def claim(owner, *, turn=TURN, principal=None, producer='piper-shared-thread'):
    owner.db.create_session(turn, source='telegram')
    identity = json.dumps([producer, 'second-brain-web', turn], separators=(',', ':'))
    admitted = admit_session_input(owner.db, epoch=owner.epoch, principal_id=principal or 'automation:' + ROUTE,
                                   session_id=turn, request_id=identity, payload={'text': 'private question'})
    row = claim_session_input(owner.db, epoch=owner.epoch, session_id=turn)
    assert row['admission_id'] == admitted['admission_id']
    return row


def finish(owner, row, response='private answer', outcome='completed', route=ROUTE):
    return finish_result(owner.db, epoch=owner.epoch, row=row, response=response, outcome=outcome,
                         result={'result': {'final_response': response, 'messages': [], 'completed': True}, 'usage': {}},
                         piper_route=route)


def events(owner):
    with owner.db._read_ctx() as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE name='piper_turn_outbox'").fetchone()
        if not exists:
            return []
        return [tuple(r) for r in conn.execute('SELECT admission_id, turn_id, event_type FROM piper_turn_outbox')]


def test_completion_replay_and_private_payload(owner, monkeypatch):
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    row = claim(owner)
    assert events(owner) == []
    finish(owner, row)
    assert events(owner) == [(row['admission_id'], TURN, 'piper.turn.completed.v1')]
    finish(owner, row, response='different replay')
    assert len(events(owner)) == 1
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['outcome'] == 'completed'


@pytest.mark.parametrize('scenario', ['disabled', 'other-route', 'other-producer', 'other-principal',
                                      'failed', 'interrupted', 'empty', 'silent'])
def test_no_ineligible_events(owner, monkeypatch, scenario):
    if scenario != 'disabled':
        monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    else:
        monkeypatch.delenv('PIPER_TURN_OUTBOX_ROUTE', raising=False)
    row = claim(owner, producer='other' if scenario == 'other-producer' else 'piper-shared-thread',
                principal='messaging:other' if scenario == 'other-principal' else None)
    finish(owner, row, route='other' if scenario == 'other-route' else ROUTE,
           outcome=scenario if scenario in ('failed', 'interrupted') else 'completed',
           response={'empty': '', 'silent': 'NO_REPLY'}.get(scenario, 'answer'))
    assert events(owner) == []


@pytest.mark.parametrize('turn', ['abcdefgh', 'web-' + 'a' * 15, 'web-' + 'a' * 61, 'web-' + 'a' * 15 + '.'])
def test_receiver_incompatible_web_ids_never_enter_outbox(owner, monkeypatch, turn):
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    row = claim(owner, turn=turn)
    finish(owner, row)
    assert events(owner) == []


@pytest.mark.parametrize('size', [16, 60])
def test_web_id_contract_boundaries_are_deliverable(owner, monkeypatch, size):
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    turn = 'web-' + 'a' * size
    row = claim(owner, turn=turn)
    finish(owner, row)
    assert events(owner) == [(row['admission_id'], turn, 'piper.turn.completed.v1')]


def test_authority_commit_path_passes_live_route(owner, monkeypatch):
    from threading import RLock
    from types import SimpleNamespace
    import gateway.session_settlement_recovery as settlement
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    monkeypatch.setattr(settlement, '_publish_completion', lambda *args: None)
    monkeypatch.setattr(settlement, '_release_media', lambda *args: None)
    monkeypatch.setattr(settlement, '_transfers', lambda *args: None)
    row = claim(owner)
    live = SimpleNamespace(route=ROUTE, event_stream=SimpleNamespace(lock=RLock()), settling={})
    captured = {'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
    result = {}
    settlement.commit_and_publish(owner, live, SimpleNamespace(), row, 'answer', 'completed', captured, result)
    assert result['settled']['outcome'] == 'completed'
    assert events(owner) == [(row['admission_id'], TURN, 'piper.turn.completed.v1')]


def test_a_gateway_notice_without_an_agent_result_is_not_a_reply(owner, monkeypatch):
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    row = claim(owner)
    finish_result(owner.db, epoch=owner.epoch, row=row, response='gateway notice', outcome='completed',
                  piper_route=ROUTE)
    assert events(owner) == []


@pytest.mark.asyncio
async def test_captured_unknown_recovery_commits_one_event(owner, monkeypatch):
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
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
    assert events(owner) == [(row['admission_id'], TURN, 'piper.turn.completed.v1')]
    await owner.resolve_unknown(actor, ref, row['admission_id'], row['generation'])
    assert len(events(owner)) == 1


@pytest.mark.asyncio
async def test_captured_unknown_outbox_failure_rolls_back_resolution(owner, monkeypatch):
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal, SessionRef
    import gateway.piper_turn_outbox as outbox

    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    row = claim(owner)
    owner.sessions[TURN] = LiveSession(None, ROUTE)
    owner.pending_results[row['admission_id']] = {
        'result': {'final_response': 'answer', 'messages': [], 'completed': True}, 'usage': {}}
    owner.db._execute_write(lambda conn: conn.execute(
        "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
    original = outbox.insert_completion

    def fail(conn, admission_id, turn_id):
        original(conn, admission_id, turn_id)
        raise sqlite3.OperationalError('simulated recovery write failure')

    monkeypatch.setattr(outbox, 'insert_completion', fail)
    async def resolved(_ref, _id, _prepared, _captured, result):
        return result
    monkeypatch.setattr(owner, '_resolved_unknown', resolved)
    actor = Principal('human', owner.profile_id, frozenset({'session:control', 'session:submit'}), 'test')
    ref = SessionRef(owner.profile_id, TURN)
    with pytest.raises(sqlite3.OperationalError, match='simulated recovery write failure'):
        await owner.resolve_unknown(actor, ref, row['admission_id'], row['generation'])
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'unknown'
    assert events(owner) == []
    monkeypatch.setattr(outbox, 'insert_completion', original)
    result = await owner.resolve_unknown(actor, ref, row['admission_id'], row['generation'])
    assert result['outcome'] == 'completed'
    assert events(owner) == [(row['admission_id'], TURN, 'piper.turn.completed.v1')]


def test_outbox_failure_rolls_back_settlement(owner, monkeypatch):
    import gateway.piper_turn_outbox as outbox
    monkeypatch.setenv('PIPER_TURN_OUTBOX_ROUTE', ROUTE)
    row = claim(owner)
    def fail(conn, admission_id, turn_id):
        outbox_insert(conn, admission_id, turn_id)
        raise sqlite3.OperationalError('simulated failure')
    outbox_insert = outbox.insert_completion
    monkeypatch.setattr(outbox, 'insert_completion', fail)
    with pytest.raises(sqlite3.OperationalError):
        finish(owner, row)
    assert get_session_admission(owner.db, admission_id=row['admission_id'])['status'] == 'started'
    assert events(owner) == []
    monkeypatch.setattr(outbox, 'insert_completion', outbox_insert)
    finish(owner, row)
    assert len(events(owner)) == 1
