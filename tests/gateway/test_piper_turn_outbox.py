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
