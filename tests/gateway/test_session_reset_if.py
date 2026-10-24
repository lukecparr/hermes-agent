"""``SessionStore.reset_session_if``: a compare-and-rotate for a messaging route.

The compare (expected session id, no active turn, no in-flight get-or-create, the caller's
boundary predicate) and the route replacement happen under ONE hold of ``SessionStore._lock``,
so two racing callers (a Telegram first message through the reset-policy plugin and a
Charm-voice ensure through the API server) produce exactly one rotation.
"""
from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionSource, SessionStore


def _store(tmp_path, with_db=False):
    with patch("gateway.session.SessionStore._ensure_loaded"):
        store = SessionStore(sessions_dir=tmp_path / "sessions", config=GatewayConfig())
    store._loaded = True
    db = None
    if with_db:
        from hermes_state import SessionDB
        db = SessionDB(db_path=tmp_path / "state.db")
        store._db = db
    return store, db


def _telegram(store, chat_id="8532409712"):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, chat_type="dm",
                           user_id=chat_id)
    return store.get_or_create_session(source)


def _due(_entry):
    return True


def test_rotates_when_expected_and_due(tmp_path):
    store, db = _store(tmp_path, with_db=True)
    old = _telegram(store)
    result = store.reset_session_if(old.session_key, expected_session_id=old.session_id,
                                    should_reset=_due, metadata={"srp_last_user_inbound": "x"})
    assert result.status == "reset"
    new = result.entry
    assert new.session_id != old.session_id
    assert new.is_fresh_reset is True
    assert new.metadata == {"srp_last_user_inbound": "x"}
    assert store.lookup_by_session_key(old.session_key).session_id == new.session_id
    # Same durable transition as reset_session: old row ended, new row parented on it.
    assert db.get_session(old.session_id)["end_reason"] == "session_reset"
    row = db.get_session(new.session_id)
    assert row["ended_at"] is None and row["parent_session_id"] == old.session_id
    assert row["source"] == "telegram"
    db.close()


def test_moved_route_is_left_alone(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)
    result = store.reset_session_if(entry.session_key, expected_session_id="someone-else",
                                    should_reset=_due)
    assert result.status == "moved"
    assert result.entry.session_id == entry.session_id
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id


def test_active_turn_is_busy(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)
    store._entries[entry.session_key].active_turn_token = "tok-1"
    result = store.reset_session_if(entry.session_key, expected_session_id=entry.session_id,
                                    should_reset=_due)
    assert result.status == "busy"
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id


def test_inflight_get_or_create_is_busy(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)
    store._inflight_sessions[entry.session_key] = object()
    try:
        result = store.reset_session_if(entry.session_key, expected_session_id=entry.session_id,
                                        should_reset=_due)
    finally:
        store._inflight_sessions.pop(entry.session_key, None)
    assert result.status == "busy"
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id


def test_predicate_runs_under_the_lock_and_false_means_not_due(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)
    seen = []

    def predicate(e):
        seen.append((store._lock.locked(), e.session_id))
        return False

    result = store.reset_session_if(entry.session_key, expected_session_id=entry.session_id,
                                    should_reset=predicate)
    assert result.status == "not_due"
    assert seen == [(True, entry.session_id)]
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id


def test_predicate_error_propagates_without_rotation(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)

    def boom(_e):
        raise RuntimeError("bad policy")

    with pytest.raises(RuntimeError):
        store.reset_session_if(entry.session_key, expected_session_id=entry.session_id,
                               should_reset=boom)
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id
    assert not store._lock.locked()


def test_no_route(tmp_path):
    store, _ = _store(tmp_path)
    result = store.reset_session_if("agent:main:telegram:dm:none", expected_session_id="x",
                                    should_reset=_due)
    assert result.status == "no_route" and result.entry is None


def test_empty_expected_is_refused(tmp_path):
    store, _ = _store(tmp_path)
    entry = _telegram(store)
    with pytest.raises(ValueError):
        store.reset_session_if(entry.session_key, expected_session_id="", should_reset=_due)


def test_local_routes_are_unsupported(tmp_path):
    store, _ = _store(tmp_path)
    entry = store.get_or_create_session(SessionSource(
        platform=Platform.LOCAL, chat_id="local-1", chat_type="dm", user_id="u"))
    result = store.reset_session_if(entry.session_key, expected_session_id=entry.session_id,
                                    should_reset=_due)
    assert result.status == "unsupported"
    assert store.lookup_by_session_key(entry.session_key).session_id == entry.session_id


def test_concurrent_callers_rotate_exactly_once(tmp_path):
    store, db = _store(tmp_path, with_db=True)
    old = _telegram(store)
    barrier = threading.Barrier(8)
    results = []

    def racer():
        barrier.wait()
        results.append(store.reset_session_if(
            old.session_key, expected_session_id=old.session_id, should_reset=_due))

    threads = [threading.Thread(target=racer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    statuses = sorted(r.status for r in results)
    assert statuses == ["moved"] * 7 + ["reset"]
    winner = next(r.entry for r in results if r.status == "reset")
    assert {r.entry.session_id for r in results} == {winner.session_id}
    assert store.lookup_by_session_key(old.session_key).session_id == winner.session_id
    # One new durable row, one ended old row.
    with db._read_ctx() as conn:
        children = conn.execute("SELECT id FROM sessions WHERE parent_session_id = ?",
                                (old.session_id,)).fetchall()
    assert [c[0] for c in children] == [winner.session_id]
    assert db.get_session(old.session_id)["end_reason"] == "session_reset"
    db.close()


def test_reevaluated_predicate_sees_a_fresh_successor(tmp_path):
    """A caller that read a stale snapshot, then lost the race to an unconditional reset
    (``/new``), sees ``moved`` and never rotates the successor."""
    store, _ = _store(tmp_path)
    old = _telegram(store)
    new = store.reset_session(old.session_key)
    result = store.reset_session_if(old.session_key, expected_session_id=old.session_id,
                                    should_reset=_due)
    assert result.status == "moved" and result.entry.session_id == new.session_id
    assert store.lookup_by_session_key(old.session_key).session_id == new.session_id


def test_reset_session_unchanged(tmp_path):
    store, _ = _store(tmp_path)
    old = _telegram(store)
    new = store.reset_session(old.session_key)
    assert new.session_id != old.session_id and new.is_fresh_reset is True
    assert new.metadata == {}
