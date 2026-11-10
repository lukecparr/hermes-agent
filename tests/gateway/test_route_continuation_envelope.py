"""Luke's fork: opt-in route continuation for producer admissions after a reset (piper-shared-thread)."""
import contextlib
import json
import sqlite3
from types import SimpleNamespace

from gateway import session_automation as sa

ROUTE = "agent:main:telegram:dm:42"


def _authority(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE session_admissions(seq INTEGER PRIMARY KEY, principal_id TEXT, payload_json TEXT)")
    for principal, payload in rows:
        conn.execute("INSERT INTO session_admissions(principal_id,payload_json) VALUES(?,?)",
                     (principal, json.dumps(payload)))

    @contextlib.contextmanager
    def read_ctx():
        yield conn
    return SimpleNamespace(db=SimpleNamespace(_read_ctx=read_ctx))


def _native(route, provenance=True, tag="x"):
    env = {"route": route, "source": {"chat_id": "42", "tag": tag}}
    if provenance:
        env["provenance"] = {"connector": "c"}
    return {"native_text_v1": env}


def test_only_listed_producers_continue_a_route():
    assert sa._continues_route(json.dumps(["piper-shared-thread", "second-brain-web", "t1"]))
    assert not sa._continues_route(json.dumps(["completion", "proc", 1.0]))
    assert not sa._continues_route(json.dumps(["plugin", "piper-shared-thread", ROUTE, "u"]))
    assert not sa._continues_route("tg-1")
    assert not sa._continues_route("[not json")
    assert not sa._continues_route(None)


def test_fallback_borrows_latest_human_envelope_of_the_same_route_only():
    authority = _authority([
        ("messaging:[1]", _native(ROUTE, tag="older")),
        ("messaging:[1]", _native(ROUTE, tag="newest")),
        ("messaging:[2]", _native("agent:main:telegram:dm:99", tag="other-chat")),
        ("automation:" + ROUTE, _native(ROUTE, tag="producer")),
        ("messaging:[1]", _native(ROUTE, provenance=False, tag="no-provenance")),
    ])
    env = sa._route_envelope_before_reset(authority, ROUTE)
    assert env["source"]["tag"] == "newest"
    assert sa._route_envelope_before_reset(authority, "agent:main:telegram:dm:7") is None


def test_committed_route_envelope_falls_back_only_when_asked(monkeypatch):
    authority = _authority([("messaging:[1]", _native(ROUTE, tag="before-reset"))])
    authority.logical_owner = lambda sid: sid
    monkeypatch.setattr("hermes_state_runtime.list_session_admissions", lambda *a, **k: [])
    entry = SimpleNamespace(session_id="fresh", session_key=ROUTE)
    assert sa.committed_route_envelope(authority, entry) is None
    assert sa.committed_route_envelope(authority, entry, route_fallback=True)["source"]["tag"] == "before-reset"
