"""The completion worker is explicit; it never starts or creates a database."""
import sqlite3

import pytest

from gateway.piper_turn_outbox import insert_completion
from scripts import piper_completion_worker as worker

URL = 'http://127.0.0.1:12345/completion'
TOKEN = 'x' * 32


def test_once_leases_and_acknowledges_existing_outbox(tmp_path, monkeypatch):
    db = tmp_path / 'state.db'
    with sqlite3.connect(db) as conn:
        insert_completion(conn, 'a' * 32, 'web-' + 'x' * 16)
        conn.commit()
    monkeypatch.setenv('PIPER_COMPLETION_URL', URL)
    monkeypatch.setenv('PIPER_COMPLETION_TOKEN', TOKEN)
    import gateway.piper_outbox_sender as sender
    monkeypatch.setattr(sender, 'post_event', lambda *args: 204)
    # The worker's wrapper uses the sender's real lease and fencing code.
    monkeypatch.setattr(worker, 'send_one', lambda conn, url, token: sender.send_one(
        conn, url, token, transport=lambda *_: 204))
    # Default arguments are bound at definition time; invoke the real run with injection.
    with sqlite3.connect(db) as conn:
        result = worker.run(conn, URL, TOKEN, once=True, send=worker.send_one)
        assert result == 'sent'
        assert conn.execute('SELECT state, attempts FROM piper_turn_outbox').fetchone() == ('sent', 1)


def test_run_drains_success_before_sleeping_and_stops_on_shutdown():
    events = iter(['sent', 'empty'])
    calls = []
    def send(*args):
        result = next(events)
        calls.append(result)
        return result
    def sleep(seconds):
        calls.append(seconds)
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        worker.run(None, URL, TOKEN, send=send, sleep=sleep, interval=7)
    assert calls == ['sent', 'empty', 7]


def test_no_missing_database_creation(tmp_path, monkeypatch):
    monkeypatch.setenv('PIPER_COMPLETION_URL', URL)
    monkeypatch.setenv('PIPER_COMPLETION_TOKEN', TOKEN)
    missing = tmp_path / 'missing.db'
    assert worker.main(['--database', str(missing), '--once']) == 1
    assert not missing.exists()


@pytest.mark.parametrize('result', ['sent', 'empty', 'pending', 'blocked', 'lease_lost'])
def test_dispatch_wakes_only_after_new_success(result):
    calls = []
    assert worker.run(None, URL, TOKEN, once=True, send=lambda *_: result,
                      on_sent=lambda: calls.append('wake')) == result
    assert calls == (['wake'] if result == 'sent' else [])


def test_wake_failure_keeps_durable_ack(caplog):
    def fail():
        raise RuntimeError('private auth must not be logged')
    assert worker.run(None, URL, TOKEN, once=True, send=lambda *_: 'sent', on_sent=fail) == 'sent'
    assert 'timer remains the retry safety net' in caplog.text
    assert 'private auth' not in caplog.text


def test_helper_runs_without_shell_or_event_content(tmp_path):
    helper = tmp_path / 'dispatch.py'
    marker = tmp_path / 'called'
    helper.write_text(f"from pathlib import Path; Path({str(marker)!r}).write_text('called')")
    worker.dispatch_wake(helper)
    assert marker.read_text() == 'called'


def test_bad_configuration_fails_closed(tmp_path, monkeypatch):
    db = tmp_path / 'state.db'
    db.touch()
    monkeypatch.setenv('PIPER_COMPLETION_URL', 'http://remote.example/endpoint')
    monkeypatch.setenv('PIPER_COMPLETION_TOKEN', TOKEN)
    assert worker.main(['--database', str(db), '--once']) == 1
