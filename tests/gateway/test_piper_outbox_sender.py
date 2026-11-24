"""Outbox leases, authenticated HTTP delivery and replay after lost acknowledgements."""
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from gateway.piper_outbox_sender import claim, send_one, validate_config
from gateway.piper_turn_outbox import insert_completion

TOKEN = 't' * 32


@pytest.fixture
def db(tmp_path):
    conn = sqlite3.connect(tmp_path / 'sender.db', isolation_level=None)
    insert_completion(conn, 'admission-id', 'web-1234567890123456')
    yield conn
    conn.close()


def test_sender_started_before_first_completion_waits(tmp_path):
    path = tmp_path / 'state.db'
    with sqlite3.connect(path, isolation_level=None) as conn:
        assert send_one(conn, 'https://example.test/piper', TOKEN, now=1) == 'empty'
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='piper_turn_outbox'").fetchone() is None
        insert_completion(conn, 'a' * 32, 'web-1234567890123456')
        assert send_one(conn, 'https://example.test/piper', TOKEN, now=2, transport=lambda *_: 204) == 'sent'


def test_existing_malformed_outbox_fails_closed(tmp_path):
    with sqlite3.connect(tmp_path / 'state.db', isolation_level=None) as conn:
        conn.execute('CREATE TABLE piper_turn_outbox (id TEXT)')
        with pytest.raises(sqlite3.OperationalError):
            send_one(conn, 'https://example.test/piper', TOKEN, now=1)


def test_retry_and_receiver_dedup(db):
    seen, visible = set(), []
    def receiver(_url, _token, event):
        if event['event_id'] not in seen:
            seen.add(event['event_id'])
            visible.append(event['event_id'])
        return None if event['attempt'] == 1 else 200  # first acknowledgement lost
    assert send_one(db, 'https://example.test/piper', TOKEN, now=10, transport=receiver) == 'pending'
    assert send_one(db, 'https://example.test/piper', TOKEN, now=11, transport=receiver) == 'empty'
    assert send_one(db, 'https://example.test/piper', TOKEN, now=12, transport=receiver) == 'sent'
    assert send_one(db, 'https://example.test/piper', TOKEN, now=100, transport=receiver) == 'empty'
    assert visible == ['admission-id']  # mock receiver only; no FCM display tested


def test_expired_lease_redelivers_same_id(db):
    event = claim(db, 10, lease_seconds=10)
    assert claim(db, 19) is None
    reclaimed = claim(db, 20)
    assert reclaimed['event_id'] == event['event_id']
    assert reclaimed['lease'] != event['lease']


def test_expired_lease_recovers_after_connection_restart(tmp_path):
    db_path = tmp_path / 'recover.db'
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        insert_completion(conn, 'admission-restart', 'web-1234567890123456')
        old = claim(conn, 100, lease_seconds=10)
        assert old['event_id'] == 'admission-restart'
    with sqlite3.connect(db_path, isolation_level=None) as conn:
        assert send_one(conn, 'https://example.test/piper', TOKEN, now=109, transport=lambda *_: 204) == 'empty'
        assert send_one(conn, 'https://example.test/piper', TOKEN, now=110, transport=lambda *_: 204) == 'sent'
        assert conn.execute('SELECT state, attempts FROM piper_turn_outbox').fetchone() == ('sent', 2)


def test_permanent_failure_blocks_and_transient_retries(db):
    assert send_one(db, 'https://example.test/piper', TOKEN, now=1, transport=lambda *_: 401) == 'blocked'
    assert send_one(db, 'https://example.test/piper', TOKEN, now=4000, transport=lambda *_: 200) == 'empty'


@pytest.mark.parametrize('url,token', [('http://example.test/push', TOKEN),
    ('https://user:pass@example.test/push', TOKEN), ('https://example.test/push?secret=x', TOKEN),
    ('https://example.test/push', 'short')])
def test_invalid_config_fails_before_claim(db, url, token):
    with pytest.raises(ValueError):
        send_one(db, url, token, now=1)
    assert db.execute('SELECT state FROM piper_turn_outbox').fetchone()[0] == 'pending'


def test_real_http_auth_and_minimal_body(db):
    received = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append((self.headers.get('Authorization'), json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
            self.send_response(204)
            self.end_headers()
        def log_message(self, *_args):
            pass
    server = HTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    try:
        assert send_one(db, f'http://127.0.0.1:{server.server_port}/piper', TOKEN, now=1) == 'sent'
    finally:
        thread.join(timeout=5)
        server.server_close()
    assert received == [('Bearer ' + TOKEN, {'event_id': 'admission-id', 'turn_id': 'web-1234567890123456',
                                              'type': 'piper.turn.completed.v1'})]
