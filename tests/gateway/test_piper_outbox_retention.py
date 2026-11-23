import sqlite3
import pytest
from gateway.piper_outbox_retention import MIN_RETENTION_SECONDS, prune_sent
from gateway.piper_turn_outbox import insert_completion
from gateway.piper_outbox_sender import send_one


def test_only_old_acknowledged_rows_are_pruned(tmp_path):
    conn = sqlite3.connect(tmp_path / 'state.db', isolation_level=None)
    for name in ('sent', 'pending', 'blocked', 'legacy'):
        insert_completion(conn, name, 'web-1234567890123456')
    conn.execute("UPDATE piper_turn_outbox SET state='sent',terminal_at=10 WHERE admission_id='sent'")
    conn.execute("UPDATE piper_turn_outbox SET state='blocked',terminal_at=10 WHERE admission_id='blocked'")
    conn.execute("UPDATE piper_turn_outbox SET state='sent' WHERE admission_id='legacy'")
    assert prune_sent(conn, now=MIN_RETENTION_SECONDS + 9) == 0
    assert prune_sent(conn, now=MIN_RETENTION_SECONDS + 10) == 1
    assert {r[0] for r in conn.execute('SELECT admission_id FROM piper_turn_outbox')} == {'pending', 'blocked', 'legacy'}
    with pytest.raises(ValueError):
        prune_sent(conn, now=99999999, retention_seconds=1)
    conn.close()


def test_old_schema_upgrades_and_delivery_records_terminal_time():
    conn = sqlite3.connect(':memory:', isolation_level=None)
    conn.execute('''CREATE TABLE piper_turn_outbox (admission_id TEXT PRIMARY KEY, turn_id TEXT,
        event_type TEXT, created_at INTEGER DEFAULT 0, state TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
        next_attempt INTEGER DEFAULT 0, lease_token TEXT, lease_until INTEGER)''')
    conn.execute("INSERT INTO piper_turn_outbox(admission_id,turn_id,event_type) VALUES ('id','web-1234567890123456','piper.turn.completed.v1')")
    assert send_one(conn, 'https://example.test/', 'x'*32, now=123, transport=lambda *_: 204) == 'sent'
    assert conn.execute('SELECT state,terminal_at FROM piper_turn_outbox').fetchone() == ('sent', 123)
