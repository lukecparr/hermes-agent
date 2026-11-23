"""Opt-in durable completion records for the existing Piper shared-thread web producer.

This is a local outbox only: it does not send notifications or expose a network API.
The owner inserts inside the same transaction as its ordinary terminal settlement.
"""
import json
import os
import re

from gateway.response_filters import is_intentional_silence_response

_ID = re.compile(r'^[A-Za-z0-9_-]{8,64}$')


def eligible_turn(row, route, response, outcome):
    """Return the web turn id only for a completed, non-silent shared-thread reply."""
    configured = os.environ.get('PIPER_TURN_OUTBOX_ROUTE')
    if not configured or route != configured or row.get('principal_id') != 'automation:' + route:
        return None
    if outcome != 'completed' or not isinstance(response, str) or not response.strip():
        return None
    if is_intentional_silence_response(response):
        return None
    try:
        identity = json.loads(row.get('request_id', ''))
    except (TypeError, ValueError):
        return None
    if (not isinstance(identity, list) or len(identity) != 3
            or identity[:2] != ['piper-shared-thread', 'second-brain-web']
            or not isinstance(identity[2], str) or not _ID.fullmatch(identity[2])):
        return None
    return identity[2]


def ensure_terminal_column(conn):
    """Upgrade an existing experimental outbox; legacy rows remain NULL and retained."""
    columns = {row[1] for row in conn.execute('PRAGMA table_info(piper_turn_outbox)')}
    if columns and 'terminal_at' not in columns:
        conn.execute('ALTER TABLE piper_turn_outbox ADD COLUMN terminal_at INTEGER')


def insert_completion(conn, admission_id, turn_id):
    """Idempotent within the caller's transaction; stores no conversation content."""
    conn.execute('''CREATE TABLE IF NOT EXISTS piper_turn_outbox (
        admission_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL,
        event_type TEXT NOT NULL CHECK (event_type = 'piper.turn.completed.v1'),
        created_at INTEGER NOT NULL DEFAULT (unixepoch()),
        state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'leased', 'sent', 'blocked')),
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0,
        lease_token TEXT, lease_until INTEGER, terminal_at INTEGER)''')
    ensure_terminal_column(conn)
    conn.execute('''INSERT INTO piper_turn_outbox(admission_id, turn_id, event_type)
        VALUES (?, ?, 'piper.turn.completed.v1') ON CONFLICT(admission_id) DO NOTHING''',
        (admission_id, turn_id))
