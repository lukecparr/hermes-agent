"""Opt-in durable completions for the configured shared conversation.

This is a local outbox only: it does not send notifications or expose a network API.
The owner inserts inside the same transaction as its ordinary terminal settlement.
"""
import json
import re

from gateway.response_filters import is_intentional_silence_response

# Match the Second Brain web admission and completion-receiver contract exactly.
# The plugin accepts a broader ID for other surfaces; those IDs cannot be sent
# to this receiver without permanently blocking the outbox on a 400 response.
_ID = re.compile(r'^web-[A-Za-z0-9_-]{16,60}$')


def eligible_turn(row, route, response, outcome, *, configured=None):
    """Return a receiver-compatible turn ID for eligible shared-conversation replies.

    Non-web turns use an opaque ID derived from the unique authority admission,
    so their transport identity is not disclosed and replay cannot create a second event.
    """
    if not configured or route != configured:
        return None
    if outcome != 'completed' or not isinstance(response, str) or not response.strip():
        return None
    if is_intentional_silence_response(response):
        return None
    principal = row.get('principal_id')
    if principal == 'automation:' + route:
        try:
            identity = json.loads(row.get('request_id', ''))
        except (TypeError, ValueError):
            return None
        if not isinstance(identity, list) or len(identity) != 3 or identity[0] != 'piper-shared-thread':
            return None
        surface, turn = identity[1:]
        if surface == 'second-brain-web':
            return turn if isinstance(turn, str) and _ID.fullmatch(turn) else None
        if surface != 'charm' or not isinstance(turn, str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,64}', turn):
            return None
    elif isinstance(principal, str) and principal.startswith('messaging:'):
        try:
            source = json.loads(principal[len('messaging:'):])
        except (TypeError, ValueError):
            return None
        if not isinstance(source, list) or len(source) != 5 or source[1] != 'telegram':
            return None
    else:
        return None
    admission = row.get('admission_id')
    return 'web-' + admission if isinstance(admission, str) and re.fullmatch(r'[a-f0-9]{32}', admission) else None


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
