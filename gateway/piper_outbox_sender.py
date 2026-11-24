"""Explicitly invoked, opt-in sender for the experimental Piper completion outbox.

No scheduler or production configuration is installed by this module. The receiver
must durably deduplicate event_id before acknowledging; delivery is at-least-once.
"""
import json
import secrets
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request

from gateway.piper_turn_outbox import ensure_terminal_column


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_config(url, token):
    parsed = urllib.parse.urlsplit(url)
    local = parsed.hostname in ('127.0.0.1', 'localhost', '::1')
    if (parsed.scheme != 'https' and not (parsed.scheme == 'http' and local)) or not parsed.hostname:
        raise ValueError('invalid endpoint')
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError('invalid endpoint')
    if not isinstance(token, str) or not 32 <= len(token) <= 512 or any(ord(c) < 33 or ord(c) > 126 for c in token):
        raise ValueError('invalid credential')


def claim(conn, now, lease_seconds=120):
    """Lease one due row; a dead process's lease becomes eligible after expiry."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        # The producer creates the table only with the first eligible completion. A
        # sender started first must wait rather than exit or create gateway schema.
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='piper_turn_outbox'").fetchone() is None:
            conn.commit()
            return None
        ensure_terminal_column(conn)
        row = conn.execute('''SELECT admission_id,turn_id,event_type,attempts FROM piper_turn_outbox
            WHERE (state='pending' AND next_attempt<=?) OR (state='leased' AND lease_until<=?)
            ORDER BY created_at,admission_id LIMIT 1''', (now, now)).fetchone()
        if row is None:
            conn.commit()
            return None
        lease = secrets.token_hex(16)
        conn.execute("UPDATE piper_turn_outbox SET state='leased',lease_token=?,lease_until=?,attempts=attempts+1 WHERE admission_id=?",
                     (lease, now + lease_seconds, row[0]))
        conn.commit()
        return {'event_id': row[0], 'turn_id': row[1], 'type': row[2], 'attempt': row[3] + 1, 'lease': lease}
    except BaseException:
        conn.rollback()
        raise


def post_event(url, token, event):
    payload = json.dumps({'event_id': event['event_id'], 'turn_id': event['turn_id'], 'type': event['type']},
                         separators=(',', ':')).encode('utf-8')
    req = urllib.request.Request(url, data=payload, method='POST',
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.build_opener(_NoRedirect).open(req, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except (urllib.error.URLError, TimeoutError, OSError):
        return None


def send_one(conn, url, token, *, now=None, transport=post_event):
    """Attempt one event. An ambiguous network result retries the same event ID."""
    validate_config(url, token)
    simulated_time = now is not None
    now = int(time.time()) if now is None else int(now)
    event = claim(conn, now)
    if event is None:
        return 'empty'
    status = transport(url, token, event)
    if isinstance(status, int) and 200 <= status < 300:
        state, next_attempt = 'sent', 0
    elif isinstance(status, int) and 400 <= status < 500 and status not in (408, 429):
        state, next_attempt = 'blocked', 0
    else:
        state = 'pending'
        next_attempt = now + min(3600, 2 ** min(event['attempt'], 11))
    conn.execute('BEGIN IMMEDIATE')
    try:
        changed = conn.execute('''UPDATE piper_turn_outbox SET state=?,next_attempt=?,lease_token=NULL,lease_until=NULL,terminal_at=?
            WHERE admission_id=? AND state='leased' AND lease_token=?''',
            (state, next_attempt, (now if simulated_time else int(time.time())) if state in ('sent', 'blocked') else None, event['event_id'], event['lease'])).rowcount
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return state if changed else 'lease_lost'
