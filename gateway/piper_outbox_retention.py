"""Manual bounded retention for successfully acknowledged outbox rows only.

Receiver event tombstones must remain durable indefinitely, including across restores.
This module is not scheduled or invoked by the worker.
"""
MIN_RETENTION_SECONDS = 90 * 86400


def prune_sent(conn, *, now, retention_seconds=MIN_RETENTION_SECONDS, limit=100):
    if retention_seconds < MIN_RETENTION_SECONDS or not 1 <= limit <= 100:
        raise ValueError('unsafe retention parameters')
    conn.execute('BEGIN IMMEDIATE')
    try:
        rows = conn.execute('''SELECT admission_id FROM piper_turn_outbox
            WHERE state='sent' AND terminal_at IS NOT NULL AND terminal_at<=?
            ORDER BY terminal_at,admission_id LIMIT ?''', (int(now) - retention_seconds, limit)).fetchall()
        for (admission_id,) in rows:
            conn.execute("DELETE FROM piper_turn_outbox WHERE admission_id=? AND state='sent' AND terminal_at IS NOT NULL AND terminal_at<=?",
                         (admission_id, int(now) - retention_seconds))
        conn.commit()
        return len(rows)
    except BaseException:
        conn.rollback()
        raise
