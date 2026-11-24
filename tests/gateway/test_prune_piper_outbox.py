import sqlite3
import time

import pytest

from gateway.piper_turn_outbox import insert_completion
from scripts.prune_piper_outbox import main


def test_manual_prune_only_old_sent_rows(tmp_path, capsys):
    path = tmp_path / 'state.db'
    with sqlite3.connect(path) as conn:
        for key in ('old', 'recent', 'pending'):
            insert_completion(conn, key, 'web-' + 'x' * 16)
        now = int(time.time())
        conn.execute("UPDATE piper_turn_outbox SET state='sent', terminal_at=? WHERE admission_id='old'", (now-91*86400,))
        conn.execute("UPDATE piper_turn_outbox SET state='sent', terminal_at=? WHERE admission_id='recent'", (now,))
    assert main(['--database', str(path), '--confirm']) == 0
    assert 'Pruned 1 acknowledged rows' in capsys.readouterr().out
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT admission_id FROM piper_turn_outbox ORDER BY admission_id').fetchall() == [('pending',), ('recent',)]


def test_requires_confirmation_and_never_creates_database(tmp_path):
    path = tmp_path / 'missing.db'
    with pytest.raises(SystemExit):
        main(['--database', str(path)])
    assert main(['--database', str(path), '--confirm']) == 1
    assert not path.exists()
