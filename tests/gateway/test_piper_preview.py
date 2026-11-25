import importlib.util
from pathlib import Path
import sqlite3

import pytest


@pytest.fixture
def preview():
    path = Path(__file__).resolve().parents[2] / 'contrib/piper-turn-outbox/preview.py'
    spec = importlib.util.spec_from_file_location('preview_fixture', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('text,expected', [
    ('**Hello**\n<b>Luke</b> &amp; hi', 'Hello Luke & hi'),
    ('<script>private</script>Done', 'Done'),
    ('<analysis>private</analysis>Done', None),
    ('&lt;tool_result&gt;private&lt;/tool_result&gt;', None),
    ('assistant to=functions.terminal private', None),
    ('Done\nMEDIA:/home/private/file', 'Done'),
    ('Hello\n```\nprivate code\n```\nDone', 'Hello Done'),
    ('[Read this](https://example.test/token) ![image](private)', 'Read this'),
    ('safe\u202eevil\x00', 'safeevil'),
    ('', None),
])
def test_plain_preview(preview, text, expected):
    assert preview.sanitize_preview(text) == expected


def test_bound_and_no_tool_history(preview):
    assert len(preview.sanitize_preview('😀' * 300)) == 240
    with sqlite3.connect(':memory:') as conn:
        conn.execute('CREATE TABLE piper_turn_outbox (admission_id TEXT PRIMARY KEY)')
        conn.execute("INSERT INTO piper_turn_outbox VALUES ('event')")
        preview.store_preview(conn, 'event', 'Hello')
        preview.store_preview(conn, 'event', 'replacement')
        assert conn.execute('SELECT preview FROM piper_turn_outbox').fetchone()[0] == 'Hello'


def test_sender_preview_gate_and_legacy_schema(preview):
    from gateway.piper_turn_outbox import insert_completion
    from gateway.piper_outbox_sender import send_one
    for enabled in (False, True):
        with sqlite3.connect(':memory:') as conn:
            insert_completion(conn, 'a' * 32, 'web-1234567890123456')
            preview.store_preview(conn, 'a' * 32, 'Hello')
            conn.commit()
            events = []
            def transport(url, token, event):
                events.append(event)
                return 204
            assert send_one(conn, 'http://127.0.0.1/completion', 'x' * 32,
                            include_preview=enabled, transport=transport) == 'sent'
            assert events[0].get('preview') == ('Hello' if enabled else None)
            assert send_one(conn, 'http://127.0.0.1/completion', 'x' * 32,
                            include_preview=enabled, transport=transport) == 'empty'
