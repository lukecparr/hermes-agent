"""Bounded final-answer-only push preview; never inspect messages, tools or reasoning."""
from html import unescape
import re
import unicodedata
from agent.redact import redact_sensitive_text

LIMIT = 240
_INTERNAL = re.compile(r'<\s*/?\s*(?:think(?:ing)?|analysis|reasoning|tool(?:_call|_result)?|function_call)\b|(?:^|\n)\s*(?:assistant\s+to=|(?:analysis|tool|function)\s*:)', re.IGNORECASE)


def sanitize_preview(value):
    if not isinstance(value, str):
        return None
    text = unescape(value[:8192])
    if _INTERNAL.search(text):
        return None
    text = redact_sensitive_text(text, force=True, redact_url_credentials=True)
    text = re.sub(r'(?is)<(script|style)\b[^>]*>.*?(?:</\1\s*>|$)', '', text)
    text = re.sub(r'(?ms)^\s*(?:```|~~~).*?(?:^\s*(?:```|~~~)[^\n]*$|\Z)', '', text)
    text = re.sub(r'(?im)^.*\b(?:MEDIA|ATTACHMENT|ATTACH):[^\n]*', '', text)
    text = re.sub(r'!\[[^\]]*\]\([^)]*\)', '', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]*\)', r'\1', text)
    text = re.sub(r'<[^>]*>', '', text)
    text = re.sub(r'https?://\S+|(?:^|\s)/(?:home|tmp|opt|var|Users)/\S+', ' ', text)
    text = re.sub(r'[`*_#>~]', '', text)
    text = ''.join(' ' if c.isspace() else c for c in text if c.isspace() or not unicodedata.category(c).startswith('C'))
    text = ' '.join(text.split())
    return (text[:LIMIT - 1].rstrip() + '…' if len(text) > LIMIT else text) or None


def store_preview(conn, admission_id, preview):
    # Nullable additive upgrade; old producers and already queued IDs remain valid.
    columns = {r[1] for r in conn.execute('PRAGMA table_info(piper_turn_outbox)')}
    if 'preview' not in columns:
        conn.execute('ALTER TABLE piper_turn_outbox ADD COLUMN preview TEXT')
    conn.execute('UPDATE piper_turn_outbox SET preview=? WHERE admission_id=? AND preview IS NULL',
                 (preview, admission_id))
