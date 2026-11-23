#!/usr/bin/env python3
"""Explicitly run the experimental Piper outbox sender; never starts automatically.

Provide PIPER_COMPLETION_URL and PIPER_COMPLETION_TOKEN via a protected environment.
No payload, URL, credential, or turn ID is written to the log.
"""
import argparse
import logging
import os
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from gateway.piper_outbox_sender import send_one, validate_config

LOG = logging.getLogger("piper-completion-worker")


def run(conn, url, token, *, once=False, interval=10, send=send_one, sleep=time.sleep):
    """Drain due rows; sleep between empty or failed passes. Leases fence other workers."""
    validate_config(url, token)
    while True:
        result = send(conn, url, token)
        if result == "blocked":
            LOG.error("Completion delivery blocked by receiver; operator action required")
        if once:
            return result
        if result != "sent":
            sleep(interval)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="existing gateway SQLite database path")
    parser.add_argument("--once", action="store_true", help="attempt at most one row and exit")
    parser.add_argument("--interval", type=int, default=10, help="seconds between idle/retry checks (1-300)")
    args = parser.parse_args(argv)
    if not 1 <= args.interval <= 300:
        parser.error("interval must be 1-300 seconds")
    url = os.environ.get("PIPER_COMPLETION_URL", "")
    token = os.environ.get("PIPER_COMPLETION_TOKEN", "")
    try:
        validate_config(url, token)
        path = Path(args.database).expanduser().resolve(strict=True)
        with sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=10) as conn:
            run(conn, url, token, once=args.once, interval=args.interval)
    except (ValueError, OSError, sqlite3.Error):
        LOG.error("Completion worker could not start or access its outbox")
        return 1
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
