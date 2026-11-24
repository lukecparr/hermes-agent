#!/usr/bin/env python3
"""Manually prune at most 100 acknowledged Piper outbox rows older than 90 days.

Keep the receiver's event tombstones, including when restoring backups.
Never deletes pending, leased or blocked rows. No database is created.
"""
import argparse
from pathlib import Path
import sqlite3
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from gateway.piper_outbox_retention import prune_sent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--confirm', action='store_true')
    args = parser.parse_args(argv)
    if not args.confirm:
        parser.error('explicit --confirm required')
    try:
        path = Path(args.database).expanduser().resolve(strict=True)
        with sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=10) as conn:
            count = prune_sent(conn, now=int(time.time()))
        print(f'Pruned {count} acknowledged rows; receiver tombstones must be retained.')
    except (OSError, sqlite3.Error, ValueError):
        print('Unable to prune the existing outbox database.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
