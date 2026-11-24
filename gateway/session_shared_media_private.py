"""Experimental isolated shared-thread media storage.

A capture and the recovery scanner must hold the same OS lock until admission
commits. Other gateway producers never publish into this private namespace.
The shared-thread capability remains default-off.
"""
from contextlib import contextmanager
import hashlib

import os
from pathlib import Path
import re
import secrets
import stat
import threading


from hermes_constants import get_hermes_home
from hermes_dir_acl import make_user_dir
from utils import fsync_directory

_OWNER = re.compile(r'[0-9a-f]{48}')
_held = threading.local()


def private_root():
    return get_hermes_home() / 'piper-shared-thread' / 'native-media-private'


def is_private_path(path):
    """Check the complete on-disk private path shape, without following links."""
    path = Path(path)
    return (path.is_absolute() and path.parent.parent.parent == private_root()
            and _OWNER.fullmatch(path.parent.parent.name) is not None
            and re.fullmatch(r'[0-9a-f]{64}', path.parent.name) is not None
            and path.resolve() == path)


@contextmanager
def private_store():
    import fcntl
    root = private_root()
    make_user_dir(root, mode=0o700, parents=True, exist_ok=True)
    info = root.stat(follow_symlinks=False)
    if root.resolve() != root or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise OSError('unsafe private media root')
    fd = os.open(root / '.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise OSError('unsafe private media lock')
        fcntl.flock(fd, fcntl.LOCK_EX)
        _held.root = root
        try:
            yield root
        finally:
            _held.root = None
    finally:
        os.close(fd)


def capture_private(root, name, data):
    """Capture bytes under a fresh exclusive owner; caller holds private_store."""
    if getattr(_held, 'root', None) != root:
        raise RuntimeError('private store lock required')
    if not isinstance(data, bytes) or not data or len(data) > 5 * 1024 * 1024:
        raise ValueError('invalid media size')
    if not isinstance(name, str) or name in ('', '.', '..') or '/' in name or '\\' in name:
        raise ValueError('invalid media name')
    owner = secrets.token_hex(24)
    directory = root / owner
    directory.mkdir(mode=0o700)
    fsync_directory(root)
    digest = hashlib.sha256(data).hexdigest()
    target_dir = directory / digest
    target_dir.mkdir(mode=0o700)
    fsync_directory(directory)
    target = target_dir / name
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as out:
        out.write(data)
        out.flush()
        os.fsync(out.fileno())
    fsync_directory(target_dir)
    return {'path': str(target), 'sha256': digest, 'size': len(data)}


def capture_private_batch(root, paths):
    """Read bounded staging files and capture them while the private lock is held."""
    from gateway.session_ingress_media import _open_regular
    from gateway.platforms.base import get_inbound_media_max_bytes
    from hermes_state_runtime import RuntimeStoreError
    staging = get_hermes_home().resolve() / 'piper-shared-thread' / 'attachments'
    limit = min(5 * 1024 * 1024, max(0, get_inbound_media_max_bytes()))
    total = 0
    references = []
    try:
        for value in paths:
            path = Path(value)
            if (not path.is_absolute() or staging.resolve() != staging or path.parent.parent != staging
                    or path.resolve() != path):
                raise ValueError('invalid staging path')
            with _open_regular(path) as source:
                if os.fstat(source.fileno()).st_nlink != 1:
                    raise ValueError('linked staging file')
                data = source.read(limit - total + 1)
            total += len(data)
            if not data or total > limit:
                raise ValueError('invalid media size')
            references.append(capture_private(root, path.name, data))
    except (OSError, ValueError) as exc:
        raise RuntimeStoreError('invalid_params') from exc
    return references


def inspect_private_owners(root):
    """List safe candidates without deleting. Invalid or uncertain entries fail closed."""
    if getattr(_held, 'root', None) != root:
        raise RuntimeError('private store lock required')
    owners = []
    for directory in root.iterdir():
        if directory.name == '.lock':
            continue
        info = directory.stat(follow_symlinks=False)
        if not _OWNER.fullmatch(directory.name) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise OSError('unsafe private owner')
        files, digests = [], []
        for digest_dir in directory.iterdir():
            meta = digest_dir.stat(follow_symlinks=False)
            if not re.fullmatch(r'[0-9a-f]{64}', digest_dir.name) or not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.getuid() or meta.st_mode & 0o077:
                raise OSError('unsafe private digest directory')
            digests.append(digest_dir.name)
            for path in digest_dir.iterdir():
                meta = path.stat(follow_symlinks=False)
                if not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.getuid() or meta.st_mode & 0o077 or meta.st_nlink != 1:
                    raise OSError('unsafe private media')
                files.append({'path': str(path), 'sha256': digest_dir.name, 'size': meta.st_size})
        owners.append({'owner': directory.name, 'files': files, 'digests': digests, 'mtime': info.st_mtime})
    return owners


def recover_private(root, db, *, older_than):
    """Experimental collector; caller holds private_store through this call.

    Retains every owner named by a live admission or any transcript, and only
    collects old owners. SQLite's write transaction serializes its holder scan
    and deletion with DB writers. This is not yet wired into production.
    """
    if root != private_root() or Path(db.db_path).resolve() != (get_hermes_home() / 'state.db').resolve():
        raise ValueError('foreign profile media or database')
    owners = [o for o in inspect_private_owners(root) if o['mtime'] < older_than]
    if not owners:
        return 0

    def collect(conn):
        admissions = [str(row[0]) for row in conn.execute(
            "SELECT payload_json FROM session_admissions WHERE status != 'terminal'")]
        history = [str(row[0]) for row in conn.execute('SELECT content FROM messages')]
        retained = admissions + history
        count = 0
        for owner in owners:
            # The random owner is present in every canonical path. Matching it
            # conservatively across JSON and transcript text protects the
            # whole batch, including a killed partially written sibling.
            if any(owner['owner'] in text for text in retained):
                continue
            directory = root / owner['owner']
            # Revalidate the entire namespace before any deletion: malformed
            # entries, links or unexpected files stop this sweep entirely.
            current = next((o for o in inspect_private_owners(root) if o['owner'] == owner['owner']), None)
            if current is None or current['mtime'] >= older_than:
                continue
            for item in current['files']:
                Path(item['path']).unlink()
            for digest in current['digests']:
                (directory / digest).rmdir()
            directory.rmdir()
            count += 1
        if count:
            fsync_directory(root)
        return count
    return db._execute_write(collect)


def sweep_private(db):
    """Sweep an existing namespace without creating one for unrelated profiles."""
    import time
    from agent.provider_media import MEDIA_CACHE_MAX_AGE_HOURS
    if not private_root().exists():
        return 0
    with private_store() as root:
        return recover_private(root, db, older_than=time.time() - MEDIA_CACHE_MAX_AGE_HOURS * 3600)
