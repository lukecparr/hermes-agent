"""Private crash evidence for shared-thread native media capture.

The capability remains disabled. This journal records intent before any native
file is published; recovery currently REPORTS records and never deletes files.
It is not an ownership protocol for other producers of the shared native cache.
"""
import json
import os
from pathlib import Path
import secrets
import stat

from hermes_constants import get_hermes_home
from hermes_dir_acl import make_user_dir
from utils import fsync_directory


class CaptureIntent:
    def __init__(self):
        self.root = get_hermes_home() / 'piper-shared-thread' / 'capture-intents'
        self.fd = None
        self.path = None

    def acquire(self):
        # Deliberately fail closed on platforms without flock.
        import fcntl
        make_user_dir(self.root, mode=0o700, parents=True, exist_ok=True)
        if self.root.resolve() != self.root:
            raise OSError('unsafe intent directory')
        info = self.root.stat(follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise OSError('unsafe intent directory')
        fd = os.open(self.root / '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise OSError('unsafe intent lock')
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        self.fd = fd
        return self

    def record(self, references, staged):
        if self.fd is None or self.path is not None:
            raise RuntimeError('intent lock not held or already recorded')
        self.path = self.root / (secrets.token_hex(24) + '.json')
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as output:
            json.dump({'version': 1, 'references': references, 'temporary': [str(p) for p in staged]}, output)
            output.flush()
            os.fsync(output.fileno())
        fsync_directory(self.root)

    def release(self):
        if self.fd is not None:
            import fcntl
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


def inspect_intents():
    """Read journals under the exclusive cross-process lock; never mutate media."""
    intent = CaptureIntent().acquire()
    try:
        results = []
        for path in intent.root.glob('*.json'):
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, encoding='utf-8') as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise OSError('unsafe intent record')
                if info.st_size > 1024 * 1024:
                    raise OSError('oversized intent record')
                results.append(json.load(source))
        return results
    finally:
        intent.release()
