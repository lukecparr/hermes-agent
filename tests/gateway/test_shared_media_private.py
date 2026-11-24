"""Isolation and process-death probe for the experimental private store."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest


def test_private_owner_survives_process_death_without_shared_cache_alias(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, inspect_private_owners, recover_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    shared = tmp_path / 'cache' / 'documents' / 'native-inputs' / ('a' * 64)
    shared.mkdir(parents=True)
    other = shared / 'same.txt'
    other.write_bytes(b'other producer')
    code = ("import os,signal; from gateway.session_shared_media_private import private_store,capture_private; "
            "ctx=private_store(); root=ctx.__enter__(); capture_private(root,'same.txt',b'private file'); "
            "os.kill(os.getpid(),signal.SIGKILL)")
    proc = subprocess.run([sys.executable, '-c', code], env=dict(os.environ), check=False)
    assert proc.returncode == -signal.SIGKILL
    with private_store() as root:
        owners = inspect_private_owners(root)
        assert len(owners) == 1 and len(owners[0]['files']) == 1
        private = Path(owners[0]['files'][0]['path'])
        assert private.read_bytes() == b'private file' and other.read_bytes() == b'other producer'
        assert private != other
        db = SessionDB(db_path=tmp_path / 'state.db')
        assert recover_private(root, db, older_than=time.time() + 10) == 1
        assert not private.exists() and other.read_bytes() == b'other producer'


def test_private_lock_rejects_hardlinks(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, private_root
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    with private_store():
        pass
    lock = private_root() / '.lock'
    alias = tmp_path / 'lock-alias'
    os.link(lock, alias)
    with pytest.raises(OSError, match='unsafe private media lock'):
        with private_store():
            pass
    assert alias.exists() and lock.exists()


def test_private_store_refuses_symlink_and_inspection_never_unlinks(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private, inspect_private_owners
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    with private_store() as root:
        ref = capture_private(root, 'file.txt', b'held')
        external = tmp_path / 'other'
        external.write_bytes(b'protected')
        owner = Path(ref['path']).parent.parent
        (owner / ('f' * 64)).symlink_to(external)
        with pytest.raises(OSError):
            inspect_private_owners(root)
        assert Path(ref['path']).read_bytes() == b'held' and external.read_bytes() == b'protected'


def test_native_restore_validates_private_namespace(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private
    from gateway.session_ingress_media import restore_native_media
    from hermes_state_runtime import RuntimeStoreError
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    with private_store() as root:
        ref = capture_private(root, 'file.txt', b'proof')
        assert restore_native_media([ref]) == [ref['path']]
        Path(ref['path']).write_bytes(b'wrong')
        with pytest.raises(RuntimeStoreError, match='storage_unavailable'):
            restore_native_media([ref])


def test_recovery_protects_queued_history_and_foreign_shared_files(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private, recover_private
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, admit_session_input
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('session', source='test')
    epoch = begin_runtime_epoch(db, instance_id='fixture')
    shared = tmp_path / 'cache' / 'documents' / 'native-inputs' / ('a' * 64)
    shared.mkdir(parents=True)
    other = shared / 'same.txt'
    other.write_bytes(b'other')
    with private_store() as root:
        queued = capture_private(root, 'queued.txt', b'queued')
        historical = capture_private(root, 'history.txt', b'history')
        orphan = capture_private(root, 'orphan.txt', b'orphan')
        admit_session_input(db, epoch=epoch, principal_id='test', session_id='session', request_id='request',
                            payload={'text': 'hello', 'native_text_v1': {'media': [queued]}})
        db.append_message('session', 'user', 'read this ' + historical['path'])
        assert recover_private(root, db, older_than=time.time() + 10) == 1
        assert Path(queued['path']).read_bytes() == b'queued'
        assert Path(historical['path']).read_bytes() == b'history'
        assert not Path(orphan['path']).exists()
        assert other.read_bytes() == b'other'


def test_recovery_refuses_foreign_database(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private, recover_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path / 'profile'))
    db = SessionDB(db_path=tmp_path / 'state.db')
    with private_store() as root:
        ref = capture_private(root, 'keep.txt', b'keep')
        with pytest.raises(ValueError, match='foreign'):
            recover_private(root, db, older_than=time.time() + 10)
        assert Path(ref['path']).exists()


def test_recovery_refuses_different_database_in_same_profile(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private, recover_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    db = SessionDB(db_path=tmp_path / 'other.db')
    with private_store() as root:
        ref = capture_private(root, 'keep.txt', b'keep')
        with pytest.raises(ValueError, match='foreign'):
            recover_private(root, db, older_than=time.time() + 10)
        assert Path(ref['path']).read_bytes() == b'keep'


def test_private_holder_does_not_block_unrelated_native_cleanup(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private
    from gateway.session_ingress_media import capture_native_media, release_unheld_media
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, admit_session_input
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('session', source='test')
    epoch = begin_runtime_epoch(db, instance_id='fixture')
    with private_store() as root:
        held = capture_private(root, 'held.txt', b'held')
        admit_session_input(db, epoch=epoch, principal_id='test', session_id='session', request_id='request',
                            payload={'text': 'hello', 'native_text_v1': {'media': [held]}})
        staging = tmp_path / 'other.txt'
        staging.write_bytes(b'unrelated')
        other = capture_native_media([staging])
        assert release_unheld_media(db, other, retain_history=False) == 1
        assert Path(held['path']).read_bytes() == b'held'


def test_native_settlement_sweeps_expired_private_but_keeps_history(tmp_path, monkeypatch):
    import agent.provider_media as provider_media
    from gateway.session_shared_media_private import private_store, capture_private
    from gateway.session_ingress_media import release_admission_media
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, admit_session_input, cancel_session_input
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(provider_media, 'MEDIA_CACHE_MAX_AGE_HOURS', 0)
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('session', source='test')
    epoch = begin_runtime_epoch(db, instance_id='fixture')
    with private_store() as root:
        orphan = capture_private(root, 'orphan.txt', b'orphan')
        historical = capture_private(root, 'history.txt', b'history')
        orphan_row = admit_session_input(db, epoch=epoch, principal_id='test', session_id='session',
              request_id='one', payload={'text': 'one', 'native_text_v1': {'media': [orphan]}})
        held_row = admit_session_input(db, epoch=epoch, principal_id='test', session_id='session',
              request_id='two', payload={'text': 'two', 'native_text_v1': {'media': [historical]}})
        db.append_message('session', 'user', 'read this ' + historical['path'])
        cancel_session_input(db, epoch=epoch, admission_id=orphan_row['admission_id'])
        cancel_session_input(db, epoch=epoch, admission_id=held_row['admission_id'])
    assert release_admission_media(db, orphan_row['admission_id']) == 1
    assert not Path(orphan['path']).exists() and Path(historical['path']).read_bytes() == b'history'


def test_private_capture_lock_fences_other_process(tmp_path, monkeypatch):
    import fcntl
    from gateway.session_shared_media_private import private_root
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    code = ("import time; from gateway.session_shared_media_private import private_store,capture_private; "
            "ctx=private_store(); root=ctx.__enter__(); capture_private(root,'live.txt',b'live'); "
            "print('locked',flush=True); time.sleep(30)")
    proc = subprocess.Popen([sys.executable, '-c', code], env=dict(os.environ), stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == 'locked'
        fd = os.open(private_root() / '.lock', os.O_RDWR | os.O_NOFOLLOW)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(fd)
    finally:
        proc.kill()
        proc.wait(timeout=10)


def test_killed_partial_capture_is_cleaned_without_a_manifest(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, recover_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    code = ("import os,signal; from gateway.session_shared_media_private import private_store; "
            "ctx=private_store(); r=ctx.__enter__(); owner=r/('a'*48); owner.mkdir(mode=0o700); "
            "d=owner/('b'*64); d.mkdir(mode=0o700); p=d/'partial.txt'; "
            "fd=os.open(p,os.O_CREAT|os.O_WRONLY,0o600); os.write(fd,b'partial'); os.fsync(fd); "
            "os.kill(os.getpid(),signal.SIGKILL)")
    proc = subprocess.run([sys.executable, '-c', code], env=dict(os.environ), check=False)
    assert proc.returncode == -signal.SIGKILL
    db = SessionDB(db_path=tmp_path / 'state.db')
    with private_store() as root:
        assert recover_private(root, db, older_than=time.time() + 10) == 1
        assert not (root / ('a' * 48)).exists()


def test_killed_after_real_admission_keeps_committed_private_bytes(tmp_path, monkeypatch):
    """A process dying after SQLite commit must not turn a live input into an orphan."""
    from gateway.session_ingress_media import restore_native_media
    from gateway.session_shared_media_private import private_store, inspect_private_owners, recover_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('session', source='test')
    code = """
import os, signal
from pathlib import Path
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, admit_session_input
from gateway.session_shared_media_private import private_store, capture_private
db = SessionDB(db_path=Path(os.environ['HERMES_HOME']) / 'state.db')
epoch = begin_runtime_epoch(db, instance_id='crash-fixture')
with private_store() as root:
    ref = capture_private(root, 'committed.txt', b'committed bytes')
    admit_session_input(db, epoch=epoch, principal_id='test', session_id='session',
        request_id='request', payload={'text': 'hello', 'native_text_v1': {'media': [ref]}})
    os.kill(os.getpid(), signal.SIGKILL)
"""
    proc = subprocess.run([sys.executable, '-c', code], env=dict(os.environ), check=False)
    assert proc.returncode == -signal.SIGKILL
    with private_store() as root:
        owners = inspect_private_owners(root)
        assert len(owners) == 1
        ref = owners[0]['files'][0]
        assert recover_private(root, db, older_than=time.time() + 10) == 0
        assert restore_native_media([ref]) == [ref['path']]
        assert Path(ref['path']).read_bytes() == b'committed bytes'


def test_collector_waits_for_other_process_to_commit(tmp_path, monkeypatch):
    """A real competing collector must observe the admission after the lock is released."""
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    db = SessionDB(db_path=tmp_path / 'state.db')
    db.create_session('session', source='test')
    producer = """
import os, sys
from pathlib import Path
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch, admit_session_input
from gateway.session_shared_media_private import private_store, capture_private
db = SessionDB(db_path=Path(os.environ['HERMES_HOME']) / 'state.db')
epoch = begin_runtime_epoch(db, instance_id='race-fixture')
with private_store() as root:
    ref = capture_private(root, 'race.txt', b'race bytes')
    print(ref['path'], flush=True)
    sys.stdin.readline()
    admit_session_input(db, epoch=epoch, principal_id='test', session_id='session',
        request_id='request', payload={'text': 'hello', 'native_text_v1': {'media': [ref]}})
"""
    collector = """
import os, time
from pathlib import Path
from hermes_state import SessionDB
from gateway.session_shared_media_private import private_store, recover_private
db = SessionDB(db_path=Path(os.environ['HERMES_HOME']) / 'state.db')
print('ready', flush=True)
with private_store() as root:
    print(recover_private(root, db, older_than=time.time() + 10), flush=True)
"""
    p = subprocess.Popen([sys.executable, '-c', producer], env=dict(os.environ),
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    c = None
    try:
        assert p.stdout is not None and p.stdin is not None
        path = Path(p.stdout.readline().strip())
        assert path.read_bytes() == b'race bytes'
        c = subprocess.Popen([sys.executable, '-c', collector], env=dict(os.environ),
                             stdout=subprocess.PIPE, text=True)
        assert c.stdout is not None
        assert c.stdout.readline().strip() == 'ready'
        with pytest.raises(subprocess.TimeoutExpired):
            c.wait(timeout=0.3)
        p.stdin.write('commit\n')
        p.stdin.flush()
        assert p.wait(timeout=10) == 0
        assert c.communicate(timeout=10)[0].strip() == '0'
        assert c.returncode == 0 and path.read_bytes() == b'race bytes'
    finally:
        for process in (p, c):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=10)


def test_idle_housekeeping_skips_live_lock_then_collects(tmp_path, monkeypatch):
    import agent.provider_media as provider_media
    import hermes_state_registry
    from gateway.run import _housekeeping_private_media
    from gateway.session_shared_media_private import private_store, capture_private
    from hermes_state import SessionDB
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(provider_media, 'MEDIA_CACHE_MAX_AGE_HOURS', 0)
    db = SessionDB(db_path=tmp_path / 'state.db')
    monkeypatch.setattr(hermes_state_registry, 'acquire', lambda: db)
    monkeypatch.setattr(hermes_state_registry, 'release_or_close', lambda _db: None)
    with private_store() as root:
        orphan = capture_private(root, 'idle.txt', b'idle')
    code = ("import time; from gateway.session_shared_media_private import private_store; "
            "ctx=private_store(); ctx.__enter__(); print('locked',flush=True); time.sleep(30)")
    proc = subprocess.Popen([sys.executable, '-c', code], env=dict(os.environ), stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == 'locked'
        _housekeeping_private_media()
        assert Path(orphan['path']).exists()
        proc.kill()
        assert proc.wait(timeout=10) == -signal.SIGKILL
        _housekeeping_private_media()
        assert not Path(orphan['path']).exists()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
