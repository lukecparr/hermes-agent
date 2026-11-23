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


def test_existing_native_restore_rejects_private_namespace(tmp_path, monkeypatch):
    from gateway.session_shared_media_private import private_store, capture_private
    from gateway.session_ingress_media import restore_native_media
    from hermes_state_runtime import RuntimeStoreError
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    with private_store() as root:
        ref = capture_private(root, 'file.txt', b'proof')
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
