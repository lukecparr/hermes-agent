"""Profile-scoped in-process writers for a terminal transaction."""
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _optional_writer(callback):
    """Drop only this notification on ordinary callback failure; preserve chat settlement."""
    def write(*, conn, **kwargs):
        conn.execute('SAVEPOINT optional_settlement_writer')
        try:
            callback(conn=conn, **kwargs)
        except Exception:
            conn.execute('ROLLBACK TO optional_settlement_writer')
            logger.error('Optional settlement writer failed; its effects were dropped')
        finally:
            conn.execute('RELEASE optional_settlement_writer')
    return write


def profile_home(db, profile_id=None):
    """The authority's absolute profile identity wins over the storage location."""
    candidate = Path(profile_id) if profile_id else None
    return candidate.resolve() if candidate is not None and candidate.is_absolute() else Path(db.db_path).resolve().parent


def required_extensions(db, profile_id=None):
    """Resolve required plugins before the SQLite write; missing registrations fail closed.

    Snapshot callbacks for this settlement: unloading a plugin later cannot remove an already
    started write. Discovery and config always use the database owner's profile, not ambient scope.
    """
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from hermes_cli.config_effective import load_user_config_effective
    from hermes_cli.plugins import discover_plugins, get_plugin_manager

    home = profile_home(db, profile_id)
    token = set_hermes_home_override(home)
    try:
        config = load_user_config_effective(fail_closed=True)
        gateway = config.get('gateway', {})
        if not isinstance(gateway, dict):
            raise ValueError('gateway config must be a mapping')
        names = gateway.get('required_settlement_plugins', [])
        optional = gateway.get('optional_settlement_plugins', [])
        for key, entries in (('required', names), ('optional', optional)):
            if (not isinstance(entries, list) or any(not isinstance(n, str) or not n.strip() or n != n.strip()
                                                     for n in entries) or len(set(entries)) != len(entries)):
                raise ValueError(f'gateway.{key}_settlement_plugins must be a list of unique plugin IDs')
        if set(names) & set(optional):
            raise ValueError('settlement plugin cannot be both required and optional')
        if not names and not optional:
            return ()
        try:
            discover_plugins()
            manager = get_plugin_manager()
        except Exception:
            if names:
                raise
            logger.error('Optional settlement plugins unavailable; notifications may be dropped')
            return ()
        with manager._discovery_lock:
            callbacks = []
            for name in names:
                plugin = manager._plugins.get(name)
                callback = manager._settlement_extensions.get(name)
                if plugin is None or not plugin.enabled or plugin.error or callback is None:
                    raise RuntimeError(f'required settlement plugin unavailable: {name}')
                callbacks.append(callback)
            for name in optional:
                plugin = manager._plugins.get(name)
                callback = manager._settlement_extensions.get(name)
                if plugin is None or not plugin.enabled or plugin.error or callback is None:
                    logger.error('Optional settlement plugin unavailable; notifications may be dropped')
                    continue
                callbacks.append(_optional_writer(callback))
            return tuple(callbacks)
    finally:
        reset_hermes_home_override(token)


def write_extensions(db, callbacks, conn, *, row, route, response, outcome, result, profile_id=None):
    """Run in the owner's profile and transaction; exceptions roll the write back."""
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    token = set_hermes_home_override(profile_home(db, profile_id))
    try:
        for callback in callbacks:
            callback(conn=conn, row=row, route=route, response=response, outcome=outcome, result=result)
    finally:
        reset_hermes_home_override(token)
