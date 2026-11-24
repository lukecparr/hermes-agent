"""Required, profile-scoped in-process writers for a terminal transaction."""
from pathlib import Path


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
        if (not isinstance(names, list) or any(not isinstance(n, str) or not n.strip() or n != n.strip()
                                               for n in names) or len(set(names)) != len(names)):
            raise ValueError('gateway.required_settlement_plugins must be a list of unique plugin IDs')
        if not names:
            return ()
        discover_plugins()
        manager = get_plugin_manager()
        with manager._discovery_lock:
            callbacks = []
            for name in names:
                plugin = manager._plugins.get(name)
                callback = manager._settlement_extensions.get(name)
                if plugin is None or not plugin.enabled or plugin.error or callback is None:
                    raise RuntimeError(f'required settlement plugin unavailable: {name}')
                callbacks.append(callback)
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
