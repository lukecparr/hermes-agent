"""Opt-in idle/daily session rotation happens only on user ingress."""
from datetime import datetime, timedelta

from gateway.config import GatewayConfig, Platform
from gateway.config_loader import bridge_toplevel_keys
from gateway.session import SessionSource, SessionStore


def _source(chat_id: str = "time-reset") -> SessionSource:
    return SessionSource(platform=Platform.TELEGRAM, chat_id=chat_id, user_id="test")


def test_timer_rotation_is_opt_in_user_activity_and_process_guarded(tmp_path):
    now = datetime.now()
    default_store = SessionStore(tmp_path / "default", GatewayConfig.from_dict({}))
    default_entry = default_store.get_or_create_session(_source("default"))
    default_entry.updated_at = now - timedelta(days=3)
    default_store._save()
    assert default_store.get_or_create_session(_source("default")).session_id == default_entry.session_id

    active = False
    raw = {}
    bridge_toplevel_keys(
        {"session_reset": {"mode": "both", "idle_minutes": 90, "at_hour": 6}},
        None,
        raw,
    )
    config = GatewayConfig.from_dict(raw)
    assert config.session_reset.mode == "both"
    assert config.session_reset.idle_minutes == 90
    assert config.session_reset.at_hour == 6
    store = SessionStore(
        tmp_path / "enabled", config,
        has_active_processes_fn=lambda _key: active,
    )
    old = store.get_or_create_session(_source())
    old.updated_at = now - timedelta(minutes=91)
    store._save()

    # Internal wakes neither rotate the conversation nor make it look recently user-active.
    internal = store.get_or_create_session(_source(), touch_activity=False)
    assert internal.session_id == old.session_id
    assert internal.updated_at == old.updated_at

    # Active background work protects an expired conversation from an inbound reset.
    active = True
    guarded = store.get_or_create_session(_source())
    assert guarded.session_id == old.session_id

    active = False
    guarded.updated_at = now - timedelta(minutes=91)
    store._save()
    rotated = store.get_or_create_session(_source())
    assert rotated.session_id != old.session_id
    assert rotated.auto_reset_reason == "idle"
    assert store._db.get_session(old.session_id)["end_reason"] == "idle"


def test_daily_boundary_rotates_on_next_user_message_only(tmp_path):
    config = GatewayConfig.from_dict({"session_reset": {"mode": "daily", "at_hour": 6}})
    store = SessionStore(tmp_path / "daily", config)
    source = _source("daily")
    old = store.get_or_create_session(source)
    # Place user activity before the most recent 06:00 local boundary.
    now = datetime.now()
    boundary = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now < boundary:
        boundary -= timedelta(days=1)
    old.updated_at = boundary - timedelta(minutes=1)
    store._save()
    assert store.get_or_create_session(source, touch_activity=False).session_id == old.session_id
    rotated = store.get_or_create_session(source)
    assert rotated.session_id != old.session_id
    assert rotated.auto_reset_reason == "daily"
    assert store._db.get_session(old.session_id)["end_reason"] == "daily"


def test_timer_rotation_survives_recovery_without_overriding_fresh_resume(tmp_path):
    config = GatewayConfig.from_dict({
        "session_reset": {"mode": "idle", "idle_minutes": 90},
    })
    store = SessionStore(tmp_path / "sessions", config)
    source = _source("restart")
    old = store.get_or_create_session(source)
    old.updated_at = datetime.now() - timedelta(minutes=91)
    store._save()
    store._db._execute_write(
        lambda conn: conn.execute(
            "UPDATE sessions SET last_activity_at = ? WHERE id = ?",
            (old.updated_at.timestamp(), old.session_id),
        )
    )

    # Simulate a lost routing index after restart: durable peer recovery must still apply policy.
    store._db.replace_gateway_routing_entries({}, scope=store._routing_scope())
    store._entries.clear()
    recovered = store.get_or_create_session(source)
    assert recovered.session_id != old.session_id
    assert recovered.auto_reset_reason == "idle"
    assert store._db.get_session(old.session_id)["end_reason"] == "idle"

    # A freshly marked interrupted turn is recovery, not an idle boundary.
    recovered.updated_at = datetime.now() - timedelta(minutes=91)
    recovered.resume_pending = True
    recovered.last_resume_marked_at = datetime.now()
    store._save()
    resumed = store.get_or_create_session(source)
    assert resumed.session_id == recovered.session_id
    assert resumed.resume_pending is True
