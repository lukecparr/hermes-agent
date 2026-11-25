"""Preserve the live X11 desktop fix when evaluating the Piper candidate."""
from tools.bot_desktop import runtime


def test_running_desktop_replaces_wayland_session_type(monkeypatch):
    monkeypatch.setattr(runtime, "published_env", lambda: {"DISPLAY": ":20"})
    monkeypatch.setattr(runtime, "touch_activity", lambda: None)
    result = runtime.desktop_env({"WAYLAND_DISPLAY": "wayland-0", "XDG_SESSION_TYPE": "wayland"})
    assert result["DISPLAY"] == ":20"
    assert result["XDG_SESSION_TYPE"] == "x11"
    assert "WAYLAND_DISPLAY" not in result


def test_inactive_desktop_preserves_seat(monkeypatch):
    monkeypatch.setattr(runtime, "published_env", dict)
    seat = {"WAYLAND_DISPLAY": "wayland-0", "XDG_SESSION_TYPE": "wayland"}
    assert runtime.desktop_env(seat) == seat
