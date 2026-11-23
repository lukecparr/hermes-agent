"""Shared-thread automation must retain bytes through the actual authority."""
import json
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_shared_media_is_captured_and_exact_retry_is_idempotent(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority, SessionAuthority
    from gateway.session_ingress_context import native_callback
    from gateway.session_envelope import restore_native
    from gateway.session_ingress_media import release_unheld_media
    from hermes_constants import get_hermes_home
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setattr(SessionAuthority, '_schedule', lambda self, ref: None)
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42')
    runner = GatewayRunner(GatewayConfig())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token='fixture-token', typing_indicator=False))
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='owner')
    runner._wire_adapter_handlers(adapter)
    source = adapter.build_source(chat_id='42', chat_type='dm', user_id='42')
    event_human = MessageEvent(text='human', source=source, message_id='human')
    with native_callback(runner, event_human, get_hermes_home()):
        human = await authority.admit_native(event_human)
    route = runner.session_store._generate_session_key(source)
    identity = json.dumps(['piper-shared-thread', 'second-brain-web', 'web-123456789'], separators=(',', ':'))
    staging = get_hermes_home() / 'piper-shared-thread' / 'attachments' / 'random'
    staging.mkdir(parents=True)
    photo = staging / 'photo.png'
    photo.write_bytes(b'\x89PNG\r\n\x1a\nimage-bytes')
    event = MessageEvent(text='look', source=source, internal=True, media_urls=[str(photo)],
                         media_types=['image/png'], metadata={'gateway_session_key': route,
                         'gateway_session_id': human.ref.session_id})
    receipt = await authority.admit_automation(adapter, event, identity)
    rows = list_session_admissions(authority.db, session_id=human.ref.session_id, pending_only=False)
    row = next(row for row in rows if row['request_id'] == identity)
    restored = restore_native(row['payload'], runner)
    assert Path(restored.media_urls[0]).read_bytes() == photo.read_bytes()
    assert restored.media_types == ['image/png']
    assert release_unheld_media(authority.db, row['payload']['native_text_v1']['media'], retain_history=False) == 0
    assert Path(restored.media_urls[0]).exists()
    assert (await authority.admit_automation(adapter, event, identity)).admission_id == receipt.admission_id
    photo.write_bytes(b'different')
    with pytest.raises(RuntimeStoreError):
        await authority.admit_automation(adapter, event, identity)
    with pytest.raises(RuntimeStoreError):
        await authority.admit_automation(adapter, event, 'other-producer')
    outside = tmp_path / 'outside.txt'
    outside.write_text('private')
    event.media_urls = [str(outside)]
    event.media_types = ['text/plain']
    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        await authority.admit_automation(adapter, event, identity)
    event.media_urls = [str(staging / 'link')]
    (staging / 'link').symlink_to(outside)
    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        await authority.admit_automation(adapter, event, identity)
