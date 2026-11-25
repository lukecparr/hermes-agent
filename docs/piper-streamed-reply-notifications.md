# Streamed reply notification writer fix

## Failure and scope

A successfully streamed reply sets `already_sent=true`. The delivery layer returns
`None` so the adapter does not send the answer twice. The optional outbox plugin
previously passed this adapter return to `eligible_turn`, which rejected it as an
empty answer despite a captured `completed=true` result containing `final_response`.

The plugin now uses the captured answer only when the adapter return is `None`
and `already_sent` is exactly true. Explicit empty responses remain suppressed;
muted, incomplete, silent and wrong-route turns produce no event. The normal
transaction and admission-key deduplication remain unchanged. Physical session IDs,
routes and transport turn IDs are distinct and must never be rewritten to fix
notification eligibility.

Only `contrib/piper-turn-outbox/__init__.py` needs installation in the existing
profile's `plugins/piper-turn-outbox/`. No gateway-core, Second Brain, schema,
configuration, routing or admission-history change is needed. No backfill is part
of this fix: replay of an already terminal admission does not create its missing
notification.

## Verification

- Regression through the real delivery suppression method, real plugin discovery
  and transactional settlement failed before the fix with no outbox table. It
  passes after the fix, preserving a separate physical session ID and deduplicating
  exact settlement retries.
- The supported test runner passes 74 tests across settlement extensions, outbox,
  sender, worker, retention, pruning and settlement-kill tests.
- `scripts/piper_combined_rc_probe.py` passes with `PIPER_TEST_STREAMING=0` and `1`.
  Both exercise synthetic Telegram, Charm and SB file turns plus replay and file
  bytes. Streaming mode requires all three captured results to report `already_sent`.
  It uses loopback fixtures, not live messaging services or a real model.
- `tests/gateway/piper_integration.mjs` passes normal streamed settlement (adapter
  return `None`) and captured-result recovery through real discovery, SQLite outbox,
  HTTP sender, Second Brain receiver/dispatcher, disposable PGlite and mocked FCM.
  Assertions read back one event, one delivery only to the opted-in fixture device,
  one mocked send, and terminal delivery state `sent`.

Run unit tests with `scripts/run_tests.sh` and the seven `test_*piper*`/settlement
files named above. For the optional SQL integration provide `PIPER_TEST_SB_REPO`,
`PIPER_TEST_PGLITE_MODULES` and `PIPER_TEST_PYTHON`. If the pinned runtime activates
imports through paths rather than a venv executable, pass those import paths as
`PIPER_TEST_PYTHONPATH`; the harness forwards only that explicit value to its child,
not the parent's credentials or production database configuration.

## Controlled rollout and rollback

1. Verify the installed writer matches the prior reviewed source. Preserve its
   original bytes and mode in a protected rollback directory, with SHA-256 manifest.
2. Atomically replace only the installed writer's `__init__.py` with the committed,
   tested bytes. Read back and compare to the exact commit blob and checksum.
3. Do not force plugin reload or restart from the executing assistant. The operator
   can run `hermes gateway restart` in a separate terminal, or `/restart` in Telegram.
4. Read back the new PID, service health and writer status. Then use one newly
   submitted supervised SB turn; correlate its committed result, local outbox,
   SB event/delivery and phone reception. Never resubmit the old test or backfill it.
5. To roll back, atomically restore the protected old writer file, verify its hash,
   and use the same supported operator restart. Preserve queues, deliveries and
   admission history; never restore an old conversation database for this file fix.

A local mocked FCM test proves the backend path, not actual phone delivery. Until
the operator restart and supervised phone test complete, report installation on
disk separately from the code loaded by the running gateway.
