# Piper immediate dispatch and reply previews

## Contract and privacy

Only a completed, eligible final answer can supply a preview. No user messages,
tool results, reasoning, transcript history, attachment paths or routing metadata
are projected. Preview normalization is bounded to an 8 KiB input window and 240
Unicode codepoints, removes markup, fenced code, media directives, URLs, control
and bidi characters, and rejects internal channel/tool markers. The producer also
forces Hermes secret redaction. Missing or rejected text falls back to `New reply`.

An optional `preview` field extends the authenticated existing completion event;
the event type, opaque ID and turn-ID validation stay unchanged. Existing receivers
reject unknown fields: do not enable previews before the SB server update.

The local SQLite upgrade adds only a nullable preview column, in the notification
transaction when enabled. The sender clears the preview on acknowledgement. SB adds
one nullable preview column with a 240-character CHECK constraint. It clears the
preview after every targeted device is terminal (or no opted-in devices exist),
retaining the existing dedup tombstone. Pending retries retain the bounded preview.
Duplicates never overwrite stored preview or recreate deliveries.

## Dispatch

The SB receiver awaits event-scoped dispatch **after** the event and per-device
rows commit. Dispatch errors cannot revoke durable acknowledgement. Existing
leases, opt-out and token-rotation checks apply to both this wake and the 60-second
retry timer. A timer racing an event-driven wake cannot claim the same live lease.

Before that SB deployment, the local sender supports `--dispatch-helper` pointing
to the existing trusted Python cron helper. It invokes the helper only after a
new receiver acknowledgement, with no shell or event content. Failure leaves the
60-second timer as the retry path; startup, empty passes, blocked sends and replay
of sent rows do not wake it. This works with the currently deployed receiver.
The existing local outbox scan interval remains 10 seconds; event-driven dispatch
removes the additional wait for the minute timer, not every possible source of
latency. No real-time latency guarantee is made for FCM/Android.

## Android

FCM remains data-only; no notification payload is added. Android reads only the
bounded `preview` field, displays it using plain text and BigTextStyle, and keeps
existing preference/resource gates, SQLite event-ID dedup, channel, notification
tag, immutable PendingIntent and fixed Piper chat destination. Older payloads and
older clients remain generic. This code must be rebuilt into a signed push-enabled
APK; do not install the debug verification APK over the user's release app. Keep
ordinary release push disabled. Carry the reviewed pushRc configuration from the
existing Android candidate, verify a versionCode greater than the installed build,
and compare signing certificates before an explicitly approved in-place update.

## Rollout

1. Publish/review both repositories; no automatic merge.
2. Local safe slice: back up and verify old sender/worker/plugin files and worker
   unit. With no due or leased local events, install the verified sender/worker,
   configure only the existing dispatch helper, and restart only the notification
   worker. Keep `piper-push-dispatch.timer` and its 60-second schedule unchanged.
   Stage plugin files but keep `include_preview` absent/false and
   `PIPER_COMPLETION_PREVIEW` absent/not `1`. No gateway restart is needed for the
   immediate local wake. No synthetic live notification or historical backfill.
3. With approval, merge/deploy the SB PR through the normal main deployment path,
   applying `20261125000000_piper_reply_preview`. Verify migration and health.
4. Build and approve the signed push-enabled Android update, preserving app data.
5. Enable `plugins.entries.piper-turn-outbox.settings.include_preview: true` and
   `PIPER_COMPLETION_PREVIEW=1` for the worker only after server compatibility is
   verified. Restart the worker; the operator runs `hermes gateway restart` from a
   separate shell (or `/restart` in Telegram). Never bypass restart guards.
6. Use one new supervised phone turn; correlate result, outbox/event/delivery and
   actual phone preview/tap behavior. Never replay prior test IDs.

Rollback: disable preview sending first if reverting SB; never send the extended
payload to the old strict receiver. Restore protected old files/unit and restart
only the affected worker, keeping the timer. Plugin activation/rollback requires
the supported operator gateway restart. Keep the nullable SQL column and queues;
do not roll back conversation or notification history. A generic notification is
an acceptable safe fallback on any old or unavailable component.

## Verification

93 Hermes tests pass via the supported runner. 27 focused SB tests, typecheck,
ESLint and Fallow audit pass. Android `testDebugUnitTest lintDebug assembleDebug`
passes with 653 tests and zero failures. Disposable integration runs cover normal
streamed settlement, captured-result recovery, immediate send failure followed by
a simulated 60-second timer retry, and local post-ACK wake with server immediate
dispatch disabled. All use real receiver/dispatcher/SQL and mocked FCM; assertions
cover preview contents, exclusion of private tool/reasoning fixtures, opt-out,
event/timer replay dedup and preview disposal. No real notification is generated.
