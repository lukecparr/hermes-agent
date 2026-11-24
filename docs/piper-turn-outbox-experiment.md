# Piper shared-thread completion outbox (experimental)

This branch is an isolated experimental producer and sender, not a push implementation. Leave it disabled. The sender worker is explicitly invoked and not installed as a scheduled service; no production configuration or deployment is included.

The installed `piper-shared-thread` plugin admits Second Brain web inputs through the existing gateway authority as `automation:<route>`, with request identity JSON `["piper-shared-thread","second-brain-web",turn_id]`. The normal in-process authority drain calls `commit_and_publish` -> `finish_result` -> `settle_session_input`. The latter commits the terminal admission, result and `_terminal_write` in one SQLite transaction. `admission_id` is created as a UUID on admission; exact admission replay returns the existing row. The plugin also keeps its own turn-id ledger. The outbox stores both ids, without message text or credentials.

The experimental `contrib/piper-turn-outbox` plugin registers a generic in-process settlement writer. It is not installed by this branch. If explicitly enabled in an isolated profile, the profile config must name it in `gateway.required_settlement_plugins`, enable it under `plugins.enabled`, and set `plugins.entries.piper-turn-outbox.settings.route`. The route must exactly match the gateway live route. The former `PIPER_TURN_OUTBOX_ROUTE` environment switch no longer enables the producer. A missing, disabled, broken, timed-out, or host-isolated required plugin refuses settlement rather than dropping the event. An empty required list runs no writers. Do not enable this experiment in production.

For an isolated profile only, the required settings have this shape (the plugin directory must first be available to normal plugin discovery):

```yaml
gateway:
  required_settlement_plugins: [piper-turn-outbox]
plugins:
  isolation: in_process
  enabled: [piper-turn-outbox]
  entries:
    piper-turn-outbox:
      settings:
        route: '<exact shared-thread route>'
```

The generic API is `ctx.register_settlement_extension(callback)`. Callbacks receive keyword arguments `conn`, `row`, `route`, `response`, `outcome`, `result`; they run synchronously inside the owner's SQLite transaction on normal settlement and same-owner captured-result recovery. They may be called again after a retry and must be idempotent. Callbacks are trusted in-process code: they must use only the provided connection, never commit/rollback or retain it, never do network/file I/O, and must run quickly. Exceptions roll back the settlement and all extension writes. A callback snapshot is held for an in-flight settlement even if plugins are concurrently unloaded. Host isolation cannot transfer the live transaction and refuses this registration. The required list and callbacks are scoped to the authority's profile identity (the database directory is a fallback for older test/embedded authorities); malformed configuration or missing registration fails closed. This is not a security sandbox against malicious plugins.

The Piper writer inserts `piper.turn.completed.v1` only for an actual shared-thread Second Brain web admission with a completed agent result, completed authority outcome and non-empty non-silent response. The admission UUID is the primary key. The producer creates its table in the settlement transaction and relies on the existing state database permissions. An outbox insertion failure rolls the settlement back; the authority's existing settlement retry/fence behavior applies. A broken required extension can pause turns. No secret is put in the route or event.

## Limitations and next steps

- The authority's captured-result unknown-resolution path now writes the outbox in the same transaction, tested with a simulated same-owner recovery. Managed workers hand accepted results to the normal drain. The existing managed-worker and recovery suites pass in a separate isolated clone (the linked worktree's home-I/O guard blocked its Git metadata); these suites do not by themselves prove the complete outbox-to-device path. Other resolution/recovery routes still need auditing. Do not enable until those gaps and real gateway configuration are verified.
- The sender leases records in SQLite, sends a minimal JSON body over HTTPS (HTTP allowed only to loopback) with a separate bearer credential, blocks non-retryable 4xx, retries transient/ambiguous outcomes with the same event ID and fences stale lease holders. `scripts/piper_completion_worker.py` explicitly runs this sender against an existing database; it does not install a service or scheduler, and never logs URLs, credentials or turn IDs. A separately supervised process with protected environment is the proposed operational model; monitor blocked rows and pending age. The receiver must commit event deduplication before returning 2xx. The table will grow indefinitely if enabled.
- Sender rows gain a nullable terminal timestamp; old rows remain NULL and are never automatically guessed. The unscheduled `piper_outbox_retention.prune_sent` function deletes at most 100 acknowledged rows only after 90 days. It never removes pending, leased, blocked or legacy untimestamped rows. This relies on the receiver retaining event-ID tombstones indefinitely, including across backup restore. Pause senders and reconcile if restoring a receiver backup older than the sender's retained acknowledgements. The receiver and Android dedup ledgers remain unbounded.
- Telegram, Charm/Deck sidecar exchanges, process/cron notices and other producer turns are intentionally out of scope, rather than blocking Second Brain web turns. Telegram remains unchanged. The current route is Telegram-backed, so removing Telegram dependency requires a separate shared-authority migration; do not silently redirect to the native phone-relay thread.
- Verify actual Android permission, a registered token, Firebase server credentials, generic private notification payload, app-closed/cold-start tap into the shared `/piper` route, and OS force-stop behavior on a device. No phone test has occurred.
