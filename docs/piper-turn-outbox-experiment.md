# Piper shared-thread completion outbox (experimental)

This branch is an isolated experimental producer and sender, not a push implementation. Leave it disabled. The sender worker is explicitly invoked and not installed as a scheduled service; no production configuration or deployment is included.

The installed `piper-shared-thread` plugin admits Second Brain web inputs through the existing gateway authority as `automation:<route>`, with request identity JSON `["piper-shared-thread","second-brain-web",turn_id]`. The normal in-process authority drain calls `commit_and_publish` -> `finish_result` -> `settle_session_input`. The latter commits the terminal admission, result and `_terminal_write` in one SQLite transaction. `admission_id` is created as a UUID on admission; exact admission replay returns the existing row. The plugin also keeps its own turn-id ledger. The outbox stores both ids, without message text or credentials.

## Recommended chat-first policy (experimental)

Use `gateway.optional_settlement_plugins: [piper-turn-outbox]` in disposable staging; keep the feature disabled in production. An optional writer runs under a SQLite savepoint: a callback exception rolls back that writer's partial changes and logs a fixed error while ordinary chat settlement continues. A missing/disabled writer also logs and skips notifications. This deliberately trades notification completeness for chat availability: dropped notifications cannot be replayed from the outbox. A severe database failure that prevents savepoint rollback/release can still block settlement. Required writers retain their original strict behavior for other uses. Both lists default empty and a writer cannot appear in both.

The legacy required-writer configuration and failure analysis below describe an alternative, not the recommended policy for Piper. No production deployment or enablement is authorized.

## Legacy required-writer configuration

The experimental `contrib/piper-turn-outbox` plugin registers a generic in-process settlement writer. It is not installed by this branch. If explicitly enabled in an isolated profile, the profile config must name it in `gateway.required_settlement_plugins`, enable it under `plugins.enabled`, and set `plugins.entries.piper-turn-outbox.settings.route`. The route must exactly match the gateway live route. The former `PIPER_TURN_OUTBOX_ROUTE` environment switch no longer enables the producer. A missing, disabled, broken, timed-out, or host-isolated required plugin refuses authority startup before epoch rotation, and is checked again at settlement after reloads, rather than dropping the event. An empty required list runs no required writers; optional writers can still be configured. Do not enable this experiment in production.

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

The generic API is `ctx.register_settlement_extension(callback)`. Callbacks receive keyword arguments `conn`, `row`, `route`, `response`, `outcome`, `result`; they run synchronously inside the owner's SQLite transaction on normal settlement, same-owner captured-result recovery, and interruption of an unknown execution (including boot recovery). For unknown executions without a captured answer, `outcome` is `interrupted`, `response` is empty and `result` is `None`; boot can recover a route from an `automation:` principal but other routes may be `None`. They may be called again after a retry and must be idempotent. Callbacks are trusted in-process code: they must use only the provided connection, never commit/rollback or retain it, never do network/file I/O, and must run quickly. Exceptions roll back the settlement and all extension writes. A callback snapshot is held for an in-flight settlement even if plugins are concurrently unloaded. Host isolation cannot transfer the live transaction and refuses this registration. The required list and callbacks are scoped to the authority's profile identity (the database directory is a fallback for older test/embedded authorities); malformed configuration or missing registration fails closed. This is not a security sandbox against malicious plugins.

The Piper writer inserts `piper.turn.completed.v1` only for an actual shared-thread Second Brain web admission with a completed agent result, completed authority outcome and non-empty non-silent response. The admission UUID is the primary key. The producer creates its table in the settlement transaction and relies on the existing state database permissions. An outbox insertion failure rolls the settlement back; the authority's existing settlement retry/fence behavior applies. A broken required extension can pause turns. No secret is put in the route or event.

## Audited authority paths

- Normal in-process, managed-worker, cron, and unviewed completion-note executions converge on the drain's `finish_result`; an already terminal replay returns the stored receipt without running writers again. The managed worker's `execution.finish` persists an empty worker-termination receipt, not its answer; its result frame is accepted by the owner before the drain settles it.
- Explicit unknown resolution and same-owner captured recovery go through `resolve_unknown_as_owner`; unattended unknowns at boot go through `settle_recovered_unattended`. Writers now run in each resolution transaction, including interrupted outcomes, and failure rolls back that resolution. A restarted owner never adopts the prior owner's in-memory answer.
- Queued cancellation, replacement, and queued steer transfer settle without executing an independent turn; they do not invoke this execution-settlement extension. Retired admission/tombstone paths and the `retain_result` test helper are not a new production completion hook. The extension is not a universal observer of every terminal admission.
- SIGKILL tests interrupt a separate owner process immediately after outbox insertion but before COMMIT and immediately after COMMIT, on both normal and same-owner captured-result paths. Before COMMIT neither event nor terminal result survives; after COMMIT both survive. Recovery before commit is interrupted, not a fabricated completed answer.

## Isolated real-gateway normal-chat smoke test

`scripts/piper_gateway_normal_probe.py` starts the actual gateway in the foreground,
with a fresh scratch profile and a loopback-only fake model. It admits a normal
interactive chat turn, checks the transcript, and waits for the SQLite admission
to settle completed. Run both the default-off configuration and the required-writer
configuration with a Python environment containing the existing gateway test deps:

```sh
env -i PATH="$PATH" TMPDIR=/absolute/disposable/scratch python -B scripts/piper_gateway_normal_probe.py
env -i PATH="$PATH" TMPDIR=/absolute/disposable/scratch python -B scripts/piper_gateway_normal_probe.py --required
```

The required case also verifies one required writer is registered. Each invocation
uses a new temporary profile, strips inherited credentials/settings and contacts
only loopback. It leaves the scratch home for safe cleanup after process exit.
This checks ordinary interactive chat, not the shared-web turn or notification
path; it does not prove behavior when the required writer fails. It does not use
Firebase, a phone, or a real agent provider. The required writer still intentionally
fails settlement closed on registration or callback failure.

## Foreground shared-web integration probe

An isolated foreground real-gateway run on this branch used a synthetic relay and loopback model, admitted a shared-web turn with the optional writer, and committed one completion row. The row's event was handed off via a local scratch file to the actual Second Brain receiver with disposable PGlite SQL and a mocked FCM transport. The receiver returned 204 for the event and replay, queued only the opted-in synthetic Android device, and the dispatcher sent one mocked message. The file handoff bypassed the Hermes HTTP sender and the Second Brain web proxy, so this is not a continuous wire-level end-to-end test. No real phone or Firebase was used.

## Limitations and next steps

- The authority's captured-result unknown-resolution path writes the outbox in the same transaction for a result still held by the same owner. After a process death that in-memory capture is lost: boot recovery settles an unattended unknown as interrupted, without inventing a completion or notification. Durable capture prior to settlement is not implemented; adding it would need a separately fenced result journal, retention/deletion and redaction rules, safe cross-epoch adoption, and correct handling of transcript and steer-transfer state. Even a journal cannot recover an answer if the process dies before the journal commit. Do not enable until that design and real gateway configuration are verified.
- A disposable integration harness exercises authority admission and settlement through the actual SQLite outbox and HTTP sender, the Second Brain receiver and its real SQL via in-memory PGlite, and its dispatcher with mocked FCM. It runs both normal and same-owner captured recovery, but does not start the full gateway, run a real model, Firebase, or a phone. The harness requires already-present Second Brain and PGlite dependencies and installs nothing: `PIPER_TEST_SB_REPO=<sb-checkout> PIPER_TEST_PGLITE_MODULES=<node_modules-containing-pglite-and-pglite-socket> PIPER_TEST_PYTHON=<test-python> node tests/gateway/piper_integration.mjs`. It creates and removes synthetic homes beneath this checkout and uses loopback and an in-memory database.
- The sender leases records in SQLite, sends a minimal JSON body over HTTPS (HTTP allowed only to loopback) with a separate bearer credential, blocks non-retryable 4xx, retries transient/ambiguous outcomes with the same event ID and fences stale lease holders. `scripts/piper_completion_worker.py` explicitly runs this sender against an existing database; it does not install a service or scheduler, and never logs URLs, credentials or turn IDs. A separately supervised process with protected environment is the proposed operational model; monitor blocked rows and pending age. The receiver must commit event deduplication before returning 2xx. The table will grow indefinitely if enabled.
- Sender rows gain a nullable terminal timestamp; old rows remain NULL and are never automatically guessed. The unscheduled `piper_outbox_retention.prune_sent` function deletes at most 100 acknowledged rows only after 90 days. It never removes pending, leased, blocked or legacy untimestamped rows. This relies on the receiver retaining event-ID tombstones indefinitely, including across backup restore. Pause senders and reconcile if restoring a receiver backup older than the sender's retained acknowledgements. The receiver and Android dedup ledgers remain unbounded.
- Telegram, Charm/Deck sidecar exchanges, process/cron notices and other producer turns are intentionally out of scope, rather than blocking Second Brain web turns. Telegram remains unchanged. The current route is Telegram-backed, so removing Telegram dependency requires a separate shared-authority migration; do not silently redirect to the native phone-relay thread.
- Verify actual Android permission, a registered token, Firebase server credentials, generic private notification payload, app-closed/cold-start tap into the shared `/piper` route, and OS force-stop behavior on a device. No phone test has occurred.
