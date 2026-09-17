# Daemon Messaging

`crates/tachyond/src/messaging/` extracts existing daemon interaction mechanics
without introducing a bus, new permissions, or model decisions.

- `commands.rs` encodes the versioned foreground command envelope, retaining
  correlation fallback, causation, turn, host-selected cwd, and process-local IDs.
- `notifications.rs` encodes work-attention, reminder, and scheduled-task notices;
  projects canonical visible interaction history; acknowledges notification
  delivery; and publishes schedule events after successful acknowledgement.
- `subscriptions.rs` owns agent/work subscription registration and socket streams.
  Connection dispatch and the foreground subscription alias remain in `main.rs`.

## Unchanged Contracts

History projection is durable: canonical nonblank user, assistant, and notification
messages enter the runtime history outbox before projection into the history store.
The outbox is acknowledged only after history application succeeds. Deltas,
intents, and timeout events are not projected. Missing history storage leaves the
outbox pending. Existing best-effort error handling is unchanged.

Reminder delivery is acknowledged only for a published user-visible notification
with the existing reminder correlation prefix. The runtime store must commit the
transition from delivering to delivered before `ReminderFired` is emitted. A
duplicate acknowledgement does not emit another fired event. Scheduled-task
acknowledgements retain their separate correlation-prefix path. This does not add
an atomic transaction spanning history, reminder state, and socket publication.

Agent subscriptions replay cached terminal usage before the cached terminal
result, then stay registered for live events. Work subscriptions replay the cached
terminal result and close; subscriptions registered before completion are drained
when that result is published. Registration and cache inspection remain under the
same registry lock. Replay preserves the original serialized event and its ID.
These are in-memory subscriptions, **not durable cursor streams**. The shared
daemon sequence and command sequence remain process-local counters.

Registry, task, and work-record fields remain private; no schema or lifecycle
authority is exported. Messaging is a private child module of the daemon binary.
Lifecycle-sensitive `push_event`, worker result collection/timeouts, scheduler
loops, artifact handling, and task input transport remain in `main.rs`. In
particular, fan-out, compaction, history projection, reminder acknowledgement,
logging, and ready-task startup keep their existing order. API request branches,
errors, aliases, and foreground model-decision ownership are unchanged.

## Verification

`cargo test -p tachyond --offline` includes messaging characterization tests for
the complete command/reminder envelopes, correlation and cwd, canonical history
and its pending outbox, committed reminder delivery and duplicate acknowledgement,
terminal replay without duplicate results, replay ordering, and stream errors.
