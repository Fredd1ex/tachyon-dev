# Tachyon v0.2.0 Durable State Plan

## Current Problem

Tachyond currently projects every agent into a Markdown file under
`~/.local/share/tachyon/memory/tasks`. Short-lived work creates hundreds of
small files containing duplicated metadata and prose. This causes inode churn,
directory scans, awkward cleanup, and weak transactional guarantees.

The file count is not itself evidence of model-token usage. Token usage only
increases when task documents are selected and serialized into model context.
Instrumentation must distinguish storage volume from context assembly.

## Target

Use redb as the authoritative durable state engine. Markdown agent/task files
are a temporary compatibility store and must be removed from the runtime write
path after migration.

Durable data is split into three ownership and retention domains:

1. **User memory** stores durable facts and preferences about the user, such as
   communication preferences, stable personal context, and explicit choices.
   Records carry provenance, confidence, sensitivity, timestamps, and an
   explicit supersession/revocation path. Runtime cleanup must never delete
   these records merely because a task or worker expired.
2. **History** stores the user-visible record of conversations and completed
   activity. It supports bounded date, conversation, project, task, and artifact
   queries without exposing internal reasoning or raw tool traffic.
3. **Operational memory** stores the state Tachyon needs to schedule and recover
   work: interactions, tasks, workers, events, generations, checkpoints,
   notifications, and commitments. This domain is bounded by lifecycle-aware
   retention and compaction policies.

The domains use separate databases, APIs, context budgets, and
garbage-collection rules:

1. `memories.redb` contains user facts, preferences, provenance, confidence,
   consent, sensitivity, corrections, revocations, and expiry.
2. `history.redb` contains conversations, user-visible messages, activity
   records, durable summaries, temporal indexes, and source references.
3. `runtime.redb` contains agent management, workers, tasks, dependencies,
   leases, generations, attempts, events, schedules, commitments,
   notifications, checkpoints, terminal outcomes, and artifact references.

No transaction may require atomic writes across databases. Runtime transitions
that produce user-visible history append to a durable runtime outbox in the same
transaction. An idempotent projector applies those records to `history.redb`.
User memory is not task state, and conversation history is not automatically
promoted into user memory. Promotion is an explicit validated operation through
the user-memory API.

Suggested tables:

```text
# memories.redb
user_facts:         fact_id -> UserFactRecord
user_fact_index:    (subject, predicate, updated_at) -> fact_id

# history.redb
conversations:      conversation_id -> ConversationRecord
messages:           (conversation_id, sequence) -> MessageRecord
activity:           activity_id -> ActivityRecord
activity_by_day:    (day, activity_id) -> ()
history_sources:    (activity_id, source_id) -> SourceReference

# runtime.redb
tasks:              task_id -> TaskRecord
task_events:        (task_id, sequence) -> TaskEvent
workers:            worker_id -> WorkerRecord
interactions:       turn_id -> InteractionRecord
notifications:      notification_id -> NotificationRecord
commitments:        commitment_id -> CommitmentRecord
generations:        logical_agent_id -> GenerationRecord
snapshots:          (logical_agent_id, generation) -> SnapshotRecord
schedules:          schedule_id -> ScheduleRecord
retention_marks:    record_id -> RetentionRecord
history_outbox:     event_id -> HistoryProjection
```

Large artifacts and user-readable exports remain files referenced by durable
records. The database should not become a blob store for arbitrary tool output.

Canonical databases live under `$TACHYON_DATA_DIR/databases` (normally
`~/.local/share/tachyon/databases`). Project `.tachyon` directories and
`~/.tachyon/exports` are versioned import/export surfaces, not locations for
live database files. Binary databases are not merged or synchronized as
dotfiles; JSONL/TOML exports use stable IDs, hashes, dry-run validation, and
explicit conflict handling.

## Context Compaction

Tachyond owns token accounting and triggers compaction when an agent reaches a
configurable soft threshold, initially 65 percent of its context window. A model
may produce a candidate summary, but Rust validates and installs the resulting
versioned snapshot with its source cursor and generation. Compaction targets a
lower watermark and uses hysteresis so it does not run every turn.

Compaction never deletes canonical conversation history, user memory, or task
events. It replaces only the bounded context projection supplied to an active
agent. The Memory Agent may retrieve memory and propose summaries or durable
facts, but it cannot write runtime state directly or manage all three databases.
Tachyond remains the runtime writer, the history projector owns history writes,
and the Memory service validates curated-memory writes.

## Migration Rules

1. Tachyond remains the only writer for authoritative runtime state.
2. Writes that change state and append the corresponding event are atomic.
3. Records are versioned and migrations are explicit.
4. Startup never scans hundreds of task files after cutover.
5. Existing Markdown task files are imported once, idempotently, then archived
   or removed only with explicit user approval. After cutover, Markdown is not
   an authoritative or writable agent-management store; it may only be a
   derived diagnostic or export format.
6. Memory snapshots remain derived projections, not task-state authority.
7. Context assembly queries bounded records by task/turn IDs; it never lists
   and injects the whole database.
8. Retention and garbage collection are deterministic policies with traceable
   deletion events.
9. User-memory writes record whether the value was stated, confirmed, inferred,
   superseded, or revoked. Inferred candidates require validation policy before
   they become durable context.
10. Context assembly queries user and operational memory independently with
    explicit per-role result and token limits.

## Bounded Growth And Cleanup

Cleanup is owned by Tachyond and runs as bounded maintenance work, never on the
foreground response path.

- Completed short-lived task detail expires first; compact summaries and
  retained artifacts may survive according to policy.
- Active tasks, commitments, pinned records, current generations, and records
  needed by a live checkpoint are not collectible.
- Canonical events are compacted only after a validated snapshot records its
  source cursor. Late events older than that cursor remain detectable and cannot
  mutate the active generation.
- Retention uses explicit age, count, and byte budgets per record class rather
  than an unbounded global history.
- Cleanup is incremental, resumable, idempotent, and emits metrics plus
  traceable tombstone/compaction events.
- Large artifacts remain outside redb and have their own reference-aware
  retention policy.
- User memory supports explicit deletion and correction. Automatic cleanup may
  remove stale inferred candidates, but must not silently erase confirmed user
  preferences.

This separation should improve maintainability through explicit ownership and
schemas, speed through indexed bounded reads instead of directory scans, and
reliability through atomic state/event updates and deterministic recovery. Those
benefits depend on enforcing context budgets and retention; changing the storage
engine alone does not prevent state or token growth.

## Timing

Finish the v0.2.0 command/event ownership boundary first, then define versioned
record schemas from those protocol types. Implement `runtime.redb` as part of
the v0.4.0 kernel before durable scheduling, retries, commitments, or restart
reconciliation are considered complete. Implement `memories.redb`
independently so memory semantics can evolve without coupling user facts to
agent lifecycle transactions.
