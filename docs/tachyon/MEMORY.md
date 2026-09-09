# Memory Service Design

Tachyon separates operational state, user-visible history, and curated user
memory into independent redb databases under
`$TACHYON_DATA_DIR/databases` (normally
`~/.local/share/tachyon/databases`).

Tachyond opens and validates all three stores during startup. If a database file
is absent it is recreated with the current empty schema; incompatible or corrupt
existing files fail startup rather than being overwritten.

```text
Tachyond -> runtime.redb
         -> durable outbox -> history projector -> history.redb
Memory service -> memories.redb
```

## Ownership

- Tachyond is the only writer for `runtime.redb`.
- The idempotent history projector is the only writer for `history.redb`.
- The Memory service validates and commits writes to `memories.redb`.
- Foreground, Background, and Ghost never open database files directly.
- No operation assumes an atomic transaction across database files.

Runtime stores tasks, workers, generations, assignments, transition events,
leases, snapshots, and the history outbox. Each committed task transition also
enqueues a typed task-history projection in the same runtime transaction.
History stores canonical conversation messages, notifications, immutable task
events, and interval summaries, indexed by timestamp and UTC day. Conversation
messages and summaries additionally use a conversation index. Streamed deltas,
reasoning, tool traffic, and transient status are not history.

Curated memory records contain:

- Stable record ID
- Kind: fact, preference, constraint, goal, routine, or relationship
- Subject, namespace, relation (stored as `predicate`), scope, and value
- Cardinality (`one` or `many`) and zero or more topics
- Provenance
- Confidence
- Sensitivity and consent
- Creation and update timestamps
- Optional expiry and superseded record
- Durable revocation marker

Stable IDs are idempotent. Reusing an ID for different content fails instead of
silently rewriting user memory. Revoked records are excluded from reads but
retain a tombstone so stale imports cannot resurrect them.
Remembering a different value for the same kind, namespace, relation, and scope
automatically performs an atomic superseding correction when cardinality is
`one`; cardinality `many` preserves distinct active values.

The memory database also contains a `primitive_catalog` table. A fresh database
transactionally seeds the supported kinds, scopes, cardinalities, and common
namespaces. These 19 schema primitives describe the vocabulary and are not user
memories: deleting or moving `memories.redb` recreates the catalog with zero
curated records. Memory records use payload schema version 2; legacy payloads
decode through conservative uncategorized defaults while the redb container
schema remains version 1 because the catalog is an additive table.

## Chronological History

The runtime database remains authoritative for current task state. Its durable
outbox carries conversation and task projections to the history projector. An
outbox event is acknowledged only after the idempotent history transaction
commits, so restart replay cannot lose or duplicate a transition.

History entries have an explicit kind: `conversation`, `task`, or
`conversation_summary`. Task entries preserve task ID, resulting state, stable
sequence-derived event ID, timestamp, objective, and transition note. They are
chronological facts, not mutable task snapshots and not curated user memory.

Every 20 canonical conversation messages, the history projector derives a
bounded deterministic digest of that interval and writes it as a
`conversation_summary`. Its ID is based on the conversation and ending message
count, making retries idempotent. Summary records are indexed with the
conversation and temporal activity but do not increment the source-message
count or recursively create summaries. These durable history digests are
separate from model-generated runtime context-compaction snapshots.

## Memory Agent

The Conversation Agent owns a contextual `memory` capability with `recall`,
`remember`, `forget`, and `correct` actions. It decides during its existing model
turn whether durable context is materially relevant; ordinary greetings and
context-free requests perform no memory IPC and require no separate memory model
pass. This decision is semantic rather than activated by canned phrases.

Recall delegates a compact model-written objective to Tachyond with an explicit
decision about whether conversation and task history are relevant. Tachyond
enforces item and character limits and returns untrusted bounded records. A
forget or correction may target only exact IDs returned by an earlier recall in
the same turn, and at most one mutation may be attempted per accepted turn.
Tachyond derives provenance, consent, timestamps, and IDs; the Rust Memory store
validates and atomically commits the typed proposal.

Memory tool calls and results exist only in the ephemeral per-turn model
projection. Their arguments and returned private context are omitted from
generic tool telemetry, checkpoints, and canonical history. Foreground waits for
the authoritative mutation result before allowing a response to claim that
memory changed. The TUI retains the generic `working...` placeholder and shows
count-only saved, recalled, forgotten, corrected, or unchanged badges beside
timing and token usage.

Deleting `memories.redb` removes curated user memory but does not rewrite
canonical conversation history. A Tachyond restart starts Foreground with a new
conversation checkpoint and turn sequence, so stale active transcript context
does not survive that restart. Historical statements remain non-authoritative
for stored-profile questions: the Conversation Agent must consult Memory with
history disabled, and an empty result means no durable profile record is
available.

### Retrieval

Retrieval currently combines redb indexes with deterministic lexical ranking:

- `memories.redb` supplies consent-, sensitivity-, revocation-, and expiry-aware
  preference candidates.
- `history.redb` supplies bounded timestamp ranges and at most 200 recent
  conversation, task, and summary candidates for lexical ranking.
- `runtime.redb` supplies durable task objectives and terminal lifecycle state
  when the Conversation Agent requests task-history context.
- Tachyond clamps result count and total characters before crossing IPC.

Tantivy is intentionally not used yet. At current data volumes, another index
would add synchronization and recovery complexity without improving structured
preference or date retrieval. The typed recall API isolates the ranker so a
future hybrid implementation can add embeddings and semantic similarity, or
Tantivy for larger full-text corpora, without changing Foreground or the TUI.

Tachyond owns context token accounting. At the configurable soft threshold,
initially 65 percent, it schedules compaction. A model may generate a candidate
summary, but Tachyond validates and installs it as a versioned runtime snapshot
with a source cursor and context epoch. Compaction never deletes canonical
history or curated memory.

## Portability

Live redb files are not dotfiles and must not be merged while open. Versioned
JSONL/TOML exports belong in `~/.tachyon/exports` or a project's `.tachyon`
directory. Imports use stable IDs, hashes, schema validation, dry-run reporting,
and explicit conflict handling.

## Progress

- [x] Authoritative `runtime.redb` task snapshots and transition events.
- [x] Indexed persistent-worker recovery without Markdown scans.
- [x] Durable runtime history outbox with idempotent acknowledgement.
- [x] Versioned `history.redb` conversation and temporal indexes, chronological
  task projections, and deterministic 20-message interval summaries.
- [x] Categorized version-2 `memories.redb` records, primitive catalog bootstrap,
  conflict checks, and revocations.
- [x] Bounded daemon-owned temporal history query API and CLI.
- [x] Scoped and bounded model-selected memory and history retrieval without a
  per-turn preflight request.
- [x] Strict semantic remember, forget, correction, normalization,
  deduplication, validation, and TUI lifecycle badges; the managed service
  identity is visible in agent listings.
- [ ] User-facing memory inspection and confirmation controls.
- [x] Durable 65-percent compaction scheduling, 45-percent target, context
  epochs, typed signals, and acknowledgements.
- [ ] Memory-Agent-generated semantic summaries during compaction.
- [ ] Portable import/export commands.
