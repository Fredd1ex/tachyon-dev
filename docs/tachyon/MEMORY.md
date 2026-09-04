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
leases, snapshots, and the history outbox. History stores only canonical
user-visible messages and notifications, indexed by conversation, timestamp,
and UTC day. Streamed deltas, reasoning, tool traffic, and transient status are
not history.

Curated memory records contain:

- Stable record ID
- Subject, predicate, and value
- Provenance
- Confidence
- Sensitivity and consent
- Creation and update timestamps
- Optional expiry and superseded record
- Durable revocation marker

Stable IDs are idempotent. Reusing an ID for different content fails instead of
silently rewriting user memory. Revoked records are excluded from reads but
retain a tombstone so stale imports cannot resurrect them.

## Memory Agent

The daemon-managed Memory Agent observes authoritative accepted user turns.
Direct preference constructions such as `I prefer ...` are converted into typed
records with statement provenance and explicit consent, then validated by the
Rust Memory store. Ordinary conversation and task history is never promoted
into curated memory.

Before each model turn, Foreground requests a bounded private recall projection
from Tachyond. Active normal-sensitivity preferences are ranked and returned.
Past conversation activity is searched only when the prompt asks about earlier
work or a temporal period. Recalled data is attached to the ephemeral model
input as untrusted context; it is never added to checkpoints, canonical history,
or model-visible tool protocols. The TUI receives count-only `memory_saved` and
`memory_recalled` lifecycle events.

### Retrieval

Retrieval currently combines redb indexes with deterministic lexical ranking:

- `memories.redb` supplies consent-, sensitivity-, revocation-, and expiry-aware
  preference candidates.
- `history.redb` supplies bounded timestamp ranges and at most 200 recent
  candidates for lexical ranking.
- `runtime.redb` supplies durable task objectives and terminal lifecycle state
  for prompted task-history recall.
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
- [x] Versioned `history.redb` conversation and temporal indexes.
- [x] Versioned `memories.redb` records, conflict checks, and revocations.
- [x] Bounded daemon-owned temporal history query API and CLI.
- [x] Scoped and bounded curated-memory and prompted history retrieval.
- [x] Explicit user-preference observation, validation, and TUI lifecycle
  indicators; the managed service identity is visible in agent listings.
- [ ] Model-proposed facts and preference corrections beyond explicit statement
  constructions.
- [x] Durable 65-percent compaction scheduling, 45-percent target, context
  epochs, typed signals, and acknowledgements.
- [ ] Memory-Agent-generated semantic summaries during compaction.
- [ ] Portable import/export commands.
