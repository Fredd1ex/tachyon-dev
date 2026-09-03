# Memory Service Design

Tachyon separates operational state, user-visible history, and curated user
memory into independent redb databases under
`$TACHYON_DATA_DIR/databases` (normally
`~/.local/share/tachyon/databases`).

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

The future Memory Agent proposes summaries and facts but does not receive direct
database access. The Rust Memory service validates its typed proposals. History
is never automatically promoted into user memory.

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
- [ ] Scoped and bounded curated-memory retrieval for agent use.
- [ ] Memory Agent proposal and validation loop.
- [ ] Durable 65-percent compaction scheduling and context epochs.
- [ ] Portable import/export commands.
