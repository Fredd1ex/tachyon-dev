# v0.4.0 - Tachyond Kernel

Estimated progress: **24%**. Status: **Early**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| durable scheduler / cron | Partial | Durable reminders and agent `start_at`/`finish_by` work support local or relative deadlines, restart recovery, and retryable modeled delivery; recurrence and schedule management remain. |
| commitments | Missing | Present only in design documents. |
| resource scheduling | Missing | No resource requests, capacity model, admission control, or allocation ledger. |
| worker supervision | Partial | Tachyond owns processes, assignments, generations, three-turn Short workers, daemon-bound Long workers, and Persistent supervisor reattachment; health and restart policy remain incomplete. |
| retries | Missing | Connection polling exists, but no durable attempt policy, backoff, or retry classification. |
| cancellation escalation | Partial | Work deadlines produce typed timeout outcomes, fence stale generations, and terminate the worker; cooperative cancellation and shared TERM-to-KILL escalation remain. |
| task dependencies | Partial | Dependency fields and basic admission gating exist; graph validation and failure propagation do not. |
| persistent state | Partial | `runtime.redb` is authoritative for task writes and indexed persistent-worker restore; history, memory, and remaining runtime records are still required. |
| restart reconciliation | Partial | Some persistent workers can be reconstructed, but stale sockets, orphan work, event replay, and foreground recovery are unresolved. |
| priority queues | Missing | Priority types are not connected to task scheduling. |
| better tracing | Partial | Correlated typed events and JSONL logs exist, but durable causal tracing and replay do not. |

## redb Storage Requirement

v0.4.0 replaces Markdown agent tracking with three separate redb databases:

- `memories.redb` owns durable user facts, preferences, corrections,
  provenance, confidence, consent, sensitivity, revocation, and expiry.
- `history.redb` owns conversations, user-visible activity, temporal indexes,
  durable summaries, and source references.
- `runtime.redb` owns agents, workers, tasks, dependencies, leases,
  generations, attempts, events, schedules, commitments, notifications,
  checkpoints, terminal outcomes, and artifact references.

Tachyond is the sole writer for authoritative runtime and agent-management
state. A runtime outbox and idempotent projector feed `history.redb`. The Memory
service owns curated-memory writes behind a typed API. No transaction may
require atomic writes across databases. Promotion from history into user memory
is an explicit, validated operation.

Markdown must not remain an authoritative or writable agent/task store after
cutover. Existing Markdown task records may be imported once, idempotently, and
then archived or deleted with user approval. Markdown may remain only as an
export or derived diagnostic projection.

Detailed storage requirements are in
[`../v0.2.0/DURABLE_STATE.md`](../v0.2.0/DURABLE_STATE.md).

## Exit Criteria

- Versioned redb schemas and explicit migrations.
- Atomic task transition plus event append in `runtime.redb`.
- Idempotent command keys and stable event cursors.
- Indexed startup reconciliation without directory scans.
- Deterministic retention, compaction, and tombstone records.
- Crash-consistency, migration, restart, and stale-generation tests.
- Removal of runtime calls to the Markdown `WriteTask`, `ReadTask`, and
  `ListTasks` authority path.
- Durable scheduler, retries, cancellation escalation, dependencies,
  commitments, priority, and tracing operate from runtime state.

## Next Work

1. Finalize runtime records from the v0.2.0 typed protocol.
2. Implement `runtime.redb` and one-time Markdown import.
3. Move task/agent reads and writes off Markdown.
4. Build restart reconciliation on durable events and snapshots.
5. Extend scheduled work with recurrence, list/cancel management, and explicit misfire policies.
