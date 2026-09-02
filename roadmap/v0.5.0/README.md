# v0.5.0 - Memory And Continuity

Estimated progress: **25%**. Status: **Early**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| context-length tracking | Partial | Context fitting and character bounds exist; per-call token-budget accounting and scheduling do not. |
| compaction scheduling | Early | Some history/tool-output compaction exists, but no durable compaction jobs or context epochs. |
| Memory Agent | Missing | The current Memory service is basic task-file CRUD, not a memory-reasoning actor. |
| snapshots | Partial | Conversation and IPython checkpoints exist; complete versioned runtime snapshots do not. |
| generation rollover | Partial | Worker generations guard some stale process updates, but rollover is not a durable end-to-end protocol. |
| research memories | Early | Foreground can retain correlated evidence locally; there is no durable typed research-memory model. |
| failure reuse | Missing | Failures are not indexed and retrieved as reusable evidence. |
| retrieval | Missing | No indexed, scoped user/research-memory retrieval API exists. |
| long-running continuity | Partial | Persistent workers and limited checkpoints exist, but full daemon restart does not restore coherent active work and conversation state. |

## Dependencies

- Requires the v0.4.0 split between `user-memory.redb` and `runtime.redb`.
- User-memory semantics must not place task or agent state in
  `user-memory.redb`.
- Runtime snapshots and generation records remain owned by `runtime.redb`.

## Exit Criteria

- Track actual context use and enforce per-role token budgets.
- Schedule durable compaction with explicit source cursors and context epochs.
- Add typed user-memory proposal, confirmation, correction, revocation, and
  deletion flows.
- Provide indexed, scoped, bounded retrieval with provenance and freshness.
- Store reusable research findings and failures without promoting them into user
  facts automatically.
- Resume long-running sessions from versioned snapshots across daemon restarts.

## Next Work

1. Define user fact, preference, provenance, consent, and revocation records.
2. Add bounded retrieval APIs over `user-memory.redb`.
3. Add runtime snapshot and compaction scheduling over `runtime.redb`.
4. Implement generation rollover and stale-publication tests.
5. Add research-memory and failure-reuse indexes.
