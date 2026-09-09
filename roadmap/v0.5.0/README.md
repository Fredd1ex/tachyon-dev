# v0.5.0 - Memory And Continuity

Estimated progress: **40%**. Status: **In progress**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| context-length tracking | Partial | Per-call context use and configured windows drive scheduling; hard per-role token budgets remain. |
| compaction scheduling | Partial | Tachyond durably schedules at 65% of the configured context window, signals the model owner, and records epoch acknowledgements; semantic summary generation remains. |
| Memory Agent | Partial | The Conversation Agent contextually invokes a bounded private memory tool for recall or typed mutation; Tachyond validates atomic changes and emits inline TUI badges. Ordinary turns have no memory preflight. User-facing inspection remains. |
| snapshots | Partial | Conversation and IPython checkpoints exist; complete versioned runtime snapshots do not. |
| generation rollover | Partial | Worker generations guard some stale process updates, but rollover is not a durable end-to-end protocol. |
| research memories | Early | Foreground can retain correlated evidence locally; there is no durable typed research-memory model. |
| failure reuse | Missing | Failures are not indexed and retrieved as reusable evidence. |
| retrieval | Partial | Redb temporal indexes and policy-filtered lexical preference/history ranking serve bounded typed recall; semantic ranking remains. |
| long-running continuity | Partial | Persistent workers and limited checkpoints exist, but full daemon restart does not restore coherent active work and conversation state. |

## Dependencies

- Requires the v0.4.0 split among `memories.redb`, `history.redb`, and
  `runtime.redb`.
- User-memory semantics must not place task or agent state in
  `memories.redb`.
- Conversation history and temporal activity queries use `history.redb` and do
  not automatically promote records into user memory.
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

1. Add semantic summary generation to the durable compaction protocol.
2. Add user-facing memory inspection and optional confirmation controls.
3. Evaluate hybrid embedding retrieval once lexical ranking has representative scale tests.
4. Implement generation rollover and stale-publication tests.
5. Add research-memory and failure-reuse indexes.
