# v0.2.0 - Architectural Separation

Estimated progress: **45%**. Status: **Partial**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| Interaction Manager | Partial | Turn admission, ordering, typed commands, and publication exist inside `tachyon-foreground`; there is no separately authoritative deterministic manager. |
| Conversation Agent separation | Partial | Conversation is separate from Ghost, but remains fused with interaction management and synchronously executes delegation. |
| Coordinator separation | Partial | Tachyond independently supervises `tachyon-background`, submits completed candidates through a bounded queue, fences decisions by generation and assignment, and fails closed. Durable scheduling and broader task planning remain. |
| `WorkRequest` / `WorkEvent` / `WorkResult` | Partial | Typed daemon-to-worker assignments, progress, candidate review, terminal outcomes, work-scoped replay, and in-process idempotency exist. Durable records remain. |
| Typed events | Partial | Interaction and agent envelopes exist, but legacy string markers and untyped payload paths remain. |
| Foreground fast path | Partial | Simple turns can complete quickly, but one-request behavior and direct-operation routing are not enforced. |
| Basic Tachyond ownership | Partial | Tachyond owns processes and lifecycle metadata, but not authoritative durable task scheduling and event reduction. |

## Exit Criteria

- Separate deterministic interaction management from bounded Conversation model
  execution.
- Make task intents daemon-consumed, idempotent commands rather than observational
  events followed by direct Foreground execution.
- Introduce the typed `WorkRequest -> WorkEvent* -> WorkResult` worker contract.
- Remove semantic dependence on line markers and string parsing.
- Run the Background Coordinator independently through a bounded queue.
- Add acceptance tests for greeting, direct operations, concurrent follow-up,
  worker failure, cancellation, and stale-generation rejection.

## Documents

- [`INVARIANTS.md`](INVARIANTS.md)
- [`ORCHESTRATION_PROTOCOL.md`](ORCHESTRATION_PROTOCOL.md)
- [`UI_PROTOCOL.md`](UI_PROTOCOL.md)
- [`MIGRATION.md`](MIGRATION.md)
- [`DURABLE_STATE.md`](DURABLE_STATE.md)

## Next Work

1. Make typed task intents authoritative and idempotent in Tachyond.
2. Move work idempotency and terminal replay from the in-memory registry into
   `runtime.redb`.
3. Persist Coordinator requests, decisions, and verified terminal transitions in
   `runtime.redb`.
4. Remove remaining synchronous worker ownership and compatibility events.

This milestone must stabilize command/event ownership before the v0.4.0 redb
runtime-state cutover.
