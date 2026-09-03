# Tachyon Roadmap

Progress snapshot: 2026-09-01.

Estimates measure working, tested behavior rather than types, prompts, or
documentation alone. Each milestone has its own tracker with scope, status,
exit criteria, dependencies, and next work.

## Milestones

| Milestone | Progress | Status | Tracker |
| --- | ---: | --- | --- |
| v0.2.0 Architectural separation | 45% | Partial | [`v0.2.0/`](v0.2.0/) |
| v0.3.0 Ghost research harness | 35% | Partial | [`v0.3.0/`](v0.3.0/) |
| v0.4.0 Tachyond kernel | 18% | Early | [`v0.4.0/`](v0.4.0/) |
| v0.5.0 Memory and continuity | 25% | Early | [`v0.5.0/`](v0.5.0/) |
| v0.6.0 Research orchestration | 25% | Early | [`v0.6.0/`](v0.6.0/) |
| v0.7+ Evaluation and hardening | 15% | Early | [`v0.7.0/`](v0.7.0/) |

The package version is v0.2.0, while the architectural milestone remains
partial. The main blocker is ownership: Foreground still performs work that
should be represented as typed commands and owned durably by Tachyond.

## Storage Direction

- `runtime.redb` becomes the authoritative store for agents, workers, tasks,
  scheduling, events, generations, and lifecycle state in v0.4.0.
- `user-memory.redb` separately owns user facts, preferences, provenance,
  consent, correction, and revocation.
- Markdown agent/task files are removed from the authoritative write path after
  one idempotent import. Markdown may remain only as an export or diagnostic
  projection.
- No transaction requires atomic writes across the two databases.

See [`v0.4.0/`](v0.4.0/) for the implementation milestone and
[`v0.2.0/DURABLE_STATE.md`](v0.2.0/DURABLE_STATE.md) for the detailed storage
contract.

## Critical Path

1. Finish v0.2.0 ownership boundaries and typed work protocol.
2. Add v0.3.0 typed outcomes, findings, artifacts, cancellation, and bounded
   execution to Ghost.
3. Implement `runtime.redb` and stop writing agent state to Markdown.
4. Implement `user-memory.redb` independently with bounded retrieval.
5. Make Tachyond an idempotent durable scheduler with restart reconciliation.
6. Introduce the independent Background Coordinator and research task graphs.
7. Build evaluation, replay, fault-injection, and benchmark gates.
