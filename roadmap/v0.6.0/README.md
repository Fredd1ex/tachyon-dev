# v0.6.0 - Research Orchestration

Estimated progress: **25%**. Status: **Early**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| better Coordinator | Early | An independently supervised Coordinator semantically verifies worker candidates and recommends lifecycle changes through a fenced, bounded protocol. Research planning and durable scheduling remain. |
| parallel research strategies | Partial | Foreground can launch parallel independent workers, but waits for the complete batch and lacks strategy management. |
| hypothesis / attempt management | Missing | No typed hypothesis or attempt graph exists. |
| cross-worker findings | Partial | Results can be synthesized together, but findings are not incrementally normalized, merged, or deduplicated. |
| steering | Missing | There is no safe active-task steering or cooperative replan path. |
| research task graphs | Early | Basic daemon dependency fields exist, but Foreground cannot express or manage research DAGs. |

## Dependencies

- Requires v0.2.0 typed task ownership and the independent Coordinator boundary.
- Requires v0.3.0 typed attempts, outcomes, findings, sources, and artifacts.
- Requires the v0.4.0 durable scheduler and task dependency graph.
- Uses v0.5.0 research-memory retrieval and failure reuse.

## Exit Criteria

- Run the Coordinator independently through a bounded, lower-priority queue.
- Represent research plans as durable task graphs with explicit dependencies.
- Manage hypotheses, attempts, outcomes, and unresolved questions as typed state.
- Stream successful findings incrementally without waiting for every sibling.
- Merge and deduplicate cross-worker findings while preserving provenance.
- Support safe steering, cancellation, and replanning at explicit safe points.

## Next Work

1. Extend the validated Coordinator protocol from result review to research plans
   and task commands.
2. Implement durable research task graphs.
3. Add hypothesis and attempt records.
4. Publish incremental findings from parallel workers.
5. Add steering and cooperative replanning scenarios.
