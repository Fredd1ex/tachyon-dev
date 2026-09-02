# v0.3.0 - Ghost Research Harness

Estimated progress: **35%**. Status: **Partial**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| read / write / edit | Partial | Possible through IPython, but no bounded first-class file tools or edit-conflict contract. |
| ls / glob / grep | Partial | Possible through Python and host tools, but results are unstructured and not harness-controlled. |
| robust exec | Partial | Exit status and timeouts exist; process-tree cleanup, output caps, cancellation, and generic model-facing exec are incomplete. |
| IPython | Partial | Persistent sessions, variable reuse, timeout reset, and serializable checkpoints work. Reliability and fallback behavior remain incomplete. |
| browser | Partial | Browser schema, bootstrap, version checks, and dispatch exist; live research behavior and safety need end-to-end tests. |
| artifacts | Missing | The completion field exists, but workers do not register or preserve typed artifacts. |
| attempts / outcomes / failures | Partial | Lifecycle states and errors exist, but no first-class attempt lineage or typed terminal outcome model exists. |
| RLM-style context access | Missing | Old context can be dropped or compacted but cannot be queried through handles or ranges. |
| checkpoint/resume | Partial | Conversation, worker, and IPython checkpoints exist, but active work cannot resume exactly. |
| structured findings | Partial | Event transport is structured; findings, claims, sources, confidence, and uncertainty remain free-form prose. |
| bounded subagents | Partial | Recursive spawning is prevented by role policy, but fan-out and aggregate resources are not bounded. |
| tool policies | Partial | Role allowlists exist; IPython remains a broad host capability without a hard sandbox. |
| harness benchmarks | Missing | No deterministic research benchmark suite or release threshold exists. |

## Exit Criteria

- Define typed attempts, outcomes, failures, findings, sources, and artifacts.
- Harden execution with process-group termination, output limits, cancellation,
  resource limits, and adversarial tests.
- Add bounded file/search tools and RLM-style context retrieval.
- Preserve registered artifacts before workspace cleanup.
- Bound worker fan-out and total resource consumption.
- Build deterministic harness benchmarks and fault-injection scenarios.

## Next Work

1. Implement process-group timeout and cancellation cleanup.
2. Add typed worker terminal outcomes and attempt IDs.
3. Implement artifact registration and preservation.
4. Add first-class bounded file and search tools.
5. Create deterministic harness scenarios.
