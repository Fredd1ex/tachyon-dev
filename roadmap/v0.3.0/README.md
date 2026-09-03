# v0.3.0 - Ghost Research Harness

Estimated progress: **75%**. Status: **Partial**.

## Scope

| Item | Status | Notes |
| --- | --- | --- |
| generic tool registry | Implemented | Object-safe Tokio dispatch, startup duplicate rejection, policy filtering, deadlines, cancellation, structured envelopes, output bounds, correlated telemetry events, and bounded durable output storage are integrated into Ghost's model loop. |
| read / write / edit | Implemented | Workspace-confined bounded `read`, atomic create/replace `write`, and exact unique-match `edit` are native and asynchronous. Writes preserve permissions, reject symlink targets, revalidate before replacement, and sync by policy. |
| ls / find / grep | Implemented | Deterministic `ls`, ignore-aware glob `find`, and regex/fixed-string `grep` are native and bounded. `grep` caches `rg` discovery for accelerated candidate filtering with tested native parity and spawn-failure fallback; optional `fd` acceleration remains. |
| robust exec | Partial | Registry-dispatched direct/shell execution now has scrubbed policy environment, bounded concurrent head/tail capture, output references, effective deadlines, cancellation, TERM-to-KILL process-group cleanup, descendant cleanup, and structured outcomes. Streaming work updates, resource limits, and hard isolation remain. |
| artifacts | Implemented | Workers register typed regular-file references with incremental SHA-256, bounded metadata, and assignment provenance. Ghost emits registration events; Tachyond correlates and persists their metadata, while completion results retain registered paths without copying bytes. Directory manifests remain deferred. |
| tool policy and results | Partial | Capability policy, strict decoding, structured errors/results, continuations, hard/model output bounds, durable output references, cancellation, deadlines, and telemetry hooks exist. Bounded reference retrieval and a hard execution sandbox remain. |
| persistent IPython | Partial | Session reuse, timeout reset, and serializable checkpoints work. Bounded rich output, deterministic recovery, lifecycle cleanup, and explicit restore failures remain. |
| quick web retrieval | Missing | No lightweight bounded Rust-native HTTP text/JSON path exists; web retrieval currently requires the browser or IPython. |
| headless Lightpanda research | Partial | The minimal non-Chromium engine has bootstrap, version checks, retries, and registry dispatch with an independent startup-failure boundary. Supervised session reuse, bounded snapshots/downloads, crash recovery, and heavy-research tests remain. |
| attempts / outcomes / failures | Partial | Lifecycle states and errors exist, but no first-class attempt lineage or typed terminal outcome model exists. |
| RLM-style context access | Missing | Old context can be dropped or compacted but cannot be queried through handles or ranges. |
| checkpoint/resume | Partial | Conversation, worker, and IPython checkpoints exist, but active work cannot resume exactly. |
| structured findings | Partial | Event transport is structured; findings, claims, sources, confidence, and uncertainty remain free-form prose. |
| bounded subagents | Partial | Recursive spawning is prevented and one delegation can fan out to at most eight workers; aggregate resources across concurrent turns are not yet bounded. |
| harness benchmarks | Partial | A scripted production-loop fixture now verifies list/find/search/read/edit/exec/artifact behavior without a provider. Broader orchestration scenarios, fault injection, metrics, and release thresholds remain. |

## Exit Criteria

- Implement the eight Rust-native P0 built-ins and immutable generic registry
  defined in [`GHOST_TOOL_RUNTIME.md`](GHOST_TOOL_RUNTIME.md).
- Define typed attempts, outcomes, failures, findings, sources, and artifacts.
- Harden execution with process-group termination, output limits, cancellation,
  resource limits, and adversarial tests.
- Add bounded file/search tools and RLM-style context retrieval.
- Preserve registered artifacts before workspace cleanup.
- Bound worker fan-out and total resource consumption.
- Build deterministic harness benchmarks and fault-injection scenarios.
- Keep persistent IPython, quick web retrieval, and headless Lightpanda research
  as complementary policy-gated tools with independent failure boundaries.

## Documents

- [`GHOST_TOOL_RUNTIME.md`](GHOST_TOOL_RUNTIME.md) - accepted P0 runtime contract
  and boundaries for complementary IPython and web tooling.

## Next Work

1. Add cached optional `fd` acceleration with native parity tests.
2. Add bounded read/search access for durable output references.
3. Add bounded streaming work updates and resource limits to `exec`.
4. Specify and implement the separate bounded quick-web retrieval tool.
5. Extend the deterministic dogfood fixture with completion gates, typed
   attempt IDs, orchestration scenarios, parity tests, and fault injection.
