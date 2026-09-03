# Harness Reliability

Tachyon cannot guarantee successful completion of every possible task. Tasks
can require unavailable credentials, unsupported hardware, inaccessible data,
human approval, or capabilities outside the configured backend. The harness is
reliable when it handles those boundaries predictably rather than pretending
that every objective is achievable.

## Task-Agnostic Contract

The harness must preserve these invariants regardless of the objective's
domain:

1. Every accepted objective retains its original text and stable correlation
   IDs through delegation, execution, evidence, and publication.
2. The model chooses among capability contracts; runtime code does not branch
   on domain nouns such as weather, deployment, package, or report.
3. Each advertised tool invokes one fixed capability. Model arguments cannot
   replace the executable or escape into an unrelated capability.
4. Every model call, tool call, worker wait, and iteration loop is bounded.
5. Every accepted turn reaches exactly one terminal success, failure,
   cancellation, or timeout outcome.
6. Worker output remains private evidence until the conversation role publishes
   one supported answer.
7. Correlation uses conversation, turn, logical task, parent task, and tool call
   IDs rather than event arrival order or process reuse.
8. Replayed events are idempotent. A reconnect or warm-worker reassignment must
   not duplicate replies, evidence, or usage.
9. Unsupported capabilities fail explicitly and identify the missing
   prerequisite. They must not silently become fabricated answers.
10. Metrics report both user-perceived and complete latency, plus self and
    aggregate token usage.

## Capability Classes

Validation should cover capability classes rather than individual demo prompts:

| Class | Examples | Required capability |
|---|---|---|
| Stable reasoning | explanation, comparison, planning | conversation model |
| Computation | arithmetic, parsing, data transformation | IPython backend |
| Workspace work | inspect, edit, build, test | IPython and jailed workspace |
| Web retrieval | documentation, current facts, browser interaction | agent-browser or networked IPython |
| Parallel research | independent sources or components | `spawn_agents` |
| Dependent workflow | inspect, modify, then verify | ordered worker/tool loop |
| Long-running work | builds, migrations, monitoring | retained worker lifecycle |
| Unsupported work | absent credential, device, approval, or network | typed limitation/failure |

Fixtures must use varied neutral objectives. Passing a weather scenario alone
does not establish generality.

## Test Matrix

The deterministic harness suite should exercise every capability class across:

| Dimension | Cases |
|---|---|
| Model result | direct response, tool call, malformed output, empty output, provider error, timeout |
| Tool result | success, nonzero exit, malformed data, timeout, missing executable |
| Worker result | success, partial success, explicit failure, timeout, process exit without terminal event |
| Concurrency | ordered completion, reverse completion, one slow sibling, one failed sibling |
| Lifecycle | fresh worker, warm reuse, reconnect replay, daemon restart, cancellation |
| Conversation | independent turn, dependent turn, answerable follow-up, fresh-work follow-up |
| Persistence | crash before publication, crash after publication, out-of-order pending turns |

Tests should use scripted model and backend implementations with channels or
barriers instead of wall-clock sleeps. Assertions belong on typed events and
terminal state, not prose wording.

## Required Properties

Property and state-machine tests should continuously verify:

- accepted turns terminate exactly once;
- publication occurs at most once;
- private planning never appears as a public reply;
- scheduling decisions never determine answerability or tool access;
- tool programs are fixed by capability contracts;
- dependency identity is explicit and never inferred from completion order;
- aggregate usage is the sum of unique logical assignments;
- timeout and failure paths release waiters;
- commit order remains acceptance order while visible completion may be
  event-time ordered;
- arbitrary objective strings survive delegation unchanged.

## Operational Gates

Before release, CI should require:

1. Workspace formatting, unit tests, doc tests, and build.
2. Scripted scenario tests for every capability class.
3. Fault-injection tests for provider, tool, worker, daemon, and persistence
   boundaries.
4. Replay tests using anonymized production event traces.
5. A capability preflight showing which optional executables and credentials
   are available in the actual worker environment.
6. Soak tests with concurrent turns, worker reuse, and repeated reconnects.

Metrics should track success rate, explicit failure rate, timeout rate,
first-visible latency, completed latency, tool retries, worker reuse, and total
tokens by task class. Regressions should be evaluated by distributions rather
than one successful transcript.

## Current Limits

The current code has strong typed event correlation, bounded model/tool/worker
operations, objective-preserving delegation, parallel workers, replay-safe
usage aggregation, and explicit role/tool contracts. It does not yet provide a
fully extracted deterministic orchestration state machine, explicit dependency
targets among multiple active roots, or revision-safe active-turn attachment.
Those are required before claiming robust arbitrary concurrency and live
replanning.
