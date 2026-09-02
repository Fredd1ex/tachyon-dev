# Tachyon v0.2.0 Invariants

## Purpose

Tachyon is a deterministic event-driven runtime with a small number of LLM
reasoning nodes. LLMs produce semantic decisions. Rust owns state, ordering,
scheduling, persistence, lifecycle, and side effects.

These invariants are architecture constraints, not aspirations. New code and
migrations must preserve them.

## Ownership

1. Every mutable state domain has exactly one authoritative owner.
2. Tachyond owns durable task state, schedules, process lifecycle, retries,
   cancellation, and recovery.
3. The Interaction Manager owns user-visible turn ordering, interruption,
   notification timing, and duplicate suppression.
4. The Task State Manager owns authoritative task state. An LLM may suggest a
   task command but cannot assert that a task is running, complete, or failed.
5. The Generation Manager owns context-epoch state and atomic rollover.
6. The Context Assembler is deterministic and owns bounded context selection.
7. The Memory Agent may produce candidate snapshots, but never owns canonical
   memory or authoritative events.

## LLM Boundaries

8. The foreground interaction path may require at most one LLM request.
9. No LLM synchronously waits for another LLM.
10. The Conversational Agent may emit structured intents, but does not start
    processes, write durable state, schedule work, or execute tools.
11. The Background Coordinator emits semantic task decisions and updates. It
    does not speak directly to the user or manipulate the UI.
12. Ghost is a worker harness. It does not import conversational or
    coordination policy.
13. A Memory Agent is ephemeral. It runs only for compaction or consolidation
    jobs and is never needed for ordinary foreground responses.

## Foreground Responsiveness

14. Foreground user input has priority over background work.
15. An independent user turn begins processing immediately, even while workers,
    coordinator work, compaction, or browser work is in progress.
16. A dependent turn reads the latest committed facts and task state; it does
    not wait for unfinished work unless the user explicitly asks to wait.
17. Background failure, delay, or cancellation cannot freeze foreground
    conversation.
18. Direct deterministic operations bypass Ghost while still emitting typed
    events, supporting cancellation, and being traceable.

## Async And Concurrency

19. No mutex, database transaction, or ownership-critical guard is held across
    an await point.
20. All queues are bounded and have explicit overload behavior.
21. Every request expecting a response has a deadline and a terminal timeout
    result.
22. Retry policy is bounded, operation-specific, and observable.
23. Cancellation is cooperative first and has an explicit escalation policy.
24. Agent/context generations are disposable. Late events from an obsolete
    generation cannot modify the active generation.

## Events And Commands

25. All asynchronous commands and events carry stable correlation IDs.
26. Important commands are idempotent through command IDs or idempotency keys.
27. Event consumers tolerate duplicate, late, and out-of-order delivery.
28. A command records intent; an event records an observed fact. The two are not
    interchangeable.
29. User-visible messages are derived by the Interaction Manager from ordered
    interaction events, not written directly by background components.

## Data, Memory, And Security

30. Durable state is reconstructible from canonical events and deterministic
    projections.
31. Memory snapshots are versioned projections over canonical events.
32. Context assembly has explicit token budgets per role and does not include
    unrelated tool schemas, prompts, or task history.
33. Credentials never enter prompts, event payloads, worker subprocess
    environments, debug output, or configuration files.
34. Provider routing, model metadata, usage, cost, and timing are shared runtime
    concerns, not owned by a particular LLM role.

## Observability

35. Each model request records role, model, provider when available, routing
    policy, input/output tokens, queue duration, request duration, first byte,
    first token, first rendered token, and terminal status.
36. Every background operation is traceable to its initiating user turn or a
    durable scheduled job.
37. Trace output is structured enough to reconstruct causal ordering without
    relying on log text.

## Required Regression Scenarios

38. A greeting uses one Conversation request and invokes no worker, coordinator,
    memory job, or tool.
39. A direct weather lookup does not create autonomous Ghost workers.
40. A follow-up submitted during a long background task reaches the foreground
    request lane without waiting for that task to finish.
41. Killing a worker, coordinator, TUI, or daemon cannot deadlock foreground
    conversation.
42. Restart, duplicate command, late event, cancellation, and context rollover
    behavior are covered by replay or fault-injection tests.
