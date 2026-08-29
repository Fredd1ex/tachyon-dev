# Tachyon TODO

## North Star

Make Tachyon feel like talking to a capable person: responsive, concurrent,
context-aware, interruptible, and low-friction. Infrastructure features must
not get ahead of the interaction model.

## Phase 1: Seamless Interaction

- [ ] Keep the Orchestrator continuously available while background work runs.
- [ ] Accept new user messages while a previous turn is still working.
- [ ] Queue follow-up messages behind active work and acknowledge the wait naturally.
- [ ] Later classify queued follow-ups as related, interrupting, or independent.
- [ ] Preserve conversation context correctly when turns overlap.
- [ ] Prevent concurrent turns from overwriting or losing conversation state.
- [ ] Let the Orchestrator acknowledge work immediately before doing longer work.
- [ ] Stream meaningful progress without exposing noisy implementation logs.
- [ ] Allow the user to interrupt, redirect, or cancel work naturally.
- [ ] Let the Orchestrator decide when a background task is no longer useful.
- [ ] Make worker retention an explicit Orchestrator decision, not an automatic completion rule.
- [ ] Allow the Orchestrator to keep a completed worker/environment alive for later use.
- [ ] Add explicit `inspect`, `await`, `message`, `release`, and `interrupt` worker lifecycle operations.
- [ ] Have Tachyond enforce lifecycle operations while never deciding usefulness on its own.
- [ ] Make worker completion appear as a natural continuation of the conversation.
- [ ] Support proactive updates without making the Orchestrator feel spammy.
- [ ] Add task priorities so urgent user messages can take precedence.
- [ ] Add cancellation and cleanup for abandoned or superseded turns.
- [ ] Add Tachyond-managed await/interrupt control messages and cancellation tokens.
- [ ] Let user and Orchestrator interrupt active workers without exposing raw syscalls.
- [ ] Define graceful stop, timeout, escalation, and forced termination semantics.
- [ ] Keep daemon safety leases as a backstop, while making Orchestrator release the normal cleanup path.
- [ ] Make failures recoverable and explain them conversationally.
- [ ] Preserve ordering between user messages, Orchestrator replies, and worker results.

## Phase 1: Interaction Model

- [ ] Replace string-prefixed control messages with structured events.
- [ ] Make the TUI consume Tachyond state/events without parsing Ghost output markers.
- [ ] Expose normalized actor, turn, task, parent, lifecycle, and tool metadata.
- [ ] Keep Orchestrator and worker visualization as client-side view concerns.
- [ ] Give every session, turn, task, and event a stable ID.
- [ ] Represent background work as children of the conversational turn that created it.
- [ ] Separate user-facing messages from internal lifecycle and tool events.
- [ ] Give the TUI a real task tree instead of reconstructing hierarchy from log lines.
- [ ] Render one coherent Orchestrator conversation with nested activity cards.
- [ ] Collapse routine tool activity by default while keeping progress visible.
- [ ] Show what is currently happening, what is waiting, and what needs the user.
- [ ] Make timestamps and status changes unobtrusive and consistent.
- [ ] Keep input usable while the conversation is streaming.
- [ ] Display the configured provider and model for the Orchestrator in the TUI.
- [ ] Display provider and model metadata for each worker agent, allowing agents to use different LLMs.
- [ ] Add clear interruption, cancel, retry, and dismiss interactions.
- [ ] Add tests for concurrent turns, interleaved worker events, and cancellation.
- [ ] Verify that substantive Orchestrator work always appears as an ephemeral `ghost <uuid>`.

## Phase 1: Memory And Context

- [ ] Define short-term conversation state separately from durable user memory.
- [ ] Prevent overlapping turns from committing stale conversation snapshots.
- [ ] Add context compaction for long-running sessions.
- [ ] Add lazy prompt/tool loading so each model call receives only task-relevant instructions and schemas.
- [ ] Measure prompt-token savings and latency before and after lazy loading.
- [ ] Allow the Orchestrator to use user preferences without exposing internal memory noise.
- [ ] Make task results available to later turns after workers have exited.

## Phase 1: Task-Agnostic Harness Reliability

- [ ] Build a deterministic end-to-end orchestration harness with scripted models, tools, workers, and event sinks.
- [ ] Test every capability class: stable reasoning, computation, workspace work, web retrieval, parallel research, dependent workflows, retained work, and unsupported tasks.
- [ ] Add fault-injection scenarios for malformed model output, provider failure, missing executables, nonzero tool exits, tool timeouts, worker timeouts, and missing terminal events.
- [ ] Require every accepted turn and delegated task to reach exactly one typed terminal success, failure, cancellation, or timeout outcome.
- [ ] Represent worker failures as typed failure evidence rather than successful reusable completion evidence.
- [ ] Carry explicit dependency target IDs when multiple turns are active; never infer dependency from `turn - 1` or event arrival order.
- [ ] Extract orchestration transitions into a model-independent state machine and property-test event-order permutations.
- [ ] Add revision-safe active-turn attachment with buffered publication and atomic ownership so refinements cannot publish stale duplicate answers.
- [ ] Add cooperative replanning safe points before tool launch, after evidence arrival, and before publication.
- [ ] Persist out-of-order pending turns and test crashes before publication, after publication, and during checkpoint writes.
- [ ] Drain or explicitly terminate accepted turns during daemon and Ghost shutdown.
- [ ] Add dependency-failure and shutdown escape paths so queued turns cannot wait forever.
- [ ] Add capability preflight reporting for configured models, credentials, IPython, network access, and backend-visible paths. Browser and Lightpanda preflight are owned by the Ghost worker harness so they can run inside its future microVM.
- [ ] Add replay tests from anonymized real event traces without asserting domain-specific prose.
- [ ] Add concurrent soak tests covering worker reuse, reconnects, reverse completion order, slow siblings, and partial batch failure.
- [ ] Track success, explicit failure, timeout, retry, worker-reuse, first-visible latency, completed latency, and aggregate tokens by task class.
- [ ] Keep task fixtures domain-neutral and verify arbitrary objective strings survive routing and delegation unchanged.
- [ ] Compact or wrap long metric badge rows so values such as aggregate `total` tokens never clip at the viewport edge.
- [ ] Define release thresholds for task success rate, timeout rate, p50/p95 latency, retry rate, and token cost before claiming general robustness.

## Deferred: Security Portals

Do not implement until the interaction milestone is working end to end.

- [ ] Add Tachyond capability requests for filesystem access.
- [ ] Add user approval cards: allow once, allow for task, deny.
- [ ] Add capability leases and expiration.
- [ ] Add network, webcam, microphone, display, and host-command capabilities.
- [ ] Keep policy decisions separate from conversational memory.
- [ ] Ensure the Orchestrator can request access but cannot grant access to itself.

## Deferred: Resources And MicroVMs

Do not implement until the interaction milestone is working end to end.

- [ ] Add resource profiles for CPU, memory, disk, GPU, and runtime versions.
- [ ] Add resource allocation, queuing, and release events.
- [ ] Add Tachyond-owned lifecycle leases and idle timeouts.
- [ ] Add SIGTERM, grace period, and SIGKILL escalation in Tachyond.
- [ ] Add Firecracker-backed execution behind the existing backend abstraction.
- [ ] Support reproducible environments such as Python 3.10 plus ML dependencies.
- [ ] Recover and clean up environments after daemon or Orchestrator failure.

## Deferred: Multimodal Interaction

- [ ] Investigate low-latency audio input and output.
- [ ] Support interruption and backchanneling for voice interaction.
- [ ] Add STT/voice-input composer state with visible recording and processing throbbers.
- [ ] Add webcam/video input only after the portal model is secure.
- [ ] Evaluate time-aligned micro-turns rather than forcing every interaction into turns.

## Acceptance Criteria

The interaction milestone is complete when a user can:

1. Send a second message while the Orchestrator is working on the first.
2. Receive an immediate natural acknowledgement and useful progress updates.
3. Redirect or cancel background work without restarting the session.
4. Ask an unrelated question and receive an answer while the first task continues.
5. See worker activity without reading raw logs or duplicated tool cards.
6. Trust that completed, cancelled, and failed work is represented correctly.
7. Continue the conversation naturally after background results arrive.
