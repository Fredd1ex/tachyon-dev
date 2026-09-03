# Implementation Status

This document describes the repository as it exists now. `PROJECT.md` remains
the source of truth for the intended system and MVP requirements.

## Working

- Rust workspace with `tachyon`, `tachyond`, `tachyon-foreground`, `ghost`, and
  shared runtime/API crates.
- Safe Rust enforcement with `forbid(unsafe_code)`.
- User-level daemon startup and single-instance locking.
- Unix-socket NDJSON transport.
- Tachyond-owned Foreground startup under the stable `foreground` identity.
- Tachyond-owned worker startup and subscriptions.
- Daemon-owned interrupt, resume, graceful stop, and forced kill operations.
- Shared OpenRouter-compatible model runtime used by Foreground and Ghost.
- Worker execution through `ipython` and `agent_browser`.
- Concurrent worker calls through `spawn_agents`.
- TUI conversation, scrolling, tool cards, and agent pane.
- Separate `tachyon-tui` crate and shared `tachyon-client` IPC crate.
- Initial `tachyon-orchestrator` domain crate for tasks, attention, conversation,
  scheduler, and control concepts.
- Markdown-first `tachyon-memory` store with TOML front matter, atomic task
  writes, task listing, and a Tachyond-started Unix-socket service.
- Separate scheduling and answerability classification for queued follow-ups,
  with conservative delegation when answerability cannot be established.
- Bounded deferred input and ordered conversation commits.
- Versioned Tachyond-to-foreground commands for multiline user turns and
  correlated worker evidence.
- Conversation state, ordering, checkpoints, streaming, and synthesis owned by
  `tachyon-foreground`; Ghost has no Conversation role.
- Environment-only provider secrets.

## Partial

- TUI is process-separated but still accepts compatibility string markers.
- Interaction commands/events and intents have protocol types, but existing
  decisions and outbound streams are not fully emitted through them yet.
- `InterruptAndReplan` is detected but currently deferred rather than actively
  cancelling work.
- Worker output is visible, but task identity and parentage are inferred rather
  than carried by structured events.
- Agent state exists in Tachyond memory but is not durable across daemon restart.
- Memory is started and stopped by Tachyond, but restart/recovery and typed
  Tachyond-to-Memory integration are not complete.
- Warm worker sessions can be reused by orchestration policy, with default
  keep-alive semantics. Explicit retention commands, durable leases, and
  semantic completion/release events are still pending; the current idle TTL
  remains only a provisional daemon safety fallback.
- Lifetime classes, retention metadata, and the session management view are
  represented in the shared API; richer activity, health, resource, artifact,
  and checkpoint reporting remains pending.
- Short sessions carry a three-turn budget, which is visible in the TUI and
  can be promoted before cleanup.
- Automatic cleanup now stages workers for a grace period first; users can see
  the deadline and request retention or promotion.
- Agent pane/CLI views now expose task type, description, lifetime, persistence,
  and termination TTL metadata.
- Persistent sessions are reported to the recreated Foreground process after daemon
  startup so it can regain control of them.
- Persistent session metadata, workspace reconstruction, Foreground conversation
  checkpoints, and serializable IPython variable recovery now survive a
  Tachyond restart. External handles and complex unserializable objects remain
  explicit artifact/checkpoint responsibilities.
- Local execution uses workspace restrictions, not a hard security boundary.

## Not Implemented

- Full structured event envelopes and a Tachyond-owned lifecycle view model.
- Durable task state, dependencies, artifacts, and recovery.
- Stable session metadata: lifetime class, purpose, owner, retention lease,
  activity, health, checkpoint, and rebuild cost.
- Tachyond-supervised Memory lifecycle and Foreground recovery integration.
- Blocking semantic `await` and unified lifecycle escalation.
- Graceful cancellation with timeout and SIGKILL escalation.
- Real `AgentExec` implementation.
- Real resource usage reporting for `top`.
- Resource scheduling and environment profiles.
- Capability portals and user approval flows.
- Firecracker or equivalent hard isolation.
- Multimodal interaction.

## Priority Order

1. Structured Tachyond events and TUI decoupling.
2. Durable task/session state in Tachyond.
3. Interrupt, await, release, and replan lifecycle operations.
4. Scripted asynchronous interaction tests and metrics.
5. Worker retention, semantic completion, and process recovery.
6. Portals, resource management, and hard sandboxing.

## Verification Gap

The workspace builds and tests, but most crates currently have no behavioral
tests. The next tests should exercise concurrent turns, dependency ordering,
router fallback, interruption, daemon restart, TUI reattachment, and worker
cleanup.
