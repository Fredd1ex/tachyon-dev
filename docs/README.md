# Tachyon Documentation

Documentation is grouped by the component that owns the behavior. The roadmap
contains milestone-specific target specifications and migration records; these
documents describe the current system and its active design contracts.

## Tachyon

User-facing behavior, shared architecture, interaction, memory, reliability,
installation, and repository status:

- [`tachyon/ARCHITECTURE.md`](tachyon/ARCHITECTURE.md) - runtime ownership and
  component boundaries.
- [`tachyon/INTERACTION.md`](tachyon/INTERACTION.md) - concurrent turns,
  delegation, events, and TUI publication.
- [`tachyon/OPERATIONAL_VIEWS.md`](tachyon/OPERATIONAL_VIEWS.md) - optional
  read-only TODO/resource tabs, bounded caches, and unchanged transcript behavior.
- [`tachyon/WORKSPACES.md`](tachyon/WORKSPACES.md) - host-selected cwd, explicit
  managed work, lazy provisioning, and workspace-matched reuse.
- [`tachyon/MEMORY.md`](tachyon/MEMORY.md) - memory service and persistence
  direction.
- [`tachyon/RELIABILITY.md`](tachyon/RELIABILITY.md) - reliability invariants
  and release gates.
- [`tachyon/STATUS.md`](tachyon/STATUS.md) - implemented, partial, and missing
  behavior.
- [`tachyon/ARCHINSTALL.md`](tachyon/ARCHINSTALL.md) - Arch Linux setup.
- [`tachyon/TREE.md`](tachyon/TREE.md) - source layout and feature locations.

## Interaction

- [`interaction/ARCHITECTURE.md`](interaction/ARCHITECTURE.md) - implemented host
  layout, completed structural refactor, and separate integration follow-up.
- [`interaction/roles.md`](interaction/roles.md) - descriptive role registry,
  Conversation/Coordinator/Campaign capabilities, and prompt compatibility.
- [`interaction/daemon-messaging.md`](interaction/daemon-messaging.md) - extracted
  command, notification, history, and legacy subscription contracts.
- [`interaction/TODOS.md`](interaction/TODOS.md) - durable structured todos,
  exact grants, operator endpoints, and transactional operational feed.
- [`interaction/CAMPAIGN_OVERSIGHT.md`](interaction/CAMPAIGN_OVERSIGHT.md) - explicit
  one-shot background assessment of host-supplied snapshots, not automatic oversight.

## Tachyond

Daemon-owned process, lifecycle, and recovery contracts:

- [`tachyond/DAEMON.md`](tachyond/DAEMON.md) - daemon responsibilities and IPC.
- [`tachyond/MONITORING.md`](tachyond/MONITORING.md) - read-only sources, typed
  monitoring IPC, scope boundaries, and coalesced sampling.
- [`tachyond/LIFECYCLE.md`](tachyond/LIFECYCLE.md) - worker lifecycle,
  retention, leases, and cleanup.
- [`tachyond/REATTACH.md`](tachyond/REATTACH.md) - persistent-worker supervision
  and reattachment.

## Ghost

Worker execution and security boundaries:

- [`ghost/ACCEPTANCE.md`](ghost/ACCEPTANCE.md) - offline scripted real-Ghost repair
  and distributed evidence acceptance, remaining workflow gaps and benchmark authorization.
- [`ghost/CONTINUATION.md`](ghost/CONTINUATION.md) - explicit local snapshot
  continuation, fresh-process claims, configuration checks, and recovery limits.
- [`ghost/WORK.md`](ghost/WORK.md) - core status, durable attention and typed CLI
  answers, resident input waits, and explicit completion proposals.

- [`ghost/HARNESS.md`](ghost/HARNESS.md) - implemented layout, model/tool loop,
  package discovery and activation, tool maintenance, IPython setup, and tests.
- [`ghost/TERMINOLOGY.md`](ghost/TERMINOLOGY.md) - canonical foundation vocabulary
  and current naming/migration boundaries.
- [`ghost/API.md`](ghost/API.md) - current metadata IPC and tool examples,
  separated from proposed admission contracts.
- [`ghost/RESEARCH.md`](ghost/RESEARCH.md) - research foundation draft, ledger and
  budget direction, and attributed external references; not launch authorization.
- [`ghost/PYTHON.md`](ghost/PYTHON.md) - workspace-only Python bridge, native
  authorization, framed protocol, in-memory state, and work-end cleanup limits.
- [`ghost/TODOS_MONITOR.md`](ghost/TODOS_MONITOR.md) - optional native/Python
  todo and monitor tools with independent permit-bound scope grants.
- [`ghost/EXECUTION.md`](ghost/EXECUTION.md) - asynchronous exec, work cleanup,
  bounded output spools, and ctx paging/search with live-work references.
- [`ghost/GROUPS.md`](ghost/GROUPS.md) - durable host-internal campaign groups,
  shared limits, transactional dispatch, terminal acknowledgements, and swarm gaps.
- [`ghost/BROWSER.md`](ghost/BROWSER.md) - restricted Lightpanda browser, eager
  provisioning, URL reading and automation (not a full web search engine).
- [`ghost/SANDBOX.md`](ghost/SANDBOX.md) - workspace access and isolation
  guardrails and proposed isolation policy (not an enforced sandbox).
- [`ghost/SECURITY.md`](ghost/SECURITY.md) - deferred microVM trust boundary,
  host-stability requirements, and pending independent security audit release gate.

## Specifications

- [`../PROJECT.md`](../PROJECT.md) - canonical product vision.
- [`../roadmap/`](../roadmap/) - versioned implementation milestones.
- [`../roadmap/v0.3.0/GHOST_TOOL_RUNTIME.md`](../roadmap/v0.3.0/GHOST_TOOL_RUNTIME.md)
  - accepted generic Ghost tool-runtime requirements.

## Maintenance

- Put cross-component and user-facing documents in `docs/tachyon/`.
- Put daemon-owned process/state documents in `docs/tachyond/`.
- Put worker harness/tool/sandbox documents in `docs/ghost/`.
- Keep target-only milestone specifications in `roadmap/`.
- Prefer relative Markdown links and update `tachyon/TREE.md` when source or
  documentation ownership changes.
