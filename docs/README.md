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
- [`tachyon/MEMORY.md`](tachyon/MEMORY.md) - memory service and persistence
  direction.
- [`tachyon/RELIABILITY.md`](tachyon/RELIABILITY.md) - reliability invariants
  and release gates.
- [`tachyon/STATUS.md`](tachyon/STATUS.md) - implemented, partial, and missing
  behavior.
- [`tachyon/ARCHINSTALL.md`](tachyon/ARCHINSTALL.md) - Arch Linux setup.
- [`tachyon/TREE.md`](tachyon/TREE.md) - source layout and feature locations.

## Tachyond

Daemon-owned process, lifecycle, and recovery contracts:

- [`tachyond/DAEMON.md`](tachyond/DAEMON.md) - daemon responsibilities and IPC.
- [`tachyond/LIFECYCLE.md`](tachyond/LIFECYCLE.md) - worker lifecycle,
  retention, leases, and cleanup.
- [`tachyond/REATTACH.md`](tachyond/REATTACH.md) - persistent-worker supervision
  and reattachment.

## Ghost

Worker execution and security boundaries:

- [`ghost/HARNESS.md`](ghost/HARNESS.md) - Ghost model/tool loop and execution
  backend.
- [`ghost/SANDBOX.md`](ghost/SANDBOX.md) - workspace access and isolation
  policy.

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
