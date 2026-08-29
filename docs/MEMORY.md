# Memory Service Design

Memory is a separate supervised service. Tachyond manages its lifecycle, but
Memory owns the memory files and their consistency. Foreground never
writes Markdown files directly.

```text
Tachyond
  ├── tachyon-foreground
  ├── worker Ghosts
  └── Memory service
        └── Markdown files
```

## Responsibilities

Memory owns:

- Durable task state and dependencies.
- Working conversational context.
- Long-term user profile and preferences.
- Project and task knowledge.
- Event/history records.
- Provenance, timestamps, freshness, and sensitivity metadata.
- Atomic reads and writes.
- The searchable metadata index.

Tachyond owns:

- Starting and supervising Memory.
- Restarting Memory after failure.
- Restricting access to the Memory API.
- Routing Foreground and client requests.
- Starting a fresh interactive Foreground session after daemon restart unless
  the user explicitly requests session resumption.

The Orchestrator owns:

- Proposing memory updates.
- Deciding which context it needs.
- Deciding whether a task should be retained or released.

Memory validates and commits Orchestrator proposals. The Orchestrator cannot
grant itself broader memory access.

## Directory Layout

The initial taxonomy is intentionally small and predictable:

```text
memory/
├── profile/                 # Stable facts about the user
├── preferences/             # User preferences and interaction choices
├── projects/                # Durable project context and summaries
├── tasks/                   # Durable task state, dependencies, and results
├── events/                  # Important chronological observations
├── working/                 # Active sessions and temporary task context
└── archive/                 # Released or superseded entries
```

Categories should not be added casually. A file belongs in the narrowest
category that makes it easy to find later. Active data stays in `working/` or
`tasks/`; compacted durable knowledge moves to the appropriate long-term
category.

## Markdown Format

Memory files use TOML front matter followed by human-readable Markdown:

```markdown
+++
id = "task-..."
kind = "task"
state = "waiting"
created_at = "2026-08-23T13:00:00Z"
updated_at = "2026-08-23T13:05:00Z"
depends_on = ["task-..."]
sensitivity = "normal"
+++

# London Weather

The forecast is pending a worker result.
```

Required metadata should remain small and machine-oriented. The Markdown body
holds explanation, evidence, and human-readable context.

## Task Recovery

Task recovery is the first milestone. On restart, Memory reconstructs:

- Task IDs and objectives.
- Task state: `ready`, `running`, `waiting`, `paused`, `completed`, or `failed`.
- Explicit dependencies.
- Evidence references and result freshness.
- Which tasks can be resumed, recreated, or released.

The Orchestrator can then be replaced without losing logical task continuity.

## Conversation Session Boundary

Interactive conversation history is separate from Tachyond's process lifetime.
The default user experience is a fresh Orchestrator conversation after a daemon
restart. Durable memory and task records are not deleted by that restart.

The future memory protocol should support explicit session operations:

```text
memory.session.create()
memory.session.resume(session_id)
memory.session.clear(session_id)
memory.session.summarize(session_id)
```

This allows Tachyond to restart cleanly while a replacement Orchestrator can
resume a selected conversation from Memory when the user wants continuity.
The TUI should make `new session` and `resume session` visibly different actions.

## Working Memory

Active sessions and temporary context live under `working/`. Working memory is
not automatically promoted to long-term memory. The Orchestrator proposes a
summary or fact update; Memory validates its category, metadata, provenance,
sensitivity, and destination before writing it.

## Access And Consistency

Only Tachyond talks directly to Memory. The initial protocol should support:

```text
memory.read(scope, query)
memory.search(scope, query)
memory.task.create(task)
memory.task.update(task_id, transition)
memory.task.dependencies(task_id)
memory.evidence.attach(task_id, evidence)
memory.propose_update(candidate)
memory.release(task_id)
```

Memory owns all writes and uses temporary files plus atomic rename. It maintains
a small structured index from front matter and supports scoped text search over
Markdown content. No database or graph store is required for the initial
implementation.

## Privacy

Entries carry a sensitivity scope. Memory must filter results before returning
them:

- `normal` — available to the Orchestrator for the relevant session/task.
- `private` — only returned through an explicitly authorized request.
- `user_only` — requires an explicit user-mediated flow.

The TUI should never display private memory merely because it is subscribed to
the Orchestrator stream.

## Implementation Progress

- [x] File layout and TOML front matter parser.
- [x] Atomic task file read/write API.
- [x] Task files with dependencies and state metadata.
- [x] Unix-socket Memory service started and stopped by Tachyond.
- [ ] Structured index and scoped text search.
- [ ] Memory health checks, restart recovery, and typed Tachyond integration.
- [ ] Orchestrator integration for task recovery.
- [ ] Validated long-term memory proposals.
