# Operational Views

The existing Agents pane (`Tab`) has two optional read-only tabs: `TODO` and
`RESOURCES`. They appear on its bottom border; use left/right with empty input
to reach them after `MEMORY`, or click their labels in `/mouse capture` mode.
The default conversation, history, response copy, native mouse selection, and
existing pane tabs keep their layout and behavior.

- `TODO` shows flat daemon-owned records for the current conversation. The scope
  comes only from the latest live foreground interaction metadata, not the
  `foreground` agent label, a selected historical turn, or an invented session ID.
  Until that metadata arrives, the scope is unknown and no todo request is made.
- `RESOURCES` shows read-only Host monitoring: campaign-ledger inference and
  funding, charged native wall time, logical retained storage, capacities, and
  registered roles. These are not total chat usage, CPU utilization, or measured
  disk usage. Unknown observations remain distinct from known zero values.
- Up/down scroll the cached rows. `PgDn` requests the next daemon-issued page;
  `PgUp` returns to the first page. Each page holds at most 100 records.
- Edits are deliberately absent. Ask an agent to edit todos through its normal
  tools. These caches confer no production authority and are not prompt content.

## Lifecycle

One lazy background worker serves only the currently visible operational tab.
Closing the pane or switching tabs shuts down its socket, interrupts pending
reads, and invalidates older results. Rendering never connects to the daemon,
requests snapshots, or formats daemon records. A single replaceable result slot
coalesces updates behind at most one outstanding TUI notification. Operational
updates do not mutate transcript items, markdown cache revisions, selection,
focus, history navigation, or copy payloads. The pre-existing one-second general
daemon/agent poll is unchanged.

Todo snapshots carry the scope revision and durable feed watermark from the
daemon's read transaction. The worker subscribes from that watermark, so a
mutation between snapshot and subscription is replayed. Record IDs and revisions
are retained verbatim; exact duplicate current records are ignored. Gaps,
out-of-order revisions, epoch changes, and invalidated pagination cursors request
a fresh snapshot rather than applying uncertain deltas. Recovery returns to the
first page, and the old page remains explicitly stale until replacement.

Monitor subscriptions deliver the current latest snapshot followed by changed
versions. Source sample clocks are displayed; an idle subscription is not itself
a stale sample. A daemon stale signal or disconnect retains the last known data
with a `STALE` label, not invented zero counters. Reconnection attempts are
bounded to one worker with a one-second retry delay. No campaign/work scope is
guessed: this initial monitor view intentionally exposes Host only.

Tests use typed fixtures, socket pairs, and in-memory terminal buffers. They do
not require a running daemon, model call, system clipboard, or interactive UI.
