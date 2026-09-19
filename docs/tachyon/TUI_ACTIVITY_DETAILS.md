# Inline Turn Activity

Primary monitoring is part of each conversation cell, below its badges and before
the response. It is not an ACTIVITY popup and never repeats the prompt or answer.
The resting surface shows at most three single-line task rows and one explicit
`+N more` row. Nested tools and the redundant `spawn_agents` call are hidden when
task rows exist. Direct tools still have compact rows without requiring a worker.
Titles are bounded, single-line first-clause previews of the recorded objective,
not inferred task semantics or domain-specific rewrites. Status and known timing
are right-aligned; titles yield space first, using terminal-cell widths and an
ellipsis. IDs, raw prompts, output, and separate action lines are not default UI.

On final publication the default becomes one task-group summary. An explicitly
opened detail view stays open through publication; rendering never changes the
user's expansion decision. Streaming text alone is not terminal evidence.

## Controls

- `Ctrl+O` toggles structured details inline for the selected/current viewport turn.
- Clicking a turn header in `/mouse` capture mode does the same.
- Clicking a compact task row opens its correlated worker details. There is no
  separate `view subagent` line in the default view. Keyboard users can use
  `Ctrl+O`, then `Ctrl+D` and Up/Down plus Enter/Space to inspect a selected record.
- PageUp/PageDown, empty-input arrows, End and captured wheel reports navigate the
  conversation. Scrolling never opens details; the detached turn anchor is retained.
- While inline details are open, `Ctrl+D` toggles the secondary diagnostics inspector.
- In that inspector, Up/Down selects a record; Enter/Space or a header click toggles
  that record. `r` separately opts into bounded, redacted raw output; `d` toggles
  diagnostic metrics. Page keys, End and captured wheel reports scroll the inspector.
- Esc closes the current details/inspector before a subsequent Esc quits.
- Copy remains prompt/answer-only. Inline details do not capture ordinary typing.

Structured sources, results and errors precede arguments. Sources are text, not
clickable URLs. Raw diagnostics remain explicit and secondary, not the primary
monitor. Routine successful command receipts no longer replace the footer; their
existing diagnostic records are retained. Errors still produce notices.

## Scope And Timing

`panels/activity.rs` supplies the same exact-turn projection to the inline layout
and diagnostics. Worker tool evidence requires persisted host work identity,
including generation/assignment; no current worker task or neighboring turn is
used to infer ownership. Correlated host spawn records can supply live task names.
Unscoped worker tool records are omitted.

Spawn timing occupies the same single row as its title and status. The main badge
shows the aggregate live tool count without repeating tool names. Terminal work
uses host execution duration when known; detailed views retain recorded tool-call
counts. Recorded counts describe retained evidence, not an assumed lifetime total.
Missing timing is omitted, not zero. Terminal and archived task timers freeze.
Repeated legacy spawn identities cannot establish assignment ownership; their
per-task live counts/timers are omitted rather than attached to a reused worker.
A terminal turn without a recorded task outcome says `outcome unknown`, not success.

Starts and terminal results merge only on exact identity within the selected
turn. Current host work evidence supersedes starts; the newest generation and
assignment supersede older evidence in default monitoring. Historical assignments
remain available in diagnostics. A result on the exact worker thread and turn can
map a worker ID to a different work ID only when that work attribution is unique;
objective text and the worker's mutable task/name are never identity joins.
Worker completions without host turn correlation
are not attached by searching neighboring turns or worker-ID substrings.

Explicit inline expansion shows at most eight diagnostic records per cell, with
an omission notice and a path to the full inspector. Structured detail formatting is deferred
until expansion and retained in the cell layout cache. Clock ticks compose fixed
timer slots, without reformatting Markdown or changing row counts/hit targets.
Worker revisions invalidate their correlated cells. Width changes rewrap content.
The secondary inspector retains its independent per-record lazy detail cache.

## Boundaries

Evidence is untrusted. Previews remove terminal controls, redact credential-shaped
fields/text, strip URL credentials/query/fragment, bound JSON depth and collections,
and cap detail text/rows. Unstructured or oversized legacy arguments/output are
withheld. Raw output requires a second explicit action; stored evidence is unchanged.
Redaction is defensive, not a guarantee that arbitrary prose contains no private data.

Tabs share one data-driven top header, wrapping when necessary; TODO and RESOURCES
are no longer on the lower border. Painting and mouse targets use the same terminal
cell widths. Header tabs are text-only blocks: black on cyan when selected and
white on dark gray when inactive. Username backgrounds, existing badge icons and
the top-left decoration are unchanged. Extremely short/narrow panes can clip tabs/content;
Left/Right still traverses the full tab registry.

## Host Checklists

The selected live cell, or newest active cell when nothing is selected, can show
an authoritative work checklist before its activity and answer. Work identity must
come from validated, fenced host activity or an exactly turn-correlated work
record. Worker names, spawn text, objectives, tool counts and neighboring turns
are not joins. Multiple work IDs require an explicit matching work selection;
the TUI does not choose an arbitrary worker or subscribe to every worker.

At most three actual titles are shown, with `[ ] pending`, `[>] active`,
`[!] blocked`, `[x] complete`, or `[-] cancelled`. `+N more` counts omitted records
in the bounded page; `or more` explicitly marks an unknown total beyond that page.
The TODO tab follows the exact selected work scope, with PgDn/PgUp pagination,
or the live conversation when no exact work is linked. It remains read-only.
Agent-created work todos use the same host service as operator-created todos.
The TUI never creates tasks, infers todo completion, invents a percentage, or
interprets completed todos as accepted worker output or a completed conversation.

The host's `TodoScope` has conversation, work and campaign variants, but `Todo`
has no turn field. Conversation-wide tasks therefore never appear on individual
cells, including unrelated follow-ups such as jokes. Without an exact work link,
opening the existing FOREGROUND pane shows a small **Global conversation checklist**,
explicitly labeled **not turn-linked**. The current TUI work correlation does not
provide a campaign-to-turn link, so campaign ownership is not guessed. Supporting
automatic campaign/turn checklists requires that host correlation contract, not
a client-side todo engine. Legacy unfenced activity is likewise insufficient.

One existing lazy snapshot/subscription worker serves the current demand: a
maximum 100-record page and a bounded 16 MiB transport frame. Hidden unrelated
scopes are not polled. An idle operational feed waits for pushes, not periodic
snapshots. Switching scope, cell, tab, or hiding the global pane cancels obsolete
reads; the exact cell binding participates in the worker generation fence.
Coalesced notifications carry only the latest view. Feed gaps trigger a first-page
resnapshot; older same-epoch snapshots cannot replace newer revisions/watermarks.

A successful empty snapshot adds no inline or global checklist. Loading is
unknown; disconnects/errors are unknown/stale, retaining any last known titles
rather than pretending the scope is empty. Checklist changes invalidate only
the linked cell (and the previous cell when selection moves), not unrelated
Markdown. Width changes recompute bounded rows; unchanged updates and clock ticks
reuse layouts. Checklists are ephemeral: they are neither archived nor included
in prompt/answer copy. Compact activity rows, existing tabs and diagnostics remain.

Offline verification: `cargo test -p tachyon-tui --offline` exercises scoping,
empty/error distinction, snapshot order, cancellation/coalescing, host statuses,
copy isolation, narrow layouts and cell-local invalidation. No daemon restart,
provider invocation, or paid run is needed.
