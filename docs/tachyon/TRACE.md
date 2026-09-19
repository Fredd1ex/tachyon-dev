# On-Demand Worker Evidence

This UI view remains bounded terminal evidence. Private campaign launches also
retain separate [durable tool trace resources](../ghost/TRACE.md), retrievable
through permit-scoped `history` without flooding this view. Those resources are
not a TUI archive or a restorable Python session.

Mouse capture is off by default so ordinary drag and terminal Copy work natively.
Task/tool monitoring is inline. Use `Ctrl+O` to expand structured turn details and
then `Ctrl+D` for the secondary diagnostics inspector. Empty-input arrows and Page
keys navigate without opening details. The wheel is terminal-owned by default, with scrollback
behavior depending on the terminal's alternate-screen settings.

For clickable details, enter `/mouse` to opt into capture, then open a conversation
turn's explicit `view subagent` action, then click a tool evidence header. The same
header collapses the cell. Inside an open cell, click **Raw diagnostics** to
show or hide diagnostic details for that item independently. Closing the worker
or trace hides its evidence even if a tool cell was previously expanded. Raw
diagnostic expansion is local UI state and is not saved in sessions.

`/mouse` again restores native selection. Help (`?` or `Ctrl+P`) shows the current
mouse state. Capture owns ordinary clicks/drags; Shift+drag may bypass it in some
terminals but is not portable. Native selection and TUI clicks cannot both own
the same crossterm event. `y` copies the selected/latest whole conversation cell;
native `Ctrl+Shift+C` belongs to the terminal and is ignored if forwarded in
native mode. Only capture mode allows forwarded `Ctrl+Shift+C` to copy a cell.
Tachyon cannot inspect or extract native selected text. Turn selection does not
paint a gray header/body background; trace mode and expansion indicators remain.

Worker evidence arrives with the terminal `WorkResult`, not through a new live
subscription. Headers show the tool name and compact recorded status (for example
`exit0` when measured), plus actual errors, timeouts, or truncation. Expanded cells
show **Code** and **Output** once, preserving code indentation and blank lines.
Absent/null sections, duplicate arguments, IDs, and native envelopes are omitted
from this default view. Actual error details precede bulk code/output.
Raw diagnostics adds assignment identity, present call/parent IDs, references,
continuation, arguments, and the native output envelope (including metadata and
null fields as recorded). A recorded result is not a claim that the tool or
assignment succeeded.

The backend evidence budget is 32 tool results and approximately 16 KiB per
assignment. Arguments and outputs can have smaller individual budgets. Worker
details show omitted-result counts; absent evidence is not proof of success.
There is no full-output retrieval or promise of a complete execution log here.
Structured evidence and assignment identity are retained in saved TUI sessions;
older sessions and results without evidence remain readable.

The producer budget is not a deserialization limit: arbitrary legacy/persisted
JSON and result counts can still consume unbounded ingestion/session memory.
Expanded terminal tool cells independently cap each serialized section at 2 KiB
(serialization stops at the budget), and render at most 128 detail lines / 16 KiB
of detail text (including the raw toggle row in the line cap), plus a truncation
notice and bounded header. Long lines are hard-wrapped without discarding
whitespace. Error details precede bulk code/content; raw IDs, output references,
continuation, and metadata also precede bulk text. Raw envelopes and metadata can
themselves be truncated or omitted by the display budget. Display truncation
does not modify stored evidence or imply the referenced full output is available
in this UI. Live calls are merged only with a known matching turn and no parent
call; uncorrelated or nested evidence remains separate rather than claiming an
unrelated live call.

Timing details distinguish unknown measurements from measured zero. Inference
and parallel-tool batch waits are subsets of execution wall time. Review is a
separate candidate-to-decision wait, including queue/IPC time. These numbers are
not summed into a synthetic total.

Conversation badges show `elapsed` since the accepted request, including queue
and orchestration wait, even before a worker acknowledges the request. Worker
rows show elapsed time since the first recorded start in that scoped turn (or
the first observed Spawn), not the lifetime of a reused agent. Unknown starts
are omitted rather than shown as zero. Terminal worker rows use recorded
`execution` when available; this is execution wall time, not CPU time or total
request latency. Otherwise elapsed time freezes at the recorded terminal event.
The main elapsed row disappears entirely on completion or failure, leaving the
recorded `done` badge when available. Previous visits have no main elapsed row,
including unfinished checkpoints; their worker timers never tick.

Expanded traces show measured foreground model tokens and worker reported tokens,
each with prompt and completion counts, using stored turn metrics only. Foreground
usage does not separate classifier and synthesis calls. Worker usage is aggregated
once per stored assignment, not added again from nested tool evidence. Missing
usage is unavailable, not zero. `review tokens unavailable` explicitly marks the
absence of per-stage review usage: `WorkTiming` contains wall times, not tokens.
No remainder is inferred from aggregate totals. These are reported token counts,
not a complete stage-by-stage monetary cost breakdown.

Live estimates advance monotonically in whole seconds. Only visible badge spans
are overlaid after layout-cache lookup; ticks do not reformat Markdown or tool
evidence, invalidate history layouts, wrap bodies, or move scroll anchors. Badge
text is clipped to the available row width and long durations saturate at `99h+`.

An open trace renders at most 20 worker summary rows, ordered by worker ID,
including an already selected worker even when it falls outside that window.
An omission row reports the remaining worker count. This is a display cap, not a
paginated worker browser: omitted rows cannot be selected from that trace window.
The collapsed conversation contains no per-worker evidence rows.

Evidence details are formatted only for expanded cells inside the selected
worker inside an open trace; native diagnostic JSON is formatted only when raw
diagnostics is also expanded. The 10,000-worker synthetic test checks bounded
summary line counts and zero detail formatting for closed workers. This does
not bound total ingestion/session memory, all event processing, the number of
expanded evidence cells, or legacy worker/model/orchestration detail. Open-trace
grouping still scans and sorts correlated events; worker revision checks scan
worker items even when layouts are cached. The cache avoids rebuilding unchanged
layouts and evidence/timing updates invalidate the affected turn. Only the first
20 ID-ordered workers (reserving a slot for an outside selection) are formatted;
the selected outside row follows the ordinary rows.
