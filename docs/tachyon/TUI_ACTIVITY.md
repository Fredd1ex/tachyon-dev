# TUI Turn Activity

For inline task/tool monitoring, explicit secondary diagnostics and scoping rules,
see [Turn Activity Details](TUI_ACTIVITY_DETAILS.md).

`crates/tachyon-tui/src/model/turn_activity.rs` owns reply transitions and ephemeral
tool monitoring. It is not an alternative transcript or a persisted event log.

## Reply Lifecycle

Both interaction and agent publication paths use the same transition function:

- A queued/working status replaces an existing, live pending reservation.
- Empty and known generic messages (including `Working on that.`) cannot replace
  a contextual acknowledgement. Background status cannot replace foreground text.
- The first delta replaces provisional text; subsequent deltas append to it.
- A live final replaces the stream in the same slot, but cannot supersede a
  terminal failure. Successful final corrections retain their existing replacement
  behavior. Late status, deltas, and errors
  cannot reopen a completed turn. Timing completion alone is not publication and
  does not prevent the legitimate final from arriving afterward.
- Durable history recovery uses an explicit `Recovered` transition, not the live
  final path. It can supersede a timeout/failure and replace a partial answer.
  The failure marker is persisted with the turn metadata so restoring a checkpoint
  does not accidentally authorize a delayed live final.
- Busy state is derived from outstanding turns, not the most recently received
  status. A follow-up finishing does not mark an unfinished parent as idle.

Status remains available in the existing diagnostic trace, but is not included
in the final answer or answer-copy payload.

Failures appear in the main conversation even when no answer was streamed. If
there is partial text, the view shows the failure first and labels the retained
text `Partial answer (incomplete)`. Partial text is not repaired or promoted to a
successful answer. Copy retains its existing user/answer-only behavior: it copies
the available partial text without the failure banner. Historical diagnostic
errors remain in the trace even after durable recovery supersedes them.

## Live Tools

Actual `ToolStarted`/`ToolFinished` events drive indicators alongside elapsed
time. The conversation shows the aggregate active-call count (for example,
`3 tools`) without repeating the nested tool names. Actor IDs and individual
tool names remain in the detailed trace, never arguments or output in the badge.

Identity includes the envelope turn, actor, session, task, and call ID, plus the
optional producer generation/assignment/attempt fence. Ghost emits this fence;
the daemon validates it before attaching host conversation/turn correlation.
The TUI also requires producer work/task IDs to agree with that envelope scope.
An incomplete or conflicting fence is rejected, not treated as a legacy event.

For each turn/work, the newest observed generation/assignment supersedes older
calls. Attempt IDs are exact-match fences, not sortable timestamps. A matching
`WorkResult` closes only that assignment; an older result or finish cannot close
the new one, even if worker and call IDs are reused. Finished calls leave
tombstones, so a finish delivered before its start cannot resurrect a tool.
Results received before any tool start also leave an assignment tombstone.
Advancing the fence reclaims superseded scopes while retaining the newest fence.

Identity-free foreground events remain supported. Legacy worker completion,
error, and release events affect only unfenced activity: they cannot identify
which assignment produced a delayed terminal event. Fenced activity ends through
matching tool finishes/work results, subscription end, or parent-turn completion.
Completing worker work cannot clear unrelated foreground calls. A reused worker
can display a new active tool alongside retained, frozen timing from older work;
that historical duration is not reinterpreted as the new assignment's elapsed time.

Monitoring is bounded to 256 scopes and 128 call identities per scope, with
bounded assignment/terminal-worker sets and identity lengths. Saturation suppresses
uncertain indicators instead of evicting tombstones and inventing activity.
Global saturation is latched for that live monitor; freeing capacity cannot
recover terminal events already dropped. Ordinary completed turns reclaim
capacity before this limit is reached.
Completed turns release their monitoring state; transcript completion prevents
late events from recreating it. A new turn can monitor a reused worker.

## Rendering

`response.rs` owns conversation response layout and shared badge rendering, rather
than the event loop in `lib.rs`. Pending, streaming, and completed replies share
the same header, badge area above the body, and body spacing. Acknowledgements are
ordinary assistant text, without an hourglass, italic style, or status prefix;
streaming answers use the same Markdown rendering as completed answers.

The elapsed overlay occupies a structurally recorded badge slot. Agent/outcome
counts precede elapsed time, followed by aggregate live tools. Labels are admitted
whole, with time reserved before optional labels, rather than clipping a partial
count into the timer. Very narrow terminals omit labels that cannot fit. There is
no extra `working` badge duplicating a generic acknowledgement. Completion removes
live tools and uses the same area for final metrics.

Clock ticks only compose the cached badge row, not reformat Markdown or change row counts/hit targets.
Tool lifecycle changes invalidate the affected turn. Indicators are not saved,
copied as answer text, or shown on archived turns. Tests exercise narrow widths,
resize, detached viewport/selection, history cache reuse, and restored archives.

Provisional acknowledgement and failure text use cell-width-aware, word-wrapped
plain-text rows in the cached layout, including hard splits for long words.
Resize recalculates row counts and hit targets; elapsed ticks never reflow them.
Status display is bounded to 4,096 input characters and 256 rows, with an ellipsis
when truncated; the stored text is unchanged. At widths too narrow for indentation,
the indentation is omitted, and a wide character that cannot fit even alone is shown
as `?`. These limits apply to status display, not final-answer storage.

## Contract Limits

Generation/assignment pairs must advance when reusing work. Different opaque
attempt IDs at the same generation/assignment cannot be ordered, so conflicting
attempts fail closed rather than using arrival time or lexical ID order. Events
without a fence cannot downgrade a work scope once a fenced assignment is known.
Missing turn/task correlation is never reconstructed from arguments or trace
text. Legacy taskless worker calls still need a correlated worker-completion
event when their results are published by another actor.

Generic versus contextual status is also not explicitly typed. The reducer
recognizes the existing generic phrases; arbitrary new fallback phrases need
to be added or identified explicitly by the producer contract.
