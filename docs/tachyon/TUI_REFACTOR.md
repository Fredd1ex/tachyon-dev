# TUI Refactor: Ownership, Controls, and Baseline

This is an incremental refactor of the existing chat renderer and event contracts,
not a daemon rewrite. Existing conversation styling, copy payloads, visit archives,
turn activity, elapsed badges, and typed event correlation are retained.

## Ownership

| Location under `crates/tachyon-tui/src` | Responsibility |
| --- | --- |
| `lib.rs` | Public `run` facade and unsafe-code prohibition |
| `app/mod.rs` | Private App state, startup, shared identities, and module wiring |
| `app/event_loop.rs` | Bounded scheduling, checkpoint coordination, terminal restoration, and shutdown |
| `app/render.rs` | Whole-frame composition and overlay ordering |
| `app/actions.rs` | Explicit slash/pane command resolution, immutable target capture, and chat admission before optimistic UI changes |
| `app/update.rs` | Conversation acceptance/replies, correlated agent/work evidence reduction, and command receipt application |
| `app/update/events.rs` | UI application of service notifications |
| `app/update/metrics.rs`, `app/update/raw_line.rs` | Aggregate accounting, event identities, and legacy wire-line reduction |
| `app/input.rs` | Active-surface ownership, navigation classification, modal key admission, and key/mouse/action dispatch |
| `app/navigation.rs`, `app/clipboard.rs` | View/selection/unread transitions and copy/clipboard handling |
| `app/editor.rs` | Character-indexed UTF-8 composer editing |
| `app/scheduler.rs` | Bounded event/input batches and redraw deadlines |
| `model/mod.rs` | Transcript scroll/follow state |
| `model/items.rs` | Transcript items, work evidence, and persisted assignment identity |
| `model/metrics.rs` | Persisted per-turn accounting and timing records |
| `model/thread.rs` | Thread construction, transcript storage, fragment/tool reconciliation, and revision invalidation |
| `model/turn_activity.rs` | Existing correlated turn/activity reducer |
| `transcript/cache.rs` | Turn projection, per-cell layouts, viewport height index |
| `transcript/render.rs` | Frame invalidation, read-anchor restoration, visible-row painting |
| `transcript/projection.rs`, `transcript/layout.rs` | Cell/revision projection and cell composition |
| `transcript/trace.rs`, `transcript/text.rs` | Diagnostic evidence, markdown/text, and compact accounting labels |
| `transcript/response.rs` | Existing conversation bodies and metadata styling |
| `transcript/elapsed.rs` | Existing time overlays without body reflow |
| `panels/mod.rs` | Inspector layout cache, independent scroll, and private hit map |
| `panels/tabs.rs` | Pane tab registry, keyboard order, labels, painting, and clipped hit rectangles |
| `panels/agents.rs`, `panels/orchestrators.rs` | Tab content, catalog rows, and stable-ID selection |
| `services/mod.rs` | Background daemon/agent/schedule status polling |
| `services/event_buffer.rs` | Source-independent count/payload admission, FIFO, cancellation-aware producer parking |
| `services/subscriptions.rs` | Owned live/recovery readers, socket cancellation, subscription generations, bounded UI handoff |
| `services/control.rs` | Bounded owned FIFO command worker, request generations, response validation, result admission, and joined drain |
| `services/history.rs` | Bounded async checkpoint mailbox, ordered body/receipt writes, revisions, retry, acknowledgement, and joined drain |
| `services/history/loading.rs` | Independent bounded page loader, navigation generations, history control policy, and explicit UI result application |
| `services/daemon_state_cache.rs` | Existing disposable operational read models |
| `session_archive.rs` | Unchanged persisted visit schema, snapshot/restore conversion, atomic indexed archives, recovery metadata, page requests/reads, and page installation |
| `ui/mod.rs` | Shared panel shell, help/info text rendering and scroll clamping, bounded geometry |
| `ui/chrome.rs`, `ui/overlays.rs`, `ui/activity.rs`, `ui/format.rs` | Composer/footer, help/info, activity styling, and time/lifetime labels |
| `app/tests/mod.rs` | Preserved cross-surface and legacy regression tests |
| `profiling.rs` | Offline synthetic full-frame benchmark and cache/anchor regression |

Modules remain private under `app`, with existing `#[path]` declarations retained
while migration proceeds. New modules use explicit production imports; App state
is private, and surface renderers/reducers take data rather than the entire App.
See [TUI_OWNERSHIP.md](TUI_OWNERSHIP.md) for the one-page ownership and dependency
rules. Older modules and regression tests still use some app-level aliases.

## Controls

| Input | Behavior |
| --- | --- |
| Up / Down, empty composer | Scroll chat rows; never open traces |
| PageUp / PageDown | Scroll the active surface; never implicitly select a trace |
| End in chat | Follow the latest turn |
| Ctrl+O | Toggle structured details inline for the current viewport turn |
| Ctrl+D while details are open | Toggle the secondary diagnostics inspector |
| Alt+Up / Alt+Down, empty composer | Explicitly inspect adjacent turns, including archived visit pages |
| Up / Down in inspector | Select a record; Enter/Space expands it |
| Page keys, captured wheel in inspector | Scroll the inspector, not chat |
| End in inspector/help/info | Scroll that panel to its bottom |
| Tab | Toggle agent/operational pane |
| Left / Right in that pane | Change tab without editing the preserved composer draft |
| Ctrl+P or `?` with empty composer | Toggle help |
| Ctrl+I, if the terminal delivers it distinctly from Tab | Toggle info |
| Esc | Close help, info, pane, or inspector first; quit only without an overlay |
| Ctrl+C | Quit; existing forwarded terminal-Copy rules still apply |
| `y`, empty composer | Copy selected inspector turn or latest chat cell |
| `/mouse` | Opt into captured wheel/clicks; default remains native terminal selection |

Help and info scroll independently. Agent/operational pane keys retain their
existing selection and cursor-pagination meanings. Captured wheel events in the
TODO/resources tabs scroll their rows; other pane wheel events are consumed.
Clicks outside an overlay do not fall through to conversation hit targets.
Overlay typing/paste does not submit or edit a hidden draft. Some terminals turn
an uncaptured wheel into arrow keys; those arrows now only scroll chat.
Pane tab labels, keyboard order, and mouse targets now share one registry. Only
painted tab cells are clickable: separators, clipped tails, and the table-header
row no longer select a tab. Counts use the same agent filtering for mouse and paint.

The inspector reuses diagnostic rendering, worker expansion, and raw-evidence
controls in a separate overlay. Live chat composition always requests closed
traces, so inspecting cannot expand or reflow the conversation. Esc preserves the
conversation reading position rather than forcing follow mode.
The inspector explicitly shows the foreground source ID and scoped turn identity,
plus observed tool/output counts from that cell and exactly correlated worker
items. These are local observations, not a claim of complete remote tool history
or a bibliography of retrieved sources. Summary rows are not clickable; timer and
diagnostic hit rows are shifted together. Chat rendering and copy remain unchanged.

## Scheduling and Rendering

- Subscription processing yields after 256 events or 4 ms between events. It does
  not discard queued deltas, terminal events, or recovery messages.
- Input processing handles at most 32 queued terminal events per loop iteration.
- Dirty input frames have a 16 ms redraw deadline; background-only frames use
  33 ms. Bursts coalesce instead of drawing once per key.
- One background status worker performs daemon, agent, and schedule socket calls.
  A capacity-one request channel prevents accumulation of polling requests.
- Warm frames skip the all-cell revision/height pass. Closed-chat frames do not
  scan worker items for diagnostic revisions. Cached starts/heights are reused,
  and binary search locates the first cell to paint.
- Changed frames still validate all cell revisions, but only stale layouts are
  rebuilt. Layout lookup uses one hash-map entry operation per cell. The cache
  retains the active and one previous width, revalidating revisions and variants
  when restoring that width. A third width evicts the older spare; structure
  changes prune both maps and reset clears both. This can retain twice the layout
  memory of a single-width cache; first-time widths still rebuild every cell.
- Detached reading retains a scoped turn identity and row offset when earlier
  content grows, item indices shift, or width changes. Uncorrelated legacy cells
  fall back to their prompt timestamp/index identity. The row offset is clamped
  if its cell becomes shorter; this is not a character-level reflow anchor.

## Offline Baseline

Run from the workspace root:

```sh
cargo test -p tachyon-tui --offline
cargo check -p tachyon-tui --offline
cargo test -p tachyon-tui --offline synthetic_frames -- --ignored --nocapture
```

The ignored benchmark creates 512 synthetic turns and renders through ratatui's
`TestBackend`. It makes no daemon, provider, clipboard, or real-terminal calls.
The initial frame is 100x32; resize alternates 60 and 100 columns. Each non-cold
scenario has 60 frames. The streaming case mutates the last turn once per frame.

Observed debug-build totals in this workspace, 2026-09-19:

| Scenario | Frames | Before (us) | After (us) | Layout builds before / after |
| --- | ---: | ---: | ---: | ---: |
| Cold | 1 | 50,474 | 51,312 | 512 / 512 |
| Warm | 60 | 226,804 | 193,973 | 0 / 0 |
| Scroll | 60 | 228,174 | 196,659 | 0 / 0 |
| Resize | 60 | 2,620,291 | 2,605,886 | 30,720 / 30,720 |
| Stream | 60 | 233,451 | 236,981 | 60 / 60 |

These are individual local observations, not a statistically established speedup
or production latency guarantee. The after-run work counters establish zero
height passes for warm/scroll frames, one for cold, and 60 each for resize/stream.
Timing thresholds are deliberately not CI assertions. Resize remains the large
cost in this fixture; streaming has no demonstrated improvement.

### Continuation Measurement

Same debug fixture, measured before/after the bounded two-width cache and
single-entry layout lookup on 2026-09-19:

| Scenario | Frames | Before (us) | After (us) | Layout builds before / after |
| --- | ---: | ---: | ---: | ---: |
| Cold | 1 | 49,952 | 51,166 | 512 / 512 |
| Warm | 60 | 190,296 | 189,445 | 0 / 0 |
| Scroll | 60 | 195,210 | 193,429 | 0 / 0 |
| Resize | 60 | 2,628,271 | 292,978 | 30,720 / 512 |
| Stream | 60 | 233,439 | 223,264 | 60 / 60 |

The alternating-width fixture benefits directly from spare-width reuse (about
89% less elapsed time in this individual run). This is not a claim about arbitrary
resize drags or a statistically established streaming improvement. Height passes
remain 1/0/0/60/60; changed-frame validation is still linear. A real-frame test
compares cached/restored widths with fresh renders after reply changes, including
mutations while the other width is active. Existing anchor and chat goldens pass.

Verification for this continuation: `cargo test -p tachyon-tui --offline` reports
202 passed, 1 ignored; `cargo check -p tachyon-tui --offline` passes. The opt-in
benchmark passes separately. Five new tests cover registry painting/hits/order,
persisted work identity, inspector correlation/hits, restored-width fresh-frame
equivalence, and width-cache pruning/bounds/reset. Existing help/info tiny-frame,
keyboard/wheel isolation, archive, screen, and copy regressions also pass.

Normal tests cover the existing screen/copy goldens, UTF-8 edit sequences, modal
key admission, tiny panel frames (including zero-sized geometry), inspector
render/copy isolation, bounded FIFO batches, redraw deadlines, layout reuse, and
read-anchor preservation across growth, insertion, and resize.

## Async Checkpoints

Periodic saves, accepted/finished turn checkpoints, local prompt checkpoints, and
post-draw attention visibility checkpoints now capture immutable data for the
`tui-history` worker. Visit partitioning, JSON serialization, file writes, rename,
and file/directory fsync run there, not in the frame or interactive event loop.
Archive schema and attention receipt JSON are unchanged. Session serialization
and restore conversion have moved from `app/mod.rs` into `session_archive.rs`.

The worker owns at most one active snapshot and one replaceable pending snapshot;
there is one replaceable acknowledgement slot, not an unbounded result channel.
Monotonic checkpoint revisions reject older submissions. Each snapshot contains
the entire current visit and pending continuation identities, so a newer snapshot
can supersede a failed or queued older snapshot without dropping a delta from the
captured state. The mailbox lock is never held during I/O, serialization, or
destruction of a replaced snapshot. Snapshot capture/cloning is still linear in
the live visit and runs on the UI thread; the bound is snapshot count, not bytes.
Worker serialization also has transient copies/partition buffers. This is not a
constant-memory or constant-time claim for arbitrarily large visits.

Attention receipt generations are captured with their matching body. The worker
must successfully fsync/rename/fsync the visit before writing the receipt. Failure
at either stage leaves the UI receipt dirty. Only a successful acknowledgement for
the current receipt generation clears that gate; stale acknowledgements cannot
release newer displayed receipts. `State::retry` still refuses to send displayed
acknowledgements while dirty. Coalescing carries the complete newer receipt state,
not just a receipt watermark without its text. Notices from older visits remain
backed by those immutable archives, as before.

Failures are surfaced in the status notice and retried after one second even if
no new UI event arrives. New snapshots replace the retry payload without bypassing
the failure backoff. Quit and terminal/input errors capture the latest state,
restore the terminal, request a drain, and join outside the interactive loop.
Drain attempts are bounded to three failures after stop is observed, in addition
to any already-running attempt; the final/latest pending snapshot is not skipped.
Permanent failure returns an error retaining the uncommitted snapshot rather than
reporting a successful save. Dropping the worker also stops and joins it; no archive
thread is detached and no unsafe code is used. There is deliberately no hard
wall-clock shutdown promise: an OS filesystem call cannot safely be cancelled.
The terminal is restored before the explicit wait. Abrupt process/power loss can
still lose not-yet-committed snapshots, but cannot advance a receipt ahead of its
durably written body.

Eight new tests cover a blocked fake sink with 10,000 coalesced submissions,
worker-thread execution, reordered revisions/acknowledgements, failed-write
supersession, autonomous retry, bounded failed drain retaining its latest payload,
drop/join, real filesystem body/receipt failures, and restart recovery of the latest
body, dedup receipts, and pending continuation identity. Existing archive paging,
atomic replacement, legacy import, screen/copy, tabs/model/cache, and attention
regressions remain in the offline suite.

Verification for this stage: `cargo test -p tachyon-tui --offline` reports 210
passed, 1 ignored; `cargo check -p tachyon-tui --offline` passes. The ignored
renderer timing benchmark was not rerun for this I/O-only stage. No paid calls,
daemon restart, or commit were made.

## Async History Loading

Interactive history page traversal, directory scans, indexed reads, and JSON
decoding now run in the separate `tui-history-load` worker. It does not share a
queue or I/O lock with the checkpoint worker: a stalled read cannot delay a body
or receipt commit. Startup import, recovery metadata initialization, and the first
page load remain synchronous before the interactive loop and terminal setup.

Each request captures the currently displayed archive cursor and a monotonically
increasing navigation generation. The loader owns at most one active request, one
replaceable pending request, and one replaceable result. Repeated requests while
blocked coalesce relative to the displayed page; they do not accumulate a hidden
backlog of page advances. Bounds are on request/result counts, not bytes in a page.
No filesystem I/O, decoding, or obsolete-page destruction holds the mailbox lock.

Alt+Up/Down still select turns locally, cross page boundaries, and re-enter history
at the newest archived turn when leaving live turns. Ctrl+L and `/clear`/`/reset`
still toggle history without deleting it; showing requests the latest page.
While a request is pending, the old page, cursor, selection, and copy target remain
valid and a loading status is shown unless another status notice takes precedence.
New direction/local selection, hide/show, End/scroll, Escape, overlay changes,
captured clicks, and live submission invalidate pending navigation. Copy, resize,
key-release events, and uncaptured native mouse events do not. Invalidation does
not join or wait for a running read. Both worker publication and UI result receipt
reject stale generations, including errors from obsolete requests.

Only the UI applies a successful result. Page installation preserves all current
visit items and checkpoint recovery state, then advances the displayed cursor and
chooses the correct first/last archived cell. The application clears obsolete
projection/layout/attention and inspector hit state and forces a paint before more
input can use the new page. A missing/corrupt page reports an error without erasing
the current page, cursor, selected cell, copy payload, or cached view; another
navigation retries it. End-of-history is a successful empty result and retains the
existing local-selection behavior. The storage module retains a test-only
synchronous navigation reference for parity tests, not a second production path.

The loader never reads its own current visit (nor visits opened after it), so
concurrent local checkpoint replacement cannot retarget navigation. Older visits
can still be checkpointed by another open TUI. Reads retain the existing atomic
file-replacement contract: each page is decoded through one opened file handle;
directory/index changes between opens can produce a read error, which leaves the
UI unchanged. No cross-process snapshot transaction or new persisted schema is
introduced. Body-before-receipt durability is unchanged.

Shutdown cancels pending requests/results, lets an in-flight load finish, and joins
after terminal restoration, outside the interactive loop. Drop also stops and joins
the loader. There is no per-frame join, unsafe code, detached reader, or claimed
hard deadline for an uninterruptible filesystem operation. Applying/restoring a
loaded page and rebuilding its projection/layout still costs CPU on the UI thread.

Nine new offline tests cover blocked-load input and copy, 10,000 latest-wins
requests, reordered results, local-direction and hide/show invalidation, native
mouse/copy/resize rules, full Alt navigation parity across pages and live turns,
missing/corrupt files with cursor recovery, latest-page showing, independent
checkpoint progress with concurrent live edits, and stop/drop ownership.

Verification for this loading stage: `cargo test -p tachyon-tui --offline --quiet`
reports 219 passed, 1 ignored; `cargo check -p tachyon-tui --offline` and the scoped
diff whitespace check pass. Only TUI code and this document were changed. No paid
calls, daemon restart, or commit were made.

## Concurrent Recovery Correction

Review found a pre-existing recovery bug, also present in `HEAD` before the async
stages: `Visits::open` inherited only the newest-mtime archive's pending map.
Independent TUI visits are not revisions of a single global state. A later stale
empty checkpoint could hide another visit's durable accepted turn; blindly
unioning the pending maps would instead resurrect completed recovery candidates.

Startup now merges recovery metadata from every visit, taking the earliest known
acceptance time for each fully correlated conversation/turn key and subtracting
explicit completion records. Completion wins regardless of file order or mtime.
`ConversationFinished` and recovered history record a terminal tombstone even if
that visit never saw acceptance. Replayed acceptance cannot reopen a known terminal
key. These records are included in the same atomic snapshot as the body, including
metadata-only/zero-page snapshots, and carried through the existing bounded async
checkpoint service. Page loading, attention receipt ordering, and daemon APIs are
unchanged; no raw database access or foreground/orchestrator change is involved.

This fix necessarily extends the recovery metadata section from a pending-only
map to `{"version":2,"pending":{...},"completed":[...]}`. The page JSON schema,
offset table, magic, and atomic file-replacement protocol are unchanged. New readers
accept old pending-only maps and reconcile them against the `completed_turns` of
foreground threads on **all** pages, using one open file handle for metadata and
pages so concurrent replacement cannot mix revisions. Worker completions, numeric
legacy/PID identities, and same-number turns in other conversations do not count.
Existing archives are not rewritten; a new visit checkpoints the merged records
in the new metadata form. Older binaries cannot read that metadata and fail rather
than silently discarding its tombstones. This is forward reading compatibility,
not a downgrade guarantee.

Unreadable/unknown recovery metadata or an unreadable legacy page now fails startup
with the archive path, before writing the new visit. The old test that deliberately
ignored a corrupt oldest archive was updated to assert this fail-closed behavior.
Deletion alone in a legacy pending map is not proof of completion. If a historical
writer erased a pending key without persisting any correlated terminal evidence,
that lost fact cannot be reconstructed locally: an outstanding copy remains a
candidate for the existing read-only `HistoryQuery` recovery, never a resubmission
of work. Known archived completions are removed from candidates, not blindly
unioned back into pending state.

Startup is now linear in all visit metadata and, for pending-only legacy archives,
their pages. This remains outside terminal setup/the interactive loop. Completion
records are not garbage-collected: pruning them without accounting for stale
writers could resurrect pending records. Metadata and inherited tombstones can
grow; the worker still bounds snapshot count rather than total bytes. Startup is
a per-file atomic scan, not a cross-process transaction; a commit occurring after
its file was read can be learned from live events or a subsequent startup.

Four new temp-filesystem tests cover a stale concurrent empty snapshot, a stale
pending snapshot after terminal-without-acceptance, reordered acceptance and scoped
identities, legacy multi-page terminal reconciliation, and corrupt legacy data
failing without replacing an archive. Both synchronous saves and captured async
snapshots participate. Full offline verification reports 223 passed, 1 ignored.

## Owned Explicit Commands

Agent slash and pane controls, daemon start/stop/restart subprocess waits, chat
delivery, and explicit `/attention`/`/ack` commands now use one `tui-control`
worker. Target IDs and attention scopes are resolved before admission. Workspace
lookup for chat, socket connection/exchange, attention pagination, and daemon
process startup/wait run off the UI thread. There are no per-command detached
threads. Daemon subprocess stdin/stdout/stderr are disconnected from the terminal;
results report exit status rather than capturing potentially unbounded CLI output.

The worker admits at most 16 outstanding commands **including running work and
unread results**, using bounded request/result channels and a pending-ID map.
Admission uses `try_send`, never a blocking send or join. A full/stopped queue
reports that the request was not accepted or sent. Chat/slash drafts are retained,
and rejected chat does not append an optimistic prompt/reply or checkpoint it.
Pane requests also report rejection rather than claiming a successful action.
The status line shows locally accepted outstanding commands; this is not a claim
that the daemon or foreground has accepted them yet.

Accepted commands run once in admission order, including across command families.
They are never coalesced, superseded on selection changes, silently discarded on
quit, or automatically retried after a socket error/timeout. A stalled command
delays subsequent commands, not UI input/rendering, history I/O, or status polling.
This serializes delivery requests only: foreground execution, scheduling, memory
recall, and turn concurrency remain owned outside the TUI. A chat response reports
daemon delivery acknowledgement, not completed execution or a correlated turn
acceptance; those continue to come from authoritative foreground events.

Monotonic request IDs correlate results against the pending map. Duplicate/unknown
IDs cannot apply twice or release another request's admission slot. Completion
receipts retain the original command/target, even after a pane selection switch.
They do not optimistically update the currently selected agent or a cached daemon
status; normal status snapshots remain authoritative. Agent/chat response IDs and
attention scope/ID responses are checked. Attention receipts include their command
identity and preserve the existing scoped attention update and checkpoint path.
Connection/exchange errors conservatively report possibly unknown outcomes, never
claim cancellation or invite automatic resubmission. Optimistic chat placeholders
are not guessed terminal on transport errors; only authoritative turn events can
resolve them, so an unaccepted/unknown submission can still leave a placeholder.

On quit or interactive error, terminal restoration precedes the explicit command
drain. A notice explains that accepted queued/running work is not cancelled. The
worker closes admission, executes all accepted requests, joins, and returns every
remaining result for reporting and UI-model application before the final history
checkpoint/drain. Drop also closes, drains and joins; an early error path reports
unconsumed outcomes to stderr rather than detaching work. Executor panics become
unknown-outcome receipts and do not skip subsequent accepted requests. There is no
hard shutdown deadline: socket writes, filesystem lookup, and subprocess waits
cannot be assumed cancellable. Process termination/power loss can still lose the
in-memory queue; this is not a durable command outbox or exactly-once remote API.

The bound is command/result **count**, not bytes: prompts, attention pagination,
response payloads, and receipt text can be large. Automatic displayed-attention
acknowledgements retain their existing body-before-receipt gate and retry worker;
they were not folded into the explicit-command FIFO. Existing status polling and
subscription/recovery workers were unchanged in the command stage (see the
subscription continuation below). Shutdown does
not wait for foreground work completion or establish a subscription replay barrier;
only turn identities already observed by the existing recovery pipeline are durable
recovery candidates. Command draining does not establish subscription completeness.

The extraction also moves thread storage/construction and conversation/agent event
reducers out of `app/mod.rs`, with explicit production imports. Work evidence keys,
reply reconciliation, revisions, history schemas, and chat/copy goldens are retained.
The legacy raw-line reducer, aggregate metrics projection, rendering, large input
match, and older tests still live in `app/mod.rs`; this is not a claim that the full
application-controller refactor or real-terminal latency validation is complete.

Seven new offline tests cover blocked intake over a real local Unix socket pair
using the API's JSON framing, input/edit/frame progress while blocked, queue-full
draft/model preservation, FIFO command/result order, unread-result bounds,
selection-switch target isolation, duplicate/unknown IDs, scoped response checks,
unknown-outcome non-retry, joined drop/drain, daemon-command thread ownership, and
continued processing after an executor panic. No fake test invokes a real daemon
command or provider. Existing history/receipt/recovery and screen/copy tests remain
in the suite. `cargo test -p tachyon-tui --offline --quiet` reports 230 passed,
1 ignored; `cargo check -p tachyon-tui --offline` passes. The scoped diff whitespace
check passes. The ignored timing benchmark was not rerun for this command/model
stage. No paid calls, daemon restart, or commit were made.

## Subscription Buffering

Live agent/foreground streams and their continuation/attention history recovery
now use a separate shared FIFO in `services/event_buffer.rs`. There is no
unbounded forwarding channel between those producers and the app. Admission uses
a capacity-256 `sync_channel`, nonblocking `try_send`, and a condition variable
for count/payload credit. Full producers park without periodic polling; dequeue,
source cancellation, and receiver drop wake waiters. No mpsc receive is performed
under the accounting or socket-registration locks. The app retains its 256-event /
4-ms processing budget across both service notifications and live events. Poll
priority alternates across frames, so neither continuously ready source starves
the other.

The normal queued payload budget is 4 MiB across all subscriptions, not 4 MiB per
agent. Accounting includes fixed message size, source/agent ID bytes, raw line
bytes, and serialized structured/recovery payload bytes (counted without a second
serialization allocation). It is **not an exact Rust heap/RSS bound**: collection
capacity and allocator overhead differ from serialized size.

**Oversized-event policy:** the existing newline JSON transport has no frame-size
maximum. A valid event larger than the budget is admitted only when the FIFO is
empty, and no other event is admitted until it is dequeued. It is not truncated,
silently dropped, or treated as a broken connection. Thus the byte budget has an
explicit single-oversized-event exception, not a hard 4-MiB ceiling. Each blocked
producer can additionally retain one decoded event; each reader can allocate one
arbitrarily large frame while parsing, and history queries hold one response page
(up to 1,000 entries). Structured wire data is released before admission waits.
Worker count scales with subscribed agents, and the applied transcript remains
unbounded. Hard process-memory bounds need a protocol-sized/chunked payload or
streaming artifact reference contract; this change does not invent an incompatible
tool-output cutoff or claim to implement disk-backed streaming.

No authoritative delta/final is coalesced or discarded because a consumer is slow.
Per-reader FIFO preserves the authoritative final after earlier deltas; independent
history/live producers retain their existing interleaving. Cancellation is reserved
for teardown/replacement, not overload. Every live, recovery, error, and end message
carries a monotonically increasing local subscription generation. The app rejects
retired generations before reduction, so an old final cannot overwrite new state
and an old Ended cannot remove a replacement subscription. Rejected stale messages
still consume a processing-budget tick.

Each subscription owns its reader and scoped attention-recovery thread. Stop wakes
blocked buffer admission and calls `socket.shutdown(Both)` on registered live and
history sockets, including readers blocked on partial JSON lines. Registration
after cancellation immediately shuts down the late socket. Completed history calls
do not leave cancellation clones keeping their connections alive. The interactive
loop only joins already-finished retired workers. Exit cancels first, restores the
terminal, then joins remaining workers. There is no detached recovery reader and
no join while the interactive consumer must free queue capacity. Initial Unix
socket connection establishment, JSON parsing, and thread scheduling do not have a
hard shutdown deadline; shutdown can interrupt socket I/O only after registration.

**Daemon residual:** `tachyond::Registry::subscribe` / `subscribe_work` still use
unbounded mpsc queues; stream writers wait on those queues outside registry locks.
When TUI admission stops reading, finite kernel socket buffers eventually stall
daemon writes and pressure accumulates in those daemon queues. This is not
daemon-to-TUI end-to-end bounded buffering, and sustained overload can still exhaust
host memory. Agent subscriptions replay terminal usage/result and work subscriptions
replay a terminal result, but this is not a complete durable delta/final replay
contract for foreground and partial turns. Blind slow-consumer disconnection would
risk unrecoverable partial state. A future host policy needs durable final/replay or
lossless upstream flow control before disconnect becomes safe. This pass does not
change the daemon queues or claim a replay barrier on exit. Separate status,
clipboard, operational, and attention-action notifications retain their existing
shared mpsc channel; they are outside this live/recovery FIFO's bounds.

Offline regressions exercise count and byte saturation independently, exclusive
oversized admission, receiver-drop and per-source cancellation, real Unix-pair
framing, 1,024 ordered deltas plus an authoritative final through the app reducer,
stopped consumers, partial-frame shutdown, reconnect generation isolation, and an
oversized valid tool result followed by its final. No daemon or provider is used.

Verification: `cargo test -p tachyon-tui -p tachyon-api -p tachyon-client --offline
--quiet` passes (TUI: 240 passed, 1 ignored). `cargo check -p tachyon-tui -p
tachyon-client -p tachyon-api --offline` passes. The ignored timing benchmark was
not rerun for this subscription-only continuation.

## Remaining Work

- Replace remaining legacy root aliases/path declarations and global conversation
  hit-map mutexes with owner-local state. Aggregate metrics/raw-line reduction,
  diagnostic/pane rendering, input dispatch, and legacy tests are now extracted.
- Replace the remaining large key/action match with a fully testable application
  reducer. Offline decoded-event and opt-in PTY tests now exercise the complete
  input handler/event loop with synthetic services; real daemon startup and
  destructive pane actions are not part of that fixture.
- Archive startup/import/initial loading remains synchronous outside the interactive
  loop. Explicit daemon/agent commands now have owned asynchronous result semantics;
  the limitations above (head-of-line waiting, unknown outcomes, count-not-byte
  bounds) remain. No foreground or orchestrator scheduling behavior was changed.
- The live/recovery FIFO now has count/payload admission, but oversized frames,
  per-reader in-flight payloads, applied transcript size, other notification paths,
  and unbounded daemon subscriber queues remain outside a hard memory bound.
  Durable replay and a safe host slow-consumer policy remain necessary; local
  backpressure alone moves sustained overload upstream rather than solving it.
- Changed-frame revision validation remains linear, and resize still lays out
  all loaded cells at uncached widths. There is no incremental height tree.
- The tab registry and detailed tab content live in `panels/`; input dispatch lives
  in `app/input.rs`, with explicit command resolution in `app/actions.rs`. The inspector remains
  an overlay, not a side pane. Help/info share scrolling/rendering but have no
  clickable close buttons, and Page keys still use the chat viewport page size.
- A worker-rich performance fixture and hardware/emulator interaction remain to
  be measured. Synthetic release-mode draw distributions and offline PTY checks
  are recorded below. No paid calls or daemon restart were used for this refactor.

## App Modularization

The residual application blob has been split by responsibility, not moved into a
replacement event-loop blob. Private App state and startup stay in `app/mod.rs`;
the loop delegates notification reduction, frame composition, and input dispatch.
`app/mod.rs` is now 525 total lines (including imports and test wiring), down from
9,848; `app/event_loop.rs` is 211 lines, not a relocated monolith.
Pane content, cell/diagnostic drawing, composer/footer, aggregate accounting, raw
lines, clipboard handling, and navigation have dedicated owners listed above.
No App fields were made public. The public `run` facade is unchanged.

All enabled legacy tests were preserved under `app/tests/`; catalog-selection
tests now run under the orchestrator panel owner. Four `cfg(any())` render functions,
the private uncalled legacy pane, and their verified uncalled helpers were removed.
Test-only legacy helpers remain test-only. No service subscription/history/control
internals or archive schemas were changed by this stage.

Offline verification retains 240 passing tests and one ignored benchmark. The
screen/copy golden assertions pass without fixture changes. The opt-in synthetic
frame benchmark also passes separately. `cargo check -p tachyon-tui --offline`
passes without warnings. The large input match and shared
conversation hit-map remain explicit follow-up work, not claimed solved by moving
them. No paid calls, daemon restart, or commit were made.

## Offline PTY Verification

The final terminal verification uses the real `App::event_loop`, crossterm
poll/read decoding, production rendering, subscription reader/buffer/reducer,
and terminal restoration. It deliberately does not invoke `run()`: that function
loads real archives/config and starts daemon-backed status/control/clipboard
services. A private `cfg(test)` constructor instead supplies temporary history,
default config, disconnected status/clipboard channels, a rejecting command
executor, and an explicitly injected synthetic Unix socket subscription without
attention recovery. No service/control semantics were changed.

The Linux parent uses safe nix openpty and starts exactly one ignored libtest child
under `setsid`; there is no shipped fixture binary or CLI flag. PTY stdio and
crossterm globals are isolated from concurrent tests and the developer's terminal.
`stty` on the private slave plus SIGWINCH changes actual kernel terminal geometry
without unsafe ioctl code. The library root continues to forbid unsafe code.

Run commands and the complete assertion/limitation list are in
[`crates/tachyon-tui/tests/README.md`](../../crates/tachyon-tui/tests/README.md).
The PTY verifies repeated arrow bytes, SGR wheels before/after mouse capture,
explicit Ctrl+O, inspector-only scrolling, Escape close-before-quit, tiny/restored
resizes, hidden panes, responsiveness during continuous API-framed socket updates,
successful joined teardown, exact termios restoration, terminal mode sequences,
and a durable applied socket reply. Observations travel over a separate socket;
this is not an ANSI emulator or a hardware trackpad test. Default tests retain all
existing TestBackend goldens and add complete-handler decoded-event coverage.

Observed debug PTY run on 2026-09-19: 2,294 synthetic socket updates sent,
19.61 ms Ctrl+P-to-help-frame, 16.52 ms Ctrl+C-to-joined-exit. Assertions require
less than 2 seconds and 5 seconds respectively, not these observed timings.
The timing observer and synthetic service setup do not establish production
latency or full-daemon correctness. No production daemon/provider was contacted.

Verification: `cargo test -p tachyon-tui --offline --quiet` reports 241 passed,
4 ignored (two benchmarks, PTY parent, private child). The final PTY test passed
twice consecutively; both opt-in release benchmarks and the unchanged debug
baseline passed separately. `cargo check -p tachyon-tui --offline` passes. No
controls, subscription behavior, shipped CLI, daemon process, or provider changed.

### Reproducible Render Measurements

`profiling::synthetic_app_frames` adds an opt-in measurement around production
`App::draw` with a synthetic 512-turn App and TestBackend, not a substitute renderer.
The existing transcript benchmark is unchanged and remains independently runnable.

Release observations (microseconds), 2026-09-19:

| App scenario | Frames | Total | p50 | p95 | Max | Layout builds / height passes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Cold | 1 | 10,758 | 10,758 | 10,758 | 10,758 | 512 / 1 |
| Warm | 60 | 15,244 | 227 | 413 | 434 | 0 / 0 |
| Scroll | 60 | 16,816 | 252 | 433 | 485 | 0 / 0 |
| Resize | 60 | 30,868 | 370 | 410 | 10,793 | 512 / 60 |
| Stream | 60 | 19,897 | 330 | 354 | 381 | 60 / 60 |

The original transcript benchmark in that release run reported totals of
6,824 / 19,481 / 20,498 / 39,845 / 26,358 us in scenario order. Different viewport
heights and timing scopes make this a reference, not evidence that adding chrome
speeds rendering up. App timing excludes mutation/resize setup and includes frame
diffing through TestBackend; it excludes terminal encoding/writes, event-loop
service work, and filesystem checkpoints. Resize alternates two cached widths;
the first new width accounts for its large max. Cold has only one sample. These
within-run distributions are not independent-run statistical confidence intervals.

For a like-for-like check against the earlier debug continuation table, rerunning
the unchanged `synthetic_frames` produced 50,574 / 192,186 / 196,294 / 293,155 /
221,715 us. The previously recorded after-values were 51,166 / 189,445 / 193,429 /
292,978 / 223,264 us. Both runs have layout-build counts 512 / 0 / 0 / 512 / 60
and height-pass counts 1 / 0 / 0 / 60 / 60. This reproduces the work-counter claims
and similar local debug totals; it does not remeasure the historical uncached
implementation or establish a statistical speedup. No timing threshold is used
in the profiling tests.
