# TUI Visit History

Visit history is separate from [Ghost research history](../ghost/HISTORY.md)
and [durable trace resources](../ghost/TRACE.md). Those use host-scoped resource
retrieval and do not automatically import user prompts or expand UI trace rows.

## User Behavior

Every TUI open creates a new visit with its own UUID, even when the daemon has
not restarted and even when no message is sent. Closing the TUI does not stop
the daemon, begin a new daemon conversation, or cancel outstanding work.

The newest available previous conversation page appears above the current
visit. Centered text labels mark both `Previous session` and
`Current session` with UTC timestamps in `DD-MM-YYYY HH:MM` format. Each has a
blank content row above and below it. Labels are dark gray and
dim, with no horizontal rules or bold emphasis; labels fit the terminal's display-cell
width, including narrow terminals. Empty and worker-only visits remain on disk but do not obscure the
last visible conversation.
Session labels appear only when archived conversation is visible. Hiding history
or having no archived conversation resident removes the current-session label
as well; an empty current transcript is then blank. With history shown, the
current-session label marks its end even before the first current message.

| Control | Behavior |
| --- | --- |
| `Ctrl+L` | Hide/show previous visits; never clear or delete messages. |
| `/clear`, `/reset` | The same nondestructive visibility toggle. |
| `Up`, `Down` (empty input) | Select conversation turns; crossing archive edges loads adjacent older/newer turns automatically. |
| `PageUp`, `PageDown` | Navigate the loaded transcript using existing controls. |
| `/mouse` | Toggle opt-in mouse capture for TUI clicks and wheel scrolling; default is native terminal selection. |
| `End` | Follow the end of the loaded transcript. |

Startup and visibility toggles close trace selection and follow the latest
conversation. Hiding history leaves only current-session messages visible and
jumps to their latest content without deleting anything. Showing history again
loads the newest previous page. Up at the oldest loaded turn selects the last
turn of the preceding page; Down at the newest archived turn selects the first
turn of the next page, eventually returning to the current session. Re-entering
history from current messages starts at the newest archived turn. Only one old
page is resident alongside the current visit, not every previous session at
once. Current messages remain part of the transcript during archive navigation.
With no current messages, `End` followed by `Up` also returns to the newest
archived turn, even if an older page was previously loaded.

Existing keyboard trace expansion, copy selection, and opt-in mouse hit targets work on the
loaded page. Archive interactions change the in-memory view only. The agent
pane continues to target live daemon agent IDs, not archived worker rows.
Internal `archived:<pid>:` and visit/conversation identity prefixes are hidden
from turn headers, not removed from their identities.

### Copying

`y` with empty input copies the whole selected chat cell, or the latest loaded
cell when nothing is selected. This includes user and assistant with speaker
labels, not just the assistant response.
It is inactive in the agent pane. Prefer `y` for application cell copy.

Mouse capture is **disabled on every TUI entry**, explicitly clearing stale
terminal reporting modes. Drag normally to select terminal text, then use your
terminal's Copy action (often `Ctrl+Shift+C`). In this default native mode a
forwarded `Ctrl+Shift+C` does nothing: it never overwrites the clipboard with a
whole cell or exits the TUI. The application cannot see or extract native
selection, and cannot automatically copy it for you.

`/mouse` toggles capture for clickable traces, worker/tool details, pane controls,
and TUI wheel scrolling. The toggle reports its state in the status line; `?`
or `Ctrl+P` shows the current state and help. This is session-local, not persisted.
In capture mode only, a forwarded `Ctrl+Shift+C` copies the selected/latest cell,
even with a draft. If the terminal intercepts it, its own Copy action wins.
Shift+drag can bypass capture in some terminals, but is terminal-dependent; use
`/mouse` again for ordinary native selection rather than relying on that override.
Toggle errors are reported without changing the recorded mode or input cursor
behavior; a partial terminal write can leave reporting uncertain, so retry.

Native selection and application click handling cannot both own the same mouse
event through crossterm. By default the wheel belongs to the terminal (normal
scrollback or terminal-specific alternate-screen behavior), not TUI transcript
scrolling. Use `PageUp`/`PageDown`, empty-input arrows, `End`, and `Ctrl+O` for
keyboard navigation and traces. Capture enables clicks but takes ordinary drag
away from terminal selection. Selected turns have no broad gray header/body
highlight; trace mode and existing expansion indicators identify the open turn,
not a text range ready to paste.

A standalone assistant cell copies only its assistant section. Pending status
is not a response; streamed reply text is copied as currently received.

Selection indexes refer to the loaded, visible conversation, including archived
pages, never to worker focus or hidden history. An empty or invalid explicit
selection does not fall back to another turn. Expanded traces, raw tool evidence,
timestamps and session separators are excluded. Reply protocol/decode noise is
filtered as in the transcript. Otherwise markdown, Unicode, indentation, line
endings and trailing whitespace are preserved, with two newlines between labeled
sections. Copy does not use terminal wrapping or cached rendered text.

Clipboard delivery tries `wl-copy`, `xclip -selection clipboard`, then `xsel -b`.
Failed starts, stdin writes or process exits advance to the next helper. If all
fail, the exact payload is written to `clipboard.txt` in the Tachyon data directory.
The status line reports `System clipboard copied` only after a helper succeeds,
`Saved to <path> (system clipboard unavailable)` for a file fallback, or an
explicit failure. A saved file does not change the system clipboard. Notices
expire after eight seconds; no copy status is printed to stderr.
Helpers require access to a working display session, not just presence on PATH.
On Wayland, `wl-copy` is supplied by the OS's `wl-clipboard` package; X11
alternatives are `xclip` and `xsel`. Tachyon does not install these automatically.

The UI snapshots the selected text before nonblocking enqueue to one clipboard
worker per TUI session. Delivery never runs on the input/render thread. One
request may be in flight and one waiting; when full, new requests are rejected
(the queued request is not replaced). A status-line notice asks you to retry and
is replaced by the next enqueue or completion notice. Accepted requests retain
their original text even if selection changes or more response text arrives.
Worker completion is delivered asynchronously to the UI; queue acceptance alone
is not copy success. The worker retains the per-helper deadlines and fallback
order. Failed or timed-out helpers have their process groups killed, but
successful helpers' forked clipboard owners are left alive to serve paste
requests. Closing the TUI drops its queue sender without joining the worker, so
clipboard I/O cannot delay exit. Accepted requests drain if the process stays
alive; process exit may interrupt outstanding copies.

## Storage

History is stored under `tui-visits/` in the existing Tachyon user data directory.
There is no new database and no direct access to daemon database files.

Each visit owns `<open-time-nanoseconds>-<uuid-v4>.visit`. The directory is the
visit index, ordered by open time. No shared latest-conversation snapshot is
overwritten by competing TUIs. Concurrent TUIs may record the same live event
in their separate visits; they do not merge their snapshots or erase each
other's messages. Already loaded archive pages are snapshots, not live views
of another TUI's file.

A visit file contains JSON conversation pages, outstanding typed-turn recovery
metadata, a fixed-width little-endian page-offset table, the page count, and
the `TUIVIS01` footer. Readers seek to the selected page using two offsets;
they do not deserialize the other pages. The format is local TUI storage, not
the daemon's canonical history format or a plain JSON document.

Changed current-visit state is written to a same-directory temporary file,
synced, atomically renamed, and followed by a directory sync. New archive files
use mode `0600`; the new archive directory uses `0700`, subject to existing
directory permissions. Orphaned temporary files are ignored on reopen.
Submission, typed acceptance, typed completion, and recovered completion cause
checkpoints. Other changes, including streaming updates, are checkpointed at
five-second intervals and on normal exit. An abrupt crash can lose changes
since the last successful checkpoint. Disk/write errors are reported rather
than replacing good files with partial data.

Corrupt archives are reported, not silently skipped or repaired. A corrupt
latest checkpoint or selected startup page can prevent startup. Failed archive
navigation leaves the displayed page and its navigation cursor unchanged.

Only the owning TUI's changed snapshot is rewritten. Loaded previous pages are
excluded from that snapshot, preventing each reopen from copying old messages
into another visit. Idle saves are skipped. Visibility toggles never delete
history or affect daemon conversation state.

There is no automatic retention deletion or age/count limit. "Infinite history"
means durable history without automatic deletion, not unlimited disk capacity.
Backups should include the complete `tui-visits/` directory.
Daemon PID changes do not select, replace, or delete visit archives. Seeing only
one previous-session label at startup is a page selection, not an archive count;
use `Up` with empty input repeatedly to reach older pages and visits. Archive
file counts alone cannot establish how many conversation turns were preserved.

## Legacy Migration

On first use, `tui-session.json` is imported into the deterministic
`00000000000000000000-legacy.visit` file. Both its old PID-prefixed turns and
its current conversation are preserved. The original JSON is left untouched.
The atomic destination itself marks successful import, so reopening or retrying
after an interrupted write does not import the same source repeatedly.

The legacy snapshot is treated as one imported visit: it did not record actual
TUI-open boundaries. Dates fall back to stored item timestamps where available;
unknown dates are labeled accordingly. It is not possible to reconstruct exact
old visit boundaries or messages that an older clear operation already deleted.
Later changes made by an old TUI to the legacy JSON are not automatically
reimported. Keep the original file as a migration backup.

## Continuations

New typed events use conversation-qualified turn identities, so a reused turn
number cannot complete a reply from a different conversation or loaded visit.
Outstanding accepted-turn identities survive checkpoints independently of the
displayed page and carry across empty visits. Recovery state is inherited from
the most recently checkpointed visit, which may be an older still-open TUI.

The foreground subscription opens before querying the existing typed
`HistoryQuery` API for recorded outstanding turns. Full time-range pages are
split into smaller half-open ranges. Recovery accepts only assistant replies
with the exact conversation and turn identity. Live final replies replace a
recovered reply rather than append another one; queued deltas cannot append to
an already completed reply. A continuation received in this visit is shown and
saved in this visit; the previous visit's snapshot remains unchanged.

This is recovery of recorded outstanding turns, not a complete mirror of
daemon history or a cross-client delivery ledger. Legacy numeric-only turns
cannot safely be correlated. An acceptance never successfully checkpointed,
or seen only by another overlapping TUI whose outstanding set was not inherited,
cannot be guaranteed recovery through this mechanism. Unavailable history APIs
produce an error; pending identities remain for a subsequent reopen. The typed
API has no event cursor: if even a single millisecond fills its maximum
1,000-entry query page, recovery reports an explicit incomplete-recovery error rather than
silently skipping possible replies. Stronger universal recovery would need
additional cursor/delivery support from the API.

## Performance Bounds

Previous pages contain up to 32 conversation turn starts and their associated
trace data. Only one previous page is loaded and cached at a time. The existing
turn layout cache is retained: unchanged old turns are not markdown-formatted
again on every frame or every live delta. Width changes, selection changes, and
explicit page changes can rebuild layouts. Rendering emits only viewport rows.
Ratatui diffs terminal buffers: a live delta does not reformat unchanged archive
markdown. Scrolling, resizing, or changing selection can still repaint old
terminal cells when their screen position or appearance changes.

These are turn/page bounds, not hard byte or constant-time bounds. A single
large reply, large trace, or uncorrelated worker evidence can still make a page
large. The current visit remains in memory and its changed snapshot is fully
serialized at a checkpoint. Render bookkeeping still scans the loaded/current
items and cells. Legacy import reads the old JSON once in full. Outstanding
identity metadata scales with the number of unresolved turns.

Directory navigation scans filenames using bounded memory; startup also stats
visit files to find the latest checkpoint. That work is linear in the number
of visits, but does not load their conversation payloads. Skipping empty visits
requires checking their footers. There is no all-archives rewrite on a frame,
checkpoint, toggle, or reopen.

## Verification

Run `cargo test -p tachyon-tui --offline`. Tests cover visit reopen and isolation,
non-destructive legacy import, interrupted temporary files, independent writers,
empty/worker-only visits, dated separators, display identity elision, hidden
copy selection and hit targets, cache reuse after append/streaming, bounded
indexed pages across 1,000 synthetic visits, outstanding identity carryover,
and typed recovery pagination without contacting a daemon or model.

Regression tests also cover concurrent writers to the migration destination,
failed navigation cursor preservation, FIFO binding of identical submissions,
live tool output isolation, distinct per-visit dates, and reading a selected
page when another page's JSON is corrupt.
Arrow-navigation render tests traverse every turn across page boundaries in both
directions and return to current content, checking selection and viewport indexes.
Separator tests check exact minute-resolution dates, centering, Unicode display
width, blank rows, both session labels without rule glyphs, and blank current
transcripts when history is hidden or absent. Production rendering tests verify that
live deltas rebuild only the changed current turn and hiding preserves its data.
Separator spans and rendered cells are checked against inherited bright/bold
paragraph styling. Synthetic daemon-marker changes and checkpoints while history
is hidden preserve all archived turns across reopen; no real daemon is restarted.

These are unit/render-layout tests, not an interactive terminal or power-loss
test. No daemon restart or model call is needed to run them.
