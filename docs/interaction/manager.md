# Daemon-Owned Interaction Projections

## Ownership

`tachyond::Registry` owns one `tachyon_interaction_manager::Manager`, independently
of client connections. The library owns response lifecycle reduction, work and
progress projections, exact links and counters, per-record revisions, and the
bounded live revision journal. `tachyond::interaction` validates producer/session/
assignment identity, supplies admitted host work, persists checkpoints, and serves
IPC. Neither a UI nor a raw telemetry subscription is a semantic authority.

Foreground generation scheduling, work admission/review, runtime history outbox,
command receipts, todos, artifacts, attention, and campaign ledgers remain their
existing authorities. This projection does not execute, retry, approve, or cancel
work. Attach, disconnect, or a slow reader never starts/stops inference. There is
no new database, provider call, or title-generation model call.

The current implementation covers the foreground conversation and host-admitted
`WorkRecord`s. It does not invent campaign membership from a work ID. Campaign
todo scopes must not be displayed as linked without a future explicit host link.

## Wire Contract

Types live in `tachyon_api::interaction_manager`.

* `InteractionSnapshot` returns `ApiResponse::InteractionFrame::Snapshot`.
* `InteractionAttach { after: Option<Revision> }` uses a separate streaming socket.
  `ForegroundSubscribe` is the existing alias for this manager stream.
* `InteractionSubmit { command: Submit }` returns an idempotent admission receipt.
* `InteractionProjection { revision, offset }` returns
  `ApiResponse::InteractionProjection { page }` for another projection page.
* `InteractionContent { reference, offset, limit }` returns
  `ApiResponse::InteractionContent { page }`. Limit defaults to 65536 bytes and
  must be in `1..=65536`. Offsets count bytes, not Unicode characters.

`Snapshot` contains:

```text
revision: Revision { epoch, sequence }
conversation_id: "foreground"
session_id: optional daemon-owned foreground session
host_state: optional sampled AgentState
history: latest 200 canonical messages, oldest first
history_content: references for history texts larger than 16 KiB
projection: Projection { responses, works, progress }
projection_next: optional continuation offset
```

`host_state` is a sampled process status, not a separately revisioned lifecycle
feed. Foreground termination does publish response interruption changes.

Projection pages contain at most 200 records in response/work/progress order.
Continue with the snapshot's **exact revision** and `projection_next`. A changed
revision or invalid offset returns `ResnapshotRequired`; restart the snapshot
assembly rather than joining pages from different states. Do not replace a
client's whole projection until all pages have been assembled. Live updates can
be buffered while assembling, then applied strictly after the snapshot revision.

`Update` now contains:

```text
revision: Revision
session_id: string (empty for host work/progress changes)
event: Option<InteractionEventEnvelope>
changes: Vec<ProjectionChange>
```

`event` retains canonical conversation publications for history/receipt consumers.
It is `None` for telemetry, work, and todo projection changes. Never fabricate a
conversation event for these changes. `changes` contains full replacement records:
`response`, `work`, `progress`, `remove_response`, `remove_work`, or
`remove_progress`. Apply every
change in order, atomically with advancement of the update cursor. A replacement
is not an additive token/counter delta. The event and response replacement in one
update describe the same publication, not two answers to append.

## Response State

Responses are keyed by exact canonical `turn_id`, with `session_id`, optional
typed `command_origin`, producer `generation`, and monotonic per-turn `revision`.
They contain:

* `phase`: `accepted`, `working`, `answering`, `completed`, `failed`, `interrupted`.
* `answer`: the exact accumulated UTF-8 prefix, at most 64 KiB.
* `answer_bytes` and optional `answer_ref`: full byte length and content handle.
* `pending`: bounded provisional status, separate from answer text.
* `intents`: typed, not-yet-finalized conversation intents. An intent is not work.
* `failure`: optional typed `provider`, `timeout`, or `interrupted` failure.
* `final_event_id`: the authoritative final publication identity.
* `metrics`, `latest_tool`, exact `work_ids`, and host-derived `work_counts`.

Acceptance creates a response even before any token is produced. Deltas append
only in the manager. An answer suppresses pending status; status cannot replace
or append to an answer. A canonical final replaces the prefix, clears pending
intent/failure state, and wins over provisional provider failure. Completed
responses reject late deltas/status/errors. Provider failure is never converted
to a fake failed task, even if its message mentions workers or task IDs.

Duplicate interaction message IDs do not increment revisions or append twice.
Older producer generations are rejected. A larger generation starts a new attempt
only on explicit acceptance, never on a token/status. An old foreground session
cannot emit an ordinary response for the current session. Host-admitted user
notifications retain their historical identities and are not new response turns.
The foreground process pump remains responsible for rejecting stale process output.

Host termination and recovery under a different/no live foreground session mark
unfinished responses interrupted while retaining their prefix. Reconnect within
the same session restores the actual prefix, phase, intent, status, and tool state;
clients do not reinterpret an old raw event log. Recovery reconciles the retained
response window against durable canonical final history, which wins over an
older checkpoint. There is no automatic execution resumption after restart.

## Work And Progress

Only admitted `WorkRecord`s create `Work` projections. The adapter uses the
request's work ID, worker ID, generation, assignment, attempt ID, and exact
host-owned origin turn. Legacy unqualified origin counters remain unlinked.
`title` is whitespace-normalized existing objective text, bounded to 120
characters. It is not inferred from worker output or generated by a model.

Work phase is `waiting`, `running`, `reviewing`, `completed`, `blocked`, `failed`,
`cancelled`, `timed_out`, or `unknown`. Terminal outcomes come from the admitted
typed `WorkResult`. Error prose and ordinary worker process state do not prove a
terminal work outcome. Terminal records cannot regress within an assignment;
older generation/assignment telemetry cannot change a newer assignment.

Tool starts/finishes require the actual worker source and current host assignment,
generation, and attempt fence. `latest_tool` means latest actual start in this
scope. Finishing an older parallel call does not replace it. Arguments and tool
outputs are excluded. Tool counters count observed calls, not rendered rows.
Terminal `observed_invocations` and `WorkTiming` come from final host evidence;
missing values are unknown. Worker usage without an assignment fence is deliberately
not attributed to a reused worker. Foreground usage/timing stays turn-scoped and
can arrive after a final without reopening its response.
Legacy foreground telemetry has no attempt-generation fence; it is not applied
to a later explicit generation of the same turn. Missing metrics stay unknown.

Heavy work results remain in existing WorkSubscribe/history/artifact authorities.
`result_available` and host `candidate_refs` expose their availability, not proof
of verification. `Response.work_ids` and `work_counts` are manager-derived from
exact admitted origin IDs; clients must not match objective text or numeric turns.

Each work includes its exact host `todo_scope`. `Progress` is keyed by typed scope
and contains `scope_revision` plus pending/in-progress/blocked/completed/cancelled
counts over the **entire scope**, not the first page of Todo.List. `None` revision
means unknown, whereas `Some(0)` means known empty. Use Todo.List with that exact
scope for item details and normal todo revision/CAS rules for mutations.

The adapter subscribes to post-commit notifications at the existing durable
operational-event transaction seam. The notification runs after redb commit and
outside the subscriber-list lock. A nonblocking bounded wake queue feeds one
daemon-owned projection worker. It refreshes only the affected host-linked
scope; it never polls conversation history or scans all todo scopes per telemetry
event. Initial binding/recovery loads known scopes once. The notification closure
does not acquire the registry or perform projection I/O, so callers already holding
it cannot deadlock by mutating a todo. Queue overflow refreshes known linked scopes
from their current records rather than dropping committed progress. Updates are
asynchronous and carry the exact projected scope revision. Out-of-order refreshes
cannot regress scope revisions. Campaign
scope access is not implicitly granted by these projection links.

## Persistence And Bounds

The existing history redb database has additive `interaction_projection_v1`,
`interaction_content_v1`, and `interaction_final_generations_v1` tables. The final
generation index prevents a previous attempt's history from completing a new
attempt during recovery. Legacy final history belongs to generation zero. The
checkpoint retains current semantic state,
duplicate identities, and telemetry cursors. Answer content uses indexed byte
chunks. Checkpoint and new answer chunks commit in the same history transaction
before publication of the live revision. The runtime outbox and canonical history
remain final-message authority, not the checkpoint.

Append-prefix handles only grow; finals have separate handles. Recovery may return
a `history:EVENT_ID` content handle. Treat all handles as opaque and read them
through InteractionContent. A content handle is not a filesystem path or permission
to execute anything. Fetch no more than the snapshot's `answer_bytes` if a prefix
has grown since the snapshot. Concatenate byte pages before UTF-8 decoding.

The journal holds at most 256 updates and 4 MiB of serialized payload. The current
projection retains every unfinished response/work and the latest 200 terminal
responses, plus their linked work and up to 200 other terminal work records.
Terminal-window eviction is an explicit remove change with durable identity
tombstones preventing late telemetry from resurrecting evicted terminals. Canonical final history
remains available through HistoryQuery. Snapshot pagination and text references
bound inline recovery payloads without silently evicting accepted active state.
Durable command/duplicate/content records currently have no retention policy;
this is not a global disk or active-admission memory quota.

The manager mutex serializes reduction, checkpoint commit, and snapshot revision
capture. Registry handles/host inputs are copied before acquiring it. No registry
guard is held during checkpoint/content I/O; no manager or registry guard is held
during socket writes. Bootstrap uses a shared OnceLock outside the registry so two
first subscribers cannot race initial progress publication. Storage errors log and
invalidate the epoch rather than promising replay of an uncommitted projection.

Epochs change on daemon incarnation or projection invalidation. Unknown epochs,
future cursors, and evicted cursors produce `ResnapshotRequired` then close the
stream. A fresh attach sends a snapshot and replays strictly after its revision.
Slow clients cannot stall execution; writes have a two-second timeout. EOF/I/O
errors require reconnect, not command resubmission or execution cancellation.

## Admission And Identity

`Submit` contains conversation/session/command IDs, text, and optional CWD. Only
`foreground` is routed. IDs must be 1..128 bytes; nonblank text is at most 65536
bytes. The existing host workspace policy validates CWD. Admission persists
`Uncertain` before delivery and upgrades to `Delivered` after transport write.
Neither status means completed or displayed.

The `(session_id, command_id)` receipt is immutable in payload and never resent on
a duplicate. Changed payload is rejected. An identical old-session command can
retrieve its receipt; a new old-session command is rejected. After unknown
transport outcome, reconcile the identical payload, not a newly allocated ID.
There is no transactional execution outbox or exactly-once execution claim.

`CommandOrigin { session_id, command_id, host_message_id }` is echoed by the host.
`AcceptedTurn { turn_id, event_id }` is committed with the reverse turn-to-command
index after daemon validation of session, origin, causation, correlation, and
accepted text. Receipt and stream may arrive in either order. Match the typed
origin pair, never identical text or arrival order. Missing legacy origins stay
unbound. Canonical turns are `HOST_SESSION:TURN`; never join the numeric suffix or
rebind an old key to the current session.

## Consumer Handoff

This change intentionally edits no TUI/CLI/client consumers. Their existing raw
foreground reducers must be replaced by rendering these projections; simply
unwrapping `Update.event` is incorrect for work/progress-only updates.

1. Assemble a snapshot and any projection pages at its exact revision. Replace
   the projection window; reconcile history by canonical event/turn identity.
2. Render response prefix/phase/pending/failure directly. Fetch content references
   only when full text is needed. No raw AgentSubscribe input is required.
3. Apply every replacement/removal in `Update.changes`, then advance the cursor.
   Use optional canonical `event` only for history/admission publication handling.
4. Render work links, counters, latest tool, and todo scopes from the records.
   Do not run title matching, error parsing, worker-origin inference, or todo linking.
5. On a gap, assemble a fresh snapshot. Do not discard an accepted prefix awaiting
   a final: the snapshot now contains the canonical in-flight state.

Legacy JSON snapshots default to an empty projection, no continuation, and no
history-content references. Legacy updates default to empty changes; existing
event objects deserialize as `Some(event)`. Existing receipts/metadata retain
serde-defaulted optional origin/binding fields. Rust struct literals must add
`projection`, `projection_next`, `history_content`, and `changes`; event literals
must use `Some(...)`. These are intentional compile-time consumer handoff changes.

## Offline Verification

```sh
cargo test --offline -p tachyon-api -p tachyon-interaction-manager
cargo test --offline -p tachyond interaction
cargo test --offline -p tachyond runtime_store::todo
cargo test --offline -p tachyond history_store
cargo check --offline -p tachyond -p tachyon-interaction-manager --all-targets
cargo check --offline --workspace --all-targets
```

Tests use temporary stores and fake Unix host sockets, not an installed daemon,
paid inference, or runtime restart. Coverage includes pre-final recovery, gaps,
large answer byte pages, checkpoint reopen, authoritative-final repair, duplicate
commands/events, two subscribers, failure-versus-work separation, stale assignment
telemetry, parallel-tool ordering, terminal metrics, and todo bus updates while
the mutation caller holds the registry. The workspace check currently reports
the intentionally deferred TUI consumer/fixture changes described above.
