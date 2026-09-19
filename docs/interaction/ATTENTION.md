# Durable Attention

Attention is a host-authored operational record in `runtime.redb`, not a model
instruction, error-log scrape, progress message, or replacement for `work.ask`.
The daemon chooses severity from the typed category. Questions are warnings;
terminal failures, terminal timeouts, and budget blockers are urgent.
Urgency here is a delivery priority for these operational categories, not a claim
that every notice is a safety emergency or that all critical conditions are covered.

## Host Integration

The transaction seam for campaign/budget producers is:

```rust
RuntimeStore::admit_attention_in(
    &transaction,
    runtime_store::attention::AttentionSource {
        scope: TodoScope::Campaign { campaign_id: campaign_id.clone() },
        campaign_id: Some(campaign_id),
        work_id: Some(work_id),
        generation,
        instruction_revision,
        category: AttentionCategory::BudgetBlocked,
        cause_id: durable_blocker_episode_id,
    },
    now_ms,
)?;
```

Call **inside the transaction establishing the validated source condition**.
The source type is host-only and not deserializable. There is intentionally no
worker-facing or operator-facing attention-create endpoint. The caller must
establish the category from typed host state, not model prose. Work sources are
checked against admitted campaign, generation, and current instruction revision.
Campaign-only sources may omit work; their generation/revision and stable cause
must come from the producer's own validated campaign state. The category alone
does not establish that funding is blocked. The budget task owns that decision.

Admission inserts the record, cause receipt, pending index, and
`OperationalChange::AttentionChanged` in one redb write transaction. Repeating
the same scope/work/campaign/generation/revision/category/cause returns the
original record and does not append another event. IDs and command IDs are
persisted UUID identities; wall-clock time and process-local counters are not
identity. All underlying records remain available after batching and acknowledgement.

Current producers:

- `suspend_work`: the validated durable question and attention commit together.
- `push_event`: only a terminal `WorkResult` passing the current daemon work,
  worker, generation, and assignment fences. Generic errors, progress, cancelled
  work, successful results, and intermediate review candidates do not qualify.
  Ordinary foreground-turn delegation failures/timeouts use their direct,
  retained `WorkResult` channel instead of producing a duplicate generic notice:
  the conversation aggregates successes and failures, including blocking failures.
  Eligibility uses host registry ownership (`foreground` or its `background`
  delegation broker), a nonempty origin turn, the exact work assignment, and a
  registered running foreground. Worker prose/correlation cannot opt out.
  Without that owner/outcome route, failures still produce attention; an unowned
  worker exception is not silently discarded. No live subscriber is required for
  the retained result route. This is ownership policy, not a model urgency field.
  Failure state, terminal result delivery, history and raw event logging remain
  unchanged. Existing attention is neither deleted nor filtered from lists.
- Campaign execution settlement: typed failed/timed-out candidates after rework
  retention and final settlement checks, with the current instruction revision.
  Campaign admission takes precedence over conversation ownership: training
  crashes still alert even when linked to a conversation. No retry-counter,
  supervisor, continuation, or root-summary policy is changed. Explicit
  `work.ask` questions always retain their existing attention admission path.
- Campaign ledger reconciliation: a durable admissions pause (including reported
  verification or oversight overruns), or known cumulative Work usage reaching a
  nonzero token/cost envelope limit, produces `BudgetBlocked` in that same writer.
  Allocation parents and unknown holds are not reported spend. The immutable
  campaign envelope is the source: these campaign-scoped records have no Work ID
  and use generation/revision zero, with one stable cause per pause/exhaustion
  threshold for the campaign lifetime. Retries and reopen do not create new records
  or funding. Simultaneous pause and exhaustion retain both distinct causes.

Budget coverage is intentionally limited to those authoritative ledger states.
Reservation denial alone, fully reserved but unknown usage, a 25% oversight
trigger, an exhausted individual service allowance, and generic provider/config
errors do not imply campaign budget exhaustion and do not create this attention.
No text/error-string parsing or invented Work terminal phase is used.

No historical log scanning or automatic publication of uncertain crash outcomes.

## Delivery Contract

The scheduler checks a durable outbox even without a connected TUI. It creates
at most one delivery frame per poll, coalescing up to 32 same-scope, same-category
records. Up to eight unacknowledged frames may be outstanding. New selection
prefers urgent records. Selection scans at most 128 pending keys, and replay
reuses the exact persisted frame, command ID, and short deterministic text.
Unsent records stay on disk, not in an unbounded in-memory queue. A frame retries
after five seconds until foreground publication is durably admitted.

The foreground receives the existing typed `NotifyUser { model: false }`
command. An `attention-command-` identity selects the deterministic overlay
path: no provider call, turn allocation, provider cancellation, or partial
conversation-history mutation. Modeled reminder work runs separately so it
cannot hold intake while awaiting a provider. The existing standalone
`UserVisibleNotificationPublished` event is used without a new UI layout or
raw JSON token rendering. Its message ID is `<command_id>:published`, correlation
and causation identify the command, and `turn_id` is absent. The daemon enriches
the validated publication with optional nested `attention: { scope, ids }`
metadata from the persisted frame, never from prose or the model's conversation
ID. Membership is 1..=32 unique IDs. Supplied membership must match the host
frame exactly; unrelated events cannot claim it. Foreground stdout is the only
accepted publication source.

During an active model synthesis request, this interjects a separate standalone
display notice. It does not abort, restart or modify that request, inject new
tokens into its already-submitted prompt, or establish a critical-interrupt
mechanism. Deterministic attention delivery consumes no model credits; this does
not make unrelated modeled reminders or budgeted campaign assessments free.

The daemon accepts this publication only from foreground stdout and validates
the frame identity, conversation, phase, text, and protocol version. It commits
the history-outbox entry and delivery transitions together **before** forwarding
the event to subscribers. A crash before this commit retries the frame; a crash
after it recovers through durable history and the operational feed. Canonical
history carries the same optional membership and publication ID, not raw frames
or database keys. Missing metadata defaults to `None` and is omitted when
serialized, preserving legacy wire and history shapes.

## API And Replay

These requests require the same-user authenticated Unix socket. Internal
dispatch rejects them; no worker grant, guest endpoint, or admin bypass is added.
Every request names an explicit `TodoScope` (conversation, campaign, or work).

```rust
ApiRequest::AttentionList { scope, after: None, limit: 32 }
// -> ApiResponse::AttentionList { snapshot }

ApiRequest::AttentionAcknowledge {
    scope,
    id,
    phase: AttentionAcknowledgement::Displayed, // or Acknowledged
}
// -> ApiResponse::AttentionAcknowledged { attention }

ApiRequest::OperationalSubscribe { scope, after: snapshot.watermark }
// -> OperationalBatch containing AttentionChanged records
```

List limits are 1..=100. `next_cursor` binds the scope and snapshot watermark.
Finish pagination before subscribing; a concurrent mutation invalidates the
cursor, requiring a fresh snapshot. Subscription uses the existing bounded,
exclusive-watermark operational feed. Persist its watermark after applying a
batch, including empty batches. On a stale database identity, restart from a
snapshot. Attention events do not increment TODO revisions.

The four independent timestamps mean:

- `accepted_at_ms`: host source and attention are durably admitted.
- `delivered_at_ms`: foreground publication is durably admitted by the daemon.
- `displayed_at_ms`: an explicit client display receipt; requires delivery.
- `acknowledged_at_ms`: an explicit operator receipt, including through list/API.

Acknowledgements are idempotent, exact-scope, and durable. They do not answer a
question, cancel work, grant budget, or imply another phase occurred.
Missing display receipts are unknown, never inferred from socket connectivity.

## TUI Receipts

Typed attention uses the existing standalone text renderer without allocating
a model turn, changing generation, gluing onto a model response, or adding
badges. Only actual notice body lines in the latest rendered viewport qualify
for `Displayed`, after the terminal draw succeeds. Header-only views, scrolling
elsewhere, hidden archives, zero-size views, and any open pane/help/info overlay
do not qualify. Seeing a notice never sends `Acknowledged`.

`tui-attention.json` persists publication deduplication and display receipt
state. Visit items retain publication identity and exact membership independently
of archive turn names. Reconnect recovery reads bounded time-window history pages
and deduplicates by publication ID; a saved past notice is not inserted again in
the new visit. Display receipts retry off the input/render thread in batches of
at most 32, including after restart or temporary socket failure. Successful
receipts are remembered; duplicate daemon requests remain idempotent.

Both ordinary receipt saves and post-render display saves persist the visit body
before the dedup receipt. An archive write failure leaves receipts dirty and
unsent, so a saved receipt cannot suppress recovery of a body that was not saved.

Use `/attention` to list the host foreground scope and scopes learned from typed
notices. `/attention conversation <id>`, `/attention campaign <id>`, and
`/attention work <id>` explicitly select other scopes. Lists follow daemon
pagination and show IDs and all four timestamps; listing alone sends no display
receipt. A stale list cursor produces an error rather than claiming a complete
list; rerun the command for a fresh snapshot.

`/ack <id>` sends only `AttentionAcknowledge { phase: Acknowledged }`, using the
exact scope learned from a typed notice or successful list. Unknown IDs require
listing their scope first. It performs no automatic work action. These commands
use the existing palette/transcript and authenticated daemon APIs, not a model.
Legacy notifications without typed membership continue rendering but never
produce attention receipts.

## Verification

Tests use temporary redb stores, local socket pairs, and pending fake tasks,
without live provider requests. They cover commit-before-notify reopen, aborted
transactions, cause deduplication, stale work fences, bounded coalescing with an
absent UI, replay identity, publication forgery rejection, separate phases,
idempotent scoped receipts, and authenticated local handlers. The foreground
publication identity test keeps another fake provider task alive throughout.
API and TUI tests also cover bounded typed membership, canonical history reopen,
receive-versus-render, visible standalone body hit lines, scroll/header/overlay
suppression, replay without duplicate items or model counters, hidden archive
resume, durable pending display retries, explicit scoped acknowledgment, and
legacy metadata absence. TUI rendering uses a test backend and receipt tests use
fake responses, without a real daemon, provider, key, or clipboard.
The [process acceptance fixture](PARALLEL_ACCEPTANCE.md) additionally holds a real
foreground synthesis HTTP request open while the host notice is published; its
model responses are local fakes and the synthesis delegation is seeded replay.
