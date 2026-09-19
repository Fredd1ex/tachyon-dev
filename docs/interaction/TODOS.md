# Durable Structured Todos

Todos are flat daemon-owned records in the runtime redb database. They are not
Markdown files, task scheduler entries, research plans, or worker completion
signals. No watcher, export/sync, automatic completion or full-plan prompt insertion
is implemented by this layer. Operator endpoints and the operational feed are
separate adapters over this authority, not separate todo stores.

## Conversation

Conversation exposes the native `todo` tool with `list`, `add`, and `update`.
The optional `scope` selector defaults to `current_conversation`; the host binds
it to the current foreground input's `conversation_id`, not transcript text or
model-supplied identity. Work, arbitrary conversation IDs, and campaign selectors
are rejected. The separate `campaign` tool can inspect a bounded plan for an
explicitly linked authorized campaign with `status` and `include_plan: true`;
that does not broaden the `todo` tool's scope or grant campaign todo mutation.

Mutations require `command_id` and `expected_revision`: the scope revision for
add, the record revision for update. Structured conflicts remain visible to the
model; it can list relevant records and issue a revised command with a new ID.
The host does not automatically retry mutations. List remains available after
mutations and context replacement, returning durable IDs and revisions on demand.
The daemon's same-user operator transport supplies provenance; model actor and
role claims are rejected. Completing a todo is not Work acceptance, and ordinary
conversation requests do not create campaigns. No Markdown synchronization,
watcher, export, automatic full-plan prompt insertion, or automatic conversation
rotation is involved. Context replacement does not delete durable todo records.

## API

`tachyon_api::todo` exports `TodoRequest`, `TodoResponse`, `TodoError`, `Todo`,
`TodoScope`, `TodoStatus`, `TodoFilter`, `TodoCursor`, and `TodoActor`.
Public operator dispatch uses these types. The private Ghost broker uses strict
identity-free selectors and the same durable facade; see
[Ghost tool usage](../ghost/TODOS_MONITOR.md).

Scopes are strict tagged JSON objects with exactly one variant:

```json
{"kind":"conversation","id":"accepted-conversation-id"}
{"kind":"work","work_id":"admitted-work-id"}
{"kind":"campaign","campaign_id":"persisted-campaign-id"}
```

A conversation needs only a host-accepted nonblank bounded ID. It does not need
a persisted conversation, research, campaign, or turn record. A turn ID is not
part of scope. Work scopes must reference admitted work and its existing,
unarchived campaign. Campaign scopes must reference an existing, unarchived
campaign. There is no automatic scope promotion or union.

Operations use an `operation` discriminator:

- `list`: scope, optional filter, limit, cursor. Default limit 32, maximum 100,
  minimum 1. Filters select one status and/or up to 100 unique exact IDs. Missing
  IDs are omitted; cross-scope IDs never resolve. Empty IDs filter returns empty.
- `add`: scope, command_id, expected_revision, title, description (default empty).
  Expected revision is the exact **scope revision**, initially zero. New status
  is pending and record revision is one.
- `update`: scope, command_id, id, expected_revision, optional title, description,
  status. Expected revision is the exact **record revision**. At least one field
  must be supplied. Empty description clears it. Scope and order are immutable.

Titles are nonblank and at most 512 UTF-8 bytes; descriptions may be empty and
are at most 16,384 bytes. Scope IDs, command IDs, and actor/source labels are
nonblank and at most 256 bytes. NULs are rejected. IDs are daemon-generated UUIDs;
timestamps are daemon wall-clock milliseconds, with updates nondecreasing per
record. Clients cannot set provenance, timestamps, IDs, revisions, or order keys.
Statuses are `pending`, `in_progress`, `blocked`, `completed`, and `cancelled`.
Transitions are explicit edits, including reopening a terminal todo.

Mutation responses contain the record, scope revision, and operational watermark.
List responses contain records, scope revision, watermark, and optional next
cursor. Errors distinguish invalid input, denied authority, not found, revision
conflict, command conflict, stale cursor, and storage failure. Transport adapters
should preserve those distinctions rather than returning successful empty lists.

Command IDs are global to todo mutations. A successful command stores its exact
typed payload, host actor, and original response. An identical replay returns
that response even after later edits or database reopen. A changed payload or
actor conflicts. Replay is checked before current revisions; rejected commands
do not create receipts. Use globally unique command IDs, and reuse them only for
retries of the same operation with the same host provenance.

## Authority And Dispatch

`RuntimeStore::todos(TodoAuthority)` returns a `TodoFacade` whose
`execute(TodoRequest)` checks exact scope equality before any lookup or replay.
Authority is host-only and is not serde-deserializable or accepted on the wire.

- `Bound { scope, actor }`: the trusted facade caller supplies one authorized
  scope and validated source/actor. Accepting a model-supplied scope as a Bound
  grant would be an authorization bug.
- `Ghost { scope, work_id, campaign_id }`: the host supplies immutable runtime
  work/campaign identity and explicitly selects either that work scope or that
  campaign scope. The persisted admission must match the pair. Conversation,
  sibling work, and unrelated campaign grants are denied. The selected scope
  remains exact; granting work does not also grant campaign. Provenance is
  derived as source `ghost`, actor the admitted work ID.

The public daemon operator route may select a scope only after same-user
IPC authorization. Private Ghost routing binds the current Work permit identity,
never claimed identity or implicit grants from tool arguments. `Control::Todo`
allows current Work; `Control::TodoCampaign` independently allows its campaign.
Neither Resource nor monitoring grants todo authority. Conversation scope remains
frontend host/operator-only. Native and generic Python adapters carry no
authoritative plan state. Monitoring uses the durable monitor projection and is
not a todo store, feed, or completion authority.

Synchronous host dispatch core (outer transport adapters preserve typed errors):

```rust,ignore
let authority = TodoAuthority::Bound {
    scope: request.scope().clone(), // ONLY after local operator authorization
    actor: TodoActor { source: "operator".into(), actor: trusted_actor_id },
};
let response = store.todos(authority).and_then(|todos| todos.execute(request));
```

For conversation tools, bind the accepted conversation ID from the host session,
not from model arguments. For Ghost tools, construct `TodoAuthority::Ghost` with
the host runtime identity and an explicitly granted scope. `todos` and `execute`
are synchronous; call them on the existing host blocking thread or in
`spawn_blocking`. No transaction or lock is held across an await. Bindings validate
references at creation; construct one per dispatched operation rather than
caching grants across host lifecycle changes.

## Operator Endpoints

The same-user daemon socket implements `ApiRequest::Todo(TodoRequest)` for
list/add/update, `TodoSnapshot { scope, limit, cursor }` for an unfiltered list,
and `OperationalSubscribe { scope, after }` for durable scoped replay. Responses
preserve `TodoResponse`, `TodoError`, and `OperationalBatch` rather than treating
failures as empty state. The daemon derives operator provenance from the
authenticated UID, not request fields.

`tachyon-client` exposes `Client::{todo, todo_list, todo_add, todo_update,
todo_snapshot}` and `OperationalSubscription::{open, recv}`. Take a snapshot,
then subscribe from its watermark on a separate stream. The subscription scans
at most 100 global feed rows per batch and emits only the authorized scope;
empty batches can advance the global watermark past other scopes. A mismatched
database instance or future sequence is stale and requires a new snapshot.

The [optional TUI TODO tab](../tachyon/OPERATIONAL_VIEWS.md) uses these endpoints
read-only, binding scope to live foreground conversation metadata. It is not a
todo editor or transcript projection. The base Conversation prompt is unchanged;
the native `todo` schema is selected by the Conversation registry and narrowed by
the host to current-conversation scope. Accepting an arbitrary conversation ID
on the operator API does not grant that scope to the model tool. Ghost access remains optional,
with independent exact `Control::Todo` and `Control::TodoCampaign` grants and an
empty default broker allowlist.

Monitoring is separately exposed by `MonitorGet` and `MonitorSubscribe`; see
[Monitoring](../tachyond/MONITORING.md). It is a read-only resource projection,
not a persistent todo event stream or a revision/completion authority.

## Transactions And Feed

Runtime schema remains **v1**. Initialization adds `todos_v1`, `todos_order_v1`,
`todo_scope_revisions_v1`, `todo_receipts_v1`, `operational_events_v1`, and
`feed_metadata` without resetting existing data. Future runtime schema versions
and future todo/receipt record versions are rejected.

Records are indexed by `(scope, id)`, with a second index `(scope, order_key, id)`.
Order keys are daemon-assigned checked `u64` scope sequences, so no floating-point
or caller-selected ordering is involved. Updates keep order keys unchanged. All
revision and event sequence increments are checked for overflow.

Each mutation uses one redb write transaction for record, index, scope revision,
operational sequence, event, and receipt. Failed revisions, conflicting commands,
or late event failures commit none of them. Successful retries append no event.
Concurrent updates with the same expected record revision have one winner.

`runtime_store::operational_events` is the shared durable feed authority:

- `METADATA` is a byte-valued table, distinct from the existing `&str -> u64`
  runtime metadata. Key `operational_v1` holds an `OperationalWatermark` with
  persistent database UUID `instance_id` and last committed global `sequence`.
- `EVENTS` stores versioned `OperationalEvent` JSON by sequence, beginning at one.
  Events carry exact scope, scope revision, timestamp, and a full TodoAdded or
  TodoUpdated record. Todo mutations do not emit unrelated task-history events.
- `append(&WriteTransaction, OperationalEvent)` assigns sequence/instance,
  writes the event and advances metadata inside the caller's transaction. The
  caller must commit; append itself never commits or publishes notifications.
- `watermark(&impl ReadableTable<...>)` reads metadata from a caller-owned read
  or write transaction. The implemented subscription decodes and validates
  event schema versions and preserves database-instance identity.

Lists read records, scope revision, and global feed watermark from the **same
read transaction**. This is the snapshot/subscription handoff boundary. The
watermark on a replayed mutation is deliberately its original commit watermark,
not a new snapshot. Operator subscriptions consume this feed; Ghost todo tools
perform explicit bounded queries rather than subscribing or maintaining a cache.

This feed persists todo events across database reopen. It does not backfill
legacy events or confer durable replay on existing agent/work/foreground streams;
their prior transport guarantees remain unchanged. Monitor ticks are not appended
to this feed. Shared service cancellation and bounded socket writes are described
in [Monitoring service shutdown](../tachyond/MONITORING.md#service-shutdown).

[Campaign assessment](CAMPAIGN_OVERSIGHT.md) can consume a caller-supplied bounded
todo page with a monitor snapshot, but does not query or edit todos itself, execute
model tools, or inject a full plan into normal chat. Markdown plan persistence,
export/sync, watchers, and automatic todo completion remain outside this layer.
The opt-in [campaign host pipeline](../tachyon/CAMPAIGN_OVERSIGHT.md) supplies that
page from its canonical read transaction; it does not grant the model todo mutations.

Pagination uses a typed opaque JSON cursor, without a base64 dependency. It binds
version, database UUID, scope, exact filter, scope revision, and the last order
key/ID. Clients echo it unchanged. Wrong scope/filter/version is invalid; changed
scope revision or database instance is `CursorStale`. Any scope mutation between
pages requires restarting the list, even if it does not match the current filter.
Changes in other scopes do not stale this scope's cursor. Cursors are not authority
tokens; facade authorization is always checked independently.

## Verification

Focused commands (no live daemon, model, installation, or version changes):

```sh
cargo test -p tachyon-api --lib
cargo test -p tachyond --bin tachyond runtime_store::todo::tests
cargo check -p tachyond --bin tachyond
```

Storage tests cover reopen/replay/conflicts, one-winner concurrency, transaction
rollback after staged record/index writes, exact-scope authorization, incorrect
Ghost work/campaign references, bounded exact retrieval, cursor invalidation,
snapshot generation consistency, additive initialization of existing databases,
persistent instance UUIDs, and future schema rejection.
