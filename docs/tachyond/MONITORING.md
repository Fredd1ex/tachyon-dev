# Read-Only Monitoring

The daemon exposes typed `MonitorGet` and `MonitorSubscribe` requests on its
same-user operator Unix socket. The schema is in `tachyon-api/src/monitor.rs`.
`Client::monitor_get` and `MonitorSubscription::{open, recv}` provide typed
client access. The [optional read-only RESOURCES tab](../tachyon/OPERATIONAL_VIEWS.md)
consumes this service without changing the default transcript or chat behavior.

## Authority

`MonitorScope` is `Host`, `Campaign { campaign_id }`, or
`Work { campaign_id, work_id }`. All three currently require the authenticated
operator connection. Selecting a scope does not grant authority. Work lookup
checks both campaign and work identity, and never falls back to Host data.
The private broker exposes selected Work/Campaign snapshots under separate
`Control::Monitor` and `Control::MonitorCampaign` grants. Neither grants this
public operator API or Host scope to workers. `Control::MonitorAvailability`
separately grants aggregate host capacity fields, not host registry identities.

Only allowlisted accounting and registry fields are projected. Prompts,
objectives, results, provider configuration, endpoint URLs, workspace/home
paths, artifact identities, device selectors, and control sockets are not
included. IDs and PIDs are metadata, not capabilities. Storage errors are
returned as typed `Unavailable`, not raw error strings containing paths.

## Sources And Meaning

- Campaign ledger roots supply authorized funding, committed funding,
  available amounts, debt, and pause counts. Work funding is its existing
  allocation allowance, not another root grant. Work and verification pools
  remain separate. Host sums include all persisted roots, not just active
  campaigns.
- Inference reports expose separate final actuals, provisional lower bounds,
  unresolved reserved amounts, and unknown/provisional/final report counts.
  Allocation parents are excluded from actuals and report counts. Admission
  holds that have not converted to allocations remain unresolved ledger holds;
  these counts are not running model calls. The ledger does not account for
  ordinary foreground/background chat, so these are campaign-accounting
  totals, not all host/provider spend.
- Native-job records supply conservative charged CPU/GPU job **wall-time**
  milliseconds and unresolved/finalized record counts. Unknown records retain
  their reserved charge; trusted cleanup may finalize the conservative charge.
  These are not OS CPU measurements, utilization, or proof of running jobs.
- Retained-storage receipts expose unresolved reserved bytes separately from
  ready bytes, with the configured logical limit when known. These cover
  managed artifacts, traces, and snapshots, not measured filesystem usage.
  Receipts have campaign attribution only, so Work storage is `Unknown`.
- Host capacities report campaign slots, resident-worker permits, execution
  permits, model permits, and native CPU/GPU admission slots. `held` means
  occupied capacity, not running work. Fair-capacity unresolved counts include
  retained cleanup debt even when it exceeds physical slots. Native unresolved
  occupancy is reported in the durable native-job section; process-local
  capacity `unresolved` is `Unknown` for native jobs. Native queues are observed
  as stored, without pruning/mutating them during monitoring.
- Host registered entries include ordinary agents without campaigns,
  foreground/background roles, ordinary logical work (IDs prefixed `work:`),
  and the available in-process memory service. A service without a PID has
  `pid: null`; no synthetic worker count is invented. `registered.total` counts
  metadata records, not processes. Campaign/Work registered entries describe
  durable admission states, not inferred execution lifecycle states.

Held and unresolved fair-capacity counts overlap; do not add them. Work-level
pause counts are unknown rather than reporting another allocation's pause
state. Funding availability is an accounting display, not execution permission.

Wide counters and monetary values use checked `u128` accumulation and decimal
JSON **strings**, including `"0"`. `Observed::Unknown` differs from
`Observed::Known(0)`. An existing empty ledger is known zero; absent campaign
accounting is unknown. Empty native/retained tables are known zero within their
documented coverage. Missing campaign or work identity returns `NotFound`.

## Consistency

Each sampler pass opens exactly one runtime read transaction for ledger,
admission, native-job, and retained-storage projections across requested scopes.
Helpers live beside their source tables and accept that read transaction.
Monitoring never calls mutating `progress()`, recovery, dispatch, model calls,
or write-transaction APIs.

`durable.sampled_at_ms` identifies that read snapshot. Each capacity has its own
`sampled_at_ms`; registry metadata has another clock. These process-local
observations are sampled independently after the durable snapshot. There is
**no global atomicity guarantee** across these sources or across pages.
Callers that later need decision-time resources must use a freshly authorized
source projection, not the display sampler cache.

## Sampling And Bounds

One daemon-owned std thread performs shared sampling no faster than once per
second. A pass may take longer when the database is large. It retains a Host
snapshot plus bounded on-demand scope/page projections. There are at most 32
cached queries (including the default Host query) and 64 subscriptions. Idle
on-demand entries expire after 60 seconds or are evicted when unpinned. New
queries wait for the next shared pass; they do not spawn samplers or DB scans.
Limits are explicit typed errors, not silently truncated authority.

Every request requires `limit` in `1..=100`. Registered metadata is returned in
deterministic ID order with exclusive `after` / `next_after` pagination. Only
`limit + 1` detail entries are retained while scanning. Totals cover all matching
records, not just the page. Pagination is a live view; restart it for a newer
complete view. Aggregate accounting necessarily scans persisted root/native/
retained/admission records once per global pass, not per subscriber. Existing
ledger roots are stored as aggregate records and must be decoded individually;
monitoring does not build an unbounded cross-root detail vector. This is not a
constant-cost database census.

The cache compares values while ignoring source timestamps. Unchanged samples
refresh cached clocks without incrementing the version or emitting a message.
Subscribers wait on a condition variable and receive the latest changed value,
not a per-client sample queue. Normal bounded socket buffering still applies.
No monitoring tick is written to the durable operational/todo event feed.

Every subscription starts with a full snapshot, regardless of `after`. Versions
contain a fresh daemon-start UUID epoch and an in-memory sequence; an old epoch
therefore gets a resnapshot, never durable replay. The UUID is unrelated to the
durable operational-feed identity. Clients should replace, not merge, full
snapshots and handle typed errors/disconnects explicitly.

On sampling failure, the snapshot retains its last successful payload and
source clocks, and sets `stale`. Initial failure has no payload. Transitioning
into or out of stale increments the version; repeated identical failures do
not. A successful later sample clears stale. Registry, fair-capacity queue and
retained-debt, compute, and campaign-capacity observations use nonblocking lock
attempts. Contention or poison produces `Unavailable`, never invented zeroes.
Host source failure does not invalidate independent Campaign/Work queries.
No source lock, read transaction, or registry lock is held during socket writes.

## Service Shutdown

The new todo/operational-feed and monitor services share the daemon's existing
signal-handler shutdown atomic, installed before accepting connections. The
operational feed checks it between bounded batches and every 250 ms while idle;
monitor waits check it at least once per second even without a notification or
client traffic. Explicit monitor stop also wakes its condition variable. Shutdown
joins the sampler without a registry lock. Feed handlers retain the existing
connection-thread ownership but observe shared cancellation and release their
store references; monitor handlers release subscriber slots and cache leases.

New-service response writes check cancellation between socket waits of at most
250 ms and enforce a one-second total frame-write deadline, including partial
writes. A stopped or timed-out partial frame closes the connection; it is never
retried on that stream. Previous socket timeout settings are restored. Existing
agent/work/foreground transcript subscription transports are unchanged.

These bounds cover service waits and socket backpressure, not OS scheduling or
synchronous storage I/O/durable scans. Cancellation cannot interrupt an in-flight
redb operation or a blocked filesystem syscall. No wire schema changed.

## Offline Verification

Monitoring tests cover wide decimal actuals/provisionals, known zero versus
unknown, allocation-parent exclusion, scoped work lookup, byte-for-byte database
stability and unchanged ledger/feed revisions, reserved versus ready storage,
native wall charges, secret redaction, ordinary-agent/service metadata, bounded
pages and scope/subscriber counts, stable timestamp-independent versions,
latest-value coalescing, stale preservation, epoch resnapshot, shared cadence,
registry access/shutdown while a subscriber does not read, connected idle-client
cancellation, source-lock contention/poison, and total socket-write deadlines.

```sh
cargo test --offline -p tachyond -p tachyon-api -p tachyon-client monitor
cargo check --workspace --offline
```
