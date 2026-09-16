# Local Research Context

Ghost can retrieve local research evidence through the private model broker.
This is not a new memory service, a conversation-log importer, or a proof engine.
Ghost never opens a database. Ordinary chat and default worker profiles do not
install `history` or add its instructions.

## Host Configuration

The internal Rust host explicitly enables `agents::Control::Resource` with
`ModelBroker::with_controls`. The existing private bootstrap advertises that
capability. Ghost then installs the separate `history` package, not an
`agents.history` action. Existing coordination tools filter out the resource
capability. Installation still does not bypass normal tool policy.

Artifact retrieval additionally requires
`ModelBroker::with_research_artifacts(Arc<ArtifactStore>)`. Pass the existing
host-owned artifact store, not a new store or worker-selected path. With no
artifact backend, explicit artifact queries are denied; attempts and findings
remain available. The explicit [campaign manifest](CAMPAIGNS.md) can opt in with
`children.history: true`, using that campaign's host-owned artifact store. This
does not add a TUI route, general history-query daemon IPC request, or automatic
campaign activation policy.

Requests use the existing tagged private Control transport:
`Request::Resource { request: context::Request }`. The host allowlist, live
permit, exact reservation, request class, current grant, closed/revoked/paused
state, and admitted funding are checked at the boundary. Storage and artifact
validation run on Tokio's blocking pool. No synchronous database guard crosses
an await.

## Authority

The permit supplies the campaign. Models cannot select a research ID or a
campaign ID. ResourceRefs and cursors are locators, not authority. A resource
capability permits evidence retrieval across Work in that exact campaign,
including prior attempts, not just direct children. It does not authorize
cross-campaign retrieval, even within the same Research. Missing scope is
denied. Verification and compaction permits cannot use this boundary.

The trusted synchronous facade is `RuntimeStore::host_research_context` in
`runtime_store/research_context.rs`. Direct host callers must supply an already
authorized campaign and dispatch synchronous work off the async runtime.

## Model Interface

```json
{"action":"attempts","query":{"limit":8}}
```

Actions are `search`, `attempts`, `findings`, `artifacts`, `traces`, `documents`, and `read`. List actions
take `query` with `limit` (1..16), optional `literal`, `after`, `since_ms`, and
`version`. Literal search is case-sensitive substring matching of the bounded
resource representation, not regex, token ranking, or semantic search.

Pages contain `resources` and `next_cursor`. Keep the same filters when following
a cursor. An empty page with a cursor is not end-of-history. Each resource has
a typed `reference`, an optional evidence timestamp, and bounded `data`.
References include kind, Work ID, causal attempt/artifact/finding ID, and version.
Read them through `history.read` or `ctx.read(resource=...)`, never as live output refs.

```python
h = require('history')
p = await h.attempts(query={'limit': 8})
```

Python methods are generated from the Rust tool schema and use the existing
generic bridge. There is no Python database client, duplicate implementation,
or special kernel code. Results have the normal native ToolResult envelope;
the page is JSON in its `content` field.

### Unified Context Navigation

`ctx` also routes explicit scoped list/search through this same history adapter:

```python
ctx = require('ctx')
p = await ctx.list(scope='campaign', kinds=['document', 'artifact'], limit=8)
p = await ctx.search(query='observation', scope='current_work', limit=8)
```

Scope is an enum, not a caller-supplied campaign identity. `campaign` means the
permit's campaign; `current_work` additionally filters by the host ToolContext's
Work ID. No new grant or database exists. Both require installed/enabled history
and the private Resource grant. Without scope/kinds, `ctx.list()` still lists live
output refs and `ctx.search(reference=..., query=...)` searches live byte pages.
Supplying kinds alone selects durable current Work retrieval.

Kinds are the existing `attempt`, `finding`, `artifact`, `trace`, and `document`;
observations use attempt/trace kinds. No unsupported observation or snapshot kind
is advertised. Each call fetches one existing bounded Search page and filters it,
preserving its opaque string `next_cursor`, including on empty pages. Limits are
1..16 pre-filter resources, 256 literal bytes, 1024 cursor bytes and the existing
8 KiB resource reply. Keep scope/kinds/query unchanged across cursor calls. These
are live index pages, not a frozen snapshot, and search never scans raw object
bytes. Artifact visibility remains Ready candidate evidence, not every registered
file. Upstream retention gaps remain gaps.

## Durability

`history.snapshot` lists both pre-inference diagnostics and host stopping snapshots
through the same checksum-verified Trace resources. Stopping snapshots carry
`data.source: host_stop` and a typed `stopping` payload; only the exact stopping
reference committed in the current execution can authorize explicit continuation.
The host records this boundary without inference, independently of compaction.
Retained artifact refs resolve by exact Ready metadata and hash even when the
artifact was not selected as candidate output. Optional worker `final_context`
observations captured before teardown are labeled `final`; old results remain
`stale` or `unknown`. Final does not mean trusted: the host validates metadata bounds
and filters live IDs without explicit same-Work/attempt/generation retained mappings,
marking `worker_observations_filtered`. Package versions are informational, never
permission grants or installed-version authority. Canonical coordination and budget
fields still come from host records, including on failed collection.
See [CONTINUATION](CONTINUATION.md) for inventory and publication failure bounds.

Successful private Ghost Work exports retained output spools before publishing its
answer. Tool/process teardown finishes while a snapshot keeps anonymous files
alive; Rust hashes and sends raw 32 KiB pages over the authenticated private broker
stream. No guest filesystem path, database handle, or new credential is exposed.
Export requires a current Work permit and Final accounting for its model holds.

Private `ResourceUpload` begin/chunk/finish declares exact retained length and
SHA-256, validates sequential offsets and bounded chunks, and optionally checks a
repeated final digest. A blocking storage actor with a one-command queue owns each
staged file. It flushes/syncs and checks on-disk length/hash before committing a
readable `ResourceRef`; no database transaction spans network I/O. Reservations
use the existing trace table in `runtime.redb`, not another database. Staging counts
toward existing 256 MiB campaign / 1 GiB global defaults and 20,000-record/file
bounds. A Work exports at most 256 resources / 128 MiB, each object at most 64 MiB.
Process retention is at most 64 MiB combined stdout/stderr (32 MiB each), separately
from small emitted previews. Completed processes release unused reserved capacity
without evicting retained bytes.

Find descriptors with `history.traces` or `history.search`, using the original
`output:...` ID as `query.literal`. `data.live_handle_id` maps that handle to the
returned exact Work/attempt/generation-scoped reference. Read it through
`history.read` or authorized external `ctx.read`, including after Work exit and
database reopen. Native workspace read/list/search envelopes are also retained
when capacity permits, not bytes those tools never read or already discarded.

Descriptors report retained, total and discarded bytes, storage failure, and
`retention_state`. Worker coverage counts are informational, not proof of what a
malicious process emitted. `ready` proves declared retained bytes were committed,
not that upstream output was complete. Bytes discarded after a cap are unrecoverable.
Raw spools may contain sensitive tool data: unlike emitted/model traces, these
exact bytes are not credential/bearer-redacted.

Errors/cancellation skip export and prevent a successful Ghost answer. Interrupted
uploads become gap descriptors; staged files and their quota charges are retained.
Crash reservations remain charged and hidden; expired five-minute leases become
gaps on subsequent upload admission, never promoted or automatically reclaimed.
Expiry is not proof that retained bytes are absent. Uploads have a host
deadline of at most four minutes within the Work deadline. Launch failures record
`output_retention_gap` when storage permits. If disk/quota failure prevents even
that record, the launch still fails, never claiming successful full retention.
There is no unbounded retry, eviction, process replay, or recovery of discarded bytes.

Private launches now persist tool request/result events before WorkResult collection.
`traces` lists immutable descriptors; `read` retrieves exact hash-versioned bytes.
`documents` lists explicitly registered host-managed inputs. Both are included in
`search` (descriptor search, not a full-text scan of raw bytes). See [TRACE](TRACE.md)
for capture boundaries, storage capacities, redaction and partial snapshot semantics.

`WorkRequest.context_refs` is an optional list of up to four explicit exact refs.
The private launcher validates scope and readability before launch. Dynamic child
proposals retain these handles alongside their existing bounded input pages.
The host-only `host_register_input_document` snapshots an exact allowlisted
`ChildInput` file through the existing ArtifactStore and records its descriptor
in runtime.redb. This is an internal upload adapter, not an automatic import of
every managed file or a new CLI/IPC upload endpoint. Document reads require the
same configured artifact backend; documents are limited to 4 MiB each.

Attempts are queried from existing current execution records and immutable
retained command-attempt history in `runtime.redb`. No second attempt journal,
outbox, or long-term memory copy is written. Existing command lifecycle hooks
record an evidence timestamp in the same commit as command evidence and retain
it with the predecessor on repair. Old records remain readable with an unknown
timestamp; `since_ms` excludes unknown timestamps rather than fabricating them.

A small campaign/Work index is updated atomically at admission. Existing
admissions are indexed once on database initialization, with a durable migration
marker. Queries never scan unrelated campaigns. The index stores identifiers,
not attempt content.

Observations expose phase, generation, instruction revision, predecessor,
command outcome, exit code, elapsed time, evaluator configuration hash, and
candidate hash. Raw stdout/stderr, worker result prose, conversation logs,
credentials, and provider configuration are not projected into attempt data or injected into
prompts. Existing typed conversation history and curated memory are unchanged.

## Authorship And Lineage

`host_record_finding` creates an immutable, idempotent host-authored finding in
the existing runtime database. It requires an author, nonblank claim, nonblank
conditions, and 1..8 exact scoped evidence references. Attempts used as evidence
must be terminal observations. Optional parent references support derived claims.
The host validates references and bounds, not the logical truth of the claim.
Changing a finding under an existing ID is rejected. There is no model-facing
finding creation action. A failed command never automatically becomes a finding,
theorem, or general impossibility claim.

`host_record_candidate_lineage` records optional, immutable candidate ancestry.
Declare roots with no parents, then descendants with up to eight already-declared
parents. Each reference must resolve to a scoped, validated Ready candidate.
This ordering and immutability prevent cycles. Attempt predecessor links are
projected directly from retained execution history; candidate ancestry is never
inferred from failure. Findings and explicit lineage are canonical authored
metadata, not copies of evidence or a second memory store.

## Bounds And Limits

- The complete resource Control reply is at most 8 KiB, including JSON escaping
  and its transport envelope. Page item and byte limits are both enforced.
- Each page scans at most 64 campaign Work/finding rows. A Work's retained
  history is capped at 32 predecessors; candidate references at 32 per current
  attempt. A cursor permits continued bounded retrieval.
- Reads return at most 1024 artifact bytes as a byte array. Before returning a
  candidate artifact, its Ready version, identity, size and stored checksum are
  validated. Research retrieval rejects artifacts larger than 4 MiB to bound
  checksum work; larger objects remain in the existing host artifact store.
- Artifact lists cover retained candidate references, not every file, process
  output, or manifest entry. Non-Ready artifacts and unbound references are not
  exposed. Filesystem paths are never accepted.
- Findings are capped at 1024 per campaign, 1024 bytes each for claim/conditions,
  and 6000 serialized bytes per resource. Derived findings still require explicit
  conditions and evidence; no theorem checker is supplied.
- Only command evidence has durable observation timestamps initially. Historical
  intermediate phases of a live attempt are not retained as separate versions;
  a stale intermediate-phase reference is denied rather than rebound.
- Queries are live pages, not a multi-request snapshot. Concurrent new records
  or phase changes may require starting a fresh query. No cross-Research export,
  global recall, raw-log search, background summarization, or automatic prompt
  injection is implemented.

## Verification

Focused tests cover reopen, retained attempts, literal/version/time filtering,
scan and serialized-byte pagination, authored/derived conditions, lineage,
artifact versions, private broker allowlists/revocation/scope, and Rust-generated
Python methods. The real Ghost subprocess test uses a localhost fake model and
real redb reopened after seeding retained lifecycle observations:

```sh
cargo test -p tachyond research_context
cargo test -p ghost history_python_methods
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_ghost_research_context_queries_retained_attempts_after_reopen -- --ignored
```

No external provider, daemon restart, installation, or UI is needed.
