# Durable Local Trace Resources

The private host launcher streams validated `ToolStarted` and `ToolFinished`
events into immutable host files before returning collected Work evidence.
This preserves submitted native tool arguments, Python cell code and emitted
tool results after Work ends and after runtime.redb reopens. Ghost never opens
redb. Retrieval requires the existing live, scoped Broker Resource permission;
references are locators, not permission grants.

```python
import json
h = require('history')
p = json.loads((await h.traces(query={'limit': 8}))['content'])
ref = p['resources'][0]['reference']
data = json.loads((await h.read(resource=ref, offset=0, limit=1024))['content'])
```

## Storage And Bounds

- Metadata and exact launch assignments use tables in existing runtime.redb.
- Objects live beside that database in `runtime.traces/`, private mode 0700;
  files are exclusively created, synced, mode 0400. The directory and database
  must remain host-controlled, outside worker-writable roots. This is not an
  isolation boundary against hostile same-UID processes or privileged mounts.
- Trace IDs derive from host campaign, Work, generation, assignment, attempt,
  call and request/result phase. Versions are SHA-256 of retained redacted bytes.
  Descriptors include schema version, original and retained byte counts,
  truncation count, original redacted-content hash and redaction policy.
- Maximum retained object size is 64 MiB. The private event frame is also capped
  at 64 MiB including JSON encoding. Oversized frames fail closed, not partially
  parsed. Upstream tools may impose smaller limits before emitting their event.
- Defaults are 256 MiB per campaign and 1 GiB of physical trace files per runtime
  store. Host environment settings `TACHYON_TRACE_CAMPAIGN_BYTES` and
  `TACHYON_TRACE_GLOBAL_BYTES` can change these finite limits at store open.
  Both must be positive, campaign <= global, global <= 64 GiB. No model override.
- Admission of each object is serialized with its scope fence and quota check.
  There are at most 20,000 descriptors and 20,000 files; retained orphans count
  against physical capacity. Documents share descriptor/campaign capacity and
  use the existing artifact store's separate physical cap.
- Storage capacity is not money. It neither consumes nor enlarges campaign token
  or monetary allocations, and storage errors do not release unknown spending.
- No automatic deletion, eviction, compression, vector database or background
  summarizer. Host retention must preserve referenced immutable objects.
- Each read returns at most 1024 bytes, with byte offsets and next_offset. The
  complete broker reply stays within 8 KiB. Files are size/type/hash checked.
  Listings/search scan bounded descriptor rows, not entire raw objects.

## Capture And Failure Semantics

Capture uses bounded backpressure, not an unbounded queue: one event is validated
and written on Tokio's blocking pool while the broker continues servicing model
and control I/O. Semantic Work events have a separate bounded collection budget;
large tool events do not consume it. Raw tool events beyond the old collection
budget are omitted from the returned in-memory event list only after durable
storage. There is not yet a lossy saturation queue or a queue-drop counter.

Envelope spoofing, conflicting event IDs, conflicting call/phase content, stale
generation/assignment and old execution identities fail closed. Identical event
replay is idempotent. Disk/create/sync/commit errors propagate from the source
store and cannot report durable capture success. A crash can leave an unreferenced
object; it is never automatically promoted. Successful earlier events remain
readable even when a later event or Work fails.

Capture never serializes model configuration, launch environment, socket
bootstrap/handshake or complete raw model/system messages. The active provider
credential and bearer-token text are redacted from tool arguments/results.
User-authored code and intentional input content are otherwise preserved: this
is not a general secret detector. Explicit uploaded documents retain their exact
allowlisted hash; hosts must choose which inputs are appropriate to retain.

## Partial State Only

Attempt descriptors expose a model-independent metadata snapshot: instruction
revision, explicit context refs, constrained permissions when recorded, deadline,
lifetime class and budget upper bound. These are persisted policy observations,
not an arbitrary runtime checkpoint or an assertion of currently active tools.
Existing coordination/attention stores retain question and revision state; this
change does not package those stores or activated capability state into a new
reattachable snapshot.

Live pages can change as background Work writes events or advances phases. A
query is not a cross-page transaction; restart the query to discover new rows
that sort before its cursor. Search matches descriptor metadata only.

Emitted-event capture cannot recover bytes already discarded upstream. Successful
private Ghost Work additionally exports retained anonymous process spools through
bounded authenticated frames, with original live-handle mappings, staged quota
admission and hash-checked Ready publication; see [HISTORY](HISTORY.md). Those raw
spool bytes are exact, not redacted emitted events. Per-hostcall Python internals
and full model invocation transcripts are not exported. `ctx` live references
remain Work-local; use `history.read` or authorized external `ctx.read` with durable
ResourceRefs. No Python kernel or process can be
reattached after restart, and unknown operations are never replayed.

## Verification

```sh
cargo test --workspace --offline
cargo build -p ghost --bin ghost --offline
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond --offline -- --ignored
```

The native/Python boundary fixture performs actual tool calls against a local
fake model, reopens runtime.redb, then reads submitted code and stdout through
the same host history facade. Storage tests cover explicit document hashes,
source deletion/reopen, scope denials, duplicates/conflicts, stale assignments,
bounded pages, truncation and disk/capacity errors. No real provider is needed.
