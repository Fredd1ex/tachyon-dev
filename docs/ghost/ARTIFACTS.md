# Durable Artifacts

The production Rust `artifact` tool hashes an existing regular workspace file and
sends `AgentEvent::ArtifactRegistered` through the existing event sink. It now
returns **pending**, not "published". The sink acknowledges queueing only; there
is no registration response channel back to the running tool. Keep the workspace
source unchanged until daemon metadata reports `publication.state = ready`.
Standalone Ghost without daemon event ingestion does not publish durable bytes.
The daemon emits pending before enqueueing a copy. Queue admission is in-memory,
not a durable manifest acknowledgement; get can still return no record until the
publication worker commits pending. A daemon crash can lose queued requests.

The private broker's command-verification wrapper also publishes collected explicit
registrations, after Ghost termination and before candidate evaluation. It uses
bounded `spawn_blocking` publication (two jobs, 32 unique registrations / 16 MiB
per collection), not the daemon event queue. Original hashes and sizes must match;
worker Ready claims are discarded. Candidate paths remain history, while optional
host `candidate_refs` identify the exact Ready IDs. No explicit registration means
no workspace snapshot fallback. See [VERIFICATION.md](VERIFICATION.md) for the
successful first-attempt command path and the remaining retry-loop boundary.

## Storage Authority

`tachyond::artifact_store` owns the snapshot files and its private transactional
redb manifest. Ghost gets neither a database handle nor arbitrary storage paths.
The daemon selects the workspace from its agent registry, checks supplied
generation/assignment against that registry, and stamps task/work provenance.
For assigned workers (assignment greater than zero), both generation and assignment
must be supplied and match. Stale or unfenced events are dropped before correlation,
so an old event cannot become a failure or publication for a replacement Work.
Storage keys are scoped by logical task ID, falling back to agent ID, and by
registration ID. Each ready record carries its SHA-256 content version.

Set `TACHYON_ARTIFACT_ROOT` in the **daemon's** environment to select a canonical
absolute managed directory. The default is `databases_dir()/artifacts`, outside
managed workspaces. The directory must be owned by the daemon user, mode 0700,
and disjoint from the source workspace. Configuration is read once per daemon
process; an initialization failure requires fixing it and restarting.

This relies on the existing host/worker authority boundary: do not mount the
managed root or the host control socket into untrusted workers. Mode 0700 and
read-only objects are not protection against unsandboxed code running under the
same UID as the daemon. No sandbox grants or mounts are added by this feature.

## Publication Protocol

1. Validate the relative registered source path, bounded size, digest, and ID.
2. Open the source using descriptor-relative traversal with `O_NOFOLLOW` at every
   component. Reject parent traversal, absolute paths, symlinks and special files.
3. Commit a pending manifest transaction, then stream a bounded copy into a
   create-new staging file while computing SHA-256.
4. Require the requested checksum and size, stable source inode/timestamps, and
   a fresh descriptor-relative path check. A stale source fails closed.
5. Make the staged file read-only and sync it. Hard-link **the staged copy**, not
   the workspace source, into a no-overwrite object name and sync that directory.
6. Commit ready metadata, then forward the event containing that metadata.

The event owner performs no store initialization, reconciliation, file copy,
hashing, or manifest transaction. A single dedicated publication thread consumes
at most 32 queued jobs using nonblocking admission. A full/disconnected queue
emits failed after pending without admitting the request to storage. There is no
blocking response channel back through the coordinator. Results have a new daemon
system event identity and retain the original task/assignment correlation and
subscribers; they are not reinterpreted against a subsequently assigned worker.
Pending is sent before the job can run, and ready is sent only after commit.
Completion events use an artifact-publication session namespace, avoiding reuse
of a worker's session-local event identity. Subscribers joining after admission
must use get/list for acknowledgement; publication is not a replayable event log.

Queued and active jobs defer daemon workspace cleanup until the last job finishes.
This is a cleanup lease, not a frozen filesystem: running code or a host can still
change/delete the source, and a worker reassignment can still mutate it. The copy
then fails closed rather than claiming to have saved the registered version.
Shutdown does not drain the in-memory queue or guarantee a final event delivery.

The filesystem and redb are **not one atomic transaction**. On opening the store,
pending records with verified published objects become ready. Other pending
records become explicitly failed. Unknown objects and staging files are retained,
never silently promoted or automatically deleted. Failed requests need a new
registration ID to retry. A metadata acknowledgement can be lost after commit;
querying the ID or replaying the identical request returns the committed record.
If the ready transaction and the subsequent failed transaction both fail, the
event reports failure and the durable record remains pending, never falsely ready.
Recovery may later promote it only after verifying the published object against
the recorded size and SHA-256; a missing hash cannot be promoted.
Preflight rejection emits failed metadata on the event without creating a manifest
record; a missing lookup is not evidence of successful publication.
Same-ID conflicting requests fail; concurrent identical requests are serialized
and idempotent within the owning daemon. Redb excludes a second store owner.

Ready snapshots survive source mutation and workspace deletion. Reads verify the
bounded object checksum and fail closed on missing or corrupt bytes. The host
administrator remains capable of altering storage; this is not a WORM device.

## Typed Host API

Registration uses the existing typed event plus the host-only `register` method;
there is no request accepting an arbitrary caller-selected workspace or DB path.
The existing daemon control API adds these requests:

```json
{"cmd":"artifact_get","scope":"work-id","id":"registration-id"}
{"cmd":"artifact_list","scope":"work-id","after":null,"limit":50}
{"cmd":"artifact_read","scope":"work-id","id":"registration-id","offset":0,"limit":65536}
```

Responses are typed `Artifact`, `ArtifactList`, and `ArtifactBytes` (JSON byte
array). Get is the publication acknowledgement lookup. List returns at most 100
records, ordered by opaque hashed registration ID; pass the last returned ID as
`after`. Read returns at most 1 MiB. No archive expansion or unbounded read exists.
These are trusted same-user host control endpoints, not worker-authenticated
capabilities; knowing a scope is not a new security boundary on that socket.
Any caller with access to the general control socket can select another work's
scope and retrieve its metadata/bytes. There is no principal-to-work authorization
check, and the typed API does not add one. Do not expose it to untrusted callers;
per-worker access would require authenticated, restricted transport/capabilities.
`tachyon-client::Client` exposes get/list/read through typed public methods (and
the generic request method), never a raw manifest database or filesystem path.

## Bounds And Retention

Each snapshot is capped at 1 GiB in the daemon (the tool may impose a lower policy
limit). The store has a conservative 10 GiB file-byte cap and 20,000-file admission
cap, counting retained staging and objects. Hard-linked staging/object bytes are
deliberately counted twice. The manifest separately admits at most 20,000 records,
including failed/pending records with no files. Updates to existing records remain
possible at the cap. Admission scans at most 20,000 physical entries; scoped list
uses an indexed range and reads only the requested page. Reconciliation refuses
an over-cap manifest rather than scanning an unbounded index.
No automatic pruning, garbage collection or campaign
storage ledger is implemented. Operators must use explicit host retention action
when full; deleting workspaces does not free published artifacts. The caps do not
constitute per-work/per-campaign budget accounting or a strict bound on redb/WAL
overhead. Reconciliation is lazy on first artifact event/query after restart and
does not retry copying from mutable sources.

## Current Limits

- The tool's asynchronous result remains pending even when the daemon has already
  committed ready metadata. There is no synchronous Ghost acknowledgement loop.
- Ready acknowledgement is a separate artifact event; existing consumers that
  ignore the additive publication field may still display legacy registration
  wording. Legacy registrations deserialize as pending, never ready.
- Sources are copied when the daemon handles the event, not at the tool's hash
  instant. A changed or deleted source fails instead of publishing another version.
- Every bounded read currently verifies the entire object (up to 1 GiB) on the
  requesting IPC connection thread. It does not hold the registry lock, but small
  repeated reads can be expensive; no read cache or per-caller rate limit exists.
- Reconciliation retains orphans for operator inspection; no retention-management
  endpoint or automatic orphan deletion is included.
- Files only; directory manifests and archive extraction are unsupported.
- Automatic publication is wired to the daemon's supervised `push_event` path.
  The separate one-shot `ModelBroker::launch_private` path collects event vectors
  without that handler; its host must explicitly use `ArtifactStore::register`.
  No campaign/broker runtime-store code is changed by this implementation.
