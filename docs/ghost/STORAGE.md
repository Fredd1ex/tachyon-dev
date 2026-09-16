# Local Storage And Retention

## Implemented Boundary

`RetainedStorage` is the host-owned cross-store admission service. Its immutable
resource receipts and campaign limits live in `runtime.redb` (`retained_refs_v1`
and `retained_limits_v1`). Artifact publication, trace objects/uploads, and managed
profile input snapshots reserve expected logical bytes before writing content.
One runtime writer serializes their combined host-root and campaign admission.
The caller binds campaign, resource kind and immutable ID from host authority;
resource IDs and receipts are not worker credentials or budget grants.

The strict campaign manifest accepts optional positive integer
`retained_storage_bytes`. Omission uses 64 GiB, including old manifests. Host
configuration `[campaign_resources].max_retained_storage_bytes` also defaults to
64 GiB and must be positive. The root cap spans campaigns and ordinary host
artifact publication; a campaign cannot increase it. Both limits apply alongside
the existing artifact (1 GiB/object, 10 GiB/store path lengths), trace, and
per-profile snapshot caps. The finite defaults do not reduce those existing
single-store defaults; aggregate retention across campaigns can exhaust the root.

This is **retained-storage admission, not a whole-filesystem disk quota**. Live
anonymous output, arbitrary native/Python writes to workspace or HOME, unmanaged
local modifications, verification scratch copies, database/metadata overhead and
filesystem allocation overhead are outside this byte ledger. Original aggregate
artifact/log/workspace physical enforcement still needs OS isolation or disk
quotas. Native same-user processes are not sandboxed by this feature.

### Publication And Recovery

ArtifactStore keeps its own `manifest.redb`. The host opens campaign stores with a
bound retained-storage adapter, commits runtime reservation intent, then copies
and publishes artifact metadata, then marks the receipt Ready. This is **not** a
two-database atomic commit. Trace staging intent and its reservation commit before
file creation; trace metadata and Ready reconciliation share a later runtime
transaction. Upload network waits never hold a database transaction. The blocking
artifact callback must not run under an already-held runtime writer.
Runtime candidate publication and managed-document registration reject injected
stores without the same runtime ledger and campaign binding. Standalone library
stores retain their per-store caps for non-runtime callers and tests.
Upload and candidate-artifact publication recheck the caller's deadline/cancellation
after reservation commits and before object creation. A reservation that completes
after cancellation remains charged without starting the content write. Copies
already in progress may finish; a cancelled waiter cannot install late candidate refs.

Identical resource reservation replay charges once; different expected bytes or
immutable identity conflict. Exclusive staging/object creation prevents an old
in-flight or crash retry from overwriting the object. Failed staging, source
mutation, digest failure, publication failure, expired uploads and interrupted
work remain charged at the reserved amount. Only authoritative Ready metadata
allows reconciliation to actual bytes, never above the reservation. There is no
automatic refund, deletion, or operator release endpoint in this implementation.
A future release must prove absence under exclusive host authority; expiry,
archiving, worker assertions and filesystem errors are not such proof.

Managed snapshots retain their separate file/input quotas. Before writing their
first marker, the host persists a reservation with the descriptor hash and the
immutable input path/hash/observed-size list. Expected bytes include input sizes
and both ownership markers. Copy checks lengths and hashes before publication;
the durable prepared marker precedes Ready reconciliation. Prepared replay uses
the original receipt and does not reread changed sources. Quarantining an
interrupted copy retains its first charge; recopy needs a separate reservation.
Committed proposal restore retains its existing charge without recopying.

Startup adopts existing trace metadata, artifact registrations and managed
proposal/preparation metadata before normal daemon admission. Known trace and
artifact orphans are charged separately. Old snapshot descriptors lack exact
sizes, so their original input upper bound plus 512 KiB of marker allowance is
conservatively charged; quarantine implies an additional possible copy. Adoption
is idempotent and may exceed new limits: that debt denies new reservations without
deleting old content. Unknown unindexed snapshot trees and modifications made
outside managed publication are not a general filesystem census.
Census rejects symlink/nonregular artifact and trace entries. Missing or invalid
objects retain their metadata charge but cannot newly reconcile a receipt as Ready.
A reconciled snapshot whose directory is missing cannot reuse its reduced charge
to write another copy.

### Accounting Unit

Each artifact registration is one logical retained resource, including its
retained hardlinked staging alias. Different registration IDs count separately,
even for identical content. Trace objects count once; document descriptors that
reference ArtifactStore do not count as a second copy. Managed snapshot copies,
including retained quarantine plus replacement, count independently. This is not
physical-block accounting, compression accounting or cross-resource deduplication.
Existing artifact path-length limits remain more conservative about hardlinks.

`runtime.redb` stores independent, durable archive markers in `local_retention_v1`.
An absent marker means unarchived, including existing databases. Campaign outcome,
Research metadata, references, receipts, authorization records, artifacts, traces,
snapshots and quarantine remain unchanged. Archive is neither cancellation nor
successful completion. Restore does not launch or replay anything and grants no
new permissions or budget.

## Operator Commands

```sh
tachyon campaign inspect campaign-<id>
tachyon campaign retention campaign-<id>
tachyon campaign archive campaign-<id>
tachyon campaign restore campaign-<id>
tachyon campaign archive research-<id>
tachyon campaign restore research-<id>
```

These commands use the existing same-user host IPC boundary, not worker tools.
`local_retention_set` requires an explicit boolean `archived`; `local_retention_get`
queries it. IDs are not credentials. Identical set operations are idempotent;
conflicting operator changes serialize, with the last committed change winning.
Use the query after an ambiguous acknowledgment rather than assuming a result.

Archive refuses a live owned scheduler, Running/Cancelling/AwaitingAcceptance
status, unsettled execution, queued/unknown dispatch, registered work lacking
settled execution, or a staging trace upload. Uncertain execution must first be
reconciled. Research can be archived only after every child campaign is archived.
Restore Research before restoring its campaigns. New campaigns cannot be created
under archived Research. Launch and retention share the lifecycle mutex and the
serialized database writer; archived campaigns cannot transition to Running or
pass the normal transactional campaign admission/status gate.

There is **no permanent purge command**. Archive and restore delete no files or
database records, and archived records remain in existing lists. No retention
timer or automatic campaign wipe is installed. Failed/expired trace uploads retain
their partial object and upper-byte charge as a gap, rather than deleting it on
disconnect or lease expiry. Expiry is not evidence of absent bytes. Full storage
can therefore refuse further admission; archiving does not free space. The TUI's
independent session history is untouched.

## Inspect Counts

`campaign inspect` supports drafts as well as launched campaigns. Its
`storage_inventory` diagnostic is a bounded metadata observation, not a cross-store
transaction, full disk census, integrity verification, or allocation ledger:

- Artifact object and staging **path lengths** include unregistered orphan files.
  Each path is charged separately, even if it is a hardlink to another path. This
  conservative logical policy matches existing artifact admission; it is not
  physical allocated-block accounting or content deduplication.
- Trace Ready, staging reservation upper lengths, and gap/uncertain recorded
  lengths are summed separately from durable metadata. A partial failed upload
  retains its declared upper length, not a falsely exact actual length.
- Document trace descriptors reference ArtifactStore; their byte counts are
  reported separately and excluded from the combined charge to avoid double
  counting a reference as another copy.
- `observed_artifact_plus_trace_charge_bytes` adds artifact path lengths and trace
  recorded charges. It is **not total campaign storage**. Artifact inventory
  failures produce an unavailable diagnostic and a null combined count, not zero.
- Managed snapshots/quarantine, verification copies, live anonymous output,
  unindexed trace orphans, native workspace/HOME, database overhead and filesystem
  overhead are explicitly unmeasured by this diagnostic. `complete` remains false.
  `shared_quota_enforced` is true for the retained scope, not for this diagnostic
  path-length sum or the whole filesystem. `retained_admission` separately reports
  authoritative logical charges, unresolved reservations, limits and debt.

Inspect enumerates at most 20,000 artifact entries and 20,000 campaign trace
records and reads no artifact/trace content for accounting. Files may change during
the observation. The daemon's blocking IPC thread performs this work, not a Tokio
event owner. Existing reference reads still perform their original scope/version
checks; archive markers do not broaden or revoke those retrieval grants.

## Remaining Boundaries

No purge, eviction, retention timer, authoritative no-file release command, hard
OS disk enforcement, unmanaged-write interception or arbitrary orphan-tree
adoption is implemented. Archive/restore remains metadata-only. These gaps do not
turn the retained artifact/trace/managed-snapshot ledger into inspect-only
accounting: its reservations are required on the actual daemon publication paths.
