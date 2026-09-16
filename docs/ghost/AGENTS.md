# Scoped Agent Coordination

`runtime_store/coordination.rs` remains the host-owned durable control boundary.
The private broker now exposes a **partial** `agents` capability: status, list,
result, send, steer, cancel, wait, group_status, group_resize, and exact approved-catalog
spawn/group admission. `HostAgentControl` is a non-serializable facade bound to
the exact current Work permit, never to a model-selected actor or campaign.
Native and Python calls execute the same native tool validation and typed requests
in `tachyon_api::agents`. Work addresses and command IDs are not credentials.

## Usable Interface

The trusted host must enroll Work before dispatch and explicitly configure
`ModelBroker::new(store, model).with_controls([Control::Status, Control::List,
Control::Result, Control::Send, Control::Steer])`. The default allowlist is empty.
`launch_private` carries the selected action catalog in its private bootstrap.
Ghost registers the `agents` package and enables the native tool only for that
control-enabled broker session. Package installation/loading alone never grants
authority. Ordinary local Ghost chat has no agents tool or package advertised.
There is no model-accessible public IPC grant or automatic scheduler activation.
The explicit host CLI manifest can enable these controls and approved templates;
see [CAMPAIGNS](CAMPAIGNS.md). It does not alter provider setup.
The explicit-catalog Rust host scheduler is described in [GROUPS](GROUPS.md).

Native tool name: `agents`. Exact argument forms:

```json
{"action":"templates","limit":32}
{"action":"spawn","template_id":"approved-child","command_id":"spawn-1"}
{"action":"group","template_id":"approved-batch","command_id":"group-1","max_running":2}
{"action":"wait","work_ids":["child"],"mode":"all","timeout_ms":10000}
{"action":"status","work_id":"child"}
{"action":"list","limit":32,"after":"previous-work-id"}
{"action":"result","work_id":"child"}
{"action":"result","work_id":"child","revision":1}
{"action":"send","work_id":"child","command_id":"message-1","text":"Evidence available"}
{"action":"steer","work_id":"child","command_id":"steer-1","expected_revision":1,"instructions":"Focus on the failing test"}
{"action":"cancel","work_id":"child","generation":1}
{"action":"group_status","group_id":"owned-group"}
{"action":"group_resize","group_id":"owned-group","expected_revision":1,"max_running":0}
```

`after` and `revision` are optional; `limit` is required, from 1 through 32.
Omitting `revision` selects current evidence. IDs are bounded to 256 UTF-8 bytes;
message/instruction bodies to 4096 bytes, not characters. Extra fields are rejected.
Only host-allowed action names appear in the native schema and Python proxy.

```python
agents = require('agents')
child = await agents.spawn(template_id='approved-child', command_id='spawn-1')
batch = await agents.group(template_id='approved-batch', command_id='group-1', max_running=2)
status = await agents.status(work_id='child')
page = await agents.list(limit=32)
result = await agents.result(work_id='child')
receipt = await agents.send(work_id='child', command_id='message-1', text='Evidence available')
steering = await agents.steer(work_id='child', command_id='steer-1',
                             expected_revision=1, instructions='Focus on the failing test')
```

These return the normal tool envelope; `content` contains JSON with `outcome`
(`status`, `list`, `result`, or `accepted`). Result contains a nullable bounded
`snapshot` with optional `research`, never a transcript or raw tool evidence. Accepted receipts contain
`command_id`, `sequence`, and `accepted_revision`; they do not claim delivery,
application or successful completion. Automatic delivery occurs at the recipient's
next permitted model boundary, not when `send`/`steer` returns. Host denials are
sanitized permission errors. Reuse a command ID only for the identical payload;
an interrupted call may already have committed. The bridge does not auto-retry.

`research` contains stored outcome and summary (at most 2 KiB of UTF-8), up to
eight evidence resource refs and eight host-resolved immutable candidate IDs,
and `truncated` when the projection is shortened. The serialized broker result
fits 4 KiB. `availability` is `available`, `pending`, or `unavailable`; absent
terminal evidence is not an answer. Errors retain their stored wording, not a
scientific interpretation. `unresolved_questions` is null when not recorded.
Old snapshots deserialize with `research: null`. Stored evidence is unchanged.

`work_usage` and `verification_usage` separately sum authoritative Final model
request records for the execution's respective funding allocations, not worker
telemetry. Input/output tokens and integer microUSD costs are exact decimal
strings. Missing records yield null, not free execution; `uncertain` means an
unresolved request or unsettled execution, so displayed sums may be partial.
These are allocation totals across attempts, not per-revision billing.
Accepted, applied and delivered instruction revisions are reported separately;
currentness still compares the candidate's canonical revision against the latest
accepted revision. Explicit revision selection retains its existing semantics.

Opt in separately with `Control::Cancel`, `Control::GroupStatus`, and
`Control::GroupResize`. Cancel requires exact generation and direct parent ownership;
it is idempotent intent, not terminal proof. Scheduler-owned tasks receive cancellation
through the shared broker registry. Outside this runner, the host must drive cleanup;
durable intent alone does not interrupt existing I/O. Group operations require every
affected descendant Work to be an explicitly enrolled direct child of the actor.
They return compact counts/revision, not all member policies. Resize has strict CAS
semantics; on a lost response read status, rather than blindly retry. Zero pauses new
claims, shrinking drains, and no resize grants money. Python generates these methods
from the same native allowed-action schema; no Python scheduler was added.

## Approved Admission

Opt in with `Control::Spawn` and/or `Control::Group`, and have the trusted Rust host
call `HostScheduler::approve(HostTemplate)` before exposing its key to the parent.
`spawn` selects one exact candidate; `group` selects a fixed batch of 1..32 candidates.
The template binds an exact campaign and logical parent Work, candidate identities,
objectives, WorkRequest, model/pricing/allowance policy, verification admissions,
evaluator binding, executable, workspace and HOME. Approval itself admits nothing.
The host can convey approved keys in instructions or enable `Control::Templates`
for `templates(limit, after?)` discovery. It returns only the exact parent's
selectors, group IDs, member counts and running ceilings, never paths or model
policies. Command-bound candidates use the immutable command gate; generic
callback evaluators remain available to internal Rust hosts and tests.

Fixed templates accept no objective or context overrides. Separately approved
dynamic profiles accept bounded objectives and evidence references, as described
below; neither form accepts provider, path, budget, parent or campaign overrides.
Grouping grants no extra allowance. Optional positive `max_running` selects an initial
cap at or below the approved host ceiling; later resize remains under that durable
ceiling and the root cap. Omitting it uses the approved cap.

Success returns `{"outcome":"admitted","command_id":"...","work_ids":["..."],
"group_id":null}` (or the host's group ID). These are durable admitted/queued
handles, **not completion** or a promise that an execution slot is available.
Use status/result for bounded observations. Opt-in `Control::Wait` releases the
parent execution slot while retaining its process and Python state, then reacquires
capacity before returning. See [LIFECYCLE](LIFECYCLE.md) for timeout, resident limits,
cancellation, and the distinction between worker termination and accepted results.

Each template is one-shot with fixed Work identities. Identical command/payload
replay returns the same handles without new holds, queue entries, or launches,
including after reopen and host re-approval. Changing the command or payload after
admission conflicts. Admission command IDs are campaign-scoped across spawn/group
templates (separate from send/steer's existing message namespace). Unknown keys,
wrong kind, another parent/campaign, stale/revoked permits, and policy overrides
are denied. Existing Work cannot be adopted by a newly approved entry.

One transaction commits all candidate and verification reservations, WorkLimits
lifetime counts, group membership, immutable logical parent links, and the receipt.
Any budget, depth, total, or membership failure rolls all of it back. Only after
commit are executable descriptors published to shared bounded host state. The
running scheduler sees them on its next tick without manual child registration.
All spend uses the existing campaign envelope and separate per-Work allocations;
verification still uses the protected pool and predeclared non-spending evaluator.

Instruction delivery is independent of permission to admit or launch children.

## Dynamic Investigations

The host may configure `children.dynamic` in the campaign manifest or call the
internal Rust `approve_profile` adapter. The native tool accepts:

```json
{"action":"spawn","profile_id":"inspect","objective":"Investigate the approved input","context_refs":[],"command_id":"investigate-1"}
{"action":"group","specs":[{"profile_id":"inspect","objective":"Check the approved input","context_refs":[]}],"command_id":"investigate-batch-1","max_running":1}
```

These map to private typed `Request::Propose`/`ProposeGroup`, under the existing
Spawn/Group allowlist. The host supplies profile IDs in instructions; `templates`
does not expose dynamic slots. Profiles are bound to the exact campaign; the root
can select configured profiles, while children inherit only their source profile's
explicit `profile_ids` allowlist (empty by default). Public `children.max_depth`
defaults to one and is bounded to eight. They predeclare per-child work and verification
quotas, permissions, input sources, managed root and finite lifetime slots.
Objectives do not become policy. Groups contain 1..16 proposals; a campaign has
1..31 dynamic slots across at most 16 profiles, shared across every parent and
depth, never multiplied per child. Completed/cancelled slots are not
recycled. Profile objective limits are at most 16384 UTF-8 bytes and context limits
at most four distinct, exact-version attempt/finding/artifact references. References
must resolve within the campaign; history must be enabled when context is allowed.
The host captures bounded evidence pages (1024-byte read limit, 32768-byte combined
serialized context limit), not arbitrary paths or full transcripts.

Each input is an explicit non-hidden relative file plus lowercase SHA-256 under
a canonical approved root. No glob or recursive source traversal is used. Symlinks
and non-regular files are rejected; counts and copied bytes are bounded per child
(at most 1024 files and 16 MiB). Each child gets a separate managed `work` and
`home`; copied inputs appear at `inputs/<source-index>/<relative-path>`. Source
changes fail the digest check. Unowned destinations are never adopted or removed.
This is a local snapshot, **not OS isolation**. Use `coding_read_only` with
`allow_exec:false` and `allow_python:false` for investigations without native
execution. Explicit host-enabled native access retains the documented local-host
risks; this feature does not broaden or sandbox it.

Preparation now durably records the exact request, original policy fingerprint,
captured context and chosen slots before filesystem work. This descriptor holds
slots for that same proposal but grants no money or execution authority. File I/O
runs without a database transaction; admission rechecks ownership and atomically
commits reservations, parent/group mappings and the receipt. Failures retain
staging, never recursively delete it. Identical committed receipt replay restores
descriptors without touching directories, rereading context or reserving again.

Explicit host re-approval plus `restore_proposals`, or `campaign recover`, may
finish uncommitted staging with no execution/gate record and no existing admission.
The read-only ownership marker binds the original campaign, parent, slot, paths,
request/proposal hash, full profile, lifecycle caps and context-bearing descriptor.
An identical fully prepared directory is reused only after checking every file's
SHA-256, byte/file quotas, exact inventory, empty HOME and both markers. Source
changes do not invalidate an already verified snapshot; incomplete staging still
requires bytes matching the original allowlist, never a newly approved digest.
These markers are same-user local integrity checks, not protection from a hostile
process with the host's filesystem/database authority.

Snapshot preparation and reconciliation resolve every configured absolute root
component from an opened `/` directory with `openat` and `O_NOFOLLOW`; symlink
ancestors are rejected, not canonicalized through. The configured path must already
be canonical. The managed root must be daemon-owned and not group/other writable;
its ancestors and input roots must be trusted host-controlled directories. All
subsequent traversal, inventory enumeration, bounded reads, exclusive staging
creation, writes, syncs and no-replace publication/quarantine use pinned descriptors,
with no `/proc` path fallback. Inventory types and device/inode identities come from
opened objects; hashes and markers are read from those same objects. This resolves
the snapshot path-based ancestor/symlink TOCTOU risk within the local trusted-host
boundary: swapping a path for a symlink cannot redirect I/O into its target.

The managed root's configured path association is rechecked before publication and
successful return. An ancestor rename can leave writes in the originally opened,
now detached tree, never in a replacement symlink target. Reconciliation pins the
validated directory and checks its device/inode before and after descriptor-relative
`renameat2(RENAME_NOREPLACE)`. A replacement detected before rename is left in place.
A mismatch detected after rename is retained at the destination for manual review,
without deletion or rollback, and is never acknowledged as a prepared snapshot.
Linux rename cannot atomically compare a source inode: a hostile same-UID process
can still substitute names between checks, mutate open files, move pinned directories
or alter the database. Privileged mount changes are likewise outside this boundary.
These checks are defense in depth, not hard isolation against those authorities;
host-controlled private staging and non-adversarial same-UID access remain required.

Each slot has one fixed `.proposal-<slot>` temporary name and at most one
`.quarantine-<slot>` retained directory. A matching interrupted temporary directory
containing only expected nonsymlink entries can be quarantined and recopied once.
A corrupted prepared snapshot is quarantined and the recovery fails closed.
Markerless directories, aliases, symlinks, conflicting markers and unexpected files
remain unchanged and block recovery. Committed directories are never reconciled
this way, even if a worker's outcome is unknown. No artifacts or user files are
deleted. Existing random-name temporary directories from older staging are not
scanned or adopted.

Retention is finite per approved slot: at most the final directory, one temporary
directory and one quarantine, each host-created snapshot bounded by the original
input quota plus two markers of at most 256 KiB each. The descriptor size limit can
reject a large otherwise valid proposal. A retained quarantine blocks further
preparation until an operator reviews it; there is no eviction, automatic garbage
collection, global disk quota or budget top-up. External same-user writes and
accumulation across separately authorized campaigns need operator storage policy.
This recovers staging and descriptor publication, not processes, Python kernels,
provider calls, unknown charges or application checkpoints.

The Rust localhost fake-model integration exercises actual Ghost children,
read-only inputs, command verification, cap-one wait, cancellation, root completion,
root repair and unknown child usage:

```sh
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_dynamic_children_managed_inputs_wait_verify_and_cancel -- --ignored
```

## Parentage And Admission

`host_admit_agent_work` requires host authorization of the exact admission and
parent. It atomically admits against the existing campaign envelope and configured
group admission limits and installs an immutable explicit Work parent mapping.
Coordination requires explicitly configured WorkLimits before admission; Work
parent chains obey max_depth independently of group ancestry (root Work depth zero).
Group ancestry is not Work parentage. Existing work can enroll only while queued;
an enrolled mapping cannot change. A parent must already be enrolled in the same
campaign, preventing cycles. Coordination adds a maximum of 256 enrolled Works per
campaign. It grants no money, permits, root authority, spawn authorization, or
extra inference capacity. Ordinary admission remains available outside coordination.

## Commands And Observation

`command` accepts typed Send and Steer actions. Send is direct parent/child only;
steering is parent-to-direct-child only. Self, sibling, unrelated-parent, and
cross-campaign messaging are denied. A campaign-scoped command ID replays the exact
original receipt only for identical actor, target and payload, otherwise conflicts.
Messages retain sender/recipient Work identity, sequence and accepted revision.

Messages are at most 4096 text bytes, command IDs at most 256 bytes, with 256
lifetime commands touching each Work and 4096 across each campaign. Receipts count
toward these limits. Full storage rejects new commands without eviction, including
after restart; identical retries still succeed. These are logical storage bounds,
not redb file-size or process RSS limits. Each campaign record is decoded as a
bounded whole; reads currently use serialized transactions, not scalable indexes.

The host-only `messages` method returns the bound actor's inbox/outbox, at most 32 entries after
a stable sequence cursor. `list` returns self and immediate relatives, at most 32
addresses with an exclusive lexical continuation cursor. Neither is a snapshot
across calls. `status` includes admission lifecycle, parent, immutable execution
generation, accepted and acknowledged (host-applied) steering revisions, plus
`delivered_revision` and `delivery_cursor`. Delivery starts at zero until a typed
boundary has been acknowledged. The execution revision
is absent until an execution record exists, rather than inferred from admission. On Linux it
also returns a compact reference to the existing execution evidence, phase,
candidate-presence and settlement, never the transcript or full candidate.
`result` explicitly selects Current or a numbered Revision. Current returns no
reference when only historical evidence exists; it never silently falls back.

## Steering Boundary

Steer compares the expected accepted revision under the serialized writer. One
concurrent command wins. Acceptance is durable queueing, not application.
At the next model boundary, after Ghost's tools complete, the host applies the
latest accepted instructions and reserves the next request atomically. It replaces
the old model permit using the same immutable admission and funding allocation.
No capacity means no application or delivery advancement. Superseded revisions
remain history. Parent messages are bounded, labeled untrusted data; authorized
steering cannot grant tools, credentials, budget or a new objective.

The private typed boundary acknowledgment advances delivery only for the exact
prepared request/cursor. Worker claims never advance application. Host-canonical
revision metadata is recorded after successful model completion and checked when
recognizing `WorkResult`. Results are current against the latest accepted revision,
even while application is pending. Older evidence remains historical; accepting
steering does not itself apply instructions or prove inference under them.
See [LIFECYCLE](LIFECYCLE.md) for ordering, context-window bounds and crash semantics.

Reopening retains parent mappings, commands, capacity counters and revisions. The
host must authenticate again and reissue the facade using the durable address.
Resident wait/resume is wired through the private host broker, not process replay.
Kernel checkpoint recovery, operational crash reconciliation and full public agent
lifecycle exposure remain unimplemented. See [LIFECYCLE](LIFECYCLE.md).
