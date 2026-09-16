# Explicit Local Continuation

`campaign continue` authorizes a **new Ghost process for the same root Work**.
It does not restore a Python kernel, variables, tasks, browser sessions, subprocesses,
or old cells. Retained snapshots and tool output are data, not instructions or
permissions. This is native same-user development execution, **not a sandbox**.

## Commands

Use the already-running daemon and its original immutable launch manifest:

```sh
tachyon campaign status <campaign-id>
tachyon campaign inspect <campaign-id>
sha256sum /absolute/path/to/the/original/ghost
tachyon campaign continue <campaign-id> continuation.json --unisolated-development
tachyon campaign status <campaign-id>
tachyon campaign inspect <campaign-id>
```

`inspect` reports `expected_state_sha256`, execution state, and
`continuation_checkpoint` references. Select the committed host stopping snapshot
for the root's current attempt (`data.source: host_stop`), not a pre-inference
diagnostic snapshot. Inspection marks only the execution record's committed stopping
reference with `current_state: true`; historical and pre-inference snapshots remain
visible with `current_state: false`. Currentness is not eligibility or permission to
continue: settlement, revision, cleanup, billing and deadline checks still apply.
Its exact reference must match the execution record. Inspection
does not start a job or prove that an unknown process has stopped.

The request file is strict JSON, at most 65,536 bytes. Replace all example identities
and hashes below with the inspected values and the hash of the exact approved
executable. `ghost_version` is the Ghost package version compiled into that binary
(the current source tree uses `0.3.0`), not an inferred build attestation.

```json
{
  "schema_version": 1,
  "command_id": "operator-continuation-001",
  "campaign_id": "campaign-00000000000000000000000000000000",
  "checkpoint": {
    "kind": "trace",
    "work_id": "campaign-00000000000000000000000000000000-root",
    "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "version": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  },
  "expected_state_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
  "expected_stopping_reason": "stopped_unverified",
  "executable_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
  "ghost_version": "0.3.0",
  "instruction": "Inspect the retained result without repeating prior side effects.",
  "goal": null
}
```

`instruction` is nonempty and at most 16,384 bytes. It becomes a new host-approved
instruction revision at the stopped boundary. The latest complete accepted
instruction is included at subsequent model boundaries. `goal` is optional; when
provided it must exactly equal the original objective. The Work, generation,
allocations, original manifest, permissions, provider/model, and absolute deadline
are not replaced. There are no budget or deadline extension fields.

## Claims And Recovery

The command payload, new attempt identity, previous attempt history, current
instruction revision, and Running status commit atomically before spawning.
After a lost CLI acknowledgement or Ctrl-C, retry the **identical request with the
same command ID**. It returns current campaign metadata without another launch.
Changing the payload under the same ID is rejected. A different competing command
must pass a fresh state comparison. A crash after claiming does not authorize
automatic restart, even if no process actually started.

The only eligible stopping reason currently is `stopped_unverified`, corresponding
to host-recorded `Reviewed(Unverified)`. Execution/reviewer uncertainty and unknown
provider usage require independently obtained operator reconciliation first:

```sh
tachyon campaign inspect <campaign-id>
tachyon campaign reconcile <campaign-id> receipt.json --unisolated-development --confirm-authoritative
tachyon campaign inspect <campaign-id>
```

See [RECOVERY.md](RECOVERY.md) for the exact receipt format. Cleanup is not billing
evidence. Billing is not process-termination evidence. Reinspect after reconciliation
because it changes the state hash. Reconciliation does not itself continue work.

Eligible local unverified stops retain open original allocations. All prior final
usage remains charged. Unknown usage blocks another attempt. Closed, missing,
cancelled, exhausted, or insufficient allocations cannot be reopened or topped up.
Expired deadlines cannot be extended. Older already-settled Work stays settled.

Unknown local execution identities conservatively occupy resident/execution slots
after daemon recovery. Current-process uncertain cleanup retains identity-bound
slots. Only a committed exact cleanup receipt, with no active campaign task, releases
those slots, once. Excess unknown identities remain blocked even when the configured
host limit is smaller than the recovered occupancy. No retained execution records
means no fabricated global occupancy.

## Logical Context

Host collection writes a bounded `WorkContextSnapshot.stopping` payload without
another model call. It includes typed collection phase/reason, accepted and applied
instruction revisions, direct logical Work handles, group IDs/revisions, pending
question references, allocation availability and retained resources produced during
the attempt, including Ready artifacts not selected as candidate output. Original
resource versions are preserved. The manifest is at most 64 KiB with at most 256
Work, group and produced-resource entries each; excess inventory fails closed,
never silently drops entries. This is not an arbitrary Python object inventory.

The existing trace writer commits staging intent before filesystem writes, syncs
the object and directory, then commits Ready metadata and the collected execution
state together. File and database persistence are separate phases, not a distributed
transaction. Failed publication retains staged data/charges and collected evidence
as unverified, with no usable stopping checkpoint. It does not replay the worker or
automatically promote incomplete staging. No database transaction crosses an await.

Ghost's optional `WorkResult.final_context` captures activated package versions and
known live output IDs from its per-Work registry before cleanup/export drops them,
including packages first required in a final cell that calls `work.complete` without
another model request. The host labels bounded final observations `final`; legacy
results without this field use the last model-boundary observation as `stale`, or
`unknown` if unavailable. Invalid final metadata becomes `unknown`, not an older
observation disguised as final. Ordinary failure can still carry final observations;
interruption before capture can leave them unavailable.

These are untrusted worker observations, not proof of installed versions or grants.
Bounds are 64 packages, 256 unique live IDs, and 256 bytes per name/version/ID.
The host retains live IDs only when explicitly mapped to committed output exports
owned by this Work, attempt and generation. `worker_observations_filtered` marks
discarded invalid metadata or unmapped IDs; no cross-scope resource is invented.
Budgets, questions, instructions, logical handles and resource refs remain host-owned.
Continuation validates every retained package version against the approved fresh
bootstrap; unknown metadata blocks continuation. Missing entries do not prove that
a package was unused. The fresh registry and host policy remain authoritative;
continuation restores neither authority nor process state.

The new attempt receives the checkpoint reference, selected resource references,
current authoritative question records, direct logical Work handles, and a compact
snapshot manifest. Old live output IDs are accompanied by their committed historical
resource mappings when retention succeeded. They are **not** installed into the new
process's `ctx` store as if they were local output. Use the exact durable references
with `history.read` when the original launch enabled history. Missing retention or
disabled history does not silently create readable resources.

Activation metadata remains informational until validated. The operator must supply
an executable SHA-256 pin; an absent pin is denied. The host hashes the configured
executable, independently hashes the launched process's `/proc/<pid>/exe`, and
validates the launched binary's bootstrap handshake against that pin,
the requested Ghost version, and every retained activated package/version. The worker
then revalidates activation against its current registry and permissions. A mismatch
allows no model request. There is no arbitrary plugin loader or kernel pickle.
Use `campaign inspect` to see a persisted configuration-mismatch diagnostic after
the asynchronous launch. A failed claimed attempt is not retried automatically.

## Current Limits

- Only root Work is an explicit continuation target. Child processes are not resumed.
- Existing direct child handles remain logical addresses under the existing scope
  checks and control allowlist; they are not credentials or new admissions.
- Continuation does not start the child scheduler or replay pending child launches.
- Missing package metadata, pre-inference-only snapshots, snapshots from another
  attempt/revision, stale state or uncommitted stopping snapshots fail closed.
- A pre-model bootstrap failure can leave no checkpoint for the claimed attempt;
  continuation then remains blocked rather than borrowing an older attempt's snapshot.
- `status` is logical reattachment, not a live process attachment. `owned` indicates
  whether this daemon currently owns a campaign task. Durable active counts alone
  do not prove a live process.
- `campaign resume` remains verification-only for retained evidence-ready states.
  `campaign recover` reapproves retained staging policy without executing it.
- There is no automatic restart, allocation reopening, provider-side receipt fetch,
  or guarantee that arbitrary native workspace state is safe to reuse.

## Local Tests

No live provider or installed-daemon restart is required:

```sh
cargo test -p tachyon-api -p tachyon --lib
cargo test -p tachyond --bin tachyond
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_stopped_snapshot_continuation_is_fresh_and_idempotent -- --ignored --nocapture
cargo test -p ghost --test python_navigation -- --ignored --nocapture
```

The explicit Ghost test uses a Rust/Tokio localhost fake provider and exercises real
Ghost Python cells. The provider fixture and continuation/recovery implementation are
Rust; no Python daemon, replay engine, or test-provider process is introduced.
The Python navigation fixture uses Cargo's freshly built Ghost binary, an existing
IPython, a private fake broker and explicit local browser stubs. It checks nested
browser CPU admission and cleanup on the model channel, forwarded Pending artifact
registration (not durable publication), and an empty filtered history page followed
by a matching page. It never downloads a browser or contacts an external provider.
