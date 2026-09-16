# Local Integration Gate

`tachyon campaign integrate` is an explicit same-user operator gate for applying
one retained Work candidate's multi-file exact patch to the campaign's approved
root workspace. It starts no model, worker, evaluator, shell, or git process.
There is no model-facing integration tool and no directory override. The host
operator is the integrating writer; child workers only propose retained artifacts.
Use this local gate after campaign execution has released its artifact store.
These two commands require no `--unisolated-development` flag: they launch no
worker or evaluator. `integrate` requires the existing `--confirm` operator gate.
Campaign execution commands still require their documented unisolated opt-in.

## Operator Workflow

Before delegating a change, inspect the **root** files:

```sh
tachyon campaign integration-snapshot CAMPAIGN src/a.rs src/b.rs
```

This returns `expected_state`, `expected_versions`, and full-file SHA-256 hashes.
Supply the relevant root content and baseline metadata as host inputs to children.
Child workspace stat versions are not root versions. The command does not copy
inputs or authorize workers to write the root workspace.

Have the child publish a UTF-8 JSON file as an immutable candidate artifact:

```json
{
  "files": [
    {"path": "src/a.rs", "old": "one exact old string", "new": "replacement"},
    {"path": "src/b.rs", "old": "another old string", "new": "replacement"}
  ]
}
```

Each file occurs once; `old` must be nonempty and match exactly once. Creation,
deletion, binary files, symlinks, absolute paths, traversal, and path aliases are
not supported. All existing targets must be beneath the stored manifest root.
Unknown fields are rejected. Limits are 32 files, 64 KiB per file/output, and
64 KiB per plan or bundle.

The operator selects a single retained Work/artifact reference and reviews its
digest, patch content, and original root versions. The operator's `plan.json` is:

```json
{
  "command_id": "operator-integration-1",
  "work_id": "CHILD_WORK",
  "artifact_id": "RETAINED_PATCH_ARTIFACT",
  "artifact_sha256": "64-character SHA-256 from artifact metadata",
  "expected_versions": {
    "src/a.rs": "stat-v1:64-character root version",
    "src/b.rs": "stat-v1:64-character root version"
  }
}
```

```sh
tachyon campaign integrate CAMPAIGN plan.json --expected-state STATE --confirm
```

The daemon resolves the source only through its scoped immutable artifact store
and the retained candidate reference, checking Work, generation, assignment,
attempt, size, and digest. It never reads the child's original artifact path.
The state token binds the stored launch and campaign record; per-file versions
separately bind the initial root snapshot. A new integration must pass both.
The retained `approval_artifact` is the SHA-256 of the serialized campaign, exact
plan (including command ID, source Work/artifact/digest and baseline versions),
and state token. It binds that complete operator approval, not just the campaign.
The snapshot state token alone is not authorization for an arbitrary source.

## Writes And Recovery

The host acquires one cross-process workspace coordinator lock, then all native
target locks in sorted canonical path order. These target locks are the same
ones used by native `edit` and `write`. All files are read and preflighted while
locks are held. One stale file rejects the whole new plan before any replacement.
Lock contention yields asynchronously and observes the deadline/cancellation;
integration I/O does not hold the daemon's registry mutex or a database transaction.

Before the first replacement, the host publishes immutable before/after snapshots
and the approved request as artifacts under `integration-TRANSACTION_HASH`, charged
to existing retained storage. A bounded journal in the existing runtime database
records `prepared`, `applying`, and `completed`, original/output artifact IDs, and
each file's `pending`, `applying`, or `applied` outcome. No new database is created.
Snapshots published before a failed preparation remain conservatively retained.
Every evidence publication must return exact Ready metadata and matching bytes.
Recovery checks snapshot content against its journal SHA-256 ID and recomputes
the approved exact edit from the retained original before trusting the output.
Journal updates use transactional revision/identity compare-and-swap, with the
unfinished-workspace fence also checked in the transaction that creates a journal.

**This is not a multi-file atomic commit.** Each file uses the native conditional
atomic replacement and durability path. Failure stops further writes. The report
includes durable outcomes when available; a lost connection or journal commit
error may leave an `applying` intent whose replacement happened. The daemon never
automatically resumes it on startup.
Before marking completion, the host rereads **all** outputs under the held locks
and compares full-file SHA-256 hashes. Reports retain `observed_sha256` and
`observed_version` from that readback. A mismatch leaves the journal incomplete
and requires investigation; these observations are not proof against later edits.

Retry the **identical** campaign, command ID, plan, original state token, and
confirmation to recover. Recovery locks and preflights every target before doing
more work. It skips an intent whose current bytes exactly equal its retained
output; otherwise a pending file must still match both its original bytes and
original stat version. Changed content conflicts without overwriting it or touching
remaining files. A different command cannot supersede an unfinished transaction
in the same workspace. Completed retries are read-only and detect changed output.

There is deliberately no automatic rollback or force-overwrite option. Originals
remain available for operator recovery. Any future rollback must itself be
conditional on proof that the current file is still the integration's output;
never restore a backup over external edits. A conflicting incomplete transaction
requires operator investigation, not a fresh command ID to bypass the fence.

## Verification And Limits

Every successful response says `verification_required: true`. Integration does
not mark the campaign, Work, or code correct and does not change prior acceptance
records. Run the appropriate verification against the integrated root; prior
child verification alone is not verification of the integrated result.

Locks are advisory, not a sandbox. Cooperating native writers serialize across
processes, but shell commands, editors, and same-user processes can still write
files or rename directories. On Linux the writer pins nonsymlink parent descriptors;
batch reads, temporary creation, rename, directory sync and cleanup use those
descriptors. Lock acquisition and replacement recheck device/inode association
with the configured path, including the batch coordinator root. A parent-path
swap cannot redirect those operations through a replacement symlink, though a
rename can leave a replacement in the originally opened, detached directory.
Native pre-rename content/version and staged-identity checks detect many
such races, but cannot eliminate the final check/rename race with noncooperating
writers. Recovery recognizes an unacknowledged output by exact content, not by
attributing which process last wrote identical bytes. Quiesce external writers.
Hostile same-user processes can alter lock files, open objects, retained evidence,
or the database; this gate is not a security boundary against that authority.
This is one bounded local gate, not a distributed or git integration framework.
