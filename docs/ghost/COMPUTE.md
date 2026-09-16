# Native Compute Admission

This is logical admission for cooperating Rust native-job supervisors on a shared
local host. It is not GPU passthrough, an OS device grant, CPU-cycle metering,
resident-memory enforcement, or a sandbox. No driver or hardware discovery runs.

## Host Inventory

```toml
[campaign_resources]
max_cpu_jobs = 2
max_gpu_jobs = 1
gpu_device_ids = ["GPU-operator-selected-stable-id"]
```

GPU defaults are zero jobs and an empty inventory. Inventory selectors must be
unique, nonempty ASCII letters/digits/hyphen/underscore/dot, at most 128 bytes;
there are at most 256 selectors and the GPU job ceiling cannot exceed inventory.
Use stable operator-selected IDs, not automatically discovered ordinal positions.
Each admitted GPU job exclusively holds one selected ID. An inventory entry is
not permission to open a device; this layer makes no physical device available.

## Root Envelope

The optional root campaign manifest `compute` field enables aggregate job-wall-time
budgets. An omitted envelope preserves CPU admission without a duration budget,
with a 120000 ms per-request bound; GPU admission stays disabled.

```json
{
  "compute": {
    "cpu_job_ms": 600000,
    "gpu_job_ms": 120000,
    "max_gpu_jobs": 1,
    "max_cpu_timeout_ms": 120000,
    "max_gpu_timeout_ms": 60000,
    "profiles": {
      "approved-analysis": {
        "max_gpu_jobs": 1,
        "max_cpu_timeout_ms": 30000,
        "max_gpu_timeout_ms": 30000
      }
    }
  }
}
```

Profile keys are declared dynamic `profile_id`s or fixed child `template_id`s.
The immutable host manifest maps exact child Work IDs to profiles; the worker
cannot choose a profile through exec. Unlisted descendants inherit the root CPU
timeout ceiling but have no GPU grant. Profile bounds cannot exceed root bounds.
The GPU root count includes all descendants; the profile count includes all live
jobs in that profile. Nesting, retries and explicit continuation do not replenish
the root allowance. Each class has its own sum; GPU wall time is not additionally
charged as CPU wall time. Zero class allowance denies admission for that class.

## Native Interface

```json
{"argv":["approved-program"],"timeout_ms":1000,"workload":{"class":"cpu"}}
```

```json
{"argv":["approved-gpu-program"],"timeout_ms":1000,"workload":{"class":"gpu"}}
```

Omitted workload means CPU. GPU requests take no device IDs or device count;
this implementation supports one logical device per job. Unknown workload fields
are rejected. Rust-backed Python `exec` uses the same schema and supervisor.
The browser command runner also uses the hook, defaulting to CPU. Local nonbroker
CPU exec remains ungated; local GPU requests without host authority are denied.

After admission, exec sets `CUDA_VISIBLE_DEVICES` to the host-selected ID (empty
for CPU). This overrides inherited exec configuration for that invocation only.
It is a selection hint, not enforcement: arbitrary native code can override it or
open devices directly. Unauthorized profile replies contain no host inventory.

## Leases And Accounting

Rust `JobLease` owns cleanup in the existing private CPU-job transport. New typed
acquire frames carry class and integer maximum duration; the original CPU-only
try-acquire form reserves the fixed 120000 ms bound through the same accounting.
Acquisition returns immediately with a grant, busy or sanitized denial. Polling
does not consume the model/control frame allowance or hold the broker stream while
waiting for capacity. Blocking redb work runs on Tokio's blocking pool.

Admission uses class-specific campaign round-robin selection and FIFO session
order within a campaign. The host retains at most 256 waiting session/class
entries; polling refreshes them and abandoned entries expire after two seconds.
Waiting consumes no compute allowance. One redb transaction verifies shared
occupancy and root remaining allowance and writes the maximum-duration hold
before a process can spawn. Each campaign/class retains at most 65536 job records;
reaching this bound rejects new jobs without eviction or allowance reset.

**Measured quantity: conservative native-job wall time, not compute cycles.**
The host's monotonic timer starts after admission commit and ends on confirmed
cleanup. It includes grant delivery, pre-spawn setup, process execution and
cleanup overhead, but not the capacity queue. Nanoseconds are rounded up to
integer milliseconds before checked narrowing from u128 to u64. Known elapsed
time releases the unused reservation; cleanup overruns are charged rather than
clamped or hidden. Therefore timeout budgets are reservation bounds for the
cooperating supervisor, not a hard bound on OS cleanup latency or physical work.
An overrun can leave the class above its root allowance: confirmed cleanup frees
the job slot, but the recorded spend still denies further admission in that class,
including after reopen. Replaying release as no-spawn cannot erase that spend.

A confirmed no-spawn cancellation or failed spawn releases the whole hold. A
cancelled RPC waiter drains its in-flight frame; a late grant is returned without
spawning. Dropping a running supervisor, losing the channel, timing out without
confirmed cleanup, or restarting never implies zero usage. Such records keep the
entire reservation and device/CPU occupancy. Admission reads unresolved records
from `runtime.redb` after reopening, independently of process-local semaphores.
Release is session-owned and final records make replay incapable of double refund.

## Operator Cleanup

`campaign inspect` reports `native_job_wall_time` records with exact lease and Work
identity, reserved milliseconds, nullable final milliseconds and cleanup state.
These records participate in the reconciliation state hash. Existing exact Work
cleanup receipts also finalize native holds for that attempt/generation at the full
reserved cost. A native job can instead be reconciled independently, including
after ordinary Work settlement, with this record in an authoritative receipt:

```json
{
  "kind": "native_cleanup",
  "work_id": "campaign-root",
  "attempt_id": "host-recorded-attempt",
  "generation": 1,
  "lease_id": "host-recorded-lease-uuid",
  "confirmation": "operator_attests_all_processes_terminated"
}
```

The existing idle-campaign, exact state-hash and explicit authoritative operator
gates apply. Stale generation, foreign Work and foreign lease targets fail closed.
Cleanup frees occupancy only after durable commit and charges the entire unknown
reservation, never a cancellation refund. Identical receipt replay is idempotent.
No automated orphan discovery, process killing, driver probing or kernel replay is
performed. See [RECOVERY](RECOVERY.md) for the outer receipt and CLI contract.

## Isolation Boundary

Arbitrary Python `!shell`, direct subprocess/OS calls, escaped descendants and
same-user filesystem/device access can bypass these cooperating hooks. GPU drivers,
device passthrough, CPU/PID controls, hard memory guarantees, physical metering and
aggregate storage enforcement remain separate isolation work. Firecracker does
not by itself establish a safe GPU passthrough configuration.

Tests use fake GPU IDs only. Targeted coverage includes denial before spawn,
exclusive leases across campaigns, concurrent budget holds, root sharing, profile
denials, host elapsed rounding/overflow, cancellation and confirmed no-spawn refund,
stale/foreign release, replay, actual redb reopen, and exact operator receipts.

```sh
cargo test -p ghost -p tachyon-model -p tachyon-api -p tachyond
cargo build -p ghost --bin ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_simultaneous_campaigns_share_capacity_and_cancel_independently -- --ignored
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond actual_nested_python_wait_and_recursive_cancel -- --ignored
```
