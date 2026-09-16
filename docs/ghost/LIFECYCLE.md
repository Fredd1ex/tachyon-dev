# Resident Swarm Lifecycle

CURRENT: the opt-in Rust `HostScheduler` and private scoped broker support actual
parent waits and automatic message/steering delivery at model boundaries in
one-shot Ghost, including a resident Python cell. This is not
complete swarm lifecycle support or ordinary daemon-startup activation.

## Wait Contract

The trusted host enables `Control::Wait`, approves child templates, and configures
both `WorkLimits.max_running` and `WorkLimits.max_resident`. Scheduler task capacity
also includes parked residents; use at least two tasks for a parent and child.
Root execution cap one is supported without restarting the parent.

```json
{"action":"wait","work_ids":["child-a","child-b"],"mode":{"count":1},"timeout_ms":10000}
```

`mode` is `"all"`, `"any"`, or `{"count":N}`. References must be 1..64 unique,
existing, directly owned children; IDs do not confer authority. Count is 1..the
number of references. Timeout is 1..300000 milliseconds, also constrained by the
original host assignment and Ghost tool/cell deadlines. No guest PID, objective,
budget, model policy, or credential fields are accepted.

```python
import json
agents = require('agents')
retained = {'answer': 41}
receipt = await agents.spawn(template_id='approved-child', command_id='spawn-1')
ids = json.loads(receipt['content'])['work_ids']
result = await agents.wait(work_ids=ids, mode='all', timeout_ms=10000)
snapshot = json.loads(result['content'])
print(retained['answer'] + 1, snapshot)
```

The normal native/Python tool envelope contains JSON with `outcome: "wait"`,
`completed`, `outstanding`, `resumed: true`, and `resource_blocked`. Completed means
host-acknowledged primary worker termination, **not accepted verification**. Use
`result` separately for evaluation. Timeout may return partial results and does
not cancel outstanding children.

## Safe Handoff

The private transport parks the control request during the tool phase. Under the
permit-authority lock, a serialized transaction rejects active/unknown/provisional
inference holds and releases the parent's execution lease. A permit pause fence
also denies new claims and controls. Persisted inactive membership denies funding
and replacement permits, including after restart. Historical billing can still
reconcile; suspension never fabricates zero usage or a new allocation.

Each poll runs storage work on Tokio's blocking pool, then sleeps asynchronously
for 20 ms. Neither the authority mutex nor a database transaction crosses an await.
Before returning, the host atomically reacquires the root and every ancestor slot
and clears the pause fence. It never returns a normal tool result with
`resumed: false`. If reacquisition remains blocked, it parks until the earlier of
the assignment deadline or five seconds beyond the wait deadline, then fails the
transport and cancels the parent. Ghost cannot continue inference without a lease.

Residents retain a separate logical lease during waits. `max_resident` defaults
to 16 for new configuration, is bounded to 1..4096, and is immutable like the
other WorkLimits. Older persisted configurations without the field decode as
4096, preserving their previously unrestricted residency within the lifetime
Work bound. Active/unknown claims, suspended workers, and verification claims
count conservatively until terminal acknowledgement. This is a count, not an RSS,
CPU, GPU, process-tree, or storage quota. A full resident limit with queued waited
work denies suspension without releasing the parent slot. Scheduler capacity one
also denies broker waits.

## Cancellation And Recovery

Cancel intent, permit revocation, interrupted transport, and wait/resume deadline
failure cannot restore a released slot just to report an error. The host launch
owner revokes the permit, kills its process group, reaps Ghost, and only then
acknowledges termination and supplies empty/unverified evidence. Broker-launched
Python stays in that host-owned group. No guest-provided PID is cleanup authority.
The existing process-group limits still apply to deliberately escaped descendants
and external effects; this is not a new containment boundary.

An abrupt host/runtime crash is different from confirmed cleanup. Wait identity,
revision, released execution lease, resident occupancy, and unknown execution or
billing survive reopening. No permit, cell, process, launch, or unknown review is
automatically replayed. There is no kernel checkpoint or reattachment protocol;
the trusted host must reconcile unknown residency/termination separately.

## Message Boundaries

Ghost completes its parallel tool batch before making the next model request.
The private Rust broker constructs context from durable host state, not from
worker transcript claims. It selects at most 32 unread inbox commands and the
latest accepted steering revision. Superseded steering remains command history.
The immutable admission objective, Work identity, generation, pricing policy and
funding allocation do not change. No agent loop or role policy moves into the host.

A single transaction persists the effective (`acknowledged_revision`) instruction
revision, bounded context window, boundary receipt and request reservation against
the existing allocation. Insufficient capacity rolls back all of these writes;
messages and steering remain pending. A revision change revokes the old permit and
creates a host-only replacement sharing the launch's cancellation lease. Future
claims check the persisted effective revision, not the initial admission revision.
Neither replacement nor wait/resume resets money or unresolved inference holds.

The host then writes a typed `boundary` frame. The client acknowledges its exact
request ID and cursor; only the host persists `delivery_cursor` and
`delivered_revision`. Accepted, applied and delivered are separate observations:
application may persist before transport delivery. This is host instruction-state
application, not proof of successful inference or compliance. A worker cannot
claim application through a tool, transcript, result, or unsolicited ack.

HTTP starts only after acknowledgment and a durable one-use dispatch claim.
Successful model completion records the revision recognized by the host; Ghost
copies the canonical revision into optional `WorkResult.instruction_revision`.
Evidence recognition checks that metadata against host records. Older evidence
becomes historical when newer steering is accepted, even before application.
Pending steering cannot turn an old completion into evidence for the new revision.

Unread delivery advances once per acknowledged boundary, not once per tool call.
A separate rolling window of 32 message bodies is reconstructed for model context,
including after acknowledged delivery followed by a lost HTTP outcome. Bodies are
labeled untrusted parent/child data, never system policy or privilege grants.
Authorized parent steering is separate from this data. No automatic replay of tool
effects is implied by context reconstruction. Older messages remain durable history
but eventually leave this bounded context window.

After a crash, instruction state, delivery cursors, context and unknown holds
survive. A host-authorized fresh channel/permit and fresh logical request can
reassemble context, but do not clear old request or launch claims. There is no
automatic reconnect, launch replay, process reattachment or operational resume CLI.

## Command Repair

Bounded command repair is available through the explicit host-only
`execute_campaign_command_loop`; see [VERIFICATION](VERIFICATION.md). It preserves
Work and allocations, archives each attempt, and starts a fresh process/kernel
only after known termination and final billing. Unknown state never restarts.

## Remaining Work

- Operational crash reconciliation remains host-owned; tests reopen durable state,
  not a production daemon crash manager.
- Scheduler fairness is bounded polling, not weighted fairness or guaranteed
  resume priority. Capacity contention can cause parent cancellation.
- MicroVM launch, resource enforcement, and recovery are not implemented. Typed
  transport and store controls remain separate from the current host launcher;
  a future adapter must provide host-owned allocation and termination evidence.
- This does not enable public IPC grants, UI controls, arbitrary child objective
  generation, or new Python model/credential actions.

## Verification

```sh
cargo test -p tachyond -p tachyon-model -p ghost -p tachyon-api
cargo build -p ghost
GHOST_TEST_BIN="$PWD/target/debug/ghost" cargo test -p tachyond -- --ignored
```

The actual localhost Ghost wait test covers native and Python waits at root cap
one, retained Python state after await, parent cancellation while the child is
still running, and observed resident-kernel termination. Store/broker tests cover
partial timeout, all/any/count, resident denial, inference/replacement fencing,
unknown billing rejection, cancel/revoke, blocked reacquisition cancellation,
ancestor limits, and reopen without execution replay.
The actual Ghost boundary test sends data and two steering commands while a real
native/Python tool is blocked, then checks the subsequent localhost model request,
latest-only steering, canonical candidate revision and unchanged funding. Store
tests cover bounded pages, stale acknowledgments, pre-delivery reopen, denied
capacity, cancellation and permit pause/revocation.
