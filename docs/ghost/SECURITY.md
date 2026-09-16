# Research Execution Security

**CURRENT:** native Ghost execution is not an isolation boundary. Workspace cwd,
scrubbed environments, tool permissions, and broker framing do not contain arbitrary
exec/IPython code. See [SANDBOX.md](SANDBOX.md) and [BROKER.md](BROKER.md) for current
guardrails. No microVM backend or automatic policy selection is claimed here.

**PROPOSED:** the following is a deferred design contract for research swarms and
ML workloads, protecting host stability as well as confidentiality and integrity.
It does not authorize VM implementation, new dependencies/crates, or VM tooling.
Firecracker is the target microVM backend, not an implemented dependency. Complete
and validate Ghost interface features with deterministic local fixtures first, then enter the
isolation and release phase. Local research completion does not establish safety
for untrusted workloads; that requires enforced isolation and the release gates below.
The decisions below constrain that future design; they do not select a final
placement policy, transport stack, provisioning API, or checkpoint implementation.
Access profiles remain in SANDBOX; canonical identities remain in
[TERMINOLOGY.md](TERMINOLOGY.md).

## Trust Boundary

- Tachyond on the host remains authoritative for the ledger, admission, permissions,
  secrets, model broker/provider access, evaluator configuration and acceptance,
  and durable artifact publication. Guest claims never settle billing or grant access.
- Ghost's existing agent loop, IPython kernels, and workload tools would run in the guest.
  No raw host database, provider credentials, daemon secrets, or general host-command
  endpoint is exposed. Evaluation of guest executable content must itself be
  contained; host-owned evaluation does not mean executing it unsandboxed on the host.
- Replaceable execution backends separate launch, scoped transport, workspace attach,
  resource supervision, cancellation, and cleanup from Work and accounting semantics.
  Native execution remains an explicit, non-isolated development option. Future
  untrusted-research and isolated-ML profiles must require a microVM; exact profile
  configuration remains deferred. A requested or required VM must fail closed if
  unavailable or unable to enforce policy, never silently fall back to native.
- Host-created channels must bind to an exact guest instance, Work, generation, and
  current grant, with bounded framing, deadlines, revocation, and no ambient authority.
  Current Linux subprocess authentication compares Unix peer PID/UID with a spawned
  host process. A guest PID is not comparable to a host PID. VM transport needs its
  own authenticated host-to-instance binding; a guest-supplied PID/ID is not proof.

## Placement And Guest Execution

- VM placement is a host policy decision, not an agent-selected privilege. Compare
  one VM per Work (stronger separation and attribution, more boot/memory overhead),
  per worker group (shared inputs and cheaper coordination, shared compromise and
  failure boundary), and per campaign (maximum reuse, widest intra-campaign exposure).
  Group/campaign membership alone does not authorize sharing secrets or writable
  state. Co-location requires compatible trust and sharing grants; separate Research
  scopes must not be combined merely to save capacity. Final granularity is deferred.
- A guest agent supervisor would run Ghost agents and tools as Rust-managed process
  units, including their Python kernels, rather than becoming a second authoritative
  scheduler. The host owns admission, placement, scheduling, budgets, and accountable
  observed resource use. Guest worker-group concurrency is a cooperative execution
  bound, independent of host-enforced VM and aggregate limits; it is not containment
  or proof of per-Work usage inside a shared VM.

## Communication And Bootstrap

- Use a narrow authenticated host/guest protocol for inference, scoped agent
  coordination, steering, status, and artifact transfer. vsock is a possible
  transport, optionally behind a guest bridge, not a selected stack or an authority
  mechanism. Mutually authenticate the host and exact guest instance, binding each
  Work/generation/grant separately even when a VM is shared. Transport addresses and
  guest PIDs do not inherit native host peer-PID authentication semantics.
- Keep inference in the host broker with provider credentials remaining on the
  host. A guest bridge may proxy only explicitly granted typed operations, not
  expose an arbitrary outbound connection to the daemon's general socket/API.
  Sandboxed send/steer must retain host authorization and accepted-versus-applied
  semantics; neither model text nor a guest message expands authority.
- Separate bounded control traffic from logs and bulk artifacts, with reserved
  cancellation priority, deadlines, and backpressure so output floods cannot starve
  steering or shutdown. Host force-stop remains available if guest control stalls.
- Establish authenticated boot identity before enabling grants. Pass bounded,
  validated host configuration through a separate bootstrap channel, not the model
  prompt: instance/Work bindings, endpoint capabilities, limits, and approved inputs
  are host control data. Prompt content cannot change that configuration. Scope and
  revoke bootstrap capabilities per launch; never embed provider or daemon secrets.

## Resources And Lifecycle

- Admission must enforce per-guest and aggregate Research/host ceilings for CPU
  time/vCPUs, memory, pids/process count, disk bytes/inodes and I/O, wall-clock time,
  and network bytes/rate/connections. Model budgets do not enforce these quotas.
  Reserve host/foreground capacity and bound VM count, output, and queued work so a
  swarm cannot exhaust the host by multiplying individually bounded guests.
- Provision admitted CPU, memory, disk, process, and network allocations before
  dispatch, and verify host enforcement independently of guest worker counts.
  Account for VMM/helper overhead and resident paused or pooled VMs. GPU compute,
  memory, and device allocations require the separate capability review below;
  requesting a GPU does not imply a supported or safely provisioned device.
- Tachyond owns stable resource IDs mapped to Work/generation and VM instances,
  VMM host processes, supervisor groups, disks/overlays, sockets, and network/device
  allocations. Guest process references are scoped handles, never host kill targets.
- Cancellation revokes broker/control grants, stops new work and provider dispatch,
  requests bounded graceful shutdown, then force-stops the VM and all owned host
  helpers after a grace deadline. Quota violation, timeout, or unresponsive guest
  must permit host-enforced termination without guest cooperation. Observe/reap host
  processes and release resources only after cleanup evidence; a cancel receipt is
  not terminal proof. Client detach does not cancel Work.
- Persist lifecycle intent and ownership before launch. Supervisor-backed orphan
  discovery must reconcile daemon crashes, partial launch/cleanup, and host restart
  without trusting stale PIDs or killing unrelated processes. Failed cleanup remains
  visible and retains capacity until reconciled. VM/host crashes do not replay Work,
  tool side effects, model requests, or evaluations, clear launch claims, or refund
  unknown charges. Recovery needs explicit authorization and fenced new execution.
- Distinguish launch, pause, resume, snapshot, and destroy from application-level
  safe checkpoint/recovery. Pausing retains resources and does not necessarily stop
  in-flight external effects; a memory/disk snapshot is not a transactional boundary
  with the host ledger or providers. Resume/restore must revalidate grants, identity,
  and fencing and reconcile observed execution/resource evidence. Never replay
  arbitrary tool side effects or model calls from a snapshot; unknown execution and
  billing outcomes remain unknown until reconciled, not free retries.

## Files, Network, And Images

- Attach a per-guest workspace with an isolated writable overlay and explicitly
  authorized read-only input/reference mounts. Do not share writable overlays across
  Research scopes or mount host HOME, databases, credentials, or control sockets.
  Reattachment must revalidate ownership and grants; a path or resource ID is no grant.
- Export artifacts through a bounded host validation/publication path into immutable
  host-owned storage before disposable workspace cleanup. Validate paths, traversal,
  symlinks/hardlinks, special files, sizes, and races while resolving/copying; reject
  escaping archives and malicious output. Hash the exact exported bytes on the host,
  retain provenance and versions, and do not trust guest-supplied hashes. Guest files,
  manifests, logs, checkpoints, and model output remain untrusted even after hashing;
  do not execute/deserialise them with host authority. The current ArtifactStore
  retains exact Ready bytes, but public Ready references are not authorization or
  proof of trustworthy content; see [ARTIFACTS.md](ARTIFACTS.md).
- Host exports to `~/Agents` must pass through private staging, bounded validation,
  and host-owned publication into an explicitly authorized destination. Never mount
  the entire `~/Agents` tree or home directory, or let a guest choose arbitrary host
  write paths. Published exports are immutable versions, not guest-writable mounts.
- Export bounded live evidence and application checkpoints during long-running Work,
  not only its final result, so a later guest loss need not lose all prior progress.
  Host-acknowledged durable publication defines what survived; unsent guest state is
  not durable. Apply the same validation, quotas, and retention policy to incremental
  exports, and distinguish retained evidence from an authorized safe restart point.
- Guest network egress is default-deny, including private host/LAN and metadata
  endpoints. Explicit host policy may enable a restricted proxy with destination,
  DNS/redirect, protocol, and traffic limits; it is not unrestricted host networking.
  Provider calls and provider credentials stay exclusively in the host model broker.
- Pin guest image, kernel, agent/tool, and runtime versions; verify image integrity
  and trusted provenance before boot, and record their identities with execution.
  Keep the guest agent loop hardened, minimally privileged, and bounded at every
  host-facing protocol. No automatic dependency, browser, or image installation in
  the hardened loop: provisioning and updates are explicit, separately authorized
  steps. This is a target, not a claim that current eager browser setup has changed.
- Compare cold boots with a bounded warm pool only after image validation. Warm
  instances must start from a validated clean baseline, receive fresh boot identity
  and grants, and carry no prior Research memory, overlays, logs, credentials, or
  capabilities. Sanitization/reset must be verifiable before reuse; otherwise destroy
  the instance. No cross-Research secrets may survive pooling or snapshot reuse.

## ML And Privilege

GPU passthrough requires a separate threat model and backend capability review:
IOMMU/device isolation, DMA, shared devices, firmware, host/guest drivers, reset and
cleanup, GPU memory leakage, and GPU compute/memory quotas. A microVM with device
passthrough must not be assumed to provide stronger isolation than the reviewed
device/driver boundary. Unsupported safe GPU configurations must fail closed;
Firecracker suitability is not a GPU-support commitment.

Normal Tachyon/Ghost operation must remain non-root. Any privileged host setup for
virtualization, networking, or devices belongs to separate explicit provisioning
with narrowly scoped permissions, not a root agent loop or a root-guard bypass.
Firecracker control stays non-root; permission to access KVM (such as `/dev/kvm`)
and any required host networking/device setup must be provisioned separately and
checked before launch. KVM access is a meaningful host privilege, not something
granted by a guest instruction. Document platform/backend prerequisites before
activation; do not choose a privileged helper or provisioning stack here.

## Security Release Gate

**PENDING:** no independent security audit results are recorded here. Any claim of
"no unsafe first-party code" remains unverified pending a separate scoped audit of
the actual source/build; safe Rust alone would not establish sandbox security or
cover dependencies, native tools, kernels, VMMs, or drivers.

Potential vsock support requires later dependency and platform API review, including
how safe APIs reach kernel interfaces and where unsafe code exists in dependencies.
The first-party `forbid(unsafe_code)` constraint must be preserved and audited, not
bypassed to add a transport. No crate, API, or transport implementation is selected
here, and that constraint is not a claim that the whole stack is free of unsafe code.

After feature completion, an independent security audit is release-blocking for
the completed research/isolation feature set. Record scope, versions, findings,
remediation, retests, and residual risks before release; documentation and passing
functional tests do not satisfy this gate. Audit execution/results are a separate
task. Do not defer targeted review and threat modeling until then.

Ongoing review and CI gates must cover dependency CVEs and supply-chain/image
integrity, secret scanning/redaction, broker and guest protocol fuzzing, permission
and cross-Research denial tests, denial-of-service quotas, VM escape/host attack
surfaces, failure injection for cancellation/crash/orphan cleanup, and verified
backup/restore recovery of ledgers and immutable artifacts without execution replay.
Track implementation and audit work in [todos/todo.md](todos/todo.md#security-and-isolation).
