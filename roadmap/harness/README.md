```text
Implement Ghost as a programmable, modular research harness with durable,
budgeted multi-agent execution.

The immediate goal is to make Tachyon useful for developing Tachyon itself.
The longer-term goal is to support research campaigns that adapt their
parallelism and strategy without losing progress or consuming unbounded
resources.

Use these instructions directly. Inspect relevant source files, build
manifests, and existing tests as needed. Reuse working implementations.
Avoid broad repository tours, speculative rewrites, and unrelated changes.

Implement in small, testable phases. Do not build everything in one patch.


1. SCOPE AND ARCHITECTURAL BOUNDARIES

Ghost owns:
- the worker model/action/observation loop;
- its programmable execution session;
- capability discovery and invocation;
- bounded task-local context;
- proposals for investigations, delegation, and completion.

Tachyond owns:
- authoritative tasks, campaigns, groups, and worker identities;
- permissions and admission control;
- resource accounting;
- process lifecycle and cancellation;
- durable events, artifacts metadata, and history;
- enforcement of generation/revision ownership.

The existing interaction layer owns:
- user-facing conversation;
- notification ordering;
- clarification delivery;
- compact views of background activity.

Do not move conversation or coordination policy into Ghost.
Do not rewrite the TUI, database, or main orchestration layer.
Only extend their contracts where Ghost requires a missing capability.

Share low-level services, not role-specific agent loops.

Do not assume that larger swarms always perform better or worse.
Model selection and coordination policies must remain replaceable.
Stronger models must not require rewriting the runtime.

Preserve:
- Rust for Tachyon-owned application/runtime code;
- forbid(unsafe_code) in all Tachyon-owned Rust crates;
- normal operation without root;
- no new Python requirement for starting Tachyond or using ordinary chat.

The IPython Ghost profile explicitly requires Python/IPython in its worker
environment. Keep that dependency separate from the Rust control plane.


2. TARGET GHOST STRUCTURE

Adapt the current layout rather than moving files unnecessarily:

ghost/
    core/
        loop
        session
        context
        completion
    capabilities/
        registry
        manifest
        policy
        lifecycle
    python/
        session
        bridge
        protocol
    plugins/
        workspace/
        exec/
        ctx/
        history/
        artifact/
        web/
        browser/
        agents/
    profiles/
    tests/

Each capability package contains:
- implementation;
- manifest and operation definitions;
- concise model-facing usage instructions;
- configuration;
- tests.

Ghost's core dispatches registered capabilities. It must not contain
special cases for grep, browsers, Python libraries, or agent topologies.

Plugins must not import other plugins' private implementations.
Use shared interfaces for process execution, context, artifacts, history,
and daemon communication.

Trusted Rust plugins are compiled in. Do not introduce dynamic Rust
libraries, a Lua runtime, or a general scripting-plugin framework.


3. CAPABILITY REGISTRY, PROFILES, AND REQUIRE()

Separate these states:
- installed;
- authorized for this work;
- interface loaded into the session;
- backing resource initialized.

Implement:

    require("workspace")
    require("ctx")
    require("agents")

require() must:
- resolve a registered capability;
- check authorization;
- return a Python proxy;
- expose concise operation signatures and usage guidance;
- be idempotent;
- avoid repeatedly inserting identical instructions.

Unknown or unauthorized capabilities return structured errors.
The model cannot install arbitrary plugins or grant itself permissions.

Use a small catalog containing names and one-line descriptions.
Expose detailed instructions only for activated capabilities.

Preload common capabilities for the selected profile so ordinary work does
not waste model turns discovering basic operations.

Initial coding profile:
    eager: workspace, exec, ctx
    optional: history, artifact, web, browser, agents

Keep work.status, work.ask, and work.complete available as core controls.

Reconstruct activated interfaces after context compaction. Do not assume
that instructions loaded in an earlier context remain visible forever.

Lazy initialization applies to expensive resources:
- IPython kernels;
- browser processes;
- sandbox environments;
- external integration services.

Loading browser instructions must not itself launch the browser.
Requiring an existing module must not restart its resources.

Record plugin versions and activated capabilities for reproducibility.
Do not hot-swap implementations underneath an active work session.


4. IPYTHON AS THE PRIMARY MODEL-FACING INTERFACE

Use persistent IPython as Ghost's main composition environment.

The model should normally:
- execute a Python cell;
- observe a bounded result;
- continue working or propose completion.

Capabilities remain Rust-backed primitives, accessible through Python.
Do not replace good native tools with generated Python reimplementations.

Example intended interaction:

    workspace = require("workspace")
    proc = require("exec")

    matches = await workspace.search(
        "WorkerState",
        path="crates",
        limit=12,
    )
    print(matches)

    run = await proc.start(
        "cargo test -p ghost",
        timeout_ms=120000,
    )
    print(run)

Reuse the current IPython integration if it is functional.

Required session behavior:
- one lazy persistent kernel per work unit;
- variables survive between cells;
- one executing cell per kernel;
- independent workers use separate sessions;
- multiline code, expressions, exceptions, and top-level await work;
- external context and tool results can remain Python values until inspected;
- session interruption does not freeze Tachyond.

Use a framed control protocol separate from arbitrary stdout/stderr.
Printed JSON, binary output, tracebacks, or subprocess output must not corrupt
control messages.

Every cell and hostcall has an ID, deadline, and terminal outcome.
Validate all hostcalls in Rust, including authorization and input size.

Do not serialize arbitrary Python objects across the trust boundary.
Use typed data and durable references.

Context compaction should not restart a healthy kernel unnecessarily.

If a kernel crashes:
- retain durable tasks, artifacts, history, and child handles;
- record that volatile Python variables were lost;
- restore explicitly checkpointed state where supported;
- do not automatically replay arbitrary previous cells with side effects;
- do not promise transparent restoration of arbitrary Python objects.

Keep direct capability invocation available internally for tests and
compatibility. Do not maintain duplicate implementations for direct tools
and Python calls.


5. RELIABLE BASE CAPABILITIES

WORKSPACE

Implement or repair:
    list
    find
    search
    read
    write
    edit

Requirements:
- explicit workspace scope;
- bounded reads and search results;
- predictable ignore-file behavior;
- useful line numbers and continuation cursors;
- deterministic binary-file handling;
- exact edits that reject missing or ambiguous matches;
- preserve the original file when an edit fails;
- detect edits against stale file versions;
- atomic replacement where practical.

Reuse the existing search backend initially.
Do not write both a new ripgrep wrapper and a new native fallback at once.

Do not scan entire files merely to calculate total line counts.

EXECUTION

Expose:
    start(command, cwd, timeout_ms) -> ProcessRef
    status(ProcessRef)
    output(ProcessRef, cursor, limits)
    wait(ProcessRef, deadline)
    cancel(ProcessRef)

A short-command run() helper may compose these operations.

Requirements:
- asynchronous process supervision;
- concurrent stdout/stderr draining;
- bounded memory and disk output;
- correct handling of huge lines without newlines;
- explicit exit, signal, timeout, cancellation, and spawn-failure outcomes;
- cooperative termination followed by bounded escalation;
- descendant cleanup and reaping where supported;
- explicit environment allowlist;
- no provider or daemon credentials inherited by tool processes.

A timeout must initiate actual cleanup, not only abandon the waiting future.

WEB / BROWSER

Preserve existing functionality through the capability interface.

Use:
- direct HTTP retrieval for reading ordinary content;
- the existing browser backend for genuinely interactive work;
- lazy persistent browser sessions per work scope.

Do not add Chromium or rewrite the browser integration in this task.
Do not create an LLM subagent for a deterministic operation merely because
the agents capability exists.


6. BOUNDED OBSERVATIONS AND EXTERNAL CONTEXT

Implement:

    ctx.search(query, scope, limit)
    ctx.read(resource_ref, cursor, limits)
    ctx.list(scope)

Resources include:
- tool output;
- work observations;
- workspace documents;
- registered artifacts;
- scoped historical records.

Large output remains external. The prompt receives:
- a compact excerpt;
- status and relevant metadata;
- a stable resource reference;
- explicit truncation/continuation information.

Initial configurable defaults:
- immediate textual excerpt: at most 8 KiB and a separate token ceiling;
- resource page: at most 8 KiB by default;
- full output spool: at most 64 MiB per operation;
- aggregate storage allowance per campaign.

When a spool reaches its limit, continue draining process pipes while
discarding excess output and recording that it was discarded.
Do not let output limits deadlock a subprocess.

Keep arbitrary raw logs out of the foreground conversation.

Context occupancy and lifetime token expenditure are separate metrics.

Before each model request, budget for:
- assembled input;
- activated capability instructions;
- expected output allowance;
- a configurable safety margin.

Use the existing compaction interface when needed. Do not build a new
long-term Memory Agent as part of this refactor.


7. HISTORY, ARTIFACTS, AND RESEARCH PROGRESS

Expose:

    history.search(query, scope, limit)
    history.attempts(query, scope, limit)
    history.findings(query, scope, limit)

    artifact.register(path, kind, description)
    artifact.read(ref, limits)
    artifact.list(scope)

Use existing persistence where available.
Ghost must not receive a raw database handle.

Store large files on the filesystem and metadata in the database.

Distinguish:
- Attempt: what was tried and under which conditions.
- Outcome: what was actually observed.
- Finding: an interpretation supported by referenced evidence.
- Candidate: an artifact proposed as a solution.
- Evaluation: a check performed against a particular candidate.

A failed run is retained as potentially useful evidence.
Do not automatically convert a timeout, crash, or poor score into a proven
scientific explanation.

Example:
    observation: process exceeded deadline
    hypothesis: possible deadlock
    verification status: unresolved

Record enough provenance to identify:
- objective and work identity;
- code revision or content hash;
- relevant configuration and seed;
- command/tool invocation;
- model and capability versions;
- outcome and evaluation;
- supporting artifact references.

Artifact registration must preserve a specific version, not merely point
at a file the next experiment can overwrite.

Use recoverable staging/publication:
- prepare artifact content;
- establish hash/version;
- publish content;
- commit ready metadata;
- reconcile incomplete registrations after crashes.

Do not claim filesystem publication and a database write are one atomic
transaction.

Keep known-good candidates separate from mutable working files.

Findings must be scoped to their evidence and conditions. Avoid turning
one failed experiment into a permanent global prohibition.

Cross-project history is not accessible unless authorized.


8. DURABLE CAMPAIGNS AND SHARED RESOURCE BUDGETS

Add a campaign abstraction, or extend the existing root task to provide
equivalent semantics.

A campaign contains:
- stable identity;
- objective and constraints;
- evaluation/completion contract;
- authorized resource envelope;
- work groups and descendant work;
- candidate/evidence references;
- current status and continuation state.

Agents are temporary. The campaign is persistent.

Distinguish:
    running
    waiting_for_input
    awaiting_verification
    paused_budget
    paused_external_dependency
    completed_verified
    completed_unverified
    cancelled
    failed_infrastructure

Budget exhaustion is not proof that the objective cannot be solved.

On exhaustion:
- stop admitting new expenditure;
- apply the configured policy to in-flight work;
- preserve artifacts, completed attempts, active-work status, and open questions;
- record the stopping reason and a resumable checkpoint;
- use already reserved allowance for necessary finalization;
- do not start an unbudgeted summarization chain.

RESOURCE LIMITS

Track independently:
- total model tokens/cost;
- maximum in-flight model calls;
- maximum concurrent CPU-heavy jobs;
- resident worker/kernel resources;
- total admitted work items;
- delegation depth;
- elapsed deadline;
- artifact/log storage;
- verifier calls and protected verification allowance.

All descendants share the root campaign budget.
A child allocation constrains root expenditure; it does not create new money.

Account for:
- worker inference;
- coordination and synthesis;
- retries;
- compaction attributable to the campaign;
- verification;
- tool/experiment compute.

Use one authoritative ledger.

Reserve before dispatch, reconcile actual usage afterward, release unused
reservations, and preserve unresolved reservations across restart.

Do not double-count suballocations and per-request reservations.

Use exact integer units for monetary accounting rather than floating-point
currency totals.

Where provider billing is delayed or uncertain:
- mark cost as provisional;
- enforce request-level limits where supported;
- use conservative reservations;
- record uncertainty;
- do not promise a mathematically exact spend cap unsupported by the provider.

Retries must consume the same campaign allowance.
Restarting an agent must not reset its budget.

Only an authorized user or trusted policy may increase the root allowance.


9. DURABLE MULTI-AGENT PRIMITIVES

Implement the agents capability only after the campaign ledger and a
competent single Ghost exist.

Proposed interface:

    await agents.spawn(spec) -> WorkRef
    await agents.group(specs, max_running) -> GroupRef
    await agents.status(ref)
    await agents.list(scope)
    await agents.result(ref) -> ResultOrPending
    await agents.wait(refs, mode, deadline)
    await agents.send(ref, message)
    await agents.steer(ref, instruction, expected_revision)
    await agents.resize(group, max_running, expected_revision)
    await agents.cancel(ref)

SEMANTICS

spawn/group:
- wait only for admission/registration;
- return durable handles before work completes;
- may return queued rather than already running;
- rejected admission returns a structured reason.

group:
- contains explicit bounded work specifications;
- creating work and increasing execution concurrency are separate operations;
- a "diverse" flag must not silently manufacture unspecified investigations.

resize:
- changes admission concurrency;
- does not create new objectives;
- shrinking initially uses drain-on-shrink;
- running work is not killed implicitly;
- cancellation/preemption is separate.

wait:
- suspends only the requesting work continuation;
- does not hold a daemon actor, database transaction, or active model permit;
- has a deadline;
- supports all, any, or a specified number of terminal results;
- returns partial results and outstanding handles when appropriate.

send:
- bounded parent/child communication initially;
- durable message identity and provenance;
- no automatic all-to-all message broadcast.

steer:
- records a versioned instruction;
- reports accepted separately from applied;
- applies at a safe boundary;
- old results remain historical evidence but cannot overwrite newer state.

INITIAL DEFAULTS

- root delegation depth = 0;
- maximum child depth = 1;
- maximum simultaneously active children = 3;
- maximum in-flight model calls per campaign = 4, including the lead;
- maximum total work items per campaign = 16, including the lead.

All values are configurable and enforced across nesting, retries, and groups.
Do not let group nesting bypass campaign limits.

WAITING AND RESOURCE SAFETY

Parents awaiting children release active inference/execution permits.
Their resident kernel memory remains accounted for.

Do not admit an impossible dependency arrangement that can only wait forever.
Return a resource-blocked result when the required child cannot be admitted.

Default children to read-only investigation.
For code-writing children, use separate worktrees/staging areas.
Use one integrating writer and explicit conflict checks.

Child results contain:
- status;
- concise summary;
- evidence and artifact references;
- unresolved questions;
- usage;
- instruction revision.

Never automatically insert full child transcripts into the parent prompt.

Durable handles must remain usable after model-context rollover or Python
session loss.


10. ADAPTIVE ALLOCATION IS A REPLACEABLE POLICY

Separate:
- strategy proposes;
- Tachyond admits and enforces;
- evaluator checks outcomes.

Provide a small policy interface:

    input:
        campaign snapshot
        recent outcomes
        remaining budget
        resource availability

    output:
        propose work
        resize group
        reallocate an existing allowance
        request verification
        stop a branch
        pause campaign

Initially provide:
- fixed allocation;
- explicit model-proposed allocation;
- a simple deterministic adaptive policy for tests/experiments.

Do not implement RL training, a learned scheduler, a universal task-difficulty
estimator, or an evolutionary-search framework in this release.

There must be one active allocation controller per group.
Reject stale proposals using revisions/generations.
User steering takes precedence over obsolete automated decisions.

Do not automatically double the swarm whenever progress stalls.

Expansion should require:
- explicit useful work;
- available authorized budget;
- available execution capacity.

Support both widening independent investigations and deepening an existing
approach. More concurrent workers are not the only way to spend more compute.

Verification effort must be budgeted alongside candidate generation.

Expose these mechanisms through IPython so later strategies can be composed
without changes to the kernel.


11. EXAMPLE TARGET WORKFLOW

The following illustrates the intended API, not an instruction to hard-code
a benchmark-specific workflow:

    agents = require("agents")
    history = require("history")
    workspace = require("workspace")

    prior = await history.search(
        "benchmark regression",
        scope="current_project",
        limit=5,
    )

    group = await agents.group(
        specs=[
            {
                "objective": "Compare preprocessing changes",
                "context_refs": prior.refs,
                "profile": "coding_read_only",
            },
            {
                "objective": "Inspect benchmark configuration changes",
                "context_refs": prior.refs,
                "profile": "coding_read_only",
            },
            {
                "objective": "Analyse execution traces for bottlenecks",
                "context_refs": prior.refs,
                "profile": "coding_read_only",
            },
        ],
        max_running=2,
    )

    # The lead continues useful work while children execute.
    matches = await workspace.search("benchmark", limit=10)
    print(matches)

    # Increasing admission capacity does not create more investigations.
    await agents.resize(
        group,
        max_running=3,
        expected_revision=group.revision,
    )

    results = await agents.wait(
        group,
        mode="any",
        deadline_seconds=30,
    )
    print(results.summary)

The displayed user experience remains:

    I'm looking into it.
    1 research task · 3 investigations

No manual switching between children is required.


12. VERIFICATION AND CANDIDATE PRESERVATION

work.complete() proposes completion. It does not establish correctness.

Support a host-configured evaluation contract:
- command/test suite;
- structured artifact validation;
- task-specific metric and constraints;
- explicit human acceptance when objective verification is unavailable.

Evaluate the exact candidate version being submitted.

Protect authoritative evaluator configuration and held-out data from worker
modification. Agent-written tests are additional evidence, not a replacement
for independent checks.

On gate failure:
- retain the failed candidate and evidence;
- return a bounded observation;
- continue only within remaining budget and retry limits.

On missing verification:
- label the result unverified;
- do not imply that an open research question has been resolved.

Preserve useful candidates during further exploration.
Only compare scores obtained under compatible evaluation configurations.
Do not discard a candidate just because a non-comparable metric changed.

For ML evaluation, keep development feedback distinct from final held-out
evaluation. Do not repeatedly expose final held-out scores for tuning.


13. CLARIFICATION, OBSERVABILITY, AND RECOVERY

Implement work.ask() as a durable attention request.
It parks the relevant work without blocking unrelated conversation.

The interaction layer displays projections. It is not the complete
observation store.

Persist causal operational and research data below the UI:
- campaign/group/work/attempt identities;
- model and tool operations;
- allocation decisions;
- budget reservations and reconciliation;
- candidate/evaluation relationships;
- clarification and steering events;
- terminal outcomes.

Keep raw high-volume logs in bounded external storage.
Keep semantic transitions durable.
The UI receives compact, coalesced projections rather than every event.

Report:
- queued work items;
- active executions;
- resident kernels;
- in-flight model calls;
- completed/blocked/failed work;
- current budget;
- useful findings;
- pending attention.

Do not label queued logical work as concurrent running agents.

Use idempotent commands and durable dispatch/reconciliation.
Handle crashes between registration and execution.
An external side effect with an unknown outcome must not be blindly replayed.

Context snapshots must retain:
- current objective;
- active handles;
- important evidence/candidate references;
- pending questions;
- current instruction revision;
- remaining budget;
- activated capability set.

Recovery must not replay arbitrary Python cells to reconstruct state.


14. SECURITY AND CONCURRENCY INVARIANTS

- No root requirement.
- No unsafe Rust in Tachyon-owned crates.
- No provider credentials in worker prompts, environment, logs, or IPC payloads.
- Agents cannot obtain unrestricted database or daemon access.
- Resource IDs are scoped and validated server-side.
- Capability activation cannot enlarge permissions.
- No locks or database transactions held across asynchronous waits.
- State owners must not await long-running work inside their event handlers.
- Bounded channels need explicit overload policies.
- Avoid cyclic mailbox waits, not only mutex deadlocks.
- Keep cancellation/control delivery separate from noisy progress traffic.
- Bound process output, model output, retries, and storage.
- Preserve capacity for foreground interaction.
- Record provider-side queueing separately from local scheduling.

IPython, shell, cwd, HOME changes, and require() are not security sandboxes.

For arbitrary code:
- use the configured isolation backend;
- or require explicit unisolated development mode;
- operate in disposable worktrees;
- never silently fall back from requested isolation to host execution.

A denied built-in file operation does not prevent arbitrary Python from
accessing the same file unless the OS boundary enforces it.

Do not modify the running Tachyon installation automatically.
Produce reviewable patches and verification evidence.
Installation/promotion remains explicitly authorized.


15. TESTS AND MEASUREMENTS

Use scripted model responses and fake capabilities for most tests.
Do not spend API credits to test scheduler mechanics.

Required test groups:

Capability loading:
- unknown/unauthorized capability;
- repeated require without duplicate initialization/instructions;
- eager versus lazy exposure;
- reconstruction after compaction.

Python:
- persistent variables;
- top-level await;
- exception and display handling;
- malformed/unbounded output;
- hostcall deadline;
- cancellation;
- kernel loss without duplicate side effects.

Tools/context:
- exact edit conflicts;
- scoped reads/search;
- huge stdout and stderr;
- bounded output storage;
- retrieval by durable reference;
- artifacts remain immutable after workspace changes.

Budgeting:
- concurrent admissions cannot overspend the ledger;
- child allocations do not multiply root allowance;
- retry and compaction costs are counted;
- cancellation preserves unresolved expenditure;
- restart reconstructs reservations;
- budget exhaustion preserves resumable progress.

Delegation:
- spawn returns an admitted/queued handle, not a completed answer;
- depth/total-work/concurrency limits;
- parent waiting with constrained capacity does not deadlock;
- resize does not create work;
- drain-on-shrink;
- duplicate spawn;
- stale steering;
- late result;
- parent replacement retains child handles;
- worktree conflicts are detected.

Verification:
- exact candidate evaluated;
- failed gate produces bounded feedback;
- protected evaluator cannot be changed by worker;
- unverified output is labelled correctly;
- best known candidate survives subsequent failures.

Scalability:
- synthetic workloads at 1, 8, 32, 128, and 1000 logical work items;
- vary active execution cap separately;
- test real subprocess lifecycle at smaller scale;
- measure event lag, cancellation, memory, disk growth, and foreground latency.

Real-model comparisons, only when authorized:
- one Ghost;
- independent bounded workers;
- fixed collaborative group;
- adaptive collaborative group.

Use matched campaign budgets, hardware limits, models, and evaluators.
Count coordination and verification costs.
Do not assume adaptive allocation wins.


16. IMPLEMENTATION ORDER AND RELEASE GATES

PHASE 1 — Preserve behavior and modularize
- Add targeted baseline fixtures/telemetry.
- Extract the kernel, registry, profiles, and capability boundaries.
- Reuse existing tools behind the registry.
Gate: adding/removing a capability does not change the kernel.

PHASE 2 — Make one Ghost dependable
- Repair workspace and process execution.
- Integrate persistent IPython and the Rust bridge.
- Implement bounded context references and output retrieval.
Gate: one Ghost can inspect, edit, execute, and diagnose failures reliably.

PHASE 3 — Make progress persistent and verifiable
- Add artifact versions, attempts, outcomes, and scoped history.
- Add completion gates and durable clarification requests.
Gate: Ghost completes representative Tachyon changes with reviewable patches,
verification evidence, and recorded human interventions.

PHASE 4 — Add budgeted collaboration
- Implement campaign ledger and admission checks first.
- Add durable child/group handles and communication.
- Add resize, steering, cancellation, and safe waiting.
Gate: bounded parallel investigations work without budget leaks, conflicting
workspace writes, or lost child state.

PHASE 5 — Add adaptive policy and evaluate
- Add the small allocation-policy interface and baseline policies.
- Expose compact projections through the existing UI.
- Run synthetic scale tests and authorized small real-model comparisons.
Gate: allocation behavior, cost, progress, and failure recovery are measurable.

Do not introduce:
- new permanent planner/reviewer/state agents;
- unrestricted recursive swarms;
- all-to-all agent chat;
- model training or RL pipelines;
- dynamic Rust plugin loading;
- full MCP integration;
- a new database or vector-search stack;
- a new TUI/orchestrator architecture;
- multimodality;
- cloud product features;
- automatic self-modification of the running system.

After each phase report:
- changes made;
- tests executed and results;
- remaining limitations;
- any unresolved security/correctness issues;
- the smallest next implementation step.

The final objective is not maximum agent count.

It is:
A capable Ghost can construct and adapt a bounded research strategy, preserve
evidence and partial progress, verify candidate results, and participate in
durable multi-agent work while Tachyon remains responsive to the user.
```

