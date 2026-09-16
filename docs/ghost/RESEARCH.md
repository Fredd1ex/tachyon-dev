# Research Foundation

Foundation draft, reviewed 2026-09-10. Canonical names are in
[TERMINOLOGY.md](TERMINOLOGY.md); implemented call shapes are in [API.md](API.md).
This document records direction and attribution, not a launch specification.

## CURRENT Boundary

Research is an approved top-level concept, not Project. Tachyond now persists
immutable Research metadata and draft Campaign metadata under it. Creating either
does not authorize spending, create groups, start a worker, call a model, or
launch research. Current task persistence and Ghost's tool runtime are useful
foundations, not an implemented durable research/evidence/budget system.

Native packages, synchronous Python workspace proxies, work-scoped async exec,
and bounded live-output `ctx` retrieval exist. Cross-Research access control,
campaign/group execution ledgers, recursive agent tools, shared admission
budgets, durable output retrieval, and kernel restoration do not. Host-local
execution remains a guardrail environment, not an enforced sandbox.

## PROPOSED Foundation

Build the **root Research/Campaign ledger before group orchestration**. A bounded
campaign belongs to one Research; nested groups coordinate approaches within
that campaign. Durable Work objectives, attempts, candidates, evaluations, and
evidence must remain attributable when workers are replaced or groups reorganize.
Use temporary synthesis/review Work rather than a permanent PI or chair agent.
Tachyond owns lifecycle and admission authority; Ghost executes assignments.

The ledger should connect exact candidate/artifact versions to evaluator inputs,
results, findings, and derivation edges. Preserve failures, contradictions, and
unknowns alongside successful results. A stage transition into verification is
not proof; a trace is not an evidence graph, and a model's declaration of success
is not an independent evaluation. Exported evidence needs retained bytes or an
explicit unavailable marker, not just a workspace path that may later disappear.

### Concurrency And Budgets

- Separate durable admission/queue depth from active execution. Thousands of
  admitted objectives do not require thousands of simultaneously resident workers.
- Bound queues, active model requests (including provider/model rate limits),
  resident kernels, and CPU/process execution independently. These are different
  bottlenecks; no single worker-count semaphore describes them all.
- A parent waiting only for children releases active execution/model permits so
  children can run. Retained state still consumes its actual memory/kernel quota;
  releasing an execution permit does not make that state free.
- One cell executes per Python kernel. Parallel children require independent
  execution contexts; async admission must not recursively wait under a parent's
  kernel lock. Registry locks and database transactions never span child/model/
  process waits; short atomic reservations precede external execution.
- Reserve authorized budget explicitly **before verification**, as before other
  execution. Verification is not free work allowed after exploration exhausts
  the limit. Reserve a verification allocation up front or obtain a new explicit
  authorization; otherwise report the candidate as unverified.
- Root accounting includes descendants, model usage, and verification costs under
  declared units. Reserve, settle actual usage, and reconcile uncertain charges
  durably. Restarts, model switches, new attempts, compaction, and worker/kernel
  replacement must not reset spent budget or erase outstanding reservations.
- Budget exhaustion prevents further unauthorized work, not evidence publication
  or truthful failure reporting. Unknown side effects/charges remain unknown until
  reconciled; recovery must not manufacture either success or a fresh allowance.

### Ownership And Recovery

Research IDs anchor durable relationships; titles and paths do not. Workspace
cleanup and Research retention are separate decisions. Cancelled Work is not a
detached viewer, and a detached viewer is not cancellation authority. Define
descendant cancellation and retained-work policy explicitly before exposing it.
Cross-Research messaging, evidence reads, and artifact imports require explicit
permission and provenance; shared workers, same-user paths, or guessed IDs are
not grants. These are proposed controls, not current sandbox guarantees.

Checkpoints must state what they restore: metadata, conversation, inputs, or
specific serializable state. They do not promise arbitrary Python object graphs,
open sockets, subprocess continuation, or safe replay of effects. Recovery must
separate queued intent, confirmed outcome, and unknown effect, then reconcile
before retrying. Kernel loss currently means a new empty kernel, not restoration.

## References And Choices

All sources below were reviewed on **2026-09-10**. Descriptions attribute claims
to their authors; no experiments, mathematical proofs, benchmark scores, or
scaling results were reproduced or independently validated for this foundation.

| Source | What We Borrow | What We Do Not Infer Or Adopt |
|---|---|---|
| OpenAI, [On the Navier-Stokes Millennium Prize Problem](https://openai.com/index/navier-stokes-solution/), 2026-09-08 | The article reports groups exploring different formulations, consolidation/cross-pollination of intermediate insights, and separate formalization/verification. It reports on the order of 10,000 concurrent agents in the successful group. Borrow diverse bounded approaches and explicit synthesis/verification stages. | The article is not a detailed runtime topology: it does not establish one OS process, kernel, CPU, or simultaneous model request per agent, nor a scheduler we can copy. No Tachyon scale or scientific-result claim follows. |
| Jin Li et al., [Praxist: From Experimental Artifacts to Solution Lineages](https://arxiv.org/abs/2608.25955), v1, 2026-08-26 | Lineage-centered artifacts, evaluator outcomes, typed findings/evidence, and separation of local construction from cohort-level synthesis. Preserve useful mechanisms and unresolved claims across attempts. | Do not import benchmark/cost claims as Tachyon expectations or duplicate the entire generational/lane/agenda architecture. No mandatory permanent PI/chair hierarchy. |
| Alex L. Zhang, Tim Kraska, and Omar Khattab, [Recursive Language Models](https://arxiv.org/abs/2512.24601), first submitted 2025-12-31; reviewed abstract v3, 2026-05-11 | Context as an external environment that can be inspected/decomposed programmatically, with bounded recursive delegation as a future direction. | Not a guarantee of arbitrary context capacity or transferable benchmark gains. Recursion still needs admission, permissions, budget accounting, and bounded output. Python need not be the sole tool. |
| Seth Karten, Alex L. Zhang, Kevin Thomas, Sebastian Muller, and the Prime Intellect Team, [Prime Agent: A self-improving RLM agent](https://www.primeintellect.ai/blog/prime-agent), 2026-08-05 | Host-owned orchestration, programmatic delegation, admission handles distinct from answers, and separating client attachment from execution lifetime. | Do not adopt sole-Python-tool restriction, dynamic plugins/extensions, unrestricted harness mutation, or arbitrary pickle/state restoration guarantees. Article recovery descriptions are not Ghost contracts. |
| PrimeIntellect-ai contributors, [Using Prime Agent](https://github.com/PrimeIntellect-ai/prime-agent/blob/main/packages/coding-agent/docs/usage.md), mutable `main` guide reviewed 2026-09-10 | The guide clarifies child handles return at admission and replies arrive as messages; host ownership is distinct from the Python bridge. It also distinguishes persistent goals from autonomous continuation policy. | The reviewed guide starts enabled autonomous runs with fresh counters and runs gates before ordinary continuation-limit checks. Do not copy those semantics into a durable campaign budget: no restart/model budget reset and no verification without prior reservation. The guide is not a pinned API dependency. |

## Contracts Still Needed

Before execution is implemented, specify budget units/authority and unknown-charge
settlement, cancellation propagation versus retention, artifact-byte retention and
Research deletion, cross-Research grant/revocation semantics, and versioned
Work/attempt admission plus recovery receipts. These are implementation contract
questions, not a request to reconsider the approved Research/Campaign vocabulary.
No unresolved item authorizes a draft campaign to spend or launch.
