Absolutely — the previous version was too flat. Here’s a cleaner nested structure that keeps the same scope but makes it much easier to navigate.

````markdown
# Tachyon

> Persistent interaction over disposable computation.

Tachyon is an experimental Rust-native runtime for persistent, asynchronous AI systems.

It combines two primary architectural ideas:

1. **Init-system-inspired supervision** for managing AI runtimes as independent services.
2. **Asynchronous conversational interaction** inspired by Thinking Machines' interaction-model architecture.

Rather than placing conversation, orchestration, tools, persistence, process management, modalities, and execution inside a single agent harness, Tachyon separates these responsibilities into small, independently replaceable components.

The central principle is:

> **Continuity belongs to the task and interaction, not to the process.**

---

## 1. Vision

### 1.1 Motivation

Modern agent harnesses increasingly accumulate responsibility:

- conversation management
- model routing
- tools
- browser integration
- memory
- context management
- subagents
- scheduling
- background jobs
- persistence
- recovery
- UI integration
- sandboxing

Tachyon explores the opposite direction.

Instead of making the harness the unit of composition, Tachyon makes the **runtime system** the unit of composition.

```text
Monolithic approach

┌───────────────────────────────┐
│          Agent Harness        │
│                               │
│ model                         │
│ tools                         │
│ memory                        │
│ subagents                     │
│ orchestration                 │
│ scheduling                    │
│ persistence                   │
│ UI                            │
│ modalities                    │
└───────────────────────────────┘


Tachyon

                  tachyond
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
 Orchestrator      Ghost          STT
       │             │
       │             └── Environment
       │
       └── interaction scheduling

                + durable tasks
````

### 1.2 Research Question

The central research question is:

> **Can persistent, coherent AI interaction be constructed from small, independently disposable computational processes?**

Tachyon investigates the combination of:

```text
minimal agent harness
        +
init-system supervision
        +
asynchronous interaction
        +
conversation scheduling
        +
attention scheduling
        +
durable external task state
```

The two primary architectural ideas are:

1. **Supervised ephemeral agent execution**
2. **Asynchronous conversational orchestration external to the agent harness**

Tachyon does not claim that these mechanisms are individually new.

The contribution is their composition into a runtime architecture where interaction, computation, and process lifetime are deliberately decoupled.

---

## 2. Design Principles

### 2.1 Unix Philosophy

Do one thing and do it well.

Functionality belongs in the smallest independent component capable of providing it.

The agent harness must not become the default location for system functionality.

Prefer:

* small components
* explicit interfaces
* composition
* low dependency counts
* low lines of code
* replaceable implementations
* simple state machines

over large frameworks.

### 2.2 Everything Is Replaceable

No model, harness, interface, modality, execution environment, or orchestrator should become a permanent architectural dependency.

Components should communicate through narrow interfaces.

Examples:

```text
Ghost v1 → Ghost v2

STT A → STT B

model A → model B

orchestrator A → interaction model

shell → Firecracker

TUI → another client
```

### 2.3 Ephemeral Computation

Processes are disposable.

Tasks are not.

```text
Task A
  │
  ▼
ghost-17
  │
  X
  │
  ▼
ghost-18
  │
  ▼
Task A continues
```

Long-running work should not require one process or one model context to remain alive indefinitely.

### 2.4 Safe Rust

Tachyon is Rust-native.

Unsafe Rust is prohibited in Tachyon's own source.

```rust
#![forbid(unsafe_code)]
```

should be used where practical.

Established safe crates should provide low-level OS functionality.

### 2.5 Never Root

Tachyon must operate as an unprivileged user.

The following must never require root:

* CLI
* daemon
* orchestrator
* Ghost
* execution environments
* modality runtimes
* model runtimes

Agents should receive the minimum capabilities necessary to perform their tasks.

Stronger isolation is future work.

---

## 3. Core Terminology

### 3.1 Task

A **task** is a durable logical objective.

Examples:

```text
"Inspect the training run."

"Research Rust inference runtimes."

"Implement PROJECT.md."

"Find the London weather."
```

A task may contain:

```text
Task
├── objective
├── state
├── memory
├── dependencies
├── events
└── artifacts
```

A task is not a process.

A task may survive many processes.

### 3.2 Runtime

A **runtime** is an implementation providing a capability that Tachyon can operate.

Examples:

```text
Ghost
Orchestrator
Interaction Model
IPython
STT
TTS
Browser
Model Server
Sandbox
```

Runtime describes **what can run**.

### 3.3 Service

A **service** is a configured runtime managed by `tachyond`.

A service may define:

* runtime
* executable
* arguments
* environment
* restart policy
* health policy
* capabilities
* resource policy

Examples:

```text
orchestrator
training-monitor
weather-worker
python-kernel
speech-to-text
```

### 3.4 Process

A **process** is a concrete operating-system instance backing a service.

Example:

```text
Ghost runtime
     │
Ghost service
     │
ghost-17 process
```

Processes are ephemeral.

### 3.5 Agent

An **agent** is a process using an agent harness runtime to work on a task.

The reference agent runtime is Ghost.

### 3.6 Orchestrator

The **orchestrator** maintains the logical interaction between the user and Tachyon.

Its primary components are:

* conversation scheduler
* attention scheduler
* task decomposition
* dependency reasoning
* agent allocation
* result synthesis

The orchestrator reasons about work.

It does not directly manage OS processes.

### 3.7 Ghost

**Ghost** is Tachyon's minimal reference agent harness.

Ghost executes assigned work.

It is intentionally not an orchestration framework.

### 3.8 tachyond

`tachyond` is the persistent deterministic supervisor.

It owns runtime/process lifecycle and authoritative system state.

### 3.9 tachyon

`tachyon` is the user-facing CLI/TUI.

It attaches to Tachyon.

It does not own computation.

---

## 4. System Architecture

### 4.1 High-Level Architecture

```text
                           USER
                             │
                             ▼
                    ┌────────────────┐
                    │    tachyon     │
                    │    CLI/TUI     │
                    └───────┬────────┘
                            │
                         attach
                            │
                            ▼
                    ┌────────────────┐
                    │  Orchestrator  │
                    │                │
                    │ conversation   │
                    │ scheduler      │
                    │                │
                    │ attention      │
                    │ scheduler      │
                    └───────┬────────┘
                            │
                        daemon API
                            │
                            ▼
                    ┌────────────────┐
                    │    tachyond    │
                    │                │
                    │   supervisor   │
                    └───────┬────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
       Ghost A           Ghost B          Runtime C
          │                 │
          ▼                 ▼
     Environment       Environment
```

`tachyond` starts first.

One of its initial jobs is starting the orchestrator.

The user attaches to the running system.

### 4.2 Shared Control Plane

The CLI and orchestrator must not independently implement process management.

Both map to the same daemon API.

```text
USER
 │
 ▼
tachyon ───────────┐
                   │
                   ▼
                tachyond
                   ▲
                   │
orchestrator ──────┘
```

Operations include:

```text
start
stop
kill
restart
interrupt
resume
status
list
subscribe
```

The operation is implemented once.

`tachyond` remains authoritative.

### 4.3 Headless Operation

The daemon owns computation.

The interface does not.

```text
tachyond
├── orchestrator
├── Ghost A
├── Ghost B
└── tasks
```

continues after:

* TUI exit
* terminal closure
* SSH disconnect

The user can later reconnect.

---

## 5. Interaction Architecture

### 5.1 Interaction Model

The interaction layer is the primary current research focus.

The user should remain free to communicate while unrelated work executes.

Example:

```text
Human:
How's our ML training run going?

Tachyon:
I'll inspect it.

→ Task A starts


Human:
What's the weather like in London?

Tachyon:
Looking that up.

→ Task B starts


Human:
Will I need a coat?

→ Task C created
→ C depends on B


Tachyon:
I'll let you know once I have the forecast.


Task B completes.

Tachyon:
Probably not. The forecast looks clear.


Task A completes.

Tachyon:
And your training run is progressing normally too.
```

The defining requirement is:

> **No user utterance should be blocked merely because unrelated work is executing.**

### 5.2 Conversation Scheduler

The conversation scheduler manages the logical structure of interaction.

It determines:

* what the user means
* which topic an utterance belongs to
* whether a task already exists
* whether a new task should be created
* dependencies between requests
* unresolved questions
* follow-up relationships
* stale requests

Example:

```text
Task A:
inspect training run

Task B:
retrieve London weather

Task C:
answer "will I need a coat?"

C depends_on B
```

Instead of blocking:

```text
C = WAITING
depends_on = [B]
```

When B completes, C becomes runnable.

### 5.3 Attention Scheduler

The attention scheduler determines:

> **What deserves the user's attention right now?**

Completion does not automatically imply interruption.

A result may:

* interrupt immediately
* wait
* merge into another response
* become a notification
* remain silent
* be discarded as stale

Conceptual priorities:

```text
CRITICAL
├── urgent timer
└── safety event

HIGH
├── clarification required
└── task failure requiring intervention

NORMAL
└── requested result completed

LOW
├── maintenance
└── internal status
```

This mechanism is responsible for making asynchronous work feel like one coherent conversation rather than multiple agents competing to speak.

### 5.4 Model-Native Interaction Runtime

Tachyon should not require its orchestrator to remain a conventional text LLM.

A future model-native interaction model could replace or augment it.

Current:

```text
                 tachyond
                    │
                    ▼
              Orchestrator
               ordinary LLM
                /        \
       conversation      attention
        scheduler        scheduler
```

Future:

```text
                 tachyond
                    │
                    ▼
            Interaction Model
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Ghost A   Ghost B   Ghost C
          │         │         │
          ▼         ▼         ▼
       coding    reasoning   local
        model      model     model
```

A specialized interaction model may provide:

* low-latency speech
* turn taking
* interruption
* overlap
* backchannels
* multimodal cues

while Tachyon continues to provide:

* process supervision
* durable tasks
* background agents
* model specialization
* lifecycle
* context replacement
* detached execution

The two architectures are complementary.

---

## 6. Scheduling and Coordination

### 6.1 Process Scheduling

The orchestrator determines:

> **What work should exist?**

`tachyond` determines:

> **How should that work live as processes?**

These responsibilities must remain separate.

Possible task states:

```text
READY
RUNNING
WAITING
PAUSED
COMPLETED
FAILED
```

Coordination should use:

* messages
* events
* immutable IDs
* explicit dependencies
* daemon-owned transitions

rather than agents synchronously waiting while holding shared locks.

### 6.2 Deadlock and Race Avoidance

Avoid:

```text
Agent A acquires shared lock
Agent A waits for B
Agent B waits for A
```

Prefer:

```text
Task A
state = WAITING
depends_on = B
```

Then:

```text
TaskCompleted(B)
       │
       ▼
scheduler reevaluates A
```

The architecture should resemble an event-driven runtime rather than a collection of mutually blocking agent threads.

`tachyond` is authoritative over lifecycle transitions.

### 6.3 Agent Lifetime

An agent may remain alive while:

* actively computing
* participating in active interaction
* waiting briefly for a dependency
* retaining its current context is valuable

An agent becomes eligible for termination when:

* its task completes
* it exceeds idle TTL
* durable progress is checkpointed
* context approaches a refresh threshold
* resources are needed elsewhere

Example:

```text
RUNNING
   │
   ▼
WAITING
   │
   ├── short wait
   │      │
   │    remain warm
   │
   └── long wait
          │
          ▼
      checkpoint
          │
          ▼
      SUSPENDED
          │
          X
       process
```

Later:

```text
event
  │
  ▼
new Ghost
  │
  ▼
restore task
```

---

## 7. Ghost Harness

### 7.1 Philosophy

Ghost is Tachyon's minimal Rust agent harness.

Its design combines:

* mini-SWE-agent-style harness minimalism
* Prime-Agent-style persistent programmable execution

The guiding principle is:

> **Minimize the harness, not the capabilities available to the agent.**

### 7.2 Agent Loop

Ghost has one job:

> **Execute an assigned task.**

Conceptually:

```text
Task
 │
 ▼
Ghost
 │
 ▼
LLM
 │
 ▼
execute
 │
 ▼
Environment
 │
 ▼
Observation
 │
 └──────────► LLM
```

Ghost handles:

* task input
* model calls
* execution
* observations
* local context
* progress
* completion
* failure
* interrupts

Ghost does not handle:

* orchestration
* subagents
* conversation scheduling
* attention scheduling
* global scheduling
* daemon lifecycle
* TUI
* global persistence
* modalities

### 7.3 Minimal Model-Facing Interface

Ghost should expose as few primitives as practical.

Target:

```text
execute
finish
```

Avoid accumulating:

```text
bash
python
read_file
write_file
grep
git
cargo
browser
download
search
spawn_agent
...
```

when existing programs can provide those capabilities.

This reduces:

* tool definitions
* token overhead
* harness complexity
* routing decisions
* duplicated code

### 7.4 Living-Off-the-Land Execution

Ghost should use existing programs available inside its execution environment.

Examples:

```text
bash
python
ipython
git
rg
cargo
agent-browser
project CLIs
system utilities
```

For example:

```text
execute
   │
   ▼
git status
```

requires no Ghost-specific Git implementation.

Likewise:

```text
execute
   │
   ▼
agent-browser ...
```

allows browser automation without embedding a browser framework into Ghost.

The execution environment becomes Ghost's capability ecosystem.

### 7.5 Persistent Programmable Environment

Ghost should eventually support a persistent programmable environment.

```text
                 Ghost
                   │
                   ▼
                execute
                   │
                   ▼
            persistent REPL
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
     Python       shell     filesystem
       │
       ▼
 persistent state
```

This allows intermediate state to remain outside model context.

For example:

```python
import pandas as pd

df = ...
results = ...
```

and later:

```python
results.mean()
```

without reconstructing the state.

### 7.6 Optional IPython Runtime

IPython should be optional.

Python must not become a hard dependency of Tachyon.

Architecture:

```text
Ghost
  │
  │ Jupyter protocol
  ▼
IPython kernel
  │
  ├── Python
  ├── persistent state
  ├── scientific libraries
  └── shell access
```

Ghost remains Rust.

Python remains an external runtime.

Ghost must not:

* install Python automatically
* modify system Python
* require a global Python environment
* mutate system packages

If IPython exists, use it.

If it does not, Ghost remains functional.

### 7.7 Execution Environment Abstraction

Ghost should not depend directly on IPython.

Conceptually:

```rust
trait Environment {
    async fn execute(&mut self, input: &str) -> Result<Observation>;
    async fn interrupt(&mut self) -> Result<()>;
}
```

Potential implementations:

```text
Environment
├── Shell
├── Jupyter
├── Firecracker
└── Remote
```

The agent loop should remain unchanged when execution infrastructure changes.

### 7.8 No Ghost Subagents

Ghost must not implement nested subagents.

Avoid:

```text
Ghost A
├── Agent A1
├── Agent A2
└── Agent A3
```

This duplicates Tachyon's orchestration system.

Tachyon scales horizontally instead:

```text
                  Orchestrator
                       │
                       ▼
                    tachyond
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
      Ghost A       Ghost B       Ghost C
```

Each Ghost remains:

* visible
* independently supervised
* interruptible
* measurable
* sandboxable
* replaceable

---

## 8. Multi-Agent Scaling

### 8.1 Agent Collaboration

If Ghost A requires assistance, it does not spawn another agent.

It reports the requirement.

```text
Ghost A
   │
   │ AssistanceRequested
   ▼
Orchestrator
   │
   │ decides
   ▼
tachyond
   │
   ▼
Ghost B
```

The orchestrator may also detect poor progress and independently allocate another worker.

Ghost B might:

* research a missing detail
* independently solve the task
* debug Ghost A's approach
* verify a result
* explore an alternative

### 8.2 Horizontal Scaling

The scaling primitive is:

> **Spawn another supervised service.**

Not:

> Spawn an agent inside an agent.

Example:

```text
                  Objective
                     │
                     ▼
                Orchestrator
                     │
           ┌─────────┴─────────┐
           ▼                   ▼
       Subtask A            Subtask B
           │                   │
           ▼                   ▼
       Ghost A             Ghost B
           │                   │
           └─────────┬─────────┘
                     ▼
                  synthesis
```

### 8.3 Model Specialization

Tachyon does not require one model to perform every role.

```text
                    Orchestrator
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       Ghost A        Ghost B        Ghost C
          │              │              │
          ▼              ▼              ▼
       coding         reasoning      lightweight
       model            model           model
```

Selection may consider:

* task difficulty
* model capability
* cost
* latency
* context requirements
* availability

This allows strong workhorse models to perform difficult background tasks while cheaper or faster models handle simpler work.

---

## 9. State and Continuity

### 9.1 Persistent State

Important information must exist outside model context.

Initial location:

```text
~/.local/state/tachyon/
```

Possible structure:

```text
tasks/
└── task-001/
    ├── project.md
    ├── state.md
    ├── memory.md
    ├── events.jsonl
    └── artifacts/
```

Prefer simple formats initially.

Do not introduce databases until implementation pressure justifies them.

### 9.2 Durable vs Ephemeral State

```text
             DURABLE

Task
├── objective
├── important progress
├── decisions
├── dependencies
├── artifacts
└── relevant memory


────────────────────────────


            EPHEMERAL

Ghost
├── model context
├── REPL state
├── intermediate computation
└── temporary files
```

The system should preserve enough durable state for a new agent to continue useful work.

### 9.3 Context Refresh

Eventually:

```text
Ghost A
context grows
    │
    ▼
checkpoint
    │
    X
    │
    ▼
Ghost B
fresh context
    │
    ▼
continue Task A
```

The same principle should eventually apply to the orchestrator.

```text
orchestrator-1
      │
 context pressure
      │
      ▼
 checkpoint
      │
      X
      │
      ▼
orchestrator-2
      │
      ▼
conversation continues
```

### 9.4 Lifetime Separations

Tachyon explicitly investigates three separations:

#### Task Lifetime != Process Lifetime

Tasks survive agents.

#### Conversation Lifetime != Context Lifetime

Interaction survives context refresh.

#### System Lifetime != Component Lifetime

Individual runtimes can be replaced without restarting the logical system.

---

## 10. Interface and Observability

### 10.1 TUI

The TUI provides:

1. conversational interaction
2. observability

Example:

```text
┌───────────────────────────────────────────────────────┐
│ Tachyon                                               │
├───────────────────────────────────────────────────────┤
│                                                       │
│ You: How's the training run going?                    │
│                                                       │
│ Tachyon: I'll check.                                  │
│                                                       │
│ ● training-monitor                      RUNNING       │
│                                                       │
│ You: What's the weather in London?                    │
│                                                       │
│ Tachyon: Looking it up.                               │
│                                                       │
│ ● weather-london                        RUNNING       │
│                                                       │
│ You: Will I need a coat?                              │
│                                                       │
│ Tachyon: I'll let you know when I have the forecast. │
│                                                       │
├───────────────────────────────────────────────────────┤
│ >                                                     │
└───────────────────────────────────────────────────────┘
```

User input must remain available while background work executes.

### 10.2 Structured Events

Every significant lifecycle event should be observable.

Examples:

```text
TaskCreated
TaskReady
TaskWaiting
TaskCompleted

ServiceStarted
ServiceStopped

ProcessSpawned
ProcessInterrupted
ProcessExited
ProcessFailed
ProcessRestarted

AgentProgress
AgentAssistanceRequested
AgentCompleted

AttentionQueued
AttentionDelivered
AttentionDiscarded
```

Events should support:

* TUI updates
* logging
* debugging
* benchmarking
* replay
* profiling

Prefer structured events over ad-hoc log strings.

---

## 11. Security

### 11.1 Least Privilege

Requirements:

* no root
* safe Rust
* explicit capabilities
* minimal host access
* clear process boundaries
* no implicit privilege escalation

### 11.2 Sandboxing

The IPython environment is not a security boundary.

Future untrusted execution should occur inside Firecracker or an equivalent isolation technology.

Minimal Ghost processes make this easier because their execution boundary is intentionally small.

---

## 12. Performance and Self-Improvement

### 12.1 Profiling

Future Tachyon development should expose enough information to measure:

* model latency
* agent runtime
* process startup
* IPC overhead
* scheduler latency
* acknowledgement latency
* interrupt latency
* context size
* token consumption
* idle time
* task completion time
* CPU usage
* memory usage

Profiling is important both for research evaluation and eventual agent-driven optimization of Tachyon itself.

### 12.2 Self-Improvement

Tachyon should remain simple enough that an agent can eventually understand and improve it.

Possible workflow:

```text
tachyon run improve.md
```

Agent:

```text
inspect
  │
modify
  │
compile
  │
test
  │
benchmark
  │
profile
  │
compare
  │
propose replacement
```

Self-modification is not an MVP requirement.

The architecture should merely avoid making it unnecessarily difficult.

---

## 13. Related Systems and Influences

### 13.1 mini-SWE-agent

Influences:

* radical harness minimalism
* simple agent loop
* shell-oriented execution
* small implementation
* benchmarkable architecture

Ghost adopts this philosophy.

Project:

[https://github.com/SWE-agent/mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent)

### 13.2 Prime Agent

Influences:

* continual execution
* persistent programmable environments
* REPL-oriented interaction
* context-efficient computation
* background operation
* daemon-backed workflows

Ghost borrows the idea of a powerful persistent execution environment.

Tachyon deliberately moves:

* orchestration
* subagents
* scheduling
* lifecycle
* interaction

outside Ghost.

Project:

[https://github.com/PrimeIntellect-ai/prime-agent](https://github.com/PrimeIntellect-ai/prime-agent)

Architecture:

[https://www.primeintellect.ai/blog/prime-agent](https://www.primeintellect.ai/blog/prime-agent)

### 13.3 Thinking Machines Interaction Models

Influences:

* continuously available interaction
* asynchronous background reasoning
* non-blocking user interaction
* interruption
* results returning into an ongoing conversation

Thinking Machines investigates model-native interaction.

Tachyon investigates complementary runtime-native interaction semantics.

A future Thinking Machines-style interaction model could itself become a Tachyon runtime.

Reference:

[https://thinkingmachines.ai/blog/interaction-models/](https://thinkingmachines.ai/blog/interaction-models/)

### 13.4 IronClaw

Influences:

* Rust-native agent infrastructure
* security
* explicit runtime architecture
* sandboxing
* persistence

Tachyon focuses more narrowly on runtime supervision and asynchronous interaction.

Project:

[https://github.com/nearai/ironclaw](https://github.com/nearai/ironclaw)

### 13.5 OpenCrabs

Related work demonstrating:

* Rust-native agents
* daemon operation
* multi-agent execution
* persistent operation

Tachyon deliberately separates orchestration, interaction, supervision, and agent execution into independent responsibilities.

Project:

[https://github.com/adolfousier/opencrabs](https://github.com/adolfousier/opencrabs)

---

## 14. Architectural Comparison

### 14.1 Tachyon

Primary abstraction:

> **Runtime composition**

Strengths:

* replaceability
* horizontal scaling
* interaction separated from workers
* heterogeneous models
* headless operation
* explicit lifecycle
* small harness
* potential context replacement

Trade-offs:

* scheduler complexity
* IPC overhead
* state consistency
* higher interaction latency than model-native interaction
* more process boundaries

### 14.2 Prime Agent

Primary abstraction:

> **Powerful continual agent harness**

Strengths:

* rich persistent REPL
* strong context manipulation
* recursive agents
* continual adaptation
* mature agent functionality

Tachyon differs by moving multi-agent orchestration and system lifecycle outside the harness.

### 14.3 IronClaw

Primary abstraction:

> **Secure extensible agent infrastructure**

Strengths:

* security
* sandboxing
* capability boundaries
* Rust-native architecture

Tachyon's central research focus is interaction and ephemeral runtime composition rather than security infrastructure itself.

### 14.4 Thinking Machines

Primary abstraction:

> **Model-native interaction**

Strengths:

* extremely low latency
* natural timing
* multimodality
* interruption
* overlap
* backchannels

Trade-off relative to Tachyon:

Interaction behavior depends on specialized model architecture and training.

Tachyon instead provides complementary interaction semantics externally so conventional and heterogeneous models can participate.

The two approaches can be combined.

---

## 15. Research Evaluation

### 15.1 Agent Performance

Compare:

```text
Ghost
vs
Ghost + Tachyon
```

using appropriate coding and terminal benchmarks.

Possible metrics:

* task success
* tokens
* latency
* cost
* context size
* recovery performance

Terminal-Bench is a candidate benchmark.

### 15.2 Interaction Performance

Create scripted asynchronous interaction scenarios.

Example:

```text
T0 user asks A
T1 A starts

T2 user asks B
T3 B starts

T4 user asks C depending on B

T5 A completes
T6 B completes

T7 C becomes answerable
```

Measure:

* acknowledgement latency
* interruption latency
* dependency correctness
* conversational coherence
* stale-result rate
* unnecessary interruptions
* blocked user turns
* result delivery correctness
* task completion

---

## 16. MVP Scope

### 16.1 Required

* [ ] Rust workspace
* [ ] safe Rust only
* [ ] user-level execution
* [ ] `tachyond`
* [ ] IPC/API
* [ ] runtime/service/process model
* [ ] task model
* [ ] structured events
* [ ] process spawn
* [ ] process stop
* [ ] process kill
* [ ] process interrupt
* [ ] process restart
* [ ] process status
* [ ] Ghost
* [ ] minimal Ghost agent loop
* [ ] configurable LLM
* [ ] shell execution
* [ ] `execute`
* [ ] `finish`
* [ ] orchestrator
* [ ] daemon starts orchestrator
* [ ] conversation scheduler
* [ ] attention scheduler
* [ ] task dependencies
* [ ] orchestrator can request Ghost processes
* [ ] concurrent Ghost agents
* [ ] basic persistent task state
* [ ] TUI
* [ ] attach/detach
* [ ] non-blocking user input
* [ ] lifecycle visible in TUI
* [ ] structured logging

### 16.2 Useful After Core MVP

* [ ] persistent IPython/Jupyter runtime
* [ ] agent assistance events
* [ ] agent replacement
* [ ] context checkpoint
* [ ] basic profiling

### 16.3 Future Work

* [ ] STT
* [ ] TTS
* [ ] duplex voice
* [ ] model-native interaction runtime
* [ ] vision
* [ ] Firecracker
* [ ] remote runtimes
* [ ] distributed execution
* [ ] advanced memory
* [ ] automatic context refresh
* [ ] model routing
* [ ] self-improvement
* [ ] runtime hot replacement

---

## 17. CLI

### 17.1 Interactive

```text
tachyon
```

Attach to the default conversational session.

### 17.2 Daemon

```text
tachyon daemon start
tachyon daemon status
tachyon daemon stop
```

### 17.3 System

```text
tachyon status
tachyon ps
```

### 17.4 Tasks

```text
tachyon run <project.md>
tachyon list
tachyon attach <task-id>
```

### 17.5 Lifecycle

```text
tachyon stop <id>
tachyon kill <id>
tachyon restart <id>
tachyon interrupt <id>
tachyon resume <id>
```

The CLI and orchestrator invoke the same daemon API.

---

## 18. Initial Project Structure

```text
tachyon/
├── Cargo.toml
├── PROJECT.md
├── README.md
│
├── crates/
│   ├── tachyon/
│   │   └── src/
│   │       └── main.rs
│   │
│   ├── tachyond/
│   │   └── src/
│   │       └── main.rs
│   │
│   ├── tachyon-core/
│   │   └── src/
│   │       └── lib.rs
│   │
│   ├── tachyon-orchestrator/
│   │   └── src/
│   │       └── main.rs
│   │
│   └── ghost/
│       └── src/
│           └── main.rs
│
└── tests/
```

Do not create additional crates merely to satisfy an architecture diagram.

Split components only when implementation pressure demonstrates a real boundary.

---

## 19. Development Plan

### 19.1 Phase 1 — Interaction Model

Current priority.

Implement:

```text
User
 ↓
Orchestrator
 ↓
Conversation Scheduler
 +
Attention Scheduler
```

Validate asynchronous conversational behavior before increasing Ghost complexity.

### 19.2 Phase 2 — Supervisor

Implement:

```text
tachyon
   ↓
tachyond
   ↓
process
```

Prove:

* spawn
* inspect
* interrupt
* stop
* restart

### 19.3 Phase 3 — Ghost

Implement:

```text
Task
 ↓
Ghost
 ↓
LLM
 ↓
execute
 ↓
observation
```

Keep it deliberately minimal.

### 19.4 Phase 4 — Integration

Connect:

```text
User
 ↓
Orchestrator
 ↓
tachyond
 ↓
Ghost
```

### 19.5 Phase 5 — Parallelism

Prove:

```text
Orchestrator
   │
   ├── Ghost A
   ├── Ghost B
   └── Ghost C
```

while the conversation remains responsive.

### 19.6 Phase 6 — Dependencies and Attention

Demonstrate:

```text
Task C depends on B
```

and that B's completion does not necessarily interrupt the user immediately.

### 19.7 Phase 7 — Persistence and Replacement

Kill Ghost A.

Preserve the task.

Start Ghost B.

Continue useful work.

This is a defining architectural demonstration.

### 19.8 Phase 8 — Persistent REPL

After the interaction architecture works, investigate persistent IPython/Jupyter execution.

Do not make Ghost's sophistication a prerequisite for proving Tachyon's central idea.

---

## 20. MVP Demonstration

The target demonstration is:

```text
Human:
How's our training run going?

Tachyon:
I'll check.

● ghost-17
  inspect-training
  RUNNING


Human:
What's the weather in London?

Tachyon:
Looking it up.

● ghost-18
  london-weather
  RUNNING


Human:
Will I need a coat?

Tachyon:
I'll let you know once I have the forecast.

○ answer-coat
  WAITING(weather)


Human:
Actually, stop checking the training run.

Tachyon:
Sure.

○ ghost-17
  INTERRUPTED


● ghost-18
  COMPLETED


Tachyon:
The forecast is clear, so you probably won't need a coat.
```

The user should be able to continue talking throughout.

The TUI can then be closed.

```text
$ exit
```

`tachyond` continues running.

Later:

```text
$ tachyon
```

The user reconnects to the same logical interaction.

---

## 21. Definition of MVP Success

The MVP succeeds if it demonstrates:

1. A persistent daemon supervises independently disposable agents.
2. The orchestrator itself is a daemon-managed service.
3. The user can interact while agents work asynchronously.
4. Multiple unrelated tasks can execute concurrently.
5. Conversational dependencies can be represented explicitly.
6. Completed work does not automatically interrupt the user.
7. The attention scheduler determines when results are surfaced.
8. Agents can be independently interrupted or terminated.
9. The TUI can detach without terminating work.
10. Important task state exists independently of agent context.
11. Ghost remains a minimal worker rather than becoming an orchestration framework.
12. Multi-agent scaling occurs horizontally through Tachyon rather than recursively inside Ghost.

The defining user experience is:

> **One natural conversation over many disposable, heterogeneous, asynchronous processes.**

The defining architectural principle is:

> **Continuity is a property of the system, not of the agent process.**


