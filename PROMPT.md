````markdown
# Tachyon — v0 Project Specification

Tachyon is a minimal, Rust-native runtime for secure autonomous agents.

The project is inspired by systemd, Docker, mini-SWE-agent, Prime Agent,
OpenCode, and SWE-ReX.

The core idea is simple:

> Tachyon manages agents the way systemd manages services and Docker manages
> containers.

v0 consists of:

- `ghost` — the agent harness
- `tachyond` — the persistent daemon
- `tachyon` — the CLI
- TUI — a client of Tachyond

Phantom/orchestration is explicitly out of scope for v0.

---

# 1. Architecture

```text
                         User
                          │
                    ┌─────┴─────┐
                    │           │
                 CLI/TUI     future clients
                    │
                    │ IPC
                    ▼
                Tachyond
                    │
             ┌──────┴──────┐
             │             │
             ▼             ▼
           Ghost         Ghost
             │             │
             ▼             ▼
          Sandbox       Sandbox
````

## Tachyon

The user-facing CLI and TUI.

Tachyon is a client of Tachyond and does not own agent state.

Responsibilities:

* Start agents
* Stop/kill agents
* Inspect agents
* View logs
* Attach to agents
* Execute commands inside agent environments
* Display resource usage
* Manage provider configuration
* Display daemon status

Closing Tachyon must not stop running agents.

---

## Tachyond

The persistent daemon and source of truth.

Responsibilities:

* Agent lifecycle
* Ghost process management
* Sandbox lifecycle
* Resource management
* Persistence
* IPC/API
* Provider management
* Event streaming
* Recovery after restart

Tachyond should remain alive independently of the CLI/TUI.

```text
TUI exits
    ↓
Tachyond continues
    ↓
Ghost continues
```

Tachyond should behave conceptually like a small init system for agents.

---

## Ghost

Ghost is the canonical agent harness.

Ghost is deliberately minimal.

Responsibilities:

* Maintain the model conversation
* Call the configured LLM
* Execute commands
* Return command output to the model
* Handle command cancellation
* Report lifecycle events to Tachyond
* Handle termination
* Communicate basic runtime state to Tachyond

Ghost does not manage other agents.

Ghost does not manage the host.

Ghost does not have orchestration capabilities in v0.

---

# 2. Ghost Tool Philosophy

The model-facing interface should be as small as possible.

Initial primary capability:

```text
exec(command)
```

The agent's environment provides the tools required to perform useful work.

Typical environment:

```text
bash
python3
git
rg
fd
agent-browser
```

This allows Ghost to perform:

* Software engineering
* Repository exploration
* File modification
* Testing
* Compilation
* Python execution
* Data analysis
* Web research
* Browser automation
* General automation

Do not initially create separate model-facing tools for:

* read
* write
* edit
* grep
* glob
* web search
* web fetch

The shell and Python environment provide these capabilities.

Additional first-class tools should only be introduced if real-world testing
shows that the single `exec()` interface is insufficient.

The goal is to minimize:

* Tool schemas
* Prompt tokens
* Implementation complexity
* Tool-specific failure modes
* Maintenance burden

---

# 3. Browser

Ghost should have browser access for research and web automation.

Initial implementation:

```text
agent-browser
```

Ghost invokes it through:

```text
exec("agent-browser ...")
```

The browser is therefore an executable inside the agent environment rather
than a native Ghost integration.

Preferred browser backend:

```text
Lightpanda
```

Optional fallback:

```text
Chromium
```

The browser should be isolated inside the Ghost environment.

The Tachyon host should not require Node.js or browser dependencies merely to
run Ghost.

The agent environment may contain additional runtimes required by tools such
as `agent-browser`.

---

# 4. Python

Python is not a Tachyon dependency.

Ghost should execute whatever Python interpreter is available inside its
environment:

```text
python3
```

Do not use:

* PyO3
* Embedded Python
* RustPython
* virtualenv management
* Python package management in Tachyon

Python versions and packages belong to the agent environment.

This allows an agent to use different Python versions without introducing
Python dependency conflicts into the Tachyon host.

---

# 5. Execution Environment

Ghost should not care where it is running.

Use an abstraction conceptually equivalent to:

```text
ExecutionEnvironment
├── Local
└── Firecracker
```

Ghost interacts with the execution environment rather than directly managing
the underlying sandbox.

The local backend is useful for development.

The Firecracker backend is the intended secure execution environment.

Each Ghost normally receives one sandbox for the lifetime of its task.

Do not create a new VM for every command.

```text
Tachyond
   ↓
Create sandbox
   ↓
Start Ghost
   ↓
Ghost executes many commands
   ↓
Ghost completes
   ↓
Destroy sandbox
```

---

# 6. Sandbox

The sandbox should provide:

* Filesystem isolation
* Process isolation
* CPU limits
* Memory limits
* Dedicated workspace
* Internet access

Network security is intentionally simple in v0.

Agents have internet access.

Advanced network policies are future work.

Agents must not receive unrestricted access to the host filesystem.

Use a dedicated Tachyon data directory:

```text
~/.local/share/tachyon/
├── agents/
├── workspaces/
├── artifacts/
└── state/
```

Each agent receives its own workspace:

```text
~/.local/share/tachyon/workspaces/<agent-id>/
```

The exact host/guest filesystem sharing mechanism is implementation-defined.

---

# 7. Security Requirements

These are hard requirements.

## No Root

Tachyon must not require root privileges during normal operation.

Do not require:

```text
sudo tachyon
sudo tachyond
```

Tachyon should operate as an unprivileged user.

User-level systemd integration should be supported:

```text
systemctl --user enable tachyond
systemctl --user start tachyond
```

---

## No Unsafe Rust

The project must contain no unsafe Rust.

Every crate should contain:

```rust
#![forbid(unsafe_code)]
```

Do not introduce unsafe Rust for performance.

Dependencies may internally use unsafe Rust unless a stronger restriction is
introduced later.

---

## Untrusted Agents

Ghost must be treated as untrusted.

Assume:

* Model output can be malicious.
* Repositories can contain malicious code.
* Packages can be compromised.
* Websites can contain prompt injection.
* Commands can be destructive.
* Agents can behave incorrectly.

A compromised Ghost must not automatically gain access to:

* Host credentials
* Arbitrary host files
* Other agent workspaces
* Other agents
* Host privileges
* Tachyond's internal state

The sandbox is a security boundary.

---

# 8. Ghost ↔ Tachyond Control Channel

Ghost should have a small control channel to Tachyond.

This is separate from the model-facing `exec()` capability.

Ghost can report:

```text
started
heartbeat
progress
command_started
command_finished
completed
failed
```

Tachyond can request:

```text
interrupt
terminate
```

Tachyond remains authoritative over lifecycle state.

Ghost reports are informational.

Tachyond independently observes the actual process and sandbox state.

Ghost cannot terminate another agent.

Ghost cannot arbitrarily modify Tachyond.

---

# 9. Agent Lifecycle

Tachyond owns the lifecycle of every Ghost.

```text
CREATED
   ↓
STARTING
   ↓
RUNNING
   ├──→ COMPLETED
   ├──→ FAILED
   ├──→ INTERRUPTED
   └──→ TERMINATED
```

Core lifecycle operations:

```text
start
stop
restart
kill
inspect
logs
attach
exec
```

`stop` should attempt graceful termination.

`kill` should force termination.

The exact signal sequence is implementation-defined, but the intended
semantics are similar to Docker/systemd.

---

# 10. Persistence and Reliability

Tachyond must persist enough state to recover after restart.

Persist:

* Agent ID
* Task/prompt
* State
* Workspace
* Sandbox ID
* Start time
* End time
* Exit reason
* Relevant events

Startup:

```text
systemd
   ↓
tachyond
   ↓
restore state
   ↓
reconnect/recover agents
   ↓
ready
```

Closing or crashing the CLI/TUI must not affect running agents.

If Tachyond itself restarts, it should recover as much agent state as possible.

The daemon is the persistent source of truth.

---

# 11. CLI

The CLI should mirror familiar systemd/Docker conventions.

The core mental model is:

```text
systemd → services
Docker   → containers
Tachyon  → agents
```

## Core Commands

```text
tachyon start <task>
tachyon ps
tachyon status
tachyon inspect <id>
tachyon logs <id>
tachyon attach <id>
tachyon exec <id> <command>
tachyon stop <id>
tachyon kill <id>
tachyon restart <id>
tachyon top
```

## Examples

Start an agent:

```text
tachyon start "Analyse this repository and fix the failing tests"
```

List agents:

```text
tachyon ps
```

View daemon status:

```text
tachyon status
```

Inspect an agent:

```text
tachyon inspect abc123
```

Follow logs:

```text
tachyon logs -f abc123
```

Attach to an agent:

```text
tachyon attach abc123
```

Execute a command inside an agent:

```text
tachyon exec abc123 "python3 --version"
```

Gracefully stop an agent:

```text
tachyon stop abc123
```

Force kill an agent:

```text
tachyon kill abc123
```

Restart an agent:

```text
tachyon restart abc123
```

View resource usage:

```text
tachyon top
```

The CLI communicates exclusively with Tachyond.

The CLI must not directly manage Ghost processes or Firecracker.

---

# 12. Daemon Management

Tachyond should normally be managed through user-level systemd.

```text
systemctl --user status tachyond
systemctl --user start tachyond
systemctl --user stop tachyond
systemctl --user restart tachyond
```

The Tachyon CLI may provide:

```text
tachyon status
```

to report whether Tachyond is available.

A dedicated:

```text
tachyon daemon ...
```

command group is optional and should not duplicate systemd functionality
unless there is a clear UX benefit.

---

# 13. TUI

The TUI is a client of Tachyond.

It should display:

* Running agents
* Agent state
* Task
* Runtime
* CPU usage
* Memory usage
* Logs
* Errors
* Completion state

The TUI should be reconnectable.

Closing the TUI must not terminate agents.

A future GUI can use the same Tachyond API.

---

# 14. Provider Management

Initial provider:

```text
OpenRouter
```

Automatically detect:

```text
OPENROUTER_API_KEY
```

Provider credentials remain on the host.

Secrets must never be placed into the Ghost sandbox.

The host-side provider layer communicates with the model.

v0 only needs one active provider/model configuration.

Multiple providers and model routing are future work.

---

# 15. IPC / API

Tachyond should expose a local IPC API.

Requirements:

* CLI and TUI use the same API.
* Local clients operate as the current user.
* Agent events can be streamed.
* Logs can be streamed.
* Agent lifecycle can be controlled.
* Future remote clients should be possible without redesigning Tachyond.

Potential future clients:

```text
CLI
TUI
GUI
SSH client
Mobile client
Remote orchestrator
```

The IPC protocol should remain independent of the CLI implementation.

---

# 16. Rust Workspace

Suggested structure:

```text
tachyon/
├── Cargo.toml
├── README.md
├── PROJECT.md
│
├── crates/
│   ├── ghost/
│   ├── tachyond/
│   └── tachyon/
│
└── tests/
```

Responsibilities:

```text
ghost/
    agent loop
    model interface
    command execution
    browser environment
    runtime control

tachyond/
    daemon
    agent manager
    sandbox manager
    persistence
    IPC
    provider management

tachyon/
    CLI
    TUI
    Tachyond client
    configuration
```

Keep dependencies minimal.

Potential initial dependencies:

```text
tokio
serde
serde_json
thiserror
tracing
tracing-subscriber
uuid
clap
ratatui
crossterm
reqwest
```

Only add dependencies when they solve a concrete problem.

---

# 17. Implementation Order

## Phase 1 — Ghost

Ghost is the first implementation target.

Do not start with Firecracker.

First prove the basic agent loop:

```text
User task
   ↓
Model
   ↓
exec()
   ↓
stdout/stderr
   ↓
Model
   ↓
...
   ↓
Completed
```

Implement:

* Model interface
* OpenRouter
* Conversation loop
* `exec()`
* stdout/stderr streaming
* Exit codes
* Timeouts
* Cancellation
* Process cleanup
* Browser access
* Ghost lifecycle events
* Basic runtime control channel

Use a local execution environment initially.

The goal is a small, useful mini-SWE/Prime-Agent-style harness.

---

## Phase 2 — Tachyond

Once Ghost is reliable, implement Tachyond.

Implement:

* Persistent daemon
* Local IPC
* Agent creation
* Agent state
* Process lifecycle
* Sandbox abstraction
* Persistence
* Event streaming
* Resource limits
* Firecracker backend
* Ghost control channel

Target:

```text
Tachyond
   ↓
start
   ↓
Firecracker
   ↓
Ghost
   ↓
task completes
   ↓
Tachyond records result
```

---

## Phase 3 — Tachyon CLI

Build the CLI against the Tachyond API.

The CLI should never need to understand how Ghost or Firecracker works.

```text
tachyon
   ↓
IPC
   ↓
Tachyond
```

Implement the systemd/Docker-style commands.

---

## Phase 4 — TUI

Build the TUI using the same Tachyond API.

```text
CLI ──────┐
          │
TUI ──────┼──→ Tachyond
          │
future GUI┘
```

The TUI must remain completely independent from Ghost and Firecracker.

---

# 18. v0 Success Criteria

The following should work:

```text
tachyon start "Analyse and improve this repository"
```

Resulting architecture:

```text
Tachyon
   ↓
Tachyond
   ↓
Sandbox
   ↓
Ghost
   ↓
LLM
   ↓
exec()
   ↓
bash / python / git / browser / etc.
```

Ghost must be able to:

* Inspect a repository
* Modify files
* Run tests
* Run Python
* Run arbitrary installed programs
* Browse the web
* Research information
* Compile software
* Install packages inside the sandbox
* Produce artifacts

Tachyond must:

* Keep agents running after the CLI/TUI exits
* Track agent state
* Stream logs/events
* Start/stop/kill agents
* Persist agent metadata
* Recover after restart
* Manage the sandbox lifecycle

Tachyon must:

* Provide a simple systemd/Docker-style CLI
* Provide a reconnectable TUI
* Never directly manage sandbox internals

Security requirements:

* No root
* No unsafe Rust
* Agents are sandboxed
* Host credentials are protected
* Agents cannot control other agents

---

# 19. Design Principles

## Small Core

Prefer a few powerful primitives over a large tool API.

The initial model-facing interface is:

```text
exec()
```

## Live Off the Land

Capabilities should generally come from programs available inside the
environment rather than bespoke Tachyon integrations.

Examples:

```text
bash
python
git
rg
fd
agent-browser
```

## Rust Host

The Tachyon host/runtime is entirely Rust.

Python, Node, browser tooling, package managers, and other runtimes may exist
inside agent environments but should not become dependencies of Tachyon itself.

## Sandbox First

Security is more important than maximum theoretical performance.

## Persistent Daemon

Tachyond is the stable foundation.

Clients can crash, restart, disconnect, or be replaced without stopping
background work.

## Familiar UX

Tachyon should feel immediately familiar to users of systemd and Docker.

```text
tachyon ps
tachyon logs
tachyon exec
tachyon inspect
tachyon stop
tachyon kill
```

## One Canonical Harness

Ghost is the canonical agent harness.

Future orchestrator functionality should build on Ghost rather than creating
a second independent harness.

---

# 20. Future Architecture — Out of Scope

Future versions may introduce Phantom.

Phantom should eventually be the same Ghost runtime with additional
orchestration capabilities.

Conceptually:

```text
                         Tachyond
                            │
                         Phantom
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
            Ghost         Ghost         Ghost
              │             │             │
             VM            VM            VM
```

Phantom would gain capabilities such as:

* Spawn agents
* Inspect agents
* Monitor agents
* Allocate resources
* Interrupt agents
* Terminate agents
* Delegate work
* Maintain long-running tasks
* Improve the Ghost harness
* Deploy updated harness versions

This should not complicate Ghost or Tachyond v0.

The architecture should instead make it possible to add these capabilities
later through the existing Tachyond API and capability model.

```
```

