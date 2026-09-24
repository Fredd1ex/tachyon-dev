# Tachyon

> **One conversation. Many autonomous research processes. One continuous research collaboration.**

Tachyon is a Rust-first persistent research runtime for working with LLMs.

Instead of treating every model call as a separate agent or chat, Tachyon presents one continuous assistant while coordinating asynchronous research, coding, experiments, tools, and background workers underneath.

The goal is simple:

**You steer the research. Tachyon manages the machinery.**

## Why Tachyon?

Most agent harnesses are built around individual tasks. Tachyon is built around **continuity**.

* **Persistent** — conversations, tasks, findings, artifacts, and research state survive model and process restarts.
* **Asynchronous** — keep talking while background work continues.
* **Model agnostic** — use models through OpenRouter without tying the runtime to a specific provider.
* **Multi-agent without the clutter** — workers are disposable implementation details, not identities the user has to manage.
* **Research oriented** — preserve experiments, evidence, failed attempts, findings, and verification.
* **Local first** — the runtime, state, tools, and orchestration live on your machine.
* **Rust first** — small binaries, predictable resource usage, and explicit lifecycle control.

## How it works

```text
                         You
                          │
                 TUI / CLI / Voice
                          │
                          ▼
                  Interaction Layer
                          │
                    Conversation
                          │
                          ▼
                      Tachyond
            persistent runtime + authority
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
       Ghost           Campaigns        Memory
      workers          background       research
          │               │               state
          └───────────────┼───────────────┘
                          ▼
                 tools / sandboxes /
                 models / web / code
```

LLMs reason.

**Tachyon owns state, ordering, scheduling, persistence, budgets, and lifecycle.**

## Features

* Persistent foreground conversation
* Background tasks and research campaigns
* Disposable Ghost workers and subagents
* Durable task, history, artifact, and research state
* Model/token/resource accounting
* Scheduling and recurring work
* Todo and monitoring primitives
* Verification and campaign oversight
* Programmable research tools
* Web research and document retrieval
* Detachable terminal UI
* CLI/headless operation
* OpenRouter model support

Tachyon is under active research and development. Interfaces may change before `v1`.

## Getting started

### Requirements

* Rust stable
* Cargo
* Git
* An OpenRouter API key
* Linux recommended for the full runtime

Optional research workloads may require additional tools such as Python, compilers, or project-specific dependencies.

### Build

```bash
git clone https://github.com/Fredd1ex/tachyon-dev.git
cd tachyon-dev

cargo build --release --locked
```

### Install from source

```bash
cargo install --path crates/tachyon --locked
cargo install --path crates/tachyond --locked
```

Make sure `~/.cargo/bin` is on your `PATH`.

### Configure

```bash
export OPENROUTER_API_KEY="..."
```

Then start Tachyon:

```bash
tachyon
```

For development:

```bash
cargo run -p tachyon
```

## Project philosophy

Tachyon tries to keep the **model-facing interface simple even when the runtime underneath is sophisticated**.

```text
small reasoning surface
        +
durable runtime
        +
composable capabilities
        =
persistent research assistant
```

A worker should remain easy to understand:

```text
objective
   ↓
model
   ↓
action
   ↓
observation
   ↓
model
   ↓
result
```

Everything else — persistence, recovery, accounting, concurrency, scheduling, supervision, and coordination — belongs to the runtime.

## Research

Tachyon is also a research platform for studying agent harnesses and persistent human–AI collaboration.

Current research directions include:

* persistent asynchronous agent interaction
* bounded multi-agent test-time compute
* adaptive research allocation
* agent harness design and evaluation
* durable scientific memory and evidence
* E/H/I surprise-based adaptation
* local multimodal interaction
* human steering of long-running autonomous research

The central question is:

> **Can conventional turn-based LLMs be composed under a persistent asynchronous runtime so that working with many autonomous research processes feels like working with one continuous research assistant?**

## Interfaces

Tachyon is designed so the interaction layer is independent of the runtime.

```text
TUI ─────┐
CLI ─────┼──► Tachyon conversation/runtime
Voice ───┘
```

This allows the same persistent assistant to be used interactively, from scripts and benchmarks, or eventually as a headless local voice assistant.

## Development

Run the workspace tests:

```bash
cargo test --workspace
```

Check the workspace:

```bash
cargo check --workspace
cargo clippy --workspace
```

Format:

```bash
cargo fmt --all
```

## Status

Tachyon is an experimental PhD research project.

The current focus is making the core runtime reliable and measurable before expanding into multimodal interaction and adaptive harness research.

Expect rough edges, breaking changes, and active experimentation.

## Principles

* **The researcher steers the work, not the agents.**
* **Hide execution topology. Expose research state.**
* **LLMs are disposable compute; continuity belongs to Tachyon.**
* **Commands go forward. Events come back.**
* **Expensive capabilities should be lazy.**
* **The bottleneck should be the work — not the harness.**

## License

See the repository license for details.

