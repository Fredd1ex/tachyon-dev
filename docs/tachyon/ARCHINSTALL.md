# Tachyon — Arch Linux dependencies

Everything you need to install on an Arch-based distro to build and run
Tachyon, with what each piece is for.

## One-liner (install core + agent tools)

```bash
sudo pacman -S --needed \
  rust cargo \
  base-devel \
   python \
   python-ipython \
  git \
  ripgrep \
  fd \
  curl \
  openssl \
  firecracker \
  tmux
```

## Required — build Tachyon itself

These are needed to **compile** the Rust workspace. Tachyon is pure Rust with
`rustls` TLS, so there are **no** OpenSSL/`pkg-config` native build deps.

| Package | Why |
|---|---|
| `rust` | Rust compiler + `cargo` (stable ≥ 1.75; we use async-fn traits) |
| `base-devel` | linker (`cc`) + `make` needed by cargo for some transitives |

## Required — run the agent harness

The `ghost` harness executes tools through the `Local` backend (workdir jail).
It uses IPython as the general execution environment:

| Package | Why |
|---|---|
| `python` | Python runtime used by IPython |
| `python-ipython` | unified `ipython` execution tool for Python and shell commands |
| `curl` | general CLI network tool; agents often use it |

## Recommended — give agents a useful environment

These are the "live off the land" tools you want agents to have. Not required
to boot, but agents can't do real work without them:

| Package | Why |
|---|---|
| `git` | clone/repo work — the #1 agent task |
| `ripgrep` | `rg` fast code search |
| `fd` | `fd` fast file finding |
| `openssh` | ssh/copy; agents may need it (still blocked by jail) |

## Browser tool

Ghost advertises an `agent_browser` tool that shells out to the
`agent-browser` CLI (vercel-labs) with Lightpanda. Worker harnesses automatically
install and verify both tools inside their execution environment when required.
Ghost downloads the official platform-specific native binary directly, for
example:

```bash
curl -L -o agent-browser \
  https://github.com/vercel-labs/agent-browser/releases/download/v0.35.0/agent-browser-linux-x64
```

Do not run `agent-browser install`; that command downloads Chrome for Testing.
Ghost downloads the supported Lightpanda nightly binary directly, configures
`AGENT_BROWSER_ENGINE=lightpanda`, and verifies a live launch. The Tachyon host
does not need Node, Chrome, or Lightpanda when workers run in microVMs.

## Optional — sandboxing (Firecracker)

The eventual secure boundary is Firecracker microVMs. On Arch:

```bash
sudo pacman -S firecracker
```

You'll also need a guest kernel + rootfs image. The jailer runs Firecracker
unprivileged; the KVM module is in the kernel, so just make sure `/dev/kvm` is
readable by your user (e.g. `sudo usermod -aG kvm "$USER"`, then re-login).

## Shell / key setup

```bash
export OPENROUTER_API_KEY="sk-or-v1-..."   # before starting the daemon
tachyon daemon restart                      # daemon inherits env at launch
```

## Build & run

```bash
cargo build --release
mkdir -p ~/.local/share/tachyon/{agents,workspaces,artifacts,state}
tachyon daemon start
tachyon
```

## Quick install script

```bash
# one-shot install (paste)
sudo pacman -S --needed rust base-devel python python-ipython ripgrep fd curl git firecracker
```
