# Tachyon Installation

These instructions target Arch Linux and Arch-based distributions.

## Dependencies

Install the build toolchain and runtime utilities:

```bash
sudo pacman -S --needed \
  rust \
  cargo \
  base-devel \
  python \
  ipython \
  git \
  ripgrep \
  fd \
  curl \
  ttf-jetbrains-mono-nerd
```

Tachyon's TUI assumes a Nerd Font for status indicators, selection markers,
icons, and panel separators. JetBrains Mono Nerd Font is installed by the
dependency command above. Select `JetBrainsMono Nerd Font` in your terminal
profile if the glyphs do not render correctly.

Tachyon uses IPython as Ghost's general execution tool. IPython can run both
Python and shell commands, so Ghost does not need separate Python and Bash
tools. Shell commands are entered inside IPython with `!`, for example:

```python
!rg TODO .
```

## Browser Tool

Ghost uses Vercel's Agent Browser for web browsing. When a worker harness starts,
Ghost checks that the CLI and Lightpanda are usable. If necessary, it installs
private copies inside the harness environment. Agent Browser is pinned to the
official native `v0.35.0` release whose commands match Ghost's prompt. Ghost
downloads the matching platform asset directly, for example:

```bash
curl -L -o agent-browser \
  https://github.com/vercel-labs/agent-browser/releases/download/v0.35.0/agent-browser-linux-x64
```

The crates.io release is currently older and does not provide the `read`
command used by the harness.

It downloads Lightpanda's official nightly binary for supported platforms and
configures Agent Browser with `AGENT_BROWSER_ENGINE=lightpanda`. Tachyon never
runs `agent-browser install`, because that command installs Chrome for Testing.

This requires network access during the first worker startup but does not
compile Agent Browser or require Node. A browser is not used by coordination
roles. In a microVM deployment, downloading, verification, and browser execution
therefore remain inside the worker VM. To use existing installations instead,
expose their absolute paths to Ghost:

```bash
export TACHYON_AGENT_BROWSER_BIN="$HOME/.cargo/bin/agent-browser"
export TACHYON_LIGHTPANDA_BIN="$HOME/.local/bin/lightpanda"
```

Ghost serializes concurrent setup attempts, performs a live Lightpanda launch
check, and refuses to start the worker if verification or installation fails.
Tachyond does not install or configure browser software.

## Configure OpenRouter

Store the API key in your operating system credential store. This does not
require systemd and keeps the key out of shell history, configuration files,
and process arguments:

```bash
tachyon providers login
tachyon daemon restart
```

On Linux, Tachyon uses the kernel keyring, so the credential is memory-backed
and must be entered again after a reboot.

For ephemeral or CI use, export the API key before starting Tachyond. An
environment value takes precedence over the credential store:

```bash
export OPENROUTER_API_KEY="sk-or-v1-..."
tachyon daemon restart
```

Set model and identity in `~/.config/tachyon/config.toml`:

```toml
[model]
name = "~deepseek/deepseek-v4-flash-latest"

[provider]
name = "openrouter"
base_url = "https://openrouter.ai/api/v1"

[provider.routing]
# Choose "cost", "performance", or "manual".
profile = "cost"
allow_fallbacks = true

[provider.routing.cost]
order = ["Makora", "BaseTen", "DeepInfra"]

[provider.routing.performance]
# Median first-token latency in seconds; output tokens per second.
sort = "throughput"
preferred_max_latency = 0.5
preferred_min_throughput = 100

[provider.routing.manual]
# Set profile = "manual" and replace this list.
order = []

[names]
user = "You"
orchestrator = "Orchestrator"
```

## Build and Run

```bash
cargo build --release
./target/release/tachyon daemon start
./target/release/tachyon
```

The TUI should show the daemon as online. Press `Ctrl+P` to view the current
keyboard commands.
