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

On Linux, Tachyon prefers the persistent default collection of a Secret Service
provider over the user session D-Bus. A provider (for example GNOME Keyring or
KeePassXC with Secret Service integration enabled) must already be installed,
running, and have an unlocked persistent default collection. Tachyon does not
install a provider or unlock collections for you. Availability is not guaranteed
by a desktop environment, including COSMIC. Headless sessions also need access
to that user session bus and unlocked store.

If persistent storage fails (including an unavailable or locked store), login
falls back to the Linux kernel keyring with an explicit warning: this credential
is volatile and is lost on reboot, or earlier if the keyring is cleared. There is
no plaintext fallback. Reads prefer an existing volatile key over a persistent
key so a newer fallback login remains active after Secret Service unlocks.
This also reads entries from the previous kernel-keyring backend. A successful
persistent login clears the volatile override; cleanup failures are reported.
After reboot, an older persistent key can become active again if you have not
logged in persistently with the new key. See [Credentials](docs/tachyon/CREDENTIALS.md)
for precedence, failure handling, and logout details. macOS Keychain and Windows
Credential Manager continue to use their native backends.

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

Run Cargo from the repository root. The interaction hosts live in
`crates/interaction/foreground` and `crates/interaction/background`; their package
and executable names remain `tachyon-foreground` and `tachyon-background`.
Tachyond resolves them beside its own executable unless `TACHYON_FOREGROUND_BIN`
or `TACHYON_BACKGROUND_BIN` overrides the corresponding path. The source move
does not change `target/release` output names or require configuration migration.

```bash
cargo build --release
./target/release/tachyon daemon start
./target/release/tachyon
```

The TUI should show the daemon as online. Press `Ctrl+P` to view the current
keyboard commands.

### Short Development Commands

For an interactive shell, a development alias can point at the checkout's actual
binary. For example, add a checkout-specific alias to your shell configuration:

```bash
alias tachyon='/absolute/path/to/tachyon/target/debug/tachyon'
```

After reloading that shell configuration, run from the repository root:

```bash
cargo b
tachyon daemon restart
tachyon
```

This virtual workspace builds its runtime binaries with `cargo b`; avoid limiting
the build to `--bin tachyon` when companion code changed. The alias does not build
or install anything, preserves your current directory, and follows rebuilt debug
binaries automatically. `cargo clean` removes its target until the next build.
Use an absolute executable path in noninteractive scripts, where shell aliases
are not normally loaded. Moving the checkout requires updating the alias.

### Development Build Size And Cleanup

The workspace's development and test profiles keep limited application debug
information, omit dependency debug information, and disable incremental caches.
This reduces disk use; Cargo can still retain old artifacts from earlier profiles,
features, and compiler versions. Release settings are unchanged.

After stopping builds and any runtime using these binaries, remove debug artifacts:

```bash
cargo clean --profile dev --target-dir "$PWD/target"
cargo build --workspace --bins
```

Run these commands from the repository root, without `sudo`. Cleaning can remove
companion executables before encountering a permission error. `cargo run --bin
tachyon` rebuilds only the CLI, not `tachyond`, Ghost, or the interaction hosts;
rebuild the workspace binaries before restarting.

If cleanup reports permission denied, inspect the reported path with `namei -l`.
Root-owned compiler files require an ownership repair restricted to those generated
artifacts. Do not change ownership of the entire filesystem, configuration, or
runtime databases. Profile changes do not shrink already-built files, and build
cleanup does not remove Tachyon's separately stored history or research data.
