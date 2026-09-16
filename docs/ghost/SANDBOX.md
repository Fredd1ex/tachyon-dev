# Agent Access And Sandbox Policy

This document defines the proposed access boundary for Ghost workers. It is a
design contract only; the profiles are not yet enforced by a secure runtime.

The current API exposes a deliberately binary `sandboxed` flag for the TUI. A
green marker means an enforced sandbox backend is active; an empty cell means
the worker is not sandboxed. Detailed profiles are intentionally not shown in
the compact view until they are enforced consistently.

## Naming

Use **access profile** for the user-facing category. `sandbox` describes the
mechanism, while `access` describes the capability decision. The API should
eventually expose both:

```text
access_profile = workspace
sandbox_backend = microvm
```

This prevents a workspace starting directory from being mistaken for a security
sandbox.

## Profiles

| Profile | Workspace | Network | Host access | Intended use |
| --- | --- | --- | --- | --- |
| `workspace` | read/write private workspace | unavailable by default | minimal | simple lookups and ordinary disposable tasks |
| `network` | read/write private workspace | explicitly enabled | minimal | weather, web, package, and API tasks |
| `trusted` | configured workspace | enabled | broader, explicit opt-in | user-approved development or administration |
| `durable` | persistent workspace/checkpoints | follows the selected network profile | follows the selected host policy | long-running or recoverable work |

`durable` is intentionally not a security profile. It controls retention and
checkpoint lifetime. It can be combined with `workspace` or `network`:

```text
access_profile = network
retention = persistent
```

## Current Boundary

Ghost currently provides local execution guardrails, not a sandbox:

- The process starts in the agent workspace.
- `HOME` is redirected into that workspace.
- The environment is scrubbed.
- Worker workspaces are selected per agent (standalone callers can select a cwd).

Registry policy checks enabled operation names and declared capabilities at
advertisement and execution. Package activation only selects instructions; it
does not grant permissions or start an isolation backend. Partial packages can
expose permitted direct schemas without exposing the package manual.

Native file-tool path validation does not restrict arbitrary host access by
authorized `exec` or IPython code. There is no separate network/egress capability
or runtime approval broker in the current `ToolPolicy`. `TACHYON_JAILED` is an
environment marker, not OS enforcement. The profiles above are proposed access
profiles, distinct from `harness/profiles.rs` compiled-in package selection.
See [HARNESS.md](HARNESS.md) for implemented dispatch and activation behavior.

Python's workspace-only `require("workspace")` bridge revalidates authorization
and dispatches native tools under the current policy, deadline, and cancellation.
This restricts hostcalls, not Python's own filesystem/network access. Its private
framed socket and bounded output are protocol guardrails, not isolation. Work-end
cleanup kills scoped kernels; see [PYTHON.md](PYTHON.md) for shutdown/escape limits.
The browser uses allowlisted CLI operations and fixed Lightpanda settings, with
eager binary setup at startup. It is not a network/SSRF sandbox or a full web
search engine; see [BROWSER.md](BROWSER.md).

This does not prevent every host access path. It must not be advertised as a
security boundary until filesystem, process, device, and network access are
enforced by the operating system.

## Firecracker Evaluation

Firecracker's current specification reports approximately 6–60 ms to API socket
availability, typically around 12 ms, and up to 125 ms from `InstanceStart` to a
minimal guest `/sbin/init` under its benchmark conditions. It also reports at
most 5 MiB VMM overhead for a 1 CPU, 128 MiB guest, excluding workload-specific
memory.

These numbers make Firecracker a candidate for long and persistent workers, not
a selected or implemented backend. Native execution may remain an explicit,
non-isolated development option. Proposed untrusted-research and isolated-ML
profiles require a microVM; a requested or required VM must fail closed, with no
silent native fallback. No automatic runtime selection is implemented here.
See [SECURITY.md](SECURITY.md) for the deferred host/guest boundary, resource and
recovery contract, GPU caveats, and independent security audit release gate.

## Required Future Enforcement

Before calling a profile secure, Tachyon must define and test:

- Filesystem mounts and read-only host paths.
- PID, process, device, and capability isolation.
- Network namespace and egress policy.
- Secret and credential exposure.
- Resource limits and cleanup on release.
- A visible profile/backend badge in the agent panel.
- Fail-closed admission when a requested/required VM or its enforcement is unavailable.
