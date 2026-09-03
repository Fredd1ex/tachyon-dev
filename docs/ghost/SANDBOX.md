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
sandbox_backend = soft
```

This prevents a workspace-only process jail from being mistaken for a security
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

Ghost currently provides a soft boundary:

- The process starts in the agent workspace.
- `HOME` is redirected into that workspace.
- The environment is scrubbed.
- Workspaces are private per agent.

This does not prevent every host access path. It must not be advertised as a
security boundary until filesystem, process, device, and network access are
enforced by the operating system.

## Firecracker Evaluation

Firecracker's current specification reports approximately 6–60 ms to API socket
availability, typically around 12 ms, and up to 125 ms from `InstanceStart` to a
minimal guest `/sbin/init` under its benchmark conditions. It also reports at
most 5 MiB VMM overhead for a 1 CPU, 128 MiB guest, excluding workload-specific
memory.

These numbers make Firecracker plausible for long and persistent workers. A
microVM for every short weather lookup would add latency and operational
complexity, so the first implementation should keep `workspace` as the default
soft profile and make Firecracker an explicit backend or a configurable default
for trusted-risk environments.

## Required Future Enforcement

Before calling a profile secure, Tachyon must define and test:

- Filesystem mounts and read-only host paths.
- PID, process, device, and capability isolation.
- Network namespace and egress policy.
- Secret and credential exposure.
- Resource limits and cleanup on release.
- A visible profile/backend badge in the agent panel.
- Fallback behavior when Firecracker is unavailable.
