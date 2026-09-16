# Worker Workspaces

## Selection Today

Workspace selection is host input, not an interpretation of the request's topic
or a directory mentioned by the model.

| Entry point | Selection |
| --- | --- |
| Ordinary TUI message | The TUI process's current directory, captured when submitting each turn |
| TUI `/managed <request>` | New isolated managed workspace for each delegated worker |
| `tachyon start --cwd . "inspect this project"` | CLI resolves the selected directory before sending it |
| `tachyon start "research this topic"` without `--cwd` | Managed |
| `ForegroundChat` with `cwd` | Existing absolute host-selected directory |
| `ForegroundChat` without `cwd`, or `cwd: null` | Managed |
| `AgentStart` / `BackgroundDelegate` without cwd | Managed for new workers |

`/managed` applies only to that message. The next ordinary TUI message selects
the TUI cwd again. There is no research classifier, sticky model-controlled
workspace, or global process `chdir`. A request mentioning "research" or an
absolute path does not change the selection. Launch the TUI from the project
directory to select it; there is no new TUI directory-picker command.

The Rust client retains `foreground_chat(text)` for no-cwd callers and adds
`foreground_chat_with_cwd(text, Some(absolute_path))` for explicit selection.
For example, the typed wire request is:

```json
{"cmd":"foreground_chat","text":"inspect this project","cwd":"/home/alice/project"}
```

Old wire names, requests without the new optional field, and persisted
interaction metadata still decode using serde defaults. No-cwd means no
authority to use a caller's project, not the daemon's cwd. **New no-cwd workers
now use the managed root instead of the old data-directory workspace default.**
The legacy empty cwd spelling on worker-start APIs also selects managed work;
an explicitly empty `ForegroundChat.cwd` is invalid.

## Managed Layout

Set the optional top-level key in Tachyon's `config.toml` (normally
`~/.config/tachyon/config.toml`):

```toml
managed_agent_root = "/home/alice/Agents"
```

The default is the host user's home directory joined with `Agents`. Use an
absolute path, not a literal `~`. Tachyond automatically creates this root
(including missing parent directories) at startup. Existing contents are left
untouched. Loading config alone creates nothing. Worker directories are still
created only when a new managed worker is allocated:

```text
~/Agents/<worker-id>/
  research/
  artifacts/
```

These two empty subdirectories are provisioned **today**. They are convenient
workspace-relative destinations, not Research/Campaign database records or an
execution protocol. Automatic per-research IDs, campaign subtrees, metadata
linking, and campaign execution remain proposals, not implemented behavior.
Campaign Draft records remain inert.

Workers can write relative paths such as `research/notes.md` and
`artifacts/report.md`. Provisioning does not force all output into those
directories or automatically register files as artifacts. Existing artifact
registration and tool output retention rules still apply; files are mutable,
and neither registration nor this layout promises immutable or durable storage.

Managed allocation refuses an existing worker directory rather than adopting,
moving, or deleting its contents. Provisioning failures are reported separately
from invalid selection and worker delivery/execution failures. A failed partial
provision may leave directories for inspection; it is not silently cleaned up.
If root provisioning fails at startup, the daemon logs a warning and continues
so ordinary chat and selected-project work remain available. Managed-worker
allocation retries provisioning and returns an explicit error if it still fails;
it never silently substitutes another root.

## Isolation And Reuse

The daemon validates selected directories, resolves symlinks to a canonical
identity, requires an existing directory, and rejects filesystem root. It does
not create a missing selected project or silently fall back to managed work.
Native filesystem tools remain constrained by their existing workspace-relative
path policy. Selecting a workspace does not enable arbitrary absolute-path
access or turn execution into an OS-enforced sandbox.

The selected cwd travels in typed per-turn interaction metadata through queued
and concurrent turns. Both single delegation and every fanout request use that
same selection. The model's delegation schema no longer offers `cwd`; stale or
invented model cwd arguments are ignored. Concurrent workers in the same
selected project still share its filesystem; this is not a worktree or file-lock
isolation feature.

A retained worker is eligible for a new assignment only when its canonical
workspace matches the selected cwd and the existing readiness, lifetime, and
task-type checks pass. A no-cwd request has no existing workspace identity, so
it allocates a fresh managed worker rather than reusing an unrelated project or
another managed assignment. Retrying the same logical work ID retains existing
idempotency behavior. Explicitly selecting an existing managed worker directory
can permit same-directory reuse.

Existing workers retain their recorded paths on recovery. No old workspaces or
artifacts are migrated or removed by this change. Existing lifecycle cleanup
for the legacy data-directory workspace tree is unchanged; avoid placing a
configured managed root under that cleanup tree. The default `~/Agents` tree is
outside it, but that is not a backup or durability guarantee.

Foreground conversation checkpoints, direct Ghost invocation, and the legacy
stdout-based delegation compatibility path retain their existing layout. This
contract covers the typed foreground/delegation APIs; it does not extend cwd
selection into scheduled-task records or campaign execution.
