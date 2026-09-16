### Workspace
Inspect with `read` and `ls`/`find`/`grep`; change files with `write`/`edit`.
Prefer native `read`/`grep` for straightforward inspection; Python is useful for
retained intermediates, aggregation, transformation, or programmatic iteration.
Honor explicit user requests for Python. Combine calls when useful, not by rule.

Paths are workspace-relative and subject to runtime policy. Use `read` line
offsets/limits and returned continuations rather than requesting entire large
files. `ls` lists directories, `find` matches paths, and `grep` searches text.
Keep searches scoped; inspect truncation and continuation metadata. `write`
replaces file content; `edit` uses exact text and rejects ambiguous matches.
Read the current content before editing. Native calls do not share Python cwd.
Pass the inspected `read` result's `metadata.version` as `expected_version` to
`edit` or `write`. Both accept optional `expected_sha256`, the lowercase SHA-256
of the entire original file. When both are supplied, both must match. Legacy
calls without these fields remain supported; an unconditional write can overwrite
another writer's work. Integrators must always supply a version and re-read and
recompute on `conflict`, never blindly retry the same patch.

`read` returns an opaque `version` from descriptor metadata checked before and
after reading (on Unix: device, inode, size, nanosecond mtime and ctime). It is a
local snapshot identity, not a content digest, path, permission, or durable ID.
Non-Unix builds use size, modification time, and creation time instead and have
weaker replacement detection; the device/inode/ctime guarantee is Unix-only.
`metadata.sha256` is non-null only when the entire file was collected within the
read bounds. Partial reads never hash the unread remainder or scan for a total
line count. Line offsets still require traversing preceding bytes. Stat tokens
depend on filesystem timestamp fidelity; they cannot prove byte equality after
undetectable in-place mutation. Use the full digest for content preconditions.
Digest validation is bounded by the existing write-size limit.

Native writers take a per-target cross-process exclusive advisory lock before
reading preconditions, holding it through exact patching, atomic rename, and any
requested durability sync. Lock contention waits asynchronously with cancellation
and deadline checks; unrelated files do not share a global writer mutex.
The Rust host helper `workspace::apply_exact_patch` requires a version and uses
the same exact, unique replacement transaction. Two conditional writers using
one version have at most one winner across cooperating Ghost processes.

Host-generated locks persist in `.tachyon-write-locks/<sha256-basename>` in the
canonical target parent, not in a workspace-root registry or on the replaceable
target inode. Thus cwd aliases and overlapping/nested authorized workspace roots
agree on the same lock. Final target symlinks are rejected; directory aliases
resolve to the canonical parent. The directory is reserved host state: never
delete or replace it while writers might be active. Empty lock files persist on
purpose; unlinking them would split the lock identity. Native writes into these
directories are rejected, as are native reads of this reserved host state.
Native listing and search exclude them even when hidden files are requested.
Creating locks requires a writable parent, including for previously existing
files; read-only source trees fail explicitly without broadening permissions.
Directories must be host-owned mode 0700 and locks single-link regular files of
mode 0600. Existing artifacts are never truncated or chmodded. Lock opens reject
symlinks and walk canonical parents using anchored directory descriptors.
This implementation requires Linux `/proc/self/fd`; other platforms or missing
procfs fail closed rather than silently falling back to process-local locking.
The proc descriptor symlink is followed intentionally; each directory descriptor
remains owned through the child open, whose final component rejects symlinks.
Ancestor traversal requires search permission, not directory listing permission.
Requested durability sync separately needs read access to the target parent.
Safe `nix` rename/unlink/chmod APIs alone do not replace the owned directory opens
provided here by procfs; no first-party unsafe descriptor conversion is used.

Temporary-file creation/guard installation and the final rename are synchronous
so cancellation cannot leave a queued create after cleanup or a queued commit
after lock release. In-flight async writes can finish only on the temporary inode,
which cleanup unlinks on cancellation. These synchronous filesystem calls (also
lock opens and cleanup) can block an executor thread; bounded content and async
lock polling do not impose a wall-clock bound on filesystem I/O. Cancellation
during post-rename sync may release the lock after the replacement has committed.

This is cooperation among same-host native writers, not a merge algorithm,
multi-file transaction, or OS compare-and-swap. Shell writers, older Ghost builds,
lock-directory replacement by the same UID, hostile ancestor renames, and filesystems
without reliable flock semantics remain outside the guarantee. Descriptor checks and
pre-rename path/metadata revalidation detect ordinary changes and symlink swaps,
but are not a filesystem compare-and-swap against uncooperative same-UID writers.
On Linux, replacement staging, rename, sync, and temporary cleanup use a pinned
parent descriptor and recheck its device/inode association with the configured
path. Moving that directory may leave a write in the detached original directory,
not in a replacement symlink target. This does not sandbox arbitrary filesystem
access or make optional parent-directory creation race-free. Failed
preconditions and missing/ambiguous matches leave the file unchanged and clean up
staged temporary files. A durability-sync failure after rename can still report
an error after the file was replaced.
When structure is unknown, sample a bounded slice before choosing extraction.
For coverage claims, follow relevant continuations and account for skipped files,
errors, and truncation; a sample or empty partial search is not exhaustive evidence.
