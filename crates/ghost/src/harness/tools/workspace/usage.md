### Workspace
Inspect with `read` and `ls`/`find`/`grep`; change files with `write`/`edit`.

Paths are workspace-relative and subject to runtime policy. Use `read` line
offsets/limits and returned continuations rather than requesting entire large
files. `ls` lists directories, `find` matches paths, and `grep` searches text.
Keep searches scoped; inspect truncation and continuation metadata. `write`
replaces file content; `edit` uses exact text and rejects ambiguous matches.
Read the current content before editing. Native calls do not share Python cwd.
