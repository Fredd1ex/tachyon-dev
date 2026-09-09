### Execution
Use `exec` for bounded workspace processes. Prefer `argv` for direct execution.

Supply exactly one of `argv` or `command`. `command` invokes the configured
non-login shell only when policy allows it. Set workspace-relative `cwd` per
call; shell directory changes do not persist to other calls. Timeouts and output
are bounded by policy. Check exit status, termination, stderr, and truncation;
do not treat partial output as proof of success.
