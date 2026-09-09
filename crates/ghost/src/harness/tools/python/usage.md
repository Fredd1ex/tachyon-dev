### Python
Call `ipython` with `code` for persistent Python analysis or a `!` shell command.
The existing worker-local session starts on first execution, not registration.

Python variables persist within the session; serializable values are
checkpointed on a best-effort basis. Use `%cd relative/path` to change the
IPython session's working directory. `!cd` runs in a child shell and does not
change the session directory; combine `!cd path && command` for one shell call.
Stay in the assigned workspace. Native tool cwd is independent of Python cwd.
Check output for Python errors as well as timeout/exit metadata. A missing
IPython executable is a runtime failure, not an instruction to install it.
No Ghost `require()` bridge or dynamic package loading is provided.
