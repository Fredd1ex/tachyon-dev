### Python
Call `ipython` with `code` for persistent Python analysis or a `!` shell command.
The existing worker-local session starts on first execution, not registration.
Rust-backed `require('exec')` and `require('browser')` calls in broker work use the same host CPU-job
admission as native exec. Direct `!shell`, subprocess APIs and arbitrary Python OS
access bypass it; this is not hard resource enforcement or a sandbox.

Prefer native `read`/`grep` for straightforward inspection. Use Python for retained
intermediates, aggregation, transformation, or programmatic iteration, and honor
explicit user requests for Python. Combine related work when useful, not mandatory.
Sample unknown structure first. Bound reads and printed output; retain intermediate
data instead of repeatedly dumping it. Check result errors and continuation metadata,
count and report skipped inputs and errors, and never silently `except: continue`.
For example, when assembling fragments, check duplicate and missing identifiers
before claiming completeness. Summarize coverage and unresolved gaps with results.

Python variables persist within the live work, including across compaction;
there is no checkpoint or replay. Use `%cd relative/path` to change the
IPython session's working directory. `!cd` runs in a child shell and does not
change the session directory; combine `!cd path && command` for one shell call.
Stay in the assigned workspace. Native tool cwd is independent of Python cwd.
Check output for Python errors as well as timeout/exit metadata. A missing
IPython executable is a runtime failure, not an instruction to install it.
`require` is synchronous idempotent metadata activation, not permission granting.
Use native schemas and dictionary envelopes:

```python
workspace = require("workspace", asynchronous=True)
print((await workspace.grep(pattern="TODO", limit=20))["content"])
proc = require("exec")
ctx = require("ctx")
p = await proc.start(argv=["/bin/printf", "hello"], timeout_ms=1000)
state = await proc.wait(operation=p["metadata"]["operation"], wait_ms=1000)
print(state["metadata"])  # Check done; wait again if pending.
print((await ctx.read(reference=p["metadata"]["stdout"], limit=8192))["content"])
```

Exec methods: `start/status/output/wait/cancel/run`. Ctx: `read/list/search`.
These only supply native `action`; all arguments and returned dictionaries are
native. Inspect `.schemas` and `.guidance`. Use `operation`, typed `reference`,
`limit`, `timeout_ms`, and `wait_ms`, not invented aliases. `workspace.search`
is a grep alias (`pattern`); `ctx.search` uses `query` on one bounded output page.
For compatibility, `require("workspace")` without `asynchronous=True` is synchronous.
Async methods execute on await without blocking the event loop. Await hostcalls
sequentially: overlapping calls and background hostcalls are unsupported. One cell
executes per kernel. Recursive IPython hostcalls are denied. Native tools
remain directly callable. Cancellation during an await closes the kernel; effects
may already exist and are never replayed. Use proc.cancel then wait for normal
process termination. Processes inherit the original cell deadline; work end cleans
up kernels, jobs, and refs. No scheduler or durable process recovery exists.

`browser = require('browser'); await browser.run(args='snapshot')` uses native
`agent_browser` string args, not a Python browser implementation. Activation does
not start or install a browser. `artifact = require('artifact')` exposes
`await artifact.register(path='report.txt', kind='report', description='Results')`.
It hashes/registers through Rust and returns pending, not Ready publication.
Both methods are always async and recheck policy on every call.

With host history authority, await `ctx.list(scope='campaign', kinds=['document',
'artifact'], limit=8)` or `ctx.search(query='literal', scope='current_work', limit=8)`.
These return one bounded durable descriptor page, JSON in `content`; follow its
string `next_cursor` even on an empty page. Omit scope/kinds for shipped live
navigation. Durable scope is host-derived, never a caller-selected campaign ID.
