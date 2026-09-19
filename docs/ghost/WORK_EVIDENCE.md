# Assignment Evidence

## Assignment Boundaries

A warm Ghost process reuses its workspace and runtime, not previous WorkRequest
dialogue. Each WorkRequest starts with current role guidance, assignment identity,
objective and explicit input handles. Checkpoint summaries and previous assistant
answers are not fresh observations. Explicit historical resources may support a
comparison, but reading an old report now does not make its contents current.
Reasoning tasks with sufficient supplied inputs do not require tool calls.

## Evidence Forwarding

The foreground builds distinct logical Work IDs for delegated children, preserving
the originating conversation turn. Tachyond supplies generation and assignment in
WorkRequest. Ghost creates a new event sink for each request; registry execution
uses the context's sink, and the candidate snapshots that same collector.

The daemon checks worker, Work ID, generation, assignment and objective before
forwarding the candidate unchanged inside WorkReviewRequest. Background serializes
that typed request into the reviewer input. Only the daemon's terminal WorkResult
returns through WorkSubscribe to foreground synthesis. Rejected candidates are
not successful evidence.

`observed_invocations` includes successful and failed calls, even when their output
is omitted. `tools` contains bounded native results; inspect `is_error`, metadata
and `truncated`. `omitted` counts excluded entries. Empty retained output with a
positive count is not "no calls" and is not proof of success. Older persisted
bundles may lack the counter; absence remains unknown, not zero. Inspect retained
entries and omissions as well. Ghost reports an explicit zero for measured no-call
assignments. Counts include Python wrapper calls and their nested native calls,
linked by `parent_call_id`, not just model-selected top-level calls.

`[work-evidence]` diagnostics mark Ghost candidate, daemon review input and
foreground terminal receipt using worker/Work/generation/assignment and counts,
without logging source content. Daemon and foreground also include origin turn;
its existing conversation correlation connects to TUI events. Daemon `turns_used`
identifies successive assignments. These diagnostics do not change TUI labels,
usage accounting or retry policy.

Daemon-synthesized timeouts retain up to 32 fenced tool telemetry observations
within a 16 KiB budget. These entries have `metadata.partial_observation: true`;
they are not source content or a reviewed answer. Invocation totals and execution,
inference and review timing remain unknown without the worker's measurements.
Generation zero and assignment zero are valid for a new worker. Evidence
diagnostics are retained in daemon logs, not forwarded as conversational errors.
With an already running daemon, `tachyon cat <full-worker-id> --result` reads the
retained typed terminal result without starting a worker or making a model call.

## Browser Evidence

Browser availability and browser implementation are separate from this contract.
A failed browser attempt is evidence of a failed attempt, not a retrieved source.
An allowed fallback can supply evidence, but its actual result must support the
claim and requested freshness. Unsupported arguments require interface correction,
not blind repetition. A later source lookup must not inherit earlier successful
fallback evidence merely because the worker is warm.

## Natural Answers

Worker and synthesis role guidance preserve source dates, uncertainty and scope
without requiring process narration in the user answer. Review stays strict:
requested fresh retrieval with no supporting observations warrants rework, while
historical comparison and reasoning are judged against their own requirements.

## Local Verification

`cargo test -p ghost --bin ghost --offline` exercises three serialized WorkRequests
through the actual warm chat loop with a scripted provider and fixture tool. The
first produces observed evidence; the second returns a cached claim without a
call and produces an empty bundle. A same-Work retry retains explicit historical
handles and feedback, but not cached dialogue or evidence. Plain-chat checkpoint
and conversation retention are tested separately. Separate local Background provider tests
check serialized reviewer input and preserve a scripted rework decision. These
tests verify plumbing, not a real model's judgment or live browser availability.
