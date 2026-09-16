# Core Work Controls

`work` is core in every private broker Ghost session. It is not an orchestrator
role, a Conversation, or an optional coordination grant. Ordinary local chat
does not gain host authority. Native `work` actions are callable immediately;
the core interface is eagerly described. Python receives a preloaded `work`
variable from Rust-owned descriptors. `require('work')` is also supported but
is not necessary. All methods are awaitable; Python implements only the generic
proxy, not scheduling, budget, or completion policy.

Root-only campaigns also enroll their root and protected verifier, with finite
limits of two lifetime Works, one running lease and one resident for command
review. Human review retains its upfront protected verifier registration with
two bounded leases; both are released before waiting for acceptance. No `children`
configuration is needed for status, ask or complete. This enrollment grants no
`agents` interface, child profiles, Spawn permission, credits or provider rights.

## Status

`work(action='status')`, or `await work.status()`, reports this exact Work's
host objective, phase, latest instruction revision, remaining token and cost
allocation, and pending questions. There is no worker-supplied campaign or Work
selector. Remaining allocation is not permission to exceed the campaign wall
deadline or acquire additional inference capacity.

## Ask And Attention

```python
retained = 40
reply = await work.ask(
    request_id='choose-offset', question='Which offset should I use?',
    timeout_ms=60000)
import json
answer = json.loads(reply['content'])
if answer['answer'] is not None:
    retained += int(answer['answer'])
```

The host atomically persists an Attention request and parks the exact running
Work. It releases execution capacity, fences new inference, and retains the
resident process and Python variables. Outstanding or unknown inference holds
prevent parking; parking never refunds them. Other eligible Work can progress
even with root execution capacity one, subject to resident capacity.

Request IDs are scoped to the exact Work and generation. Identical replay
returns the previous answer or timeout; changed question, timeout, generation,
or revision conflicts. There are at most 32 requests per Work. Questions and
answers are bounded to 4096 bytes. Waits are 1..300000 milliseconds and always
consume the existing campaign wall deadline. No default unbudgeted input wait
exists. The tool/Python policy deadline may interrupt a longer requested wait
earlier; interruption closes the private channel and triggers host cleanup,
not a successful resume. Timeout returns `answer: null` only after execution capacity is
reacquired. Reacquisition has at most five additional seconds, still within the
host deadline. Failure, revocation, cancellation, or stale steering cancels the
parked parent rather than allowing it to run without a lease. Restart does not
recreate the Python process or replay execution.

Same-user operator endpoints are explicit `CampaignAttentionList` and
`CampaignAttentionAnswer` API requests. The CLI uses those same typed endpoints:

```sh
tachyon campaign attention list CAMPAIGN_ID
tachyon campaign attention answer CAMPAIGN_ID WORK_ID REQUEST_ID \
  --generation 1 --instruction-revision 1 '2'
```

List is bounded (default 32); `--after` accepts its next cursor. Answer requires
the exact generation and instruction revision printed by list. Identical answer
replay is idempotent; different answers conflict. Unknown, expired, cancelled,
or superseded pending questions cannot be answered. These endpoints are
privileged same-user host controls, not worker cross-campaign read authority.
The daemon socket has mode `0600`; Linux also checks the peer UID before reading
any IPC request. Answers remain typed tool-result data, not system instructions
or permission grants. Do not submit credentials: questions and answers are
persisted and answers can enter model context and retained tool evidence.
Native same-user execution remains **not a sandbox**: a trusted worker process
can access local host sockets outside its private tool interface.

The existing interaction notification path receives at most one compact,
escaped notification per new request, with no raw worker logs and no model
inference. The bounded best-effort notification queue cannot block execution.
With no connected Conversation, questions remain durably accessible through the
CLI; they do not block unrelated conversations or work.

## Completion Proposals

```python
await work.complete(
    summary='Candidate result is ready',
    candidate_refs=[],
    unresolved_questions=['Host review is still required'])
```

`complete` proposes a summary, candidate references, and unresolved questions.
There is deliberately no `verified` flag. The host requires the latest
recognized model instruction revision. A shared broker-session proposal signal,
read through the model trait, ends the generic loop after the tool batch (or
Python cell) finishes. The loop does not match special tool names. Do not place
additional work after `complete` in a Python cell.
The first accepted proposal is immutable for that broker session. Exact repeats
return it without another host request; conflicting completion arguments are denied.

The resulting ordinary WorkCandidate still passes the existing host collector,
revision checks, artifact reconstruction, and evaluator gate. Worker-proposed
references are not artifact authority: the collector rebuilds candidate refs
from registered evidence. The full unverified proposal, including reference
claims and unresolved questions, is retained in candidate context.
Explicit completion can be rejected or remain unverified by the host.

For compatibility, a normal nonempty tool-free model response still completes
ordinary chat and broker Work exactly as before. That completed text remains a
**candidate proposal baseline**, not verified success. Explicit `complete` adds
a control path; it does not redefine natural completion or verification.
