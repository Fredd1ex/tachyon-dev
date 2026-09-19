## Current-Fact Lookup

The primary Conversation prompt treats a request needing current or external
facts as authorization for ordinary allowed read-only lookup when accepted
evidence is insufficient. It prefers native `websearch` for straightforward current
facts and `webfetch` for known URLs, reserving browser delegation for interaction, rather than a
training-cutoff answer or an offer to look up. Materially ambiguous objectives
and required host approvals still warrant questions; denied access remains
denied. Brief confirmations retain the preceding objective, scope, and freshness
requirement. Stable knowledge and sufficient accepted evidence can still be
answered directly.

This is model-facing guidance, not a deterministic guarantee of model compliance.
The existing structured answerability policy still governs eligible follow-ups;
there is no additional classifier request, text-based routing heuristic, or
permission-question output filter. Local fake-provider tests verify the outbound
prompt and native tool loop, not a live model's judgment or real-world facts.

Native web tools admit no agents. Host `[web]` policy controls availability,
provider/model, and per-root budget; disabled tools are neither advertised nor
dispatched. Fetch forwards HTML and PDF URLs unchanged. Reports are bounded JSON
(8 KiB), prioritizing citations over answer prose, with explicit unknown freshness
and omission counts. Receipt timestamps are not publication dates. The daemon
retains original typed results; native reports alone do not certify source claims.
Provider token usage is currently absent from the shared `WebResult` reply, so
foreground token totals cannot include it; observed server tool-use counts remain
in the report. No synthetic token counts or currency estimates are added.

## Acknowledgment Timing

Input admission immediately emits `UserTurnAccepted` and an `input_accepted`
timing event. Admission is not a claim that a provider or source has responded.
Each admitted turn keeps its own Tokio task, including concurrent routing and
execution. A one-shot timer in that task publishes the pending status
`Working on that.` if no answer or acknowledgment has appeared. The host process
environment variable `TACHYON_ACK_DELAY_MS` controls the debounce: default 500 ms,
range 0-60000 ms (larger values are capped; invalid values use the default).

Decoded tool calls can publish `I'm looking into it.` before tool execution if
the delayed receipt has not already appeared. Only an exact, generic safe phrase
may be reused from model prose. Neither receipt asserts that retrieval succeeded,
and arbitrary tool-capable planning stays buffered until a no-tool completion is
proven. The tool protocol is unchanged; there is no extra acknowledgment model
call. A fast answer suppresses the timer; completion, task cancellation, and
dropping the turn future cannot leave a detached timer publishing later.

### Contextual Provisional Status

After routing selects a dependent follow-up and the host confirms that its
preceding request is still pending, it publishes the provisional status:
`I'm waiting for those results. I'll answer when I have enough information.`
The terminal ledger and active predecessor entry are checked while publication
is serialized with completion/cancellation. No parent request is quoted or topic
inferred. Existing intake metadata has a decision and untrusted free-form
acknowledgment; answerability has only an outcome. Neither supplies a validated
short topic label, so this implementation deliberately uses the generic fallback.
This is host-generated text, not model-authored claims about sources, workers,
results, or newly started work. The existing routing
JSON acknowledgement remains non-authoritative and is not displayed.

The existing outbound contract is unchanged: an `EventEnvelope` containing
`AgentEvent::Status { turn: Some(turn), phase: "working", message: text }`, with
the same envelope `turn_id` and foreground session/conversation correlation as
other turn events. A nonempty message replaces that turn's pending status; it is
not an answer delta or durable assistant message. A contextual status may replace
the generic 500 ms receipt once. Later generic/empty statuses cannot erase it.
`InteractionEvent::ConversationDelta { text }` starts the actual answer and
`InteractionEvent::ConversationFinished { text }` supplies the final answer under
the original interaction metadata. No new event variant or protocol version is
required. The additional diagnostic timing stage is
`contextual_acknowledgement_published`. Foreground status publication is suppressed
after the first answer or completion; consumers should also reject stale events
for an answered turn. No TUI changes are part of this implementation.

Each input retains its own task. Unrelated `AnswerNow` inputs do not wait for this
dependency. Correlated accepted evidence still wakes a dependent answer before
the parent or all siblings finish when answerability finds it sufficient. If the
first partial result is insufficient, changed accepted evidence triggers another
assessment without waiting for parent completion; progress-only notifications do
not. Parent termination remains a wakeup even without successful evidence. There
is no extra acknowledgement model call, tool dispatch, or authority derived from
the request text. Dependency selection remains the existing preceding-turn
policy, not a new arbitrary multi-turn dependency graph.

The following lifecycle timings use elapsed time from that turn's admission
(including time spent routing and assessing answerability):

- `input_accepted`: the input was admitted.
- `acknowledgement_published`: the pending receipt/status was emitted, if needed.
- `provider_first_output`: the first semantic text or tool-call delta parsed from
  a conversation response request, including tool-only responses. Role-only,
  empty, whitespace, usage, and reasoning frames do not qualify. Internal routing
  and answerability calls are excluded.
- `first_answer`: the first nonblank answer delta published to the interaction
  channel, not buffered planning or acknowledgment text.
- `completed`: the final answer event was published, including error outcomes.

The separate `answerability outcome=...` diagnostic measures that assessment's
duration, not an admission-relative lifecycle point.

The TUI labels these separately as `accepted`, `ack`, `first output`,
`first answer`, and `completed`, ordered by measured elapsed time. Missing stages
are omitted rather than represented as zero. A response that only publishes a
final answer has no invented first-answer delta. Model-request completion is not
treated as turn completion. Timing does not alter answer text, usage tokens,
clipboard contents, or durable conversation history.

This is a host-side receipt policy, not native 200 ms provider response latency.
The debounce is subject to runtime scheduling and transport/UI delivery; elapsed
publication timing measures foreground emission, not confirmed screen display.

## Evidence Synthesis Streaming

Tool-free synthesis publishes protocol-filtered text as it arrives; it does not
wait for the full answer. Only possible protocol-marker prefixes are held across
frames. Tool-capable planning remains buffered until routing is known. Answers
are concise by default, with detail and formatting when requested.

On transport failure, incomplete SSE, unexpected tool calls, protocol markup, or
output truncation, finalization preserves the exact published prefix and appends
one interruption/verification limitation. It does not replay or retract deltas.
Before any text has been published, failure uses the bounded evidence brief's
safe fallback. Neither path exposes raw worker reports.

The synthesis brief keeps the latest request, relevant user scope corrections,
and matched native delegation results. A follow-up attachment is selected by the
host and passed by message index; preceding user JSON is never inferred to be
canonical worker evidence. All evidence remains data, not instruction authority.
`TACHYON_POLICY_CONTEXT_CHARS` retains its legacy character-budget meaning for
policy context; for the synthesis brief its value is a serialized UTF-8 byte
budget, clamped to 1,000-32,000 (default 8,000). JSON escaping counts toward this
budget. Oversized outcomes are omitted whole, including their source references,
not cut into uncitable fragments. Budget checks stream into a byte counter rather
than allocating serialized raw reports. Raw delegation envelopes over 1 MiB are
omitted before parsing; this is separate from the response-stream limit below.

`TACHYON_SYNTHESIS_RESPONSE_BYTES` bounds the synthesis response in the provider
reader, including SSE framing and unexpected tool arguments: default 1 MiB,
clamped to 16 KiB-16 MiB; invalid values use the default. Reaching that bound is an
interruption, not a replacement answer. The existing provider token limit still
applies. There is no separate 4,000-byte answer cap. Synthesis evidence guidance
is included from the conversation role's `synthesis_evidence.md`; the frozen
base synthesis prompt is unchanged.

## Final Answer Naturalness

Primary and synthesis guidance favor a direct practical answer with a relevant
qualification, not unrelated scope status or unsolicited offers to do what was
already requested. Creative requests receive only the requested content. Latest
release answers favor a supported name, date, and source without unrequested
comparisons. Partial document coverage is disclosed before detailed claims.
Material uncertainty, retrieval failure, citations, and relevant recalled
preferences remain required; brevity does not authorize hiding limitations.

These are generic model-facing rules, not semantic enforcement. Runtime guards
suppress late provisional status after an answer starts or finishes; they do not
detect or delete incidental waiting language inside model answers. Completed
answers are not silently rewritten. Local fake-provider tests inspect outbound
primary/synthesis prompts, pending versus terminal dependency state, stale status
suppression, and exact answer preservation, including deliberately noncompliant
model prose. They do not prove live-model compliance or source accuracy.

No model call, tool call, schema field, prompt/context limit, or token limit was
added. The existing 4,100-byte primary prompt test remains; the frozen baseline
fixture is unchanged and intentional primary wording changes are explicit in
the byte regression test. Base synthesis, classification, and answerability
prompts are unchanged by this naturalness revision. Dependency selection is still
the preceding-turn policy, not a validated arbitrary dependency graph.
