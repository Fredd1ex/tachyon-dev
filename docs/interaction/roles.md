# Interaction Roles

Interaction is the user-facing surface. Foreground and background are runtime
host lanes, not additional inference roles. Policy lives in
`crates/orchestrators/src/agents/`; hosts own model invocation, execution loops,
tool adapters, and permission enforcement.

`crates/orchestrators/src/registry.rs` contains the single available-role
registration list, populated by each role module's `definition()`. Definitions
bind typed invocations to pure prompt and ordered schema callbacks. Resolution
checks the role, enabled state, host lane, and invocation before rendering.
Registration grants no permission and starts no execution.

| Role | Policy directory | Host lane | Output |
| --- | --- | --- | --- |
| Conversation | `agents/conversation` | Foreground | User-facing answers |
| Coordinator | `agents/coordinator` | Background | Internal findings and reviews |
| Campaign | `agents/campaign` | Background | Internal advisory assessments |

These are the three registered roles. Role IDs (`conversation`, `coordinator`,
`campaign`) are not daemon actor IDs. Existing wire
identities, including `foreground` and `background`, are unchanged. The public
`tachyon_orchestrator::conversation` reexport and
`tachyon_orchestrator::background` alias preserve shipped foreground, background,
and Ghost imports; neither defines another role or implementation.

Memory is currently a storage service, not an inference role. Its existing
Conversation capability remains available without inventing a memory prompt.
Its storage is unchanged. Campaign is registered for explicit assessment, not
automatic campaign lifecycle execution.

Each role's `tools.rs` owns its ordered tool selection. Shared capability schemas
remain in `src/tools.rs`. Conversation `Primary` returns `spawn_agent`,
`spawn_agents`, `memory`, and `schedule` in that order. Coordinator `Primary`
returns its existing eight delegation/lifecycle schemas; Coordinator `Review`
returns only `submit_work_review` with the distinct review prompt. Review's
provider-neutral schema and `REVIEW_TOOL` constant live in
`agents/coordinator/tools.rs`; review is not a general lifecycle capability.
Registry capabilities describe the broad role, not the tools for every invocation.

Campaign `Primary` declares narrow Todo/Monitor read schemas. The background
assessment adapter requires explicit host grants but passes no executable tools
to the model. An explicit caller supplies a bounded campaign snapshot; the host
makes one call and validates a typed internal assessment. Registration neither
queries the services nor starts periodic inference. See
[Campaign Oversight](CAMPAIGN_OVERSIGHT.md) for the request and authority contract.

Hosts still enforce capability/name authorization and invocation mode. In review
mode, allow only `REVIEW_TOOL`, require exactly one decision call, and retain the
existing parsing and validation. Mapping a schema to a provider `ToolSpec` grants
no execution permission. Foreground context-specific filtering, delegation
requirements, and the unused `force_delegation` selection flag remain host policy;
the registry does not add `respond` to the primary call. Classification,
answerability, and synthesis still use existing policy adapters, not generic
registry dispatch.

Conversation and Coordinator model-facing prose lives in each role's `prompt.md` and named auxiliary
Markdown files. Rust assembles identity and optional persona text without
substituting inside user-provided values. Markdown's trailing file whitespace is
excluded from prompt output; supplied names and personas are not trimmed.
`tests/fixtures/prompts.rs` freezes the original literals independently of these
documents. Byte-equality tests were validated against the original renderers
before extraction and cover default/custom names, absent/empty/multiline
personas, literal placeholder-like values, and auxiliary prompts.
Campaign has its own assessment prompt; it is not inserted into normal chat.
Neither todo storage nor monitoring adds automatic plan injection or changes
Conversation's four default tools.

## Host Wiring

Conversation primary rendering (inside a fallible host function):

```rust
use tachyon_orchestrator::registry::{
    self, ConversationIdentity, HostLane, InvocationContext, InvocationKind, RoleId,
};

let rendered = registry::builtin()
    .resolve(RoleId::Conversation, HostLane::Foreground, InvocationKind::Primary)?
    .render(InvocationContext {
        identity: Some(ConversationIdentity {
            user_name: &user_name,
            conversation_name: &conversation_name,
        }),
        persona: configured.persona.as_deref(),
    })?;
let prompt = rendered.prompt;
let tools: Vec<tachyon_model::ToolSpec> = rendered.tools.into_iter()
    .map(|schema| tachyon_model::ToolSpec::new(
        schema.name, schema.description, schema.parameters,
    ))
    .collect();
```

Coordinator review rendering, with the same schema-to-`ToolSpec` mapping:

```rust
use tachyon_orchestrator::agents::coordinator::tools::REVIEW_TOOL;

let rendered = registry::builtin()
    .resolve(RoleId::Coordinator, HostLane::Background, InvocationKind::Review)?
    .render(InvocationContext { identity: None, persona })?;
let required_tool = (REVIEW_TOOL, "decision");
```

The review host is wired to these registry exports, preserving request
serialization, timeout, required-tool mode, response validation, and advisory-only
lifecycle handling. Foreground resolves Conversation/Foreground/Primary;
background resolves Coordinator/Background/Review or Campaign/Background/Primary
on their distinct explicit request paths. Each executable owns an independent
Tokio runtime; there is no new shared runtime crate. Neither registry visibility
metadata nor an assessment's attention field publishes output. Conversation
retains user-facing publication; no automatic campaign-to-Conversation notification
or new urgent path is implemented. The separate caller/lifecycle follow-up is
recorded in [Outstanding Integration](ARCHITECTURE.md#outstanding-integration).

`Registry::new(&definitions)` validates a borrowed slice, rejecting duplicate
stable role IDs, mismatched IDs, and duplicate invocation kinds within a role.
Disabled definitions cannot resolve; removed and unknown roles fail lookup.
Unsupported invocations and wrong lanes return errors without fallback.
`RoleId::Custom("stable-test-id")` permits additional definitions and callbacks
without editing registry dispatch. IDs are stable policy keys, never runtime
actor renames. Conversation requires typed identity; coordinator prompts use
only persona. Identity/persona values are passed verbatim to the existing prompt
renderers.

`cargo test -p tachyon-orchestrator` checks registry dispatch against frozen prompt
bytes and ordered schema snapshots, including the original background review
schema. Intentional schema changes can regenerate the JSON fixtures with
`UPDATE_ORCHESTRATOR_SNAPSHOTS=1 cargo test -p tachyon-orchestrator --test registry_dispatch`;
review the resulting fixture diff before accepting it. No model/runtime or Tokio
dependency is needed.
