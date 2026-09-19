#![forbid(unsafe_code)]

#[path = "fixtures/prompts.rs"]
mod frozen;

use tachyon_orchestrator::registry::{
    self, ConversationIdentity, HostLane, InvocationContext, InvocationKind, RoleId,
};
use tachyon_orchestrator::{background::prompt as coordinator, conversation};

// Deliberate evidence-contract addition; retain the original review policy fixture.
const REVIEW_EVIDENCE: &str = "\n\nThe evidence bundle records this assignment only. `observed_invocations` counts calls including errors and results excluded by the budget; `omitted` and truncated outputs mean incomplete visibility, not zero calls or success. Inspect result error status and source content. A call, opaque reference, timestamp, cached report, or worker claim alone does not establish source freshness. Historical evidence is valid for requested historical comparison, not as a substitute for requested fresh retrieval. Reasoning-only objectives need no tool calls when their supplied inputs suffice.\n\nAn absent or null invocation count means unknown legacy coverage, not zero observations. Counts include both Python wrapper calls and nested native calls linked by `parent_call_id`; they are not a count of independent sources or model-selected calls.";

#[test]
fn conversation_rendered_bytes_preserve_base_and_lookup_contract() {
    // Intentional lookup and preference-policy changes; keep the original fixture frozen.
    for (user_name, conversation_name) in [
        ("you", "Conversational Agent"),
        ("Freddie", "Jarvis"),
        ("{conversation_name}\nUser", "{user_name} Assistant"),
        ("", ""),
    ] {
        for persona in [
            None,
            Some("conversation persona"),
            Some(""),
            Some("  style\n{user_name}\n"),
        ] {
            let mut expected = format!(
                "You are {conversation_name}; the user is {user_name}. {}",
                frozen::CONVERSATION_BODY.replace(
                    "Return literal spoken text; use formatting only when requested.",
                    "Use spoken text by default; honor relevant recalled format preferences unless current instructions override them.",
                ).replace(
                    "Use `memory` only for relevant durable context or a stable fact, preference, constraint, goal, routine, relationship, correction, or deletion.",
                    "Use `memory` for relevant durable user context, including recommendations.",
                ).replace(
                    "Treat memory results as authoritative: claim changes only after applied or already-applied results and never mention mechanics.",
                    "Treat memory results as authoritative: confirm only applied changes, without mechanics.",
                ).replace(
                    "For a stored-profile question, always call `memory` with action `recall` and `include_history` false, even if conversation appears to answer it.",
                    "For stored-profile questions, always call `memory` with action `recall` unless already recalled this turn.",
                ).replace(
                    "Set `include_history` true only for contextual questions about past conversation or task activity.",
                    "Set `include_history` true only for past conversation or task activity.",
                ).replace(
                    "After a memory tool result, answer from that result instead of repeating the same action. ",
                    "",
                ).replace(
                    "Be warm, direct, concise, and answer the request first. Adapt detail and format to the request, reuse context, and avoid repetition. Use spoken text by default; honor relevant recalled format preferences unless current instructions override them.",
                    "Be concise: practical answers need a direct answer and relevant qualification. Retain material uncertainty or failure. Omit unrelated status and unsolicited offers. Creative requests need only requested content. Latest releases: supported name, date, source; no unrequested comparisons. Partial documents: scope limitation before detailed claims. Default to spoken text; honor recalled format preferences unless overridden.",
                ).replace(
                    "Never expose reasoning, tools, workers, internal reports, credentials, or implementation details.",
                    "Never expose reasoning, credentials, or internals.",
                ).replace(
                    "Ask a question only when clarification is required for a responsible answer.",
                    "Clarify only when necessary.",
                ).replacen(
                    "\n\n",
                    "\n\nFor preference-sensitive requests, recall relevant preferences and constraints with `include_history` false before answering or delegating. Apply relevant results; explicit current instructions take precedence. Never invent preferences from empty or failed recall. Reuse sufficient recall from this turn.\n\nRequests needing factual information you do not know, or current or external facts unsupported by sufficient accepted evidence, authorize ordinary allowed read-only lookup. Search before asking for a full name, author, title, or description that lookup could establish; a short query is enough to start. Ask only if material ambiguity remains or host approval is required. Never bypass host restrictions or treat policy-denied access as allowed. Resolve brief confirmations against the preceding conversation; preserve the original objective, scope, timeline, and freshness requirement.\n\n",
                    1,
                )
            );
            expected.push_str("\n\nFor factual lookup prefer available `websearch` with `query`; for known URLs use `webfetch` with `urls`. Prefer native lookup over delegation. Reserve browser work for interaction, rendered state, or retrieval these tools cannot meet. Delegate independent subtasks in parallel; honor an explicit requested agent count within host limits. If native lookup is unavailable, delegate allowed retrieval. Never ask permission just to search. Web reports are model-mediated, not source-verified; cite supported URLs and keep unknown freshness explicit.");
            if let Some(persona) = persona {
                expected.push_str(frozen::CONVERSATION_PERSONA);
                expected.push_str(persona);
            }
            let actual = conversation::prompt::system_prompt(conversation::prompt::PromptContext {
                user_name,
                conversation_name,
                persona,
            });
            assert_eq!(actual.as_bytes(), expected.as_bytes());
            let rendered = registry::builtin()
                .resolve(
                    RoleId::Conversation,
                    HostLane::Foreground,
                    InvocationKind::Primary,
                )
                .unwrap()
                .render(InvocationContext {
                    identity: Some(ConversationIdentity {
                        user_name,
                        conversation_name,
                    }),
                    persona,
                })
                .unwrap();
            assert_eq!(rendered.prompt.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn coordinator_rendered_bytes_preserve_base_and_evidence_contract() {
    for persona in [
        None,
        Some("background persona"),
        Some(""),
        Some("  style\n{persona}\n"),
    ] {
        for (kind, render, base, heading) in [
            (
                InvocationKind::Primary,
                coordinator::system_prompt as fn(coordinator::PromptContext<'_>) -> String,
                frozen::COORDINATOR,
                frozen::COORDINATOR_PERSONA,
            ),
            (
                InvocationKind::Review,
                coordinator::review_system_prompt,
                frozen::REVIEW,
                frozen::REVIEW_PERSONA,
            ),
        ] {
            let mut expected = base.to_string();
            if kind == InvocationKind::Review {
                expected.push_str(REVIEW_EVIDENCE);
            }
            if let Some(persona) = persona {
                expected.push_str(heading);
                expected.push_str(persona);
            }
            assert_eq!(
                render(coordinator::PromptContext { persona }).as_bytes(),
                expected.as_bytes()
            );
            let rendered = registry::builtin()
                .resolve(RoleId::Coordinator, HostLane::Background, kind)
                .unwrap()
                .render(InvocationContext {
                    identity: None,
                    persona,
                })
                .unwrap();
            assert_eq!(rendered.prompt.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn auxiliary_prompt_bytes_match_original() {
    assert_eq!(
        conversation::prompt::SYNTHESIS_PROMPT.as_bytes(),
        frozen::SYNTHESIS.as_bytes()
    );
    assert_eq!(
        conversation::policy::CLASSIFICATION_PROMPT.as_bytes(),
        frozen::CLASSIFICATION.as_bytes()
    );
    assert_eq!(
        conversation::policy::ANSWERABILITY_PROMPT.as_bytes(),
        format!("{}\n\nA missing name, author, or title is a searchable fact, not necessarily ambiguous scope: choose `NeedsNewWork` to look it up first.", frozen::ANSWERABILITY).as_bytes()
    );
}
