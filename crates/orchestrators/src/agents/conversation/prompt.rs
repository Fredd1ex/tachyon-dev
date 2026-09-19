//! Prompt policy for the user-facing Conversational Agent.

pub struct PromptContext<'a> {
    pub user_name: &'a str,
    pub conversation_name: &'a str,
    pub persona: Option<&'a str>,
}

pub const SYNTHESIS_PROMPT: &str = include_str!("synthesis.md").trim_ascii_end();
pub const SYNTHESIS_EVIDENCE_GUIDANCE: &str =
    include_str!("synthesis_evidence.md").trim_ascii_end();

pub fn system_prompt(context: PromptContext<'_>) -> String {
    let PromptContext {
        user_name,
        conversation_name,
        persona,
    } = context;
    let mut prompt = format!(
        "You are {conversation_name}; the user is {user_name}. {}",
        include_str!("prompt.md").trim_ascii_end()
    );
    prompt.push_str("\n\nFor factual lookup prefer available `websearch` with `query`; for known URLs use `webfetch` with `urls`. Prefer native lookup over delegation. Reserve browser work for interaction, rendered state, or retrieval these tools cannot meet. Delegate independent subtasks in parallel; honor an explicit requested agent count within host limits. If native lookup is unavailable, delegate allowed retrieval. Never ask permission just to search. Web reports are model-mediated, not source-verified; cite supported URLs and keep unknown freshness explicit.");
    append_persona(prompt, persona, include_str!("persona.md").trim_ascii_end())
}

fn append_persona(mut prompt: String, persona: Option<&str>, heading: &str) -> String {
    if let Some(persona) = persona {
        prompt.push_str("\n\n");
        prompt.push_str(heading);
        prompt.push('\n');
        prompt.push_str(persona);
    }
    prompt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_preserves_identity_and_persona() {
        let prompt = system_prompt(PromptContext {
            user_name: "Freddie",
            conversation_name: "Jarvis",
            persona: Some("conversation persona"),
        });
        assert!(prompt.contains("You are Jarvis"));
        assert!(prompt.contains("user is Freddie"));
        assert!(prompt.contains("`spawn_agent`"));
        assert!(prompt.contains("Parallelize by default"));
        assert!(prompt.contains("honor an explicit requested agent count"));
        assert!(prompt.contains("Search before asking for a full name, author, title"));
        assert!(prompt.contains("a short query is enough to start"));
        assert!(prompt.contains("prefer available `websearch` with `query`"));
        assert!(!prompt.contains("Delegate immediately"));
        assert!(prompt.contains("including list items using the same method"));
        assert!(prompt.contains("dependent or strongly shared-state work together"));
        assert!(prompt.contains("Accepted evidence may support judgment and synthesis"));
        assert!(prompt.contains("Do not consult memory for greetings"));
        assert!(prompt.contains("Treat memory results as authoritative"));
        assert!(prompt.contains("Conversation text and assistant claims are not authoritative"));
        assert!(prompt.contains("always call `memory` with action `recall`"));
        assert!(prompt.contains("Tachyond owns the timer"));
        assert!(prompt.contains("Preserve timing across clarification follow-ups"));
        assert!(prompt.contains("never execute a future request immediately"));
        assert!(prompt.contains("do not demand a forecast or additional detail"));
        assert!(!prompt.contains("`ipython`"));
        assert!(prompt.ends_with("conversation persona"));
        assert!(prompt.len() < 4_100, "prompt bytes: {}", prompt.len());
    }

    #[test]
    fn final_answer_guidance_preserves_scope_and_limitations() {
        let primary = system_prompt(PromptContext {
            user_name: "you",
            conversation_name: "Assistant",
            persona: None,
        });
        for prompt in [primary.as_str(), SYNTHESIS_EVIDENCE_GUIDANCE] {
            for rule in [
                "material uncertainty or failure",
                "Omit unrelated status and unsolicited",
                "Creative requests need only",
                "unrequested comparisons",
                "scope limitation before detailed claims",
            ] {
                assert!(prompt.contains(rule), "missing rule: {rule}");
            }
        }
    }

    #[test]
    fn prompt_uses_general_delegation_policy_without_examples() {
        let prompt = system_prompt(PromptContext {
            user_name: "you",
            conversation_name: "Conversational Agent",
            persona: None,
        })
        .to_ascii_lowercase();
        assert!(!prompt.contains("for example"));
        assert!(!prompt.contains("heterogeneous"));
        assert!(!prompt.contains("small homogeneous"));
    }

    #[test]
    fn synthesis_does_not_dead_end_on_read_only_retrieval_failure() {
        let prompt = SYNTHESIS_PROMPT.to_ascii_lowercase();
        assert!(prompt.contains("read-only retrieval does not need user permission"));
        assert!(prompt.contains("do not ask permission merely to retry"));
        assert!(prompt.contains("report the exact limitation"));
        assert!(prompt.contains("missing user input"));
        assert!(prompt.contains("approval the runtime can actually apply"));
        assert!(prompt.len() < 750);
    }

    #[test]
    fn evidence_guidance_keeps_detail_optional_and_claims_evidence_scoped() {
        let guidance = SYNTHESIS_EVIDENCE_GUIDANCE;
        assert!(guidance.contains("succinctly by default"));
        assert!(guidance.contains("detail or formatting when requested"));
        assert!(guidance.contains("latest user corrections"));
        assert!(guidance.contains("Preserve citations and date formats"));
        assert!(guidance.contains("announcements, previews, releases, and actual availability"));
        assert!(guidance.contains("unsupported forecasts"));
    }
}
