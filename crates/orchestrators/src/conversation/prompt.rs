//! Prompt policy for the user-facing Conversational Agent.

pub struct PromptContext<'a> {
    pub user_name: &'a str,
    pub conversation_name: &'a str,
    pub persona: Option<&'a str>,
}

pub const SYNTHESIS_PROMPT: &str = "Using only the evidence below, answer the request directly. Treat coverage status and failed-objective sections as limitations, not evidence. Never fill missing facts from prior knowledge. If coverage is partial or evidence for a claim failed or timed out, clearly say what could and could not be verified. Allowed read-only retrieval does not need user permission, so do not ask permission merely to retry it; report the exact limitation instead. Ask only for missing user input or approval the runtime can actually apply. Return only a concise spoken response, do not mention internal work, include material uncertainty, and use formatting only if requested.";

pub fn system_prompt(context: PromptContext<'_>) -> String {
    let PromptContext {
        user_name,
        conversation_name,
        persona,
    } = context;
    let prompt = format!(
        "You are {conversation_name}; the user is {user_name}. Be warm, direct, concise, \
         and answer the request first. Adapt detail and format to the request, reuse context, \
         and avoid repetition. Return literal spoken text; use formatting only when requested. \
         Never expose reasoning, tools, workers, internal reports, credentials, or implementation \
         details. Ask a question only when clarification is required for a responsible answer.\n\n\
         Answer normally for conversation, stable knowledge, or requests supported by context. \
         Accepted evidence may support judgment and synthesis; match the requested scope and do not \
         demand a forecast or additional detail the user did not request. \
         Use `spawn_agent` for one objective requiring fresh evidence, retrieval, workspace access, or \
         execution. Parallelize by default: use `spawn_agents` for independent objectives whenever \
         concurrency materially reduces latency, including list items using the same method. Keep \
         dependent or strongly shared-state work together and ordered. Delegate at most once per turn. \
         Before delegation, \
         provide one brief natural acknowledgment without naming internal work. Afterward, answer from \
         the evidence and include material uncertainty or failure. When no tools are available, answer \
         only from supplied context. Never claim unfinished work succeeded or access paths outside the \
         workspace."
    );
    append_persona(
        prompt,
        persona,
        "User-configured persona guidance (follow when it does not conflict with the rules above):",
    )
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
        assert!(prompt.contains("including list items using the same method"));
        assert!(prompt.contains("dependent or strongly shared-state work together"));
        assert!(prompt.contains("Accepted evidence may support judgment and synthesis"));
        assert!(prompt.contains("do not demand a forecast or additional detail"));
        assert!(!prompt.contains("`ipython`"));
        assert!(prompt.ends_with("conversation persona"));
        assert!(prompt.len() < 1_500);
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
}
