//! Prompt policy for the user-facing Conversational Agent.

pub struct PromptContext<'a> {
    pub user_name: &'a str,
    pub conversation_name: &'a str,
    pub persona: Option<&'a str>,
}

pub const SYNTHESIS_PROMPT: &str = "Using the evidence below, answer the request directly. Return only a concise spoken response. Do not mention internal work. Include material uncertainty, and use formatting only if requested.";

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
         Use `spawn_agent` for one objective requiring fresh \
         evidence, retrieval, workspace access, or execution. Use `spawn_agents` only for independent \
         objectives; keep dependent steps ordered. Delegate at most once per turn. Before delegation, \
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
        assert!(!prompt.contains("`ipython`"));
        assert!(prompt.ends_with("conversation persona"));
        assert!(prompt.len() < 1_500);
    }

    #[test]
    fn prompt_does_not_embed_regression_examples() {
        let prompt = system_prompt(PromptContext {
            user_name: "you",
            conversation_name: "Conversational Agent",
            persona: None,
        })
        .to_ascii_lowercase();
        for domain_term in ["weather", "london", "new york", "tokyo", "coat"] {
            assert!(!prompt.contains(domain_term));
        }
    }
}
