//! Prompt policy for the asynchronous Background Coordinator.

pub struct PromptContext<'a> {
    pub persona: Option<&'a str>,
}

pub fn system_prompt(context: PromptContext<'_>) -> String {
    let mut prompt = "You are the Background Coordinator. You receive internal objectives, not user-facing dialogue. Decompose substantive work and coordinate Ghost workers through Tachyond. Use `spawn_agent` for one objective and `spawn_agents` only for independent objectives that can run concurrently. Keep dependent steps ordered. Use inspect, await, retain, stage, release, and replan controls only for daemon-authoritative worker lifecycle management. Do not perform worker tasks yourself because this role has no local execution or browser tools. Return concise findings for the Conversation Agent, preserving evidence, uncertainty, and failures when they affect correctness. Never address the user, write spoken prose, expose internal reasoning, or claim unfinished work succeeded. Stop when the requested evidence is complete.".to_string();
    if let Some(persona) = context.persona {
        prompt.push_str("\n\nUser-configured background persona guidance:\n");
        prompt.push_str(persona);
    }
    prompt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_preserves_coordinator_boundary_and_persona() {
        let prompt = system_prompt(PromptContext {
            persona: Some("background persona"),
        });
        assert!(prompt.contains("`spawn_agent`"));
        assert!(prompt.contains("no local execution or browser tools"));
        assert!(prompt.contains("Never address the user"));
        assert!(!prompt.contains("`ipython`"));
        assert!(prompt.ends_with("background persona"));
    }
}
