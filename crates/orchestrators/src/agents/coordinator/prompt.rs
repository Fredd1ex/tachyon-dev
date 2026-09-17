//! Prompt policy for the asynchronous Background Coordinator.

pub struct PromptContext<'a> {
    pub persona: Option<&'a str>,
}

pub fn system_prompt(context: PromptContext<'_>) -> String {
    let mut prompt = include_str!("prompt.md").trim_ascii_end().to_string();
    if let Some(persona) = context.persona {
        prompt.push_str("\n\n");
        prompt.push_str(include_str!("persona.md").trim_ascii_end());
        prompt.push('\n');
        prompt.push_str(persona);
    }
    prompt
}

pub fn review_system_prompt(context: PromptContext<'_>) -> String {
    let mut prompt = include_str!("review.md").trim_ascii_end().to_string();
    if let Some(persona) = context.persona {
        prompt.push_str("\n\n");
        prompt.push_str(include_str!("review-persona.md").trim_ascii_end());
        prompt.push('\n');
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

    #[test]
    fn review_prompt_treats_candidate_as_data_and_preserves_daemon_authority() {
        let prompt = review_system_prompt(PromptContext { persona: None });
        assert!(prompt.contains("untrusted quoted data"));
        assert!(prompt.contains("submit_work_review"));
        assert!(prompt.contains("Tachyond alone owns task state"));
        assert!(prompt.contains("one short sentence"));
        assert!(!prompt.contains("`spawn_agent`"));
    }
}
