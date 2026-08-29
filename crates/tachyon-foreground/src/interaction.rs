#![forbid(unsafe_code)]

//! Foreground model adapters for conversation policy.

use tachyon_model::{ChatMessage, Completion, Model, ModelError, Role, TokenUsage, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    Answerability, InteractionDecision, ANSWERABILITY_PROMPT, CLASSIFICATION_PROMPT,
};
use tachyon_orchestrator::conversation::prompt::SYNTHESIS_PROMPT;

const DEFAULT_POLICY_CONTEXT_CHARS: usize = 8_000;

/// Allow a direct response or delegation while filtering accidental protocol
/// markup from the user-facing stream.
pub async fn chat_with_delegation(
    model: &Model,
    messages: &[ChatMessage],
    tools: &[ToolSpec],
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> std::result::Result<Completion, ModelError> {
    let mut visible = VisibleResponseStream::default();
    let mut relay = |delta: &str| {
        if let Some(delta) = visible.push(delta) {
            on_delta(&delta);
        }
    };
    let completion = model.chat(messages, Some(tools), &mut relay).await;
    drop(relay);
    if let Some(delta) = visible.finish() {
        on_delta(&delta);
    }
    completion
}

#[derive(Default)]
struct VisibleResponseStream {
    pending: String,
    protocol_blocked: bool,
}

impl VisibleResponseStream {
    fn push(&mut self, delta: &str) -> Option<String> {
        if self.protocol_blocked {
            return None;
        }
        self.pending.push_str(delta);
        if let Some(marker) = self.pending.find("DSML") {
            let marker = self.pending[..marker].rfind('<').unwrap_or(marker);
            let safe = self.pending[..marker].to_string();
            self.pending.clear();
            self.protocol_blocked = true;
            return (!safe.is_empty()).then_some(safe);
        }
        if let Some(marker) = self.pending.rfind('<') {
            let safe = self.pending[..marker].to_string();
            self.pending.drain(..marker);
            return (!safe.is_empty()).then_some(safe);
        }
        Some(std::mem::take(&mut self.pending))
    }

    fn finish(&mut self) -> Option<String> {
        if self.protocol_blocked {
            return None;
        }
        (!self.pending.is_empty()).then(|| std::mem::take(&mut self.pending))
    }
}

/// Classifies a new message independently of the tool-enabled conversation.
pub async fn classify(
    model: &Model,
    active_turn: &str,
    incoming: &str,
) -> tachyon_model::Result<(InteractionDecision, TokenUsage)> {
    let context = format!("Active turn:\n{active_turn}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, CLASSIFICATION_PROMPT),
        ChatMessage::new(Role::User, context),
    ];
    let mut relay = |_text: &str| {};
    let completion = model.chat(&messages, None, &mut relay).await?;
    Ok((
        InteractionDecision::parse(&completion.text)
            .unwrap_or(InteractionDecision::WaitForActiveTurn),
        completion.usage,
    ))
}

/// Decide whether an active follow-up already has enough evidence to answer.
/// This is deliberately separate from the tool-enabled Conversation loop so a
/// known answer cannot accidentally trigger another worker lookup.
pub async fn assess_answerability(
    model: &Model,
    context: &str,
    incoming: &str,
) -> tachyon_model::Result<(Answerability, TokenUsage)> {
    let input =
        format!("Existing conversation and evidence:\n{context}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, ANSWERABILITY_PROMPT),
        ChatMessage::new(Role::User, input),
    ];
    let mut relay = |_text: &str| {};
    let completion = model.chat(&messages, None, &mut relay).await?;
    Ok((Answerability::parse(&completion.text), completion.usage))
}

/// Convert private worker/tool evidence into the final spoken response. This
/// pass has no tools, so it cannot start more work or expose planning output.
pub async fn synthesize_spoken_response(
    model: &Model,
    conversation: &[ChatMessage],
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> tachyon_model::Result<(String, TokenUsage)> {
    let evidence = synthesis_context(conversation);
    let messages = vec![
        ChatMessage::new(Role::System, SYNTHESIS_PROMPT),
        ChatMessage::new(Role::User, evidence),
    ];
    let completion = model.chat(&messages, None, on_delta).await?;
    let text = completion.text.trim().replace('\n', " ");
    if text.is_empty() || text.len() > 4000 {
        return Err(ModelError::Api(
            "spoken synthesis returned no usable response".into(),
        ));
    }
    Ok((text, completion.usage))
}

/// Project model history into policy context without system instructions,
/// tool protocol, or provider-specific serialization.
pub fn policy_context(messages: &[ChatMessage]) -> String {
    let visible = messages
        .iter()
        .filter(|message| matches!(message.role, Role::User | Role::Assistant))
        .filter_map(|message| {
            let text = message.plain();
            (!text.trim().is_empty()).then_some((message.role, text))
        })
        .collect::<Vec<_>>();
    let rendered = visible
        .into_iter()
        .map(|(role, text)| format!("{role:?}: {text}"))
        .collect::<Vec<_>>()
        .join("\n");
    bounded_text(&rendered, policy_context_chars())
}

fn synthesis_context(messages: &[ChatMessage]) -> String {
    let request_index = messages
        .iter()
        .rposition(|message| message.role == Role::User)
        .unwrap_or(0);
    let request = messages
        .get(request_index)
        .map(ChatMessage::plain)
        .unwrap_or_default();
    let evidence_start = request_index.saturating_add(1).min(messages.len());
    let evidence = messages[evidence_start..]
        .iter()
        .filter(|message| message.role == Role::Tool)
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            tachyon_model::Content::ToolResult { output, .. } => Some(output.clone()),
            _ => None,
        })
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    bounded_text(
        &format!("Request:\n{request}\n\nEvidence:\n{evidence}"),
        policy_context_chars(),
    )
}

fn policy_context_chars() -> usize {
    std::env::var("TACHYON_POLICY_CONTEXT_CHARS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 1_000)
        .unwrap_or(DEFAULT_POLICY_CONTEXT_CHARS)
}

fn bounded_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let head = max_chars * 3 / 4;
    let tail = max_chars - head;
    let start = text.chars().take(head).collect::<String>();
    let end = text
        .chars()
        .rev()
        .take(tail)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{start}\n[context truncated]\n{end}")
}

#[cfg(test)]
mod tests {
    use super::{policy_context, synthesis_context, VisibleResponseStream};
    use tachyon_model::{ChatMessage, Content, Role, ToolCall};

    #[test]
    fn visible_response_never_streams_dsml_protocol_markup() {
        let mut stream = VisibleResponseStream::default();
        assert_eq!(
            stream.push("I’ll check that. <｜D"),
            Some("I’ll check that. ".into())
        );
        assert_eq!(stream.push("SML｜tool_calls>secret"), None);
        assert_eq!(stream.finish(), None);
    }

    #[test]
    fn policy_context_excludes_system_and_tool_protocol() {
        let messages = vec![
            ChatMessage::new(Role::System, "large private system prompt"),
            ChatMessage::new(Role::User, "question"),
            ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "spawn_agent".into(),
                    arguments: "private arguments".into(),
                })],
            },
            ChatMessage::new(Role::Assistant, "visible answer"),
        ];
        let context = policy_context(&messages);
        assert!(context.contains("question"));
        assert!(context.contains("visible answer"));
        assert!(!context.contains("private system"));
        assert!(!context.contains("private arguments"));
    }

    #[test]
    fn synthesis_uses_only_current_request_and_evidence() {
        let messages = vec![
            ChatMessage::new(Role::System, "private system"),
            ChatMessage::new(Role::User, "old request"),
            ChatMessage::new(Role::Assistant, "old answer"),
            ChatMessage::new(Role::User, "current request"),
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "call-1".into(),
                    output: "current evidence".into(),
                }],
            },
        ];
        let context = synthesis_context(&messages);
        assert!(context.contains("current request"));
        assert!(context.contains("current evidence"));
        assert!(!context.contains("private system"));
        assert!(!context.contains("old request"));
    }
}
