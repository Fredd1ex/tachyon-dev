#![forbid(unsafe_code)]

//! Foreground model adapters for conversation policy.

use tachyon_model::{ChatMessage, Completion, Model, ModelError, Role, TokenUsage, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    Answerability, InteractionDecision, ANSWERABILITY_PROMPT, CLASSIFICATION_PROMPT,
};
use tachyon_orchestrator::conversation::prompt::SYNTHESIS_PROMPT;

/// Require a conversation decision tool while exposing only the direct
/// response argument to the user-facing stream.
pub async fn chat_requiring_decision(
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
    let completion = model
        .chat_requiring_tool(messages, tools, ("respond", "response"), &mut relay)
        .await;
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
    let evidence = serde_json::to_string(conversation).unwrap_or_default();
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

#[cfg(test)]
mod tests {
    use super::VisibleResponseStream;

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
}
