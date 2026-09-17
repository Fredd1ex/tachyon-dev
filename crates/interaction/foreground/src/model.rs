#![forbid(unsafe_code)]

//! Foreground model adapters for conversation policy.

use tachyon_model::{ChatMessage, Completion, Model, ModelError, Role, TokenUsage, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    Answerability, InteractionDecision, InteractionIntake, ANSWERABILITY_PROMPT,
    CLASSIFICATION_PROMPT,
};
use tachyon_orchestrator::conversation::prompt::SYNTHESIS_PROMPT;

const DEFAULT_POLICY_CONTEXT_CHARS: usize = 8_000;
const ANSWERABILITY_TOOL_NAME: &str = "submit_answerability";

/// Allow a direct response or delegation while filtering accidental protocol
/// markup from the user-facing stream.
pub(super) async fn chat_with_delegation(
    model: &Model,
    messages: &[ChatMessage],
    tools: &[ToolSpec],
    publish_direct: bool,
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> std::result::Result<Completion, ModelError> {
    let mut visible = VisibleResponseStream::default();
    let mut buffered = String::new();
    let mut relay = |delta: &str| {
        if let Some(delta) = visible.push(delta) {
            buffered.push_str(&delta);
        }
    };
    let completion = model.chat(messages, Some(tools), &mut relay).await;
    drop(relay);
    if let Some(delta) = visible.finish() {
        buffered.push_str(&delta);
    }
    if completion
        .as_ref()
        .is_ok_and(|completion| should_publish_direct(completion, publish_direct))
        && !visible.protocol_blocked
        && !buffered.is_empty()
    {
        on_delta(&buffered);
    }
    completion
}

fn should_publish_direct(completion: &Completion, publish_direct: bool) -> bool {
    publish_direct && completion.tool_calls.is_empty()
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
pub(super) async fn classify(
    model: &Model,
    active_turn: &str,
    incoming: &str,
) -> tachyon_model::Result<(InteractionDecision, Option<String>, TokenUsage)> {
    let context = format!("Active turn:\n{active_turn}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, CLASSIFICATION_PROMPT),
        ChatMessage::new(Role::User, context),
    ];
    let mut relay = |_text: &str| {};
    let completion = model.chat(&messages, None, &mut relay).await?;
    let intake = InteractionIntake::parse(&completion.text);
    Ok(match intake {
        Some(intake) => (
            intake.decision,
            Some(intake.acknowledgement),
            completion.usage,
        ),
        None => (
            InteractionDecision::WaitForActiveTurn,
            None,
            completion.usage,
        ),
    })
}

/// Decide whether an active follow-up already has enough evidence to answer.
/// This is deliberately separate from the tool-enabled Conversation loop so a
/// known answer cannot accidentally trigger another worker lookup.
pub(super) async fn assess_answerability(
    model: &Model,
    context: &str,
    incoming: &str,
) -> tachyon_model::Result<(Answerability, TokenUsage, bool)> {
    let input =
        format!("Existing conversation and evidence:\n{context}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, ANSWERABILITY_PROMPT),
        ChatMessage::new(Role::User, input),
    ];
    let mut relay = |_text: &str| {};
    let completion = model
        .chat_requiring_tool(
            &messages,
            &[answerability_tool()],
            (ANSWERABILITY_TOOL_NAME, "outcome"),
            &mut relay,
        )
        .await?;
    let answerability = parse_answerability_tool_call(&completion);
    Ok((
        answerability.unwrap_or(Answerability::NeedsNewWork),
        completion.usage,
        answerability.is_none(),
    ))
}

fn answerability_tool() -> ToolSpec {
    ToolSpec::new(
        ANSWERABILITY_TOOL_NAME,
        "Submit the internal answerability classification.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "outcome": {
                    "type": "string",
                    "enum": ["AnswerFromContext", "NeedsNewWork"]
                }
            },
            "required": ["outcome"],
            "additionalProperties": false
        }),
    )
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AnswerabilityToolOutput {
    outcome: Answerability,
}

fn parse_answerability_tool_call(completion: &Completion) -> Option<Answerability> {
    if !completion.text.trim().is_empty() || completion.tool_calls.len() != 1 {
        return None;
    }
    let call = &completion.tool_calls[0];
    if call.name != ANSWERABILITY_TOOL_NAME {
        return None;
    }
    serde_json::from_str::<AnswerabilityToolOutput>(&call.arguments)
        .ok()
        .map(|output| output.outcome)
}

/// Convert private worker/tool evidence into the final spoken response. This
/// pass has no tools, so it cannot start more work or expose planning output.
pub(super) async fn synthesize_spoken_response(
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
pub(super) fn policy_context(messages: &[ChatMessage]) -> String {
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
    bounded_policy_text(&rendered)
}

pub(super) fn bounded_policy_text(text: &str) -> String {
    bounded_text(text, policy_context_chars())
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
    bounded_policy_text(&format!("Request:\n{request}\n\nEvidence:\n{evidence}"))
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
pub(super) mod test_provider {
    use std::sync::Arc;
    use tachyon_model::{Model, ModelConfig};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    pub(crate) struct LocalProvider {
        pub model: Arc<Model>,
        pub requests: mpsc::UnboundedReceiver<Request>,
        server: tokio::task::JoinHandle<()>,
    }

    pub(crate) struct Request {
        pub body: serde_json::Value,
        stream: TcpStream,
    }

    impl LocalProvider {
        pub async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let model = Arc::new(Model::new(ModelConfig {
                base_url: format!("http://{}", listener.local_addr().unwrap()),
                api_key: "local-test-only".into(),
                model: "scripted-test-model".into(),
                temperature: 0.0,
                max_completion_tokens: Some(128),
                context_length: Some(4096),
                parallel_tool_calls: false,
                reasoning: Default::default(),
                routing: None,
                debug: false,
                debug_log: None,
            }));
            let (tx, requests) = mpsc::unbounded_channel();
            let server = tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    assert_eq!(line, "POST /chat/completions HTTP/1.1\r\n");
                    let mut length = None;
                    loop {
                        line.clear();
                        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = Some(value.trim().parse::<usize>().unwrap());
                            }
                        }
                    }
                    let length = length.expect("model request must have a content length");
                    assert!(length < 1024 * 1024);
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).await.unwrap();
                    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(body["model"], "scripted-test-model");
                    assert_eq!(body["stream"], true);
                    // Hand the socket to the script: no response until explicitly released.
                    if tx
                        .send(Request {
                            body,
                            stream: reader.into_inner(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            });
            Self {
                model,
                requests,
                server,
            }
        }

        pub async fn shutdown(&mut self) {
            self.server.abort();
            let result = (&mut self.server).await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
    }

    impl Drop for LocalProvider {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl Request {
        pub async fn respond(mut self, deltas: &[&str]) {
            let mut body = String::new();
            for delta in deltas {
                body.push_str(&format!("data: {}\n\n", serde_json::json!({
                    "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": null}]
                })));
            }
            body.push_str("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":4,\"total_tokens\":16}}\n\ndata: [DONE]\n\n");
            let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            self.stream.write_all(headers.as_bytes()).await.unwrap();
            // Split writes inside SSE records as well as splitting text across events.
            for chunk in body.as_bytes().chunks(7) {
                self.stream.write_all(chunk).await.unwrap();
                self.stream.flush().await.unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        answerability_tool, parse_answerability_tool_call, policy_context, should_publish_direct,
        synthesis_context, VisibleResponseStream, ANSWERABILITY_TOOL_NAME,
    };
    use tachyon_model::{ChatMessage, Completion, Content, Role, TokenUsage, ToolCall};
    use tachyon_orchestrator::conversation::policy::Answerability;

    #[tokio::test]
    async fn local_sse_synthesis_preserves_visible_text_across_event_boundaries() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for chunks in [
                vec!["The release is ready."],
                vec!["The rel", "ease ", "is ready."],
            ] {
                let model = provider.model.clone();
                jobs.spawn(async move {
                    let messages = vec![
                        ChatMessage::new(Role::User, "old request"),
                        ChatMessage::new(Role::Assistant, "old answer"),
                        ChatMessage::new(Role::User, "current request"),
                        ChatMessage {
                            role: Role::Tool,
                            content: vec![Content::ToolResult {
                                id: "call-1".into(),
                                output: "current evidence: release ready".into(),
                            }],
                        },
                    ];
                    let mut deltas = Vec::new();
                    let (answer, usage) =
                        super::synthesize_spoken_response(&model, &messages, &mut |delta| {
                            deltas.push(delta.to_string())
                        })
                        .await
                        .unwrap();
                    (answer, usage, deltas)
                });
                let request = provider.requests.recv().await.unwrap();
                assert!(request.body.get("tools").is_none());
                let context = request.body["messages"][1]["content"].as_str().unwrap();
                assert!(context.contains("current request"));
                assert!(context.contains("current evidence: release ready"));
                assert!(!context.contains("old request"));
                assert!(!context.contains("old answer"));
                request.respond(&chunks).await;
                let (answer, usage, deltas) = jobs.join_next().await.unwrap().unwrap();
                assert_eq!(deltas, chunks);
                assert_eq!(deltas.concat(), answer);
                assert_eq!(answer, "The release is ready.");
                assert_eq!(usage.total_tokens, 16);
            }
            assert!(provider.requests.try_recv().is_err());
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("local synthesis stream deadlocked");
    }

    #[tokio::test]
    async fn local_sse_protocol_markup_is_not_published_across_event_boundaries() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for chunks in [
                vec!["Private plan <DSML>hidden</DSML>"],
                vec!["Private plan <D", "S", "ML>hidden</DSML>"],
            ] {
                let model = provider.model.clone();
                jobs.spawn(async move {
                    let mut visible = String::new();
                    let completion = super::chat_with_delegation(
                        &model,
                        &[ChatMessage::new(Role::User, "request")],
                        &[],
                        true,
                        &mut |delta| visible.push_str(delta),
                    )
                    .await
                    .unwrap();
                    (completion, visible)
                });
                provider
                    .requests
                    .recv()
                    .await
                    .unwrap()
                    .respond(&chunks)
                    .await;
                let (completion, visible) = jobs.join_next().await.unwrap().unwrap();
                assert_eq!(completion.text, chunks.concat());
                assert_eq!(completion.finish_reason.as_deref(), Some("stop"));
                assert!(completion.tool_calls.is_empty());
                assert!(
                    visible.is_empty(),
                    "even the buffered planning prefix stays private"
                );
            }
            assert!(provider.requests.try_recv().is_err());
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("local protocol stream deadlocked");
    }

    fn answerability_completion(arguments: &str) -> Completion {
        Completion {
            text: String::new(),
            tool_calls: vec![ToolCall {
                id: "classification-1".into(),
                name: ANSWERABILITY_TOOL_NAME.into(),
                arguments: arguments.into(),
            }],
            usage: TokenUsage::default(),
            finish_reason: Some("tool_calls".into()),
        }
    }

    #[test]
    fn answerability_tool_schema_is_strict() {
        let tool = answerability_tool();
        assert_eq!(tool.name, ANSWERABILITY_TOOL_NAME);
        assert_eq!(tool.parameters["required"], serde_json::json!(["outcome"]));
        assert_eq!(tool.parameters["additionalProperties"], false);
        assert_eq!(
            tool.parameters["properties"]["outcome"]["enum"],
            serde_json::json!(["AnswerFromContext", "NeedsNewWork"])
        );
    }

    #[test]
    fn answerability_tool_parser_accepts_only_exact_structured_output() {
        assert_eq!(
            parse_answerability_tool_call(&answerability_completion(
                r#"{"outcome":"AnswerFromContext"}"#
            )),
            Some(Answerability::AnswerFromContext)
        );
        for arguments in [
            r#"{"outcome":"answer_from_context"}"#,
            r#"{"outcome":"AnswerFromContext","detail":"extra"}"#,
            r#"{"answerability":"AnswerFromContext"}"#,
            "AnswerFromContext",
        ] {
            assert_eq!(
                parse_answerability_tool_call(&answerability_completion(arguments)),
                None
            );
        }

        let mut prose = answerability_completion(r#"{"outcome":"AnswerFromContext"}"#);
        prose.text = "AnswerFromContext".into();
        assert_eq!(parse_answerability_tool_call(&prose), None);
    }

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
    fn tool_capable_prose_is_published_only_for_confirmed_direct_answers() {
        let direct = Completion {
            text: "A direct answer.".into(),
            tool_calls: Vec::new(),
            usage: TokenUsage::default(),
            finish_reason: Some("stop".into()),
        };
        assert!(should_publish_direct(&direct, true));
        assert!(!should_publish_direct(&direct, false));

        let delegated = answerability_completion(r#"{"outcome":"AnswerFromContext"}"#);
        assert!(!should_publish_direct(&delegated, true));
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

pub(super) fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut cp = s.chars();
        let cut: String = cp.by_ref().take(n).collect();
        format!("{cut}…")
    }
}
