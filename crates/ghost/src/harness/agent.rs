#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;

use futures_util::future::join_all;
use tachyon_model::{
    ChatMessage, Completion, Content, Model, ModelError, TokenUsage, ToolCall, ToolSpec,
};

use super::runtime::{ToolContext, ToolRegistry, ToolResult, MAX_RETURN_BYTES};

pub type ModelFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Completion, ModelError>> + Send + 'a>>;

pub trait AgentModel: Send + Sync {
    fn chat<'a>(&'a self, messages: &'a [ChatMessage], tools: &'a [ToolSpec]) -> ModelFuture<'a>;
}

impl AgentModel for Model {
    fn chat<'a>(&'a self, messages: &'a [ChatMessage], tools: &'a [ToolSpec]) -> ModelFuture<'a> {
        Box::pin(async move {
            let mut relay = |_delta: &str| {};
            self.chat(messages, Some(tools), &mut relay).await
        })
    }
}

#[derive(Clone, Debug)]
pub enum AgentLoopEvent {
    ToolStarted(ToolCall),
    ToolFinished { id: String, output: String },
}

pub trait AgentLoopEventSink: Send + Sync {
    fn emit(&self, event: AgentLoopEvent);
}

pub async fn run_loop<M: AgentModel>(
    model: &M,
    messages: &mut Vec<ChatMessage>,
    registry: &ToolRegistry,
    context: &ToolContext,
    max_iterations: usize,
    events: &dyn AgentLoopEventSink,
) -> Result<(String, TokenUsage), String> {
    let tools = registry.definitions(&context.policy);
    let mut usage = TokenUsage::default();
    let mut last_batch = None;
    let mut repeats = 0;
    for _ in 0..max_iterations {
        let completion = model
            .chat(messages, &tools)
            .await
            .map_err(|error| error.to_string())?;
        usage += completion.usage;
        let answer = completion.text.trim().to_string();
        let calls = completion.tool_calls.clone();
        if calls.is_empty() {
            if answer.is_empty() {
                return Err("model returned an empty response".into());
            }
            messages.push(completion.to_message());
            return Ok((answer, usage));
        }
        let batch = calls
            .iter()
            .map(normalized_call_signature)
            .collect::<Vec<_>>();
        if last_batch.as_ref() == Some(&batch) {
            repeats += 1;
        } else {
            repeats = 0;
            last_batch = Some(batch);
        }
        if repeats >= 3 {
            return Err("repeated identical tool-call batch".into());
        }
        for call in &calls {
            events.emit(AgentLoopEvent::ToolStarted(call.clone()));
        }
        messages.push(completion.to_message());
        let outputs = join_all(calls.iter().map(|call| run_tool(call, registry, context))).await;
        for (call, result) in calls.into_iter().zip(outputs) {
            let output = result.to_json(MAX_RETURN_BYTES);
            events.emit(AgentLoopEvent::ToolFinished {
                id: call.id.clone(),
                output,
            });
            messages.push(ChatMessage {
                role: tachyon_model::Role::Tool,
                content: vec![Content::ToolResult {
                    id: call.id,
                    output: result.to_json(context.policy.max_model_content_bytes),
                }],
            });
        }
    }
    Err("max iterations reached".into())
}

pub fn normalized_call_signature(call: &ToolCall) -> String {
    let arguments = serde_json::from_str::<serde_json::Value>(&call.arguments)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| call.arguments.trim().to_string());
    format!("{}|{arguments}", call.name)
}

async fn run_tool(call: &ToolCall, registry: &ToolRegistry, context: &ToolContext) -> ToolResult {
    let input = match serde_json::from_str(&call.arguments) {
        Ok(input) => input,
        Err(error) => {
            return super::runtime::ToolError::invalid(format!("invalid JSON arguments: {error}"))
                .into_result();
        }
    };
    let call_context = context.for_call(call.id.clone());
    registry
        .execute(&call.name, &call_context, input)
        .await
        .unwrap_or_else(super::runtime::ToolError::into_result)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use serde_json::json;
    use tachyon_api::types::ArtifactRegistration;
    use tachyon_model::{Role, ToolCall};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{
        native_registry, NoopOutputStore, ToolEventSink, ToolIdentity, ToolPolicy, ToolTelemetry,
    };

    struct ScriptedModel {
        steps: Mutex<VecDeque<Completion>>,
    }

    impl AgentModel for ScriptedModel {
        fn chat<'a>(
            &'a self,
            messages: &'a [ChatMessage],
            tools: &'a [ToolSpec],
        ) -> ModelFuture<'a> {
            Box::pin(async move {
                assert_eq!(tools.len(), 8);
                if let Some(previous) = messages.last().filter(|message| message.role == Role::Tool)
                {
                    let output = previous
                        .content
                        .iter()
                        .find_map(|content| match content {
                            Content::ToolResult { output, .. } => Some(output),
                            _ => None,
                        })
                        .expect("structured tool result");
                    let result: serde_json::Value = serde_json::from_str(output).unwrap();
                    assert_eq!(result["is_error"], false, "{output}");
                }
                Ok(self.steps.lock().unwrap().pop_front().expect("script step"))
            })
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        tools: Mutex<Vec<String>>,
        call_ids: Mutex<Vec<Option<String>>>,
        artifacts: Mutex<Vec<ArtifactRegistration>>,
    }

    impl ToolEventSink for RecordingSink {
        fn emit(&self, event: ToolTelemetry) {
            self.tools.lock().unwrap().push(event.tool_name);
            self.call_ids.lock().unwrap().push(event.identity.call_id);
        }

        fn register_artifact(&self, artifact: ArtifactRegistration) -> Result<(), String> {
            self.artifacts.lock().unwrap().push(artifact);
            Ok(())
        }
    }

    impl AgentLoopEventSink for RecordingSink {
        fn emit(&self, _event: AgentLoopEvent) {}
    }

    fn tool(id: &str, name: &str, arguments: serde_json::Value) -> Completion {
        Completion {
            text: String::new(),
            tool_calls: vec![ToolCall {
                id: id.into(),
                name: name.into(),
                arguments: arguments.to_string(),
            }],
            usage: TokenUsage::default(),
            finish_reason: Some("tool_calls".into()),
        }
    }

    #[tokio::test]
    async fn repository_dogfood_flow_uses_the_production_agent_loop() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("tests")).unwrap();
        std::fs::write(
            root.join("src/lib.rs"),
            "pub fn release_marker() -> &'static str {\n    \"pending\"\n}\n",
        )
        .unwrap();
        std::fs::write(
            root.join("tests/check.sh"),
            "set -eu\nfound=false\nwhile IFS= read -r line; do\n    case \"$line\" in *'\"ready\"'*) found=true ;; esac\ndone < src/lib.rs\n[ \"$found\" = true ]\nprintf 'dogfood fixture passed\\n' > test-report.txt\n",
        )
        .unwrap();

        let steps = VecDeque::from([
            tool("1", "ls", json!({"path":"."})),
            tool("2", "find", json!({"path":"src","pattern":"*.rs"})),
            tool(
                "3",
                "grep",
                json!({"path":"src","pattern":"release_marker","fixed_string":true}),
            ),
            tool(
                "4",
                "read",
                json!({"path":"src/lib.rs","offset":1,"limit":4}),
            ),
            tool(
                "5",
                "edit",
                json!({"path":"src/lib.rs","old":"\"pending\"","new":"\"ready\""}),
            ),
            tool(
                "6",
                "exec",
                json!({"argv":["/bin/sh","tests/check.sh"],"timeout_ms":2000}),
            ),
            tool(
                "7",
                "artifact",
                json!({"path":"test-report.txt","kind":"log","description":"Ghost repository dogfood result"}),
            ),
            Completion {
                text: "fixture complete".into(),
                tool_calls: Vec::new(),
                usage: TokenUsage::default(),
                finish_reason: Some("stop".into()),
            },
        ]);
        let model = ScriptedModel {
            steps: Mutex::new(steps),
        };
        let registry = native_registry();
        let sink = Arc::new(RecordingSink::default());
        let context = ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity {
                task_id: Some("dogfood-task".into()),
                work_id: Some("dogfood-work".into()),
                ..ToolIdentity::default()
            },
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root.clone())),
            event_sink: sink.clone(),
            output_store: Arc::new(NoopOutputStore),
        };
        let mut messages = vec![ChatMessage::new(
            Role::User,
            "repair and verify the fixture",
        )];

        let (answer, _) = run_loop(
            &model,
            &mut messages,
            &registry,
            &context,
            10,
            sink.as_ref(),
        )
        .await
        .unwrap();

        assert_eq!(answer, "fixture complete");
        assert!(model.steps.lock().unwrap().is_empty());
        assert!(std::fs::read_to_string(root.join("src/lib.rs"))
            .unwrap()
            .contains("\"ready\""));
        assert_eq!(
            std::fs::read_to_string(root.join("test-report.txt")).unwrap(),
            "dogfood fixture passed\n"
        );
        assert_eq!(
            *sink.tools.lock().unwrap(),
            ["ls", "find", "grep", "read", "edit", "exec", "artifact"]
        );
        assert_eq!(
            *sink.call_ids.lock().unwrap(),
            ["1", "2", "3", "4", "5", "6", "7"].map(|id| Some(id.into()))
        );
        let artifacts = sink.artifacts.lock().unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].path, "test-report.txt");
        assert_eq!(artifacts[0].work_id.as_deref(), Some("dogfood-work"));
    }
}
