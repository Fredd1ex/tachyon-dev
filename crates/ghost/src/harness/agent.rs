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
    fn retain_outputs<'a>(
        &'a self,
        _outputs: &'a dyn super::runtime::ToolOutputStore,
    ) -> super::runtime::OutputStoreFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
    fn chat_with_context<'a>(
        &'a self,
        messages: &'a [ChatMessage],
        tools: &'a [ToolSpec],
        _context: &'a tachyon_api::context::WorkerContextMetadata,
    ) -> ModelFuture<'a> {
        self.chat(messages, tools)
    }
    fn completion_proposal(&self) -> Option<tachyon_api::work::CompletionProposal> {
        None
    }
    fn instruction_revision(&self) -> Option<u64> {
        None
    }
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

#[cfg(unix)]
impl AgentModel for tachyon_model::broker::BrokerClient {
    fn retain_outputs<'a>(
        &'a self,
        outputs: &'a dyn super::runtime::ToolOutputStore,
    ) -> super::runtime::OutputStoreFuture<'a, Result<(), String>> {
        Box::pin(async move {
            use sha2::{Digest, Sha256};
            use tachyon_model::broker::{ResourceUpload, UploadReply};
            for reference in outputs.references() {
                let first = outputs.export_page(&reference, 0).await?;
                let (retained, total, storage_failed) =
                    (first.retained, first.total, first.storage_failed);
                let mut hash = Sha256::new();
                let mut offset = 0;
                let mut page = first;
                loop {
                    hash.update(&page.bytes);
                    offset += page.bytes.len() as u64;
                    if offset == retained {
                        break;
                    }
                    page = outputs.export_page(&reference, offset).await?;
                }
                let sha256 = format!("{:x}", hash.finalize());
                let reply = self
                    .resource_upload(ResourceUpload::Begin {
                        handle: reference.id.clone(),
                        retained,
                        total,
                        storage_failed,
                        sha256: sha256.clone(),
                    })
                    .await
                    .map_err(|_| "output retention channel failed")?;
                if !matches!(reply, UploadReply::Accepted { offset: 0 }) {
                    return Err("output retention denied; retained spool gap".into());
                }
                offset = 0;
                while offset < retained {
                    let page = outputs.export_page(&reference, offset).await?;
                    let next = offset + page.bytes.len() as u64;
                    if next <= offset || page.retained != retained || page.total != total {
                        return Err("output changed during retention".into());
                    }
                    let reply = self
                        .resource_upload(ResourceUpload::Chunk {
                            offset,
                            bytes: page.bytes,
                        })
                        .await
                        .map_err(|_| "output retention channel failed")?;
                    if !matches!(reply, UploadReply::Accepted { offset } if offset == next) {
                        return Err("output retention chunk rejected".into());
                    }
                    offset = next;
                }
                let reply = self
                    .resource_upload(ResourceUpload::Finish {
                        sha256: Some(sha256.clone()),
                    })
                    .await
                    .map_err(|_| "output retention channel failed")?;
                if !matches!(reply, UploadReply::Ready { resource } if resource.valid() && resource.version == sha256)
                {
                    return Err("output retention did not commit; retained spool gap".into());
                }
            }
            Ok(())
        })
    }
    fn chat_with_context<'a>(
        &'a self,
        messages: &'a [ChatMessage],
        tools: &'a [ToolSpec],
        context: &'a tachyon_api::context::WorkerContextMetadata,
    ) -> ModelFuture<'a> {
        Box::pin(self.chat_with_context(messages, tools, context))
    }
    fn completion_proposal(&self) -> Option<tachyon_api::work::CompletionProposal> {
        self.completion_proposal()
    }
    fn instruction_revision(&self) -> Option<u64> {
        self.instruction_revision()
    }
    fn chat<'a>(&'a self, messages: &'a [ChatMessage], tools: &'a [ToolSpec]) -> ModelFuture<'a> {
        Box::pin(self.chat(messages, tools))
    }
}

#[derive(Clone, Debug)]
pub enum AgentLoopEvent {
    ToolStarted(ToolCall),
    ToolFinished { id: String, output: String },
}

pub trait AgentLoopEventSink: Send + Sync {
    fn emit(&self, event: AgentLoopEvent);

    fn record_wait(&self, _inference: bool, _elapsed: std::time::Duration) {}
}

pub async fn run_loop<M: AgentModel>(
    model: &M,
    messages: &mut Vec<ChatMessage>,
    registry: &ToolRegistry,
    context: &ToolContext,
    max_iterations: usize,
    events: &dyn AgentLoopEventSink,
) -> Result<(String, TokenUsage), String> {
    let mut usage = TokenUsage::default();
    let mut last_batch = None;
    let mut repeats = 0;
    for _ in 0..max_iterations {
        let tools = registry.definitions(&context.policy);
        // Ephemeral host guidance survives history compaction without accumulating
        // stale or duplicate system instructions in the transcript.
        let mut request = messages.clone();
        let guidance = registry.guidance(&context.policy);
        if !guidance.is_empty() {
            if let Some(system) = request
                .iter_mut()
                .find(|m| m.role == tachyon_model::Role::System)
            {
                system.content.push(Content::Text(guidance));
            } else {
                request.insert(0, ChatMessage::new(tachyon_model::Role::System, guidance));
            }
        }
        let started = tokio::time::Instant::now();
        let metadata = registry.context_metadata();
        let completion = model.chat_with_context(&request, &tools, &metadata).await;
        events.record_wait(true, started.elapsed());
        let completion = completion.map_err(|error| error.to_string())?;
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
        let started = tokio::time::Instant::now();
        let outputs = join_all(calls.iter().map(|call| run_tool(call, registry, context))).await;
        events.record_wait(false, started.elapsed());
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
        if let Some(proposal) = model.completion_proposal() {
            return Ok((proposal.summary, usage));
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
    let call_context = context.for_call(call.id.clone());
    let input = match serde_json::from_str(&call.arguments) {
        Ok(input) => input,
        Err(error) => {
            let result =
                super::runtime::ToolError::invalid(format!("invalid JSON arguments: {error}"))
                    .into_result();
            context.event_sink.record_result(
                &call.name,
                &call_context,
                &serde_json::json!({"invalid_arguments": call.arguments}),
                &result,
            );
            return result;
        }
    };
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
                assert_eq!(tools.len(), 9);
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
        evidence: crate::harness::runtime::WorkEvidenceCollector,
        waits: Mutex<Vec<(bool, Duration)>>,
        tools: Mutex<Vec<String>>,
        call_ids: Mutex<Vec<Option<String>>>,
        artifacts: Mutex<Vec<ArtifactRegistration>>,
    }

    impl ToolEventSink for RecordingSink {
        fn record_result(
            &self,
            name: &str,
            context: &ToolContext,
            input: &serde_json::Value,
            result: &ToolResult,
        ) {
            self.evidence.record(name, context, input, result);
        }

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
        fn record_wait(&self, inference: bool, elapsed: Duration) {
            self.waits.lock().unwrap().push((inference, elapsed));
        }
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
    async fn rejected_calls_are_observed_once() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let sink = Arc::new(RecordingSink::default());
        let mut context = ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: sink.clone(),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        let registry = native_registry();
        for (index, (name, arguments)) in [
            ("read", "{"),
            ("unknown", "{}"),
            ("read", r#"{"path":42}"#),
            ("read", r#"{"path":"missing"}"#),
            ("read", "{}"),
        ]
        .into_iter()
        .enumerate()
        {
            if index == 3 {
                Arc::make_mut(&mut context.policy)
                    .enabled_tools
                    .remove("read");
            } else if index == 4 {
                Arc::make_mut(&mut context.policy)
                    .enabled_tools
                    .insert("read".into());
                context.cancellation.cancel();
            }
            let call = ToolCall {
                id: format!("rejected-{index}"),
                name: name.into(),
                arguments: arguments.into(),
            };
            assert!(run_tool(&call, &registry, &context).await.is_error);
            let evidence = sink.evidence.snapshot();
            assert_eq!(evidence.observed_invocations, Some(index as u64 + 1));
            assert_eq!(evidence.tools.len(), index + 1);
            assert_eq!(
                evidence.tools[index].call_id.as_deref(),
                Some(call.id.as_str())
            );
            assert_eq!(evidence.tools[index].output["is_error"], true);
            assert_eq!(evidence.omitted, 0);
        }
    }

    #[tokio::test]
    async fn timing_records_one_wait_for_a_parallel_tool_batch() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let mut batch = tool("1", "ls", json!({"path":"."}));
        batch
            .tool_calls
            .extend(tool("2", "ls", json!({"path":"."})).tool_calls);
        let model = ScriptedModel {
            steps: Mutex::new(VecDeque::from([
                batch,
                Completion {
                    text: "done".into(),
                    tool_calls: Vec::new(),
                    usage: TokenUsage::default(),
                    finish_reason: Some("stop".into()),
                },
            ])),
        };
        let sink = Arc::new(RecordingSink::default());
        let context = ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: sink.clone(),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        run_loop(
            &model,
            &mut vec![],
            &native_registry(),
            &context,
            3,
            sink.as_ref(),
        )
        .await
        .unwrap();
        assert_eq!(sink.tools.lock().unwrap().len(), 2);
        let waits = sink.waits.lock().unwrap();
        assert_eq!(
            waits
                .iter()
                .map(|(inference, _)| *inference)
                .collect::<Vec<_>>(),
            [true, false, true]
        );
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
            host_service: None,
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
