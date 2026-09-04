#![forbid(unsafe_code)]

//! Ghost worker harness. User-facing Conversation orchestration lives in
//! `tachyon-foreground`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ghost::harness::agent::{run_loop as run_agent_loop, AgentLoopEvent, AgentLoopEventSink};
use ghost::harness::backend::Local;
use ghost::harness::browser_setup;
use ghost::harness::runtime::{
    native_registry, AgentBrowserTool, BrowserAvailability, IpythonTool, ToolContext,
    ToolEventSink, ToolIdentity, ToolOutputStore, ToolPolicy, ToolRegistry, ToolTelemetry,
    WorkspaceOutputStore, MAX_RETURN_BYTES,
};
use ghost::model::{from_agent_config, ChatMessage, Content, Model, Role, TokenUsage};
use ghost::role::AgentRole;
#[cfg(test)]
use ghost::{harness::agent::normalized_call_signature, model::ToolCall};
use tachyon_api::types::{
    Actor, AgentEvent, ArtifactRegistration, ContextCompactionCommand, EventEnvelope,
    ToolTelemetryIdentity as ApiToolTelemetryIdentity, WorkEvent, WorkEventKind, WorkOutcome,
    WorkRequest, WorkResult,
};
use tokio::io::AsyncBufReadExt;

static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const DEFAULT_TOOL_CONTEXT_CHARS: usize = 12_000;
const DEFAULT_MAX_ITERATIONS: usize = 100;

struct GhostToolEventSink {
    role: AgentRole,
    agent_id: Option<String>,
    artifacts: Mutex<Vec<String>>,
}

impl GhostToolEventSink {
    fn new(role: AgentRole, agent_id: Option<&str>) -> Self {
        Self {
            role,
            agent_id: agent_id.map(str::to_string),
            artifacts: Mutex::new(Vec::new()),
        }
    }

    fn artifact_paths(&self) -> Vec<String> {
        self.artifacts
            .lock()
            .map_or_else(|_| Vec::new(), |paths| paths.clone())
    }
}

impl ToolEventSink for GhostToolEventSink {
    fn emit(&self, event: ToolTelemetry) {
        let error_code = event
            .error_code
            .and_then(|code| serde_json::to_value(code).ok())
            .and_then(|value| value.as_str().map(str::to_owned));
        emit_event(
            AgentEvent::ToolTelemetry {
                tool_name: event.tool_name,
                call_id: event.identity.call_id,
                duration_ms: event.duration.as_millis().min(u64::MAX as u128) as u64,
                success: event.success,
                truncated: event.truncated,
                bytes_out: event.bytes_out.min(u64::MAX as usize) as u64,
                error_code,
                identity: ApiToolTelemetryIdentity {
                    task_id: event.identity.task_id,
                    work_id: event.identity.work_id,
                    generation: event.identity.generation,
                    assignment: event.identity.assignment,
                    attempt_id: event.identity.attempt_id,
                },
            },
            self.role,
            self.agent_id.as_deref(),
        );
    }

    fn register_artifact(&self, artifact: ArtifactRegistration) -> Result<(), String> {
        let mut paths = self
            .artifacts
            .lock()
            .map_err(|_| "artifact collector lock is poisoned".to_string())?;
        if !paths.contains(&artifact.path) {
            paths.push(artifact.path.clone());
        }
        drop(paths);
        emit_event(
            AgentEvent::ArtifactRegistered { artifact },
            self.role,
            self.agent_id.as_deref(),
        );
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ChatCheckpoint {
    messages: Vec<ChatMessage>,
    #[serde(default)]
    evidence: Vec<serde_json::Value>,
    #[serde(default = "initial_commit")]
    next_commit: u64,
    #[serde(default)]
    context_epoch: u64,
}

fn initial_commit() -> u64 {
    1
}

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let (task, chat, cwd, role, agent_id) = parse_args();
    if role == AgentRole::Background {
        eprintln!("ghost: the Background coordinator is daemon-owned and is not hosted by Ghost");
        return ExitCode::FAILURE;
    }
    let browser_availability = match browser_setup::ensure() {
        Ok(()) => BrowserAvailability::Available,
        Err(error) => {
            eprintln!("ghost: browser capability unavailable: {error}");
            BrowserAvailability::Unavailable(error.to_string())
        }
    };
    let workspace = cwd.map(PathBuf::from).unwrap_or_else(|| {
        ghost::harness::backend::ensure_workspace(agent_id.as_deref().unwrap_or("anon"))
            .unwrap_or_else(|_| std::env::temp_dir().join("tachyon-ghost"))
    });
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ghost: tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        if let Err(error) = tokio::fs::create_dir_all(&workspace).await {
            eprintln!(
                "ghost: cannot create workspace {}: {error}",
                workspace.display()
            );
            return ExitCode::FAILURE;
        }
        let workspace = match tokio::fs::canonicalize(&workspace).await {
            Ok(workspace) => workspace,
            Err(error) => {
                eprintln!(
                    "ghost: cannot resolve workspace {}: {error}",
                    workspace.display()
                );
                return ExitCode::FAILURE;
            }
        };
        let backend = Arc::new(Local::new(&workspace));
        let output_store: Arc<dyn ToolOutputStore> =
            match WorkspaceOutputStore::open(&workspace, 8 * 1024 * 1024, MAX_RETURN_BYTES).await {
                Ok(store) => Arc::new(store),
                Err(error) => {
                    eprintln!("ghost: cannot initialize durable tool output: {error}");
                    return ExitCode::FAILURE;
                }
            };
        let mut registry = native_registry();
        registry
            .register(IpythonTool::new(Arc::clone(&backend)))
            .expect("unique built-in tool");
        if matches!(&browser_availability, BrowserAvailability::Available) {
            registry
                .register(AgentBrowserTool::new(
                    Arc::clone(&backend),
                    browser_availability,
                ))
                .expect("unique built-in tool");
        }
        let mut policy = ToolPolicy::worker_default(workspace.clone());
        policy.max_model_content_bytes = tool_context_chars();
        let policy = Arc::new(policy);
        println!("[ghost] workspace: {}", workspace.display());
        if chat {
            run_chat(&registry, policy, output_store, role, agent_id, &workspace).await
        } else {
            run_task(
                task.unwrap_or_default(),
                &registry,
                policy,
                output_store,
                role,
                agent_id,
                &workspace,
            )
            .await
        }
    })
}

async fn run_task(
    task: String,
    registry: &ToolRegistry,
    policy: Arc<ToolPolicy>,
    output_store: Arc<dyn ToolOutputStore>,
    role: AgentRole,
    agent_id: Option<String>,
    workspace: &std::path::Path,
) -> ExitCode {
    let cfg = tachyon_util::config::Config::load();
    let model = match from_agent_config(&role.config(&cfg)) {
        Ok(model) => model,
        Err(error) => {
            eprintln!("ghost: {error}");
            return ExitCode::FAILURE;
        }
    };
    println!("[ghost] task: {task}");
    let mut messages = vec![
        ChatMessage::new(Role::System, role.system_prompt(&cfg)),
        ChatMessage::new(Role::User, task.clone()),
    ];
    let event_sink = Arc::new(GhostToolEventSink::new(role, agent_id.as_deref()));
    let context = tool_context(workspace, policy, output_store, None, event_sink.clone());
    match run_loop(
        &model,
        &mut messages,
        registry,
        &context,
        role,
        agent_id.as_deref(),
    )
    .await
    {
        Ok((answer, usage)) => {
            emit_event(
                AgentEvent::Usage {
                    turn: None,
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                    context_tokens: usage.context_tokens,
                    context_window: usage.context_window,
                },
                role,
                agent_id.as_deref(),
            );
            emit_answer(
                &answer,
                &task,
                None,
                role,
                agent_id.as_deref(),
                event_sink.artifact_paths(),
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("ghost: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_chat(
    registry: &ToolRegistry,
    policy: Arc<ToolPolicy>,
    output_store: Arc<dyn ToolOutputStore>,
    role: AgentRole,
    agent_id: Option<String>,
    workspace: &std::path::Path,
) -> ExitCode {
    let cfg = tachyon_util::config::Config::load();
    let model = match from_agent_config(&role.config(&cfg)) {
        Ok(model) => model,
        Err(error) => {
            println!("[ghost:error] model not ready: {error}");
            return ExitCode::FAILURE;
        }
    };
    let checkpoint_path = chat_checkpoint_path(workspace, role);
    let checkpoint = load_chat_checkpoint(&checkpoint_path);
    let mut messages = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.messages.clone())
        .unwrap_or_else(|| vec![ChatMessage::new(Role::System, role.system_prompt(&cfg))]);
    let system_prompt = ChatMessage::new(Role::System, role.system_prompt(&cfg));
    if let Some(system) = messages
        .iter_mut()
        .find(|message| message.role == Role::System)
    {
        *system = system_prompt;
    } else {
        messages.insert(0, system_prompt);
    }
    let evidence = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.evidence.clone())
        .unwrap_or_default();
    let mut next_commit = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.next_commit)
        .unwrap_or(1);
    let mut context_epoch = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.context_epoch)
        .unwrap_or_default();
    println!("[ghost] ready");
    emit_ready(role, agent_id.as_deref());
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        if let Ok(command) = serde_json::from_str::<ContextCompactionCommand>(text) {
            if command.epoch > context_epoch {
                compact_context_messages(&mut messages, command.target_tokens);
                context_epoch = command.epoch;
                write_chat_checkpoint(
                    &checkpoint_path,
                    &ChatCheckpoint {
                        messages: messages.clone(),
                        evidence: evidence.clone(),
                        next_commit,
                        context_epoch,
                    },
                );
            }
            emit_event(
                AgentEvent::ContextCompacted {
                    request_id: command.request_id,
                    epoch: command.epoch,
                    retained_context_tokens: estimated_context_tokens(&messages),
                },
                role,
                agent_id.as_deref(),
            );
            continue;
        }
        let request = serde_json::from_str::<WorkRequest>(text).ok();
        let objective = request
            .as_ref()
            .map(|request| request.objective.as_str())
            .unwrap_or(text);
        if let Some(request) = &request {
            emit_event(
                AgentEvent::WorkProgress {
                    event: WorkEvent {
                        work_id: request.work_id.clone(),
                        generation: request.generation,
                        assignment: request.assignment,
                        kind: WorkEventKind::Started,
                    },
                },
                role,
                agent_id.as_deref(),
            );
        }
        messages.push(ChatMessage::new(Role::User, objective));
        let event_sink = Arc::new(GhostToolEventSink::new(role, agent_id.as_deref()));
        let context = tool_context(
            workspace,
            Arc::clone(&policy),
            Arc::clone(&output_store),
            request.as_ref(),
            event_sink.clone(),
        );
        match run_loop(
            &model,
            &mut messages,
            registry,
            &context,
            role,
            agent_id.as_deref(),
        )
        .await
        {
            Ok((answer, usage)) => {
                emit_event(
                    AgentEvent::Usage {
                        turn: None,
                        prompt_tokens: usage.prompt_tokens,
                        completion_tokens: usage.completion_tokens,
                        total_tokens: usage.total_tokens,
                        context_tokens: usage.context_tokens,
                        context_window: usage.context_window,
                    },
                    role,
                    agent_id.as_deref(),
                );
                emit_answer(
                    &answer,
                    objective,
                    request.as_ref(),
                    role,
                    agent_id.as_deref(),
                    event_sink.artifact_paths(),
                );
            }
            Err(error) => {
                if let Some(request) = &request {
                    emit_event(
                        AgentEvent::WorkCandidate {
                            candidate: WorkResult {
                                work_id: request.work_id.clone(),
                                objective: request.objective.clone(),
                                generation: request.generation,
                                assignment: request.assignment,
                                outcome: WorkOutcome::Failed {
                                    message: error.clone(),
                                },
                            },
                        },
                        role,
                        agent_id.as_deref(),
                    );
                }
                println!("[ghost:error] {error}");
            }
        }
        compact_completed_history(&mut messages);
        next_commit = next_commit.saturating_add(1);
        write_chat_checkpoint(
            &checkpoint_path,
            &ChatCheckpoint {
                messages: messages.clone(),
                evidence: evidence.clone(),
                next_commit,
                context_epoch,
            },
        );
        emit_ready(role, agent_id.as_deref());
    }
    ExitCode::SUCCESS
}

fn chat_checkpoint_path(workspace: &std::path::Path, role: AgentRole) -> PathBuf {
    let name = match role {
        AgentRole::Worker => "worker.json",
        AgentRole::Background => "background.json",
    };
    workspace.join(".tachyon").join(name)
}

fn load_chat_checkpoint(path: &std::path::Path) -> Option<ChatCheckpoint> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
}

fn write_chat_checkpoint(path: &std::path::Path, checkpoint: &ChatCheckpoint) {
    let Ok(data) = serde_json::to_vec_pretty(checkpoint) else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, data).is_ok() {
        let _ = std::fs::rename(tmp, path);
    }
}

fn emit_ready(role: AgentRole, agent_id: Option<&str>) {
    if let Some(event) = ready_event(role) {
        emit_event(event, role, agent_id);
    }
}

fn ready_event(role: AgentRole) -> Option<AgentEvent> {
    (role == AgentRole::Worker).then(|| AgentEvent::Status {
        turn: None,
        phase: "ready".into(),
        message: "idle".into(),
    })
}

async fn run_loop(
    model: &Model,
    messages: &mut Vec<ChatMessage>,
    registry: &ToolRegistry,
    context: &ToolContext,
    role: AgentRole,
    agent_id: Option<&str>,
) -> Result<(String, TokenUsage), String> {
    let sink = GhostAgentLoopSink { role, agent_id };
    run_agent_loop(model, messages, registry, context, max_iterations(), &sink).await
}

fn max_iterations() -> usize {
    std::env::var("TACHYON_MAX_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_ITERATIONS)
}

fn tool_context_chars() -> usize {
    std::env::var("TACHYON_TOOL_OUTPUT_CONTEXT_CHARS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 1_000)
        .unwrap_or(DEFAULT_TOOL_CONTEXT_CHARS)
}

fn compact_completed_history(messages: &mut Vec<ChatMessage>) {
    messages.retain(|message| match message.role {
        Role::System | Role::User => true,
        Role::Assistant => message
            .content
            .iter()
            .all(|content| matches!(content, Content::Text(_))),
        Role::Tool => false,
    });
}

fn estimated_context_tokens(messages: &[ChatMessage]) -> u32 {
    messages
        .iter()
        .map(|message| {
            serde_json::to_vec(message)
                .map(|encoded| (encoded.len() / 4 + 1) as u32)
                .unwrap_or_default()
        })
        .fold(0, u32::saturating_add)
}

fn compact_context_messages(messages: &mut Vec<ChatMessage>, target_tokens: u32) {
    if estimated_context_tokens(messages) <= target_tokens {
        return;
    }
    let mut retained = Vec::new();
    let mut used = 0_u32;
    if let Some(system) = messages.iter().find(|message| message.role == Role::System) {
        used = used.saturating_add(estimated_context_tokens(std::slice::from_ref(system)));
        retained.push((0, system.clone()));
    }
    for (index, message) in messages.iter().enumerate().rev() {
        if message.role == Role::System {
            continue;
        }
        let cost = estimated_context_tokens(std::slice::from_ref(message));
        if used.saturating_add(cost) <= target_tokens || retained.len() < 3 {
            retained.push((index.saturating_add(1), message.clone()));
            used = used.saturating_add(cost);
        }
    }
    retained.sort_by_key(|(index, _)| *index);
    *messages = retained.into_iter().map(|(_, message)| message).collect();
}

struct GhostAgentLoopSink<'a> {
    role: AgentRole,
    agent_id: Option<&'a str>,
}

impl AgentLoopEventSink for GhostAgentLoopSink<'_> {
    fn emit(&self, event: AgentLoopEvent) {
        match event {
            AgentLoopEvent::ToolStarted(call) => {
                emit_event(
                    AgentEvent::ToolStarted {
                        turn: None,
                        id: call.id.clone(),
                        name: call.name.clone(),
                        arguments: call.arguments.clone(),
                    },
                    self.role,
                    self.agent_id,
                );
                println!("[tool:{}] {} {}", call.id, call.name, call.arguments);
            }
            AgentLoopEvent::ToolFinished { id, output } => {
                emit_event(
                    AgentEvent::ToolFinished {
                        turn: None,
                        id: id.clone(),
                        output: output.clone(),
                    },
                    self.role,
                    self.agent_id,
                );
                println!("[tool-result:{id}] {}", truncate(&output, 600));
            }
        }
    }
}

fn tool_context(
    workspace: &std::path::Path,
    policy: Arc<ToolPolicy>,
    output_store: Arc<dyn ToolOutputStore>,
    request: Option<&WorkRequest>,
    event_sink: Arc<dyn ToolEventSink>,
) -> ToolContext {
    let deadline = request
        .map(|request| instant_from_unix_ms(request.deadline_ms))
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(5 * 60));
    ToolContext {
        workspace_root: workspace.to_path_buf(),
        cwd: workspace.to_path_buf(),
        identity: ToolIdentity {
            work_id: request.map(|request| request.work_id.clone()),
            generation: request.map(|request| request.generation),
            assignment: request.map(|request| request.assignment),
            ..ToolIdentity::default()
        },
        deadline,
        cancellation: tokio_util::sync::CancellationToken::new(),
        policy,
        event_sink,
        output_store,
    }
}

fn instant_from_unix_ms(deadline_ms: u64) -> Instant {
    let now_wall_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    Instant::now() + Duration::from_millis(deadline_ms.saturating_sub(now_wall_ms))
}

fn emit_answer(
    answer: &str,
    task: &str,
    request: Option<&WorkRequest>,
    role: AgentRole,
    agent_id: Option<&str>,
    artifacts: Vec<String>,
) {
    if let Some(worker_id) = agent_id {
        let event = if let Some(request) = request {
            AgentEvent::WorkCandidate {
                candidate: WorkResult {
                    work_id: request.work_id.clone(),
                    objective: request.objective.clone(),
                    generation: request.generation,
                    assignment: request.assignment,
                    outcome: WorkOutcome::Completed {
                        result: answer.into(),
                        artifacts,
                        context: "live Ghost and IPython session remain available".into(),
                        suggested_reuse: true,
                    },
                },
            }
        } else {
            AgentEvent::WorkerCompleted {
                worker_id: worker_id.into(),
                objective: task.into(),
                result: answer.into(),
                artifacts,
                context: "live Ghost and IPython session remain available".into(),
                suggested_reuse: true,
            }
        };
        emit_event(event, role, agent_id);
    }
    emit_event(
        AgentEvent::Reply {
            turn: None,
            text: answer.into(),
            final_reply: true,
        },
        role,
        agent_id,
    );
    for line in answer.lines() {
        println!("[agent] {line}");
    }
}

fn emit_event(event: AgentEvent, role: AgentRole, agent_id: Option<&str>) {
    let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let session = agent_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("ghost-{}", std::process::id()));
    let actor = match role {
        AgentRole::Worker => Actor::Worker {
            id: session.clone(),
        },
        AgentRole::Background => Actor::Background,
    };
    let envelope = EventEnvelope {
        event_id: sequence,
        session_id: session,
        conversation_id: None,
        turn_id: None,
        task_id: agent_id.map(str::to_string),
        parent_task_id: None,
        tool_call_id: None,
        actor,
        sequence,
        occurred_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64),
        kind: event,
    };
    if let Ok(json) = serde_json::to_string(&envelope) {
        println!("{json}");
    }
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.into()
    } else {
        format!("{}...", text.chars().take(limit).collect::<String>())
    }
}

fn parse_args() -> (
    Option<String>,
    bool,
    Option<String>,
    AgentRole,
    Option<String>,
) {
    let args: Vec<_> = std::env::args().collect();
    let mut task = None;
    let mut chat = false;
    let mut cwd = None;
    let mut role = AgentRole::Worker;
    let mut agent_id = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--chat" => {
                chat = true;
                index += 1;
            }
            "--task" | "--cwd" | "--role" | "--agent-id" if index + 1 < args.len() => {
                match args[index].as_str() {
                    "--task" => task = Some(args[index + 1].clone()),
                    "--cwd" => cwd = Some(args[index + 1].clone()),
                    "--agent-id" => agent_id = Some(args[index + 1].clone()),
                    "--role" => {
                        role = if args[index + 1] == "background" {
                            AgentRole::Background
                        } else {
                            AgentRole::Worker
                        }
                    }
                    _ => {}
                }
                index += 2;
            }
            _ => index += 1,
        }
    }
    (task, chat, cwd, role, agent_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_workers_publish_idle_readiness() {
        assert!(matches!(
            ready_event(AgentRole::Worker),
            Some(AgentEvent::Status { phase, message, .. })
                if phase == "ready" && message == "idle"
        ));
        assert!(ready_event(AgentRole::Background).is_none());
    }

    #[test]
    fn chat_checkpoint_paths_are_role_isolated() {
        let root = std::path::Path::new("/tmp/workspace");
        assert!(chat_checkpoint_path(root, AgentRole::Worker).ends_with(".tachyon/worker.json"));
        assert!(
            chat_checkpoint_path(root, AgentRole::Background).ends_with(".tachyon/background.json")
        );
    }

    #[test]
    fn completed_history_drops_tool_protocol_but_keeps_transcript() {
        let mut messages = vec![
            ChatMessage::new(Role::System, "system"),
            ChatMessage::new(Role::User, "objective"),
            ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "ipython".into(),
                    arguments: "{}".into(),
                })],
            },
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "call-1".into(),
                    output: "raw output".into(),
                }],
            },
            ChatMessage::new(Role::Assistant, "final finding"),
        ];
        compact_completed_history(&mut messages);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[1].plain(), "objective");
        assert_eq!(messages[2].plain(), "final finding");
    }

    #[test]
    fn call_signatures_normalize_json_formatting() {
        let compact = ToolCall {
            id: "one".into(),
            name: "ipython".into(),
            arguments: r#"{"code":"1 + 1"}"#.into(),
        };
        let spaced = ToolCall {
            id: "two".into(),
            name: "ipython".into(),
            arguments: r#"{ "code": "1 + 1" }"#.into(),
        };
        assert_eq!(
            normalized_call_signature(&compact),
            normalized_call_signature(&spaced)
        );
    }
}
