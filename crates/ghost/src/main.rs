#![forbid(unsafe_code)]

//! Ghost worker harness. User-facing Conversation orchestration lives in
//! `tachyon-foreground`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::future::join_all;
use ghost::harness::backend::{Backend, ExecRequest, Local};
use ghost::harness::browser_setup;
use ghost::model::{from_agent_config, ChatMessage, Content, Model, Role, TokenUsage, ToolCall};
use ghost::role::AgentRole;
use tachyon_api::types::{
    Actor, AgentEvent, EventEnvelope, WorkEvent, WorkEventKind, WorkOutcome, WorkRequest,
    WorkResult,
};
use tokio::io::AsyncBufReadExt;

static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const DEFAULT_TOOL_CONTEXT_CHARS: usize = 12_000;
const DEFAULT_MAX_ITERATIONS: usize = 100;

#[derive(serde::Serialize, serde::Deserialize)]
struct ChatCheckpoint {
    messages: Vec<ChatMessage>,
    #[serde(default)]
    evidence: Vec<serde_json::Value>,
    #[serde(default = "initial_commit")]
    next_commit: u64,
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
    if role == AgentRole::Worker {
        if let Err(error) = browser_setup::ensure() {
            eprintln!("ghost: browser capability setup failed: {error}");
            return ExitCode::FAILURE;
        }
    }
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
        let backend = Local::new(&workspace);
        println!("[ghost] workspace: {}", workspace.display());
        if chat {
            run_chat(&backend, role, agent_id, &workspace).await
        } else {
            run_task(task.unwrap_or_default(), &backend, role, agent_id).await
        }
    })
}

async fn run_task(
    task: String,
    backend: &Local,
    role: AgentRole,
    agent_id: Option<String>,
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
    match run_loop(&model, &mut messages, backend, role, agent_id.as_deref()).await {
        Ok((answer, usage)) => {
            emit_event(
                AgentEvent::Usage {
                    turn: None,
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                },
                role,
                agent_id.as_deref(),
            );
            emit_answer(&answer, &task, None, role, agent_id.as_deref());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("ghost: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_chat(
    backend: &Local,
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
    println!("[ghost] ready");
    emit_ready(role, agent_id.as_deref());
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let text = line.trim();
        if text.is_empty() {
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
        match run_loop(&model, &mut messages, backend, role, agent_id.as_deref()).await {
            Ok((answer, usage)) => {
                emit_event(
                    AgentEvent::Usage {
                        turn: None,
                        prompt_tokens: usage.prompt_tokens,
                        completion_tokens: usage.completion_tokens,
                        total_tokens: usage.total_tokens,
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
    backend: &Local,
    role: AgentRole,
    agent_id: Option<&str>,
) -> Result<(String, TokenUsage), String> {
    let tools = role.tools(false);
    let mut usage = TokenUsage::default();
    let mut last_batch = None;
    let mut repeats = 0;
    for _ in 0..max_iterations() {
        let mut relay = |_delta: &str| {};
        let completion = model
            .chat(messages, Some(&tools), &mut relay)
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
            emit_event(
                AgentEvent::ToolStarted {
                    turn: None,
                    id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                },
                role,
                agent_id,
            );
            println!("[tool:{}] {} {}", call.id, call.name, call.arguments);
        }
        messages.push(completion.to_message());
        let outputs = join_all(calls.iter().map(|call| run_tool(call, backend, role))).await;
        for (call, output) in calls.into_iter().zip(outputs) {
            emit_event(
                AgentEvent::ToolFinished {
                    turn: None,
                    id: call.id.clone(),
                    output: output.clone(),
                },
                role,
                agent_id,
            );
            println!("[tool-result:{}] {}", call.id, truncate(&output, 600));
            messages.push(ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: call.id,
                    output: bounded_tool_output(&output, tool_context_chars()),
                }],
            });
        }
    }
    Err("max iterations reached".into())
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

fn normalized_call_signature(call: &ToolCall) -> String {
    let arguments = serde_json::from_str::<serde_json::Value>(&call.arguments)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| call.arguments.trim().to_string());
    format!("{}|{arguments}", call.name)
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

fn bounded_tool_output(output: &str, max_chars: usize) -> String {
    if output.chars().count() <= max_chars {
        return output.to_string();
    }
    let head_chars = max_chars * 3 / 4;
    let tail_chars = max_chars - head_chars;
    let head = output.chars().take(head_chars).collect::<String>();
    let tail = output
        .chars()
        .rev()
        .take(tail_chars)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{head}\n[tool output truncated for model context]\n{tail}")
}

async fn run_tool(call: &ToolCall, backend: &Local, role: AgentRole) -> String {
    if !role.allows_tool(&call.name) {
        return format!("{} is not available to the {role:?} role", call.name);
    }
    match call.name.as_str() {
        "ipython" => backend
            .run_ipython(&arg(&call.arguments, "code"))
            .await
            .combined(),
        "agent_browser" => match browser_request(&arg(&call.arguments, "args")) {
            Ok(request) => backend.run(&request).await.combined(),
            Err(error) => error,
        },
        other => format!("{other} remains available only to the temporary Background coordinator"),
    }
}

fn browser_request(line: &str) -> Result<ExecRequest, String> {
    let args = shell_words::split(line).map_err(|error| error.to_string())?;
    if args.is_empty() {
        return Err("no args for agent_browser".into());
    }
    let forbidden = [
        "--engine",
        "--executable-path",
        "--provider",
        "--cdp",
        "--auto-connect",
        "--max-output",
    ];
    if let Some(argument) = args.iter().find(|argument| {
        forbidden
            .iter()
            .any(|option| argument == option || argument.starts_with(&format!("{option}=")))
    }) {
        return Err(format!(
            "agent_browser cannot override its fixed Lightpanda configuration with {argument}"
        ));
    }
    Ok(ExecRequest {
        program: std::env::var("TACHYON_AGENT_BROWSER_BIN")
            .unwrap_or_else(|_| "agent-browser".into()),
        args,
    })
}

fn emit_answer(
    answer: &str,
    task: &str,
    request: Option<&WorkRequest>,
    role: AgentRole,
    agent_id: Option<&str>,
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
                        artifacts: Vec::new(),
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
                artifacts: Vec::new(),
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

fn arg(arguments: &str, key: &str) -> String {
    serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|value| {
            value
                .get(key)
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
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
    fn tool_output_context_keeps_boundaries() {
        let output = format!("START{}END", "x".repeat(2_000));
        let bounded = bounded_tool_output(&output, 1_000);
        assert!(bounded.starts_with("START"));
        assert!(bounded.ends_with("END"));
        assert!(bounded.contains("truncated for model context"));
        assert!(bounded.chars().count() < output.chars().count());
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
