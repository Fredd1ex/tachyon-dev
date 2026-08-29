#![forbid(unsafe_code)]

//! Ghost worker harness. User-facing Conversation orchestration lives in
//! `tachyon-foreground`.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use ghost::harness::backend::{Backend, ExecRequest, Local};
use ghost::harness::browser_setup;
use ghost::model::{from_agent_config, ChatMessage, Content, Model, Role, TokenUsage, ToolCall};
use ghost::role::AgentRole;
use tachyon_api::types::{Actor, AgentEvent, EventEnvelope};
use tokio::io::AsyncBufReadExt;

static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let (task, chat, cwd, role, agent_id) = parse_args();
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
            run_chat(&backend, role, agent_id).await
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
    match run_loop(&model, &mut messages, backend, role).await {
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
            emit_answer(&answer, &task, role, agent_id.as_deref());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("ghost: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_chat(backend: &Local, role: AgentRole, agent_id: Option<String>) -> ExitCode {
    let cfg = tachyon_util::config::Config::load();
    let model = match from_agent_config(&role.config(&cfg)) {
        Ok(model) => model,
        Err(error) => {
            println!("[ghost:error] model not ready: {error}");
            return ExitCode::FAILURE;
        }
    };
    let mut messages = vec![ChatMessage::new(Role::System, role.system_prompt(&cfg))];
    println!("[ghost] ready");
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        messages.push(ChatMessage::new(Role::User, text));
        match run_loop(&model, &mut messages, backend, role).await {
            Ok((answer, _)) => emit_answer(&answer, text, role, agent_id.as_deref()),
            Err(error) => println!("[ghost:error] {error}"),
        }
    }
    ExitCode::SUCCESS
}

async fn run_loop(
    model: &Model,
    messages: &mut Vec<ChatMessage>,
    backend: &Local,
    role: AgentRole,
) -> Result<(String, TokenUsage), String> {
    let tools = role.tools(false);
    let mut usage = TokenUsage::default();
    let mut last_call = None;
    let mut repeats = 0;
    for _ in 0..100 {
        let mut relay = |_delta: &str| {};
        let completion = model
            .chat(messages, Some(&tools), &mut relay)
            .await
            .map_err(|error| error.to_string())?;
        usage += completion.usage;
        let answer = completion.text.trim().to_string();
        let calls = completion.tool_calls.clone();
        messages.push(completion.to_message());
        if calls.is_empty() {
            if answer.is_empty() {
                return Err("model returned an empty response".into());
            }
            return Ok((answer, usage));
        }
        for call in calls {
            let signature = format!("{}|{}", call.name, call.arguments);
            if last_call.as_ref() == Some(&signature) {
                repeats += 1;
            } else {
                repeats = 0;
                last_call = Some(signature);
            }
            if repeats >= 3 {
                return Err("repeated identical tool call".into());
            }
            println!("[tool:{}] {} {}", call.id, call.name, call.arguments);
            let output = run_tool(&call, backend, role).await;
            println!("[tool-result:{}] {}", call.id, truncate(&output, 600));
            messages.push(ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: call.id,
                    output,
                }],
            });
        }
    }
    Err("max iterations reached".into())
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

fn emit_answer(answer: &str, task: &str, role: AgentRole, agent_id: Option<&str>) {
    if let Some(worker_id) = agent_id {
        emit_event(
            AgentEvent::WorkerCompleted {
                worker_id: worker_id.into(),
                objective: task.into(),
                result: answer.into(),
                artifacts: Vec::new(),
                context: "live Ghost and IPython session remain available".into(),
                suggested_reuse: true,
            },
            role,
            agent_id,
        );
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
