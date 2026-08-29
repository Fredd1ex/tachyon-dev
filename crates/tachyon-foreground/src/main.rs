#![forbid(unsafe_code)]

//! Tachyon's user-facing Conversation runtime.

mod interaction;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use futures_util::future::join_all;
use interaction::{
    assess_answerability, chat_requiring_decision, classify, synthesize_spoken_response,
};
use tachyon_api::transport::Connection;
use tachyon_api::types::{
    Actor, AgentEvent, ApiRequest, ApiResponse, EventEnvelope, EventStream, LifetimeClass,
};
use tachyon_api::{InteractionCommand, InteractionCommandEnvelope};
use tachyon_model::{ChatMessage, Content, Model, Role, TokenUsage, ToolCall, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    execution_policy, publication_requires_dependency, Answerability, InteractionDecision,
};
use tokio::io::AsyncBufReadExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentRole {
    Conversation,
}

impl AgentRole {
    fn config(self, cfg: &tachyon_util::config::Config) -> tachyon_util::config::AgentConfig {
        cfg.conversation_config()
    }

    fn system_prompt(self, cfg: &tachyon_util::config::Config) -> String {
        let user_name = cfg.user_name();
        let conversation_name = cfg.conversation_name();
        let configured = cfg.conversation_config();
        tachyon_orchestrator::conversation::prompt::system_prompt(
            tachyon_orchestrator::conversation::prompt::PromptContext {
                user_name: &user_name,
                conversation_name: &conversation_name,
                persona: configured.persona.as_deref(),
            },
        )
    }

    fn tools(self, force_delegation: bool) -> Vec<ToolSpec> {
        let capabilities = if force_delegation {
            tachyon_orchestrator::conversation::DELEGATION_CAPABILITIES
        } else {
            tachyon_orchestrator::conversation::CAPABILITIES
        };
        capabilities
            .iter()
            .map(|capability| match capability {
                tachyon_orchestrator::capabilities::Capability::Respond => {
                    tachyon_orchestrator::tools::respond()
                }
                tachyon_orchestrator::capabilities::Capability::DelegateOne => {
                    tachyon_orchestrator::tools::spawn_agent()
                }
                tachyon_orchestrator::capabilities::Capability::DelegateMany => {
                    tachyon_orchestrator::tools::spawn_agents()
                }
                _ => unreachable!("conversation capability set contains lifecycle tool"),
            })
            .map(|schema| ToolSpec::new(schema.name, schema.description, schema.parameters))
            .collect()
    }

    fn allows_tool(self, name: &str) -> bool {
        tachyon_orchestrator::conversation::CAPABILITIES
            .iter()
            .any(|capability| capability.tool_name() == name)
    }
}

fn from_agent_config(agent: &tachyon_util::config::AgentConfig) -> Result<Model, String> {
    let cfg = tachyon_util::config::Config::load();
    let api_key = cfg.resolve_key("openrouter").ok_or_else(|| {
        "api key not configured (set OPENROUTER_API_KEY and restart the daemon)".to_string()
    })?;
    let configured = agent.model(&cfg.model);
    let model_name = configured
        .name
        .clone()
        .unwrap_or_else(|| tachyon_util::config::Config::default_model().into());
    eprintln!("[foreground] model: {model_name}");
    Ok(Model::new(tachyon_model::ModelConfig {
        base_url: cfg.provider_base_url(),
        api_key,
        model: model_name,
        temperature: configured.temperature.unwrap_or(0.2),
        max_completion_tokens: configured.max_completion_tokens,
        context_length: configured.context_length,
        parallel_tool_calls: configured.parallel_tool_calls,
        reasoning: configured.reasoning,
        routing: cfg.provider_routing(),
        debug: std::env::var("TACHYON_DEBUG").is_ok_and(|value| value == "1" || value == "true"),
        debug_log: Some(tachyon_util::daemon::logs_dir().join("debug-http.log")),
    }))
}

struct ConversationState {
    messages: Vec<ChatMessage>,
    evidence: Vec<EvidenceRecord>,
    pending: BTreeMap<u64, Vec<ChatMessage>>,
    next_commit: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ConversationCheckpoint {
    messages: Vec<ChatMessage>,
    #[serde(default)]
    evidence: Vec<EvidenceRecord>,
    next_commit: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum EvidenceRecord {
    Correlated(EventEnvelope),
    Legacy(AgentEvent),
}

enum ChatInput {
    User(String),
    Evidence(EvidenceRecord),
    Ignore,
}

impl EvidenceRecord {
    fn event(&self) -> &AgentEvent {
        match self {
            Self::Correlated(envelope) => &envelope.kind,
            Self::Legacy(event) => event,
        }
    }

    fn origin_turn(&self) -> Option<u64> {
        match self {
            Self::Correlated(envelope) => envelope.turn_id.as_deref()?.parse().ok(),
            Self::Legacy(_) => None,
        }
    }
}

struct EventContext {
    session_id: String,
    conversation_id: Option<String>,
    actor: Actor,
}

static EVENT_CONTEXT: OnceLock<EventContext> = OnceLock::new();
static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let (cwd, agent_id, new_session) = parse_args();
    let role = AgentRole::Conversation;
    init_event_context(role, agent_id.as_deref());

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("tachyon-foreground: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        // The agent's workspace is its prison: all tool output lands here. If
        // the daemon gave us an explicit cwd (workspace), honour it; otherwise
        // fall back to a per-id workspace under ~/.local/share/tachyon.
        let workspace = match &cwd {
            Some(c) => PathBuf::from(c),
            None => {
                let id = agent_id.clone().unwrap_or_else(|| "anon".into());
                tachyon_util::daemon::workspaces_dir().join(id)
            }
        };
        println!("[foreground] workspace: {}", workspace.display());
        run_chat(role, &workspace, new_session, agent_id).await
    })
}

/// Chat mode: read user lines from stdin forever. Stays alive even if the
/// model can't be configured (e.g. missing key) so the user always sees why.
async fn run_chat(
    role: AgentRole,
    workspace: &PathBuf,
    new_session: bool,
    agent_id: Option<String>,
) -> ExitCode {
    let cfg = tachyon_util::config::Config::load();
    let model_config = role.config(&cfg);
    let model = match from_agent_config(&model_config) {
        Ok(m) => Some(m),
        Err(e) => {
            println!("[foreground:error] model not ready: {e}");
            println!("[foreground] ready");
            None
        }
    };
    let model = model.map(Arc::new);
    let cfg = tachyon_util::config::Config::load();
    let checkpoint_path = chat_checkpoint_path(workspace, role);
    if new_session {
        let _ = std::fs::remove_file(&checkpoint_path);
    }
    let checkpoint = load_checkpoint(&checkpoint_path);
    let mut initial_messages = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.messages.clone())
        .unwrap_or_else(|| vec![ChatMessage::new(Role::System, role.system_prompt(&cfg))]);
    // Checkpoints contain conversation history, but the system prompt must
    // always come from the current binary/configuration rather than a stale
    // prompt persisted by an earlier session.
    if let Some(system) = initial_messages
        .iter_mut()
        .find(|message| message.role == Role::System)
    {
        *system = ChatMessage::new(Role::System, role.system_prompt(&cfg));
    } else {
        initial_messages.insert(0, ChatMessage::new(Role::System, role.system_prompt(&cfg)));
    }
    let conversation = Arc::new(Mutex::new(ConversationState {
        messages: initial_messages,
        evidence: checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.evidence.clone())
            .unwrap_or_default(),
        pending: BTreeMap::new(),
        next_commit: checkpoint.as_ref().map(|c| c.next_commit).unwrap_or(1),
    }));
    let state_changed = Arc::new(tokio::sync::Notify::new());
    let checkpoint_tx = start_checkpoint_writer(checkpoint_path.clone());
    let next_turn = Arc::new(AtomicU64::new(
        checkpoint.as_ref().map(|c| c.next_commit).unwrap_or(1),
    ));
    let active_turns = Arc::new(Mutex::new(BTreeMap::<u64, String>::new()));
    // Bound deferred work so input cannot grow memory without limit. Independent
    // turns do not use this queue and may run while it is draining.
    let (turn_tx, mut turn_rx) = tokio::sync::mpsc::channel::<(
        u64,
        String,
        bool,
        InteractionDecision,
        TokenUsage,
        std::time::Instant,
    )>(64);

    // Every accepted turn gets its own task. Publication policy, rather than
    // admission order, decides whether it may answer alongside active work.
    let processor_model = model.clone();
    let processor_conversation = Arc::clone(&conversation);
    let processor_active_turns = Arc::clone(&active_turns);
    let processor_state_changed = Arc::clone(&state_changed);
    let processor_checkpoint_tx = checkpoint_tx.clone();
    tokio::spawn(async move {
        while let Some((turn, text, queued, decision, routing_usage, accepted_at)) =
            turn_rx.recv().await
        {
            let args = (
                turn,
                text,
                queued,
                decision,
                routing_usage,
                accepted_at,
                processor_model.clone(),
                Arc::clone(&processor_conversation),
                Arc::clone(&processor_active_turns),
                Arc::clone(&processor_state_changed),
                processor_checkpoint_tx.clone(),
                role,
                agent_id.clone(),
            );
            tokio::spawn(process_turn(args));
        }
    });

    println!("[foreground] ready");
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin).lines();
    while let Ok(Some(line)) = reader.next_line().await {
        let text = match decode_chat_input(&line, role) {
            ChatInput::User(text) => text,
            ChatInput::Evidence(record) => {
                if matches!(record.event(), AgentEvent::WorkerCompleted { .. }) {
                    let mut state = conversation.lock().unwrap();
                    if !state
                        .evidence
                        .iter()
                        .any(|existing| same_evidence(existing, &record))
                    {
                        state.evidence.push(record);
                        let _ = checkpoint_tx.send(checkpoint_snapshot(&state));
                        state_changed.notify_waiters();
                    }
                }
                continue;
            }
            ChatInput::Ignore => continue,
        };
        let turn = next_turn.fetch_add(1, Ordering::Relaxed);
        let accepted_at = std::time::Instant::now();
        let (queued, classifier_context) = {
            let mut active = active_turns.lock().unwrap();
            let queued = !active.is_empty();
            let context = active
                .iter()
                .map(|(turn, text)| format!("Active turn {turn}: {text}"))
                .collect::<Vec<_>>()
                .join("\n");
            active.insert(turn, text.clone());
            (queued, context)
        };
        if !queued {
            if turn_tx
                .send((
                    turn,
                    text,
                    false,
                    InteractionDecision::WaitForActiveTurn,
                    TokenUsage::default(),
                    accepted_at,
                ))
                .await
                .is_err()
            {
                active_turns.lock().unwrap().remove(&turn);
                break;
            }
            continue;
        }

        emit_turn(Some(turn), "[status] routing alongside active work".into());
        let classifier_tx = turn_tx.clone();
        let classifier_model = model.clone();
        let classifier_active_turns = Arc::clone(&active_turns);
        tokio::spawn(async move {
            let (decision, usage) = if let Some(model) = classifier_model {
                tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    classify(&model, &classifier_context, &text),
                )
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or((
                    InteractionDecision::WaitForActiveTurn,
                    TokenUsage::default(),
                ))
            } else {
                (
                    InteractionDecision::WaitForActiveTurn,
                    TokenUsage::default(),
                )
            };
            emit_event(AgentEvent::Timing {
                turn,
                stage: "routing".into(),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
            if classifier_tx
                .send((turn, text, true, decision, usage, accepted_at))
                .await
                .is_err()
            {
                classifier_active_turns.lock().unwrap().remove(&turn);
            }
        });
    }
    ExitCode::SUCCESS
}

async fn process_turn(
    args: (
        u64,
        String,
        bool,
        InteractionDecision,
        TokenUsage,
        std::time::Instant,
        Option<Arc<Model>>,
        Arc<Mutex<ConversationState>>,
        Arc<Mutex<BTreeMap<u64, String>>>,
        Arc<tokio::sync::Notify>,
        std::sync::mpsc::Sender<ConversationCheckpoint>,
        AgentRole,
        Option<String>,
    ),
) {
    let (
        turn,
        text,
        queued,
        decision,
        mut auxiliary_usage,
        accepted_at,
        model,
        conversation,
        active_turns,
        state_changed,
        checkpoint_tx,
        role,
        _agent_id,
    ) = args;
    let Some(model) = model else {
        if publication_requires_dependency(queued, decision) {
            wait_for_prior_turn(&conversation, &state_changed, turn).await;
        }
        let answer =
            "I couldn't complete that request because no conversation model is configured.";
        emit_turn(
            Some(turn),
            "[foreground:error] model is not configured (set OPENROUTER_API_KEY and restart the daemon)"
                .into(),
        );
        emit_turn_block(Some(turn), "[agent]", answer);
        let mut current = conversation.lock().unwrap();
        current.pending.insert(
            turn,
            vec![
                ChatMessage::new(Role::User, text),
                ChatMessage::new(Role::Assistant, answer),
            ],
        );
        commit_ready_turns(&mut current);
        let _ = checkpoint_tx.send(checkpoint_snapshot(&current));
        state_changed.notify_waiters();
        mark_turn_inactive(&active_turns, turn);
        return;
    };
    if queued {
        if publication_requires_dependency(queued, decision) {
            emit_turn(
                Some(turn),
                format!("[status] dependency=turn {}", turn.saturating_sub(1)),
            );
            wait_for_context_or_evidence(&conversation, &state_changed, turn, &text).await;
        }
    }
    emit_event(AgentEvent::Timing {
        turn,
        stage: "ready".into(),
        elapsed_ms: accepted_at.elapsed().as_millis() as u64,
    });
    let snapshot = conversation.lock().unwrap().messages.clone();
    let mut local = snapshot;
    if queued && decision != InteractionDecision::AnswerNow {
        let evidence = conversation.lock().unwrap().evidence.clone();
        let relevant = evidence
            .iter()
            .filter_map(|record| match record.event() {
                AgentEvent::WorkerCompleted {
                    worker_id,
                    objective,
                    result,
                    ..
                } if evidence_relevant_to_follow_up(
                    record,
                    turn.saturating_sub(1),
                    &text,
                ) => Some(format!(
                    "Available background evidence (worker {worker_id}, objective {objective}):\n{result}"
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        if !relevant.is_empty() {
            local.push(ChatMessage::new(Role::System, relevant.join("\n\n")));
        }
    }
    let answerability = if publication_requires_dependency(queued, decision) {
        let context = serde_json::to_string(&local).unwrap_or_default();
        let (answerability, usage) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            assess_answerability(&model, &context, &text),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or((Answerability::NeedsNewWork, TokenUsage::default()));
        auxiliary_usage += usage;
        Some(answerability)
    } else {
        None
    };
    let policy = execution_policy(answerability);

    // Correlated evidence is private turn context, not durable assistant prose.
    // Commit only the accepted user message and this turn's generated records.
    let base_len = local.len();
    local.push(ChatMessage::new(Role::User, text.clone()));
    println!("[turn:{turn}] [user] {text}");
    emit_turn(Some(turn), "[status] working".into());
    // Routing controls scheduling only. A separate answerability decision
    // controls whether dependent turns may answer without fresh work.
    let tools_enabled = true;
    match loop_until_done(
        &model,
        &mut local,
        role,
        Some(turn),
        tools_enabled,
        policy.force_delegation,
        policy.answer_from_context,
        true,
        Some(accepted_at),
    )
    .await
    {
        Ok(Turn::Done(answer, mut usage)) => {
            usage += auxiliary_usage;
            record_final_answer(&mut local, &answer);
            emit_event(AgentEvent::Usage {
                turn: Some(turn),
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
            });
            emit_event(AgentEvent::Timing {
                turn,
                stage: "publication_started".into(),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
            emit_turn_block(Some(turn), "[agent]", &answer);
            emit_event(AgentEvent::Timing {
                turn,
                stage: "completed".into(),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
        }
        Ok(Turn::MaxIterations(mut usage)) => {
            usage += auxiliary_usage;
            emit_event(AgentEvent::Usage {
                turn: Some(turn),
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
            });
            let answer =
                "I couldn't complete that request because the agent reached its processing limit.";
            record_final_answer(&mut local, answer);
            emit_turn(Some(turn), "[foreground:error] max iterations".into());
            emit_turn_block(Some(turn), "[agent]", answer);
        }
        Err(error) => {
            let answer =
                "I couldn't complete that request because the agent encountered an internal error.";
            record_final_answer(&mut local, answer);
            emit_turn(Some(turn), format!("[foreground:error] {error}"));
            emit_turn_block(Some(turn), "[agent]", answer);
        }
    }
    let mut current = conversation.lock().unwrap();
    current
        .pending
        .insert(turn, local.into_iter().skip(base_len).collect());
    commit_ready_turns(&mut current);
    let _ = checkpoint_tx.send(checkpoint_snapshot(&current));
    state_changed.notify_waiters();
    mark_turn_inactive(&active_turns, turn);
}

fn mark_turn_inactive(active_turns: &Arc<Mutex<BTreeMap<u64, String>>>, turn: u64) {
    active_turns.lock().unwrap().remove(&turn);
}

fn commit_ready_turns(conversation: &mut ConversationState) {
    loop {
        let next_commit = conversation.next_commit;
        let Some(messages) = conversation.pending.remove(&next_commit) else {
            break;
        };
        conversation.messages.extend(messages);
        conversation.next_commit += 1;
    }
}

fn chat_checkpoint_path(workspace: &PathBuf, role: AgentRole) -> PathBuf {
    let _ = role;
    workspace.join(".tachyon").join("conversation.json")
}

async fn wait_for_prior_turn(
    conversation: &Arc<Mutex<ConversationState>>,
    state_changed: &tokio::sync::Notify,
    turn: u64,
) {
    loop {
        let notified = state_changed.notified();
        if conversation.lock().unwrap().next_commit >= turn {
            return;
        }
        notified.await;
    }
}

async fn wait_for_context_or_evidence(
    conversation: &Arc<Mutex<ConversationState>>,
    state_changed: &tokio::sync::Notify,
    turn: u64,
    incoming: &str,
) {
    loop {
        let notified = state_changed.notified();
        let (committed, relevant) = {
            let state = conversation.lock().unwrap();
            (
                state.next_commit >= turn,
                state.evidence.iter().any(|record| {
                    evidence_relevant_to_follow_up(record, turn.saturating_sub(1), incoming)
                }),
            )
        };
        if committed || relevant {
            return;
        }
        notified.await;
    }
}

fn evidence_relevant_to_follow_up(
    record: &EvidenceRecord,
    prior_turn: u64,
    incoming: &str,
) -> bool {
    let AgentEvent::WorkerCompleted { objective, .. } = record.event() else {
        return false;
    };
    record
        .origin_turn()
        .is_none_or(|origin| origin == prior_turn)
        && evidence_matches(incoming, objective)
}

fn evidence_matches(incoming: &str, objective: &str) -> bool {
    let incoming = context_terms(incoming);
    let objective = context_terms(objective);
    !incoming.is_empty() && incoming.iter().any(|term| objective.contains(term))
}

fn context_terms(text: &str) -> std::collections::BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|term| term.len() > 2)
        .filter(|term| {
            !matches!(
                term.as_str(),
                "the"
                    | "and"
                    | "for"
                    | "with"
                    | "from"
                    | "that"
                    | "this"
                    | "what"
                    | "when"
                    | "where"
                    | "will"
                    | "would"
                    | "could"
                    | "should"
                    | "need"
                    | "have"
                    | "about"
            )
        })
        .collect()
}

fn load_checkpoint(path: &std::path::Path) -> Option<ConversationCheckpoint> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
}

fn checkpoint_snapshot(conversation: &ConversationState) -> ConversationCheckpoint {
    ConversationCheckpoint {
        messages: conversation.messages.clone(),
        evidence: conversation.evidence.clone(),
        next_commit: conversation.next_commit,
    }
}

fn start_checkpoint_writer(path: PathBuf) -> std::sync::mpsc::Sender<ConversationCheckpoint> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(checkpoint) = rx.recv() {
            write_checkpoint(&path, &checkpoint);
        }
    });
    tx
}

fn write_checkpoint(path: &std::path::Path, checkpoint: &ConversationCheckpoint) {
    let Ok(data) = serde_json::to_vec_pretty(&checkpoint) else {
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

enum Turn {
    Done(String, TokenUsage),
    MaxIterations(TokenUsage),
}

fn emit_turn(turn: Option<u64>, message: String) {
    if let Some(rest) = message.strip_prefix("[status] ") {
        let mut parts = rest.splitn(2, ' ');
        emit_event(AgentEvent::Status {
            turn,
            phase: parts.next().unwrap_or("working").to_string(),
            message: parts.next().unwrap_or_default().to_string(),
        });
    } else if let Some(message) = message.strip_prefix("[foreground:error] ") {
        emit_event(AgentEvent::Error {
            turn,
            message: message.to_string(),
        });
    } else if let Some(rest) = message.strip_prefix("[tool:") {
        if let Some((id, rest)) = rest.split_once("] ") {
            let mut parts = rest.splitn(2, ' ');
            emit_event(AgentEvent::ToolStarted {
                turn,
                id: id.to_string(),
                name: parts.next().unwrap_or_default().to_string(),
                arguments: parts.next().unwrap_or_default().to_string(),
            });
        }
    }
    if let Some(turn) = turn {
        println!("[turn:{turn}] {message}");
    } else {
        println!("{message}");
    }
}

fn emit_turn_block(turn: Option<u64>, kind: &str, text: &str) {
    if kind == "[agent]" {
        emit_event(AgentEvent::Reply {
            turn,
            text: text.to_string(),
            final_reply: true,
        });
        if turn.is_some() {
            return;
        }
    } else if let Some(id) = kind
        .strip_prefix("[tool-result:")
        .and_then(|s| s.strip_suffix(']'))
    {
        emit_event(AgentEvent::ToolFinished {
            turn,
            id: id.to_string(),
            output: text.to_string(),
        });
    }
    for line in text.lines() {
        emit_turn(turn, format!("{kind} {line}"));
    }
    if text.is_empty() {
        emit_turn(turn, kind.to_string());
    }
}

fn emit_acknowledgement(turn: Option<u64>, text: &str) {
    emit_turn(turn, format!("[status] working {text}"));
}

fn emit_event(event: AgentEvent) {
    let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let turn_id = event_turn(&event).map(|turn| turn.to_string());
    let tool_call_id = match &event {
        AgentEvent::ToolStarted { id, .. } | AgentEvent::ToolFinished { id, .. } => {
            Some(id.clone())
        }
        _ => None,
    };
    let task_id = match &event {
        AgentEvent::WorkerCompleted { worker_id, .. } => Some(worker_id.clone()),
        _ => None,
    };
    let fallback = EventContext {
        session_id: format!("foreground-{}", std::process::id()),
        conversation_id: None,
        actor: Actor::System,
    };
    let context = EVENT_CONTEXT.get().unwrap_or(&fallback);
    let envelope = EventEnvelope {
        event_id: sequence,
        session_id: context.session_id.clone(),
        conversation_id: context.conversation_id.clone(),
        turn_id,
        task_id,
        parent_task_id: None,
        tool_call_id,
        actor: context.actor.clone(),
        sequence,
        occurred_at_ms: unix_now_ms(),
        kind: event,
    };
    if let Ok(data) = serde_json::to_string(&envelope) {
        println!("{data}");
    }
}

fn init_event_context(_role: AgentRole, agent_id: Option<&str>) {
    let session_id = agent_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("conversation-{}", std::process::id()));
    let actor = Actor::Foreground;
    let conversation_id = Some(session_id.clone());
    let _ = EVENT_CONTEXT.set(EventContext {
        session_id,
        conversation_id,
        actor,
    });
}

fn event_turn(event: &AgentEvent) -> Option<u64> {
    match event {
        AgentEvent::Usage { turn, .. }
        | AgentEvent::Status { turn, .. }
        | AgentEvent::Reply { turn, .. }
        | AgentEvent::ReplyDelta { turn, .. }
        | AgentEvent::ToolStarted { turn, .. }
        | AgentEvent::ToolFinished { turn, .. }
        | AgentEvent::WorkerStarted { turn, .. }
        | AgentEvent::Error { turn, .. } => *turn,
        AgentEvent::Timing { turn, .. } => Some(*turn),
        AgentEvent::WorkerCompleted { .. } | AgentEvent::WorkerReleaseRequested { .. } => None,
    }
}

fn decode_event(data: &str) -> Option<AgentEvent> {
    serde_json::from_str::<EventEnvelope>(data)
        .map(|envelope| envelope.kind)
        .or_else(|_| serde_json::from_str::<AgentEvent>(data))
        .ok()
}

fn decode_evidence(data: &str) -> Option<EvidenceRecord> {
    serde_json::from_str::<EventEnvelope>(data)
        .map(EvidenceRecord::Correlated)
        .or_else(|_| serde_json::from_str::<AgentEvent>(data).map(EvidenceRecord::Legacy))
        .ok()
}

fn decode_chat_input(line: &str, role: AgentRole) -> ChatInput {
    if role == AgentRole::Conversation {
        if let Ok(envelope) = serde_json::from_str::<InteractionCommandEnvelope>(line) {
            if envelope.metadata.protocol_version != tachyon_api::INTERACTION_PROTOCOL_VERSION {
                return ChatInput::Ignore;
            }
            return match envelope.command {
                InteractionCommand::AcceptUserTurn { text } => {
                    let text = text.trim().to_string();
                    if text.is_empty() {
                        ChatInput::Ignore
                    } else {
                        ChatInput::User(text)
                    }
                }
                InteractionCommand::PublishBackgroundUpdate { event } => {
                    ChatInput::Evidence(EvidenceRecord::Correlated(event))
                }
                InteractionCommand::BeginConversation { .. }
                | InteractionCommand::CancelConversation { .. }
                | InteractionCommand::NotifyUser { .. } => ChatInput::Ignore,
            };
        }
    }
    let text = line.trim().to_string();
    if text.is_empty() {
        ChatInput::Ignore
    } else if let Some(data) = text.strip_prefix("[daemon:evidence] ") {
        decode_evidence(data)
            .map(ChatInput::Evidence)
            .unwrap_or(ChatInput::Ignore)
    } else {
        ChatInput::User(text)
    }
}

fn same_evidence(left: &EvidenceRecord, right: &EvidenceRecord) -> bool {
    match (left, right) {
        (EvidenceRecord::Correlated(left), EvidenceRecord::Correlated(right)) => {
            left.session_id == right.session_id && left.event_id == right.event_id
        }
        _ => serde_json::to_string(left).ok() == serde_json::to_string(right).ok(),
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Run the agent loop until the model stops calling tools.
async fn loop_until_done(
    model: &Model,
    conversation: &mut Vec<ChatMessage>,
    role: AgentRole,
    turn: Option<u64>,
    tools_enabled: bool,
    force_delegation: bool,
    mut answer_from_dependency: bool,
    stream_reply: bool,
    accepted_at: Option<std::time::Instant>,
) -> Result<Turn, String> {
    let tools = tools_enabled.then(|| role.tools(force_delegation));
    let max_iter = 8;
    let mut delegation_used = false;
    // Loop guard: if the model requests the exact same tool call repeatedly
    // (same tool + same arguments), it is stuck re-verifying. Stop and tell it.
    let mut last_sig: Option<String> = None;
    let mut repeat_count = 0usize;
    let mut usage = TokenUsage::default();
    let mut acknowledgement_sent = false;
    let mut first_visible_emitted = false;

    for iteration in 0..max_iter {
        // Only tool-free completions can be published as they arrive. Text from
        // a completion that may call tools is private planning until routing is
        // known.
        let mut relay = |delta: &str| {
            if stream_reply && !delta.is_empty() {
                if !first_visible_emitted && delta.trim().is_empty() {
                    return;
                }
                if !first_visible_emitted {
                    first_visible_emitted = true;
                    if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
                        emit_event(AgentEvent::Timing {
                            turn,
                            stage: "first_visible".into(),
                            elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                        });
                    }
                }
                emit_event(AgentEvent::ReplyDelta {
                    turn,
                    text: delta.to_string(),
                });
            }
        };
        if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
            emit_event(AgentEvent::Timing {
                turn,
                stage: format!("model_request_{}_started", iteration + 1),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
        }
        let completion = if role == AgentRole::Conversation && answer_from_dependency {
            answer_from_dependency = false;
            model.chat(conversation, None, &mut relay).await
        } else if role == AgentRole::Conversation && tools_enabled && !delegation_used {
            chat_requiring_decision(
                model,
                conversation,
                tools.as_deref().expect("conversation tools are enabled"),
                &mut relay,
            )
            .await
        } else if role == AgentRole::Conversation && delegation_used {
            model.chat(conversation, None, &mut relay).await
        } else {
            model.chat(conversation, tools.as_deref(), &mut relay).await
        };
        if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
            emit_event(AgentEvent::Timing {
                turn,
                stage: format!("model_request_{}_completed", iteration + 1),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
        }
        let completion = completion.map_err(|e| e.to_string())?;
        drop(relay);
        usage += completion.usage;
        let text_out = completion.text.trim().to_string();
        let mut tool_calls = completion.tool_calls.clone();
        if role == AgentRole::Conversation
            && direct_response(&tool_calls).is_some_and(|response| response.contains("DSML"))
        {
            tool_calls = dsml_delegation_call(&tool_calls, turn)
                .or_else(|| fallback_delegation_call(conversation, turn))
                .into_iter()
                .collect();
        }
        let has_delegation = tool_calls
            .iter()
            .any(|call| matches!(call.name.as_str(), "spawn_agent" | "spawn_agents"));
        let fallback_delegation =
            role == AgentRole::Conversation && force_delegation && !has_delegation;
        if fallback_delegation {
            tool_calls.clear();
            if let Some(call) = fallback_delegation_call(conversation, turn) {
                tool_calls.push(call);
            }
        }
        let has_tool = !tool_calls.is_empty();

        // A "thinking" model (DeepSeek) can stream reasoning and end with
        // empty visible content. If there's genuinely nothing — no text AND
        // no tool call — report it instead of appending a blank assistant
        // message that poisons the next turn ("message came through empty").
        if !has_tool && text_out.is_empty() {
            return Err("model returned an empty response (no text, no tool call). \
                        This is usually a provider glitch — try again."
                .into());
        }

        // A truncated reply ("finish_reason=stop_length/max_tokens") risks a
        // half-answer being trusted. Warn but still use it.
        if let Some(fr) = &completion.finish_reason {
            if fr != "stop" && fr != "tool_calls" {
                emit_turn(
                    turn,
                    format!(
                        "[foreground:error] model text possibly truncated (finish_reason={fr})"
                    ),
                );
            }
        }

        if fallback_delegation {
            conversation.push(ChatMessage {
                role: Role::Assistant,
                content: tool_calls.iter().cloned().map(Content::ToolCall).collect(),
            });
        } else {
            conversation.push(completion.to_message());
        }

        if !has_tool {
            return Ok(Turn::Done(text_out, usage));
        }

        if role == AgentRole::Conversation {
            if let Some(response) = direct_response(&tool_calls) {
                return Ok(Turn::Done(response, usage));
            }
        }
        if role == AgentRole::Conversation && !acknowledgement_sent && !fallback_delegation {
            acknowledgement_sent = true;
            if let Some(acknowledgement) = usable_acknowledgement(&text_out) {
                emit_acknowledgement(turn, &acknowledgement);
            }
        }
        for tc in &tool_calls {
            let sig = format!("{}|{}", tc.name, tc.arguments.trim());
            if Some(&sig) == last_sig.as_ref() {
                repeat_count += 1;
            } else {
                repeat_count = 0;
                last_sig = Some(sig.clone());
            }

            emit_turn(
                turn,
                format!("[tool:{}] {} {}", tc.id, tc.name, tc.arguments),
            );

            if repeat_count >= 3 {
                emit_turn(
                    turn,
                    format!(
                        "[foreground:error] loop detected: repeated identical tool call `{sig}`"
                    ),
                );
                return Ok(Turn::Done(
                    "I kept repeating the same command and detected a loop; I'm stopping here."
                        .into(),
                    usage,
                ));
            }
        }

        // Independent tool calls, especially ephemeral workers, should run in
        // parallel. Preserve the model's original call order in the results.
        let mut delegation_in_this_batch = false;
        let mut tool_jobs = Vec::with_capacity(tool_calls.len());
        for tc in &tool_calls {
            let is_delegation = matches!(tc.name.as_str(), "spawn_agent" | "spawn_agents");
            let allowed = !delegation_used && !delegation_in_this_batch;
            if is_delegation && allowed {
                delegation_in_this_batch = true;
            }
            tool_jobs.push((tc, allowed));
        }
        let outputs_future = async {
            join_all(
                tool_jobs
                    .iter()
                    .map(|(tc, allowed)| run_tool(tc, role, *allowed, turn)),
            )
            .await
        };
        let outputs = outputs_future.await;
        if delegation_in_this_batch {
            if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
                emit_event(AgentEvent::Timing {
                    turn,
                    stage: "evidence_ready".into(),
                    elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                });
            }
        }
        delegation_used |= delegation_in_this_batch;
        let mut results: Vec<ChatMessage> = Vec::new();
        for (tc, out) in tool_calls.iter().zip(outputs) {
            emit_turn_block(
                turn,
                &format!("[tool-result:{}]", tc.id),
                &truncate(&out, 600),
            );
            results.push(ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: tc.id.clone(),
                    output: out,
                }],
            });
        }
        conversation.extend(results);
        if role == AgentRole::Conversation && delegation_used {
            if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
                emit_event(AgentEvent::Timing {
                    turn,
                    stage: "synthesis_started".into(),
                    elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                });
            }
            let mut synthesis_relay = |delta: &str| {
                if stream_reply && !delta.is_empty() {
                    if !first_visible_emitted {
                        first_visible_emitted = true;
                        if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
                            emit_event(AgentEvent::Timing {
                                turn,
                                stage: "first_visible".into(),
                                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                            });
                        }
                    }
                    emit_event(AgentEvent::ReplyDelta {
                        turn,
                        text: delta.to_string(),
                    });
                }
            };
            let answer =
                match synthesize_spoken_response(model, conversation, &mut synthesis_relay).await {
                    Ok((answer, synthesis_usage)) => {
                        usage += synthesis_usage;
                        answer
                    }
                    Err(_) => compose_worker_response(conversation),
                };
            if let (Some(turn), Some(accepted_at)) = (turn, accepted_at) {
                emit_event(AgentEvent::Timing {
                    turn,
                    stage: "synthesis_completed".into(),
                    elapsed_ms: accepted_at.elapsed().as_millis() as u64,
                });
            }
            return Ok(Turn::Done(answer, usage));
        }
    }
    Ok(Turn::MaxIterations(usage))
}

fn direct_response(tool_calls: &[ToolCall]) -> Option<String> {
    if tool_calls.len() != 1 {
        return None;
    }
    let call = tool_calls.iter().find(|call| call.name == "respond")?;
    let response = arg(&call.arguments, "response");
    (!response.trim().is_empty()).then_some(response)
}

fn dsml_delegation_call(tool_calls: &[ToolCall], turn: Option<u64>) -> Option<ToolCall> {
    let response = direct_response(tool_calls)?;
    let name = response.split_once("invoke name=\"")?.1.split_once('"')?.0;
    let arguments = match name {
        "spawn_agent" => {
            let task = dsml_parameter(&response, "prompt")
                .or_else(|| dsml_parameter(&response, "description"))?;
            serde_json::json!({
                "task": task,
                "purpose": dsml_parameter(&response, "description").unwrap_or("fresh work"),
                "lifetime_class": "long"
            })
        }
        "spawn_agents" => {
            let agents =
                serde_json::from_str::<serde_json::Value>(dsml_parameter(&response, "agents")?)
                    .ok()?;
            let tasks = agents
                .as_array()?
                .iter()
                .filter_map(|agent| {
                    agent
                        .get("prompt")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| agent.get("description").and_then(serde_json::Value::as_str))
                })
                .collect::<Vec<_>>();
            if tasks.is_empty() {
                return None;
            }
            serde_json::json!({ "tasks": tasks, "lifetime_class": "long" })
        }
        _ => return None,
    };
    Some(ToolCall {
        id: format!("dsml-recovered-{}", turn.unwrap_or_default()),
        name: name.into(),
        arguments: arguments.to_string(),
    })
}

fn dsml_parameter<'a>(response: &'a str, name: &str) -> Option<&'a str> {
    let marker = format!("parameter name=\"{name}\"");
    let parameter = response.split_once(&marker)?.1;
    let body = parameter.split_once('>')?.1;
    Some(body.split_once("</")?.0.trim())
}

fn record_final_answer(conversation: &mut Vec<ChatMessage>, answer: &str) {
    let already_recorded = conversation.last().is_some_and(|message| {
        message.role == Role::Assistant
            && message
                .content
                .iter()
                .all(|content| matches!(content, Content::Text(_)))
            && message.plain().trim() == answer.trim()
    });
    if !already_recorded {
        conversation.push(ChatMessage::new(Role::Assistant, answer));
    }
}

fn fallback_delegation_call(conversation: &[ChatMessage], turn: Option<u64>) -> Option<ToolCall> {
    let task = conversation
        .iter()
        .rev()
        .find(|message| message.role == Role::User)
        .map(ChatMessage::plain)
        .filter(|task| !task.trim().is_empty())?;
    Some(ToolCall {
        id: format!("fallback-{}", turn.unwrap_or_default()),
        name: "spawn_agent".into(),
        arguments: serde_json::json!({
            "task": task,
            "lifetime_class": "long",
            "purpose": "fresh work"
        })
        .to_string(),
    })
}

fn compose_worker_response(conversation: &[ChatMessage]) -> String {
    let start = conversation
        .iter()
        .rposition(|message| message.role == Role::User)
        .unwrap_or(0);
    let outputs = conversation[start..]
        .iter()
        .filter(|message| message.role == Role::Tool)
        .flat_map(|message| {
            message.content.iter().filter_map(|content| match content {
                Content::ToolResult { output, .. } => Some(output.clone()),
                _ => None,
            })
        })
        .filter(|output| !output.trim().is_empty())
        .collect::<Vec<_>>();
    if outputs.is_empty() {
        "I completed the delegated work, but it returned no usable result.".into()
    } else {
        outputs.join("\n\n")
    }
}

fn usable_acknowledgement(text: &str) -> Option<String> {
    let text = text.trim().replace('\n', " ");
    let lower = text.to_ascii_lowercase();
    if text.is_empty()
        || text.len() > 240
        || [
            "tool",
            "worker",
            "delegat",
            "agent",
            "reasoning",
            "prompt",
            "internal",
        ]
        .iter()
        .any(|term| lower.contains(term))
    {
        None
    } else {
        Some(text)
    }
}

async fn run_tool(
    tc: &ToolCall,
    role: AgentRole,
    delegation_allowed: bool,
    turn: Option<u64>,
) -> String {
    if !role.allows_tool(&tc.name) {
        return format!("{} is not available to the {role:?} role", tc.name);
    }
    match tc.name.as_str() {
        "spawn_agent" => {
            let task = arg(&tc.arguments, "task");
            if !delegation_allowed {
                return "Delegation has already been used for this turn. Synthesize an answer from the worker results already received; do not spawn another worker.".into();
            }
            let cwd_arg = arg(&tc.arguments, "cwd");
            let cwd = (!cwd_arg.is_empty()).then_some(cwd_arg);
            let value =
                serde_json::from_str::<serde_json::Value>(&tc.arguments).unwrap_or_default();
            let lifetime_class = value
                .get("lifetime_class")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default();
            let purpose = value
                .get("purpose")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let correlation = delegation_correlation(tc, turn, None);
            match tokio::task::spawn_blocking(move || {
                spawn_via_daemon(&task, cwd, lifetime_class, purpose, correlation)
            })
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => format!("worker spawn failed: {error}"),
                Err(error) => format!("worker spawn task failed: {error}"),
            }
        }
        "spawn_agents" => {
            if !delegation_allowed {
                return "Delegation has already been used for this turn. Synthesize an answer from the worker results already received; do not spawn another worker.".into();
            }
            let tasks = match serde_json::from_str::<serde_json::Value>(&tc.arguments)
                .ok()
                .and_then(|value| value.get("tasks").cloned())
                .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
            {
                Some(tasks) if !tasks.is_empty() => tasks,
                _ => return "spawn_agents requires a non-empty tasks array".into(),
            };
            let lifetime_class = serde_json::from_str::<serde_json::Value>(&tc.arguments)
                .ok()
                .and_then(|value| value.get("lifetime_class").cloned())
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default();
            let jobs = tasks.into_iter().enumerate().map(|(index, task)| {
                let lifetime_class = lifetime_class;
                let correlation = delegation_correlation(tc, turn, Some(index));
                tokio::task::spawn_blocking(move || {
                    spawn_via_daemon(&task, None, lifetime_class, String::new(), correlation)
                })
            });
            let results = join_all(jobs).await;
            results
                .into_iter()
                .map(|result| match result {
                    Ok(Ok(answer)) => answer,
                    Ok(Err(error)) => format!("worker failed: {error}"),
                    Err(error) => format!("worker task failed: {error}"),
                })
                .collect::<Vec<_>>()
                .join("\n\n")
        }
        other => format!("unknown tool: {other}"),
    }
}

struct DelegationCorrelation {
    logical_task_id: String,
    origin_turn_id: Option<String>,
    parent_task_id: Option<String>,
    tool_call_id: Option<String>,
}

fn delegation_correlation(
    tool_call: &ToolCall,
    turn: Option<u64>,
    batch_index: Option<usize>,
) -> DelegationCorrelation {
    let session_id = EVENT_CONTEXT
        .get()
        .map(|context| context.session_id.as_str())
        .unwrap_or("foreground");
    let origin_turn_id = turn.map(|turn| turn.to_string());
    let suffix = batch_index
        .map(|index| format!("-{index}"))
        .unwrap_or_default();
    DelegationCorrelation {
        logical_task_id: format!(
            "{session_id}:{}:{}{suffix}",
            origin_turn_id.as_deref().unwrap_or("task"),
            tool_call.id
        ),
        origin_turn_id,
        parent_task_id: None,
        tool_call_id: (!tool_call.id.is_empty()).then(|| tool_call.id.clone()),
    }
}

/// Ask Tachyond to create an ephemeral worker and wait for its terminal result.
/// The worker's live output remains available to TUI subscribers by its ID.
fn spawn_via_daemon(
    task: &str,
    cwd: Option<String>,
    lifetime_class: LifetimeClass,
    purpose: String,
    correlation: DelegationCorrelation,
) -> Result<String, String> {
    let origin_turn = correlation
        .origin_turn_id
        .as_deref()
        .and_then(|turn| turn.parse::<u64>().ok());
    let socket = tachyon_util::daemon::socket_path();
    let mut client = Connection::connect(&socket).map_err(|e| e.to_string())?;
    let response = client
        .exchange(&ApiRequest::BackgroundDelegate {
            task: task.to_string(),
            cwd,
            depends_on: Vec::new(),
            lifetime_class,
            purpose,
            logical_task_id: Some(correlation.logical_task_id),
            origin_turn_id: correlation.origin_turn_id,
            parent_task_id: correlation.parent_task_id,
            tool_call_id: correlation.tool_call_id,
        })
        .map_err(|e| e.to_string())?;
    let id = match response {
        ApiResponse::Agent { info } => info.id,
        ApiResponse::Error { message, .. } => return Err(message),
        other => return Err(format!("unexpected spawn response: {other:?}")),
    };
    emit_event(AgentEvent::WorkerStarted {
        turn: origin_turn,
        worker_id: id.clone(),
        objective: task.to_string(),
    });

    let mut stream = Connection::connect(&socket).map_err(|e| e.to_string())?;
    stream
        .send(&ApiRequest::AgentSubscribe { id: id.clone() })
        .map_err(|e| e.to_string())?;
    let deadline = std::time::Instant::now() + worker_result_timeout();
    let mut result: Option<String> = None;
    loop {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| format!("worker {id} timed out waiting for a result"))?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|e| e.to_string())?;
        match stream.recv().map_err(|error| {
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) {
                format!("worker {id} timed out waiting for a result")
            } else {
                error.to_string()
            }
        })? {
            ApiResponse::Event {
                stream: EventStream::Stdout,
                data,
            } => {
                if let Some(AgentEvent::WorkerCompleted { result: answer, .. }) =
                    decode_event(&data)
                {
                    result = Some(answer);
                    break;
                }
            }
            ApiResponse::Event {
                stream: EventStream::Exit,
                data,
            } => {
                if result.is_none() {
                    result = Some(data);
                }
                break;
            }
            ApiResponse::Error { message, .. } => return Err(message),
            _ => {}
        }
    }
    let answer = result.unwrap_or_else(|| "worker completed without a response".into());
    println!("[worker:result] {id} {answer}");
    Ok(answer)
}

fn worker_result_timeout() -> std::time::Duration {
    let seconds = std::env::var("TACHYON_WORKER_RESULT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60)
        .max(1);
    std::time::Duration::from_secs(seconds)
}

fn arg(args: &str, key: &str) -> String {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(args) {
        if let Some(v) = json.get(key) {
            if let serde_json::Value::String(s) = v {
                return s.clone();
            }
        }
    }
    String::new()
}

fn parse_args() -> (Option<String>, Option<String>, bool) {
    let args: Vec<String> = std::env::args().collect();
    let mut cwd: Option<String> = None;
    let mut agent_id: Option<String> = None;
    let mut new_session = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            // Accepted for compatibility with the pre-split daemon command.
            "--chat" => i += 1,
            "--cwd" => {
                if i + 1 < args.len() {
                    cwd = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--agent-id" => {
                if i + 1 < args.len() {
                    agent_id = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--role" => i += usize::from(i + 1 < args.len()) + 1,
            "--new-session" => {
                new_session = true;
                i += 1;
            }
            _ => i += 1,
        }
    }
    (cwd, agent_id, new_session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::FOREGROUND_ID;

    fn interaction_metadata() -> tachyon_api::InteractionMetadata {
        tachyon_api::InteractionMetadata::new("command-1", "turn-1", FOREGROUND_ID, 1)
    }

    #[test]
    fn conversation_accepts_versioned_multiline_user_turns() {
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::AcceptUserTurn {
                text: "first line\nsecond line".into(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::User(text) => assert_eq!(text, "first line\nsecond line"),
            _ => panic!("expected a user turn"),
        }
    }

    #[test]
    fn conversation_accepts_correlated_background_updates() {
        let event = EventEnvelope {
            event_id: 7,
            session_id: "worker-1".into(),
            conversation_id: Some(FOREGROUND_ID.into()),
            turn_id: Some("1".into()),
            task_id: Some("task-1".into()),
            parent_task_id: None,
            tool_call_id: Some("call-1".into()),
            actor: Actor::Worker {
                id: "worker-1".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: AgentEvent::WorkerCompleted {
                worker_id: "worker-1".into(),
                objective: "weather".into(),
                result: "rain".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        };
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::PublishBackgroundUpdate {
                event: event.clone(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::Evidence(EvidenceRecord::Correlated(decoded)) => {
                assert_eq!(decoded, event)
            }
            _ => panic!("expected correlated evidence"),
        }
    }

    #[test]
    fn conversation_checkpoint_round_trips_messages() {
        let path = std::env::temp_dir().join(format!(
            "tachyon-conversation-checkpoint-{}.json",
            std::process::id()
        ));
        let conversation = ConversationState {
            messages: vec![
                ChatMessage::new(Role::System, "system"),
                ChatMessage::new(Role::User, "remember this"),
            ],
            evidence: Vec::new(),
            pending: BTreeMap::new(),
            next_commit: 3,
        };
        write_checkpoint(&path, &checkpoint_snapshot(&conversation));
        let restored = load_checkpoint(&path).expect("checkpoint should load");
        assert_eq!(restored.next_commit, 3);
        assert_eq!(restored.messages.len(), 2);
        assert_eq!(restored.messages[1].plain(), "remember this");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn commit_cursor_advances_only_through_contiguous_terminal_turns() {
        let mut conversation = ConversationState {
            messages: Vec::new(),
            evidence: Vec::new(),
            pending: BTreeMap::from([
                (2, vec![ChatMessage::new(Role::Assistant, "second")]),
                (3, vec![ChatMessage::new(Role::Assistant, "third")]),
            ]),
            next_commit: 1,
        };
        commit_ready_turns(&mut conversation);
        assert_eq!(conversation.next_commit, 1);
        assert!(conversation.messages.is_empty());

        conversation
            .pending
            .insert(1, vec![ChatMessage::new(Role::Assistant, "first")]);
        commit_ready_turns(&mut conversation);
        assert_eq!(conversation.next_commit, 4);
        assert_eq!(
            conversation
                .messages
                .iter()
                .map(ChatMessage::plain)
                .collect::<Vec<_>>(),
            ["first", "second", "third"]
        );
    }

    #[test]
    fn correlated_evidence_identifies_its_origin_turn() {
        let record = EvidenceRecord::Correlated(EventEnvelope {
            event_id: 7,
            session_id: "worker-1".into(),
            conversation_id: None,
            turn_id: Some("4".into()),
            task_id: Some("task-4".into()),
            parent_task_id: None,
            tool_call_id: Some("call-4".into()),
            actor: Actor::Worker {
                id: "worker-1".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: AgentEvent::WorkerCompleted {
                worker_id: "worker-1".into(),
                objective: "objective".into(),
                result: "result".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        });

        assert_eq!(record.origin_turn(), Some(4));
        assert!(matches!(
            record.event(),
            AgentEvent::WorkerCompleted { worker_id, .. } if worker_id == "worker-1"
        ));
        assert!(evidence_relevant_to_follow_up(
            &record,
            4,
            "follow up on the objective"
        ));
        assert!(!evidence_relevant_to_follow_up(
            &record,
            3,
            "follow up on the objective"
        ));
        assert!(!evidence_relevant_to_follow_up(
            &record,
            4,
            "unrelated subject"
        ));
    }

    #[test]
    fn visible_final_answer_is_recorded_exactly_once() {
        let mut conversation = vec![ChatMessage::new(Role::User, "hello")];
        record_final_answer(&mut conversation, "Hi.");
        record_final_answer(&mut conversation, "Hi.");
        assert_eq!(conversation.len(), 2);
        assert_eq!(conversation[1].role, Role::Assistant);
        assert_eq!(conversation[1].plain(), "Hi.");
    }

    #[test]
    fn worker_response_uses_only_current_turn_results() {
        let conversation = vec![
            ChatMessage::new(Role::User, "earlier request"),
            ChatMessage::new(Role::Assistant, "earlier answer"),
            ChatMessage::new(Role::User, "current request"),
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "tool-1".into(),
                    output: "current result".into(),
                }],
            },
        ];
        assert_eq!(compose_worker_response(&conversation), "current result");
    }

    #[test]
    fn worker_response_preserves_all_parallel_results() {
        let conversation = vec![
            ChatMessage::new(Role::User, "current request"),
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "batch".into(),
                    output: "first result\n\nsecond result\n\nthird result".into(),
                }],
            },
        ];
        assert_eq!(
            compose_worker_response(&conversation),
            "first result\n\nsecond result\n\nthird result"
        );
    }

    #[test]
    fn acknowledgement_filter_rejects_internal_planning() {
        assert_eq!(
            usable_acknowledgement("I will ask a worker to inspect this."),
            None
        );
        assert_eq!(
            usable_acknowledgement("I am checking the details now."),
            Some("I am checking the details now.".into())
        );
    }

    #[test]
    fn direct_response_requires_one_explicit_respond_call() {
        let response = ToolCall {
            id: "reply-1".into(),
            name: "respond".into(),
            arguments: r#"{"response":"Hello Freddie."}"#.into(),
        };
        assert_eq!(
            direct_response(&[response.clone()]),
            Some("Hello Freddie.".into())
        );

        let delegation = ToolCall {
            id: "spawn-1".into(),
            name: "spawn_agent".into(),
            arguments: r#"{"task":"get current information"}"#.into(),
        };
        assert_eq!(direct_response(&[response, delegation]), None);
    }

    #[test]
    fn recovers_batched_delegation_from_textual_dsml() {
        let response = r#"I’ll verify those. <｜DSML｜tool_calls><｜DSML｜invoke name="spawn_agents"><｜DSML｜parameter name="agents">[{"description":"London","prompt":"Get current London weather"},{"description":"Tokyo","prompt":"Get current Tokyo weather"}]</｜DSML｜parameter></｜DSML｜invoke></｜DSML｜tool_calls>"#;
        let call = ToolCall {
            id: "respond-1".into(),
            name: "respond".into(),
            arguments: serde_json::json!({ "response": response }).to_string(),
        };
        let recovered = dsml_delegation_call(&[call], Some(7)).expect("delegation");
        assert_eq!(recovered.name, "spawn_agents");
        let arguments: serde_json::Value = serde_json::from_str(&recovered.arguments).unwrap();
        assert_eq!(arguments["tasks"].as_array().unwrap().len(), 2);
        assert_eq!(arguments["tasks"][1], "Get current Tokyo weather");
    }

    #[test]
    fn fallback_delegation_preserves_the_user_objective() {
        for objective in [
            "inspect the deployment and verify rollback readiness",
            "compare prerelease package APIs",
            "summarize the report with emphasis on security findings",
            "calculate a checksum for the generated artifact",
        ] {
            let conversation = vec![
                ChatMessage::new(Role::System, "system"),
                ChatMessage::new(Role::User, objective),
            ];
            let call = fallback_delegation_call(&conversation, Some(2)).unwrap();
            assert_eq!(call.id, "fallback-2");
            assert_eq!(call.name, "spawn_agent");
            let arguments: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
            assert_eq!(arguments["task"], objective);
            assert_eq!(arguments["lifetime_class"], "long");
        }
    }

    #[test]
    fn evidence_matching_is_objective_agnostic() {
        assert!(evidence_matches(
            "did the deployment finish?",
            "verify the deployment status"
        ));
        assert!(evidence_matches(
            "what changed in the package?",
            "inspect the package changes"
        ));
        assert!(!evidence_matches(
            "summarize the database migration",
            "check the frontend bundle size"
        ));
    }

    #[test]
    fn conversation_checkpoint_path_is_stable() {
        let workspace = PathBuf::from("/tmp/tachyon-foreground-checkpoint");
        let conversation = chat_checkpoint_path(&workspace, AgentRole::Conversation);
        assert!(conversation.ends_with(".tachyon/conversation.json"));
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut cp = s.chars();
        let cut: String = cp.by_ref().take(n).collect();
        format!("{cut}…")
    }
}
