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
    assess_answerability, bounded_policy_text, chat_with_delegation, classify, policy_context,
    synthesize_spoken_response,
};
use tachyon_api::transport::Connection;
use tachyon_api::types::{
    Actor, AgentEvent, ApiRequest, ApiResponse, EventEnvelope, EventStream, LifetimeClass,
    WorkOutcome,
};
use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionEvent, InteractionEventEnvelope,
    InteractionIntent, InteractionMetadata, RecoveredSession, TaskIntent, FOREGROUND_ID,
};
use tachyon_model::{ChatMessage, Content, Model, Role, TokenUsage, ToolCall, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    follow_up_execution_policy, publication_requires_dependency, Answerability, InteractionDecision,
};
use tokio::io::AsyncBufReadExt;

const QUEUED_TURN_ACKNOWLEDGEMENT: &str =
    "Let me pull that together and I'll get back to you shortly.";
const CONCURRENT_TURN_ACKNOWLEDGEMENT: &str =
    "Absolutely - I'll handle that while I keep the other request moving.";
const DEFAULT_ANSWERABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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

    fn tools(self, _force_delegation: bool) -> Vec<ToolSpec> {
        tachyon_orchestrator::conversation::CAPABILITIES
            .iter()
            .map(|capability| match capability {
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
    User {
        text: String,
        metadata: InteractionMetadata,
    },
    Evidence(EvidenceRecord),
    Recovery(Vec<RecoveredSession>),
    Notification {
        text: String,
        metadata: InteractionMetadata,
    },
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

fn is_completed_evidence(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::WorkerCompleted { .. }
            | AgentEvent::WorkResult {
                result: tachyon_api::WorkResult {
                    outcome: WorkOutcome::Completed { .. },
                    ..
                }
            }
    )
}

struct EventContext {
    session_id: String,
    conversation_id: Option<String>,
    actor: Actor,
}

static EVENT_CONTEXT: OnceLock<EventContext> = OnceLock::new();
static EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static LEGACY_INPUT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
        InteractionMetadata,
        bool,
        InteractionDecision,
        Option<String>,
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
        while let Some((
            turn,
            text,
            metadata,
            queued,
            decision,
            acknowledgement,
            routing_usage,
            accepted_at,
        )) = turn_rx.recv().await
        {
            let args = (
                turn,
                text,
                metadata,
                queued,
                decision,
                acknowledgement,
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
        let (text, mut metadata) = match decode_chat_input(&line, role) {
            ChatInput::User { text, metadata } => (text, metadata),
            ChatInput::Evidence(record) => {
                if is_completed_evidence(record.event()) {
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
            ChatInput::Recovery(sessions) => {
                if !sessions.is_empty() {
                    let summary = sessions
                        .iter()
                        .map(|session| format!("{} ({})", session.description, session.state))
                        .collect::<Vec<_>>()
                        .join(", ");
                    emit_interaction_event(
                        &synthetic_interaction_metadata(None),
                        InteractionEvent::UserVisibleNotificationPublished {
                            text: format!("Recovered persistent work: {summary}"),
                        },
                    );
                }
                continue;
            }
            ChatInput::Notification { text, metadata } => {
                emit_interaction_event(
                    &metadata,
                    InteractionEvent::UserVisibleNotificationPublished { text },
                );
                continue;
            }
            ChatInput::Ignore => continue,
        };
        let turn = next_turn.fetch_add(1, Ordering::Relaxed);
        metadata.turn_id = Some(turn.to_string());
        emit_interaction_event(
            &metadata,
            InteractionEvent::UserTurnAccepted { text: text.clone() },
        );
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
                    metadata,
                    false,
                    InteractionDecision::WaitForActiveTurn,
                    None,
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

        let fallback_decision = fallback_interaction_decision(&classifier_context, &text);
        emit_turn(Some(turn), "[status] routing alongside active work".into());
        if publication_requires_dependency(true, fallback_decision) {
            emit_queued_turn(turn, None);
        } else {
            emit_turn(
                Some(turn),
                format!("[status] working {CONCURRENT_TURN_ACKNOWLEDGEMENT}"),
            );
        }
        let classifier_tx = turn_tx.clone();
        let classifier_model = model.clone();
        let classifier_active_turns = Arc::clone(&active_turns);
        let classifier_metadata = metadata.clone();
        tokio::spawn(async move {
            let (decision, acknowledgement, usage) = if let Some(model) = classifier_model {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    classify(&model, &classifier_context, &text),
                )
                .await
                {
                    Ok(Ok((decision, Some(acknowledgement), usage))) => {
                        (decision, Some(acknowledgement), usage)
                    }
                    Ok(Ok((_, None, usage))) => (fallback_decision, None, usage),
                    Ok(Err(_)) | Err(_) => (fallback_decision, None, TokenUsage::default()),
                }
            } else {
                (fallback_decision, None, TokenUsage::default())
            };
            emit_event(AgentEvent::Timing {
                turn,
                stage: "routing".into(),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
            if classifier_tx
                .send((
                    turn,
                    text,
                    classifier_metadata,
                    true,
                    decision,
                    acknowledgement,
                    usage,
                    accepted_at,
                ))
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
        InteractionMetadata,
        bool,
        InteractionDecision,
        Option<String>,
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
        metadata,
        queued,
        decision,
        acknowledgement,
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
        emit_interaction_event(
            &metadata,
            InteractionEvent::ConversationFinished {
                text: answer.into(),
            },
        );
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
            if let Some(acknowledgement) =
                acknowledgement.as_deref().and_then(usable_acknowledgement)
            {
                emit_queued_turn(turn, Some(&acknowledgement));
            }
            wait_for_context_or_evidence(&conversation, &state_changed, turn, &text).await;
        } else if let Some(acknowledgement) =
            acknowledgement.as_deref().and_then(usable_acknowledgement)
        {
            emit_turn(Some(turn), format!("[status] working {acknowledgement}"));
        }
    }
    emit_event(AgentEvent::Timing {
        turn,
        stage: "ready".into(),
        elapsed_ms: accepted_at.elapsed().as_millis() as u64,
    });
    let active_snapshot = active_turns.lock().unwrap().clone();
    let mut local =
        available_conversation_snapshot(&conversation.lock().unwrap(), &active_snapshot, turn);
    let follow_up_evidence = accepted_follow_up_evidence(
        &conversation.lock().unwrap().evidence,
        turn.saturating_sub(1),
        &text,
    );
    if let Some(evidence) = &follow_up_evidence {
        local.push(ChatMessage::new(Role::User, evidence.clone()));
    }
    let requires_dependency = publication_requires_dependency(queued, decision);
    let has_accepted_evidence = follow_up_evidence.is_some();
    let mut answerability = None;
    if let Some(timeout) = answerability_timeout(has_accepted_evidence) {
        let context = policy_context(&local);
        let started = std::time::Instant::now();
        let (outcome, usage, fallback, timed_out) = match tokio::time::timeout(
            timeout,
            assess_answerability(&model, &context, &text),
        )
        .await
        {
            Ok(Ok((outcome, usage, malformed))) => (outcome, usage, malformed, false),
            Ok(Err(_)) => (
                Answerability::AnswerFromContext,
                TokenUsage::default(),
                true,
                false,
            ),
            Err(_) => (
                Answerability::AnswerFromContext,
                TokenUsage::default(),
                true,
                true,
            ),
        };
        auxiliary_usage += usage;
        answerability = Some(outcome);
        emit_event(answerability_timing(
            turn,
            outcome,
            fallback,
            timed_out,
            started.elapsed(),
        ));
    } else if requires_dependency {
        emit_event(answerability_timing(
            turn,
            Answerability::NeedsNewWork,
            false,
            false,
            std::time::Duration::ZERO,
        ));
    }
    let policy =
        follow_up_execution_policy(requires_dependency, has_accepted_evidence, answerability);

    local.push(ChatMessage::new(Role::User, text.clone()));
    emit_turn(Some(turn), "[status] working".into());
    // Routing controls scheduling only. A separate answerability decision
    // controls whether follow-ups may answer without fresh work.
    let tools_enabled = !policy.answer_from_context;
    let final_answer = match loop_until_done(
        &model,
        &mut local,
        role,
        Some(turn),
        tools_enabled,
        policy.force_delegation,
        policy.answer_from_context,
        true,
        Some(accepted_at),
        Some(&metadata),
    )
    .await
    {
        Ok(Turn::Done(answer, mut usage)) => {
            usage += auxiliary_usage;
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
            emit_interaction_event(
                &metadata,
                InteractionEvent::ConversationFinished {
                    text: answer.clone(),
                },
            );
            emit_event(AgentEvent::Timing {
                turn,
                stage: "completed".into(),
                elapsed_ms: accepted_at.elapsed().as_millis() as u64,
            });
            answer
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
            emit_turn(Some(turn), "[foreground:error] max iterations".into());
            emit_interaction_event(
                &metadata,
                InteractionEvent::ConversationFinished {
                    text: answer.into(),
                },
            );
            answer.into()
        }
        Err(error) => {
            let answer =
                "I couldn't complete that request because the agent encountered an internal error.";
            emit_turn(Some(turn), format!("[foreground:error] {error}"));
            emit_interaction_event(
                &metadata,
                InteractionEvent::ConversationFinished {
                    text: answer.into(),
                },
            );
            answer.into()
        }
    };
    let mut current = conversation.lock().unwrap();
    current
        .pending
        .insert(turn, durable_turn_messages(text, final_answer));
    commit_ready_turns(&mut current);
    let _ = checkpoint_tx.send(checkpoint_snapshot(&current));
    state_changed.notify_waiters();
    mark_turn_inactive(&active_turns, turn);
}

fn mark_turn_inactive(active_turns: &Arc<Mutex<BTreeMap<u64, String>>>, turn: u64) {
    active_turns.lock().unwrap().remove(&turn);
}

fn answerability_timeout(has_accepted_evidence: bool) -> Option<std::time::Duration> {
    has_accepted_evidence.then_some(DEFAULT_ANSWERABILITY_TIMEOUT)
}

fn answerability_timing(
    turn: u64,
    outcome: Answerability,
    fallback: bool,
    timed_out: bool,
    elapsed: std::time::Duration,
) -> AgentEvent {
    let outcome = match outcome {
        Answerability::AnswerFromContext => "answer_from_context",
        Answerability::NeedsNewWork => "needs_new_work",
    };
    AgentEvent::Timing {
        turn,
        stage: format!("answerability outcome={outcome} fallback={fallback} timeout={timed_out}"),
        elapsed_ms: elapsed.as_millis() as u64,
    }
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

fn available_conversation_snapshot(
    conversation: &ConversationState,
    active_turns: &BTreeMap<u64, String>,
    current_turn: u64,
) -> Vec<ChatMessage> {
    let mut messages = conversation.messages.clone();
    for pending in conversation
        .pending
        .range(..current_turn)
        .map(|(_, messages)| messages)
    {
        messages.extend(pending.iter().cloned());
    }
    let active = active_turns
        .range(..current_turn)
        .filter(|(turn, _)| !conversation.pending.contains_key(turn))
        .map(|(turn, request)| {
            format!(
                "Request {turn} (still in progress): {}",
                truncate(request, 400)
            )
        })
        .collect::<Vec<_>>();
    if !active.is_empty() {
        messages.push(ChatMessage::new(
            Role::System,
            format!(
                "Live conversation context. These requests are visible to the user but do not have final answers yet:\n{}",
                active.join("\n")
            ),
        ));
    }
    messages
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
    let objective = match record.event() {
        AgentEvent::WorkerCompleted { objective, .. } => objective,
        AgentEvent::WorkResult {
            result:
                tachyon_api::WorkResult {
                    objective,
                    outcome: WorkOutcome::Completed { .. },
                    ..
                },
        } => objective,
        _ => return false,
    };
    record
        .origin_turn()
        .is_none_or(|origin| origin == prior_turn)
        && evidence_matches(incoming, objective)
}

fn accepted_follow_up_evidence(
    evidence: &[EvidenceRecord],
    prior_turn: u64,
    incoming: &str,
) -> Option<String> {
    let candidates = evidence
        .iter()
        .filter(|record| is_completed_evidence(record.event()))
        .filter(|record| {
            record
                .origin_turn()
                .is_none_or(|origin| origin == prior_turn)
        })
        .collect::<Vec<_>>();
    let matched = candidates
        .iter()
        .copied()
        .filter(|record| evidence_relevant_to_follow_up(record, prior_turn, incoming))
        .collect::<Vec<_>>();
    let selected = if matched.is_empty() {
        candidates
    } else {
        matched
    };
    let relevant = selected
        .into_iter()
        .filter_map(|record| match record.event() {
            AgentEvent::WorkerCompleted {
                worker_id,
                objective,
                result,
                ..
            } => Some(format!(
                "Available background evidence (worker {worker_id}, objective {objective}):\n{result}"
            )),
            AgentEvent::WorkResult {
                result:
                    tachyon_api::WorkResult {
                        work_id,
                        objective,
                        outcome: WorkOutcome::Completed { result, .. },
                        ..
                    },
            } => Some(format!(
                "Available background evidence (work {work_id}, objective {objective}):\n{result}"
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!relevant.is_empty()).then(|| bounded_policy_text(&relevant.join("\n\n")))
}

fn fallback_interaction_decision(active_context: &str, incoming: &str) -> InteractionDecision {
    if evidence_matches(incoming, active_context) {
        InteractionDecision::WaitForActiveTurn
    } else {
        InteractionDecision::AnswerNow
    }
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
                    | "conversation"
                    | "current"
                    | "currently"
                    | "summary"
                    | "summarize"
                    | "recap"
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
        emit_event(status_event(turn, rest));
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

fn status_event(turn: Option<u64>, status: &str) -> AgentEvent {
    let mut parts = status.splitn(2, ' ');
    AgentEvent::Status {
        turn,
        phase: parts.next().unwrap_or("working").to_string(),
        message: parts.next().unwrap_or_default().to_string(),
    }
}

fn emit_queued_turn(turn: u64, acknowledgement: Option<&str>) {
    emit_turn(
        Some(turn),
        format!(
            "[status] queued {}",
            acknowledgement.unwrap_or(QUEUED_TURN_ACKNOWLEDGEMENT)
        ),
    );
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
        AgentEvent::WorkProgress { event } => Some(event.work_id.clone()),
        AgentEvent::WorkResult { result } => Some(result.work_id.clone()),
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

fn emit_interaction_event(metadata: &InteractionMetadata, event: InteractionEvent) {
    let sequence = EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut event_metadata = metadata.clone();
    event_metadata.protocol_version = tachyon_api::INTERACTION_PROTOCOL_VERSION;
    event_metadata.message_id = format!("interaction-event-{sequence}");
    event_metadata.causation_id = Some(metadata.message_id.clone());
    event_metadata.occurred_at_ms = unix_now_ms();
    let envelope = InteractionEventEnvelope {
        metadata: event_metadata,
        event,
    };
    if let Ok(data) = serde_json::to_string(&envelope) {
        println!("{data}");
    }
}

fn synthetic_interaction_metadata(turn: Option<u64>) -> InteractionMetadata {
    let sequence = LEGACY_INPUT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let message_id = format!("legacy-input-{sequence}");
    let conversation_id = EVENT_CONTEXT
        .get()
        .and_then(|context| context.conversation_id.clone())
        .unwrap_or_else(|| FOREGROUND_ID.into());
    let mut metadata =
        InteractionMetadata::new(&message_id, &message_id, conversation_id, unix_now_ms());
    metadata.turn_id = turn.map(|turn| turn.to_string());
    metadata
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
        AgentEvent::WorkerCompleted { .. }
        | AgentEvent::ToolTelemetry { .. }
        | AgentEvent::ArtifactRegistered { .. }
        | AgentEvent::WorkCandidate { .. }
        | AgentEvent::WorkProgress { .. }
        | AgentEvent::WorkResult { .. }
        | AgentEvent::WorkerReleaseRequested { .. } => None,
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
            let metadata = envelope.metadata;
            return match envelope.command {
                InteractionCommand::AcceptUserTurn { text } => {
                    let text = text.trim().to_string();
                    if text.is_empty() {
                        ChatInput::Ignore
                    } else {
                        ChatInput::User { text, metadata }
                    }
                }
                InteractionCommand::PublishBackgroundUpdate { event } => {
                    ChatInput::Evidence(EvidenceRecord::Correlated(event))
                }
                InteractionCommand::RestoreOperationalState { sessions } => {
                    ChatInput::Recovery(sessions)
                }
                InteractionCommand::NotifyUser { text } => {
                    ChatInput::Notification { text, metadata }
                }
                InteractionCommand::BeginConversation { .. }
                | InteractionCommand::CancelConversation { .. } => ChatInput::Ignore,
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
        ChatInput::User {
            text,
            metadata: synthetic_interaction_metadata(None),
        }
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
    interaction_metadata: Option<&InteractionMetadata>,
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
                if let Some(metadata) = interaction_metadata {
                    emit_interaction_event(
                        metadata,
                        InteractionEvent::ConversationDelta {
                            text: delta.to_string(),
                        },
                    );
                }
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
            chat_with_delegation(
                model,
                conversation,
                tools.as_deref().expect("conversation tools are enabled"),
                !force_delegation,
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
        let mut tool_calls = if tools_enabled {
            completion.tool_calls.clone()
        } else {
            Vec::new()
        };
        let mut protocol_recovered = false;
        let dsml_response = direct_response(&tool_calls)
            .filter(|response| response.contains("DSML"))
            .or_else(|| text_out.contains("DSML").then(|| text_out.clone()));
        if role == AgentRole::Conversation && tools_enabled && dsml_response.is_some() {
            tool_calls = dsml_response
                .as_deref()
                .and_then(|response| dsml_delegation_response(response, turn))
                .or_else(|| fallback_delegation_call(conversation, turn))
                .into_iter()
                .collect();
            protocol_recovered = !tool_calls.is_empty();
        }
        let has_delegation = tool_calls
            .iter()
            .any(|call| matches!(call.name.as_str(), "spawn_agent" | "spawn_agents"));
        let fallback_delegation =
            role == AgentRole::Conversation && force_delegation && !has_delegation;
        if fallback_delegation || protocol_recovered {
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
        let tasks = tool_jobs
            .iter()
            .filter(|(call, allowed)| {
                *allowed && matches!(call.name.as_str(), "spawn_agent" | "spawn_agents")
            })
            .flat_map(|(call, _)| task_intents(call))
            .collect::<Vec<_>>();
        if !tasks.is_empty() {
            if let Some(metadata) = interaction_metadata {
                emit_interaction_event(
                    metadata,
                    InteractionEvent::ConversationIntentProduced {
                        intents: vec![InteractionIntent::StartTasks { tasks }],
                    },
                );
            }
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
        let delegation_succeeded =
            tool_jobs
                .iter()
                .zip(&outputs)
                .any(|((call, allowed), output)| {
                    *allowed
                        && matches!(call.name.as_str(), "spawn_agent" | "spawn_agents")
                        && output.succeeded
                });
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
                &truncate(&out.text, 600),
            );
            results.push(ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: tc.id.clone(),
                    output: out.text,
                }],
            });
        }
        conversation.extend(results);
        if role == AgentRole::Conversation && delegation_used {
            if !delegation_succeeded {
                return Ok(Turn::Done(delegation_failure_response(conversation), usage));
            }
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
                    if let Some(metadata) = interaction_metadata {
                        emit_interaction_event(
                            metadata,
                            InteractionEvent::ConversationDelta {
                                text: delta.to_string(),
                            },
                        );
                    }
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

fn dsml_delegation_response(response: &str, turn: Option<u64>) -> Option<ToolCall> {
    let name = response.split_once("invoke name=\"")?.1.split_once('"')?.0;
    let arguments = match name {
        "spawn_agent" => {
            let task = dsml_parameter(&response, "prompt")
                .or_else(|| dsml_parameter(&response, "description"))?;
            serde_json::json!({
                "task": task,
                "purpose": dsml_parameter(&response, "description").unwrap_or("fresh work"),
                "lifetime_class": "short"
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
            serde_json::json!({ "tasks": tasks, "lifetime_class": "short" })
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

fn durable_turn_messages(user: String, answer: String) -> Vec<ChatMessage> {
    vec![
        ChatMessage::new(Role::User, user),
        ChatMessage::new(Role::Assistant, answer),
    ]
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
            "lifetime_class": "short",
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
    let signals_progress = [
        "check",
        "look",
        "gather",
        "review",
        "work",
        "find",
        "pull",
        "get back",
        "verify",
        "compare",
        "investigat",
        "handle",
        "help",
    ]
    .iter()
    .any(|term| lower.contains(term));
    if text.is_empty()
        || text.len() > 160
        || text.matches(['.', '!', '?']).count() > 1
        || !signals_progress
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

fn task_intents(call: &ToolCall) -> Vec<TaskIntent> {
    let value = serde_json::from_str::<serde_json::Value>(&call.arguments).unwrap_or_default();
    let lifetime_class = value
        .get("lifetime_class")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(LifetimeClass::Short);
    match call.name.as_str() {
        "spawn_agent" => value
            .get("task")
            .and_then(|value| value.as_str())
            .filter(|task| !task.trim().is_empty())
            .map(|task| {
                vec![TaskIntent {
                    objective: task.into(),
                    purpose: value
                        .get("purpose")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .into(),
                    lifetime_class,
                }]
            })
            .unwrap_or_default(),
        "spawn_agents" => value
            .get("tasks")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .filter(|task| !task.trim().is_empty())
            .map(|task| TaskIntent {
                objective: task.into(),
                purpose: String::new(),
                lifetime_class,
            })
            .collect(),
        _ => Vec::new(),
    }
}

struct ToolOutput {
    text: String,
    succeeded: bool,
}

impl ToolOutput {
    fn success(text: String) -> Self {
        Self {
            text,
            succeeded: true,
        }
    }

    fn failure(text: String) -> Self {
        Self {
            text,
            succeeded: false,
        }
    }
}

async fn run_tool(
    tc: &ToolCall,
    role: AgentRole,
    delegation_allowed: bool,
    turn: Option<u64>,
) -> ToolOutput {
    if !role.allows_tool(&tc.name) {
        return ToolOutput::failure(format!("{} is not available to the {role:?} role", tc.name));
    }
    match tc.name.as_str() {
        "spawn_agent" => {
            let task = arg(&tc.arguments, "task");
            if !delegation_allowed {
                return ToolOutput::failure("Delegation has already been used for this turn. Synthesize an answer from the worker results already received; do not spawn another worker.".into());
            }
            let cwd_arg = arg(&tc.arguments, "cwd");
            let cwd = (!cwd_arg.is_empty()).then_some(cwd_arg);
            let value =
                serde_json::from_str::<serde_json::Value>(&tc.arguments).unwrap_or_default();
            let lifetime_class = value
                .get("lifetime_class")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(LifetimeClass::Short);
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
                Ok(Ok(result)) => ToolOutput::success(result),
                Ok(Err(error)) => ToolOutput::failure(format!("worker spawn failed: {error}")),
                Err(error) => ToolOutput::failure(format!("worker spawn task failed: {error}")),
            }
        }
        "spawn_agents" => {
            if !delegation_allowed {
                return ToolOutput::failure("Delegation has already been used for this turn. Synthesize an answer from the worker results already received; do not spawn another worker.".into());
            }
            let tasks = match serde_json::from_str::<serde_json::Value>(&tc.arguments)
                .ok()
                .and_then(|value| value.get("tasks").cloned())
                .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
            {
                Some(tasks) if !tasks.is_empty() => tasks,
                _ => {
                    return ToolOutput::failure(
                        "spawn_agents requires a non-empty tasks array".into(),
                    );
                }
            };
            if tasks.len() > 8 {
                return ToolOutput::failure(
                    "spawn_agents accepts at most 8 tasks; group related objectives".into(),
                );
            }
            let lifetime_class = serde_json::from_str::<serde_json::Value>(&tc.arguments)
                .ok()
                .and_then(|value| value.get("lifetime_class").cloned())
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(LifetimeClass::Short);
            let jobs = tasks.iter().cloned().enumerate().map(|(index, task)| {
                let lifetime_class = lifetime_class;
                let correlation = delegation_correlation(tc, turn, Some(index));
                tokio::task::spawn_blocking(move || {
                    spawn_via_daemon(&task, None, lifetime_class, String::new(), correlation)
                })
            });
            let results = join_all(jobs).await;
            let outcomes = results
                .into_iter()
                .map(|result| match result {
                    Ok(Ok(answer)) => ToolOutput::success(answer),
                    Ok(Err(error)) => ToolOutput::failure(format!("worker failed: {error}")),
                    Err(error) => ToolOutput::failure(format!("worker task failed: {error}")),
                })
                .collect();
            compose_fanout_output(&tasks, outcomes)
        }
        other => ToolOutput::failure(format!("unknown tool: {other}")),
    }
}

fn compose_fanout_output(tasks: &[String], outcomes: Vec<ToolOutput>) -> ToolOutput {
    let succeeded = outcomes.iter().filter(|outcome| outcome.succeeded).count();
    let status = if succeeded == tasks.len() {
        "complete"
    } else if succeeded == 0 {
        "failed"
    } else {
        "partial"
    };
    let mut evidence = Vec::new();
    let mut failures = Vec::new();
    let mut outcomes = outcomes.into_iter();
    for (index, objective) in tasks.iter().enumerate() {
        let outcome = outcomes.next().unwrap_or_else(|| {
            ToolOutput::failure("worker returned no outcome for this objective".into())
        });
        let outcome_status = if outcome.succeeded {
            "succeeded"
        } else {
            "failed"
        };
        let entry = format!(
            "Objective {} [{outcome_status}]: {}\n{}",
            index + 1,
            objective,
            outcome.text
        );
        if outcome.succeeded {
            evidence.push(entry);
        } else {
            failures.push(entry);
        }
    }

    let mut sections = vec![format!(
        "Coverage: {status} ({succeeded}/{} objectives succeeded).",
        tasks.len()
    )];
    if !evidence.is_empty() {
        sections.push(format!("Valid evidence:\n{}", evidence.join("\n\n")));
    }
    if !failures.is_empty() {
        sections.push(format!(
            "Failed objectives (not evidence):\n{}",
            failures.join("\n\n")
        ));
    }
    ToolOutput {
        text: sections.join("\n\n"),
        succeeded: succeeded > 0,
    }
}

fn delegation_failure_response(conversation: &[ChatMessage]) -> String {
    let evidence = compose_worker_response(conversation);
    if evidence.contains("timed out waiting for a result") {
        "I couldn't complete the lookup before its deadline, so I don't have reliable current information to answer that yet.".into()
    } else {
        "I couldn't complete the lookup, so I don't have reliable information to answer that yet."
            .into()
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
    let work_id = correlation.logical_task_id.clone();
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
            logical_task_id: Some(work_id.clone()),
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
        .send(&ApiRequest::WorkSubscribe {
            work_id: work_id.clone(),
        })
        .map_err(|e| e.to_string())?;
    loop {
        match stream.recv().map_err(|error| error.to_string())? {
            ApiResponse::Event {
                stream: EventStream::Stdout,
                data,
            } => {
                if let Some(AgentEvent::WorkResult { result }) = decode_event(&data) {
                    return match result.outcome {
                        WorkOutcome::Completed { result, .. } => {
                            println!("[worker:result] {id} {result}");
                            Ok(result)
                        }
                        WorkOutcome::TimedOut { .. } => {
                            Err(format!("worker {id} timed out waiting for a result"))
                        }
                        WorkOutcome::Failed { message } => Err(format!("worker {id}: {message}")),
                        WorkOutcome::Blocked { reason } => {
                            Err(format!("worker {id} blocked: {reason}"))
                        }
                        WorkOutcome::Cancelled { reason } => {
                            Err(format!("worker {id} cancelled: {reason}"))
                        }
                    };
                }
            }
            ApiResponse::Event {
                stream: EventStream::Exit,
                data,
            } => {
                return Err(format!("worker {id} exited without a work result: {data}"));
            }
            ApiResponse::Error { message, .. } => return Err(message),
            _ => {}
        }
    }
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
    fn queued_turn_acknowledgement_is_a_turn_correlated_status() {
        let event = status_event(Some(7), &format!("queued {QUEUED_TURN_ACKNOWLEDGEMENT}"));
        assert!(matches!(
            event,
            AgentEvent::Status {
                turn: Some(7),
                phase,
                message,
            } if phase == "queued" && message == QUEUED_TURN_ACKNOWLEDGEMENT
        ));
        assert!(!QUEUED_TURN_ACKNOWLEDGEMENT.contains("turn"));
        assert!(!QUEUED_TURN_ACKNOWLEDGEMENT.contains("queue"));
        assert!(usable_acknowledgement(CONCURRENT_TURN_ACKNOWLEDGEMENT).is_some());
    }

    #[test]
    fn routing_fallback_keeps_unrelated_conversation_moving() {
        let active = "Active turn 2: get the weather in London";
        assert_eq!(
            fallback_interaction_decision(active, "will I need a coat in London?"),
            InteractionDecision::WaitForActiveTurn
        );
        assert_eq!(
            fallback_interaction_decision(active, "tell me a joke"),
            InteractionDecision::AnswerNow
        );
        assert_eq!(
            fallback_interaction_decision(active, "summarize our current conversation"),
            InteractionDecision::AnswerNow
        );
    }

    fn completed_evidence(turn: u64, result: impl Into<String>) -> EvidenceRecord {
        completed_objective_evidence(turn, "inspect the release state", result)
    }

    fn completed_objective_evidence(
        turn: u64,
        objective: &str,
        result: impl Into<String>,
    ) -> EvidenceRecord {
        EvidenceRecord::Correlated(EventEnvelope {
            event_id: turn,
            session_id: format!("worker-{turn}"),
            conversation_id: Some(FOREGROUND_ID.into()),
            turn_id: Some(turn.to_string()),
            task_id: Some(format!("task-{turn}")),
            parent_task_id: None,
            tool_call_id: Some(format!("call-{turn}")),
            actor: Actor::Worker {
                id: format!("worker-{turn}"),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: AgentEvent::WorkerCompleted {
                worker_id: format!("worker-{turn}"),
                objective: objective.into(),
                result: result.into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        })
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
            ChatInput::User { text, metadata } => {
                assert_eq!(text, "first line\nsecond line");
                assert_eq!(metadata.message_id, "command-1");
                assert_eq!(metadata.correlation_id, "turn-1");
            }
            _ => panic!("expected a user turn"),
        }
    }

    #[test]
    fn operational_recovery_never_becomes_a_user_turn() {
        let sessions = vec![RecoveredSession {
            session_id: "worker-1".into(),
            task_type: "research".into(),
            description: "compare sources".into(),
            state: tachyon_api::AgentState::Waiting,
        }];
        let line = serde_json::to_string(&InteractionCommandEnvelope {
            metadata: interaction_metadata(),
            command: InteractionCommand::RestoreOperationalState {
                sessions: sessions.clone(),
            },
        })
        .unwrap();
        match decode_chat_input(&line, AgentRole::Conversation) {
            ChatInput::Recovery(decoded) => assert_eq!(decoded, sessions),
            _ => panic!("expected operational recovery"),
        }
    }

    #[test]
    fn delegation_calls_produce_provider_neutral_task_intents() {
        let call = ToolCall {
            id: "call-1".into(),
            name: "spawn_agents".into(),
            arguments: serde_json::json!({
                "tasks": ["London weather", "Tokyo weather"],
                "lifetime_class": "short"
            })
            .to_string(),
        };
        let intents = task_intents(&call);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].objective, "London weather");
        assert_eq!(intents[1].objective, "Tokyo weather");
        assert_eq!(intents[0].lifetime_class, LifetimeClass::Short);
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
    fn immediate_turn_context_includes_visible_unfinished_requests() {
        let conversation = ConversationState {
            messages: vec![ChatMessage::new(Role::System, "system")],
            evidence: Vec::new(),
            pending: BTreeMap::from([(
                2,
                durable_turn_messages("second request".into(), "second answer".into()),
            )]),
            next_commit: 1,
        };
        let active = BTreeMap::from([
            (1, "first request".into()),
            (2, "second request".into()),
            (3, "third request".into()),
        ]);

        let snapshot = available_conversation_snapshot(&conversation, &active, 3);
        let text = snapshot
            .iter()
            .map(ChatMessage::plain)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("first request"));
        assert!(text.contains("second answer"));
        assert!(!text.contains("third request"));
        assert_eq!(
            snapshot
                .iter()
                .filter(|message| message.plain().contains("second request"))
                .count(),
            1
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
    fn post_completion_follow_up_can_answer_without_spawning() {
        let evidence = completed_evidence(4, "verified result".repeat(2_000));
        let attached =
            accepted_follow_up_evidence(&[evidence], 4, "Can you explain what that means?")
                .expect("preceding accepted evidence should be attached");

        assert!(attached.contains("verified result"));
        assert!(attached.contains("[context truncated]"));
        let policy =
            follow_up_execution_policy(false, true, Some(Answerability::AnswerFromContext));
        let tools_enabled = !policy.answer_from_context;
        assert!(policy.answer_from_context);
        assert!(!policy.force_delegation);
        assert!(!tools_enabled);
    }

    #[test]
    fn follow_up_attaches_only_matching_objectives_when_available() {
        let evidence = [
            completed_objective_evidence(4, "weather in New York", "New York result"),
            completed_objective_evidence(4, "weather in London", "London result"),
        ];
        let attached = accepted_follow_up_evidence(&evidence, 4, "Do I need a coat in London?")
            .expect("London evidence");

        assert!(attached.contains("London result"));
        assert!(!attached.contains("New York result"));
    }

    #[test]
    fn post_completion_follow_up_needing_new_evidence_still_delegates() {
        let evidence = completed_evidence(7, "previously verified");
        assert!(accepted_follow_up_evidence(
            &[evidence],
            7,
            "Has that changed since the verification?"
        )
        .is_some());

        let policy = follow_up_execution_policy(false, true, Some(Answerability::NeedsNewWork));
        let tools_enabled = !policy.answer_from_context;
        assert!(!policy.answer_from_context);
        assert!(policy.force_delegation);
        assert!(tools_enabled);
    }

    #[test]
    fn answerability_timeout_only_applies_to_accepted_follow_up_evidence() {
        assert_eq!(answerability_timeout(false), None);
        assert_eq!(
            answerability_timeout(true),
            Some(std::time::Duration::from_secs(5))
        );
    }

    #[test]
    fn missing_follow_up_evidence_forces_delegation_without_classifier_timeout() {
        let policy = follow_up_execution_policy(true, false, None);
        assert_eq!(answerability_timeout(false), None);
        assert!(policy.force_delegation);
        assert!(!policy.answer_from_context);
    }

    #[test]
    fn answerability_trace_contains_no_evidence() {
        let event = answerability_timing(
            9,
            Answerability::NeedsNewWork,
            true,
            true,
            std::time::Duration::from_millis(5_000),
        );
        assert!(matches!(
            event,
            AgentEvent::Timing { turn: 9, stage, elapsed_ms: 5_000 }
                if stage == "answerability outcome=needs_new_work fallback=true timeout=true"
        ));
    }

    #[test]
    fn durable_turn_contains_only_visible_transcript() {
        let conversation = durable_turn_messages("hello".into(), "Hi.".into());
        assert_eq!(conversation.len(), 2);
        assert_eq!(conversation[0].role, Role::User);
        assert_eq!(conversation[1].role, Role::Assistant);
        assert_eq!(conversation[1].plain(), "Hi.");
        assert!(conversation
            .iter()
            .flat_map(|message| &message.content)
            .all(|content| matches!(content, Content::Text(_))));
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
    fn fanout_output_preserves_partial_coverage_and_separates_failures() {
        let tasks = vec![
            "verify package release".into(),
            "inspect deployment status".into(),
            "check security advisory".into(),
        ];
        let output = compose_fanout_output(
            &tasks,
            vec![
                ToolOutput::success("release evidence".into()),
                ToolOutput::failure("worker timed out".into()),
                ToolOutput::success("advisory evidence".into()),
            ],
        );

        assert!(output.succeeded);
        assert!(output
            .text
            .contains("Coverage: partial (2/3 objectives succeeded)."));
        let evidence = output.text.find("Valid evidence:").unwrap();
        let failures = output
            .text
            .find("Failed objectives (not evidence):")
            .unwrap();
        assert!(evidence < failures);
        assert!(output.text[..failures].contains("verify package release"));
        assert!(output.text[..failures].contains("check security advisory"));
        assert!(!output.text[..failures].contains("worker timed out"));
        assert!(output.text[failures..].contains("inspect deployment status"));
        assert!(output.text[failures..].contains("worker timed out"));
    }

    #[test]
    fn failed_delegation_does_not_turn_timeout_into_factual_evidence() {
        let conversation = vec![
            ChatMessage::new(Role::User, "get current release information"),
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "lookup".into(),
                    output: "worker spawn failed: worker worker-1 timed out waiting for a result"
                        .into(),
                }],
            },
        ];

        let response = delegation_failure_response(&conversation);
        assert!(response.contains("before its deadline"));
        assert!(!response.contains("worker"));
        assert!(!response.contains("release"));
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
        assert_eq!(
            usable_acknowledgement(
                "Based on the current conditions, you will not need a coat. It is mild outside."
            ),
            None
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
        let legacy_response = direct_response(&[call]).expect("legacy response");
        let recovered = dsml_delegation_response(&legacy_response, Some(7)).expect("delegation");
        assert_eq!(recovered.name, "spawn_agents");
        let arguments: serde_json::Value = serde_json::from_str(&recovered.arguments).unwrap();
        assert_eq!(arguments["tasks"].as_array().unwrap().len(), 2);
        assert_eq!(arguments["tasks"][1], "Get current Tokyo weather");

        let direct = dsml_delegation_response(response, Some(8)).expect("direct delegation");
        assert_eq!(direct.name, "spawn_agents");
        assert_eq!(direct.id, "dsml-recovered-8");
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
            assert_eq!(arguments["lifetime_class"], "short");
        }
    }

    #[test]
    fn only_completed_work_results_are_reusable_evidence() {
        let completed = AgentEvent::WorkResult {
            result: tachyon_api::WorkResult {
                work_id: "work-1".into(),
                objective: "inspect".into(),
                generation: 0,
                assignment: 0,
                outcome: WorkOutcome::Completed {
                    result: "verified".into(),
                    artifacts: Vec::new(),
                    context: String::new(),
                    suggested_reuse: false,
                },
            },
        };
        let timeout = AgentEvent::WorkResult {
            result: tachyon_api::WorkResult {
                work_id: "work-2".into(),
                objective: "inspect".into(),
                generation: 0,
                assignment: 0,
                outcome: WorkOutcome::TimedOut { deadline_ms: 10 },
            },
        };
        assert!(is_completed_evidence(&completed));
        assert!(!is_completed_evidence(&timeout));
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
