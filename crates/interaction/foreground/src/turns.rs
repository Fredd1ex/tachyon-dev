//! Ordered durable history and snapshots for independently running turns.

use std::collections::BTreeMap;
use tachyon_model::{ChatMessage, Role};

use std::sync::{Arc, Mutex};
use tachyon_api::types::{AgentEvent, EventEnvelope, WorkOutcome};

use crate::model::{bounded_policy_text, truncate};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub(super) enum EvidenceRecord {
    Correlated(EventEnvelope),
    Legacy(AgentEvent),
}

impl EvidenceRecord {
    pub(super) fn event(&self) -> &AgentEvent {
        match self {
            Self::Correlated(envelope) => &envelope.kind,
            Self::Legacy(event) => event,
        }
    }

    pub(super) fn origin_turn(&self) -> Option<u64> {
        match self {
            Self::Correlated(envelope) => envelope.turn_id.as_deref()?.parse().ok(),
            Self::Legacy(_) => None,
        }
    }
}

pub(super) fn is_completed_evidence(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::WorkerCompleted { .. }
            | AgentEvent::ContextCompacted { .. }
            | AgentEvent::WorkResult {
                result: tachyon_api::WorkResult {
                    outcome: WorkOutcome::Completed { .. },
                    ..
                }
            }
    )
}

pub(super) async fn wait_for_prior_turn(
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

pub(super) async fn wait_for_context_or_evidence(
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

pub(super) fn evidence_relevant_to_follow_up(
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

pub(super) fn accepted_follow_up_evidence(
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

pub(super) fn evidence_matches(incoming: &str, objective: &str) -> bool {
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

pub(super) fn estimated_context_tokens(messages: &[ChatMessage]) -> u32 {
    messages
        .iter()
        .map(|message| {
            serde_json::to_vec(message)
                .map(|encoded| (encoded.len() / 4 + 1) as u32)
                .unwrap_or_default()
        })
        .fold(0, u32::saturating_add)
}

pub(super) fn compact_context_messages(messages: &mut Vec<ChatMessage>, target_tokens: u32) {
    if estimated_context_tokens(messages) <= target_tokens {
        return;
    }
    let mut retained = Vec::new();
    let mut used = 0_u32;
    if let Some(system) = messages.iter().find(|message| message.role == Role::System) {
        let cost = estimated_context_tokens(std::slice::from_ref(system));
        retained.push((0, system.clone()));
        used = used.saturating_add(cost);
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

pub(super) fn same_evidence(left: &EvidenceRecord, right: &EvidenceRecord) -> bool {
    match (left, right) {
        (EvidenceRecord::Correlated(left), EvidenceRecord::Correlated(right)) => {
            left.session_id == right.session_id && left.event_id == right.event_id
        }
        _ => serde_json::to_string(left).ok() == serde_json::to_string(right).ok(),
    }
}

pub(super) struct ConversationState {
    pub(super) messages: Vec<ChatMessage>,
    pub(super) evidence: Vec<EvidenceRecord>,
    pub(super) pending: BTreeMap<u64, Vec<ChatMessage>>,
    pub(super) next_commit: u64,
    pub(super) context_epoch: u64,
}

pub(super) fn commit_ready_turns(conversation: &mut ConversationState) {
    loop {
        let next_commit = conversation.next_commit;
        let Some(messages) = conversation.pending.remove(&next_commit) else {
            break;
        };
        conversation.messages.extend(messages);
        conversation.next_commit += 1;
    }
}

pub(super) fn available_conversation_snapshot(
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

pub(super) fn durable_turn_messages(user: String, answer: String) -> Vec<ChatMessage> {
    vec![
        ChatMessage::new(Role::User, user),
        ChatMessage::new(Role::Assistant, answer),
    ]
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use tachyon_api::types::Actor;
    use tachyon_api::FOREGROUND_ID;
    use tachyon_model::Content;
    pub(crate) fn completed_evidence(turn: u64, result: impl Into<String>) -> EvidenceRecord {
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
    fn context_compaction_keeps_system_and_recent_messages() {
        let mut messages = vec![ChatMessage::new(Role::System, "system")];
        for index in 0..20 {
            messages.push(ChatMessage::new(
                if index % 2 == 0 {
                    Role::User
                } else {
                    Role::Assistant
                },
                format!("message {index} {}", "content ".repeat(20)),
            ));
        }
        let original = messages.len();
        compact_context_messages(&mut messages, 300);
        assert!(messages.len() < original);
        assert_eq!(messages[0].role, Role::System);
        assert!(messages.last().unwrap().plain().contains("message 19"));
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
            context_epoch: 0,
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
    fn only_completed_work_results_are_reusable_evidence() {
        let completed = AgentEvent::WorkResult {
            result: tachyon_api::WorkResult {
                attempt_id: None,
                instruction_revision: None,
                work_id: "work-1".into(),
                candidate_refs: None,
                final_context: None,
                evidence: Default::default(),
                timing: None,
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
                attempt_id: None,
                instruction_revision: None,
                work_id: "work-2".into(),
                candidate_refs: None,
                final_context: None,
                evidence: Default::default(),
                timing: None,
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
    fn commit_cursor_advances_only_through_contiguous_terminal_turns() {
        let mut conversation = ConversationState {
            messages: Vec::new(),
            evidence: Vec::new(),
            pending: BTreeMap::from([
                (2, vec![ChatMessage::new(Role::Assistant, "second")]),
                (3, vec![ChatMessage::new(Role::Assistant, "third")]),
            ]),
            next_commit: 1,
            context_epoch: 0,
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
}
