#![forbid(unsafe_code)]
//! Host-neutral response/work/progress reduction and bounded revision replay.
//! The adapter serializes durable publication and snapshots; clients never own execution.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use tachyon_api::interaction_manager::*;
use tachyon_api::{AgentEvent, EventEnvelope, InteractionEvent, InteractionEventEnvelope};

pub fn answer_reference(turn: &str, generation: u64, final_event: Option<&str>) -> String {
    serde_json::to_string(&(turn, generation, final_event)).expect("answer key")
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub projection: Projection,
    #[serde(default)]
    seen: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    telemetry: BTreeMap<String, u64>,
    #[serde(default)]
    calls: BTreeSet<String>,
    #[serde(default)]
    retired_responses: BTreeMap<String, (u64, ResponsePhase)>,
    #[serde(default)]
    retired_works: BTreeMap<String, (u64, u64)>,
}

#[derive(Clone)]
pub struct Manager {
    revision: Revision,
    updates: VecDeque<(Update, usize)>,
    bytes: usize,
    floor: u64,
    checkpoint: Checkpoint,
    pub restored: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, event: InteractionEvent) -> InteractionEventEnvelope {
        let mut metadata = tachyon_api::InteractionMetadata::new(id, "command", "foreground", 1);
        metadata.turn_id = Some("session:1".into());
        InteractionEventEnvelope { metadata, event }
    }

    fn telemetry(sequence: u64, kind: AgentEvent) -> EventEnvelope {
        EventEnvelope {
            event_id: sequence,
            session_id: "session".into(),
            conversation_id: Some("foreground".into()),
            turn_id: Some("session:1".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::Actor::Foreground,
            sequence,
            occurred_at_ms: sequence,
            kind,
        }
    }

    #[test]
    fn headless_prefix_pending_intents_duplicates_and_final_are_canonical() {
        let mut manager = Manager::default();
        manager.publish(
            "session".into(),
            event(
                "accepted",
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            ),
        );
        manager.telemetry(
            &telemetry(
                1,
                AgentEvent::Status {
                    turn: Some(1),
                    phase: "working".into(),
                    message: "Checking sources".into(),
                },
            ),
            None,
        );
        assert_eq!(
            manager.projection().responses[0].pending.as_deref(),
            Some("Checking sources")
        );
        manager.publish(
            "session".into(),
            event(
                "intent",
                InteractionEvent::ConversationIntentProduced {
                    intents: vec![tachyon_api::InteractionIntent::CancelTask {
                        task_id: "work".into(),
                    }],
                },
            ),
        );
        let delta = event(
            "delta",
            InteractionEvent::ConversationDelta {
                text: "answer prefix".into(),
            },
        );
        manager.publish("session".into(), delta.clone());
        let revision = manager.revision();
        manager.publish("session".into(), delta);
        assert_eq!(manager.revision(), revision);
        let response = &manager.projection().responses[0];
        assert_eq!(response.answer, "answer prefix");
        assert_eq!(response.phase, ResponsePhase::Answering);
        assert_eq!(response.intents.len(), 1);
        assert!(manager.projection().works.is_empty());
        let checkpoint =
            serde_json::from_slice(&serde_json::to_vec(manager.checkpoint()).unwrap()).unwrap();
        let mut restored = Manager::default();
        restored.restore(checkpoint);
        assert_eq!(restored.projection(), manager.projection());
        restored.publish(
            "session".into(),
            event(
                "final",
                InteractionEvent::ConversationFinished {
                    text: "canonical answer".into(),
                },
            ),
        );
        let final_revision = restored.revision();
        restored.publish(
            "session".into(),
            event(
                "late",
                InteractionEvent::ConversationDelta {
                    text: "wrong".into(),
                },
            ),
        );
        assert_eq!(restored.revision(), final_revision);
        assert_eq!(
            restored.projection().responses[0].answer,
            "canonical answer"
        );
    }

    #[test]
    fn failure_is_not_work_and_new_attempt_is_explicit() {
        let mut manager = Manager::default();
        manager.publish(
            "session".into(),
            event(
                "accepted",
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            ),
        );
        manager.telemetry(
            &telemetry(
                1,
                AgentEvent::Error {
                    turn: Some(1),
                    message: "worker task failed: pretend work".into(),
                },
            ),
            None,
        );
        assert!(manager.projection().works.is_empty());
        assert_eq!(
            manager.projection().responses[0]
                .failure
                .as_ref()
                .unwrap()
                .kind,
            FailureKind::Provider
        );
        let mut next = event(
            "attempt",
            InteractionEvent::UserTurnAccepted {
                text: "retry".into(),
            },
        );
        next.metadata.generation = 1;
        manager.publish("session".into(), next);
        manager.publish(
            "session".into(),
            event(
                "late-final",
                InteractionEvent::ConversationFinished { text: "old".into() },
            ),
        );
        assert_eq!(
            manager.projection().responses[0].phase,
            ResponsePhase::Accepted
        );
        assert!(manager.projection().responses[0].answer.is_empty());
        manager.reconcile(Some("new-session"), &[], &BTreeMap::new());
        assert_eq!(
            manager.projection().responses[0].phase,
            ResponsePhase::Interrupted
        );
        let mut final_event = event(
            "durable-final",
            InteractionEvent::ConversationFinished {
                text: "final beats interrupted checkpoint".into(),
            },
        );
        final_event.metadata.generation = 1;
        manager.publish("session".into(), final_event);
        assert_eq!(
            manager.projection().responses[0].phase,
            ResponsePhase::Completed
        );
    }

    fn work(assignment: u64) -> Work {
        Work {
            work_id: "work".into(),
            worker_id: "worker".into(),
            origin_turn_id: Some("session:1".into()),
            generation: 1,
            assignment,
            attempt_id: None,
            revision: 0,
            title: "Read files".into(),
            phase: WorkPhase::Running,
            metrics: Metrics::default(),
            latest_tool: None,
            result_available: false,
            candidate_refs: vec![],
            todo_scope: tachyon_api::todo::TodoScope::Work {
                work_id: "work".into(),
            },
        }
    }

    #[test]
    fn work_fences_latest_tool_and_terminal_metrics_do_not_regress() {
        let mut manager = Manager::default();
        manager.work(work(2));
        let identity = tachyon_api::ToolTelemetryIdentity {
            work_id: Some("work".into()),
            generation: Some(1),
            assignment: Some(2),
            task_id: Some("work".into()),
            attempt_id: None,
        };
        for (sequence, id) in [(1, "first"), (2, "latest")] {
            manager.telemetry(
                &telemetry(
                    sequence,
                    AgentEvent::ToolStarted {
                        turn: Some(1),
                        id: id.into(),
                        name: "read".into(),
                        arguments: "heavy arguments excluded".into(),
                        identity: Some(identity.clone()),
                    },
                ),
                Some("work"),
            );
        }
        manager.telemetry(
            &telemetry(
                3,
                AgentEvent::ToolFinished {
                    turn: Some(1),
                    id: "first".into(),
                    output: "heavy output excluded".into(),
                    identity: Some(identity.clone()),
                },
            ),
            Some("work"),
        );
        assert_eq!(
            manager.projection().works[0]
                .latest_tool
                .as_ref()
                .unwrap()
                .call_id,
            "latest"
        );
        assert!(
            !manager.projection().works[0]
                .latest_tool
                .as_ref()
                .unwrap()
                .finished
        );
        let mut stale = identity;
        stale.assignment = Some(1);
        manager.telemetry(
            &telemetry(
                4,
                AgentEvent::ToolStarted {
                    turn: Some(1),
                    id: "stale".into(),
                    name: "read".into(),
                    arguments: "{}".into(),
                    identity: Some(stale),
                },
            ),
            Some("work"),
        );
        manager.work(work(1));
        assert_eq!(manager.projection().works[0].metrics.tools_started, 2);
        let mut terminal = work(2);
        terminal.phase = WorkPhase::Completed;
        terminal.result_available = true;
        terminal.metrics.timing = Some(tachyon_api::WorkTiming {
            execution_ms: Some(123),
            ..Default::default()
        });
        manager.work(terminal);
        manager.work(work(2));
        let work = &manager.projection().works[0];
        assert_eq!(work.phase, WorkPhase::Completed);
        assert_eq!(work.metrics.tools_started, 2);
        assert_eq!(
            work.metrics.timing.as_ref().unwrap().execution_ms,
            Some(123)
        );
    }

    #[test]
    fn oversized_prefix_and_gap_remain_recoverable_without_raw_replay() {
        let mut manager = Manager::default();
        let cursor = manager.revision();
        for n in 0..300 {
            manager.publish(
                "session".into(),
                event(
                    &format!("delta-{n}"),
                    InteractionEvent::ConversationDelta {
                        text: "x".repeat(1024),
                    },
                ),
            );
        }
        assert!(manager.replay(&cursor).is_err());
        let response = &manager.projection().responses[0];
        assert_eq!(response.answer.len(), 65536);
        assert_eq!(response.answer_bytes, 300 * 1024);
        assert!(response.answer_ref.is_some());
        assert_eq!(response.phase, ResponsePhase::Answering);
        assert_eq!(
            manager
                .page(&manager.revision(), 0)
                .unwrap()
                .projection
                .responses[0],
            *response
        );
    }

    #[test]
    fn oversized_update_and_epoch_change_require_resnapshot() {
        let mut manager = Manager::default();
        let before = manager.revision();
        manager.publish(
            "session".into(),
            tachyon_api::InteractionEventEnvelope {
                metadata: tachyon_api::InteractionMetadata::new(
                    "event",
                    "command",
                    "foreground",
                    1,
                ),
                event: tachyon_api::InteractionEvent::ConversationDelta {
                    text: "x".repeat(4 * 1024 * 1024),
                },
            },
        );
        assert!(manager.updates.is_empty());
        assert_eq!(manager.bytes, 0);
        assert!(manager.replay(&before).is_err());
        let current = manager.revision();
        assert!(manager.replay(&current).unwrap().is_empty());
        let mut future = current.clone();
        future.sequence += 1;
        assert!(manager.replay(&future).is_err());
        manager.invalidate();
        assert!(manager.replay(&current).is_err());
    }

    #[test]
    fn paged_active_state_is_not_evicted_and_retired_terminals_cannot_resurrect() {
        let mut manager = Manager::default();
        for n in 0..205 {
            let mut accepted = event(
                &format!("accepted-{n}"),
                InteractionEvent::UserTurnAccepted {
                    text: "hello".into(),
                },
            );
            accepted.metadata.turn_id = Some(format!("session:{n}"));
            manager.publish("session".into(), accepted);
        }
        let revision = manager.revision();
        let first = manager.page(&revision, 0).unwrap();
        assert_eq!(first.projection.responses.len(), 200);
        assert_eq!(
            manager
                .page(&revision, first.next_offset.unwrap())
                .unwrap()
                .projection
                .responses
                .len(),
            5
        );
        for n in 0..205 {
            let mut finished = event(
                &format!("final-{n}"),
                InteractionEvent::ConversationFinished {
                    text: "done".into(),
                },
            );
            finished.metadata.turn_id = Some(format!("session:{n}"));
            manager.publish("session".into(), finished);
        }
        assert_eq!(manager.projection().responses.len(), 200);
        assert!(manager.page(&revision, 200).is_err());
        let before = manager.revision();
        let mut late = event(
            "late",
            InteractionEvent::ConversationDelta {
                text: "must not resurrect".into(),
            },
        );
        late.metadata.turn_id = Some("session:0".into());
        manager.publish("session".into(), late);
        assert_eq!(manager.revision(), before);
    }
}

impl Default for Manager {
    fn default() -> Self {
        Self {
            revision: Revision {
                epoch: uuid::Uuid::new_v4().to_string(),
                sequence: 0,
            },
            updates: VecDeque::new(),
            bytes: 0,
            floor: 0,
            checkpoint: Checkpoint::default(),
            restored: false,
        }
    }
}

impl Manager {
    /// Storage failure invalidates cursors rather than promising replay coverage.
    pub fn invalidate(&mut self) {
        self.revision = Self::default().revision;
        self.updates.clear();
        self.bytes = 0;
        self.floor = 0;
    }
    pub fn revision(&self) -> Revision {
        self.revision.clone()
    }

    pub fn projection(&self) -> Projection {
        self.checkpoint.projection.clone()
    }

    pub fn page(&self, revision: &Revision, offset: usize) -> Result<ProjectionPage, Revision> {
        if revision != &self.revision {
            return Err(self.revision());
        }
        let projection = &self.checkpoint.projection;
        let total = projection.responses.len() + projection.works.len() + projection.progress.len();
        if offset > total {
            return Err(self.revision());
        }
        let mut page = Projection::default();
        let end = offset.saturating_add(200).min(total);
        for index in offset..end {
            if index < projection.responses.len() {
                page.responses.push(projection.responses[index].clone());
            } else if index < projection.responses.len() + projection.works.len() {
                page.works
                    .push(projection.works[index - projection.responses.len()].clone());
            } else {
                page.progress.push(
                    projection.progress
                        [index - projection.responses.len() - projection.works.len()]
                    .clone(),
                );
            }
        }
        Ok(ProjectionPage {
            revision: revision.clone(),
            projection: page,
            next_offset: (end < total).then_some(end),
        })
    }

    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    pub fn restore(&mut self, checkpoint: Checkpoint) {
        self.checkpoint = checkpoint;
        self.restored = true;
        self.invalidate();
    }

    /// Durable final history wins over an interrupted checkpoint (including a
    /// crash between outbox projection and checkpoint commit).
    pub fn reconcile(
        &mut self,
        session: Option<&str>,
        history: &[tachyon_api::HistoryEntry],
        final_generations: &BTreeMap<String, u64>,
    ) {
        let mut changes = Vec::new();
        for response in &mut self.checkpoint.projection.responses {
            let before = response.clone();
            if let Some(final_message) = history.iter().rev().find(|entry| {
                entry.role == tachyon_api::HistoryRole::Assistant
                    && entry.turn_id.as_ref() == Some(&response.turn_id)
                    && entry.kind == tachyon_api::HistoryKind::Conversation
                    && final_generations.get(&entry.event_id).copied().unwrap_or(0)
                        == response.generation
            }) {
                response.phase = ResponsePhase::Completed;
                response.failure = None;
                response.pending = None;
                response.intents.clear();
                response.final_event_id = Some(final_message.event_id.clone());
                response.answer_bytes = final_message.text.len() as u64;
                let mut end = 65536.min(final_message.text.len());
                while !final_message.text.is_char_boundary(end) {
                    end -= 1;
                }
                response.answer = final_message.text[..end].into();
                // Canonical final text remains available through HistoryQuery.
                response.answer_ref = (end < final_message.text.len())
                    .then(|| format!("history:{}", final_message.event_id));
            } else if session != Some(response.session_id.as_str()) && !response.phase.is_terminal()
            {
                response.phase = ResponsePhase::Interrupted;
                response.pending = None;
                response.failure = Some(Failure {
                    kind: FailureKind::Interrupted,
                    message: "Foreground session ended".into(),
                });
            }
            if *response != before {
                response.revision += 1;
                changes.push(ProjectionChange::Response {
                    response: response.clone(),
                });
            }
        }
        if !changes.is_empty() {
            self.record(session.unwrap_or_default().into(), None, changes);
        }
    }

    pub fn publish(&mut self, session_id: String, event: InteractionEventEnvelope) {
        if !matches!(
            event.event,
            InteractionEvent::UserVisibleNotificationPublished { .. }
        ) {
            if let Some((generation, phase)) = event
                .metadata
                .turn_id
                .as_ref()
                .and_then(|turn| self.checkpoint.retired_responses.get(turn))
            {
                let final_after_failure = event.metadata.generation == *generation
                    && matches!(phase, ResponsePhase::Failed | ResponsePhase::Interrupted)
                    && matches!(event.event, InteractionEvent::ConversationFinished { .. });
                let new_attempt = event.metadata.generation > *generation
                    && matches!(event.event, InteractionEvent::UserTurnAccepted { .. });
                if !final_after_failure && !new_attempt {
                    return;
                }
                self.checkpoint
                    .retired_responses
                    .remove(event.metadata.turn_id.as_ref().unwrap());
            }
        }
        let duplicate_scope = if matches!(
            event.event,
            InteractionEvent::UserVisibleNotificationPublished { .. }
        ) {
            String::new()
        } else {
            event.metadata.turn_id.clone().unwrap_or_default()
        };
        if !self
            .checkpoint
            .seen
            .entry(duplicate_scope)
            .or_default()
            .insert(event.metadata.message_id.clone())
        {
            return;
        }
        let mut changes = Vec::new();
        if let Some(turn) = event.metadata.turn_id.as_ref().filter(|_| {
            !matches!(
                event.event,
                InteractionEvent::UserVisibleNotificationPublished { .. }
            )
        }) {
            let responses = &mut self.checkpoint.projection.responses;
            let index = responses.iter().position(|r| &r.turn_id == turn);
            let fresh = || Response {
                session_id: session_id.clone(),
                turn_id: turn.clone(),
                command_origin: event.metadata.command_origin.clone(),
                generation: event.metadata.generation,
                revision: 0,
                phase: ResponsePhase::Accepted,
                answer: String::new(),
                answer_bytes: 0,
                answer_ref: None,
                pending: None,
                intents: Vec::new(),
                failure: None,
                final_event_id: None,
                metrics: Metrics::default(),
                latest_tool: None,
                work_ids: vec![],
                work_counts: WorkCounts::default(),
            };
            let index = index.unwrap_or_else(|| {
                responses.push(fresh());
                responses.len() - 1
            });
            let response = &mut responses[index];
            let generation = event.metadata.generation;
            if generation < response.generation {
                return;
            }
            if generation > response.generation {
                // A new attempt is explicit, not inferred from a late token/status.
                if !matches!(event.event, InteractionEvent::UserTurnAccepted { .. }) {
                    return;
                }
                let revision = response.revision;
                *response = fresh();
                response.revision = revision;
            }
            if response.phase.is_terminal()
                && !(matches!(
                    response.phase,
                    ResponsePhase::Failed | ResponsePhase::Interrupted
                ) && matches!(event.event, InteractionEvent::ConversationFinished { .. }))
            {
                return;
            }
            match &event.event {
                InteractionEvent::UserTurnAccepted { .. } => {
                    response.command_origin = event.metadata.command_origin.clone();
                }
                InteractionEvent::ConversationDelta { text } => {
                    let contiguous = response.answer_bytes == response.answer.len() as u64;
                    response.answer_bytes += text.len() as u64;
                    let remaining = if contiguous {
                        65536usize.saturating_sub(response.answer.len())
                    } else {
                        0
                    };
                    let mut end = remaining.min(text.len());
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    response.answer.push_str(&text[..end]);
                    if response.answer_bytes > response.answer.len() as u64 {
                        response.answer_ref = Some(answer_reference(
                            &response.turn_id,
                            response.generation,
                            None,
                        ));
                    }
                    response.phase = ResponsePhase::Answering;
                    response.pending = None;
                }
                InteractionEvent::ConversationFinished { text } => {
                    response.answer = text.clone();
                    response.answer_bytes = text.len() as u64;
                    let mut end = 65536.min(text.len());
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    response.answer.truncate(end);
                    response.answer_ref = (end < text.len()).then(|| {
                        answer_reference(
                            &response.turn_id,
                            response.generation,
                            Some(&event.metadata.message_id),
                        )
                    });
                    response.phase = ResponsePhase::Completed;
                    response.pending = None;
                    response.intents.clear();
                    response.final_event_id = Some(event.metadata.message_id.clone());
                    response.failure = None;
                }
                InteractionEvent::ConversationIntentProduced { intents } => {
                    response.intents = intents.clone();
                    if response.answer.is_empty() {
                        response.phase = ResponsePhase::Working;
                    }
                }
                InteractionEvent::ForegroundRequestTimedOut { .. } => {
                    response.phase = ResponsePhase::Failed;
                    response.failure = Some(Failure {
                        kind: FailureKind::Timeout,
                        message: "Foreground request timed out".into(),
                    });
                    response.pending = None;
                }
                InteractionEvent::UserVisibleNotificationPublished { .. } => {}
            }
            response.revision += 1;
            changes.push(ProjectionChange::Response {
                response: response.clone(),
            });
        }
        self.record(session_id, Some(event), changes);
    }

    /// The adapter supplies only admitted work and its host-fenced assignment.
    pub fn work(&mut self, mut work: Work) {
        if self
            .checkpoint
            .retired_works
            .get(&work.work_id)
            .is_some_and(|fence| (work.generation, work.assignment) <= *fence)
        {
            return;
        }
        let works = &mut self.checkpoint.projection.works;
        if let Some(previous) = works.iter_mut().find(|w| w.work_id == work.work_id) {
            if (work.generation, work.assignment) < (previous.generation, previous.assignment) {
                return;
            }
            if (work.generation, work.assignment) == (previous.generation, previous.assignment) {
                if work.attempt_id != previous.attempt_id || previous.phase.is_terminal() {
                    return;
                }
                if matches!(
                    (previous.phase, work.phase),
                    (WorkPhase::Running, WorkPhase::Waiting)
                        | (
                            WorkPhase::Reviewing,
                            WorkPhase::Waiting | WorkPhase::Running
                        )
                ) {
                    return;
                }
                work.metrics = Metrics {
                    timing: work
                        .metrics
                        .timing
                        .clone()
                        .or(previous.metrics.timing.clone()),
                    observed_invocations: work
                        .metrics
                        .observed_invocations
                        .or(previous.metrics.observed_invocations),
                    tools_started: work
                        .metrics
                        .tools_started
                        .max(previous.metrics.tools_started),
                    ..previous.metrics.clone()
                };
                work.latest_tool = previous.latest_tool.clone();
                work.revision = previous.revision;
                if &work == previous {
                    return;
                }
            }
            work.revision = previous.revision + 1;
            *previous = work.clone();
        } else {
            work.revision = 1;
            works.push(work.clone());
        }
        let mut changes = Vec::new();
        if !self
            .checkpoint
            .projection
            .progress
            .iter()
            .any(|p| p.scope == work.todo_scope)
        {
            let progress = Progress {
                scope: work.todo_scope.clone(),
                scope_revision: None,
                pending: 0,
                in_progress: 0,
                blocked: 0,
                completed: 0,
                cancelled: 0,
            };
            self.checkpoint.projection.progress.push(progress.clone());
            changes.push(ProjectionChange::Progress { progress });
        }
        changes.push(ProjectionChange::Work { work });
        self.record(String::new(), None, changes);
    }

    pub fn progress(&mut self, progress: Progress) {
        let entries = &mut self.checkpoint.projection.progress;
        if let Some(previous) = entries.iter_mut().find(|p| p.scope == progress.scope) {
            if progress.scope_revision < previous.scope_revision || previous == &progress {
                return;
            }
            *previous = progress.clone();
        } else {
            entries.push(progress.clone());
        }
        self.record(
            String::new(),
            None,
            vec![ProjectionChange::Progress { progress }],
        );
    }

    /// Source/session/assignment validation belongs to the host, not consumers.
    pub fn telemetry(&mut self, event: &EventEnvelope, work_id: Option<&str>) {
        let scope = work_id.or(event.turn_id.as_deref()).unwrap_or("unscoped");
        let mut key = format!("{}:{scope}", event.session_id);
        if let Some(id) = work_id {
            let Some(work) = self
                .checkpoint
                .projection
                .works
                .iter()
                .find(|w| w.work_id == id)
            else {
                return;
            };
            let identity = match &event.kind {
                AgentEvent::ToolStarted {
                    identity: Some(identity),
                    ..
                }
                | AgentEvent::ToolFinished {
                    identity: Some(identity),
                    ..
                }
                | AgentEvent::ToolTelemetry { identity, .. } => identity,
                _ => return,
            };
            if identity.work_id.as_deref() != Some(id)
                || identity.generation != Some(work.generation)
                || identity.assignment != Some(work.assignment)
                || identity.attempt_id != work.attempt_id
            {
                return;
            }
            key.push_str(&format!(
                ":{}:{}:{:?}",
                work.generation, work.assignment, work.attempt_id
            ));
        }
        let previous = self.checkpoint.telemetry.entry(key.clone()).or_default();
        if event.sequence <= *previous {
            return;
        }
        *previous = event.sequence;
        let projection = &mut self.checkpoint.projection;
        let (metrics, latest, response_index, work_index) = if let Some(id) = work_id {
            let Some(index) = projection.works.iter().position(|w| w.work_id == id) else {
                return;
            };
            let work = &mut projection.works[index];
            if work.phase.is_terminal() {
                return;
            }
            (&mut work.metrics, &mut work.latest_tool, None, Some(index))
        } else {
            let Some(index) = projection.responses.iter().position(|r| {
                Some(&r.turn_id) == event.turn_id.as_ref() && r.session_id == event.session_id
            }) else {
                return;
            };
            let response = &mut projection.responses[index];
            // Legacy foreground telemetry has no attempt generation. It cannot
            // safely mutate a later explicit attempt of the same canonical turn.
            if response.generation != 0 {
                return;
            }
            if response.phase.is_terminal()
                && !matches!(
                    event.kind,
                    AgentEvent::Usage { .. } | AgentEvent::Timing { .. }
                )
            {
                return;
            }
            match &event.kind {
                AgentEvent::Status { phase, message, .. }
                    if phase == "working" && response.answer.is_empty() && !message.is_empty() =>
                {
                    response.pending = Some(message.chars().take(512).collect());
                    response.phase = ResponsePhase::Working;
                }
                AgentEvent::Error { message, .. } => {
                    response.failure = Some(Failure {
                        kind: FailureKind::Provider,
                        message: message.chars().take(2048).collect(),
                    });
                    response.phase = ResponsePhase::Failed;
                    response.pending = None;
                }
                AgentEvent::Status { phase, .. }
                    if phase == "interrupted" || phase == "cancelled" =>
                {
                    response.phase = ResponsePhase::Interrupted;
                    response.pending = None;
                    response.failure = Some(Failure {
                        kind: FailureKind::Interrupted,
                        message: "Response interrupted".into(),
                    });
                }
                _ => {}
            }
            (
                &mut response.metrics,
                &mut response.latest_tool,
                Some(index),
                None,
            )
        };
        match &event.kind {
            AgentEvent::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                context_tokens,
                context_window,
                ..
            } => {
                metrics.prompt_tokens = Some((*prompt_tokens).into());
                metrics.completion_tokens = Some((*completion_tokens).into());
                metrics.total_tokens = Some((*total_tokens).into());
                metrics.context_tokens = Some((*context_tokens).into());
                metrics.context_window = context_window.map(u64::from);
            }
            AgentEvent::ToolStarted { id, name, .. } => {
                if self.checkpoint.calls.insert(format!("{key}:start:{id}")) {
                    metrics.tools_started += 1;
                    *latest = Some(ToolActivity {
                        call_id: id.clone(),
                        name: name.chars().take(128).collect(),
                        finished: false,
                        success: None,
                    });
                }
            }
            AgentEvent::ToolFinished { id, .. } => {
                if self.checkpoint.calls.insert(format!("{key}:finish:{id}")) {
                    metrics.tools_finished += 1;
                }
                if let Some(tool) = latest.as_mut().filter(|tool| tool.call_id == *id) {
                    tool.finished = true;
                }
            }
            AgentEvent::ToolTelemetry {
                call_id: Some(id),
                success,
                ..
            } => {
                if let Some(tool) = latest.as_mut().filter(|tool| tool.call_id == *id) {
                    tool.success = Some(*success);
                }
            }
            AgentEvent::Timing {
                stage, elapsed_ms, ..
            } => match stage.as_str() {
                "completed" => metrics.response_ms = Some(*elapsed_ms),
                "first_answer" => metrics.first_answer_ms = Some(*elapsed_ms),
                _ => return,
            },
            AgentEvent::Status { .. } | AgentEvent::Error { .. } => {}
            _ => return,
        }
        let change = if let Some(index) = response_index {
            let response = &mut projection.responses[index];
            response.revision += 1;
            ProjectionChange::Response {
                response: response.clone(),
            }
        } else {
            let work = &mut projection.works[work_index.unwrap()];
            work.revision += 1;
            ProjectionChange::Work { work: work.clone() }
        };
        self.record(event.session_id.clone(), None, vec![change]);
    }

    fn record(
        &mut self,
        session_id: String,
        event: Option<InteractionEventEnvelope>,
        mut changes: Vec<ProjectionChange>,
    ) {
        let projection = &mut self.checkpoint.projection;
        for response in projection
            .responses
            .iter()
            .filter(|r| r.phase.is_terminal())
        {
            // Terminal phase/generation guards replace per-token duplicate IDs.
            self.checkpoint.seen.remove(&response.turn_id);
        }
        for response in &mut projection.responses {
            let works: Vec<_> = projection
                .works
                .iter()
                .filter(|work| work.origin_turn_id.as_ref() == Some(&response.turn_id))
                .collect();
            let ids: Vec<_> = works.iter().map(|work| work.work_id.clone()).collect();
            let mut counts = WorkCounts::default();
            for work in works {
                match work.phase {
                    WorkPhase::Completed => counts.completed += 1,
                    WorkPhase::Unknown => counts.unknown += 1,
                    phase if phase.is_terminal() => counts.unsuccessful += 1,
                    _ => counts.active += 1,
                }
            }
            if response.work_ids != ids || response.work_counts != counts {
                response.work_ids = ids;
                response.work_counts = counts;
                response.revision += 1;
                changes.push(ProjectionChange::Response {
                    response: response.clone(),
                });
            }
        }
        while projection
            .responses
            .iter()
            .filter(|r| r.phase.is_terminal())
            .count()
            > 200
        {
            let index = projection
                .responses
                .iter()
                .position(|r| r.phase.is_terminal())
                .unwrap();
            let response = projection.responses.remove(index);
            self.checkpoint.retired_responses.insert(
                response.turn_id.clone(),
                (response.generation, response.phase),
            );
            changes.push(ProjectionChange::RemoveResponse {
                turn_id: response.turn_id,
            });
        }
        while projection
            .works
            .iter()
            .filter(|w| {
                w.phase.is_terminal()
                    && !projection
                        .responses
                        .iter()
                        .any(|r| w.origin_turn_id.as_ref() == Some(&r.turn_id))
            })
            .count()
            > 200
        {
            let index = projection
                .works
                .iter()
                .position(|w| {
                    w.phase.is_terminal()
                        && !projection
                            .responses
                            .iter()
                            .any(|r| w.origin_turn_id.as_ref() == Some(&r.turn_id))
                })
                .unwrap();
            let work = projection.works.remove(index);
            self.checkpoint
                .retired_works
                .insert(work.work_id.clone(), (work.generation, work.assignment));
            projection
                .progress
                .retain(|progress| progress.scope != work.todo_scope);
            changes.push(ProjectionChange::RemoveProgress {
                scope: work.todo_scope,
            });
            changes.push(ProjectionChange::RemoveWork {
                work_id: work.work_id,
            });
        }
        self.revision.sequence = self
            .revision
            .sequence
            .checked_add(1)
            .expect("revision exhausted");
        let update = Update {
            revision: self.revision(),
            session_id,
            event,
            changes,
        };
        let size = serde_json::to_vec(&update)
            .expect("serializable update")
            .len();
        self.bytes += size;
        self.updates.push_back((update, size));
        while self.updates.len() > 256 || self.bytes > 4 * 1024 * 1024 {
            let (update, size) = self.updates.pop_front().unwrap();
            self.bytes -= size;
            self.floor = update.revision.sequence;
        }
    }

    pub fn replay(&self, after: &Revision) -> Result<Vec<Update>, Revision> {
        if after.epoch != self.revision.epoch
            || after.sequence > self.revision.sequence
            || after.sequence < self.floor
        {
            return Err(self.revision());
        }
        Ok(self
            .updates
            .iter()
            .filter(|(u, _)| u.revision.sequence > after.sequence)
            .map(|(u, _)| u.clone())
            .collect())
    }
}
