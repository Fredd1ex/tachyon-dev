//! Canonical manager records mapped to presentation, never reduced from telemetry.
use crate::app::{find_or_create_thread, session_archive, App, ItemKind, Thread, TuiEvent};
use std::collections::{HashMap, HashSet};
use std::io::BufReader;
use std::os::unix::net::UnixStream;
use tachyon_api::interaction_manager::{
    CommandOrigin, Frame, Projection, ProjectionChange, Revision, Submit,
};
use tachyon_api::{
    transport::read_response, ApiResponse, HistoryRole, InteractionEvent, FOREGROUND_ID,
};

pub(in crate::app) const SOURCE: &str = "@interaction";

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(in crate::app) struct State {
    pub(in crate::app) session: Option<String>,
    pub(in crate::app) pending: HashMap<String, Submit>,
    pub(in crate::app) projection: Projection,
    revision: Option<Revision>,
    publications: HashSet<String>,
}

pub(in crate::app) fn pending_key(session: &str, command: &str) -> String {
    format!(
        "pending:{}",
        serde_json::to_string(&(session, command)).unwrap()
    )
}

pub(in crate::app) fn bind(thread: &mut Thread, origin: &CommandOrigin, turn: &str) {
    let pending = pending_key(&origin.session_id, &origin.command_id);
    if !thread
        .items
        .iter()
        .any(|i| i.turn.as_deref() == Some(&pending))
    {
        return;
    }
    thread.touch_structure();
    for kind in [ItemKind::User, ItemKind::PendingReply] {
        let Some(slot) = thread
            .items
            .iter()
            .position(|i| i.turn.as_deref() == Some(&pending) && i.kind == kind)
        else {
            continue;
        };
        let canonical = thread.items.iter().position(|i| {
            i.turn.as_deref() == Some(turn)
                && if kind == ItemKind::User {
                    i.kind == kind
                } else {
                    matches!(i.kind, ItemKind::Reply | ItemKind::PendingReply)
                        && i.attention.is_none()
                }
        });
        if let Some(index) = canonical {
            thread.items.swap(slot, index);
            thread.items.remove(index);
            thread.history_len -= usize::from(index < thread.history_len);
        } else {
            thread.items[slot].turn = Some(turn.into());
            thread.items[slot].revision = thread.revision;
        }
    }
}

/// Runs on the subscription worker, not the terminal thread.
pub(super) fn read(
    reader: &mut BufReader<UnixStream>,
    mut send: impl FnMut(TuiEvent) -> Result<(), String>,
) -> Result<(), String> {
    loop {
        match read_response(reader).map_err(|e| e.to_string())? {
            ApiResponse::InteractionFrame { mut frame } => {
                match &mut frame {
                    Frame::Snapshot { snapshot } => {
                        if snapshot.projection_next.is_some()
                            || !snapshot.history_content.is_empty()
                            || snapshot
                                .projection
                                .responses
                                .iter()
                                .any(|r| r.answer_ref.is_some())
                        {
                            *snapshot = tachyon_client::Client::connect()
                                .map_err(|e| e.to_string())?
                                .assemble_interaction_snapshot(snapshot.clone())
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Frame::Update { update } => {
                        for change in &mut update.changes {
                            if let ProjectionChange::Response { response } = change {
                                if response.answer_ref.is_some() {
                                    tachyon_client::Client::connect()
                                        .map_err(|e| e.to_string())?
                                        .hydrate_response(response)
                                        .map_err(|e| e.to_string())?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                let gap = matches!(frame, Frame::ResnapshotRequired { .. });
                send(TuiEvent::Manager(frame))?;
                if gap {
                    return Err("projection gap; reattach required".into());
                }
            }
            ApiResponse::Error { message, .. } => return Err(message),
            _ => return Err("unexpected manager frame".into()),
        }
    }
}

impl App {
    pub(in crate::app) fn reconcile_commands(&mut self) -> Result<(), String> {
        for (key, command) in &self.interaction.pending {
            if self
                .threads
                .iter()
                .any(|t| t.items.iter().any(|i| i.turn.as_ref() == Some(key)))
            {
                self.controls.submit(
                    "reconcile foreground admission".into(),
                    super::control::Command::Submit(command.clone()),
                )?;
            }
        }
        Ok(())
    }

    pub(in crate::app) fn interaction_disconnected(&mut self) {
        // Preserve the last canonical prefix and phase until an atomic replacement.
        self.interaction.revision = None;
        self.interaction.session = None;
    }

    pub(in crate::app) fn canonical_history(&mut self, entries: Vec<tachyon_api::HistoryEntry>) {
        let idx = find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
        for entry in entries {
            if entry.conversation_id != FOREGROUND_ID
                || entry.kind != tachyon_api::HistoryKind::Conversation
            {
                continue;
            }
            if !self.interaction.publications.insert(entry.event_id.clone()) {
                continue;
            }
            if entry.attention.is_some() {
                self.attention.receive(entry, &mut self.threads);
                continue;
            }
            let turn = entry
                .turn_id
                .as_deref()
                .map(|t| session_archive::conversation_turn(&entry.conversation_id, t));
            let kind = if entry.role == HistoryRole::User {
                ItemKind::User
            } else {
                ItemKind::Reply
            };
            let thread = &mut self.threads[idx];
            let slot = turn.as_ref().and_then(|_| {
                thread.items.iter().position(|i| {
                    i.turn == turn
                        && i.attention.is_none()
                        && (i.kind == kind
                            || (kind == ItemKind::Reply && i.kind == ItemKind::PendingReply))
                })
            });
            thread.touch_structure();
            if let Some(slot) = slot {
                thread.items[slot].kind = kind;
                thread.items[slot].text = entry.text;
                thread.items[slot].timestamp = entry.occurred_at_ms / 1000;
                thread.items[slot].revision = thread.revision;
            } else {
                thread.add_turn(kind, entry.text, turn.clone());
                thread.items.last_mut().unwrap().timestamp = entry.occurred_at_ms / 1000;
            }
            if entry.role == HistoryRole::Assistant {
                if let Some(turn) = turn {
                    thread.completed_turns.insert(turn);
                }
            }
        }
    }

    fn present_projection(&mut self) {
        let idx = find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
        let thread = &mut self.threads[idx];
        let projection = &self.interaction.projection;
        let next: HashMap<_, _> = projection
            .works
            .iter()
            .map(|w| (w.work_id.clone(), w.clone()))
            .collect();
        if thread.canonical_works != next {
            thread.touch_structure();
            thread.items.retain(|i| {
                i.work
                    .as_ref()
                    .is_none_or(|w| next.contains_key(&w.key.work_id))
            });
            thread.history_len = thread.history_len.min(thread.items.len());
            thread.canonical_works = next;
            for work in &projection.works {
                let turn = work
                    .origin_turn_id
                    .as_deref()
                    .map(|t| session_archive::conversation_turn(FOREGROUND_ID, t));
                let text = format!("[{}] {}\n{:?}", work.work_id, work.title, work.phase);
                let slot = thread.items.iter().position(|i| {
                    i.work
                        .as_ref()
                        .is_some_and(|w| w.key.work_id == work.work_id)
                });
                let slot = slot.unwrap_or_else(|| {
                    thread.add_turn(ItemKind::Spawn, text.clone(), turn.clone());
                    thread.items.len() - 1
                });
                let item = &mut thread.items[slot];
                item.text = text;
                item.turn = turn;
                item.revision = thread.revision;
                item.work = Some(crate::app::WorkDetail {
                    raw_open: false,
                    key: crate::app::model::items::AssignmentKey {
                        work_id: work.work_id.clone(),
                        generation: work.generation,
                        assignment: work.assignment,
                    },
                    slot: None,
                    tool: None,
                    timing: work.metrics.timing.clone(),
                    omitted: 0,
                });
            }
        }
        for response in &self.interaction.projection.responses {
            let turn = session_archive::conversation_turn(FOREGROUND_ID, &response.turn_id);
            if let Some(origin) = &response.command_origin {
                bind(thread, origin, &turn);
            }
            let text = if !response.answer.is_empty() {
                response.answer.clone()
            } else if let Some(failure) = &response.failure {
                failure.message.clone()
            } else {
                response
                    .pending
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", response.phase))
            };
            let kind = if response.answer.is_empty() {
                ItemKind::PendingReply
            } else {
                ItemKind::Reply
            };
            let slot = thread.items.iter().position(|i| {
                i.turn.as_deref() == Some(&turn)
                    && matches!(i.kind, ItemKind::Reply | ItemKind::PendingReply)
                    && i.attention.is_none()
            });
            if let Some(slot) = slot {
                if thread.items[slot].kind != kind || thread.items[slot].text != text {
                    thread.touch();
                    thread.items[slot].kind = kind;
                    thread.items[slot].text = text;
                    thread.items[slot].revision = thread.revision;
                }
            } else {
                thread.add_turn(kind, text, Some(turn.clone()));
            }
            if response.phase.is_terminal() {
                thread.completed_turns.insert(turn.clone());
            } else {
                thread.completed_turns.remove(&turn);
            }
            let metrics = thread.metrics.entry(turn.clone()).or_default();
            metrics.failure = response.failure.as_ref().map(|f| f.message.clone());
            metrics.first_visible_ms = response.metrics.first_answer_ms;
            metrics.completed_ms = response.metrics.response_ms;
            metrics.self_usage = response
                .metrics
                .prompt_tokens
                .zip(response.metrics.completion_tokens)
                .zip(response.metrics.total_tokens)
                .map(
                    |((prompt, completion), total)| crate::app::model::metrics::TokenTotals {
                        prompt,
                        completion,
                        total,
                    },
                );
            thread
                .metric_revisions
                .insert(turn.clone(), response.revision);
            let progress: Vec<_> = response
                .work_ids
                .iter()
                .filter_map(|id| projection.works.iter().find(|w| &w.work_id == id))
                .filter_map(|work| {
                    projection
                        .progress
                        .iter()
                        .find(|p| p.scope == work.todo_scope)
                        .map(|p| (work, p))
                })
                .map(|(work, p)| match p.scope_revision {
                    Some(_) => format!(
                        "{}: {} pending, {} active, {} blocked, {} completed, {} cancelled",
                        work.title, p.pending, p.in_progress, p.blocked, p.completed, p.cancelled
                    ),
                    None => format!("{}: progress unknown", work.title),
                })
                .collect();
            let text = progress.join("\n");
            if thread.recorded_checklists.get(&turn) != Some(&text) {
                thread.recorded_checklists.insert(turn.clone(), text);
                thread.touch();
                thread.metric_revisions.insert(turn, thread.revision);
            }
        }
        self.foreground_busy = self
            .interaction
            .projection
            .responses
            .iter()
            .any(|r| !r.phase.is_terminal());
        thread.streaming = self.foreground_busy;
    }

    pub(in crate::app) fn manager_frame(&mut self, frame: Frame) -> bool {
        match frame {
            Frame::ResnapshotRequired { .. } => {
                self.interaction_disconnected();
                false
            }
            Frame::Snapshot { snapshot } => {
                if snapshot.projection_next.is_some() {
                    return false;
                }
                self.interaction.session = snapshot.session_id;
                self.visits.observe(&snapshot.history);
                self.canonical_history(snapshot.history);
                self.interaction.projection = snapshot.projection;
                self.present_projection();
                self.interaction.revision = Some(snapshot.revision);
                true
            }
            Frame::Update { update } => {
                let Some(cursor) = &self.interaction.revision else {
                    return false;
                };
                if cursor.epoch == update.revision.epoch
                    && update.revision.sequence <= cursor.sequence
                {
                    return false;
                }
                if cursor.epoch != update.revision.epoch
                    || update.revision.sequence != cursor.sequence + 1
                {
                    self.interaction_disconnected();
                    self.subscriptions.end(SOURCE);
                    return false;
                }
                if let Some(envelope) = update.event {
                    self.live_conversation
                        .observe(FOREGROUND_ID, &envelope.metadata);
                    if let (Some(origin), Some(turn)) = (
                        &envelope.metadata.command_origin,
                        &envelope.metadata.turn_id,
                    ) {
                        let idx =
                            find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
                        bind(
                            &mut self.threads[idx],
                            origin,
                            &session_archive::conversation_turn(FOREGROUND_ID, turn),
                        );
                    }
                    let publication = match envelope.event {
                        InteractionEvent::UserTurnAccepted { text } => {
                            Some((HistoryRole::User, text))
                        }
                        InteractionEvent::ConversationFinished { text } => {
                            Some((HistoryRole::Assistant, text))
                        }
                        InteractionEvent::UserVisibleNotificationPublished { text } => {
                            Some((HistoryRole::Notification, text))
                        }
                        _ => None,
                    };
                    if let Some((role, text)) = publication {
                        self.canonical_history(vec![tachyon_api::HistoryEntry {
                            event_id: envelope.metadata.message_id,
                            conversation_id: envelope.metadata.conversation_id,
                            turn_id: envelope.metadata.turn_id,
                            occurred_at_ms: envelope.metadata.occurred_at_ms,
                            attention: envelope.metadata.attention,
                            kind: tachyon_api::HistoryKind::Conversation,
                            role,
                            text,
                            task_id: None,
                            task_state: None,
                        }]);
                    }
                }
                let projection = &mut self.interaction.projection;
                for change in update.changes {
                    match change {
                        ProjectionChange::Response { response } => {
                            projection
                                .responses
                                .retain(|r| r.turn_id != response.turn_id);
                            projection.responses.push(response);
                        }
                        ProjectionChange::Work { work } => {
                            projection.works.retain(|w| w.work_id != work.work_id);
                            projection.works.push(work);
                        }
                        ProjectionChange::Progress { progress } => {
                            projection.progress.retain(|p| p.scope != progress.scope);
                            projection.progress.push(progress);
                        }
                        ProjectionChange::RemoveResponse { turn_id } => {
                            projection.responses.retain(|r| r.turn_id != turn_id)
                        }
                        ProjectionChange::RemoveWork { work_id } => {
                            projection.works.retain(|w| w.work_id != work_id)
                        }
                        ProjectionChange::RemoveProgress { scope } => {
                            projection.progress.retain(|p| p.scope != scope)
                        }
                    }
                }
                self.present_projection();
                self.interaction.revision = Some(update.revision);
                true
            }
        }
    }
}
