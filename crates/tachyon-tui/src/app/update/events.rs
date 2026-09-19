//! UI application of service notifications; transport workers remain independent.
use crate::app::model::items::ItemKind;
use crate::app::model::thread::find_or_create_thread;
use crate::app::panels::agents::pane_agent_ids;
use crate::app::panels::tabs::PaneTab;
use crate::app::update::metrics::{qualify_event_turn, record_correlated_metrics};
use crate::app::update::raw_line::{accept_event, classify_line};
use crate::app::update::{apply_actor_event, apply_interaction_event};
use crate::app::{attention, daemon_state_cache, session_archive, turn_activity, App, TuiEvent};
use std::time::Instant;
use tachyon_api::types::EventStream;
use tachyon_api::{InteractionEvent, FOREGROUND_ID, MEMORY_ID};

impl App {
    pub(in crate::app) fn apply_event(&mut self, ev: TuiEvent) -> bool {
        let mut checkpoint = false;
        match ev {
            TuiEvent::Status(snapshot) => {
                if self.daemon.is_none() && snapshot.daemon.is_some() {
                    self.daemon_since = Some(Instant::now());
                } else if snapshot.daemon.is_none() {
                    self.daemon_since = None;
                }
                self.daemon = snapshot.daemon;
                self.agent_infos.clear();
                for agent in snapshot.agents {
                    if agent.id != MEMORY_ID && !self.subscriptions.contains(&agent.id) {
                        if let Err(error) = self.subscriptions.start(agent.id.clone(), Vec::new()) {
                            self.clipboard_notice =
                                Some((format!("Subscribe: {error}"), Instant::now()));
                        }
                    }
                    self.agent_infos.insert(agent.id.clone(), agent);
                }
                if self.daemon.is_some() && !self.subscriptions.contains(FOREGROUND_ID) {
                    if let Err(error) = self
                        .subscriptions
                        .start(FOREGROUND_ID.into(), self.visits.recovery())
                    {
                        self.clipboard_notice =
                            Some((format!("Subscribe: {error}"), Instant::now()));
                    }
                }
                self.scheduled_tasks = snapshot.schedules;
                self.focus = self.focus.min(
                    pane_agent_ids(&self.agent_infos).len()
                        + usize::from(self.agent_infos.contains_key(FOREGROUND_ID)),
                );
            }
            TuiEvent::Attention(result) => {
                self.attention.complete(result, &mut self.threads);
                checkpoint = true;
            }
            TuiEvent::Operational => {
                if let Some(view) = self.operational_worker.take() {
                    if matches!(
                        &view.query,
                        Some(daemon_state_cache::Query::Todos { turn: Some(_), .. })
                    ) && self.inline_checklist_query() != view.query
                    {
                        return false;
                    }
                    if self.operational_query != view.query {
                        self.operational_query = view.query.clone();
                        self.operational_scroll = 0;
                    }
                    self.apply_checklist(&view);
                    self.operational_view = view;
                }
            }
            TuiEvent::Clipboard(outcome) => {
                self.clipboard_notice = Some((outcome.notice(), Instant::now()));
            }
            TuiEvent::Recovered(entry) => {
                if entry.attention.is_some() {
                    checkpoint |= self.attention.receive(entry, &mut self.threads);
                    return checkpoint;
                }
                checkpoint = true;
                let idx = find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
                let turn = entry
                    .turn_id
                    .as_deref()
                    .map(|turn| session_archive::conversation_turn(&entry.conversation_id, turn));
                if let Some(turn) = &turn {
                    self.visits.recovered(turn);
                }
                turn_activity::reply(
                    &mut self.threads[idx],
                    turn,
                    turn_activity::ReplyUpdate::Recovered(entry.text),
                );
                self.foreground_busy = turn_activity::busy(&self.threads[idx]);
            }
            TuiEvent::Interaction {
                agent_id,
                mut envelope,
            } => {
                let identity = (
                    envelope.metadata.conversation_id.clone(),
                    envelope.metadata.message_id.clone(),
                    envelope.metadata.generation,
                );
                // Delivery frames are independent of model turn identity.
                if agent_id == FOREGROUND_ID {
                    if let Some(entry) = attention::publication(&envelope) {
                        checkpoint |= self.attention.receive(entry, &mut self.threads);
                        return checkpoint;
                    }
                }
                if !self.seen_interactions.insert(identity) {
                    return checkpoint;
                }
                self.visits.observe(&envelope);
                if self
                    .live_conversation
                    .observe(&agent_id, &envelope.metadata)
                    && self.pane_tab == PaneTab::Todos
                {
                    self.operational_worker.select(None, &self.sub_out);
                    self.operational_query = None;
                    self.operational_view = daemon_state_cache::View::default();
                    self.operational_scroll = 0;
                }
                checkpoint |= matches!(
                    envelope.event,
                    InteractionEvent::UserTurnAccepted { .. }
                        | InteractionEvent::ConversationFinished { .. }
                );
                let is_foreground = agent_id == FOREGROUND_ID;
                let idx = find_or_create_thread(&mut self.threads, &agent_id, is_foreground, None);
                envelope.metadata.turn_id = envelope.metadata.turn_id.as_deref().map(|turn| {
                    session_archive::conversation_turn(&envelope.metadata.conversation_id, turn)
                });
                apply_interaction_event(&mut self.threads[idx], envelope);
                if is_foreground {
                    self.foreground_busy = turn_activity::busy(&self.threads[idx]);
                }
            }
            TuiEvent::Structured {
                agent_id,
                mut envelope,
            } => {
                if !accept_event(&mut self.seen_events, &envelope) {
                    return checkpoint;
                }
                qualify_event_turn(&mut envelope);
                record_correlated_metrics(&mut self.threads, &envelope);
                let actor = envelope.actor.clone();
                let envelope_turn = envelope.turn_id.clone();
                let event = envelope.kind;
                let is_foreground = agent_id == FOREGROUND_ID;
                let idx = find_or_create_thread(&mut self.threads, &agent_id, is_foreground, None);
                apply_actor_event(
                    &mut self.threads[idx],
                    event,
                    &actor,
                    envelope_turn.as_deref(),
                );
                if is_foreground {
                    self.foreground_busy = turn_activity::busy(&self.threads[idx]);
                }
            }
            TuiEvent::Line {
                agent_id,
                stream,
                data,
            } => {
                let is_foreground = agent_id == FOREGROUND_ID;
                let text = if stream == EventStream::Stderr {
                    format!("⚠ {data}")
                } else if data.starts_with("[ghost:error]") {
                    format!("⚠{}", &data["[ghost:error]".len()..].trim())
                } else if data.starts_with("[foreground:error]") {
                    format!("⚠{}", &data["[foreground:error]".len()..].trim())
                } else {
                    data
                };

                let idx = find_or_create_thread(&mut self.threads, &agent_id, is_foreground, None);
                let thread = &mut self.threads[idx];
                classify_line(thread, &text);
            }
            TuiEvent::Ended { agent_id, summary } => {
                self.subscriptions.end(&agent_id);
                let root = find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
                let changed = self.threads[root].activity.finish_actor(&agent_id);
                self.threads[root].touch();
                let revision = self.threads[root].revision;
                for turn in changed {
                    self.threads[root].metric_revisions.insert(turn, revision);
                }
                let is_foreground = agent_id == FOREGROUND_ID;
                let idx = find_or_create_thread(&mut self.threads, &agent_id, is_foreground, None);
                self.threads[idx].add(ItemKind::System, format!("∎ {summary}"));
            }
            TuiEvent::ChatResult { error: Some(error) } => {
                let idx = find_or_create_thread(&mut self.threads, FOREGROUND_ID, true, None);
                self.threads[idx].add(ItemKind::Error, format!("⚠ {error}"));
            }
            TuiEvent::ChatResult { error: None } => {}
        }
        checkpoint
    }
}
