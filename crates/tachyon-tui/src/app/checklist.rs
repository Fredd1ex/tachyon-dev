//! Bounded on-demand host scopes, bound to an exact live cell. Never a todo engine.
use super::{
    daemon_state_cache::{Query, View},
    App, Thread,
};
use tachyon_api::todo::TodoScope;

pub(super) const MAX_PROGRESS_SCOPES: usize = 8;

#[derive(Default)]
pub(super) struct Progress {
    entries: Vec<(Query, super::daemon_state_cache::Worker, View)>,
    cached: Vec<(Query, View)>,
    omitted: usize,
}

fn work_scope(threads: &[Thread], turn: &str, selected: Option<&str>) -> Option<TodoScope> {
    let mut ids = std::collections::BTreeSet::new();
    for thread in threads {
        ids.extend(thread.activity.work_ids(turn));
        ids.extend(
            thread
                .items
                .iter()
                .filter(|i| i.turn.as_deref() == Some(turn))
                .filter_map(|i| i.work.as_ref().map(|w| w.key.work_id.as_str())),
        );
    }
    let id = match selected {
        Some(id) if ids.contains(id) => id,
        Some(worker) => {
            // A selected worker may have a distinct opaque ID. Only its exact
            // host work records for this turn can translate it, never its name/task.
            let mut linked = threads
                .iter()
                .filter(|t| t.id == worker)
                .flat_map(|t| &t.items)
                .filter(|i| i.turn.as_deref() == Some(turn))
                .filter_map(|i| i.work.as_ref().map(|w| w.key.work_id.as_str()));
            let id = linked.next()?;
            if linked.any(|other| other != id) {
                return None;
            }
            id
        }
        None if ids.len() == 1 => *ids.first()?,
        None => return None,
    };
    (!id.is_empty()).then(|| TodoScope::Work { work_id: id.into() })
}

impl App {
    fn progress_queries(&mut self) -> (Vec<Query>, usize) {
        let Some(thread) = self.threads.iter().find(|t| t.is_foreground) else {
            return (Vec::new(), 0);
        };
        self.turn_projection.update(thread);
        let cell = self
            .open_trace
            .or_else(|| {
                (!self.transcript_scroll.follow)
                    .then_some(self.transcript_view.anchor_turn)
                    .flatten()
            })
            .and_then(|i| self.turn_projection.cells.get(i))
            .or_else(|| {
                self.turn_projection.cells.iter().rev().find(|cell| {
                    thread.items[cell.prompt]
                        .turn
                        .as_ref()
                        .is_some_and(|t| !thread.completed_turns.contains(t))
                })
            });
        let Some(cell) = cell.filter(|c| c.prompt >= thread.history_len) else {
            return (Vec::new(), 0);
        };
        let Some(turn) = thread.items[cell.prompt]
            .turn
            .as_ref()
            .filter(|t| !thread.completed_turns.contains(*t))
        else {
            return (Vec::new(), 0);
        };
        let mut ids = std::collections::BTreeSet::new();
        for source in self
            .threads
            .iter()
            .filter(|source| !source.is_foreground || std::ptr::eq(*source, thread))
        {
            ids.extend(source.activity.work_ids(turn));
            ids.extend(
                source
                    .items
                    .iter()
                    .filter(|i| i.turn.as_ref() == Some(turn))
                    .filter_map(|i| i.work.as_ref().map(|w| w.key.work_id.as_str())),
            );
        }
        ids.remove("");
        let omitted = ids.len().saturating_sub(MAX_PROGRESS_SCOPES);
        (
            ids.into_iter()
                .take(MAX_PROGRESS_SCOPES)
                .map(|id| Query::Todos {
                    scope: TodoScope::Work { work_id: id.into() },
                    cursor: None,
                    turn: Some(turn.clone()),
                })
                .collect(),
            omitted,
        )
    }

    pub(super) fn sync_progress(&mut self) {
        let (queries, omitted) = self.progress_queries();
        let changed = self
            .progress
            .entries
            .iter()
            .map(|e| &e.0)
            .ne(queries.iter())
            || self.progress.omitted != omitted;
        if !changed {
            return;
        }
        self.progress.entries.retain(|(query, _, view)| {
            if queries.contains(query) {
                return true;
            }
            if !view.checklist.is_empty() {
                self.progress.cached.push((query.clone(), view.clone()));
            }
            false
        });
        if self.progress.cached.len() > 32 {
            self.progress
                .cached
                .drain(..self.progress.cached.len() - 32);
        }
        for query in queries {
            if self.progress.entries.iter().any(|e| e.0 == query) {
                continue;
            }
            let mut worker = super::daemon_state_cache::Worker::default();
            worker.select(Some(query.clone()), &self.sub_out);
            let mut snapshot = self
                .progress
                .cached
                .iter()
                .position(|(old, _)| *old == query)
                .map(|index| self.progress.cached.remove(index).1)
                .unwrap_or_default();
            snapshot.stale = !snapshot.checklist.is_empty();
            snapshot.query = Some(query.clone());
            self.progress
                .entries
                .push((query.clone(), worker, snapshot));
        }
        self.progress.entries.sort_by_key(|e| match &e.0 {
            Query::Todos {
                scope: TodoScope::Work { work_id },
                ..
            } => work_id.clone(),
            _ => String::new(),
        });
        self.progress.omitted = omitted;
        self.render_progress();
    }

    pub(super) fn receive_progress(&mut self) {
        // Reconcile before taking results: navigation/turn completion can beat a queued notification.
        self.sync_progress();
        let mut changed = false;
        for (query, worker, view) in &mut self.progress.entries {
            if let Some(next) = worker.take().filter(|v| v.query.as_ref() == Some(query)) {
                if next.stale && next.todo_revision.is_none() && !view.checklist.is_empty() {
                    view.stale = true;
                } else {
                    *view = next;
                }
                changed = true;
            }
        }
        if changed {
            self.render_progress();
        }
    }

    fn render_progress(&mut self) {
        let mut view = View {
            query: self.progress.entries.first().map(|e| e.0.clone()),
            ..Default::default()
        };
        let mut sections = Vec::new();
        for (_, _, snapshot) in &self.progress.entries {
            if snapshot.checklist.is_empty() {
                continue;
            }
            let mut text = snapshot.checklist.clone();
            if snapshot.stale {
                text.push_str("\nLast known data - stale");
            }
            sections.push(text);
        }
        if !sections.is_empty() {
            if self
                .progress
                .entries
                .iter()
                .any(|e| e.2.todo_revision.is_none())
            {
                sections.push("Other work plans unavailable (not counted)".into());
            }
            if self.progress.omitted > 0 {
                sections.push(format!(
                    "+{} work scopes not monitored",
                    self.progress.omitted
                ));
            }
        }
        view.checklist = sections.join("\n");
        self.apply_checklist(&view);
        self.redraw = true;
    }

    pub(super) fn inline_checklist_query(&mut self) -> Option<Query> {
        let thread = self.threads.iter().find(|t| t.is_foreground)?;
        self.turn_projection.update(thread);
        let cell = match self.open_trace {
            Some(index) => self.turn_projection.cells.get(index)?,
            None => self.turn_projection.cells.iter().rev().find(|cell| {
                thread.items[cell.prompt].turn.as_ref().is_some_and(|turn| {
                    !thread.completed_turns.contains(turn)
                        && cell.items.iter().any(|index| {
                            matches!(
                                thread.items[*index].kind,
                                super::ItemKind::PendingReply | super::ItemKind::Reply
                            )
                        })
                })
            })?,
        };
        if cell.prompt < thread.history_len {
            return None;
        }
        let turn = thread.items[cell.prompt].turn.as_deref()?;
        let selected = self
            .open_worker
            .as_ref()
            .filter(|(index, _)| Some(*index) == self.open_trace)
            .map(|(_, id)| id.as_str());
        Some(Query::Todos {
            scope: work_scope(&self.threads, turn, selected)?,
            cursor: None,
            turn: Some(turn.into()),
        })
    }

    pub(super) fn apply_checklist(&mut self, view: &View) {
        let next = match &view.query {
            Some(Query::Todos {
                scope: TodoScope::Work { .. } | TodoScope::Campaign { .. },
                turn: Some(turn),
                ..
            }) => {
                let mut text = view.checklist.clone();
                if view.stale && !text.is_empty() {
                    text = format!("Checklist unknown / stale (last known data)\n{text}");
                }
                Some((turn.clone(), text))
            }
            _ => None,
        };
        let Some(thread) = self.threads.iter_mut().find(|t| t.is_foreground) else {
            return;
        };
        if thread.checklist == next {
            return;
        }
        let old = std::mem::replace(&mut thread.checklist, next);
        if let Some((turn, text)) = &old {
            if !text.is_empty() {
                thread
                    .recorded_checklists
                    .insert(turn.clone(), format!("Recorded plan snapshot\n{text}"));
            }
        }
        if let Some((turn, _)) = &thread.checklist {
            thread.recorded_checklists.remove(turn);
        }
        thread.touch();
        while thread.recorded_checklists.len() > 128 {
            if let Some((turn, _)) = thread.recorded_checklists.pop_first() {
                thread.metric_revisions.insert(turn, thread.revision);
            }
        }
        for turn in old
            .as_ref()
            .map(|(t, _)| t)
            .into_iter()
            .chain(thread.checklist.as_ref().map(|(t, _)| t))
        {
            thread
                .metric_revisions
                .insert(turn.clone(), thread.revision);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{
        model::items::{AssignmentKey, WorkDetail},
        ItemKind,
    };

    fn link(thread: &mut Thread, turn: &str, work: &str) {
        thread.add_turn(
            ItemKind::SpawnResult,
            "Host work evidence".into(),
            Some(turn.into()),
        );
        thread
            .items
            .iter_mut()
            .rev()
            .find(|i| {
                i.turn.as_deref() == Some(turn)
                    && i.work.is_none()
                    && i.text == "Host work evidence"
            })
            .unwrap()
            .work = Some(WorkDetail {
            raw_open: false,
            key: AssignmentKey {
                work_id: work.into(),
                generation: 1,
                assignment: 1,
            },
            slot: None,
            tool: None,
            timing: None,
            omitted: 0,
        });
    }

    #[test]
    fn multi_work_push_updates_survive_worker_focus_and_freeze_without_acceptance() {
        use std::{io::BufReader, os::unix::net::UnixStream, time::Duration};
        use tachyon_api::{
            operational_events::{
                OperationalBatch, OperationalChange, OperationalEvent, OperationalWatermark,
            },
            todo::{Todo, TodoActor, TodoResponse, TodoStatus},
            transport::{read_request, write_response},
            types::{ApiRequest, ApiResponse},
        };
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 2);
        link(&mut app.threads[0], "0", "a");
        link(&mut app.threads[0], "0", "b");
        app.open_trace = Some(0);
        let (queries, omitted) = app.progress_queries();
        assert_eq!(queries.len(), 2);
        assert_eq!(omitted, 0);
        let copy = crate::app::selected_chat_cell_text(&app.threads, Some(0));
        let completed = app.threads[0].completed_turns.clone();
        let mut screen =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 80)).unwrap();
        let mut servers = Vec::new();
        let mut joins = Vec::new();
        let watermark = |sequence| OperationalWatermark {
            instance_id: "fixture".into(),
            sequence,
        };
        for query in &queries {
            let Query::Todos { scope, .. } = query else {
                unreachable!()
            };
            let (client, mut server) = UnixStream::pair().unwrap();
            server
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let (worker, join) = super::super::daemon_state_cache::Worker::test_socket(
                query.clone(),
                client,
                app.sub_out.clone(),
            );
            app.progress
                .entries
                .push((query.clone(), worker, View::default()));
            joins.push(join);
            let mut reader = BufReader::new(server.try_clone().unwrap());
            assert!(
                matches!(read_request(&mut reader).unwrap(), ApiRequest::TodoSnapshot { scope: s, .. } if s == *scope)
            );
            let actor = TodoActor {
                source: "worker".into(),
                actor: "fixture".into(),
            };
            let todo = Todo {
                schema_version: 1,
                id: "same-id-in-each-scope".into(),
                scope: scope.clone(),
                title: format!("Verify {scope:?}"),
                description: String::new(),
                status: TodoStatus::Pending,
                order_key: 1,
                revision: 1,
                created_ms: 1,
                updated_ms: 1,
                created_by: actor.clone(),
                updated_by: actor,
            };
            write_response(
                &mut server,
                &ApiResponse::Todo {
                    response: TodoResponse::List {
                        todos: vec![todo.clone()],
                        scope_revision: 1,
                        watermark: watermark(1),
                        next_cursor: None,
                    },
                },
            )
            .unwrap();
            assert!(
                matches!(read_request(&mut reader).unwrap(), ApiRequest::OperationalSubscribe { scope: s, .. } if s == *scope)
            );
            servers.push((server, todo));
        }
        for _ in 0..2 {
            app.apply_event(app.sub_rx.recv_timeout(Duration::from_secs(2)).unwrap());
            app.receive_progress();
        }
        assert_eq!(
            app.threads[0]
                .checklist
                .as_ref()
                .unwrap()
                .1
                .matches("[ ] pending")
                .count(),
            2
        );
        for (revision, status, label) in [
            (2, TodoStatus::InProgress, "[>] active"),
            (3, TodoStatus::Blocked, "[!] blocked"),
            (4, TodoStatus::Completed, "[x] complete"),
        ] {
            for index in 0..2 {
                app.open_worker = Some((0, if index == 0 { "b" } else { "a" }.into()));
                assert_eq!(app.progress_queries().0, queries);
                let (server, todo) = &mut servers[index];
                todo.revision = revision;
                todo.status = status.clone();
                write_response(
                    server,
                    &ApiResponse::OperationalBatch {
                        batch: OperationalBatch {
                            events: vec![OperationalEvent {
                                schema_version: 1,
                                occurred_at_ms: revision,
                                watermark: watermark(revision),
                                scope: todo.scope.clone(),
                                scope_revision: revision,
                                change: OperationalChange::TodoUpdated { todo: todo.clone() },
                            }],
                            watermark: watermark(revision),
                        },
                    },
                )
                .unwrap();
                app.apply_event(app.sub_rx.recv_timeout(Duration::from_secs(2)).unwrap());
                app.receive_progress();
                let text = &app.threads[0].checklist.as_ref().unwrap().1;
                assert!(text.contains(label), "{text}");
                assert!(text.contains("Work checklist (a)") && text.contains("Work checklist (b)"));
                assert_eq!(app.threads[0].completed_turns, completed);
                screen.draw(|f| app.draw(f)).unwrap();
                let painted: String = screen
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .map(|c| c.symbol())
                    .collect();
                assert!(painted.contains("Active work") && painted.contains("Progress"));
                assert!(painted.contains(label));
                assert_eq!(
                    crate::app::selected_chat_cell_text(&app.threads, Some(0)),
                    copy
                );
            }
        }
        servers[0].0.shutdown(std::net::Shutdown::Both).unwrap();
        app.apply_event(app.sub_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        app.receive_progress();
        assert!(app.threads[0]
            .checklist
            .as_ref()
            .unwrap()
            .1
            .contains("Last known data - stale"));
        // A tool observation is not a plan mutation.
        let plan = app.threads[0].checklist.clone();
        app.threads[0].add_turn(ItemKind::Tool, "Read file.rs".into(), Some("0".into()));
        app.sync_progress();
        assert_eq!(app.threads[0].checklist, plan);
        let (server, todo) = &mut servers[1];
        todo.revision = 5;
        todo.title = "Late response must not replace the recorded plan".into();
        write_response(
            server,
            &ApiResponse::OperationalBatch {
                batch: OperationalBatch {
                    events: vec![OperationalEvent {
                        schema_version: 1,
                        occurred_at_ms: 5,
                        watermark: watermark(5),
                        scope: todo.scope.clone(),
                        scope_revision: 5,
                        change: OperationalChange::TodoUpdated { todo: todo.clone() },
                    }],
                    watermark: watermark(5),
                },
            },
        )
        .unwrap();
        app.open_trace = Some(1);
        app.apply_event(app.sub_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        app.receive_progress();
        assert!(app.progress.entries.is_empty());
        assert!(!app.threads[0].recorded_checklists["0"].contains("Late response"));
        app.threads[0].completed_turns.insert("0".into());
        app.sync_progress();
        assert!(app.progress.entries.is_empty());
        assert!(app.threads[0].recorded_checklists["0"].contains("[x] complete"));
        app.open_trace = Some(1);
        app.receive_progress();
        assert!(app.threads[0].checklist.is_none());
        app.open_trace = Some(0);
        app.threads[0].history_len = app.threads[0].items.len();
        assert!(app.progress_queries().0.is_empty());
        drop(app);
        for join in joins {
            join.join().unwrap();
        }
    }

    #[test]
    fn progress_scope_budget_and_absent_plans_never_create_joke_components() {
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 2);
        for id in 0..MAX_PROGRESS_SCOPES + 3 {
            link(&mut app.threads[0], "0", &format!("w{id:02}"));
        }
        app.open_trace = Some(0);
        let (queries, omitted) = app.progress_queries();
        assert_eq!(queries.len(), MAX_PROGRESS_SCOPES);
        assert_eq!(omitted, 3);
        // Unpublished/failed initial snapshots have no checklist to render.
        for query in queries {
            app.progress.entries.push((
                query.clone(),
                Default::default(),
                View {
                    query: Some(query),
                    stale: true,
                    ..Default::default()
                },
            ));
        }
        app.render_progress();
        assert!(app.threads[0].checklist.as_ref().unwrap().1.is_empty());
        app.open_trace = Some(1);
        assert!(app.progress_queries().0.is_empty());
        for width in [0, 1, 4, 28, 100] {
            let layout = crate::app::transcript::layout::inline_cell_layout(
                0,
                1,
                &app.threads,
                &app.turn_projection.cells[1],
                width,
                0,
                false,
                "",
                false,
                None,
            );
            let text = format!("{:?}", layout.lines);
            assert!(!text.contains("Active work") && !text.contains("Progress"));
        }
    }

    #[test]
    fn live_work_requires_host_task_and_producer_fence_agreement() {
        use tachyon_api::types::{Actor, AgentEvent, EventEnvelope, ToolTelemetryIdentity};
        let mut threads = vec![Thread::new_foreground()];
        let mut event = EventEnvelope {
            event_id: 1,
            sequence: 1,
            occurred_at_ms: 1,
            session_id: "session".into(),
            conversation_id: Some("conversation".into()),
            turn_id: Some("conversation/turn".into()),
            task_id: Some("different-work".into()),
            parent_task_id: None,
            tool_call_id: None,
            actor: Actor::Worker {
                id: "worker".into(),
            },
            kind: AgentEvent::ToolStarted {
                turn: Some(1),
                id: "call".into(),
                name: "Read".into(),
                arguments: "{}".into(),
                identity: Some(ToolTelemetryIdentity {
                    task_id: Some("work".into()),
                    work_id: Some("work".into()),
                    generation: Some(1),
                    assignment: Some(1),
                    attempt_id: Some("attempt".into()),
                }),
            },
        };
        threads[0].activity.record(&event);
        assert_eq!(work_scope(&threads, "conversation/turn", None), None);
        event.task_id = Some("work".into());
        threads[0].activity.record(&event);
        assert_eq!(
            work_scope(&threads, "conversation/turn", None),
            Some(TodoScope::Work {
                work_id: "work".into()
            })
        );
        assert_eq!(work_scope(&threads, "conversation/joke", None), None);
    }

    #[test]
    fn hosted_sessions_keep_raw_tools_badges_and_todo_keys_separate() {
        use tachyon_api::{Actor, AgentEvent, EventEnvelope, ToolTelemetryIdentity, FOREGROUND_ID};
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 0);
        let old = "conversation:foreground:old-host:1";
        let new = "conversation:foreground:new-host:1";
        for turn in [old, new] {
            app.threads[0].add_turn(ItemKind::User, "same prompt".into(), Some(turn.into()));
            app.threads[0].reserve_reply();
            app.threads[0].items.last_mut().unwrap().turn = Some(turn.into());
        }
        let raw = |session: &str, turn: &str, id: u64, actor: Actor, work: Option<&str>, kind| {
            EventEnvelope {
                event_id: id,
                sequence: id,
                occurred_at_ms: id,
                session_id: session.into(),
                conversation_id: Some(FOREGROUND_ID.into()),
                turn_id: Some(turn.into()),
                task_id: work.map(str::to_owned),
                parent_task_id: None,
                tool_call_id: None,
                actor,
                kind,
            }
        };
        for (session, time) in [("old-host", 10), ("new-host", 20)] {
            app.apply_event(crate::app::TuiEvent::Structured {
                agent_id: FOREGROUND_ID.into(),
                envelope: raw(
                    session,
                    &format!("{session}:1"),
                    1,
                    Actor::Foreground,
                    None,
                    AgentEvent::Timing {
                        turn: 999,
                        stage: "first_visible".into(),
                        elapsed_ms: time,
                    },
                ),
            });
            app.apply_event(crate::app::TuiEvent::Structured {
                agent_id: FOREGROUND_ID.into(),
                envelope: raw(
                    session,
                    &format!("{session}:1"),
                    2,
                    Actor::Foreground,
                    None,
                    AgentEvent::ToolStarted {
                        turn: Some(999),
                        id: "front-call".into(),
                        name: "Read".into(),
                        arguments: "{}".into(),
                        identity: None,
                    },
                ),
            });
            app.apply_event(crate::app::TuiEvent::Structured {
                agent_id: FOREGROUND_ID.into(),
                envelope: raw(
                    session,
                    &format!("{session}:1"),
                    3,
                    Actor::Foreground,
                    None,
                    AgentEvent::Usage {
                        turn: None,
                        prompt_tokens: time as u32,
                        completion_tokens: 0,
                        total_tokens: time as u32,
                        context_tokens: 0,
                        context_window: None,
                    },
                ),
            });
        }
        for (id, session, work) in [(3, "new-host", "new-work"), (4, "old-host", "old-work")] {
            app.apply_event(crate::app::TuiEvent::Structured {
                agent_id: "worker".into(),
                envelope: raw(
                    "worker-session",
                    &format!("{session}:1"),
                    id,
                    Actor::Worker {
                        id: "worker".into(),
                    },
                    Some(work),
                    AgentEvent::ToolStarted {
                        turn: Some(999),
                        id: "reused-call".into(),
                        name: "Read".into(),
                        arguments: "{}".into(),
                        identity: Some(ToolTelemetryIdentity {
                            task_id: Some(work.into()),
                            work_id: Some(work.into()),
                            generation: Some(1),
                            assignment: Some(1),
                            attempt_id: None,
                        }),
                    },
                ),
            });
        }
        assert!(app.threads[0].metrics.is_empty());
        assert!(app.threads[0].activity.compact_summary(new).is_empty());
        for index in [0, 1] {
            app.open_trace = Some(index);
            assert_eq!(app.inline_checklist_query(), None);
        }
        app.apply_event(crate::app::TuiEvent::Structured {
            agent_id: FOREGROUND_ID.into(),
            envelope: raw(
                "old-host",
                "old-host:1",
                9,
                Actor::Foreground,
                None,
                AgentEvent::ToolFinished {
                    turn: Some(999),
                    id: "front-call".into(),
                    output: "done".into(),
                    identity: None,
                },
            ),
        });
        assert!(app.threads[0].activity.compact_summary(new).is_empty());
        assert!(app.threads[0].activity.compact_summary(old).is_empty());
    }

    #[test]
    fn global_checklist_is_visible_only_in_existing_pane_and_tiny_frames_are_safe() {
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 1);
        app.operational_view = View {
            query: Some(Query::Todos { scope: TodoScope::Conversation { id: "c".into() }, cursor: None, turn: None }),
            checklist: "Global conversation checklist (c) - not turn-linked\n[ ] pending  Shared actual task\n[>] active  Another task\n[!] blocked  Await access\n+2 more".into(),
            todo_revision: Some(5), ..Default::default()
        };
        for (width, height) in [(100, 36), (28, 18), (4, 8), (1, 1)] {
            let mut screen =
                ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, height)).unwrap();
            for open in [true, false] {
                app.pane_open = open;
                screen.draw(|f| app.draw(f)).unwrap();
                let text: String = screen
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .map(|c| c.symbol())
                    .collect();
                if width == 100 {
                    assert_eq!(text.contains("not turn-linked"), open);
                    assert_eq!(text.contains("Shared actual task"), open);
                }
                assert!(app.threads[0].checklist.is_none());
            }
        }
    }

    #[test]
    fn exact_work_only_never_joke_neighbor_worker_name_or_ambiguous_work() {
        let mut thread = Thread::new_foreground();
        link(&mut thread, "conversation/weather", "host-work");
        thread.add_turn(
            ItemKind::Spawn,
            "worker: host-work".into(),
            Some("conversation/joke".into()),
        );
        let mut threads = vec![thread];
        assert_eq!(
            work_scope(&threads, "conversation/weather", None),
            Some(TodoScope::Work {
                work_id: "host-work".into()
            })
        );
        assert_eq!(work_scope(&threads, "conversation/joke", None), None);
        assert_eq!(work_scope(&threads, "other/weather", None), None);
        assert_eq!(
            work_scope(&threads, "conversation/weather", Some("worker-id")),
            None
        );
        link(&mut threads[0], "conversation/weather", "second-work");
        assert_eq!(work_scope(&threads, "conversation/weather", None), None);
        assert_eq!(
            work_scope(&threads, "conversation/weather", Some("second-work")),
            Some(TodoScope::Work {
                work_id: "second-work".into()
            })
        );
    }

    #[test]
    fn selection_active_stream_and_archive_bind_only_exact_cell() {
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 2);
        link(&mut app.threads[0], "0", "work");
        assert_eq!(app.inline_checklist_query(), None); // newest active cell has no link
        app.open_trace = Some(0);
        let query = app.inline_checklist_query().unwrap();
        assert!(matches!(query, Query::Todos { turn: Some(ref t), .. } if t == "0"));
        app.open_trace = Some(1);
        assert_eq!(app.inline_checklist_query(), None);
        app.open_trace = None;
        app.threads[0].completed_turns.insert("1".into());
        assert_eq!(app.inline_checklist_query(), Some(query)); // streaming reply, no pending slot
        app.threads[0].history_len = app.threads[0].items.len();
        assert_eq!(app.inline_checklist_query(), None);
    }

    #[test]
    fn only_linked_cell_invalidates_and_todos_never_accept_work_or_enter_copy() {
        use crate::app::transcript::projection::cell_revision;
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 2);
        link(&mut app.threads[0], "0", "work");
        app.open_trace = Some(0);
        let query = app.inline_checklist_query();
        let copy = crate::app::selected_chat_cell_text(&app.threads, Some(0));
        let unrelated = cell_revision(&app.threads[0], &app.turn_projection.cells[1]);
        let completed = app.threads[0].completed_turns.clone();
        let mut screen =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 60)).unwrap();
        screen.draw(|f| app.draw(f)).unwrap();
        let baseline = app.transcript_cache.builds;
        for (index, status) in ["[ ] pending", "[x] complete"].iter().enumerate() {
            let view = View {
                query: query.clone(),
                checklist: format!("Work checklist (work)\n{status}  Verify the release"),
                todo_revision: Some(index as u64 + 1),
                ..Default::default()
            };
            app.apply_checklist(&view);
            screen.draw(|f| app.draw(f)).unwrap();
            assert_eq!(app.transcript_cache.builds, baseline + index + 1);
            assert_eq!(
                cell_revision(&app.threads[0], &app.turn_projection.cells[1]),
                unrelated
            );
            assert_eq!(app.threads[0].completed_turns, completed);
            assert!(app.threads[0].metrics.is_empty());
            assert_eq!(
                crate::app::selected_chat_cell_text(&app.threads, Some(0)),
                copy
            );
            let revision = app.threads[0].revision;
            app.apply_checklist(&view);
            screen.draw(|f| app.draw(f)).unwrap();
            assert_eq!(app.threads[0].revision, revision);
            assert_eq!(app.transcript_cache.builds, baseline + index + 1);
        }
        for width in [1, 4, 28, 72] {
            let layout = crate::app::transcript::layout::inline_cell_layout(
                0,
                0,
                &app.threads,
                &app.turn_projection.cells[0],
                width,
                0,
                false,
                "",
                false,
                None,
            );
            for line in layout.lines.iter().filter(|line| {
                line.spans.iter().any(|span| {
                    span.content.contains("checklist") || span.content.contains("Verify")
                })
            }) {
                assert!(line.width() <= width as usize);
            }
            let joke = crate::app::transcript::layout::inline_cell_layout(
                0,
                1,
                &app.threads,
                &app.turn_projection.cells[1],
                width,
                0,
                false,
                "",
                false,
                None,
            );
            assert!(!format!("{:?}", joke.lines).contains("checklist"));
        }
    }

    #[test]
    fn empty_is_not_unknown_and_conversation_scope_never_attaches_to_cell() {
        let root = tempfile::tempdir().unwrap();
        let mut app = crate::app::verification::fixture(root.path(), 1);
        link(&mut app.threads[0], "0", "work");
        let mut view = View {
            query: app.inline_checklist_query(),
            ..Default::default()
        };
        app.apply_checklist(&view);
        assert!(app.threads[0].checklist.as_ref().unwrap().1.is_empty());
        view.todo_revision = Some(0);
        app.apply_checklist(&view);
        assert!(app.threads[0].checklist.as_ref().unwrap().1.is_empty());
        view.stale = true;
        app.apply_checklist(&view);
        assert!(app.threads[0].checklist.as_ref().unwrap().1.is_empty());
        view.checklist = "[ ] pending  Published task".into();
        app.apply_checklist(&view);
        assert!(app.threads[0]
            .checklist
            .as_ref()
            .unwrap()
            .1
            .contains("unknown / stale"));
        view.query = Some(Query::Todos {
            scope: TodoScope::Conversation {
                id: "conversation".into(),
            },
            cursor: None,
            turn: Some("0".into()),
        });
        view.checklist = "Global task must not attach".into();
        app.apply_checklist(&view);
        assert!(app.threads[0].checklist.is_none());
    }
}
