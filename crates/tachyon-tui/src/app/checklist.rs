//! One on-demand host scope, bound to an exact live cell. Never a todo engine.
use super::{
    daemon_state_cache::{Query, View},
    App, Thread,
};
use tachyon_api::todo::TodoScope;

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
                if view.stale {
                    text = format!("Checklist unknown / stale (last known data)\n{text}");
                } else if view.todo_revision.is_none() {
                    text = "Checklist unknown (loading)".into();
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
        thread.touch();
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
            ItemKind::System,
            "Host work evidence".into(),
            Some(turn.into()),
        );
        thread.items.last_mut().unwrap().work = Some(WorkDetail {
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
        assert!(app.threads[0]
            .checklist
            .as_ref()
            .unwrap()
            .1
            .contains("unknown"));
        view.todo_revision = Some(0);
        app.apply_checklist(&view);
        assert!(app.threads[0].checklist.as_ref().unwrap().1.is_empty());
        view.stale = true;
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
