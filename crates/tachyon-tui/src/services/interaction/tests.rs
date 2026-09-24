use super::*;
use tachyon_api::interaction_manager::{Response, ResponsePhase, Snapshot, Update};

fn revision(sequence: u64) -> Revision {
    Revision {
        epoch: "daemon".into(),
        sequence,
    }
}
fn response(phase: ResponsePhase, answer: &str) -> Response {
    Response {
        session_id: "host".into(),
        turn_id: "host:1".into(),
        command_origin: None,
        generation: 0,
        revision: 1,
        phase,
        answer: answer.into(),
        answer_bytes: answer.len() as u64,
        answer_ref: None,
        pending: Some("Checking the request".into()),
        intents: vec![],
        failure: None,
        final_event_id: None,
        metrics: Default::default(),
        latest_tool: None,
        work_ids: vec![],
        work_counts: Default::default(),
    }
}
fn entry(role: HistoryRole, text: &str) -> tachyon_api::HistoryEntry {
    tachyon_api::HistoryEntry {
        event_id: format!("host:{role:?}"),
        kind: tachyon_api::HistoryKind::Conversation,
        conversation_id: FOREGROUND_ID.into(),
        turn_id: Some("host:1".into()),
        occurred_at_ms: 1000,
        role,
        text: text.into(),
        attention: None,
        task_id: None,
        task_state: None,
    }
}
fn snapshot(sequence: u64, responses: Vec<Response>) -> Frame {
    Frame::Snapshot {
        snapshot: Snapshot {
            revision: revision(sequence),
            conversation_id: FOREGROUND_ID.into(),
            session_id: Some("host".into()),
            host_state: None,
            history: vec![entry(HistoryRole::User, "question")],
            history_content: vec![],
            projection: Projection {
                responses,
                ..Default::default()
            },
            projection_next: None,
        },
    }
}
fn update(sequence: u64, response: Response) -> Frame {
    Frame::Update {
        update: Update {
            revision: revision(sequence),
            session_id: "host".into(),
            event: None,
            changes: vec![ProjectionChange::Response { response }],
        },
    }
}

#[test]
fn canonical_snapshot_replay_two_interfaces_reopen_without_files() {
    let root = tempfile::tempdir().unwrap();
    let legacy = root.path().join("old.visit");
    std::fs::write(&legacy, b"old TUI-only content, deliberately not imported").unwrap();
    let bytes = std::fs::read(&legacy).unwrap();
    for _ in 0..2 {
        let mut app = crate::app::verification::fixture(root.path(), 0);
        app.manager_frame(snapshot(
            0,
            vec![response(ResponsePhase::Answering, "prefix")],
        ));
        assert_eq!(app.threads[0].items[1].text, "prefix");
        app.interaction_disconnected();
        assert_eq!(app.threads[0].items[1].text, "prefix");
        app.manager_frame(snapshot(
            3,
            vec![response(ResponsePhase::Answering, "prefix restored")],
        ));
        let final_frame = update(4, response(ResponsePhase::Completed, "final answer"));
        app.manager_frame(final_frame.clone());
        app.manager_frame(final_frame);
        app.canonical_history(vec![entry(HistoryRole::Assistant, "final answer")]);
        assert_eq!(app.threads[0].items.len(), 2);
        assert_eq!(app.threads[0].items[1].text, "final answer");
        assert!(!app.foreground_busy);
    }
    assert_eq!(std::fs::read(&legacy).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[test]
fn pending_answer_final_never_accept_raw_role_or_unowned_stream() {
    let root = tempfile::tempdir().unwrap();
    let mut app = crate::app::verification::fixture(root.path(), 0);
    app.manager_frame(snapshot(0, vec![response(ResponsePhase::Accepted, "")]));
    for (sequence, phase, answer, kind) in [
        (1, ResponsePhase::Working, "", ItemKind::PendingReply),
        (
            2,
            ResponsePhase::Answering,
            "answer prefix",
            ItemKind::Reply,
        ),
        (3, ResponsePhase::Completed, "final answer", ItemKind::Reply),
    ] {
        for raw in [
            "[user] wrong role",
            "[text] unowned",
            "[agent] raw final",
            "[foreground:error] raw error",
            "[status] control",
        ] {
            app.apply_event(TuiEvent::Line {
                agent_id: FOREGROUND_ID.into(),
                stream: tachyon_api::EventStream::Stdout,
                data: raw.into(),
            });
        }
        app.manager_frame(update(sequence, response(phase, answer)));
        let thread = &app.threads[0];
        assert_eq!(thread.items.len(), 2);
        assert_eq!(thread.items[0].kind, ItemKind::User);
        assert_eq!(thread.items[1].kind, kind);
        let cells = crate::app::build_turn_cells(thread);
        assert_eq!(cells.len(), 1);
        let layout = crate::app::main_conversation_layout(
            thread,
            &cells[0],
            90,
            thread.items[1].timestamp,
            app.foreground_busy,
            "working",
        );
        for span in layout
            .lines
            .iter()
            .flat_map(|l| &l.spans)
            .filter(|s| s.content.contains("answer") || s.content.contains("Checking"))
        {
            assert_ne!(span.style.fg, Some(ratatui::style::Color::Green));
        }
    }
}

#[test]
fn accepted_response_without_recent_prompt_is_still_visible_and_gap_preserves_it() {
    let root = tempfile::tempdir().unwrap();
    let mut app = crate::app::verification::fixture(root.path(), 0);
    let mut frame = snapshot(5, vec![response(ResponsePhase::Accepted, "")]);
    if let Frame::Snapshot { snapshot } = &mut frame {
        snapshot.history.clear();
    }
    app.manager_frame(frame);
    assert_eq!(crate::app::build_turn_cells(&app.threads[0]).len(), 1);
    assert!(app.foreground_busy);
    app.manager_frame(update(
        7,
        response(ResponsePhase::Completed, "must not apply gap"),
    ));
    assert_eq!(app.threads[0].items[0].kind, ItemKind::PendingReply);
    assert!(!app.threads[0].items[0].text.contains("must not apply"));
}
