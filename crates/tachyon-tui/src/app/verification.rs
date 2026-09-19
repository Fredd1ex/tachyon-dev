//! Private offline App fixture, shared by default regressions, profiling and PTY tests.
use super::*;
use std::{io::Write, os::unix::net::UnixStream, path::Path};

pub(super) fn fixture(root: &Path, turns: usize) -> App {
    let (sub_out, sub_rx) = mpsc::channel();
    let (status_requests, _) = mpsc::sync_channel(1);
    let (clipboard, _) = mpsc::sync_channel(1);
    let mut thread = Thread::new_foreground();
    for turn in 0..turns {
        let id = Some(turn.to_string());
        thread.add_turn(ItemKind::User, format!("Question {turn}"), id.clone());
        thread.add_turn(
            ItemKind::Reply,
            "A synthetic **answer** with enough text to wrap on a narrow terminal.\n".repeat(4),
            id.clone(),
        );
        for _ in 0..8 {
            thread.add_turn(
                ItemKind::System,
                "synthetic trace detail".into(),
                id.clone(),
            );
        }
    }
    App {
        probe: None,
        attention: attention::State::open(root).unwrap(),
        visits: session_archive::Visits::open(root).unwrap(),
        threads: vec![thread],
        history: services::history::History::start().unwrap(),
        pages: services::history::Navigator::start().unwrap(),
        controls: services::control::Worker::offline().unwrap(),
        sub_out,
        sub_rx,
        status_requests,
        subscriptions: services::subscriptions::Subscriptions::default(),
        prefer_notifications: true,
        seen_events: HashSet::new(),
        seen_interactions: HashSet::new(),
        agent_infos: HashMap::new(),
        scheduled_tasks: Vec::new(),
        config: tachyon_util::config::Config::default(),
        daemon: None,
        daemon_since: None,
        input: String::new(),
        input_cursor: 0,
        foreground_busy: false,
        foreground_activity: "working".into(),
        transcript_scroll: TranscriptScroll::default(),
        transcript_cache: TurnLayoutCache::default(),
        transcript_view: TranscriptView::default(),
        open_trace: None,
        open_worker: None,
        inspector: panels::Inspector::default(),
        turn_projection: TurnProjection::default(),
        pane_open: false,
        pane_tab: PaneTab::Foreground,
        orchestrator_selection: OrchestratorSelection::default(),
        operational_worker: daemon_state_cache::Worker::default(),
        operational_query: None,
        operational_view: daemon_state_cache::View::default(),
        operational_scroll: 0,
        live_conversation: daemon_state_cache::CurrentConversation::default(),
        commands_open: false,
        info_open: false,
        commands_scroll: 0,
        info_scroll: 0,
        focus: 1,
        last_poll: Instant::now(),
        last_save: Instant::now(),
        clipboard,
        clipboard_notice: None,
        mouse_capture: MouseCapture::default(),
        redraw: true,
        input_redraw: true,
        last_draw: Instant::now() - Duration::from_secs(1),
    }
}

impl App {
    pub(super) fn observe(&self, event: &str, frame: Option<&ratatui::CompletedFrame<'_>>) {
        let Some(mut socket) = self.probe.as_ref() else {
            return;
        };
        let screen = frame.map(|frame| {
            frame
                .buffer
                .content
                .iter()
                .map(|cell| cell.symbol())
                .collect::<String>()
        });
        let state = serde_json::json!({
            "event": event, "top": self.transcript_scroll.top,
            "follow": self.transcript_scroll.follow, "trace": self.open_trace,
            "anchor": self.transcript_view.anchor_turn,
            "inspector_top": self.inspector.scroll_top(),
            "pane": self.pane_open, "help": self.commands_open,
            "capture": self.mouse_capture.0, "draft": self.input,
            "revision": self.threads[0].revision,
            "size": frame.map(|f| [f.area.width, f.area.height]),
            "inspector_painted": screen.as_ref().is_some_and(|s| s.contains("DIAGNOSTICS")),
        });
        serde_json::to_writer(&mut socket, &state).unwrap();
        socket.write_all(b"\n").unwrap();
    }
}

#[test]
fn full_app_decoded_navigation_and_modal_regression() {
    use crossterm::event::{Event, KeyEvent, MouseEvent, MouseEventKind};
    let root = tempfile::tempdir().unwrap();
    let mut app = fixture(root.path(), 32);
    let mut screen = Terminal::new(ratatui::backend::TestBackend::new(100, 32)).unwrap();
    screen.draw(|f| app.draw(f)).unwrap();
    // handle_input's terminal is used only for explicit terminal commands/click size.
    // No terminal operations occur for these decoded events.
    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(io::stdout()),
        ratatui::TerminalOptions {
            viewport: ratatui::Viewport::Fixed(Rect::new(0, 0, 100, 32)),
        },
    )
    .unwrap();
    let key = |code, modifiers| Event::Key(KeyEvent::new(code, modifiers));
    let top = app.transcript_scroll.top;
    for _ in 0..40 {
        assert!(!app
            .handle_input(key(KeyCode::Up, KeyModifiers::NONE), &mut terminal)
            .unwrap());
        assert_eq!(app.open_trace, None);
    }
    assert_eq!(app.transcript_scroll.top, top - 120);
    app.mouse_capture = MouseCapture(true);
    assert!(!app
        .handle_input(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 5,
                row: 5,
                modifiers: KeyModifiers::NONE,
            }),
            &mut terminal
        )
        .unwrap());
    assert_eq!(app.transcript_scroll.top, top - 112);
    assert_eq!(app.open_trace, None);
    screen.draw(|f| app.draw(f)).unwrap();
    let anchor = app.transcript_view.anchor_turn;
    assert!(!app
        .handle_input(
            key(KeyCode::Char('o'), KeyModifiers::CONTROL),
            &mut terminal
        )
        .unwrap());
    assert_eq!(app.open_trace, anchor);
    assert!(app.open_trace.is_some());
    let turn_index = app.open_trace.unwrap();
    let turn = app.threads[0].items[app.turn_projection.cells[turn_index].prompt]
        .turn
        .clone();
    app.threads[0].add_tool(
        "Search {\"query\":\"London\"}".into(),
        "activity-call".into(),
        turn,
    );
    screen.draw(|f| app.draw(f)).unwrap();
    let copy = selected_chat_cell_text(&app.threads, app.open_trace);
    assert!(!app.inspector.secondary);
    let painted = screen
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect::<String>();
    assert!(!painted.contains("ACTIVITY"));
    assert!(!painted.contains("DIAGNOSTICS"));
    app.handle_input(
        key(KeyCode::Char('d'), KeyModifiers::CONTROL),
        &mut terminal,
    )
    .unwrap();
    screen.draw(|f| app.draw(f)).unwrap();
    assert!(app.inspector.secondary);
    app.input = "unsent draft".into();
    for code in [
        KeyCode::Enter,
        KeyCode::Char('r'),
        KeyCode::Char('d'),
        KeyCode::Char(' '),
        KeyCode::Down,
        KeyCode::Up,
    ] {
        assert!(!app
            .handle_input(key(code, KeyModifiers::NONE), &mut terminal)
            .unwrap());
        screen.draw(|f| app.draw(f)).unwrap();
        assert_eq!(app.input, "unsent draft");
        assert_eq!(app.open_trace, anchor);
        assert_eq!(app.transcript_scroll.top, top - 112);
        assert_eq!(selected_chat_cell_text(&app.threads, app.open_trace), copy);
    }
    assert!(!app
        .handle_input(key(KeyCode::Esc, KeyModifiers::NONE), &mut terminal)
        .unwrap());
    assert_eq!(app.open_trace, None);
    assert_eq!(app.transcript_scroll.top, top - 112);
    assert!(app
        .handle_input(key(KeyCode::Esc, KeyModifiers::NONE), &mut terminal)
        .unwrap());
}

#[cfg(target_os = "linux")]
mod pty;
