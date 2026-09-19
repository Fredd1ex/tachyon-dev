use super::*;
use crate::app::{
    attention, draw_conversation, panels, selected_chat_cell_text, services::history::History,
    session_snapshot, ItemKind,
};
use crossterm::event::KeyEvent;
use std::{
    fs,
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};

struct Fixture {
    directory: tempfile::TempDir,
    archive: PathBuf,
    visits: Visits,
    ui: Ui,
}

#[derive(Default)]
struct Ui {
    threads: Vec<Thread>,
    selected: Option<usize>,
    view: TranscriptView,
    scroll: TranscriptScroll,
    cache: TurnLayoutCache,
    projection: TurnProjection,
}

fn conversation(count: usize) -> Vec<Thread> {
    let mut thread = Thread::new_foreground();
    for index in 0..count {
        let turn = format!("conversation:test:{index}");
        thread.add_turn(
            ItemKind::User,
            format!("question {index}"),
            Some(turn.clone()),
        );
        thread.finish_reply(format!("answer {index}"), Some(turn));
    }
    vec![thread]
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir_in("/tmp/opencode").unwrap();
        let mut previous = Visits::open(directory.path()).unwrap();
        previous.save(&conversation(67)).unwrap();
        let archive = fs::read_dir(directory.path().join("tui-visits"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut visits = Visits::open(directory.path()).unwrap();
        let mut ui = Ui {
            threads: conversation(1),
            ..Ui::default()
        };
        ui.threads[0].items[0].text = "LIVE".into();
        visits.latest(&mut ui.threads).unwrap();
        ui.draw();
        Self {
            directory,
            archive,
            visits,
            ui,
        }
    }
}

impl Ui {
    fn draw(&mut self) {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                draw_conversation(
                    frame,
                    frame.area(),
                    &self.threads,
                    false,
                    "",
                    &mut self.scroll,
                    &mut self.cache,
                    &mut self.view,
                    None,
                    None,
                    &mut self.projection,
                )
            })
            .unwrap();
    }

    fn select(&mut self, nav: &mut Navigator, visits: &Visits, direction: i8) -> bool {
        nav.select(
            visits,
            &self.threads,
            &mut self.selected,
            &self.view,
            &mut self.scroll,
            &mut self.projection,
            direction,
        )
    }

    fn apply(&mut self, result: Loaded, visits: &mut Visits) -> io::Result<()> {
        result.apply(
            visits,
            &mut self.threads,
            &mut self.selected,
            &mut self.view,
            &mut self.scroll,
            &mut self.cache,
            &mut self.projection,
        )
    }

    fn copy(&self) -> Option<String> {
        selected_chat_cell_text(&self.threads, self.selected)
    }
}

fn ready(nav: &mut Navigator) -> Loaded {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = nav.take() {
            return result;
        }
        assert!(Instant::now() < deadline, "loader did not complete");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn blocked_load_keeps_copy_and_input_responsive_and_end_esc_cancel_it() {
    let mut fixture = Fixture::new();
    let (started, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let ui_thread = thread::current().id();
    let mut nav = Navigator::with_loader(move |request| {
        assert_ne!(thread::current().id(), ui_thread);
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
        request.load()
    })
    .unwrap();
    fixture.ui.selected = Some(0);
    let copied = fixture.ui.copy();
    assert!(fixture.ui.select(&mut nav, &fixture.visits, -1));
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(fixture.ui.copy(), copied);
    let mut pane = false;
    let mut info = false;
    let mut help = false;
    let mut worker = None;
    let mut info_scroll = 0;
    let mut help_scroll = 0;
    let mut inspector = panels::Inspector::default();
    let mut draft = String::new();
    let mut cursor = 0;
    for event in [
        Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        Event::Key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE)),
        Event::Paste("still responsive".into()),
    ] {
        let surface = input::surface(pane, info, help, fixture.ui.selected.is_some());
        nav.input(&event, surface, draft.is_empty(), false);
        let mut routing = input::Routing {
            pane: &mut pane,
            info: &mut info,
            help: &mut help,
            trace: &mut fixture.ui.selected,
            worker: &mut worker,
            info_scroll: &mut info_scroll,
            help_scroll: &mut help_scroll,
            inspector: &mut inspector,
            scroll: &mut fixture.ui.scroll,
            view: &fixture.ui.view,
            draft: &mut draft,
            cursor: &mut cursor,
            capture: false,
        };
        assert_eq!(routing.dispatch(&event), input::Dispatch::Consumed);
    }
    assert_eq!(draft, "still responsive");
    assert!(fixture.ui.selected.is_none());
    assert!(fixture.ui.scroll.follow);
    release.send(()).unwrap();
    nav.shutdown().unwrap();
    assert!(nav.take().is_none());
    assert!(fixture.ui.copy().unwrap().contains("LIVE"));
}

#[test]
fn ten_thousand_load_requests_are_bounded_latest_wins_and_reordered_results_are_rejected() {
    let mut fixture = Fixture::new();
    let (started, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut calls = 0;
    let mut nav = Navigator::with_loader(move |request| {
        calls += 1;
        started.send(calls).unwrap();
        if calls == 1 {
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        request.load()
    })
    .unwrap();
    fixture.ui.selected = Some(0);
    fixture.ui.select(&mut nav, &fixture.visits, -1);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let old_generation = nav.generation;
    for _ in 0..10_000 {
        nav.request(
            fixture.visits.request(false, true),
            Selection::Step {
                older: true,
                entering: false,
            },
        );
    }
    {
        let mailbox = nav.shared.0.lock().unwrap();
        assert_eq!(mailbox.pending.as_ref().unwrap().generation, nav.generation);
        assert!(mailbox.result.is_none());
    }
    release.send(()).unwrap();
    let result = ready(&mut nav);
    assert_eq!(result.generation, nav.generation);
    fixture.ui.apply(result, &mut fixture.visits).unwrap();
    assert!(fixture.ui.copy().unwrap().contains("question 63"));
    assert_eq!(rx.try_iter().collect::<Vec<_>>(), [2]);
    // UI-side rejection is independent of the worker-side publication gate.
    nav.shared.0.lock().unwrap().result = Some(Loaded {
        generation: old_generation,
        selection: Selection::Show,
        page: fixture.visits.request(true, true).load(),
    });
    assert!(nav.take().is_none());
    assert!(fixture.ui.copy().unwrap().contains("question 63"));
    nav.shutdown().unwrap();
}

#[test]
fn newer_local_direction_and_hide_show_invalidate_blocked_pages() {
    for hide in [false, true] {
        let mut fixture = Fixture::new();
        let (started, rx) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let mut calls = 0;
        let mut nav = Navigator::with_loader(move |request| {
            calls += 1;
            if calls == 1 {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            request.load()
        })
        .unwrap();
        fixture.ui.selected = Some(0);
        fixture.ui.select(&mut nav, &fixture.visits, -1);
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        if hide {
            nav.toggle(&fixture.visits, &mut fixture.ui.threads);
            assert!(fixture.ui.threads[0].hide_history);
            nav.toggle(&fixture.visits, &mut fixture.ui.threads);
            nav.toggle(&fixture.visits, &mut fixture.ui.threads);
            assert!(fixture.ui.threads[0].hide_history);
        } else {
            assert!(!fixture.ui.select(&mut nav, &fixture.visits, 1));
            assert_eq!(fixture.ui.selected, Some(1));
            assert!(fixture.ui.copy().unwrap().contains("question 65"));
        }
        release.send(()).unwrap();
        nav.shutdown().unwrap();
        assert!(nav.take().is_none());
        assert_eq!(fixture.ui.threads[0].history_len, 6);
    }
}

#[test]
fn async_alt_navigation_matches_existing_cross_page_and_live_controls() {
    let mut fixture = Fixture::new();
    let mut reference = Visits::open(fixture.directory.path()).unwrap();
    let mut ui = Ui {
        threads: conversation(1),
        ..Ui::default()
    };
    ui.threads[0].items[0].text = "LIVE".into();
    reference.latest(&mut ui.threads).unwrap();
    ui.draw();
    let mut nav = Navigator::start().unwrap();
    for direction in std::iter::repeat_n(-1, 72).chain(std::iter::repeat_n(1, 74)) {
        reference
            .select(
                &mut ui.threads,
                &mut ui.selected,
                &mut ui.view,
                &mut ui.scroll,
                &mut ui.cache,
                &mut ui.projection,
                direction,
            )
            .unwrap();
        if fixture.ui.select(&mut nav, &fixture.visits, direction) {
            fixture
                .ui
                .apply(ready(&mut nav), &mut fixture.visits)
                .unwrap();
        }
        assert_eq!(fixture.ui.copy(), ui.copy());
        assert_eq!(fixture.ui.selected, ui.selected);
        assert_eq!(fixture.ui.scroll.follow, ui.scroll.follow);
        assert_eq!(fixture.ui.threads[0].history_len, ui.threads[0].history_len);
        fixture.ui.draw();
        ui.draw();
    }
    // End then Alt+Up re-enters the newest page, not a neighbor of the old cursor.
    fixture.ui.selected = None;
    fixture.ui.scroll.end();
    fixture.ui.select(&mut nav, &fixture.visits, -1);
    assert!(fixture.ui.copy().unwrap().contains("LIVE"));
    assert!(fixture.ui.select(&mut nav, &fixture.visits, -1));
    fixture
        .ui
        .apply(ready(&mut nav), &mut fixture.visits)
        .unwrap();
    assert!(fixture.ui.copy().unwrap().contains("question 66"));
    nav.shutdown().unwrap();
}

#[test]
fn missing_or_corrupt_page_preserves_cursor_selection_copy_and_cached_view() {
    for missing in [false, true] {
        let mut fixture = Fixture::new();
        let bytes = fs::read(&fixture.archive).unwrap();
        if missing {
            fs::remove_file(&fixture.archive).unwrap();
        } else {
            fs::write(&fixture.archive, b"corrupt").unwrap();
        }
        fixture.ui.selected = Some(0);
        let copied = fixture.ui.copy();
        let starts = fixture.ui.view.starts.clone();
        let history = fixture.ui.threads[0].history_len;
        let mut nav = Navigator::start().unwrap();
        assert!(fixture.ui.select(&mut nav, &fixture.visits, -1));
        assert!(fixture
            .ui
            .apply(ready(&mut nav), &mut fixture.visits)
            .is_err());
        assert_eq!(fixture.ui.copy(), copied);
        assert_eq!(fixture.ui.selected, Some(0));
        assert_eq!(fixture.ui.view.starts, starts);
        assert_eq!(fixture.ui.threads[0].history_len, history);
        fs::write(&fixture.archive, bytes).unwrap();
        fixture.ui.select(&mut nav, &fixture.visits, -1);
        fixture
            .ui
            .apply(ready(&mut nav), &mut fixture.visits)
            .unwrap();
        assert!(fixture.ui.copy().unwrap().contains("question 63"));
        assert!(fixture.ui.view.starts.is_empty());
        assert!(fixture.ui.view.attention_hits.is_empty());
        assert!(fixture.ui.cache.layouts.is_empty());
        nav.shutdown().unwrap();
    }
}

#[test]
fn checkpoint_progresses_during_blocked_load_and_apply_preserves_new_live_text() {
    let mut fixture = Fixture::new();
    let (started, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut nav = Navigator::with_loader(move |request| {
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
        request.load()
    })
    .unwrap();
    fixture.ui.selected = Some(0);
    fixture.ui.select(&mut nav, &fixture.visits, -1);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mut history = History::start().unwrap();
    let attention = attention::State::open(fixture.directory.path()).unwrap();
    fixture.ui.threads[0].add(ItemKind::User, "LIVE WHILE LOADING".into());
    history.checkpoint(&mut fixture.visits, &fixture.ui.threads, &attention);
    history.shutdown().unwrap();
    let before = serde_json::to_value(session_snapshot(&fixture.ui.threads)).unwrap();
    release.send(()).unwrap();
    fixture
        .ui
        .apply(ready(&mut nav), &mut fixture.visits)
        .unwrap();
    assert_eq!(
        serde_json::to_value(session_snapshot(&fixture.ui.threads)).unwrap(),
        before
    );
    assert!(fixture.ui.copy().unwrap().contains("question 63"));
    let mut reopened = Visits::open(fixture.directory.path()).unwrap();
    let mut restored = vec![Thread::new_foreground()];
    reopened.latest(&mut restored).unwrap();
    assert!(restored[0]
        .items
        .iter()
        .any(|item| item.text == "LIVE WHILE LOADING"));
    nav.shutdown().unwrap();
}

#[test]
fn shutdown_discards_queued_loads_and_joins_in_flight_without_publishing() {
    let fixture = Fixture::new();
    let (started, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let mut nav = Navigator::with_loader(move |request| {
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(10)).unwrap();
        request.load()
    })
    .unwrap();
    nav.request(fixture.visits.request(true, true), Selection::Show);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    nav.request(fixture.visits.request(false, true), Selection::Show);
    nav.stop();
    release.send(()).unwrap();
    nav.shutdown().unwrap();
    assert!(nav.join.is_none());
    assert!(nav.take().is_none());
    assert!(nav.shared.0.lock().unwrap().pending.is_none());
    assert!(rx.try_recv().is_err());
    nav.request(fixture.visits.request(true, true), Selection::Show);
    assert!(nav.shared.0.lock().unwrap().pending.is_none());
}

#[test]
fn drop_joins_loader_and_show_returns_to_latest_without_changing_live_snapshot() {
    let mut fixture = Fixture::new();
    let (finished, rx) = mpsc::channel();
    let mut nav = Navigator::with_loader(move |request| {
        let result = request.load();
        finished.send(()).unwrap();
        result
    })
    .unwrap();
    fixture.ui.selected = Some(0);
    fixture.ui.select(&mut nav, &fixture.visits, -1);
    fixture
        .ui
        .apply(ready(&mut nav), &mut fixture.visits)
        .unwrap();
    assert!(fixture.ui.copy().unwrap().contains("question 63"));
    let live = serde_json::to_value(session_snapshot(&fixture.ui.threads)).unwrap();
    nav.toggle(&fixture.visits, &mut fixture.ui.threads);
    assert!(!nav.is_loading());
    nav.toggle(&fixture.visits, &mut fixture.ui.threads);
    assert!(nav.is_loading());
    fixture
        .ui
        .apply(ready(&mut nav), &mut fixture.visits)
        .unwrap();
    assert!(!nav.is_loading());
    assert!(fixture.ui.selected.is_none());
    assert!(fixture.ui.scroll.follow);
    assert_eq!(fixture.ui.threads[0].history_len, 6);
    assert_eq!(fixture.ui.threads[0].items[0].text, "question 64");
    assert_eq!(
        serde_json::to_value(session_snapshot(&fixture.ui.threads)).unwrap(),
        live
    );
    drop(nav);
    assert_eq!(rx.try_iter().count(), 2);
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
}

#[test]
fn navigation_invalidation_preserves_copy_resize_and_native_mouse_rules() {
    let mut nav = Navigator::start().unwrap();
    let fixture = Fixture::new();
    let copy = Event::Key(KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));
    let resize = Event::Resize(60, 20);
    let native_wheel = Event::Mouse(crossterm::event::MouseEvent {
        kind: MouseEventKind::ScrollUp,
        column: 0,
        row: 0,
        modifiers: KeyModifiers::NONE,
    });
    let mut release = KeyEvent::new(KeyCode::End, KeyModifiers::NONE);
    release.kind = KeyEventKind::Release;
    for event in [copy, resize, native_wheel, Event::Key(release)] {
        let before = nav.generation;
        nav.input(&event, input::Surface::Transcript, true, false);
        assert_eq!(nav.generation, before);
    }
    for code in [
        KeyCode::End,
        KeyCode::Esc,
        KeyCode::Tab,
        KeyCode::Enter,
        KeyCode::PageUp,
    ] {
        nav.request(fixture.visits.request(true, true), Selection::Show);
        let before = nav.generation;
        nav.input(
            &Event::Key(KeyEvent::new(code, KeyModifiers::NONE)),
            input::Surface::Transcript,
            true,
            false,
        );
        assert!(nav.generation > before);
        assert!(!nav.is_loading());
        assert!(nav.take().is_none());
    }
    nav.shutdown().unwrap();
}
