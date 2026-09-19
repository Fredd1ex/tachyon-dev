use super::{accepts_key, navigation, surface, Navigation, Surface};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

#[test]
fn dispatched_trackpad_history_never_opens_inspector_and_overlays_close_before_quit() {
    use super::{Dispatch, Routing};
    use crate::app::{panels::Inspector, TranscriptScroll, TranscriptView};
    use crossterm::event::{KeyEventKind, MouseEvent, MouseEventKind};
    let mut pane = false;
    let mut info = false;
    let mut help = false;
    let mut trace = None;
    let mut worker = None;
    let mut info_scroll = 0;
    let mut help_scroll = 0;
    let mut inspector = Inspector::default();
    let mut scroll = TranscriptScroll::default();
    scroll.sync(200, 20, 1);
    let view = TranscriptView {
        total_height: 200,
        viewport: 20,
        ..Default::default()
    };
    let mut draft = String::new();
    let mut cursor = 0;
    let mut route = Routing {
        pane: &mut pane,
        info: &mut info,
        help: &mut help,
        trace: &mut trace,
        worker: &mut worker,
        info_scroll: &mut info_scroll,
        help_scroll: &mut help_scroll,
        inspector: &mut inspector,
        scroll: &mut scroll,
        view: &view,
        draft: &mut draft,
        cursor: &mut cursor,
        capture: false,
    };
    let key = |code| Event::Key(KeyEvent::new(code, KeyModifiers::NONE));
    let wheel = |kind| {
        Event::Mouse(MouseEvent {
            kind,
            column: 5,
            row: 5,
            modifiers: KeyModifiers::NONE,
        })
    };
    for (event, expected) in [
        (key(KeyCode::Up), 177),
        (
            Event::Key(KeyEvent::new_with_kind(
                KeyCode::Up,
                KeyModifiers::NONE,
                KeyEventKind::Repeat,
            )),
            174,
        ),
        (key(KeyCode::Down), 177),
        (key(KeyCode::PageUp), 157),
        (key(KeyCode::PageDown), 177),
    ] {
        assert_eq!(route.dispatch(&event), Dispatch::Consumed);
        assert_eq!(route.scroll.top, expected);
        assert_eq!(*route.trace, None);
        assert_eq!(*route.worker, None);
    }
    assert_eq!(
        route.dispatch(&wheel(MouseEventKind::ScrollUp)),
        Dispatch::Remaining
    );
    assert_eq!(
        route.scroll.top, 177,
        "native selection mode ignores mouse reports"
    );
    route.capture = true;
    for (kind, expected) in [
        (MouseEventKind::ScrollUp, 169),
        (MouseEventKind::ScrollDown, 177),
    ] {
        assert_eq!(route.dispatch(&wheel(kind)), Dispatch::Consumed);
        assert_eq!(route.scroll.top, expected);
        assert_eq!(*route.trace, None);
    }
    assert_eq!(
        route.dispatch(&Event::Key(KeyEvent::new_with_kind(
            KeyCode::Up,
            KeyModifiers::NONE,
            KeyEventKind::Release
        ))),
        Dispatch::Consumed
    );
    assert_eq!(route.scroll.top, 177);
    *route.draft = "untouched draft".into();
    *route.cursor = 4;
    *route.help = true;
    *route.info = true;
    *route.pane = true;
    *route.trace = Some(3);
    route.inspector.secondary = true;
    *route.worker = Some((3, "worker".into()));
    for active in [
        Surface::Help,
        Surface::Info,
        Surface::Pane,
        Surface::Inspector,
    ] {
        assert_eq!(
            surface(*route.pane, *route.info, *route.help, route.trace.is_some()),
            active
        );
        for event in [
            key(KeyCode::Char('z')),
            key(KeyCode::Backspace),
            key(KeyCode::Enter),
            Event::Paste("hidden paste".into()),
        ] {
            assert_eq!(
                route.dispatch(&event),
                Dispatch::Consumed,
                "{active:?}: {event:?}"
            );
            assert_eq!(route.draft, "untouched draft");
            assert_eq!(*route.cursor, 4);
        }
        if active != Surface::Pane {
            assert_eq!(route.dispatch(&key(KeyCode::Down)), Dispatch::Consumed);
            assert_eq!(
                route.dispatch(&wheel(MouseEventKind::ScrollDown)),
                Dispatch::Consumed
            );
        }
        assert_eq!(route.scroll.top, 177, "overlay must not scroll history");
        assert_eq!(route.dispatch(&key(KeyCode::Esc)), Dispatch::Consumed);
    }
    assert_eq!(*route.help_scroll, 11);
    assert_eq!(*route.info_scroll, 11);
    assert_eq!(*route.worker, None);
    assert_eq!(route.dispatch(&key(KeyCode::Esc)), Dispatch::Quit);
}

#[test]
fn terminal_wheel_arrows_and_page_keys_are_only_navigation() {
    for (key, expected) in [
        (KeyCode::Up, Navigation::Up(3)),
        (KeyCode::Down, Navigation::Down(3)),
        (KeyCode::PageUp, Navigation::Up(20)),
        (KeyCode::PageDown, Navigation::Down(20)),
    ] {
        assert_eq!(
            navigation(
                &Event::Key(KeyEvent::new(key, KeyModifiers::NONE)),
                true,
                false,
                20
            ),
            Some(expected)
        );
    }
    assert_eq!(
        navigation(
            &Event::Key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)),
            false,
            false,
            20
        ),
        None
    );
}

#[test]
fn topmost_overlay_owns_input_and_does_not_submit_hidden_drafts() {
    assert_eq!(surface(true, true, true, true), Surface::Help);
    assert_eq!(surface(true, true, false, true), Surface::Info);
    assert_eq!(surface(true, false, false, true), Surface::Pane);
    assert_eq!(surface(false, false, false, true), Surface::Inspector);
    for surface in [
        Surface::Help,
        Surface::Info,
        Surface::Pane,
        Surface::Inspector,
    ] {
        for key in [KeyCode::Char('z'), KeyCode::Backspace, KeyCode::Enter] {
            assert!(!accepts_key(
                surface,
                KeyEvent::new(key, KeyModifiers::NONE),
                false
            ));
        }
        assert!(accepts_key(
            surface,
            KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
            false
        ));
    }
}
