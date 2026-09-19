//! Offline, opt-in full-frame baseline. No daemon, terminal, or provider calls.
use super::{
    draw_conversation, ItemKind, Thread, TranscriptScroll, TranscriptView, TurnLayoutCache,
    TurnProjection,
};
use ratatui::{backend::TestBackend, Terminal};
use std::time::Instant;

#[test]
#[ignore = "opt-in production App::draw timing; use --release --ignored --nocapture"]
fn synthetic_app_frames() {
    let root = tempfile::tempdir().unwrap();
    let mut app = super::verification::fixture(root.path(), 512);
    let mut terminal = Terminal::new(TestBackend::new(100, 32)).unwrap();
    for scenario in ["cold", "warm", "scroll", "resize", "stream"] {
        let frames = if scenario == "cold" { 1 } else { 60 };
        let before = (
            app.transcript_cache.builds,
            app.transcript_cache.height_passes,
        );
        let mut samples = Vec::new();
        for frame in 0..frames {
            match scenario {
                "scroll" => app.transcript_scroll.scroll_up(3),
                "resize" => {
                    terminal
                        .backend_mut()
                        .resize(if frame % 2 == 0 { 60 } else { 100 }, 32);
                    terminal.autoresize().unwrap();
                }
                "stream" => {
                    app.threads[0].add_reply_fragment(" delta".into(), Some("511".into()), false)
                }
                _ => {}
            }
            let start = Instant::now();
            terminal.draw(|f| app.draw(f)).unwrap();
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        let builds = app.transcript_cache.builds - before.0;
        let passes = app.transcript_cache.height_passes - before.1;
        eprintln!(
            "App::draw {scenario}: frames={frames} total_us={} p50_us={} p95_us={} max_us={} layout_builds={builds} height_passes={passes}",
            samples.iter().sum::<u128>() / 1000,
            samples[(frames - 1) / 2] / 1000,
            samples[(frames * 95).div_ceil(100) - 1] / 1000,
            samples[frames - 1] / 1000
        );
        if matches!(scenario, "warm" | "scroll") {
            assert_eq!((builds, passes), (0, 0));
        }
    }
}

#[test]
fn restored_width_revalidates_mutations_and_matches_cold_frames() {
    let mut thread = Thread::new_foreground();
    thread.add_turn(ItemKind::User, "Question".into(), Some("1".into()));
    thread.finish_reply("Answer ".repeat(30), Some("1".into()));
    let mut cache = TurnLayoutCache::default();
    let mut view = TranscriptView::default();
    let mut projection = TurnProjection::default();
    let mut scroll = TranscriptScroll::default();
    for (step, width) in [100, 60, 100, 60, 100, 40, 60].into_iter().enumerate() {
        if step == 3 {
            thread.finish_reply("Updated answer ".repeat(40), Some("1".into()));
        }
        let before = cache.builds;
        let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    std::slice::from_ref(&thread),
                    false,
                    "",
                    &mut scroll,
                    &mut cache,
                    &mut view,
                    None,
                    None,
                    &mut projection,
                )
            })
            .unwrap();
        if step == 2 {
            assert_eq!(
                cache.builds, before,
                "returning to a width reuses its layout"
            );
        }
        if step == 3 || step == 4 {
            assert_eq!(
                cache.builds,
                before + 1,
                "both widths revalidate changed replies"
            );
        }
        let mut cold = Terminal::new(TestBackend::new(width, 24)).unwrap();
        cold.draw(|f| {
            draw_conversation(
                f,
                f.area(),
                std::slice::from_ref(&thread),
                false,
                "",
                &mut TranscriptScroll::default(),
                &mut TurnLayoutCache::default(),
                &mut TranscriptView::default(),
                None,
                None,
                &mut TurnProjection::default(),
            )
        })
        .unwrap();
        assert_eq!(terminal.backend().buffer(), cold.backend().buffer());
    }
}

#[test]
#[ignore = "synthetic timing baseline; run explicitly with --ignored --nocapture"]
fn synthetic_frames() {
    let mut thread = Thread::new_foreground();
    for turn in 0..512 {
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
    let mut threads = vec![thread];
    let mut terminal = Terminal::new(TestBackend::new(100, 32)).unwrap();
    let mut scroll = TranscriptScroll::default();
    let mut cache = TurnLayoutCache::default();
    let mut view = TranscriptView::default();
    let mut projection = TurnProjection::default();
    for scenario in ["cold", "warm", "scroll", "resize", "stream"] {
        let frames = if scenario == "cold" { 1 } else { 60 };
        let before = cache.builds;
        let passes = cache.height_passes;
        let start = Instant::now();
        for frame in 0..frames {
            match scenario {
                "scroll" => scroll.scroll_up(3),
                "resize" => {
                    terminal
                        .backend_mut()
                        .resize(if frame % 2 == 0 { 60 } else { 100 }, 32);
                    terminal.autoresize().unwrap();
                }
                "stream" => {
                    threads[0].add_reply_fragment(" delta".into(), Some("511".into()), false)
                }
                _ => {}
            }
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        &threads,
                        false,
                        "",
                        &mut scroll,
                        &mut cache,
                        &mut view,
                        None,
                        None,
                        &mut projection,
                    )
                })
                .unwrap();
        }
        eprintln!(
            "{scenario}: frames={frames} elapsed_us={} layout_builds={} height_passes={}",
            start.elapsed().as_micros(),
            cache.builds - before,
            cache.height_passes - passes
        );
        if matches!(scenario, "warm" | "scroll") {
            assert_eq!(cache.builds, before, "warm frames must reuse layouts");
            assert_eq!(
                cache.height_passes, passes,
                "warm frames must not inspect all cell revisions"
            );
        }
    }
}

#[test]
fn rendered_history_cells_survive_earlier_insert_resize_and_warm_redraw() {
    let mut thread = Thread::new_foreground();
    for turn in 0..16 {
        let id = Some(format!("turn-{turn}"));
        thread.add_turn(ItemKind::User, format!("Question {turn}"), id.clone());
        thread.finish_reply(
            format!("Answer {turn}: {}", "stable wrapped content ".repeat(12)),
            id,
        );
    }
    let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
    let mut scroll = TranscriptScroll::default();
    let mut cache = TurnLayoutCache::default();
    let mut view = TranscriptView::default();
    let mut projection = TurnProjection::default();
    let draw = |terminal: &mut Terminal<TestBackend>,
                thread: &Thread,
                scroll: &mut TranscriptScroll,
                cache: &mut TurnLayoutCache,
                view: &mut TranscriptView,
                projection: &mut TurnProjection| {
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    std::slice::from_ref(thread),
                    false,
                    "",
                    scroll,
                    cache,
                    view,
                    None,
                    None,
                    projection,
                )
            })
            .unwrap();
    };
    draw(
        &mut terminal,
        &thread,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    scroll.follow = false;
    scroll.top = view.starts[7] + 2;
    draw(
        &mut terminal,
        &thread,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    // Establish the activity indicator before comparing complete terminal buffers.
    thread.add_turn(
        ItemKind::System,
        "late detail".into(),
        Some("turn-0".into()),
    );
    draw(
        &mut terminal,
        &thread,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    let cells = terminal.backend().buffer().clone();
    let copied = super::selected_chat_cell_text(std::slice::from_ref(&thread), Some(7));
    let late = thread.items.pop().unwrap();
    thread.items.insert(1, late);
    thread.touch_structure();
    draw(
        &mut terminal,
        &thread,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    assert_eq!(scroll.top, view.starts[7] + 2);
    assert_eq!(view.anchor_turn, Some(7));
    assert_eq!(terminal.backend().buffer(), &cells);
    for (width, height) in [(36, 12), (36, 18), (80, 12)] {
        terminal.backend_mut().resize(width, height);
        terminal.autoresize().unwrap();
        draw(
            &mut terminal,
            &thread,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        assert_eq!(scroll.top, view.starts[7] + 2);
        assert_eq!(view.anchor_turn, Some(7));
        assert!(!scroll.follow);
        let warm_cells = terminal.backend().buffer().clone();
        let counters = (cache.builds, cache.height_passes);
        for _ in 0..3 {
            draw(
                &mut terminal,
                &thread,
                &mut scroll,
                &mut cache,
                &mut view,
                &mut projection,
            );
            assert_eq!(terminal.backend().buffer(), &warm_cells);
            assert_eq!((cache.builds, cache.height_passes), counters);
        }
    }
    assert_eq!(terminal.backend().buffer(), &cells);
    assert_eq!(
        super::selected_chat_cell_text(std::slice::from_ref(&thread), Some(7)),
        copied
    );
}

#[test]
fn full_frames_preserve_read_anchor_and_reuse_warm_layouts() {
    let mut thread = Thread::new_foreground();
    for turn in 0..12 {
        thread.add_turn(
            ItemKind::User,
            format!("Question {turn}"),
            Some(turn.to_string()),
        );
        thread.add_turn(
            ItemKind::PendingReply,
            "A response that wraps when the terminal becomes narrow. ".repeat(4),
            Some(turn.to_string()),
        );
    }
    let mut threads = vec![thread];
    let mut terminal = Terminal::new(TestBackend::new(100, 20)).unwrap();
    let mut scroll = TranscriptScroll::default();
    let mut cache = TurnLayoutCache::default();
    let mut view = TranscriptView::default();
    let mut projection = TurnProjection::default();
    let draw = |terminal: &mut Terminal<TestBackend>,
                threads: &[Thread],
                scroll: &mut TranscriptScroll,
                cache: &mut TurnLayoutCache,
                view: &mut TranscriptView,
                projection: &mut TurnProjection| {
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    threads,
                    false,
                    "",
                    scroll,
                    cache,
                    view,
                    None,
                    None,
                    projection,
                )
            })
            .unwrap();
    };
    draw(
        &mut terminal,
        &threads,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    scroll.follow = false;
    scroll.top = view.starts[5] + 2;
    let before = (cache.builds, cache.height_passes);
    draw(
        &mut terminal,
        &threads,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    assert_eq!((cache.builds, cache.height_passes), before);
    let copied = super::selected_chat_cell_text(&threads, Some(5));
    threads[0].add_reply_fragment(
        "Earlier response grew.\n".repeat(30),
        Some("0".into()),
        false,
    );
    draw(
        &mut terminal,
        &threads,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    assert_eq!(scroll.top, view.starts[5] + 2);
    assert_eq!(cache.builds, before.0 + 1);
    // Late correlated insertion changes every later prompt's item index.
    threads[0].add_turn(ItemKind::System, "late telemetry".into(), Some("0".into()));
    let late = threads[0].items.pop().unwrap();
    threads[0].items.insert(1, late);
    draw(
        &mut terminal,
        &threads,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    assert_eq!(scroll.top, view.starts[5] + 2);
    terminal.backend_mut().resize(40, 20);
    terminal.autoresize().unwrap();
    draw(
        &mut terminal,
        &threads,
        &mut scroll,
        &mut cache,
        &mut view,
        &mut projection,
    );
    assert_eq!(scroll.top, view.starts[5] + 2);
    assert_eq!(super::selected_chat_cell_text(&threads, Some(5)), copied);
    assert!(!scroll.follow);
}
