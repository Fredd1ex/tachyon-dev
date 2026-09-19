//! Count and time bounds preserve FIFO events while yielding to input/rendering.
#[cfg(test)]
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

pub(super) const EVENT_LIMIT: usize = 256;
pub(super) const INPUT_LIMIT: usize = 32;

pub(super) fn frame_due(dirty: bool, input: bool, elapsed: Duration) -> bool {
    dirty && elapsed >= Duration::from_millis(if input { 16 } else { 33 })
}

pub(super) fn budget() -> impl Iterator<Item = ()> {
    let started = Instant::now();
    (0..EVENT_LIMIT)
        .map_while(move |_| (started.elapsed() < Duration::from_millis(4)).then_some(()))
}

/// Alternate priority across frames as well as within a batch.
pub(super) fn next<T>(
    prefer_first: &mut bool,
    first: impl FnOnce() -> Option<T>,
    second: impl FnOnce() -> Option<T>,
) -> Option<T> {
    let first_now = *prefer_first;
    *prefer_first = !first_now;
    if first_now {
        first().or_else(second)
    } else {
        second().or_else(first)
    }
}

#[cfg(test)]
pub(super) fn batch<T>(receiver: &Receiver<T>) -> impl Iterator<Item = T> + '_ {
    let started = Instant::now();
    batch_with_clock(receiver, move || started.elapsed())
}

#[cfg(test)]
fn batch_with_clock<'a, T>(
    receiver: &'a Receiver<T>,
    mut elapsed: impl FnMut() -> Duration + 'a,
) -> impl Iterator<Item = T> + 'a {
    (0..EVENT_LIMIT).map_while(move |_| {
        if elapsed() >= Duration::from_millis(4) {
            None
        } else {
            receiver.try_recv().ok()
        }
    })
}

pub(super) fn inputs<T>(
    mut poll: impl FnMut(Duration) -> std::io::Result<bool>,
    mut read: impl FnMut() -> std::io::Result<T>,
) -> impl Iterator<Item = std::io::Result<T>> {
    (0..INPUT_LIMIT).map_while(move |index| {
        match poll(if index == 0 {
            Duration::from_millis(16)
        } else {
            Duration::ZERO
        }) {
            Ok(true) => Some(read()),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{batch, frame_due, EVENT_LIMIT};
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn busy_notification_and_subscription_sources_cannot_starve_each_other() {
        let mut prefer_first = true;
        let received: Vec<_> = (0..6)
            .map(|_| {
                super::next(
                    &mut prefer_first,
                    || Some("notification"),
                    || Some("subscription"),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(
            received,
            [
                "notification",
                "subscription",
                "notification",
                "subscription",
                "notification",
                "subscription"
            ]
        );
        assert_eq!(
            super::next(&mut prefer_first, || None, || Some("live")),
            Some("live")
        );
        assert_eq!(
            super::next(&mut prefer_first, || Some("notice"), || None),
            Some("notice")
        );
    }

    #[test]
    fn service_budget_stops_at_exactly_256_or_four_ms_without_dequeuing_final() {
        use super::batch_with_clock;
        let (tx, rx) = mpsc::channel();
        for value in 0..=256 {
            tx.send(value).unwrap();
        }
        drop(tx);
        assert_eq!(
            batch_with_clock(&rx, || Duration::ZERO).collect::<Vec<_>>(),
            (0..256).collect::<Vec<_>>()
        );
        assert!(batch_with_clock(&rx, || Duration::from_millis(4))
            .next()
            .is_none());
        assert_eq!(
            batch_with_clock(&rx, || Duration::from_micros(3999)).collect::<Vec<_>>(),
            vec![256]
        );
        assert!(batch_with_clock(&rx, || Duration::ZERO).next().is_none());

        let (tx, rx) = mpsc::channel();
        for value in ["delta 1", "delta 2", "final"] {
            tx.send(value).unwrap();
        }
        let elapsed = std::cell::Cell::new(Duration::ZERO);
        let mut batch = batch_with_clock(&rx, || elapsed.get());
        assert_eq!(batch.next(), Some("delta 1"));
        elapsed.set(Duration::from_micros(3999));
        assert_eq!(batch.next(), Some("delta 2"));
        elapsed.set(Duration::from_millis(4));
        assert_eq!(batch.next(), None);
        drop(batch);
        assert_eq!(
            batch_with_clock(&rx, || Duration::ZERO).collect::<Vec<_>>(),
            vec!["final"]
        );
    }

    #[test]
    fn queued_reply_final_after_budget_yield_replaces_stream_and_renders_once() {
        use crate::app::{
            apply_correlated_agent_event, draw_conversation, selected_chat_cell_text, AgentEvent,
            ItemKind, Thread, TranscriptScroll, TranscriptView, TurnLayoutCache, TurnProjection,
        };
        use ratatui::{backend::TestBackend, Terminal};
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "Question".into(), None);
        thread.reserve_reply();
        crate::app::accept_user_turn(&mut thread, "Question", Some("1".into()));
        let (tx, rx) = mpsc::channel();
        for _ in 0..256 {
            tx.send(AgentEvent::ReplyDelta {
                turn: Some(1),
                text: "x".into(),
            })
            .unwrap();
        }
        tx.send(AgentEvent::Reply {
            turn: Some(1),
            text: "Authoritative final answer".into(),
            final_reply: true,
        })
        .unwrap();
        drop(tx);
        for event in super::batch_with_clock(&rx, || Duration::ZERO) {
            apply_correlated_agent_event(&mut thread, event, None);
        }
        assert!(thread
            .items
            .iter()
            .any(|item| item.kind == ItemKind::Reply && item.text == "x".repeat(256)));
        assert!(!thread.completed_turns.contains("1"));
        let mut terminal = Terminal::new(TestBackend::new(80, 16)).unwrap();
        let mut scroll = TranscriptScroll::default();
        let mut cache = TurnLayoutCache::default();
        let mut view = TranscriptView::default();
        let mut projection = TurnProjection::default();
        let mut draw = |thread: &Thread| {
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        std::slice::from_ref(thread),
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
        };
        draw(&thread);
        assert_eq!(
            super::batch_with_clock(&rx, || Duration::from_millis(4)).count(),
            0
        );
        for event in super::batch_with_clock(&rx, || Duration::ZERO) {
            apply_correlated_agent_event(&mut thread, event, None);
        }
        draw(&thread);
        assert!(!thread
            .items
            .iter()
            .any(|item| item.kind == ItemKind::PendingReply));
        assert!(thread.completed_turns.contains("1"));
        assert_eq!(
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::Reply)
                .count(),
            1
        );
        let copied = selected_chat_cell_text(std::slice::from_ref(&thread), Some(0)).unwrap();
        assert!(copied.contains("Authoritative final answer"));
        assert!(!copied.contains(&"x".repeat(256)));
        let screen = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert_eq!(screen.matches("Authoritative final answer").count(), 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn input_batch_reads_32_then_yields_and_preserves_tail_and_poll_errors() {
        use super::inputs;
        use std::{cell::RefCell, collections::VecDeque, io};
        let queue = RefCell::new((0..65).collect::<VecDeque<_>>());
        let waits = RefCell::new(Vec::new());
        let mut received = Vec::new();
        for expected in [32, 32, 1] {
            waits.borrow_mut().clear();
            let batch = inputs(
                |wait| {
                    waits.borrow_mut().push(wait);
                    Ok(!queue.borrow().is_empty())
                },
                || Ok(queue.borrow_mut().pop_front().unwrap()),
            )
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
            assert_eq!(batch.len(), expected);
            assert_eq!(waits.borrow()[0], Duration::from_millis(16));
            assert!(waits.borrow()[1..]
                .iter()
                .all(|wait| *wait == Duration::ZERO));
            assert_eq!(waits.borrow().len(), if expected == 1 { 2 } else { 32 });
            received.extend(batch);
        }
        assert_eq!(received, (0..65).collect::<Vec<_>>());
        let error = inputs(
            |_| Err(io::Error::from(io::ErrorKind::Interrupted)),
            || -> io::Result<()> { panic!("must not read after poll failure") },
        )
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        let error = inputs(
            |_| Ok(true),
            || -> io::Result<()> { Err(io::Error::from(io::ErrorKind::UnexpectedEof)) },
        )
        .next()
        .unwrap()
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn bursts_coalesce_but_input_gets_the_shorter_frame_deadline() {
        assert!(!frame_due(true, true, Duration::from_millis(15)));
        assert!(frame_due(true, true, Duration::from_millis(16)));
        assert!(!frame_due(true, false, Duration::from_millis(32)));
        assert!(frame_due(true, false, Duration::from_millis(33)));
        assert!(!frame_due(false, true, Duration::from_secs(1)));
    }

    #[test]
    fn bounded_batches_preserve_all_deltas_and_final_in_order() {
        let (tx, rx) = mpsc::channel();
        for value in 0..EVENT_LIMIT * 4 + 1 {
            tx.send(value).unwrap();
        }
        let first = batch(&rx).collect::<Vec<_>>();
        assert!(!first.is_empty());
        assert!(first.len() <= EVENT_LIMIT);
        let mut values = first;
        while values.len() < EVENT_LIMIT * 4 + 1 {
            values.extend(batch(&rx));
        }
        assert_eq!(values, (0..EVENT_LIMIT * 4 + 1).collect::<Vec<_>>());
    }
}
