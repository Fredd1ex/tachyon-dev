//! Conversation navigation, unread visibility, and view reset.
use crate::app::model::thread::Thread;
use crate::app::model::TranscriptScroll;
use crate::app::transcript_cache::{TranscriptView, TurnLayoutCache, TurnProjection};
use crate::app::{icon, session_archive};

pub(in crate::app) fn ready_earlier_turn(threads: &[Thread]) -> Option<u64> {
    let thread = threads.iter().find(|thread| thread.is_foreground)?;
    thread
        .unread_turns
        .iter()
        .filter_map(|turn| live_turn_number(turn))
        .min()
}

pub(in crate::app) fn ready_notice(turn: u64) -> String {
    format!("{} response {turn} ready", icon::SUCCESS)
}

pub(in crate::app) fn mark_ready_turn_seen(threads: &mut [Thread], turn: &str) {
    if let Some(thread) = threads.iter_mut().find(|thread| thread.is_foreground) {
        thread.unread_turns.remove(turn);
    }
}

pub(in crate::app) fn live_turn_number(turn: &str) -> Option<u64> {
    if turn.starts_with("visit:") || turn.starts_with("archived:") {
        return None;
    }
    session_archive::display_turn(turn).parse().ok()
}

pub(in crate::app) fn later_turn(candidate: &str, turn: &str) -> bool {
    let namespace = |id: &str| id.rsplit_once(':').map(|(prefix, _)| prefix.to_owned());
    namespace(candidate) == namespace(turn)
        && live_turn_number(candidate)
            .zip(live_turn_number(turn))
            .is_some_and(|(a, b)| a > b)
}

pub(in crate::app) fn mark_visible_ready_turns_seen(
    threads: &mut [Thread],
    projection: &TurnProjection,
    view: &TranscriptView,
    scroll: &TranscriptScroll,
) {
    let Some(thread) = threads.iter_mut().find(|thread| thread.is_foreground) else {
        return;
    };
    let bottom = scroll.top.saturating_add(view.viewport);
    let visible = projection
        .cells
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let start = view.starts.get(*index).copied().unwrap_or(usize::MAX);
            let end = start.saturating_add(view.heights.get(*index).copied().unwrap_or(0));
            start >= scroll.top && end <= bottom
        })
        .filter_map(|(_, cell)| thread.items[cell.prompt].turn.clone())
        .collect::<Vec<_>>();
    for turn in visible {
        thread.unread_turns.remove(&turn);
    }
}

#[cfg(test)]
pub(in crate::app) fn toggle_trace(open_trace: &mut Option<usize>, turn: usize) {
    *open_trace = (*open_trace != Some(turn)).then_some(turn);
}

pub(in crate::app) fn toggle_worker(
    open_worker: &mut Option<(usize, String)>,
    turn: usize,
    worker: String,
) {
    *open_worker = (*open_worker != Some((turn, worker.clone()))).then_some((turn, worker));
}

pub(in crate::app) fn close_trace_details(
    open_trace: &mut Option<usize>,
    open_worker: &mut Option<(usize, String)>,
    scroll: &mut TranscriptScroll,
) -> bool {
    let closed = open_trace.take().is_some() | open_worker.take().is_some();
    if closed {
        scroll.end();
    }
    closed
}

pub(in crate::app) fn ctrl_o_target(view: &TranscriptView, follow: bool) -> Option<usize> {
    if view.turns == 0 {
        None
    } else if follow {
        Some(view.turns - 1)
    } else {
        view.anchor_turn.map(|turn| turn.min(view.turns - 1))
    }
}

pub(in crate::app) fn select_trace_turn(
    open_trace: &mut Option<usize>,
    view: &TranscriptView,
    scroll: &mut TranscriptScroll,
    direction: i8,
) {
    if view.turns == 0 {
        return;
    }
    let current = open_trace.unwrap_or_else(|| {
        if scroll.follow {
            view.turns - 1
        } else {
            view.anchor_turn.unwrap_or(view.turns - 1)
        }
    });
    let target = if open_trace.is_none() {
        current
    } else if direction < 0 {
        current.saturating_sub(1)
    } else if current + 1 < view.turns {
        current + 1
    } else {
        *open_trace = None;
        scroll.end();
        return;
    };
    *open_trace = Some(target);
    scroll.follow = false;
    scroll.new_activity = false;
    scroll.top = view.starts.get(target).copied().unwrap_or(scroll.top);
}

#[cfg(test)]
pub(in crate::app) fn page_trace_turn(
    open_trace: &mut Option<usize>,
    view: &TranscriptView,
    scroll: &mut TranscriptScroll,
    direction: i8,
) {
    let Some(current) = open_trace.as_ref().copied() else {
        select_trace_turn(open_trace, view, scroll, direction);
        return;
    };
    let start = view.starts.get(current).copied().unwrap_or(0);
    let end = start.saturating_add(view.heights.get(current).copied().unwrap_or(0));
    if direction < 0 && scroll.top > start {
        scroll.scroll_up(view.viewport.max(1));
        scroll.top = scroll.top.max(start);
        return;
    }
    if direction > 0 && scroll.top.saturating_add(view.viewport) < end {
        scroll.top = scroll
            .top
            .saturating_add(view.viewport.max(1))
            .min(end.saturating_sub(view.viewport));
        scroll.follow = false;
        return;
    }
    select_trace_turn(open_trace, view, scroll, direction);
}

// ---- session persistence --------------------------------------------------

pub(in crate::app) fn toggle_history(threads: &mut [Thread]) {
    if let Some(thread) = threads.iter_mut().find(|thread| thread.is_foreground) {
        thread.hide_history = !thread.hide_history;
        thread.touch_structure();
    }
}

pub(in crate::app) fn reset_transcript(
    scroll: &mut TranscriptScroll,
    view: &mut TranscriptView,
    cache: &mut TurnLayoutCache,
    open_trace: &mut Option<usize>,
    projection: &mut TurnProjection,
) {
    *scroll = TranscriptScroll::default();
    *view = TranscriptView::default();
    cache.reset();
    *open_trace = None;
    projection.reset();
}

pub(in crate::app) fn foreground_focus(threads: &[Thread]) -> usize {
    threads
        .iter()
        .position(|thread| thread.is_foreground)
        .map(|index| index + 1)
        .unwrap_or(1)
}

// ---- drawing -------------------------------------------------------------
