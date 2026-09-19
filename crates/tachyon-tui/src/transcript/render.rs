//! Height invalidation and visible-row painting. No daemon or filesystem I/O.
use super::{
    cell_key, cell_revision, elapsed, foreground_thread, latest_conversation_timestamp,
    session_archive, should_show_activity, transcript, transcript_content_height,
    worker_turn_revisions, ClickTarget, Thread, TranscriptScroll, TranscriptView, TurnLayoutCache,
    TurnProjection, HITS, VIEW,
};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

pub(super) fn draw_conversation(
    f: &mut Frame,
    area: Rect,
    threads: &[Thread],
    foreground_busy: bool,
    foreground_activity: &str,
    scroll: &mut TranscriptScroll,
    cache: &mut TurnLayoutCache,
    view: &mut TranscriptView,
    open_trace: Option<usize>,
    open_worker: Option<&(usize, String)>,
    projection: &mut TurnProjection,
) {
    let Some((thread_index, thread)) = foreground_thread(threads) else {
        view.attention_hits.clear();
        return;
    };
    projection.update(thread);
    let cells = &projection.cells;
    cache.prepare(area.width, cells, thread.structure_revision);
    if area.height == 0 || area.width == 0 {
        if let Ok(mut guard) = HITS.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = VIEW.lock() {
            *guard = (area.y, area.height);
        }
        *view = TranscriptView {
            viewport: area.height as usize,
            ..TranscriptView::default()
        };
        return;
    }
    let latest = cells.len().saturating_sub(1);
    let latest_timestamp = cells
        .last()
        .map(|cell| latest_conversation_timestamp(thread, cell))
        .unwrap_or(0);
    let key = transcript::LayoutKey {
        width: area.width,
        revision: thread.revision,
        structure: thread.structure_revision,
        trace: open_trace,
        worker: open_worker.cloned(),
        busy: foreground_busy,
        activity: foreground_activity.to_owned(),
        workers: {
            threads
                .iter()
                .filter(|thread| !thread.is_foreground)
                .map(|thread| (thread.id.clone(), thread.revision))
                .collect()
        },
    };
    let changed = cache.frame_key.as_ref() != Some(&key) || view.starts.len() != cells.len();
    let anchor = if changed && !scroll.follow {
        let index = view
            .starts
            .partition_point(|start| *start <= scroll.top)
            .saturating_sub(1);
        cache.order.get(index).cloned().zip(
            view.starts
                .get(index)
                .map(|start| scroll.top.saturating_sub(*start)),
        )
    } else {
        None
    };
    let worker_revisions = if changed {
        worker_turn_revisions(threads)
    } else {
        HashMap::new()
    };
    let unpositioned = view.starts.is_empty();
    let mut heights = std::mem::take(&mut view.heights);
    let mut starts = std::mem::take(&mut view.starts);
    if changed {
        heights.clear();
        starts.clear();
    }
    let mut total_height = if changed {
        0
    } else {
        starts.last().copied().unwrap_or(0) + heights.last().copied().unwrap_or(0)
    };
    if changed {
        #[cfg(test)]
        {
            cache.height_passes += 1;
        }
        for (index, cell) in cells.iter().enumerate() {
            starts.push(total_height);
            let open = open_trace == Some(index);
            let selected_worker = open_worker
                .filter(|(turn, _)| *turn == index)
                .map(|(_, worker)| worker.as_str());
            let is_latest = index == latest && cell.prompt >= thread.history_len;
            let mut revision = cell_revision(thread, cell);
            {
                revision.worker = thread.items[cell.prompt]
                    .turn
                    .as_deref()
                    .and_then(|turn| worker_revisions.get(turn))
                    .copied()
                    .unwrap_or(0);
                if let Some(worker) = selected_worker {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    worker.hash(&mut hasher);
                    revision.worker ^= hasher.finish();
                }
            }
            let variant = u8::from(is_latest)
                | (u8::from(open) << 1)
                | (u8::from(selected_worker.is_some()) << 2);
            let height = cache
                .layout(cell_key(cell), revision, variant, || {
                    let compose = if selected_worker.is_some() {
                        transcript::layout::turn_cell_layout
                    } else {
                        transcript::layout::inline_cell_layout
                    };
                    compose(
                        thread_index,
                        index,
                        threads,
                        cell,
                        area.width,
                        if cell.prompt < thread.history_len {
                            0
                        } else {
                            latest_timestamp
                        },
                        is_latest && foreground_busy && cell.prompt >= thread.history_len,
                        foreground_activity,
                        open,
                        selected_worker,
                    )
                })
                .lines
                .len();
            heights.push(height);
            total_height = total_height.saturating_add(height);
        }
        if let Some(((key, turn), offset)) = anchor {
            if let Some(index) = cells.iter().position(|cell| {
                if let Some(turn) = &turn {
                    thread.items[cell.prompt].turn.as_ref() == Some(turn)
                } else {
                    cell_key(cell) == key
                }
            }) {
                scroll.top = starts[index] + offset.min(heights[index].saturating_sub(1));
            }
        }
        cache.order = cells
            .iter()
            .map(|cell| (cell_key(cell), thread.items[cell.prompt].turn.clone()))
            .collect();
        cache.frame_key = Some(key);
    }
    // An empty current session needs a boundary only after visible history.
    let empty_current = cells
        .last()
        .is_some_and(|cell| cell.prompt < thread.history_len);
    let current_start = total_height;
    let current_separator = empty_current.then(|| {
        [
            Line::raw(""),
            session_archive::separator(
                &format!(
                    "Current session {}",
                    session_archive::date_label(thread.session_started)
                ),
                area.width,
            ),
            Line::raw(""),
        ]
    });
    if empty_current {
        total_height += 3;
    }
    if unpositioned && !scroll.follow {
        if let Some(start) = open_trace.and_then(|index| starts.get(index)) {
            scroll.top = *start;
        }
    }
    scroll.sync(total_height, area.height as usize, thread.revision);
    let show_activity = should_show_activity(scroll, total_height, area.height as usize);
    let viewport = transcript_content_height(area.height, show_activity);
    scroll.sync(total_height, viewport, thread.revision);
    let top = scroll.top;
    let bottom = top.saturating_add(viewport);
    let mut visible_lines = Vec::new();
    let mut visible_hits = Vec::new();
    let frame_ms = elapsed::frame_ms();
    let mut anchor_turn = None;
    let first_visible = starts
        .partition_point(|start| *start <= top)
        .saturating_sub(1);
    for (index, cell) in cells.iter().enumerate().skip(first_visible) {
        let start = starts[index];
        let end = start.saturating_add(heights[index]);
        if end > top && start < bottom {
            anchor_turn.get_or_insert(index);
            let layout = &cache.layouts[&cell_key(cell)].layout;
            let skip = top.saturating_sub(start);
            let take = bottom.min(end).saturating_sub(start + skip);
            visible_lines.extend(
                (skip..skip + take).map(|row| elapsed::overlay(layout, row, area.width, frame_ms)),
            );
            visible_hits.extend(layout.hits.iter().skip(skip).take(take).cloned());
        }
        if end >= bottom {
            break;
        }
    }
    if let Some(lines) = current_separator {
        for (offset, line) in lines.into_iter().enumerate() {
            let row = current_start + offset;
            if row >= top && row < bottom {
                visible_lines.push(line);
                visible_hits.push(None);
            }
        }
    }
    if show_activity {
        visible_lines.push(Line::from(Span::styled(
            "  new activity below",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        )));
        visible_hits.push(None);
    }
    let attention_hits = visible_hits
        .iter()
        .filter(|hit| matches!(hit, Some(ClickTarget::Attention(..))))
        .cloned()
        .collect();
    if let Ok(mut guard) = HITS.lock() {
        *guard = visible_hits;
    }
    if let Ok(mut guard) = VIEW.lock() {
        *guard = (area.y, area.height);
    }
    f.render_widget(Paragraph::new(visible_lines), area);
    *view = TranscriptView {
        attention_hits,
        total_height,
        viewport,
        turns: cells.len(),
        anchor_turn,
        starts,
        heights,
    };
}
