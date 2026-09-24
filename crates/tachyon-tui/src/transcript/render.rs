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
        trace: None,
        worker: open_worker.cloned(),
        record: None,
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
            let open = key.trace == Some(index);
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
                if open {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    projection.record.hash(&mut hasher);
                    revision.worker ^= hasher.finish();
                }
            }
            let variant = u8::from(is_latest)
                | (u8::from(open) << 1)
                | (u8::from(selected_worker.is_some()) << 2);
            let height = cache
                .layout(cell_key(cell), revision, variant, || {
                    let layout = transcript::layout::inline_cell_layout(
                        thread_index,
                        index,
                        threads,
                        cell,
                        area.width.saturating_sub(2),
                        if cell.prompt < thread.history_len {
                            0
                        } else {
                            latest_timestamp
                        },
                        is_latest && foreground_busy && cell.prompt >= thread.history_len,
                        foreground_activity,
                        open,
                        selected_worker,
                    );
                    layout
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
    let empty_current = cells.last().is_some_and(|cell| {
        cell.prompt < thread.history_len
            && !thread.items[cell.prompt]
                .turn
                .as_deref()
                .is_some_and(|t| t.starts_with("conversation:foreground:"))
    });
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
    let selected_scope = open_trace
        .and_then(|i| cells.get(i))
        .and_then(|cell| thread.items[cell.prompt].turn.as_deref());
    let focused_record = crate::app::panels::activity::tasks(threads, thread_index, selected_scope)
        .get(projection.record_focus)
        .map(|row| row.id);
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
            visible_lines.extend((skip..skip + take).map(|row| {
                let mut line =
                    elapsed::overlay(layout, row, area.width.saturating_sub(2), frame_ms);
                line.spans.insert(
                    0,
                    Span::styled(
                        if open_trace == Some(index) {
                            "│ "
                        } else {
                            "  "
                        },
                        Style::default().fg(Color::Cyan),
                    ),
                );
                if layout.hits.get(row).is_some_and(|hit| {
                    focused_record.is_some_and(|(t, i)| *hit == Some(ClickTarget::Item(t, i)))
                }) {
                    line.style = Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD);
                }
                line
            }));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::ItemKind;
    use ratatui::{backend::TestBackend, Terminal};

    #[test]
    fn selection_is_a_paint_only_rail_for_pending_final_and_empty_diagnostics() {
        for final_reply in [false, true] {
            for width in [1, 2, 24, 100] {
                let mut thread = Thread::new_foreground();
                thread.add_turn(ItemKind::User, "question".into(), Some("turn".into()));
                if final_reply {
                    thread.finish_reply(
                        "An answer that wraps on a narrow screen.".into(),
                        Some("turn".into()),
                    );
                } else {
                    thread.add_turn(
                        ItemKind::PendingReply,
                        "Working on the answer.".into(),
                        Some("turn".into()),
                    );
                }
                let threads = vec![thread];
                let mut terminal = Terminal::new(TestBackend::new(width, 40)).unwrap();
                let mut scroll = TranscriptScroll::default();
                let mut cache = TurnLayoutCache::default();
                let mut view = TranscriptView::default();
                let mut projection = TurnProjection::default();
                let mut baseline = None;
                let mut builds = 0;
                let mut heights = Vec::new();
                for selected in [None, Some(0), None] {
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
                                selected,
                                None,
                                &mut projection,
                            )
                        })
                        .unwrap();
                    let mut buffer = terminal.backend().buffer().clone();
                    if selected.is_some() {
                        assert!(buffer.content.iter().any(|cell| cell.symbol() == "│"));
                    }
                    for row in buffer.content.chunks_mut(width as usize) {
                        for cell in row.iter_mut().take(2) {
                            cell.reset();
                        }
                    }
                    if let Some(expected) = &baseline {
                        assert_eq!(&buffer, expected);
                        assert_eq!(cache.builds, builds);
                        assert_eq!(view.heights, heights);
                    } else {
                        baseline = Some(buffer);
                        builds = cache.builds;
                        heights = view.heights.clone();
                    }
                    assert!(projection.details.is_none());
                }
            }
        }
    }
}
