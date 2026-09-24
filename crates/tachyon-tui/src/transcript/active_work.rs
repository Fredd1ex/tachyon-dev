//! Compact observed activity, independent of host plan completion.
use crate::app::transcript_cache::CellLayout;
use crate::app::{
    panels::activity::{compact, compact_row, latest, tasks},
    ClickTarget, Thread,
};
use ratatui::{
    style::{Color, Style},
    text::Line,
};

pub(super) fn insert(
    layout: &mut CellLayout,
    threads: &[Thread],
    thread: usize,
    turn: Option<&str>,
    turn_index: usize,
    width: u16,
) {
    let Some(at) = layout.activity_row else {
        return;
    };
    let rows = tasks(threads, thread, turn);
    if rows.is_empty() {
        return;
    }
    let indent = if width >= 8 { "    " } else { "" };
    let budget = width.min(92) as usize - indent.len();
    let mut lines = vec![Line::styled(
        format!("{indent}{}", compact("Active work", budget)),
        Style::default().fg(Color::DarkGray),
    )];
    let mut hits = vec![None];
    for row in rows.iter().take(3) {
        lines.push(Line::styled(
            format!("{indent}{}", compact_row(threads, row, budget)),
            Style::default().fg(Color::Gray),
        ));
        lines.push(Line::styled(
            format!("{indent}{}", latest(threads, thread, row, budget)),
            Style::default().fg(Color::DarkGray),
        ));
        hits.extend(vec![Some(ClickTarget::Item(row.id.0, row.id.1)); 2]);
    }
    if rows.len() > 3 {
        lines.push(Line::raw(format!(
            "{indent}{}",
            compact(
                &format!("+{} more - click for details", rows.len() - 3),
                budget
            )
        )));
        hits.push(Some(ClickTarget::TraceSummary(turn_index)));
    }
    layout.activity_row = Some(at + lines.len());
    layout.hits.splice(at..at, hits);
    layout.lines.splice(at..at, lines);
}
