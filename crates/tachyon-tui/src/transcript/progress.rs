//! Only published host plans belong here; tools and work status are not todos.
use crate::app::{panels::activity::compact, transcript_cache::CellLayout};
use ratatui::{
    style::{Color, Style},
    text::Line,
};

pub(super) fn insert(layout: &mut CellLayout, text: &str, width: u16) {
    let Some(at) = layout.activity_row.filter(|_| !text.is_empty()) else {
        return;
    };
    let indent = if width >= 8 { "    " } else { "" };
    let budget = width.min(92) as usize - indent.len();
    let lines: Vec<_> = std::iter::once("Progress")
        .chain(text.lines())
        .map(|row| {
            Line::styled(
                format!("{indent}{}", compact(row, budget)),
                Style::default().fg(Color::DarkGray),
            )
        })
        .collect();
    layout.activity_row = Some(at + lines.len());
    layout.hits.splice(at..at, vec![None; lines.len()]);
    layout.lines.splice(at..at, lines);
}
