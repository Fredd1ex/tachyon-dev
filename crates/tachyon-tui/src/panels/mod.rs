//! Compact activity surface; never renders conversation bodies.
pub(in crate::app) mod activity;
pub(in crate::app) mod agents;
pub(in crate::app) mod orchestrators;
pub(super) mod tabs;

use super::{foreground_thread, input::Navigation, ui, Thread, TurnProjection};
use activity::{ActivityRow, RowId};
use ratatui::{
    layout::Rect,
    text::Line,
    widgets::{Clear, Paragraph},
    Frame,
};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(super) struct Inspector {
    pub(super) secondary: bool,
    top: usize,
    area: Rect,
    rows: Vec<ActivityRow>,
    lines: Vec<Line<'static>>,
    hits: Vec<Option<RowId>>,
    selected: usize,
    expanded: HashSet<RowId>,
    raw: HashSet<RowId>,
    details: HashMap<(RowId, u64, bool), String>,
    diagnostics: bool,
    key: Option<(String, u16, Vec<u64>)>,
    dirty: bool,
    reveal: bool,
}

impl Inspector {
    #[cfg(test)]
    pub(super) fn scroll_top(&self) -> usize {
        self.top
    }

    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(super) fn navigate(&mut self, action: Navigation) {
        let max = self.lines.len().saturating_sub(self.area.height as usize);
        self.top = match action {
            Navigation::Up(n) => self.top.saturating_sub(n),
            Navigation::Down(n) => self.top.saturating_add(n).min(max),
            Navigation::Latest => max,
        };
    }

    pub(super) fn key(&mut self, code: crossterm::event::KeyCode) -> bool {
        use crossterm::event::KeyCode;
        match code {
            KeyCode::Up => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down => {
                self.selected = (self.selected + 1).min(self.rows.len().saturating_sub(1))
            }
            KeyCode::Enter | KeyCode::Char(' ') => {
                if let Some(row) = self.rows.get(self.selected) {
                    if !self.expanded.remove(&row.id) {
                        self.expanded.insert(row.id);
                    }
                }
            }
            KeyCode::Char('r') => {
                if let Some(row) = self.rows.get(self.selected) {
                    if !self.raw.remove(&row.id) {
                        self.raw.insert(row.id);
                    }
                    self.expanded.insert(row.id);
                }
            }
            KeyCode::Char('d') => self.diagnostics = !self.diagnostics,
            _ => return false,
        }
        self.dirty = true;
        self.reveal = true;
        true
    }

    pub(super) fn click(&mut self, x: u16, y: u16) {
        if !self.area.contains((x, y).into()) {
            return;
        }
        if let Some(Some(id)) = self.hits.get(self.top + usize::from(y - self.area.y)) {
            if let Some(index) = self.rows.iter().position(|row| row.id == *id) {
                self.selected = index;
                self.key(crossterm::event::KeyCode::Enter);
            }
        }
    }

    pub(super) fn draw(
        &mut self,
        f: &mut Frame,
        area: Rect,
        threads: &[Thread],
        projection: &TurnProjection,
        turn: usize,
    ) {
        let Some((ti, thread)) = foreground_thread(threads) else {
            return;
        };
        let Some(cell) = projection.cells.get(turn) else {
            return;
        };
        let scope = thread.items[cell.prompt].turn.as_deref();
        let width = area.width.min(90).saturating_sub(2);
        let key = (
            scope.unwrap_or("").to_owned(),
            width,
            threads.iter().map(|t| t.revision).collect(),
        );
        if self.key.as_ref() != Some(&key) || self.dirty {
            if self.key.as_ref().is_some_and(|old| old.0 != key.0) {
                let secondary = self.secondary;
                self.reset();
                self.secondary = secondary;
            }
            let selected = self.rows.get(self.selected).map(|r| r.id);
            self.rows = activity::project(threads, ti, scope);
            self.selected = selected
                .and_then(|id| self.rows.iter().position(|r| r.id == id))
                .unwrap_or(0);
            self.details.retain(|(id, revision, _), _| {
                self.rows
                    .iter()
                    .any(|r| r.id == *id && r.revision == *revision)
            });
            self.lines.clear();
            self.hits.clear();
            for (index, row) in self.rows.iter().enumerate() {
                for line in activity::wrap(
                    &format!(
                        "{} {} {}",
                        if index == self.selected { ">" } else { " " },
                        if self.expanded.contains(&row.id) {
                            "[-]"
                        } else {
                            "[+]"
                        },
                        row.label
                    ),
                    width,
                ) {
                    self.lines.push(Line::raw(line));
                    self.hits.push(Some(row.id));
                }
                if self.expanded.contains(&row.id) {
                    let raw = self.raw.contains(&row.id);
                    let detail = self
                        .details
                        .entry((row.id, row.revision, raw))
                        .or_insert_with(|| activity::details(threads, row, raw));
                    for line in activity::wrap(detail, width) {
                        self.lines.push(Line::raw(line));
                        self.hits.push(None);
                    }
                }
            }
            if self.rows.is_empty() {
                self.lines
                    .push(Line::raw("No correlated activity for this turn."));
                self.hits.push(None);
            }
            if self.diagnostics {
                let metrics = scope.and_then(|s| thread.metrics.get(s));
                let text = format!(
                    "Turn: {} | foreground tokens: {} | completed ms: {}",
                    scope.unwrap_or("unknown"),
                    metrics
                        .and_then(|m| m.self_usage.as_ref())
                        .map(|u| u.total.to_string())
                        .unwrap_or_else(|| "unavailable".into()),
                    metrics
                        .and_then(|m| m.completed_ms)
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "unavailable".into())
                );
                for line in activity::wrap(&text, width) {
                    self.lines.push(Line::raw(line));
                    self.hits.push(None);
                }
                for receipt in thread
                    .items
                    .iter()
                    .rev()
                    .filter(|item| item.text.starts_with("command #"))
                    .take(16)
                {
                    for line in activity::wrap(
                        &format!("Session receipt: {}", activity::safe(&receipt.text, 1024)),
                        width,
                    ) {
                        self.lines.push(Line::raw(line));
                        self.hits.push(None);
                    }
                }
            }
            self.key = Some(key);
            self.dirty = false;
        }
        let popup = ui::popup_rect(
            area,
            90,
            (self.lines.len().saturating_add(3).min(18) as u16).min(area.height),
        );
        let block = ui::panel_block(" DIAGNOSTICS - Esc closes ");
        self.area = block.inner(popup);
        // Keep the selected header visible on keyboard movement/expansion, without
        // changing the conversation viewport or following new transcript activity.
        self.top = self
            .top
            .min(self.lines.len().saturating_sub(self.area.height as usize));
        if self.reveal {
            if let Some(row) = self.rows.get(self.selected) {
                if let Some(y) = self.hits.iter().position(|hit| *hit == Some(row.id)) {
                    if y < self.top {
                        self.top = y;
                    }
                    if y >= self.top + self.area.height as usize {
                        self.top = y.saturating_sub(self.area.height.saturating_sub(1) as usize);
                    }
                }
            }
            self.reveal = false;
        }
        f.render_widget(Clear, popup);
        f.render_widget(block, popup);
        f.render_widget(
            Paragraph::new(
                self.lines
                    .iter()
                    .skip(self.top)
                    .take(self.area.height as usize)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            self.area,
        );
    }
}

#[cfg(test)]
mod tests;
