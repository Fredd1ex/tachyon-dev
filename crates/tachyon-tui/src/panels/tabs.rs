//! One registry owns tab order, labels, painting, and clipped mouse targets.
use ratatui::text::Span;
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::Paragraph,
    Frame,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PaneTab {
    Foreground,
    Agents,
    Scheduled,
    Memory,
    Todos,
    Resources,
}

const TABS: [(PaneTab, &str); 6] = [
    (PaneTab::Foreground, "ORCHESTRATORS"),
    (PaneTab::Agents, "AGENTS"),
    (PaneTab::Scheduled, "SCHEDULED"),
    (PaneTab::Memory, "MEMORY"),
    (PaneTab::Todos, "TODO"),
    (PaneTab::Resources, "RESOURCES"),
];

impl PaneTab {
    pub(crate) fn adjacent(self, forward: bool) -> Self {
        let index = TABS.iter().position(|(tab, _)| *tab == self).unwrap();
        TABS[if forward {
            (index + 1).min(TABS.len() - 1)
        } else {
            index.saturating_sub(1)
        }]
        .0
    }
}

pub(crate) struct TabStrip(Vec<(PaneTab, String, Rect)>);

impl TabStrip {
    pub(crate) fn new(area: Rect, agents: usize, scheduled: usize) -> Self {
        let left = area.x.saturating_add(1);
        let right = area.right().saturating_sub(1);
        let mut top =
            left.saturating_add(Span::raw(super::super::WINDOW_LOGO_BUTTON).width() as u16 + 1);
        let mut y = area.y;
        let mut entries = Vec::new();
        if area.width < 3 || area.height < 2 {
            return Self(entries);
        }
        for (tab, label) in TABS {
            let label = match tab {
                PaneTab::Agents => format!(" {label} ({agents}) "),
                PaneTab::Scheduled => format!(" {label} ({scheduled}) "),
                _ => format!(" {label} "),
            };
            let width = Span::raw(label.as_str()).width() as u16;
            if top.saturating_add(width) > right && top > left {
                y = y.saturating_add(1);
                top = left;
            }
            if y >= area.bottom().saturating_sub(1) {
                break;
            }
            let rect = Rect::new(top.min(right), y, width.min(right.saturating_sub(top)), 1);
            entries.push((tab, label, rect));
            top = top.saturating_add(width + 1);
        }
        Self(entries)
    }

    pub(crate) fn height(&self, area: Rect) -> u16 {
        self.0
            .last()
            .map(|(_, _, r)| r.y.saturating_sub(area.y) + 1)
            .unwrap_or(1)
    }

    pub(crate) fn hit(&self, x: u16, y: u16) -> Option<PaneTab> {
        self.0
            .iter()
            .find(|(_, _, rect)| x >= rect.x && x < rect.right() && y == rect.y && rect.width > 0)
            .map(|(tab, _, _)| *tab)
    }

    pub(crate) fn draw(&self, f: &mut Frame, selected: PaneTab) {
        for (tab, label, rect) in &self.0 {
            let active = *tab == selected;
            let style = Style::default()
                .fg(if active { Color::Black } else { Color::White })
                .bg(if active { Color::Cyan } else { Color::DarkGray })
                .add_modifier(Modifier::BOLD);
            f.render_widget(Paragraph::new(label.as_str()).style(style), *rect);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{backend::TestBackend, Terminal};

    #[test]
    fn text_only_tab_golden() {
        let strip = TabStrip::new(Rect::new(0, 0, 120, 8), 3, 2);
        assert_eq!(
            strip
                .0
                .iter()
                .map(|(_, label, _)| label.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            " ORCHESTRATORS   AGENTS (3)   SCHEDULED (2)   MEMORY   TODO   RESOURCES "
        );
        for width in [12, 24, 40, 120] {
            let strip = TabStrip::new(Rect::new(0, 0, width, 12), 3, 2);
            let mut terminal = Terminal::new(TestBackend::new(width, 12)).unwrap();
            terminal
                .draw(|f| strip.draw(f, PaneTab::Foreground))
                .unwrap();
            for (tab, label, rect) in strip.0 {
                assert!(label.is_ascii());
                for x in rect.x..rect.right() {
                    let cell = &terminal.backend().buffer()[(x, rect.y)];
                    assert_eq!(
                        (cell.fg, cell.bg),
                        if tab == PaneTab::Foreground {
                            (Color::Black, Color::Cyan)
                        } else {
                            (Color::White, Color::DarkGray)
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn registry_drives_order_and_exact_painted_hits() {
        for agents in [0, 9, 1000] {
            for width in [0, 1, 2, 20, 100] {
                let area = Rect::new(0, 0, width, 8);
                let strip = TabStrip::new(area, agents, 123);
                let mut terminal = Terminal::new(TestBackend::new(width, 8)).unwrap();
                terminal.draw(|f| strip.draw(f, PaneTab::Agents)).unwrap();
                for (index, (tab, _)) in TABS.iter().enumerate() {
                    assert_eq!(tab.adjacent(true), TABS[(index + 1).min(5)].0);
                    assert_eq!(tab.adjacent(false), TABS[index.saturating_sub(1)].0);
                }
                for (tab, label, rect) in &strip.0 {
                    assert_eq!(rect.intersection(area), *rect);
                    for x in rect.x..rect.right() {
                        assert_eq!(strip.hit(x, rect.y), Some(*tab));
                    }
                    assert_eq!(strip.hit(rect.right(), rect.y), None);
                    if rect.width as usize >= Span::raw(label.as_str()).width() {
                        let text: String = (rect.x..rect.right())
                            .map(|x| terminal.backend().buffer()[(x, rect.y)].symbol())
                            .collect();
                        assert_eq!(&text, label);
                    }
                    for x in rect.x..rect.right() {
                        assert_eq!(
                            terminal.backend().buffer()[(x, rect.y)].bg,
                            if *tab == PaneTab::Agents {
                                Color::Cyan
                            } else {
                                Color::DarkGray
                            }
                        );
                    }
                }
                for (_, _, rect) in &strip.0 {
                    assert!(rect.y < area.bottom() - 1);
                }
            }
        }
    }
}
