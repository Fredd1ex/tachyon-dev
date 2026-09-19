//! Shared panel geometry. All rectangles stay inside their parent, even at 0x0.
use ratatui::layout::Rect;
use ratatui::{
    style::{Color, Style},
    text::Line,
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

pub(super) fn text_panel(
    f: &mut Frame,
    popup: Rect,
    title: &'static str,
    lines: Vec<Line<'static>>,
    scroll: &mut u16,
) {
    let block = panel_block(title);
    let max = lines
        .len()
        .saturating_sub(block.inner(popup).height as usize);
    *scroll = (*scroll).min(max.min(u16::MAX as usize) as u16);
    f.render_widget(Clear, popup);
    f.render_widget(
        Paragraph::new(lines).scroll((*scroll, 0)).block(block),
        popup,
    );
}

pub(super) fn panel_block(title: &'static str) -> Block<'static> {
    panel_shell().title(super::popup_title(title))
}

pub(super) fn panel_shell() -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .style(Style::default().bg(Color::Rgb(12, 12, 15)))
}

pub(super) fn popup_rect(area: Rect, percent_x: u16, height: u16) -> Rect {
    let width = ((u32::from(area.width) * u32::from(percent_x.min(100))) / 100) as u16;
    let width = width.min(area.width.saturating_sub(4));
    let height = height.max(3).min(area.height.saturating_sub(2));
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::popup_rect;
    use ratatui::layout::Rect;

    #[test]
    fn percentages_and_tiny_parents_are_safe() {
        assert_eq!(
            popup_rect(Rect::new(7, 9, 100, 40), 72, 18),
            Rect::new(21, 20, 72, 18)
        );
        for width in [0, 1, 2, 3, 4, 10, 100, 1000] {
            for height in 0..40 {
                let area = Rect::new(7, 9, width, height);
                for percent in [0, 58, 72, 100, u16::MAX] {
                    let popup = popup_rect(area, percent, u16::MAX);
                    assert_eq!(popup.intersection(area), popup);
                }
            }
        }
    }

    #[test]
    fn all_panel_renderers_tolerate_tiny_frames_and_help_can_scroll() {
        use crate::app::{
            daemon_state_cache, draw_agent_pane, draw_command_palette, draw_info_panel,
            MouseCapture, PaneTab,
        };
        use ratatui::{backend::TestBackend, Terminal};
        use std::collections::HashMap;
        let config = tachyon_util::config::Config::default();
        for (width, height) in [(0, 0), (1, 1), (2, 2), (3, 3), (20, 5), (100, 30)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            let mut scroll = u16::MAX;
            terminal
                .draw(|f| draw_command_palette(f, f.area(), MouseCapture(false), &mut scroll))
                .unwrap();
            if width == 100 {
                let screen = terminal
                    .backend()
                    .buffer()
                    .content
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(screen.contains("/exit"));
                assert!(scroll > 0);
            }
            terminal
                .draw(|f| {
                    draw_info_panel(
                        f,
                        f.area(),
                        None,
                        None,
                        &HashMap::new(),
                        &[],
                        &config,
                        "test",
                        &mut scroll,
                    )
                })
                .unwrap();
            for tab in [
                PaneTab::Foreground,
                PaneTab::Agents,
                PaneTab::Scheduled,
                PaneTab::Memory,
                PaneTab::Todos,
                PaneTab::Resources,
            ] {
                terminal
                    .draw(|f| {
                        draw_agent_pane(
                            f,
                            popup_rect(f.area(), 72, 18),
                            &[],
                            0,
                            None,
                            None,
                            &HashMap::new(),
                            &[],
                            tab,
                            &daemon_state_cache::View::default(),
                            0,
                        )
                    })
                    .unwrap();
            }
        }
    }
}

pub(in crate::app) mod activity;

pub(in crate::app) mod chrome;

pub(in crate::app) mod format;

pub(in crate::app) mod overlays;
