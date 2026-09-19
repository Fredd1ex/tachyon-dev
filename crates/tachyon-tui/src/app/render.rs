//! Whole-frame composition; surface renderers own their content.
use crate::app::navigation::mark_visible_ready_turns_seen;
use crate::app::panels::agents::draw_agent_pane;
use crate::app::panels::tabs::PaneTab;
use crate::app::transcript_render::draw_conversation;
use crate::app::ui::chrome::{draw_input, draw_statusline};
use crate::app::ui::overlays::{draw_command_palette, draw_info_panel};
use crate::app::ui::popup_rect;
use crate::app::App;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

impl App {
    pub(super) fn draw(&mut self, f: &mut Frame) {
        let area = f.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(
                [
                    Constraint::Min(0),
                    Constraint::Length(3),
                    Constraint::Length(1),
                ]
                .as_ref(),
            )
            .split(area);

        let position_selected =
            self.transcript_view.starts.is_empty() && !self.transcript_scroll.follow;
        draw_conversation(
            f,
            chunks[0],
            &self.threads,
            self.foreground_busy,
            &self.foreground_activity,
            &mut self.transcript_scroll,
            &mut self.transcript_cache,
            &mut self.transcript_view,
            self.open_trace,
            self.open_worker.as_ref(),
            &mut self.turn_projection,
        );
        if position_selected {
            if let Some(start) = self
                .open_trace
                .and_then(|turn| self.transcript_view.starts.get(turn))
            {
                self.transcript_scroll.top = *start;
            }
        }
        if !self.pane_open && !self.info_open && !self.commands_open && self.open_trace.is_none() {
            mark_visible_ready_turns_seen(
                &mut self.threads,
                &self.turn_projection,
                &self.transcript_view,
                &self.transcript_scroll,
            );
        }
        if self.pane_open {
            let popup = popup_rect(chunks[0], 72, chunks[0].height.saturating_sub(4).min(18));
            draw_agent_pane(
                f,
                popup,
                &self.threads,
                if self.pane_tab == PaneTab::Foreground {
                    self.orchestrator_selection
                        .index(self.daemon.as_ref())
                        .unwrap_or(usize::MAX)
                } else {
                    self.focus
                },
                self.daemon.as_ref(),
                self.daemon_since,
                &self.agent_infos,
                &self.scheduled_tasks,
                self.pane_tab,
                &self.operational_view,
                self.operational_scroll,
            );
        }
        draw_input(
            f,
            chunks[1],
            &self.input,
            self.input_cursor,
            self.daemon.as_ref(),
            self.foreground_busy,
        );
        draw_statusline(
            f,
            chunks[2],
            self.daemon.as_ref(),
            &self.agent_infos,
            &self.threads,
            self.open_trace,
            self.transcript_scroll.follow,
        );
        if let Some((notice, _)) = &self.clipboard_notice {
            f.render_widget(Paragraph::new(notice.as_str()), chunks[2]);
        } else if self.pages.is_loading() {
            f.render_widget(Paragraph::new("Loading history..."), chunks[2]);
        }

        if self.info_open {
            draw_info_panel(
                f,
                area,
                self.daemon.as_ref(),
                self.daemon_since,
                &self.agent_infos,
                &self.threads,
                &self.config,
                "new TUI visit",
                &mut self.info_scroll,
            );
        }
        if self.commands_open {
            draw_command_palette(f, area, self.mouse_capture, &mut self.commands_scroll);
        }
        if !self.pane_open && !self.info_open && !self.commands_open && self.inspector.secondary {
            if let Some(turn) = self.open_trace {
                self.inspector
                    .draw(f, chunks[0], &self.threads, &self.turn_projection, turn);
            }
        }
    }
}
