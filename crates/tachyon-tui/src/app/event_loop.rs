//! Bounded service/input scheduling, checkpoints, and terminal teardown.
use crate::app::navigation::foreground_focus;
use crate::app::panels::tabs::PaneTab;
use crate::app::{daemon_state_cache, panels, scheduler, update, App, HITS, VIEW};
use crossterm::{
    event::{self, DisableMouseCapture},
    terminal::{disable_raw_mode, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{
    io,
    time::{Duration, Instant},
};

impl App {
    pub(super) fn event_loop(
        mut self,
        mut terminal: Terminal<CrosstermBackend<io::Stdout>>,
    ) -> io::Result<()> {
        let loop_result = 'app: loop {
            self.subscriptions.reap();
            while let Some(result) = self.controls.poll() {
                if matches!(
                    &result.output,
                    crate::app::services::control::Output::Message(Err(_))
                        | crate::app::services::control::Output::Attention(Err(_))
                ) {
                    self.clipboard_notice = Some((result.report(), Instant::now()));
                }
                update::control(result, &mut self.attention, &mut self.threads);
                self.redraw = true;
            }
            let inline = self.inline_checklist_query();
            let desired = if self.pane_open {
                match self.pane_tab {
                    PaneTab::Todos => inline
                        .as_ref()
                        .and_then(|query| match query {
                            daemon_state_cache::Query::Todos { scope, .. } => Some(scope.clone()),
                            _ => None,
                        })
                        .or_else(|| self.live_conversation.scope())
                        .map(|scope| match &self.operational_query {
                            Some(daemon_state_cache::Query::Todos {
                                scope: old,
                                turn: None,
                                ..
                            }) if old == &scope => self.operational_query.clone().unwrap(),
                            _ => daemon_state_cache::Query::Todos {
                                scope,
                                cursor: None,
                                turn: None,
                            },
                        }),
                    PaneTab::Resources => Some(match &self.operational_query {
                        Some(query @ daemon_state_cache::Query::Resources { .. }) => query.clone(),
                        _ => daemon_state_cache::Query::Resources { after: None },
                    }),
                    PaneTab::Foreground => self.live_conversation.scope().map(|scope| {
                        daemon_state_cache::Query::Todos {
                            scope,
                            cursor: None,
                            turn: None,
                        }
                    }),
                    _ => None,
                }
            } else {
                None
            };
            if desired != self.operational_query {
                self.operational_query = desired;
                self.operational_view = daemon_state_cache::View {
                    query: self.operational_query.clone(),
                    ..Default::default()
                };
                self.operational_scroll = 0;
                self.redraw = true;
            }
            self.operational_worker
                .select(self.operational_query.clone(), &self.sub_out);
            // Poll daemon + agents; subscribe to new agents.
            if self.last_poll.elapsed() > Duration::from_secs(1) {
                self.last_poll = Instant::now();
                let _ = self.status_requests.try_send(());
                self.redraw = true; // elapsed badges still tick when a status request stalls
            }

            // Drain subscription events into threads.
            for _ in scheduler::budget() {
                let ev = match scheduler::next(
                    &mut self.prefer_notifications,
                    || self.sub_rx.try_recv().ok().map(Some),
                    || self.subscriptions.poll(),
                ) {
                    Some(Some(event)) => event,
                    Some(None) => continue, // retired subscription generation
                    None => break,
                };
                self.redraw = true;
                self.apply_event(ev);
            }

            if let Some(loaded) = self.pages.take() {
                match loaded.apply(
                    &mut self.visits,
                    &mut self.threads,
                    &mut self.open_trace,
                    &mut self.transcript_view,
                    &mut self.transcript_scroll,
                    &mut self.transcript_cache,
                    &mut self.turn_projection,
                ) {
                    Ok(()) => {
                        self.open_worker = None;
                        self.inspector = panels::Inspector::default();
                        HITS.lock().unwrap().clear();
                        *VIEW.lock().unwrap() = (0, 0);
                        self.focus = foreground_focus(&self.threads);
                        // Install/selection/hits are one UI transition. Paint before
                        // accepting more input, even within the usual frame deadline.
                        self.last_draw = Instant::now() - Duration::from_secs(1);
                    }
                    Err(error) => {
                        self.clipboard_notice =
                            Some((format!("History load failed: {error}"), Instant::now()));
                    }
                }
                self.redraw = true;
            }

            if self
                .clipboard_notice
                .as_ref()
                .is_some_and(|(_, at)| at.elapsed() >= Duration::from_secs(8))
            {
                self.clipboard_notice = None;
                self.redraw = true;
            }

            // Coalesce both event sources; input gets the shorter frame deadline.
            if scheduler::frame_due(self.redraw, self.input_redraw, self.last_draw.elapsed()) {
                match terminal.draw(|f| {
                    self.draw(f);
                }) {
                    Err(error) => break 'app Err(error),
                    Ok(_frame) => {
                        #[cfg(test)]
                        self.observe("frame", Some(&_frame));
                    }
                }
                // Receipt/layout alone is not display: the terminal draw must succeed.
                self.attention.visible(
                    &self.threads,
                    &self.transcript_view.attention_hits,
                    self.pane_open
                        || self.info_open
                        || self.commands_open
                        || self.open_trace.is_some(),
                );
                self.redraw = false;
                self.input_redraw = false;
                self.last_draw = Instant::now();
            }
            self.attention.retry(&self.sub_out);

            // Keep keyboard latency below one frame while still allowing streamed
            // agent events to update the conversation continuously.
            for input_event in scheduler::inputs(event::poll, event::read) {
                let input_event = match input_event {
                    Ok(event) => event,
                    Err(error) => break 'app Err(error),
                };
                self.redraw = true;
                self.input_redraw = true;
                #[cfg(test)]
                let decoded = format!("{input_event:?}");
                let result = self.handle_input(input_event, &mut terminal);
                #[cfg(test)]
                self.observe(&decoded, None);
                match result {
                    Ok(true) => break 'app Ok(()),
                    Ok(false) => {}
                    Err(error) => break 'app Err(error),
                }
            }
        };

        // Capture even on terminal/input errors, restore the terminal before waiting
        // for the bounded drain, and never detach an archive writer on an error path.
        self.pages.stop();
        self.subscriptions.stop();
        let restored = (|| -> io::Result<()> {
            disable_raw_mode()?;
            terminal.show_cursor()?;
            terminal
                .backend_mut()
                .execute(crossterm::event::DisableBracketedPaste)?;
            terminal.backend_mut().execute(DisableMouseCapture)?;
            terminal.backend_mut().execute(LeaveAlternateScreen)?;
            Ok(())
        })();
        // Cancellation wakes full-buffer senders and blocked socket reads. Join only
        // after leaving the terminal, never while the interactive loop is running.
        drop(self.subscriptions);
        if self.controls.pending() != 0 {
            eprintln!(
                "Waiting for {} accepted command(s); queued and running work is not cancelled.",
                self.controls.pending()
            );
        }
        for result in self.controls.shutdown() {
            eprintln!("{}", result.report());
            update::control(result, &mut self.attention, &mut self.threads);
        }
        let loaded = self.pages.shutdown();
        loaded.and(loop_result).and(restored)
    }
}
