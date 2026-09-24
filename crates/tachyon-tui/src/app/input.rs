//! Surface routing policy and UI-thread key/mouse/action dispatch.
use crate::app::actions::{daemon_control, handle_slash, pane_control};
use crate::app::clipboard::handle_copy_key;
use crate::app::editor::{
    backspace_at, chars, delete_at, delete_word_left, insert_at, move_word_left, move_word_right,
};
use crate::app::navigation::{
    close_trace_details, foreground_focus, mark_ready_turn_seen, reset_transcript, toggle_worker,
};
use crate::app::panels::agents::pane_agent_ids;
use crate::app::panels::orchestrators::orchestrator_offset;
use crate::app::panels::tabs::{PaneTab, TabStrip};
use crate::app::transcript::projection::foreground_thread;
use crate::app::ui::popup_rect;
use crate::app::{
    actions, daemon_state_cache, input, panels, services, App, ClickTarget, HITS, VIEW,
};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseEventKind};
use ratatui::layout::Rect;
use ratatui::{backend::CrosstermBackend, Terminal};
use std::io;
use std::time::Instant;
use tachyon_api::FOREGROUND_ID;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Surface {
    Transcript,
    Inspector,
    Pane,
    Help,
    Info,
}

pub(super) fn surface(pane: bool, info: bool, help: bool, inspector: bool) -> Surface {
    if help {
        Surface::Help
    } else if info {
        Surface::Info
    } else if pane {
        Surface::Pane
    } else if inspector {
        Surface::Inspector
    } else {
        Surface::Transcript
    }
}

pub(super) fn accepts_key(surface: Surface, key: crossterm::event::KeyEvent, empty: bool) -> bool {
    if surface == Surface::Transcript {
        return true;
    }
    match key.code {
        KeyCode::Esc | KeyCode::Tab => true,
        KeyCode::Char('c' | 'C' | 'p' | 'i' | 'o')
            if key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            true
        }
        KeyCode::Char('?') if empty => true,
        KeyCode::Char('y') if surface == Surface::Inspector && empty => true,
        KeyCode::Up | KeyCode::Down
            if surface == Surface::Inspector && key.modifiers.contains(KeyModifiers::ALT) =>
        {
            true
        }
        KeyCode::Up
        | KeyCode::Down
        | KeyCode::Left
        | KeyCode::Right
        | KeyCode::PageUp
        | KeyCode::PageDown
            if surface == Surface::Pane =>
        {
            true
        }
        KeyCode::Enter | KeyCode::Char('a' | 's' | 'x' | 'k' | 'r' | 'u' | 'S')
            if surface == Surface::Pane && empty =>
        {
            true
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Navigation {
    Up(usize),
    Down(usize),
    Latest,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Dispatch {
    Consumed,
    Remaining,
    Quit,
}

// Shared by the terminal loop and offline event-sequence regression tests.
pub(super) struct Routing<'a> {
    pub pane: &'a mut bool,
    pub info: &'a mut bool,
    pub help: &'a mut bool,
    pub trace: &'a mut Option<usize>,
    pub worker: &'a mut Option<(usize, String)>,
    pub info_scroll: &'a mut u16,
    pub help_scroll: &'a mut u16,
    pub inspector: &'a mut super::panels::Inspector,
    pub scroll: &'a mut super::TranscriptScroll,
    pub view: &'a super::TranscriptView,
    pub draft: &'a mut String,
    pub cursor: &'a mut usize,
    pub capture: bool,
}

impl Routing<'_> {
    pub(super) fn dispatch(&mut self, event: &Event) -> Dispatch {
        let active = surface(
            *self.pane,
            *self.info,
            *self.help,
            self.trace.is_some() && self.inspector.secondary,
        );
        if !*self.pane && !*self.info && !*self.help && self.trace.is_some() {
            if matches!(event, Event::Key(key) if key.kind != KeyEventKind::Release && key.code == KeyCode::Char('d') && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                if self.inspector.secondary {
                    self.inspector.key(KeyCode::Char('d'));
                } else {
                    self.inspector.secondary = true;
                }
                return Dispatch::Consumed;
            }
            if active == Surface::Transcript
                && matches!(event, Event::Key(key) if key.kind != KeyEventKind::Release && key.code == KeyCode::Esc)
            {
                *self.trace = None;
                *self.worker = None;
                return Dispatch::Consumed;
            }
        }
        if matches!(event, Event::Key(key) if key.kind == KeyEventKind::Release) {
            return Dispatch::Consumed;
        }
        if active == Surface::Inspector {
            if let Event::Key(key) = event {
                if key.modifiers == KeyModifiers::NONE && self.inspector.key(key.code) {
                    return Dispatch::Consumed;
                }
            }
            if let Event::Mouse(mouse) = event {
                if self.capture
                    && matches!(
                        mouse.kind,
                        MouseEventKind::Down(crossterm::event::MouseButton::Left)
                    )
                {
                    self.inspector.click(mouse.column, mouse.row);
                    return Dispatch::Consumed;
                }
            }
        }
        let navigation = navigation(
            event,
            self.draft.is_empty() || active != Surface::Transcript,
            self.capture,
            self.view.viewport,
        );
        if matches!(event, Event::Key(key) if !accepts_key(active, *key, self.draft.is_empty()))
            && navigation.is_none()
        {
            return Dispatch::Consumed;
        }
        if let Some(action) = navigation {
            if *self.help || *self.info {
                let scroll = if *self.help {
                    &mut *self.help_scroll
                } else {
                    &mut *self.info_scroll
                };
                match action {
                    Navigation::Up(rows) => {
                        *scroll = scroll.saturating_sub(rows.min(u16::MAX as usize) as u16)
                    }
                    Navigation::Down(rows) => {
                        *scroll = scroll.saturating_add(rows.min(u16::MAX as usize) as u16)
                    }
                    Navigation::Latest => *scroll = u16::MAX,
                }
                return Dispatch::Consumed;
            }
            if !*self.pane {
                if active == Surface::Inspector {
                    self.inspector.navigate(action);
                } else {
                    match action {
                        Navigation::Up(rows) => self.scroll.scroll_up(rows),
                        Navigation::Down(rows) => self.scroll.scroll_down(
                            rows,
                            self.view.total_height,
                            self.view.viewport,
                        ),
                        Navigation::Latest => self.scroll.end(),
                    }
                }
                return Dispatch::Consumed;
            }
        }
        if matches!(event, Event::Key(key) if key.code == KeyCode::Esc) {
            match active {
                Surface::Help => *self.help = false,
                Surface::Info => *self.info = false,
                Surface::Pane => *self.pane = false,
                Surface::Inspector => {
                    *self.trace = None;
                    *self.worker = None;
                }
                Surface::Transcript => return Dispatch::Quit,
            }
            return Dispatch::Consumed;
        }
        if let Event::Paste(text) = event {
            if active == Surface::Transcript {
                super::paste_text(self.draft, self.cursor, text);
            }
            return Dispatch::Consumed;
        }
        Dispatch::Remaining
    }
}

pub(super) fn navigation(
    event: &Event,
    empty: bool,
    capture: bool,
    page: usize,
) -> Option<Navigation> {
    match event {
        Event::Key(key)
            if key.kind != KeyEventKind::Release && key.modifiers == KeyModifiers::NONE =>
        {
            match key.code {
                KeyCode::Up if empty => Some(Navigation::Up(3)),
                KeyCode::Down if empty => Some(Navigation::Down(3)),
                KeyCode::PageUp => Some(Navigation::Up(page.max(1))),
                KeyCode::PageDown => Some(Navigation::Down(page.max(1))),
                KeyCode::End => Some(Navigation::Latest),
                _ => None,
            }
        }
        Event::Mouse(mouse) if capture => match mouse.kind {
            MouseEventKind::ScrollUp => Some(Navigation::Up(8)),
            MouseEventKind::ScrollDown => Some(Navigation::Down(8)),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests;

impl App {
    /// Apply one terminal event, returning true when the loop should quit.
    pub(super) fn handle_input(
        &mut self,
        input_event: Event,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> io::Result<bool> {
        if matches!(&input_event, Event::Key(key) if key.code == KeyCode::Char('o') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            return Ok(false);
        }
        if !self.pane_open && !self.info_open && !self.commands_open && self.inspector.secondary {
            if matches!(&input_event, Event::Key(key) if key.kind != KeyEventKind::Release && key.code == KeyCode::Esc)
            {
                self.inspector.secondary = false;
                self.turn_projection.details = None;
                self.turn_projection.record = None;
                return Ok(false);
            }
        }
        if !self.pane_open && !self.info_open && !self.commands_open && !self.inspector.secondary {
            if let Event::Key(key) = &input_event {
                if key.kind != KeyEventKind::Release
                    && self.input.is_empty()
                    && self.open_trace.is_some()
                {
                    let scope = foreground_thread(&self.threads).and_then(|(_, thread)| {
                        self.open_trace
                            .and_then(|i| self.turn_projection.cells.get(i))
                            .and_then(|cell| thread.items[cell.prompt].turn.as_deref())
                    });
                    let rows = foreground_thread(&self.threads)
                        .map(|(index, _)| panels::activity::tasks(&self.threads, index, scope))
                        .unwrap_or_default();
                    match key.code {
                        KeyCode::Left | KeyCode::Right if key.modifiers == KeyModifiers::ALT => {
                            self.turn_projection.record_focus = if key.code == KeyCode::Left {
                                self.turn_projection.record_focus.saturating_sub(1)
                            } else {
                                (self.turn_projection.record_focus + 1)
                                    .min(rows.len().saturating_sub(1))
                            };
                            return Ok(false);
                        }
                        KeyCode::Enter | KeyCode::Char(' ')
                            if key.modifiers == KeyModifiers::NONE =>
                        {
                            if let Some(row) = rows.get(self.turn_projection.record_focus) {
                                self.turn_projection.record = Some(row.id);
                                self.turn_projection.details = scope.map(str::to_owned);
                                self.inspector.open(Some(row.id));
                            }
                            return Ok(false);
                        }
                        _ => {}
                    }
                }
            }
            if let Event::Mouse(mouse) = &input_event {
                if self.mouse_capture.0
                    && mouse.kind == MouseEventKind::Down(crossterm::event::MouseButton::Left)
                {
                    let (y, height) = *VIEW.lock().unwrap();
                    if mouse.row >= y && mouse.row < y.saturating_add(height) {
                        let hit = HITS
                            .lock()
                            .unwrap()
                            .get((mouse.row - y) as usize)
                            .cloned()
                            .flatten();
                        if let Some(ClickTarget::Item(t, i)) = hit {
                            if let Some((foreground, thread)) = foreground_thread(&self.threads) {
                                let turn = self
                                    .threads
                                    .get(t)
                                    .and_then(|source| source.items.get(i))
                                    .and_then(|item| item.turn.clone());
                                if let Some(index) = panels::activity::tasks(
                                    &self.threads,
                                    foreground,
                                    turn.as_deref(),
                                )
                                .iter()
                                .position(|row| row.id == (t, i))
                                {
                                    self.turn_projection.record_focus = index;
                                    self.open_trace =
                                        self.turn_projection.cells.iter().position(|cell| {
                                            thread.items[cell.prompt].turn == turn
                                        });
                                    self.turn_projection.details = turn;
                                    self.open_worker = None;
                                    self.turn_projection.record = Some((t, i));
                                    self.inspector.open(Some((t, i)));
                                    return Ok(false);
                                }
                            }
                        }
                    }
                }
            }
        }
        if !self.pane_open
            && !self.info_open
            && !self.commands_open
            && !self.inspector.secondary
            && !self.threads[0].hide_history
            && self.transcript_scroll.top == 0
            && matches!(
                input::navigation(
                    &input_event,
                    self.input.is_empty(),
                    self.mouse_capture.0,
                    self.transcript_view.viewport
                ),
                Some(input::Navigation::Up(_))
            )
        {
            self.pages.older(&self.visits);
            return Ok(false);
        }
        self.pages.input(
            &input_event,
            input::surface(
                self.pane_open,
                self.info_open,
                self.commands_open,
                self.open_trace.is_some() && self.inspector.secondary,
            ),
            self.input.is_empty(),
            self.mouse_capture.0,
        );
        match (input::Routing {
            pane: &mut self.pane_open,
            info: &mut self.info_open,
            help: &mut self.commands_open,
            trace: &mut self.open_trace,
            worker: &mut self.open_worker,
            info_scroll: &mut self.info_scroll,
            help_scroll: &mut self.commands_scroll,
            inspector: &mut self.inspector,
            scroll: &mut self.transcript_scroll,
            view: &self.transcript_view,
            draft: &mut self.input,
            cursor: &mut self.input_cursor,
            capture: self.mouse_capture.0,
        })
        .dispatch(&input_event)
        {
            input::Dispatch::Consumed => return Ok(false),
            input::Dispatch::Quit => return Ok(true),
            input::Dispatch::Remaining => {}
        }
        let surface = input::surface(
            self.pane_open,
            self.info_open,
            self.commands_open,
            self.open_trace.is_some() && self.inspector.secondary,
        );
        let navigation_empty = self.input.is_empty() || surface != input::Surface::Transcript;
        // The topmost surface owns navigation, including wheel events.
        if let Some(action) = input::navigation(
            &input_event,
            navigation_empty,
            self.mouse_capture.0,
            self.transcript_view.viewport,
        ) {
            if matches!(input_event, Event::Mouse(_)) {
                if matches!(self.pane_tab, PaneTab::Todos | PaneTab::Resources) {
                    match action {
                        input::Navigation::Up(rows) => {
                            self.operational_scroll =
                                self.operational_scroll.saturating_sub(rows as u16)
                        }
                        input::Navigation::Down(rows) => {
                            self.operational_scroll =
                                self.operational_scroll.saturating_add(rows as u16).min(
                                    self.operational_view
                                        .rows
                                        .lines()
                                        .count()
                                        .saturating_sub(1)
                                        .min(u16::MAX as usize)
                                        as u16,
                                )
                        }
                        _ => {}
                    }
                }
                return Ok(false);
            }
        }
        match input_event {
            Event::Key(key) => match key.code {
                _ if handle_copy_key(
                    key,
                    self.mouse_capture,
                    self.pane_open || self.info_open || self.commands_open,
                    &self.input,
                    &self.threads,
                    self.open_trace,
                    |text| {
                        let accepted = self.clipboard.try_send(text.to_owned()).is_ok();
                        self.clipboard_notice = Some((
                            if accepted {
                                "Copy queued..."
                            } else {
                                "Copy not queued: clipboard busy or unavailable; retry shortly."
                            }
                            .into(),
                            Instant::now(),
                        ));
                        accepted
                    },
                ) => {}
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(true)
                }
                KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.pages.toggle(&self.visits, &mut self.threads);
                    self.focus = foreground_focus(&self.threads);
                    reset_transcript(
                        &mut self.transcript_scroll,
                        &mut self.transcript_view,
                        &mut self.transcript_cache,
                        &mut self.open_trace,
                        &mut self.turn_projection,
                    );
                    self.open_worker = None;
                    self.inspector = panels::Inspector::default();
                    HITS.lock().unwrap().clear();
                    *VIEW.lock().unwrap() = (0, 0);
                }
                KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.commands_open = !self.commands_open;
                    if self.commands_open {
                        self.pane_open = false;
                        self.info_open = false;
                    }
                }
                KeyCode::Char('?') if self.input.is_empty() => {
                    self.commands_open = !self.commands_open;
                    if self.commands_open {
                        self.pane_open = false;
                        self.info_open = false;
                    }
                }
                KeyCode::Char('i') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.info_open = !self.info_open;
                    if self.info_open {
                        self.pane_open = false;
                        self.commands_open = false;
                    }
                }
                KeyCode::Tab => {
                    self.pane_open = !self.pane_open;
                    if self.pane_open {
                        self.commands_open = false;
                        self.info_open = false;
                    }
                }
                _ if self.commands_open || self.info_open => {}
                KeyCode::Up | KeyCode::Down | KeyCode::PageUp | KeyCode::PageDown
                    if self.pane_open
                        && matches!(self.pane_tab, PaneTab::Todos | PaneTab::Resources) =>
                {
                    match key.code {
                        KeyCode::Up => {
                            self.operational_scroll = self.operational_scroll.saturating_sub(1)
                        }
                        KeyCode::Down => {
                            self.operational_scroll = self
                                .operational_scroll
                                .saturating_add(1)
                                .min(self.operational_view.rows.lines().count().saturating_sub(1)
                                    as u16)
                        }
                        _ => {
                            let first = key.code == KeyCode::PageUp;
                            let next = match &self.operational_query {
                                Some(daemon_state_cache::Query::Todos { scope, .. })
                                    if first || self.operational_view.next_todo.is_some() =>
                                {
                                    Some(daemon_state_cache::Query::Todos {
                                        turn: None,
                                        scope: scope.clone(),
                                        cursor: if first {
                                            None
                                        } else {
                                            self.operational_view.next_todo.clone()
                                        },
                                    })
                                }
                                Some(daemon_state_cache::Query::Resources { .. })
                                    if first || self.operational_view.next_resource.is_some() =>
                                {
                                    Some(daemon_state_cache::Query::Resources {
                                        after: if first {
                                            None
                                        } else {
                                            self.operational_view.next_resource.clone()
                                        },
                                    })
                                }
                                _ => self.operational_query.clone(),
                            };
                            if next != self.operational_query {
                                self.operational_query = next;
                                self.operational_view = daemon_state_cache::View::default();
                                self.operational_scroll = 0;
                            }
                        }
                    }
                }
                KeyCode::Up => {
                    if self.pane_open && self.pane_tab == PaneTab::Foreground {
                        self.orchestrator_selection
                            .step(self.daemon.as_ref(), false);
                    } else if self.pane_open {
                        self.focus = self.focus.saturating_sub(1);
                    } else if self.input.is_empty() && key.modifiers.contains(KeyModifiers::ALT) {
                        let previous = self.open_trace;
                        self.pages.select(
                            &self.visits,
                            &self.threads,
                            &mut self.open_trace,
                            &self.transcript_view,
                            &mut self.transcript_scroll,
                            &mut self.turn_projection,
                            -1,
                        );
                        if self.open_trace != previous || self.transcript_view.starts.is_empty() {
                            self.turn_projection.details = None;
                            self.open_worker = None;
                            self.inspector = panels::Inspector::default();
                        }
                    }
                }
                KeyCode::Down => {
                    if self.pane_open && self.pane_tab == PaneTab::Foreground {
                        self.orchestrator_selection.step(self.daemon.as_ref(), true);
                    } else if self.pane_open {
                        if self.focus
                            < pane_agent_ids(&self.agent_infos).len()
                                + usize::from(self.agent_infos.contains_key(FOREGROUND_ID))
                        {
                            self.focus += 1;
                        }
                    } else if self.input.is_empty() && key.modifiers.contains(KeyModifiers::ALT) {
                        let previous = self.open_trace;
                        self.pages.select(
                            &self.visits,
                            &self.threads,
                            &mut self.open_trace,
                            &self.transcript_view,
                            &mut self.transcript_scroll,
                            &mut self.turn_projection,
                            1,
                        );
                        if self.open_trace != previous || self.transcript_view.starts.is_empty() {
                            self.turn_projection.details = None;
                            self.open_worker = None;
                            self.inspector = panels::Inspector::default();
                        }
                    }
                }
                KeyCode::End | KeyCode::PageUp | KeyCode::PageDown if self.pane_open => {}
                KeyCode::End => {
                    close_trace_details(
                        &mut self.open_trace,
                        &mut self.open_worker,
                        &mut self.transcript_scroll,
                    );
                    self.transcript_scroll.end();
                }
                // Ctrl+Backspace / Ctrl+H: delete the previous word.
                KeyCode::Backspace | KeyCode::Delete | KeyCode::Char('h')
                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    delete_word_left(&mut self.input, &mut self.input_cursor);
                }
                // Pane control keys: await/release/stop/kill/restart the focused agent
                // Only arm actions with an empty draft. Panels own input;
                // closing them restores the untouched composer.
                KeyCode::Char('a')
                | KeyCode::Char('s')
                | KeyCode::Char('x')
                | KeyCode::Char('k')
                | KeyCode::Char('r')
                | KeyCode::Char('u')
                    if self.pane_open
                        && matches!(self.pane_tab, PaneTab::Foreground | PaneTab::Agents)
                        && self.input.is_empty() =>
                {
                    let verb = if key.code == KeyCode::Char('a') {
                        "await"
                    } else if key.code == KeyCode::Char('x') {
                        "release"
                    } else if key.code == KeyCode::Char('s') {
                        "stop"
                    } else if key.code == KeyCode::Char('k') {
                        "kill"
                    } else if key.code == KeyCode::Char('u') {
                        "resume"
                    } else {
                        "restart"
                    };
                    if self.pane_tab == PaneTab::Foreground {
                        if self.orchestrator_selection.target(self.daemon.as_ref())
                            == Some(tachyon_api::OrchestratorHost::Daemon)
                        {
                            let result = match verb {
                                "stop" | "kill" => daemon_control("stop", &mut self.controls),
                                "restart" => daemon_control("restart", &mut self.controls),
                                _ => Ok(()),
                            };
                            if let Err(error) = result {
                                self.clipboard_notice = Some((error, Instant::now()));
                            }
                        }
                    } else {
                        if let Err(error) = pane_control(
                            verb,
                            self.focus,
                            &self.agent_infos,
                            &mut self.threads,
                            &mut self.controls,
                        ) {
                            self.clipboard_notice = Some((error, Instant::now()));
                        }
                    }
                }
                KeyCode::Char('S')
                    if self.pane_open
                        && matches!(self.pane_tab, PaneTab::Foreground | PaneTab::Agents)
                        && self.input.is_empty() =>
                {
                    if self.pane_tab == PaneTab::Agents
                        || self.orchestrator_selection.target(self.daemon.as_ref())
                            == Some(tachyon_api::OrchestratorHost::Daemon)
                    {
                        if let Err(error) = daemon_control("start", &mut self.controls) {
                            self.clipboard_notice = Some((error, Instant::now()));
                        }
                    }
                }
                KeyCode::Enter => {
                    // Shift/Ctrl+Enter inserts a newline; plain Enter submits.
                    if key
                        .modifiers
                        .intersects(KeyModifiers::SHIFT | KeyModifiers::CONTROL)
                    {
                        insert_at(&mut self.input, &mut self.input_cursor, '\n');
                        return Ok(false);
                    }
                    let cmd = self.input.trim().to_string();
                    if cmd.is_empty()
                        && self.pane_open
                        && matches!(
                            self.pane_tab,
                            PaneTab::Foreground | PaneTab::Todos | PaneTab::Resources
                        )
                    {
                        return Ok(false);
                    }
                    if cmd.is_empty() {
                        // No text: toggle collapse on the focused thread.
                        if self.focus > 0 {
                            let id = if self.focus == 1 {
                                Some(FOREGROUND_ID.to_owned())
                            } else {
                                pane_agent_ids(&self.agent_infos)
                                    .get(self.focus - 2)
                                    .cloned()
                            };
                            if let Some(t) =
                                self.threads.iter_mut().find(|t| Some(&t.id) == id.as_ref())
                            {
                                t.collapsed = !t.collapsed;
                            }
                        }
                        self.input.clear();
                        self.input_cursor = 0;
                        return Ok(false);
                    }
                    if let Some(rest) = cmd
                        .strip_prefix('/')
                        .filter(|_| !cmd.starts_with("/managed "))
                    {
                        match rest {
                            "exit" | "quit" | "q" => return Ok(true),
                            "mouse" => {
                                let notice = self
                                    .mouse_capture
                                    .toggle(|mode| mode.apply(terminal.backend_mut()));
                                self.clipboard_notice = Some((notice, Instant::now()));
                            }
                            "clear" | "reset" => {
                                self.pages.toggle(&self.visits, &mut self.threads);
                                self.focus = foreground_focus(&self.threads);
                                reset_transcript(
                                    &mut self.transcript_scroll,
                                    &mut self.transcript_view,
                                    &mut self.transcript_cache,
                                    &mut self.open_trace,
                                    &mut self.turn_projection,
                                );
                                self.open_worker = None;
                                self.inspector = panels::Inspector::default();
                                HITS.lock().unwrap().clear();
                                *VIEW.lock().unwrap() = (0, 0);
                            }
                            _ => {
                                let result = if rest == "reconcile" {
                                    self.reconcile_commands()
                                } else {
                                    match self.attention.command(rest) {
                                        Some(Ok(requests)) => self
                                            .controls
                                            .submit(
                                                rest.into(),
                                                services::control::Command::Attention(requests),
                                            )
                                            .map(|_| ()),
                                        Some(Err(error)) => Err(error),
                                        None => handle_slash(
                                            rest,
                                            &mut self.threads,
                                            &mut self.controls,
                                        ),
                                    }
                                };
                                if let Err(error) = result {
                                    self.clipboard_notice = Some((error, Instant::now()));
                                    return Ok(false);
                                }
                            }
                        }
                        self.input.clear();
                        self.input_cursor = 0;
                        return Ok(false);
                    }
                    // User message -> foreground thread.
                    if let Err(error) = actions::submit_chat(
                        &cmd,
                        &mut self.threads,
                        &mut self.controls,
                        &mut self.interaction,
                    ) {
                        self.clipboard_notice = Some((error, Instant::now()));
                        return Ok(false);
                    }
                    self.open_trace = None;
                    self.open_worker = None;
                    self.transcript_scroll.end();
                    self.input.clear();
                    self.input_cursor = 0;
                    self.foreground_busy = true;
                }
                KeyCode::Char('w') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    delete_word_left(&mut self.input, &mut self.input_cursor);
                }
                KeyCode::Char(c)
                    if !key.modifiers.intersects(
                        KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                    ) =>
                {
                    insert_at(&mut self.input, &mut self.input_cursor, c)
                }
                KeyCode::Backspace => {
                    backspace_at(&mut self.input, &mut self.input_cursor);
                }
                KeyCode::Left if self.pane_open => {
                    self.pane_tab = self.pane_tab.adjacent(false);
                    if self.pane_tab == PaneTab::Foreground && self.focus > 1 {
                        self.focus = 0;
                    }
                }
                KeyCode::Right if self.pane_open => {
                    self.pane_tab = self.pane_tab.adjacent(true);
                    if self.pane_tab == PaneTab::Agents
                        && self.focus < 2
                        && !pane_agent_ids(&self.agent_infos).is_empty()
                    {
                        self.focus = 2;
                    }
                }
                KeyCode::Left => {
                    if key.modifiers.contains(KeyModifiers::CONTROL) {
                        move_word_left(&self.input, &mut self.input_cursor);
                    } else if self.input_cursor > 0 {
                        self.input_cursor -= 1;
                    }
                }
                KeyCode::Right => {
                    if key.modifiers.contains(KeyModifiers::CONTROL) {
                        move_word_right(&self.input, &mut self.input_cursor);
                    } else if self.input_cursor < chars(&self.input) {
                        self.input_cursor += 1;
                    }
                }
                KeyCode::Home => self.input_cursor = 0,
                KeyCode::Delete => delete_at(&mut self.input, &mut self.input_cursor),
                _ => {}
            },
            Event::Mouse(m) if self.mouse_capture.0 => {
                use crossterm::event::{MouseButton, MouseEventKind};
                if self.commands_open || self.info_open {
                    return Ok(false);
                }
                match m.kind {
                    MouseEventKind::ScrollUp => {
                        self.transcript_scroll.scroll_up(8);
                    }
                    MouseEventKind::ScrollDown => {
                        self.transcript_scroll.scroll_down(
                            8,
                            self.transcript_view.total_height,
                            self.transcript_view.viewport,
                        );
                    }
                    MouseEventKind::Down(MouseButton::Left) => {
                        let size = match terminal.size() {
                            Ok(size) => size,
                            Err(error) => return Err(error),
                        };
                        if m.row == size.height.saturating_sub(1) {
                            return Ok(false);
                        }
                        if self.pane_open {
                            let chat_area = Rect {
                                x: 0,
                                y: 0,
                                width: size.width,
                                height: size.height.saturating_sub(4),
                            };
                            let pane = popup_rect(
                                chat_area,
                                72,
                                chat_area.height.saturating_sub(4).min(18),
                            );
                            let inner_x = pane.x.saturating_add(1);
                            let inner_right = pane.x + pane.width.saturating_sub(1);
                            let tabs = TabStrip::new(
                                pane,
                                pane_agent_ids(&self.agent_infos).len(),
                                self.scheduled_tasks.len(),
                            );
                            if let Some(tab) = tabs.hit(m.column, m.row) {
                                self.pane_tab = tab;
                                return Ok(false);
                            }
                            // The table header occupies the first inner row.
                            let extra = tabs.height(pane).saturating_sub(1);
                            let checklist_height = if self.pane_tab == PaneTab::Foreground {
                                (self.operational_view.global_checklist().lines().count() as u16)
                                    .min(pane.height.saturating_sub(7 + extra))
                            } else {
                                0
                            };
                            let first_data_row = pane.y.saturating_add(2 + extra);
                            if m.column >= inner_x
                                && m.column < inner_right
                                && m.row >= first_data_row
                                && m.row < pane.y + pane.height.saturating_sub(3 + checklist_height)
                            {
                                let row = (m.row - first_data_row) as usize;
                                match self.pane_tab {
                                    PaneTab::Foreground => {
                                        let offset = orchestrator_offset(
                                            self.orchestrator_selection
                                                .index(self.daemon.as_ref())
                                                .unwrap_or(usize::MAX),
                                            pane.height.saturating_sub(extra + checklist_height),
                                        );
                                        self.orchestrator_selection
                                            .select(self.daemon.as_ref(), offset + row);
                                    }
                                    PaneTab::Agents => {
                                        if row < pane_agent_ids(&self.agent_infos).len() {
                                            self.focus = row + 2;
                                        }
                                    }
                                    PaneTab::Scheduled => {}
                                    PaneTab::Memory | PaneTab::Todos | PaneTab::Resources => {}
                                }
                                return Ok(false);
                            }
                        }
                        if self.pane_open {
                            return Ok(false);
                        }
                        let (vy, vh) = *VIEW.lock().unwrap();
                        // Only map clicks inside the conversation area.
                        if m.row >= vy && m.row < vy + vh {
                            let row = m.row.saturating_sub(vy) as usize;
                            let hit = HITS.lock().unwrap().get(row).cloned().flatten();
                            if let Some(ClickTarget::TraceSummary(turn)) = hit {
                                if let Some((_, thread)) = foreground_thread(&self.threads) {
                                    if let Some(turn_id) = self
                                        .turn_projection
                                        .cells
                                        .get(turn)
                                        .and_then(|cell| thread.items[cell.prompt].turn.as_deref())
                                        .map(str::to_owned)
                                    {
                                        mark_ready_turn_seen(&mut self.threads, &turn_id);
                                    }
                                }
                                self.open_trace = Some(turn);
                                let identity =
                                    foreground_thread(&self.threads).and_then(|(_, thread)| {
                                        self.turn_projection
                                            .cells
                                            .get(turn)
                                            .and_then(|cell| thread.items[cell.prompt].turn.clone())
                                    });
                                self.turn_projection.details =
                                    if self.turn_projection.details == identity {
                                        None
                                    } else {
                                        identity
                                    };
                                self.turn_projection.record = None;
                                self.turn_projection.record_focus = 0;
                                self.open_worker = None;
                                self.inspector.open(None);
                                return Ok(false);
                            }
                            if let Some(ClickTarget::Worker(turn, worker)) = hit {
                                self.open_trace = Some(turn);
                                toggle_worker(&mut self.open_worker, turn, worker);
                                return Ok(false);
                            }
                            if let Some(ClickTarget::RawEvidence(ti, ii)) = hit {
                                if let Some(thread) = self.threads.get_mut(ti) {
                                    thread.touch();
                                    if let Some(item) = thread.items.get_mut(ii) {
                                        if let Some(work) = item.work.as_mut() {
                                            work.raw_open = !work.raw_open;
                                            item.revision = thread.revision;
                                        }
                                    }
                                }
                                return Ok(false);
                            }
                            if let Some(ClickTarget::Item(ti, ii)) = hit {
                                if let Some(thread) = self.threads.get_mut(ti) {
                                    if !thread.is_foreground && thread.collapsed {
                                        thread.collapsed = false;
                                        thread.touch();
                                        return Ok(false);
                                    }
                                    thread.collapsed = false;
                                    thread.touch();
                                    let revision = thread.revision;
                                    if let Some(item) = thread.items.get_mut(ii) {
                                        item.hidden = !item.hidden;
                                        item.revision = revision;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(false)
    }
}
