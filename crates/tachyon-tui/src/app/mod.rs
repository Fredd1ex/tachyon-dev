//! Interactive Tachyon interface.
//!
//! A conversation of agent threads. The foreground spawns worker agents; a
//! worker's output renders as a nested, collapsible thread under its parent.
//! A floating pane (like telescope.nvim) lists running agents.
//!
//! Keys:
//!   Enter        submit input / toggle collapse on focused thread
//!   Tab          toggle floating agent pane
//!   Up / Down    scroll transcript (empty input)
//!   PageUp/Down  scroll the active surface
//!   Alt+Up/Down  explicitly select turns across visit history
//!   Ctrl+L       hide/show previous visits without deleting history
//!   End          return to the latest turn
//!   Ctrl+O       toggle inline details for the current turn
//!   Ctrl+D       toggle secondary diagnostics while details are open
//!   y             copy the selected or latest chat cell (empty input)
//!   Drag          native terminal text selection (default)
//!   Ctrl+Shift+C  terminal Copy (cell copy only in /mouse capture mode)
//!   Ctrl+C / Esc / /exit  quit
//!
//! Slash commands: /mouse, /exit, /await, /stop, /release, /replan, /kill
#[cfg(test)]
use actions::foreground_workspace_request;
#[cfg(test)]
use clipboard::{
    clipboard_command, copy_to_clipboard_with, handle_copy_key, selected_chat_cell_text,
};
use clipboard::{clipboard_worker, copy_to_clipboard, CopyOutcome};
#[cfg(test)]
use crossterm::event;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
#[cfg(test)]
use crossterm::event::{KeyCode, KeyModifiers};
use crossterm::terminal::{enable_raw_mode, EnterAlternateScreen};
use crossterm::ExecutableCommand;
use editor::paste_text;
#[cfg(test)]
use model::items::AssignmentKey;
use model::items::{Item, ItemKind, WorkDetail};
use model::metrics::TurnMetrics;
#[cfg(test)]
use model::metrics::{MemoryTurnMetrics, ScheduleTurnMetrics, TokenTotals};
use model::thread::{find_or_create_thread, Thread};
use model::TranscriptScroll;
#[cfg(test)]
use navigation::{
    close_trace_details, ctrl_o_target, live_turn_number, mark_ready_turn_seen,
    mark_visible_ready_turns_seen, page_trace_turn, ready_earlier_turn, ready_notice, toggle_trace,
    toggle_worker,
};
use navigation::{
    foreground_focus, later_turn, reset_transcript, select_trace_turn, toggle_history,
};
use panels::agents::pane_agent_ids;
#[cfg(test)]
use panels::agents::{draw_agent_pane, scheduled_task_columns};
use panels::orchestrators::OrchestratorSelection;
use panels::tabs::PaneTab;
use ratatui::backend::CrosstermBackend;
#[cfg(test)]
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
#[cfg(test)]
use ratatui::widgets::Paragraph;
use ratatui::Terminal;
#[cfg(test)]
use response::main_conversation_layout;
use response::turn_cell_badges;
#[cfg(test)]
use session_archive::{restore_session, session_snapshot, SessionItem};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tachyon_api::types::{
    Actor, AgentEvent, AgentInfo, ApiResponse, DaemonInfo, EventEnvelope, EventStream,
    ScheduledTaskInfo,
};
#[cfg(test)]
use tachyon_api::types::{
    AgentState, LifetimeClass, MemoryMutationKind, MemoryMutationResult, ScheduledTaskMode,
    ScheduledTaskStatus, WorkOutcome,
};
use tachyon_api::{InteractionEvent, InteractionEventEnvelope, FOREGROUND_ID};
use tachyon_client::Client;
#[cfg(test)]
use transcript::layout::turn_cell_layout;
use transcript::projection::{
    build_turn_cells, cell_key, cell_revision, foreground_thread, latest_conversation_timestamp,
    should_show_activity, transcript_content_height, turn_response, worker_turn_revisions,
};
use transcript::text::{
    agent_count, correlated_worker_outcomes, format_count, markdown_body_lines, memory_badges,
    pending_reply_activity, sanitize_reply_text, schedule_badges, tool_parts, truncate_text,
};
#[cfg(test)]
use transcript::text::{lifecycle_badge, tool_icon, trace_summary, turn_badges};
#[cfg(test)]
use transcript::trace::{
    compact_model_timeline, elide_work_id, push_trace_item, trace_count_summary, work_tool_details,
    worker_error_summary, DETAIL_FORMATS,
};
use transcript::trace::{worker_record, WorkerTrace};
#[cfg(test)]
use transcript_cache::{CellKey, CellRevision};
use transcript_cache::{CellLayout, TranscriptView, TurnCell, TurnLayoutCache, TurnProjection};
#[cfg(test)]
use transcript_render::draw_conversation;
#[cfg(test)]
use ui::activity::agent_pane_status;
#[cfg(test)]
use ui::chrome::{footer_mode_text, status_task_count, status_task_label};
#[cfg(test)]
use ui::format::{agent_duration, agent_lifetime};
use ui::format::{format_duration, now_seconds, timestamp_label};
use ui::overlays::popup_title;
#[cfg(test)]
use ui::overlays::{draw_command_palette, draw_info_panel};
#[cfg(test)]
use update::metrics::{qualify_event_turn, record_correlated_metrics};
use update::projected_turn;
#[cfg(test)]
use update::raw_line::{accept_event, classify_line};
use update::raw_line::{decode_interaction_event, is_structured_legacy_marker};
#[cfg(test)]
use update::{
    accept_user_turn, apply_actor_event, apply_agent_event, apply_correlated_agent_event,
    apply_interaction_event,
};

mod actions;
#[path = "../attention.rs"]
mod attention;
mod checklist;
#[path = "../services/daemon_state_cache.rs"]
mod daemon_state_cache;
mod editor;
mod input;
#[path = "../model/mod.rs"]
mod model;
#[path = "../panels/mod.rs"]
mod panels;
#[cfg(test)]
#[path = "../profiling.rs"]
mod profiling;
mod scheduler;
#[path = "../services/mod.rs"]
mod services;
#[path = "../transcript/mod.rs"]
mod transcript;
#[path = "../transcript/cache.rs"]
mod transcript_cache;
#[path = "../transcript/render.rs"]
mod transcript_render;
#[path = "../ui/mod.rs"]
mod ui;
mod update;
#[cfg(test)]
mod verification;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MouseCapture(bool);

impl MouseCapture {
    fn apply(self, writer: &mut impl io::Write) -> io::Result<()> {
        if self.0 {
            writer.execute(EnableMouseCapture)?;
        } else {
            // Clear stale reporting modes too, not just our own opt-in state.
            writer.execute(DisableMouseCapture)?;
        }
        Ok(())
    }

    fn toggle(&mut self, apply: impl FnOnce(Self) -> io::Result<()>) -> String {
        let next = Self(!self.0);
        match apply(next) {
            Ok(()) => {
                *self = next;
                self.label().into()
            }
            Err(error) => {
                format!("Mouse toggle failed: {error}; terminal mode may be partial; retry /mouse")
            }
        }
    }

    fn label(self) -> &'static str {
        if self.0 {
            "Mouse: capture ON (click/wheel); /mouse for native selection"
        } else {
            "Mouse: native selection (drag + terminal Copy); /mouse for clicks"
        }
    }
}

// Restrained semantic vocabulary for diagnostic UI. Keep conversation chrome
// separate so the resting Freddie/Jarvis visual contract does not drift.
mod icon {
    pub const MODEL: &str = "󰧑";
    pub const AGENT: &str = "󰚩";
    pub const TOOL: &str = "󰆍";
    pub const BROWSER: &str = "󰖟";
    pub const SEARCH: &str = "";
    pub const FILE: &str = "󰈙";
    pub const DURATION: &str = "󰔛";
    pub const TOKENS: &str = "󰘚";
    pub const RUNNING: &str = "󰔟";
    pub const WAITING: &str = "󰏤";
    pub const SUCCESS: &str = "󰄬";
    pub const WARNING: &str = "󰀪";
    pub const FAILURE: &str = "󰅙";
    pub const COLLAPSED: &str = "";
    pub const EXPANDED: &str = "";
    pub const BULLET: &str = "";
    pub const SCROLL: &str = "󰍽";
    pub const CLOSE: &str = "󰅖";
    pub const HELP: &str = "";
    pub const LIVE: &str = "󰐊";
    pub const TURN: &str = "";
}

// ---- names from config ---------------------------------------------------

struct Names {
    user: String,
    conversation: String,
}

impl Names {
    fn from_config() -> Self {
        #[cfg(not(test))]
        let cfg = tachyon_util::config::Config::load();
        #[cfg(test)]
        let cfg = tachyon_util::config::Config::default();
        Names {
            user: cfg.user_name(),
            conversation: cfg.conversation_name(),
        }
    }
}

static NAMES: std::sync::OnceLock<Names> = std::sync::OnceLock::new();

fn names() -> &'static Names {
    NAMES.get_or_init(Names::from_config)
}

// ---- conversation model ---------------------------------------------------

fn name_block_background(color: Color) -> Color {
    match color {
        Color::Blue => Color::Blue,
        Color::Green => Color::Green,
        Color::Cyan => Color::Cyan,
        Color::Magenta => Color::Magenta,
        Color::Yellow => Color::Yellow,
        Color::Red => Color::Red,
        Color::Gray => Color::Gray,
        Color::White => Color::White,
        _ => color,
    }
}

/// Events from background subscription threads.
enum TuiEvent {
    Status(services::Snapshot),
    Attention(attention::ResultEvent),
    Operational,
    Clipboard(CopyOutcome),
    Recovered(tachyon_api::types::HistoryEntry),
    Line {
        agent_id: String,
        stream: EventStream,
        data: String,
    },
    Structured {
        agent_id: String,
        envelope: EventEnvelope,
    },
    Interaction {
        agent_id: String,
        envelope: InteractionEventEnvelope,
    },
    Ended {
        agent_id: String,
        summary: String,
    },
    ChatResult {
        error: Option<String>,
    },
}

const WINDOW_LOGO: &str = "󰘵";
const WINDOW_LOGO_BUTTON: &str = " 󰘵 ";
const INPUT_PROMPT_MARKER: &str = "❯ ";
// const INPUT_PROMPT_MARKER: &str = "⌥ ";

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClickTarget {
    Attention(usize, usize),
    TraceSummary(usize),
    Worker(usize, String),
    Item(usize, usize),
    RawEvidence(usize, usize),
}

/// Per-render-row click target. Rows without a target are `None`.
type Hit = Option<ClickTarget>;
static HITS: std::sync::Mutex<Vec<Hit>> = std::sync::Mutex::new(Vec::new());
/// (area.y, area.height) of the last conversation render.
static VIEW: std::sync::Mutex<(u16, u16)> = std::sync::Mutex::new((0, 0));

#[path = "../transcript/elapsed.rs"]
mod elapsed;
#[path = "../transcript/response.rs"]
mod response;
#[cfg(test)]
#[path = "../response_tests.rs"]
mod response_tests;
#[path = "../model/turn_activity.rs"]
mod turn_activity;

fn session_file() -> std::path::PathBuf {
    tachyon_util::daemon::data_dir().join("tui-session.json")
}

#[path = "../session_archive.rs"]
mod session_archive;

/// UI-thread state. Child dispatch/render modules can borrow it; fields stay private.
struct App {
    #[cfg(test)]
    probe: Option<std::os::unix::net::UnixStream>,
    attention: attention::State,
    visits: session_archive::Visits,
    threads: Vec<Thread>,
    history: services::history::History,
    pages: services::history::Navigator,
    controls: services::control::Worker,
    sub_out: mpsc::Sender<TuiEvent>,
    sub_rx: mpsc::Receiver<TuiEvent>,
    status_requests: mpsc::SyncSender<()>,
    subscriptions: services::subscriptions::Subscriptions,
    prefer_notifications: bool,
    seen_events: HashSet<(String, u64)>,
    seen_interactions: HashSet<(String, String, u64)>,
    agent_infos: HashMap<String, AgentInfo>,
    scheduled_tasks: Vec<ScheduledTaskInfo>,
    config: tachyon_util::config::Config,
    daemon: Option<DaemonInfo>,
    daemon_since: Option<Instant>,
    input: String,
    input_cursor: usize,
    foreground_busy: bool,
    foreground_activity: String,
    transcript_scroll: TranscriptScroll,
    transcript_cache: TurnLayoutCache,
    transcript_view: TranscriptView,
    open_trace: Option<usize>,
    open_worker: Option<(usize, String)>,
    inspector: panels::Inspector,
    turn_projection: TurnProjection,
    pane_open: bool,
    pane_tab: PaneTab,
    orchestrator_selection: OrchestratorSelection,
    operational_worker: daemon_state_cache::Worker,
    operational_query: Option<daemon_state_cache::Query>,
    operational_view: daemon_state_cache::View,
    operational_scroll: u16,
    live_conversation: daemon_state_cache::CurrentConversation,
    commands_open: bool,
    info_open: bool,
    commands_scroll: u16,
    info_scroll: u16,
    focus: usize,
    last_poll: Instant,
    last_save: Instant,
    clipboard: mpsc::SyncSender<String>,
    clipboard_notice: Option<(String, Instant)>,
    mouse_capture: MouseCapture,
    redraw: bool,
    input_redraw: bool,
    last_draw: Instant,
}

pub fn run() -> io::Result<()> {
    let attention = attention::State::open(&tachyon_util::daemon::data_dir())?;
    let mut visits = session_archive::Visits::open(&tachyon_util::daemon::data_dir())?;
    let mut threads = vec![Thread::new_foreground()];
    visits.latest(&mut threads)?;
    let history = services::history::History::start()?;
    let pages = services::history::Navigator::start()?;
    let controls = services::control::Worker::start()?;
    // Ensure the foreground thread exists.
    if !threads.iter().any(|t| t.is_foreground) {
        threads.insert(0, Thread::new_foreground());
    }

    let (sub_out, sub_rx) = mpsc::channel::<TuiEvent>();
    let status_requests = services::status_worker(sub_out.clone())?;
    let subscriptions = services::subscriptions::Subscriptions::default();
    let prefer_notifications = true;
    let seen_events: HashSet<(String, u64)> = HashSet::new();
    let seen_interactions: HashSet<(String, String, u64)> = HashSet::new();
    let agent_infos: HashMap<String, AgentInfo> = HashMap::new();
    let scheduled_tasks: Vec<ScheduledTaskInfo> = Vec::new();
    let config = tachyon_util::config::Config::load();
    let daemon: Option<DaemonInfo> = None;
    let daemon_since: Option<Instant> = None;
    let input = String::new();
    let input_cursor: usize = 0; // char index into `input`
    let foreground_busy = false;
    let foreground_activity = "working".to_string();

    // Chat view state.
    let transcript_scroll = TranscriptScroll::default();
    let transcript_cache = TurnLayoutCache::default();
    let transcript_view = TranscriptView::default();
    let open_trace = None;
    let open_worker: Option<(usize, String)> = None;
    let inspector = panels::Inspector::default();
    let turn_projection = TurnProjection::default();

    // Floating agent pane.
    let pane_open: bool = false;
    let pane_tab = PaneTab::Foreground;
    let orchestrator_selection = OrchestratorSelection::default();
    let operational_worker = daemon_state_cache::Worker::default();
    let operational_query = None;
    let operational_view = daemon_state_cache::View::default();
    let operational_scroll = 0u16;
    let live_conversation = daemon_state_cache::CurrentConversation::default();
    let commands_open: bool = false;
    let info_open: bool = false;
    let commands_scroll = 0u16;
    let info_scroll = 0u16;

    // Pane focus: 0 is the daemon; agent threads start at 1. Default to the
    // Foreground so destructive controls never target the daemon by accident.
    let focus = foreground_focus(&threads);

    let last_poll = Instant::now() - Duration::from_secs(1);
    let last_save = Instant::now();
    let clipboard = clipboard_worker(copy_to_clipboard, sub_out.clone())?;
    let clipboard_notice: Option<(String, Instant)> = None;
    let mouse_capture = MouseCapture::default();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    mouse_capture.apply(&mut stdout)?;
    stdout.execute(crossterm::event::EnableBracketedPaste)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    let redraw = true;
    let input_redraw = true;
    let last_draw = Instant::now() - Duration::from_secs(1);

    let app = App {
        #[cfg(test)]
        probe: None,
        attention,
        visits,
        threads,
        history,
        pages,
        controls,
        sub_out,
        sub_rx,
        status_requests,
        subscriptions,
        prefer_notifications,
        seen_events,
        seen_interactions,
        agent_infos,
        scheduled_tasks,
        config,
        daemon,
        daemon_since,
        input,
        input_cursor,
        foreground_busy,
        foreground_activity,
        transcript_scroll,
        transcript_cache,
        transcript_view,
        open_trace,
        open_worker,
        inspector,
        turn_projection,
        pane_open,
        pane_tab,
        orchestrator_selection,
        operational_worker,
        operational_query,
        operational_view,
        operational_scroll,
        live_conversation,
        commands_open,
        info_open,
        commands_scroll,
        info_scroll,
        focus,
        last_poll,
        last_save,
        clipboard,
        clipboard_notice,
        mouse_capture,
        redraw,
        input_redraw,
        last_draw,
    };
    app.event_loop(terminal)
}

#[cfg(test)]
#[path = "../correlation_tests.rs"]
mod correlation_tests;
#[cfg(test)]
#[path = "../parallel_acceptance.rs"]
mod parallel_acceptance;

#[cfg(test)]
mod tests;

pub(in crate::app) mod clipboard;

pub(in crate::app) mod navigation;

mod event_loop;
mod render;
