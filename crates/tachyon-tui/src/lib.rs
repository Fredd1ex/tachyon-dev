#![forbid(unsafe_code)]

//! Interactive Tachyon interface.
//!
//! A conversation of agent threads. The foreground spawns worker agents; a
//! worker's output renders as a nested, collapsible thread under its parent.
//! A floating pane (like telescope.nvim) lists running agents.
//!
//! Keys:
//!   Enter        submit input / toggle collapse on focused thread
//!   Tab          toggle floating agent pane
//!   Up / Down    move focus between threads
//!   Up/PageUp    scroll chat up (hold) / PageUp
//!   End          jump to bottom
//!   Ctrl+C / Esc / /exit  quit
//!
//! Slash commands: /exit, /await, /stop, /release, /replan, /kill

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers,
};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table,
};
use ratatui::Frame;
use ratatui::Terminal;

use tachyon_api::types::{
    Actor, AgentEvent, AgentInfo, AgentState, ApiResponse, DaemonInfo, EventEnvelope, EventStream,
    WorkOutcome,
};
use tachyon_api::{InteractionEvent, InteractionEventEnvelope, FOREGROUND_ID};

use tachyon_client::{Client, Subscription};

// ---- names from config ---------------------------------------------------

struct Names {
    user: String,
    conversation: String,
}

impl Names {
    fn from_config() -> Self {
        let cfg = tachyon_util::config::Config::load();
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

const AGENT_COLORS: [Color; 6] = [
    Color::Cyan,
    Color::Magenta,
    Color::Yellow,
    Color::Red,
    Color::Blue,
    Color::Green,
];

fn agent_color(id: &str) -> Color {
    let n = id
        .bytes()
        .fold(0usize, |acc, b| acc.wrapping_add(b as usize));
    AGENT_COLORS[n % AGENT_COLORS.len()]
}

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

/// A kind of conversation line.
#[derive(Clone, Debug, PartialEq)]
enum ItemKind {
    User,         // the human user's message
    PendingReply, // reserved assistant position for an accepted user turn
    Reply,        // an agent/foreground model reply
    Tool,         // a tool invocation
    ToolResult,   // tool output
    System,       // lifecycle / status notice
    Spawn,        // "spawned worker <id>"
    SpawnResult,  // "worker <id> -> result"
    Error,
}

/// One line in a thread.
struct Item {
    kind: ItemKind,
    text: String,
    /// When true, the item's body is hidden (collapsed); a click/hotkey on its
    /// header toggles it. Used for long tool output so the chat stays clean.
    hidden: bool,
    /// Output belonging to a tool call. Keeping it on the call makes the UI
    /// card atomic instead of depending on adjacent stream items.
    output: Option<String>,
    #[allow(dead_code)]
    tool_id: Option<String>,
    turn: Option<String>,
    timestamp: u64,
}

fn sanitize_reply_text(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let text = if let Some(dsml) = lower.find("dsml") {
        let protocol = &lower[dsml..];
        if protocol.contains("tool_calls")
            || protocol.contains("invoke name=")
            || protocol.contains("parameter name=")
        {
            if let Some(start) = text[..dsml].rfind('<') {
                text[..start].trim_end()
            } else {
                text
            }
        } else {
            text
        }
    } else {
        text
    };
    let cleaned = text
        .lines()
        .filter(|line| {
            let chars = line.chars().count();
            let replacements = line.chars().filter(|ch| *ch == '\u{fffd}').count();
            chars == 0 || replacements.saturating_mul(4) < chars
        })
        .collect::<Vec<_>>()
        .join("\n");
    if cleaned.trim().is_empty() && text.contains('\u{fffd}') {
        "[model output could not be decoded]".into()
    } else {
        cleaned
    }
}

fn reply_completed_after_later_user(thread: &Thread, item: &Item) -> bool {
    let Some(reply_turn) = item
        .turn
        .as_deref()
        .and_then(|turn| turn.parse::<u64>().ok())
    else {
        return false;
    };
    thread.items.iter().any(|candidate| {
        candidate.kind == ItemKind::User
            && candidate.timestamp < item.timestamp
            && candidate
                .turn
                .as_deref()
                .and_then(|turn| turn.parse::<u64>().ok())
                .is_some_and(|user_turn| user_turn > reply_turn)
    })
}

/// A single agent thread. The foreground thread is root; workers nest under
/// their parent (or the foreground).
struct Thread {
    id: String,
    parent: Option<String>,
    task: Option<String>,
    is_foreground: bool,
    collapsed: bool,
    streaming: bool,
    last_activity: Instant,
    revision: u64,
    items: Vec<Item>,
    usage: HashMap<u64, (u32, u32, u32)>,
    metrics: HashMap<String, TurnMetrics>,
}

#[derive(Clone, Default, Hash, serde::Serialize, serde::Deserialize)]
struct TokenTotals {
    prompt: u64,
    completion: u64,
    total: u64,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct TurnMetrics {
    first_visible_ms: Option<u64>,
    completed_ms: Option<u64>,
    self_usage: Option<TokenTotals>,
    #[serde(default)]
    worker_usage: HashMap<String, TokenTotals>,
}

impl Thread {
    fn new_foreground() -> Self {
        Thread {
            id: FOREGROUND_ID.into(),
            parent: None,
            task: Some("foreground".into()),
            is_foreground: true,
            collapsed: false,
            streaming: false,
            last_activity: Instant::now(),
            revision: 0,
            items: Vec::new(),
            usage: HashMap::new(),
            metrics: HashMap::new(),
        }
    }

    fn add(&mut self, kind: ItemKind, text: String) {
        self.add_turn(kind, text, None);
    }

    fn reserve_reply(&mut self) {
        self.touch();
        self.items.push(Item {
            kind: ItemKind::PendingReply,
            text: "waiting".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: None,
            timestamp: now_seconds(),
        });
    }

    fn add_turn(&mut self, kind: ItemKind, text: String, turn: Option<String>) {
        self.touch();
        self.streaming = kind == ItemKind::Reply;
        self.last_activity = Instant::now();
        let timestamp = now_seconds();
        if kind == ItemKind::ToolResult {
            if let Some(last) = self
                .items
                .iter_mut()
                .rev()
                .find(|item| item.kind == ItemKind::Tool && item.output.is_none())
            {
                last.output = Some(text);
                last.hidden = true;
                self.streaming = false;
                self.last_activity = Instant::now();
                return;
            }
        }

        // Glue consecutive streamed fragments into one logical item.
        let can_glue = match (&kind, self.items.last()) {
            (ItemKind::ToolResult, Some(last))
                if last.kind == ItemKind::ToolResult && last.turn == turn =>
            {
                true
            }
            (ItemKind::Tool, Some(last)) if last.kind == ItemKind::Tool && last.turn == turn => {
                true
            }
            (ItemKind::System, Some(last))
                if last.kind == ItemKind::System && last.turn == turn =>
            {
                true
            }
            (ItemKind::Reply, Some(last)) if last.kind == ItemKind::Reply && last.turn == turn => {
                true
            }
            _ => false,
        };
        if can_glue {
            if let Some(last) = self.items.last_mut() {
                if !last.text.is_empty() && !text.is_empty() {
                    last.text.push('\n');
                }
                last.text.push_str(&text);
                if last.kind == ItemKind::ToolResult && last.text.chars().count() > 400 {
                    last.hidden = true;
                }
            }
        } else {
            match &kind {
                ItemKind::ToolResult => {
                    // Start long tool output collapsed (a clickable header);
                    // keep short output visible.
                    self.items.push(Item {
                        kind,
                        text,
                        hidden: true,
                        output: None,
                        tool_id: None,
                        turn,
                        timestamp,
                    });
                }
                _ => self.items.push(Item {
                    kind,
                    text,
                    hidden: false,
                    output: None,
                    tool_id: None,
                    turn,
                    timestamp,
                }),
            }
        }
    }

    fn add_reply_fragment(&mut self, text: String, turn: Option<String>, line_break: bool) {
        self.touch();
        self.streaming = true;
        self.last_activity = Instant::now();
        if let Some(existing) = self
            .items
            .iter_mut()
            .rev()
            .find(|item| item.kind == ItemKind::Reply && item.turn == turn)
        {
            if line_break {
                existing.text.push('\n');
            }
            existing.text.push_str(&text);
            return;
        }
        if let Some(existing) = self
            .items
            .iter_mut()
            .rev()
            .find(|item| item.kind == ItemKind::PendingReply && item.turn == turn)
        {
            existing.kind = ItemKind::Reply;
            existing.text = text;
            existing.timestamp = now_seconds();
            return;
        }
        let item = Item {
            kind: ItemKind::Reply,
            text,
            hidden: false,
            output: None,
            tool_id: None,
            turn: turn.clone(),
            timestamp: now_seconds(),
        };
        let index = turn
            .as_deref()
            .and_then(|turn| turn.parse::<u64>().ok())
            .and_then(|turn| {
                self.items.iter().position(|item| {
                    item.turn
                        .as_deref()
                        .and_then(|value| value.parse::<u64>().ok())
                        .is_some_and(|item_turn| item_turn > turn)
                })
            })
            .unwrap_or(self.items.len());
        self.items.insert(index, item);
    }

    fn finish_reply(&mut self, text: String, turn: Option<String>) {
        self.touch();
        self.streaming = false;
        let text = sanitize_reply_text(&text);
        if let Some(existing) = self
            .items
            .iter_mut()
            .rev()
            .find(|item| item.kind == ItemKind::Reply && item.turn == turn)
        {
            existing.text = text;
            self.last_activity = Instant::now();
            return;
        }
        self.add_reply_fragment(text, turn, false);
        self.streaming = false;
    }

    fn add_tool(&mut self, text: String, id: String, turn: Option<String>) {
        self.touch();
        self.streaming = false;
        self.items.push(Item {
            kind: ItemKind::Tool,
            text,
            hidden: true,
            output: None,
            tool_id: Some(id),
            turn,
            timestamp: now_seconds(),
        });
    }

    fn add_tool_result(&mut self, id: String, text: String, turn: Option<String>) {
        self.touch();
        if let Some(tool) = self.items.iter_mut().rev().find(|item| {
            item.kind == ItemKind::Tool && item.tool_id.as_deref() == Some(id.as_str())
        }) {
            tool.output = Some(match tool.output.take() {
                Some(mut output) => {
                    output.push('\n');
                    output.push_str(&text);
                    output
                }
                None => text,
            });
            tool.hidden = true;
            return;
        }
        self.add(ItemKind::ToolResult, text);
        if let Some(item) = self.items.last_mut() {
            item.turn = turn;
            item.tool_id = Some(id);
        }
    }

    fn touch(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

/// Events from background subscription threads.
enum TuiEvent {
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

#[derive(Clone, Copy, PartialEq)]
enum PaneTab {
    Foreground,
    Agents,
}

/// Per-render-row click target: maps a visible conversation row to an item in
/// `threads[thread].items[item]`. Rows without a target are `None`.
type Hit = Option<(usize, usize)>;
static HITS: std::sync::Mutex<Vec<Hit>> = std::sync::Mutex::new(Vec::new());
/// (area.y, area.height, top_scroll) of the last conversation render, used to
/// map a mouse click row back to the hitmap.
static VIEW: std::sync::Mutex<(u16, u16, usize)> = std::sync::Mutex::new((0, 0, 0));

struct ConversationCache {
    signature: u64,
    lines: Vec<Line<'static>>,
    hits: Vec<Hit>,
}

static CONVERSATION_CACHE: std::sync::Mutex<Option<ConversationCache>> =
    std::sync::Mutex::new(None);

fn conversation_signature(
    threads: &[Thread],
    width: u16,
    show_traces: bool,
    foreground_busy: bool,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    width.hash(&mut hasher);
    show_traces.hash(&mut hasher);
    foreground_busy.hash(&mut hasher);
    for thread in threads {
        thread.id.hash(&mut hasher);
        thread.collapsed.hash(&mut hasher);
        thread.streaming.hash(&mut hasher);
        thread.revision.hash(&mut hasher);
    }
    hasher.finish()
}

// ---- session persistence --------------------------------------------------

fn session_file() -> std::path::PathBuf {
    tachyon_util::daemon::data_dir().join("tui-session.json")
}

fn daemon_session_file() -> std::path::PathBuf {
    tachyon_util::daemon::data_dir().join("tui-daemon.pid")
}

fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn format_age(created_secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(created_secs);
    format_duration(Duration::from_secs(now.saturating_sub(created_secs)))
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {:02}m", seconds / 3600, (seconds / 60) % 60)
    }
}

fn timestamp_label(timestamp: u64) -> String {
    let seconds = if timestamp < 10_000_000_000 {
        timestamp
    } else {
        timestamp / 1_000
    };
    let day = seconds % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        day / 3_600,
        (day % 3_600) / 60,
        day % 60
    )
}

fn spawn_display_label(default_label: &str, text: &str) -> String {
    if let Some(id) = text.strip_prefix("spawned worker ") {
        format!("ghost {id}")
    } else if let Some((worker, objective)) = text
        .strip_prefix("worker ")
        .and_then(|text| text.split_once(": "))
    {
        format!(
            "{default_label} worker {} · {}",
            truncate_text(worker, 8),
            short_preview(objective)
        )
    } else {
        format!("{default_label} {}", short_preview(text))
    }
}

fn worker_objective(text: &str) -> &str {
    text.split_once(": ")
        .map(|(_, objective)| objective.lines().next().unwrap_or(objective))
        .unwrap_or(text)
}

fn worker_progress_badge(thread: &Thread, turn: Option<&str>) -> Option<String> {
    let turn = turn?;
    let spawned = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Spawn && item.turn.as_deref() == Some(turn))
        .count();
    if spawned == 0 {
        return None;
    }
    let completed = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::SpawnResult && item.turn.as_deref() == Some(turn))
        .count();
    Some(if completed == 0 {
        format!("󰚩 {spawned} agents running")
    } else if completed == spawned {
        format!("󰄬 {completed} agents complete")
    } else {
        format!("󰚩 {spawned} agents · 󰄬 {completed} complete")
    })
}

fn is_worker_runtime_detail(text: &str) -> bool {
    text.contains("agent-browser ready")
        || text.starts_with("[ghost] workspace:")
        || text.starts_with("[ghost] model:")
        || text == "[ghost] ready"
        || text.starts_with("[foreground] workspace:")
        || text.starts_with("[foreground] model:")
        || text == "[foreground] ready"
        || text == "[ready] startup"
}

fn trace_summary(text: &str) -> String {
    if let Some(timing) = text.strip_prefix("[timing] ") {
        let Some((stage, elapsed)) = timing.rsplit_once(' ') else {
            return format!("󰐊 {timing}");
        };
        let label = stage.replace('_', " ");
        if stage.ends_with("_started") {
            return format!("󰐊 {} · +{elapsed}", label.trim_end_matches(" started"));
        }
        if stage.ends_with("_completed") {
            return format!("󰅐 {} · {elapsed}", label.trim_end_matches(" completed"));
        }
        return match stage {
            "ready" => format!("󰐊 ready · +{elapsed}"),
            "publication_started" => format!("󰒓 publishing · {elapsed}"),
            "completed" => format!("󰄬 turn completed · {elapsed}"),
            _ => format!("󰐊 {label} · {elapsed}"),
        };
    }
    if let Some(detail) = text.strip_prefix("[working]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            "󰔟 working".into()
        } else {
            format!("󰔟 working · {detail}")
        };
    }
    if let Some(detail) = text.strip_prefix("[ready]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            "󰐊 ready".into()
        } else {
            format!("󰐊 ready · {detail}")
        };
    }
    text.to_string()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionThread {
    id: String,
    parent: Option<String>,
    task: Option<String>,
    is_foreground: bool,
    items: Vec<SessionItem>,
    #[serde(default)]
    metrics: HashMap<String, TurnMetrics>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionItem {
    kind: String,
    text: String,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    tool_id: Option<String>,
    #[serde(default)]
    turn: Option<String>,
    #[serde(default)]
    timestamp: u64,
}

fn kind_str(k: &ItemKind) -> &'static str {
    match k {
        ItemKind::User => "user",
        ItemKind::PendingReply => "pending_reply",
        ItemKind::Reply => "reply",
        ItemKind::Tool => "tool",
        ItemKind::ToolResult => "tool_result",
        ItemKind::System => "system",
        ItemKind::Spawn => "spawn",
        ItemKind::SpawnResult => "spawn_result",
        ItemKind::Error => "error",
    }
}

fn kind_from_str(s: &str) -> ItemKind {
    match s {
        "user" => ItemKind::User,
        "pending_reply" => ItemKind::PendingReply,
        "reply" => ItemKind::Reply,
        "tool" => ItemKind::Tool,
        "tool_result" => ItemKind::ToolResult,
        "spawn" => ItemKind::Spawn,
        "spawn_result" => ItemKind::SpawnResult,
        "error" => ItemKind::Error,
        _ => ItemKind::System,
    }
}

fn save_session(threads: &[Thread]) {
    let s: Vec<SessionThread> = threads
        .iter()
        .map(|t| SessionThread {
            id: t.id.clone(),
            parent: t.parent.clone(),
            task: t.task.clone(),
            is_foreground: t.is_foreground,
            metrics: t.metrics.clone(),
            items: t
                .items
                .iter()
                .map(|i| SessionItem {
                    kind: kind_str(&i.kind).to_string(),
                    text: if i.kind == ItemKind::Reply {
                        sanitize_reply_text(&i.text)
                    } else {
                        i.text.clone()
                    },
                    hidden: i.hidden,
                    output: i.output.clone(),
                    tool_id: i.tool_id.clone(),
                    turn: i.turn.clone(),
                    timestamp: i.timestamp,
                })
                .collect(),
        })
        .collect();
    if let Ok(json) = serde_json::to_string_pretty(&s) {
        let _ = std::fs::write(session_file(), json);
    }
}

fn load_session() -> Vec<Thread> {
    let Ok(data) = std::fs::read_to_string(session_file()) else {
        return vec![Thread::new_foreground()];
    };
    let Ok(list) = serde_json::from_str::<Vec<SessionThread>>(&data) else {
        return vec![Thread::new_foreground()];
    };
    let threads: Vec<Thread> = list
        .into_iter()
        .map(|t| Thread {
            id: t.id,
            parent: t.parent,
            task: t.task,
            is_foreground: t.is_foreground,
            collapsed: !t.is_foreground,
            streaming: false,
            last_activity: Instant::now(),
            revision: 0,
            items: t
                .items
                .into_iter()
                .map(|i| {
                    let kind = kind_from_str(&i.kind);
                    let hidden = i.hidden || kind == ItemKind::Tool || kind == ItemKind::ToolResult;
                    let text = if kind == ItemKind::Reply {
                        sanitize_reply_text(&i.text)
                    } else {
                        i.text
                    };
                    Item {
                        kind,
                        text,
                        hidden,
                        output: i.output,
                        tool_id: i.tool_id,
                        turn: i.turn,
                        timestamp: i.timestamp,
                    }
                })
                .collect(),
            usage: HashMap::new(),
            metrics: t.metrics,
        })
        .collect();
    if threads.is_empty() {
        vec![Thread::new_foreground()]
    } else {
        threads
    }
}

// ---- main loop -----------------------------------------------------------

pub fn run() -> io::Result<()> {
    let current_daemon_pid = Client::connect()
        .ok()
        .and_then(|mut client| client.daemon_status().ok().map(|info| info.pid));
    let previous_daemon_pid = std::fs::read_to_string(daemon_session_file())
        .ok()
        .and_then(|pid| pid.trim().parse::<u32>().ok());
    let daemon_changed = current_daemon_pid.is_some() && current_daemon_pid != previous_daemon_pid;
    let mut threads: Vec<Thread> = if daemon_changed {
        let _ = std::fs::remove_file(session_file());
        vec![Thread::new_foreground()]
    } else {
        load_session()
    };
    if let Some(pid) = current_daemon_pid {
        let _ = std::fs::write(daemon_session_file(), pid.to_string());
    }
    // Ensure the foreground thread exists.
    if !threads.iter().any(|t| t.is_foreground) {
        threads.insert(0, Thread::new_foreground());
    }

    let (sub_out, sub_rx) = mpsc::channel::<TuiEvent>();
    let mut subscribed: HashMap<String, ()> = HashMap::new();
    let mut seen_events: HashSet<(String, u64)> = HashSet::new();
    let mut seen_interactions: HashSet<(String, String, u64)> = HashSet::new();
    let mut agent_infos: HashMap<String, AgentInfo> = HashMap::new();
    let config = tachyon_util::config::Config::load();
    let mut daemon: Option<DaemonInfo> = None;
    let mut daemon_since: Option<Instant> = None;
    let mut input = String::new();
    let mut input_cursor: usize = 0; // char index into `input`
    let mut foreground_busy = false;
    let mut foreground_activity = "working".to_string();

    // Chat view state.
    let mut chat_scroll: usize = 0;
    let mut chat_follow: bool = true;

    // Floating agent pane.
    let mut pane_open: bool = false;
    let mut pane_tab = PaneTab::Foreground;
    let mut commands_open: bool = false;
    let mut info_open: bool = false;
    let mut show_traces: bool = false;

    // Pane focus: 0 is the daemon; agent threads start at 1. Default to the
    // Foreground so destructive controls never target the daemon by accident.
    let mut focus = foreground_focus(&threads);

    let mut last_poll = Instant::now() - Duration::from_secs(1);
    let mut last_save = Instant::now();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    stdout.execute(EnableMouseCapture)?;
    stdout.execute(crossterm::event::EnableBracketedPaste)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut redraw = true;
    let mut input_redraw = true;
    let mut last_draw = Instant::now() - Duration::from_secs(1);

    loop {
        // Poll daemon + agents; subscribe to new agents.
        if last_poll.elapsed() > Duration::from_secs(1) {
            last_poll = Instant::now();
            redraw = true;
            match Client::connect() {
                Ok(mut c) => {
                    let next_daemon = c.daemon_status().ok();
                    if daemon.is_none() && next_daemon.is_some() {
                        daemon_since = Some(Instant::now());
                    }
                    daemon = next_daemon;
                    match c.agent_list() {
                        Ok(list) => {
                            // The daemon is authoritative for the live process list.
                            // Do not retain rows from a previous daemon instance.
                            agent_infos.clear();
                            for a in &list {
                                agent_infos.insert(a.id.clone(), a.clone());
                                if !subscribed.contains_key(&a.id) {
                                    subscribed.insert(a.id.clone(), ());
                                    spawn_stream_thread(a.id.clone(), sub_out.clone());
                                }
                            }
                            // The foreground stream is separate from the worker list.
                            if !subscribed.contains_key(FOREGROUND_ID) {
                                subscribed.insert(FOREGROUND_ID.into(), ());
                                spawn_stream_thread(FOREGROUND_ID.into(), sub_out.clone());
                            }
                        }
                        Err(_) => {
                            agent_infos.clear();
                        }
                    }
                    let max_focus = pane_agent_ids(&agent_infos).len()
                        + usize::from(agent_infos.contains_key(FOREGROUND_ID));
                    if focus > max_focus {
                        focus = max_focus;
                    }
                }
                Err(_) => {
                    daemon = None;
                    daemon_since = None;
                    agent_infos.clear();
                }
            }
        }

        // Drain subscription events into threads.
        while let Ok(ev) = sub_rx.try_recv() {
            redraw = true;
            match ev {
                TuiEvent::Interaction { agent_id, envelope } => {
                    let identity = (
                        envelope.metadata.conversation_id.clone(),
                        envelope.metadata.message_id.clone(),
                        envelope.metadata.generation,
                    );
                    if !seen_interactions.insert(identity) {
                        continue;
                    }
                    let is_foreground = agent_id == FOREGROUND_ID;
                    let idx = find_or_create_thread(&mut threads, &agent_id, is_foreground, None);
                    match &envelope.event {
                        InteractionEvent::UserTurnAccepted { .. }
                        | InteractionEvent::ConversationDelta { .. }
                        | InteractionEvent::ConversationIntentProduced { .. } => {
                            foreground_busy = true;
                            foreground_activity = "working".into();
                        }
                        InteractionEvent::ConversationFinished { .. }
                        | InteractionEvent::ForegroundRequestTimedOut { .. } => {
                            foreground_busy = false;
                            foreground_activity = "working".into();
                        }
                        InteractionEvent::UserVisibleNotificationPublished { .. } => {}
                    }
                    apply_interaction_event(&mut threads[idx], envelope);
                }
                TuiEvent::Structured { agent_id, envelope } => {
                    if !accept_event(&mut seen_events, &envelope) {
                        continue;
                    }
                    record_correlated_metrics(&mut threads, &envelope);
                    let actor = envelope.actor.clone();
                    let event = envelope.kind;
                    if agent_id == FOREGROUND_ID {
                        match &event {
                            AgentEvent::Status { phase, message, .. }
                                if matches!(phase.as_str(), "working" | "queued") =>
                            {
                                foreground_busy = true;
                                foreground_activity = if message.trim().is_empty() {
                                    phase.clone()
                                } else {
                                    message.clone()
                                };
                            }
                            AgentEvent::Reply { .. } | AgentEvent::Error { .. } => {
                                foreground_busy = false;
                                foreground_activity = "working".to_string();
                            }
                            AgentEvent::ToolStarted { name, .. } => {
                                foreground_busy = true;
                                foreground_activity = format!("using {name}");
                            }
                            _ => {}
                        }
                    }
                    let is_foreground = agent_id == FOREGROUND_ID;
                    let idx = find_or_create_thread(&mut threads, &agent_id, is_foreground, None);
                    apply_actor_event(&mut threads[idx], event, &actor);
                }
                TuiEvent::Line {
                    agent_id,
                    stream,
                    data,
                } => {
                    if agent_id == FOREGROUND_ID && stream == EventStream::Stdout {
                        if data.contains("[status] working") || data.contains("[status] queued") {
                            foreground_busy = true;
                            foreground_activity = data
                                .split_once(']')
                                .map(|(_, rest)| rest.trim().to_string())
                                .filter(|text| !text.is_empty())
                                .unwrap_or_else(|| "working".to_string());
                        } else if data.starts_with("[agent]") {
                            foreground_busy = false;
                            foreground_activity = "working".to_string();
                        }
                    }
                    let is_foreground = agent_id == FOREGROUND_ID;
                    let text = if stream == EventStream::Stderr {
                        format!("⚠ {data}")
                    } else if data.starts_with("[ghost:error]") {
                        format!("⚠{}", &data["[ghost:error]".len()..].trim())
                    } else if data.starts_with("[foreground:error]") {
                        format!("⚠{}", &data["[foreground:error]".len()..].trim())
                    } else {
                        data
                    };

                    let idx = find_or_create_thread(&mut threads, &agent_id, is_foreground, None);
                    let thread = &mut threads[idx];
                    classify_line(thread, &text);
                }
                TuiEvent::Ended { agent_id, summary } => {
                    let is_foreground = agent_id == FOREGROUND_ID;
                    let idx = find_or_create_thread(&mut threads, &agent_id, is_foreground, None);
                    threads[idx].add(ItemKind::System, format!("∎ {summary}"));
                }
                TuiEvent::ChatResult { error: Some(error) } => {
                    let idx = find_or_create_thread(&mut threads, FOREGROUND_ID, true, None);
                    threads[idx].add(ItemKind::Error, format!("⚠ {error}"));
                }
                TuiEvent::ChatResult { error: None } => {}
            }
        }

        if chat_follow {
            chat_scroll = 0;
        }

        // Periodic session save.
        if last_save.elapsed() > Duration::from_secs(5) {
            last_save = Instant::now();
            save_session(&threads);
        }

        // Streaming agents can emit hundreds of trace events per second. Batch
        // those redraws, but let user input redraw immediately.
        if redraw && (input_redraw || last_draw.elapsed() >= Duration::from_millis(33)) {
            terminal.draw(|f| {
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

                draw_conversation(
                    f,
                    chunks[0],
                    &threads,
                    focus,
                    chat_scroll,
                    show_traces,
                    foreground_busy,
                    &foreground_activity,
                );
                if pane_open {
                    let popup =
                        popup_rect(chunks[0], 72, chunks[0].height.saturating_sub(4).min(18));
                    draw_agent_pane(
                        f,
                        popup,
                        &threads,
                        focus,
                        daemon.as_ref(),
                        daemon_since,
                        &agent_infos,
                        pane_tab,
                    );
                }
                if info_open {
                    draw_info_panel(
                        f,
                        area,
                        daemon.as_ref(),
                        daemon_since,
                        &agent_infos,
                        &threads,
                        &config,
                        if daemon_changed { "fresh" } else { "active" },
                    );
                }
                draw_input(
                    f,
                    chunks[1],
                    &input,
                    input_cursor,
                    daemon.as_ref(),
                    foreground_busy,
                );
                draw_statusline(
                    f,
                    chunks[2],
                    daemon.as_ref(),
                    &agent_infos,
                    &threads,
                    &config,
                    show_traces,
                    if daemon_changed { "fresh" } else { "active" },
                );

                if commands_open {
                    draw_command_palette(f, area);
                }
            })?;
            redraw = false;
            input_redraw = false;
            last_draw = Instant::now();
        }

        // Keep keyboard latency below one frame while still allowing streamed
        // agent events to update the conversation continuously.
        if event::poll(Duration::from_millis(16))? {
            let input_event = event::read()?;
            redraw = true;
            input_redraw = true;
            match input_event {
                Event::Key(key) => match key.code {
                    KeyCode::Char('c')
                        if key.modifiers.contains(KeyModifiers::CONTROL)
                            && key.modifiers.contains(KeyModifiers::SHIFT) =>
                    {
                        yank_reply(&threads, focus);
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        clear_history(&mut threads);
                        focus = foreground_focus(&threads);
                        chat_scroll = 0;
                        chat_follow = true;
                    }
                    KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        commands_open = !commands_open;
                        if commands_open {
                            pane_open = false;
                            info_open = false;
                        }
                    }
                    KeyCode::Char('i') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        info_open = !info_open;
                        if info_open {
                            pane_open = false;
                            commands_open = false;
                        }
                    }
                    KeyCode::Char('o') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        show_traces = !show_traces;
                    }
                    KeyCode::Esc if commands_open => commands_open = false,
                    KeyCode::Esc => break,
                    KeyCode::Tab => {
                        pane_open = !pane_open;
                        if pane_open {
                            commands_open = false;
                            info_open = false;
                        }
                    }
                    KeyCode::Up | KeyCode::PageUp => {
                        if pane_open {
                            focus = focus.saturating_sub(1);
                        } else {
                            chat_follow = false;
                            let step = if key.code == KeyCode::PageUp { 24 } else { 1 };
                            chat_scroll = chat_scroll.saturating_add(step);
                        }
                    }
                    KeyCode::Down | KeyCode::PageDown => {
                        if pane_open {
                            if focus + 1 <= threads.len() {
                                focus += 1;
                            }
                        } else {
                            let step = if key.code == KeyCode::PageDown { 24 } else { 1 };
                            chat_scroll = chat_scroll.saturating_sub(step);
                            if chat_scroll == 0 {
                                chat_follow = true;
                            }
                        }
                    }
                    KeyCode::End => {
                        chat_follow = true;
                        chat_scroll = 0;
                    }
                    // Ctrl+Backspace / Ctrl+H: delete the previous word.
                    KeyCode::Backspace | KeyCode::Delete | KeyCode::Char('h')
                        if key.modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        delete_word_left(&mut input, &mut input_cursor);
                    }
                    // Pane control keys: await/release/stop/kill/restart the focused agent
                    // (only while the floating pane is open AND input is empty,
                    // so typing still works).
                    KeyCode::Char('a')
                    | KeyCode::Char('s')
                    | KeyCode::Char('x')
                    | KeyCode::Char('k')
                    | KeyCode::Char('r')
                    | KeyCode::Char('u')
                        if pane_open && input.is_empty() =>
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
                        pane_control(verb, focus, &agent_infos, &mut threads);
                    }
                    KeyCode::Char('S') if pane_open && input.is_empty() => {
                        daemon_control("start", &mut threads);
                    }
                    KeyCode::Enter => {
                        // Shift/Ctrl+Enter inserts a newline; plain Enter submits.
                        if key
                            .modifiers
                            .intersects(KeyModifiers::SHIFT | KeyModifiers::CONTROL)
                        {
                            insert_at(&mut input, &mut input_cursor, '\n');
                            continue;
                        }
                        let cmd = input.trim().to_string();
                        if cmd.is_empty() {
                            // No text: toggle collapse on the focused thread.
                            if focus > 0 {
                                if let Some(t) = threads.get_mut(focus - 1) {
                                    t.collapsed = !t.collapsed;
                                }
                            }
                            input.clear();
                            input_cursor = 0;
                            continue;
                        }
                        if let Some(rest) = cmd.strip_prefix('/') {
                            match rest {
                                "exit" | "quit" | "q" => break,
                                "clear" | "reset" => {
                                    clear_history(&mut threads);
                                    focus = foreground_focus(&threads);
                                    chat_scroll = 0;
                                    chat_follow = true;
                                }
                                _ => handle_slash(rest, &mut threads),
                            }
                            input.clear();
                            input_cursor = 0;
                            continue;
                        }
                        // User message -> foreground thread.
                        let idx = find_or_create_thread(&mut threads, FOREGROUND_ID, true, None);
                        threads[idx].add(ItemKind::User, cmd.clone());
                        threads[idx].reserve_reply();
                        input.clear();
                        input_cursor = 0;
                        foreground_busy = true;
                        let chat_out = sub_out.clone();
                        std::thread::spawn(move || {
                            let error = Client::connect()
                                .and_then(|mut c| c.foreground_chat(cmd))
                                .err()
                                .map(|e| e.to_string());
                            let _ = chat_out.send(TuiEvent::ChatResult { error });
                        });
                    }
                    KeyCode::Char('w') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        delete_word_left(&mut input, &mut input_cursor);
                    }
                    KeyCode::Char(c)
                        if !key.modifiers.intersects(
                            KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                        ) =>
                    {
                        insert_at(&mut input, &mut input_cursor, c)
                    }
                    KeyCode::Backspace => {
                        backspace_at(&mut input, &mut input_cursor);
                    }
                    KeyCode::Left if pane_open && input.is_empty() => {
                        pane_tab = PaneTab::Foreground;
                        if focus > 1 {
                            focus = 0;
                        }
                    }
                    KeyCode::Right if pane_open && input.is_empty() => {
                        pane_tab = PaneTab::Agents;
                        if focus < 2 && !pane_agent_ids(&agent_infos).is_empty() {
                            focus = 2;
                        }
                    }
                    KeyCode::Left => {
                        if key.modifiers.contains(KeyModifiers::CONTROL) {
                            move_word_left(&input, &mut input_cursor);
                        } else if input_cursor > 0 {
                            input_cursor -= 1;
                        }
                    }
                    KeyCode::Right => {
                        if key.modifiers.contains(KeyModifiers::CONTROL) {
                            move_word_right(&input, &mut input_cursor);
                        } else if input_cursor < chars(&input) {
                            input_cursor += 1;
                        }
                    }
                    KeyCode::Home => input_cursor = 0,
                    KeyCode::Delete => delete_at(&mut input, &mut input_cursor),
                    _ => {}
                },
                // Bracketed paste: insert pasted text at the cursor.
                Event::Paste(text) => {
                    paste_text(&mut input, &mut input_cursor, &text);
                }
                Event::Mouse(m) => {
                    use crossterm::event::{MouseButton, MouseEventKind};
                    match m.kind {
                        MouseEventKind::ScrollUp => {
                            chat_follow = false;
                            chat_scroll = chat_scroll.saturating_add(8);
                        }
                        MouseEventKind::ScrollDown => {
                            chat_scroll = chat_scroll.saturating_sub(8);
                            if chat_scroll == 0 {
                                chat_follow = true;
                            }
                        }
                        MouseEventKind::Down(MouseButton::Left) => {
                            let size = terminal.size()?;
                            if m.row == size.height.saturating_sub(1) {
                                let controls = if show_traces {
                                    " 󰀄 AGENTS ·  HELP · 󰈈 COMPACT · 󰋼 INFO "
                                } else {
                                    " 󰀄 AGENTS ·  HELP · 󰈈 TRACES · 󰋼 INFO "
                                };
                                let controls_start = 12u16;
                                if m.column >= controls_start
                                    && m.column < controls_start + controls.chars().count() as u16
                                {
                                    let relative = m.column - controls_start;
                                    let help_start = 12;
                                    let trace_start = if show_traces { 21 } else { 21 };
                                    let info_start = if show_traces { 34 } else { 32 };
                                    if relative < help_start {
                                        pane_open = true;
                                        commands_open = false;
                                        info_open = false;
                                    } else if relative < trace_start {
                                        commands_open = true;
                                        pane_open = false;
                                        info_open = false;
                                    } else if relative < info_start {
                                        show_traces = !show_traces;
                                    } else {
                                        info_open = true;
                                        pane_open = false;
                                        commands_open = false;
                                    }
                                    continue;
                                }
                            }
                            if pane_open {
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
                                let tabs_x = pane.x.saturating_add(2);
                                let foreground_width = " FOREGROUND ".chars().count() as u16;
                                let agents_x = tabs_x.saturating_add(foreground_width + 1);
                                if m.row >= pane.y
                                    && m.row <= pane.y.saturating_add(1)
                                    && m.column >= tabs_x
                                    && m.column < inner_right
                                {
                                    pane_tab = if m.column < agents_x {
                                        PaneTab::Foreground
                                    } else {
                                        PaneTab::Agents
                                    };
                                    continue;
                                }
                                // The table header occupies the first inner row.
                                let first_data_row = pane.y.saturating_add(2);
                                if m.column >= inner_x
                                    && m.column < inner_right
                                    && m.row >= first_data_row
                                    && m.row < pane.y + pane.height.saturating_sub(3)
                                {
                                    let row = (m.row - first_data_row) as usize;
                                    match pane_tab {
                                        PaneTab::Foreground => {
                                            if row <= 1 {
                                                focus = row;
                                            }
                                        }
                                        PaneTab::Agents => {
                                            if row < pane_agent_ids(&agent_infos).len() {
                                                focus = row + 2;
                                            }
                                        }
                                    }
                                    continue;
                                }
                            }
                            let (vy, vh, top) = *VIEW.lock().unwrap();
                            // Only map clicks inside the conversation area.
                            if m.row >= vy && m.row < vy + vh {
                                let row = top + (m.row - vy) as usize;
                                let hit = HITS.lock().unwrap().get(row).cloned().flatten();
                                if let Some((ti, ii)) = hit {
                                    if let Some(thread) = threads.get_mut(ti) {
                                        if !thread.is_foreground && thread.collapsed {
                                            thread.collapsed = false;
                                            thread.touch();
                                            continue;
                                        }
                                        thread.collapsed = false;
                                        if let Some(item) = thread.items.get_mut(ii) {
                                            item.hidden = !item.hidden;
                                        }
                                        thread.touch();
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    save_session(&threads);
    disable_raw_mode()?;
    terminal.show_cursor()?;
    terminal
        .backend_mut()
        .execute(crossterm::event::DisableBracketedPaste)?;
    terminal.backend_mut().execute(DisableMouseCapture)?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    Ok(())
}

fn record_correlated_metrics(threads: &mut Vec<Thread>, envelope: &EventEnvelope) {
    let root = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    threads[root].touch();
    match (&envelope.actor, &envelope.kind) {
        (
            Actor::Foreground,
            AgentEvent::Timing {
                turn,
                stage,
                elapsed_ms,
            },
        ) => {
            let metrics = threads[root].metrics.entry(turn.to_string()).or_default();
            match stage.as_str() {
                "first_visible" => metrics.first_visible_ms = Some(*elapsed_ms),
                "completed" => metrics.completed_ms = Some(*elapsed_ms),
                _ => {}
            }
        }
        (
            Actor::Foreground,
            AgentEvent::Usage {
                turn: Some(turn),
                prompt_tokens,
                completion_tokens,
                total_tokens,
            },
        ) => {
            threads[root]
                .metrics
                .entry(turn.to_string())
                .or_default()
                .self_usage = Some(token_totals(
                *prompt_tokens,
                *completion_tokens,
                *total_tokens,
            ));
        }
        (
            Actor::Worker { id },
            AgentEvent::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..
            },
        ) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let assignment = envelope
                .task_id
                .clone()
                .unwrap_or_else(|| format!("{}:{}", envelope.session_id, id));
            threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .worker_usage
                .insert(
                    assignment,
                    token_totals(*prompt_tokens, *completion_tokens, *total_tokens),
                );
        }
        _ => {}
    }
}

fn token_totals(prompt: u32, completion: u32, total: u32) -> TokenTotals {
    TokenTotals {
        prompt: u64::from(prompt),
        completion: u64::from(completion),
        total: u64::from(if total == 0 {
            prompt.saturating_add(completion)
        } else {
            total
        }),
    }
}

/// Find a thread by id, creating one if missing (and attaching to the
/// foreground as parent when it's a worker). Returns the thread's index.
fn find_or_create_thread(
    threads: &mut Vec<Thread>,
    id: &str,
    is_foreground: bool,
    task: Option<String>,
) -> usize {
    if let Some(idx) = threads.iter().position(|t| t.id == id) {
        return idx;
    }
    threads.push(Thread {
        id: id.to_string(),
        parent: if is_foreground {
            None
        } else {
            Some(FOREGROUND_ID.into())
        },
        task,
        is_foreground,
        collapsed: !is_foreground,
        streaming: false,
        last_activity: Instant::now(),
        revision: 0,
        items: Vec::new(),
        usage: HashMap::new(),
        metrics: HashMap::new(),
    });
    threads.len() - 1
}

/// Route a streamed line into the right kind of item.
fn classify_line(t: &mut Thread, text: &str) {
    t.touch();
    let (turn, text) = if let Some(rest) = text.strip_prefix("[turn:") {
        if let Some((turn, rest)) = rest.split_once("] ") {
            (Some(turn.to_string()), rest)
        } else {
            (None, text)
        }
    } else if text.starts_with("[user]") {
        (None, text)
    } else {
        (None, text)
    };
    if text.starts_with("[user]") {
        // Reconcile the optimistic local message with the daemon-assigned turn
        // instead of retaining a second uncorrelated conversation history.
        let body = text["[user]".len()..].trim_start();
        accept_user_turn(t, body, turn);
        return;
    } else if let Some(rest) = text.strip_prefix("[tool-result:") {
        if let Some((id, output)) = rest.split_once("] ") {
            t.add_tool_result(id.to_string(), output.to_string(), turn);
        }
    } else if let Some(rest) = text.strip_prefix("[tool:") {
        if let Some((id, call)) = rest.split_once("] ") {
            t.add_tool(call.to_string(), id.to_string(), turn);
        }
    } else if text.starts_with("[tool-result]") {
        t.add_turn(
            ItemKind::ToolResult,
            text["[tool-result]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[tool]") {
        t.add_turn(
            ItemKind::Tool,
            text["[tool]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[daemon:spawn]") {
        let spec = &text["[daemon:spawn]".len()..].trim();
        t.add_turn(ItemKind::Spawn, format!("⇣ spawn: {spec}"), turn);
    } else if text.starts_with("[daemon:result]") {
        let rest = &text["[daemon:result]".len()..].trim_start();
        t.add_turn(ItemKind::SpawnResult, format!("⇡ worker → {rest}"), turn);
    } else if text.starts_with("[worker:start]") {
        t.add_turn(
            ItemKind::Spawn,
            format!(
                "spawned worker {}",
                text["[worker:start]".len()..].trim_start()
            ),
            turn,
        );
    } else if text.starts_with("[worker:result]") {
        t.add_turn(
            ItemKind::SpawnResult,
            text["[worker:result]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[daemon]") {
        t.add(
            ItemKind::System,
            text["[daemon]".len()..].trim_start().to_string(),
        );
    } else if text.starts_with("[ghost") {
        t.add(ItemKind::System, text.to_string());
    } else if text.starts_with("[agent]") {
        t.finish_reply(text["[agent]".len()..].trim_start().to_string(), turn);
    } else if text.starts_with("[status]") {
        t.add_turn(
            ItemKind::System,
            text["[status]".len()..].trim_start().to_string(),
            turn,
        );
    } else if text.starts_with("[text]") {
        t.add_reply_fragment(text["[text]".len()..].to_string(), turn, false);
    } else if text.starts_with("[text-break]") {
        t.add_reply_fragment(String::new(), turn, true);
    } else if text.starts_with('⚠') {
        if is_worker_runtime_detail(text) {
            t.add(ItemKind::System, text.to_string());
        } else {
            t.add(ItemKind::Error, text.to_string());
        }
    } else if !text.trim().is_empty() {
        // Plain content glues onto whatever the thread was doing.
        t.add(ItemKind::System, text.to_string());
    }
}

fn accept_user_turn(thread: &mut Thread, text: &str, turn: Option<String>) {
    if let Some(index) = thread.items.iter().rposition(|item| {
        item.kind == ItemKind::User && item.turn.is_none() && item.text.trim() == text.trim()
    }) {
        thread.items[index].turn = turn.clone();
        if let Some(pending) = thread.items[index + 1..]
            .iter_mut()
            .find(|item| item.kind == ItemKind::PendingReply && item.turn.is_none())
        {
            pending.turn = turn;
        }
    }
}

fn apply_interaction_event(thread: &mut Thread, envelope: InteractionEventEnvelope) {
    let turn = envelope.metadata.turn_id;
    match envelope.event {
        InteractionEvent::UserTurnAccepted { text } => accept_user_turn(thread, &text, turn),
        InteractionEvent::ConversationDelta { text } => {
            thread.add_reply_fragment(text, turn, false)
        }
        InteractionEvent::ConversationFinished { text } => thread.finish_reply(text, turn),
        InteractionEvent::ConversationIntentProduced { .. } => thread.touch(),
        InteractionEvent::ForegroundRequestTimedOut { deadline_ms } => {
            if let Some(pending) = thread
                .items
                .iter_mut()
                .rev()
                .find(|item| item.kind == ItemKind::PendingReply && item.turn == turn)
            {
                pending.kind = ItemKind::Error;
                pending.text = format!("request timed out after {deadline_ms}ms");
            } else {
                thread.add_turn(
                    ItemKind::Error,
                    format!("request timed out after {deadline_ms}ms"),
                    turn,
                );
            }
            thread.streaming = false;
            thread.touch();
        }
        InteractionEvent::UserVisibleNotificationPublished { text } => {
            thread.add_turn(ItemKind::System, text, turn)
        }
    }
}

fn apply_agent_event(thread: &mut Thread, event: AgentEvent) {
    match event {
        AgentEvent::Status {
            turn,
            phase,
            message,
        } => thread.add_turn(
            ItemKind::System,
            format!("[{phase}] {message}"),
            turn.map(|t| t.to_string()),
        ),
        AgentEvent::ReplyDelta { turn, text } => {
            thread.add_reply_fragment(text, turn.map(|t| t.to_string()), false);
        }
        AgentEvent::Reply {
            turn,
            text,
            final_reply: _,
        } => thread.finish_reply(text, turn.map(|t| t.to_string())),
        AgentEvent::Timing {
            turn,
            stage,
            elapsed_ms,
        } => thread.add_turn(
            ItemKind::System,
            format!("[timing] {stage} {elapsed_ms}ms"),
            Some(turn.to_string()),
        ),
        AgentEvent::WorkerStarted {
            turn,
            worker_id,
            objective,
        } => thread.add_turn(
            ItemKind::Spawn,
            format!("worker {worker_id}: {objective}"),
            turn.map(|turn| turn.to_string()),
        ),
        AgentEvent::Usage {
            turn: Some(turn),
            prompt_tokens,
            completion_tokens,
            total_tokens,
        } => {
            thread
                .usage
                .insert(turn, (prompt_tokens, completion_tokens, total_tokens));
        }
        AgentEvent::Usage { turn: None, .. } => {}
        AgentEvent::WorkerCompleted {
            worker_id,
            objective,
            result,
            ..
        } => {
            let turn = thread
                .items
                .iter()
                .rev()
                .find(|item| item.kind == ItemKind::Spawn && item.text.contains(&worker_id))
                .and_then(|item| item.turn.clone());
            thread.add_turn(
                ItemKind::SpawnResult,
                format!("worker {worker_id}: {objective}\n{result}"),
                turn,
            );
        }
        AgentEvent::WorkCandidate { .. } => {}
        AgentEvent::WorkProgress { event } => thread.add_turn(
            ItemKind::System,
            format!("work {}: {:?}", event.work_id, event.kind),
            None,
        ),
        AgentEvent::WorkResult { result } => {
            let (kind, text) = match result.outcome {
                WorkOutcome::Completed { result: text, .. } => (ItemKind::SpawnResult, text),
                WorkOutcome::Blocked { reason } => (ItemKind::Error, format!("blocked: {reason}")),
                WorkOutcome::Failed { message } => (ItemKind::Error, message),
                WorkOutcome::Cancelled { reason } => {
                    (ItemKind::Error, format!("cancelled: {reason}"))
                }
                WorkOutcome::TimedOut { .. } => (ItemKind::Error, "timed out".into()),
            };
            thread.add_turn(
                kind,
                format!("work {}: {}\n{text}", result.work_id, result.objective),
                None,
            );
        }
        AgentEvent::WorkerReleaseRequested { reason } => thread.add_turn(
            ItemKind::System,
            format!("worker release requested: {reason}"),
            None,
        ),
        AgentEvent::ToolStarted {
            turn,
            id,
            name,
            arguments,
        } => {
            let turn = turn.map(|t| t.to_string());
            thread.add_tool(format!("{name} {arguments}"), id, turn);
        }
        AgentEvent::ToolFinished { turn, id, output } => {
            thread.add_tool_result(id, output, turn.map(|t| t.to_string()));
        }
        AgentEvent::Error { turn, message } => {
            thread.add_turn(ItemKind::Error, message, turn.map(|t| t.to_string()));
        }
    }
}

fn apply_actor_event(thread: &mut Thread, mut event: AgentEvent, actor: &Actor) {
    if matches!(actor, Actor::Background) {
        match &mut event {
            AgentEvent::Reply { text, .. } => *text = format!("[Background] {text}"),
            AgentEvent::Status { message, .. } => *message = format!("[Background] {message}"),
            AgentEvent::ToolStarted { name, .. } => *name = format!("Background::{name}"),
            _ => {}
        }
    }
    apply_agent_event(thread, event);
}

fn handle_slash(cmd: &str, threads: &mut Vec<Thread>) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let foreground_idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    let foreground = &mut threads[foreground_idx];
    match parts.as_slice() {
        ["replan", id, task @ ..] if !task.is_empty() => {
            let objective = task.join(" ");
            let result =
                Client::connect().and_then(|mut c| c.agent_replan((*id).to_string(), objective));
            match result {
                Ok(info) => {
                    foreground.add(ItemKind::System, format!("replan {id} → {:?}", info.state))
                }
                Err(e) => foreground.add(ItemKind::Error, format!("⚠ {e}")),
            }
        }
        ["await", id]
        | ["stop", id]
        | ["interrupt", id]
        | ["kill", id]
        | ["release", id]
        | ["restart", id]
        | ["resume", id] => {
            let verb = parts[0];
            let result = Client::connect().and_then(|mut c| match verb {
                "await" => c.agent_await(id.to_string()),
                "stop" => c.agent_stop(id.to_string()),
                "interrupt" => c.agent_interrupt(id.to_string()),
                "kill" => c.agent_kill(id.to_string()),
                "release" => c.agent_release(id.to_string()),
                "restart" => c.agent_restart(id.to_string()),
                _ => c.agent_resume(id.to_string()),
            });
            match result {
                Ok(info) => {
                    foreground.add(ItemKind::System, format!("{verb} {id} → {:?}", info.state))
                }
                Err(e) => foreground.add(ItemKind::Error, format!("⚠ {e}")),
            }
        }
        ["help"] | [] => {
            foreground.add(ItemKind::System, "/clear       clear chat history".into());
            foreground.add(ItemKind::System, "/stop <id>  stop an agent".into());
            foreground.add(
                ItemKind::System,
                "/await <id>  show current agent state".into(),
            );
            foreground.add(
                ItemKind::System,
                "/interrupt <id>  interrupt an agent".into(),
            );
            foreground.add(ItemKind::System, "/kill <id>  kill an agent".into());
            foreground.add(
                ItemKind::System,
                "/release <id>  terminate and clean an agent".into(),
            );
            foreground.add(
                ItemKind::System,
                "/replan <id> <task>  replace an agent objective".into(),
            );
            foreground.add(ItemKind::System, "/restart <id>  restart an agent".into());
            foreground.add(ItemKind::System, "/resume <id>  resume an agent".into());
            foreground.add(ItemKind::System, "/exit       quit Tachyon".into());
        }
        _ => foreground.add(ItemKind::Error, format!("unknown slash command /{cmd}")),
    }
}

/// Pane control: lifecycle operations on the agent thread at `focus` (1:1 mapping to
/// the daemon API). The foreground is not controllable from the pane.
fn pane_control(
    verb: &str,
    focus: usize,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &mut Vec<Thread>,
) {
    if focus == 0 {
        let action = match verb {
            "stop" => "stop",
            "restart" => "restart",
            "kill" => "stop",
            _ => return,
        };
        daemon_control(action, threads);
        return;
    }
    if focus == 1 {
        let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
        threads[idx].add(
            ItemKind::System,
            "can't manage the foreground from the pane".into(),
        );
        return;
    }
    let Some(id) = pane_agent_ids(agent_infos).get(focus - 2).cloned() else {
        return;
    };
    let result = Client::connect().and_then(|mut c| match verb {
        "await" => c.agent_await(id.clone()),
        "stop" => c.agent_stop(id.clone()),
        "interrupt" => c.agent_interrupt(id.clone()),
        "kill" => c.agent_kill(id.clone()),
        "release" => c.agent_release(id.clone()),
        "restart" => c.agent_restart(id.clone()),
        _ => c.agent_resume(id.clone()),
    });
    let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    let foreground = &mut threads[idx];
    match result {
        Ok(info) => foreground.add(
            ItemKind::System,
            format!("pane: {verb} {id} → {:?}", info.state),
        ),
        Err(e) => foreground.add(ItemKind::Error, format!("⚠ {e}")),
    }
}

fn pane_agent_ids(agent_infos: &HashMap<String, AgentInfo>) -> Vec<String> {
    let mut ids = Vec::new();
    let mut worker_ids: Vec<String> = agent_infos
        .keys()
        .filter(|id| id.as_str() != FOREGROUND_ID)
        .cloned()
        .collect();
    worker_ids.sort();
    ids.extend(worker_ids);
    ids
}

fn daemon_control(action: &str, threads: &mut Vec<Thread>) {
    let result = tachyon_cli_command()
        .arg("daemon")
        .arg(action)
        .status()
        .map_err(|error| error.to_string())
        .and_then(|status| {
            if status.success() {
                Ok(())
            } else {
                Err(format!("tachyon daemon {action} exited with {status}"))
            }
        });
    let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
    match result {
        Ok(()) => threads[idx].add(ItemKind::System, format!("daemon {action} requested")),
        Err(error) => threads[idx].add(ItemKind::Error, format!("⚠ daemon {action}: {error}")),
    }
}

fn tachyon_cli_command() -> std::process::Command {
    if let Ok(path) = std::env::var("TACHYON_CLI_BIN") {
        return std::process::Command::new(path);
    }
    if let Some(path) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|parent| parent.join("tachyon")))
        .filter(|path| path.is_file())
    {
        return std::process::Command::new(path);
    }
    std::process::Command::new("tachyon")
}

/// Copy the focused thread's last reply to the system clipboard (via
/// wl-copy/xclip/xsel) or, if none is available, to a file + stdout note.
fn yank_reply(threads: &[Thread], focus: usize) {
    let index = if focus == 0 { 0 } else { focus - 1 };
    let Some(t) = threads.get(index) else { return };
    let text = t
        .items
        .iter()
        .rev()
        .find(|i| i.kind == ItemKind::Reply)
        .map(|i| i.text.clone())
        .unwrap_or_default();
    if text.is_empty() {
        return;
    }
    // Never write status text to stderr while the alternate-screen TUI is active.
    let _ = copy_to_clipboard(&text);
}

fn copy_to_clipboard(text: &str) -> bool {
    // Try common clipboard tools (no extra deps).
    let cmds: [&[&str]; 3] = [
        &["wl-copy"],
        &["xclip", "-selection", "clipboard"],
        &["xsel", "-b"],
    ];
    for cmd in cmds {
        if let Ok(mut child) = std::process::Command::new(cmd[0])
            .args(&cmd[1..])
            .stdin(std::process::Stdio::piped())
            .spawn()
        {
            use std::io::Write;
            let _ = child.stdin.as_mut().map(|stdin| {
                let _ = stdin.write_all(text.as_bytes());
            });
            if child.wait().map(|s| s.success()).unwrap_or(false) {
                return true;
            }
        }
    }
    // Fallback: write to a file next to the session.
    if let Some(p) = session_file().parent().map(|d| d.join("clipboard.txt")) {
        if std::fs::write(&p, text).is_ok() {
            return true;
        }
    }
    false
}

fn clear_history(threads: &mut Vec<Thread>) {
    threads.clear();
    threads.push(Thread::new_foreground());
    save_session(threads);
}

fn foreground_focus(threads: &[Thread]) -> usize {
    threads
        .iter()
        .position(|thread| thread.is_foreground)
        .map(|index| index + 1)
        .unwrap_or(1)
}

// ---- drawing -------------------------------------------------------------

fn popup_rect(area: Rect, percent_x: u16, height: u16) -> Rect {
    let w = (area.width * percent_x).min(area.width.saturating_sub(4));
    let h = height.min(area.height.saturating_sub(2)).max(3);
    let x = area.width.saturating_sub(w) / 2;
    let y = area.height.saturating_sub(h) / 2;
    Rect {
        x: area.x + x,
        y: area.y + y,
        width: w,
        height: h,
    }
}

fn help_section(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {} ", title),
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn help_key(key: &str, description: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {:<14} ", key),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {description}")),
    ])
}

fn help_divider(width: usize) -> Line<'static> {
    Line::from(Span::styled(
        "─".repeat(width),
        Style::default().fg(Color::DarkGray),
    ))
}

fn column_rule() -> Span<'static> {
    Span::styled(" │ ", Style::default().fg(Color::Rgb(65, 65, 70)))
}

fn agent_table_header(first: &str) -> Line<'static> {
    let style = Style::default()
        .fg(Color::Gray)
        .add_modifier(Modifier::BOLD);
    Line::from(vec![
        Span::raw("  "),
        Span::styled(format!("{first:<18}"), style),
        column_rule(),
        Span::styled(format!("{:<13}", "STATUS"), style),
        column_rule(),
        Span::styled(format!("{:<21}", "POLICY"), style),
        column_rule(),
        Span::styled(format!("{:^9}", "AGE"), style),
        column_rule(),
        Span::styled(format!("{:^9}", "PID"), style),
        column_rule(),
        Span::styled(format!("{:^8}", "SANDBOX"), style),
        column_rule(),
        Span::styled("TASK", style),
    ])
}

fn activity_color(state: ActivityState) -> Color {
    match state {
        ActivityState::Ready | ActivityState::Completed => Color::Green,
        ActivityState::Error => Color::Red,
        _ => Color::Yellow,
    }
}

fn status_badge(text: &str, color: Color) -> Span<'static> {
    Span::styled(
        format!("{:^13}", truncate_text(text, 13)),
        Style::default().fg(Color::Black).bg(color),
    )
}

fn draw_command_palette(f: &mut Frame, area: Rect) {
    let popup = popup_rect(area, 68, 28);
    let block = Block::default()
        .title(" HELP ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(Color::Rgb(12, 12, 15)));
    let commands = vec![
        help_section("KEYBINDS"),
        Line::from(""),
        help_key("Ctrl+P", "toggle help"),
        help_key("Tab", "agents pane"),
        help_key("Ctrl+O", "show/hide traces"),
        help_key("Ctrl+Shift+C", "copy last reply"),
        help_key("Shift+Enter", "insert newline"),
        help_key("Ctrl+L", "clear chat history"),
        help_key("Ctrl+C / Esc", "quit"),
        help_divider(popup.width.saturating_sub(2) as usize),
        help_section("AGENT ACTIONS"),
        Line::from(""),
        help_key("Up / Down", "focus daemon or agent"),
        help_key("a", "await"),
        help_key("u", "resume"),
        help_key("s / k", "stop / kill"),
        help_key("r / x", "restart / release"),
        help_key("S", "start daemon"),
        help_divider(popup.width.saturating_sub(2) as usize),
        help_section("COMMANDS"),
        Line::from(""),
        help_key("/clear", "clear chat history"),
        help_key("/exit", "quit Tachyon"),
    ];
    f.render_widget(Clear, popup);
    f.render_widget(Paragraph::new(commands).block(block), popup);
}

fn draw_info_panel(
    f: &mut Frame,
    area: Rect,
    daemon: Option<&DaemonInfo>,
    daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &[Thread],
    config: &tachyon_util::config::Config,
    session_label: &str,
) {
    let popup = popup_rect(area, 58, 25);
    let block = Block::default()
        .title(" INFO ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(Color::Rgb(12, 12, 15)));
    let section = |title: &str| {
        Line::from(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
    };
    let value = |label: &str, text: String| {
        Line::from(vec![
            Span::styled(format!("  {label:<14}"), Style::default().fg(Color::Gray)),
            Span::styled(text, Style::default().fg(Color::White)),
        ])
    };
    let running = agent_infos
        .values()
        .filter(|info| matches!(info.state, AgentState::Starting | AgentState::Running))
        .count();
    let completed = agent_infos
        .values()
        .filter(|info| {
            matches!(
                info.state,
                AgentState::Completed | AgentState::Released | AgentState::Terminated
            )
        })
        .count();
    let failed = agent_infos
        .values()
        .filter(|info| info.state == AgentState::Failed)
        .count();
    let daemon_status = match daemon {
        Some(info) if info.provider_ready => format!("online · v{}", info.version),
        Some(info) => format!("online · provider unavailable · v{}", info.version),
        None => "offline".into(),
    };
    let daemon_pid = daemon
        .map(|info| info.pid.to_string())
        .unwrap_or_else(|| "-".into());
    let uptime = daemon_since
        .map(|since| format_duration(since.elapsed()))
        .unwrap_or_else(|| "-".into());
    let provider = config
        .provider
        .as_ref()
        .map(|provider| provider.name.clone())
        .unwrap_or_else(|| "not configured".into());
    let provider = if config.provider.is_some() {
        format!("{} · config.toml", provider)
    } else {
        provider
    };
    let model = if config.model.name.is_some() {
        format!("{} · config.toml", config.active_model())
    } else {
        format!("{} · default", config.active_model())
    };
    let (routing_profile, routing_policy, routing_fallbacks) = config
        .provider_routing()
        .map(|routing| {
            let profile = match routing.profile {
                tachyon_util::config::RoutingProfile::Cost => "cost",
                tachyon_util::config::RoutingProfile::Performance => "performance",
                tachyon_util::config::RoutingProfile::Manual => "manual",
            };
            let preferences = routing.active_preferences();
            let mut settings = Vec::new();
            if let Some(order) = &preferences.order {
                if !order.is_empty() {
                    settings.push(format!("order: {}", order.join(" > ")));
                }
            }
            if let Some(sort) = &preferences.sort {
                settings.push(format!("sort: {sort}"));
            }
            if let Some(latency) = preferences.preferred_max_latency {
                settings.push(format!("TTFT: <= {latency:.2}s"));
            }
            if let Some(throughput) = preferences.preferred_min_throughput {
                settings.push(format!("throughput: >= {throughput:.0} tok/s"));
            }
            let policy = if settings.is_empty() {
                "OpenRouter defaults".into()
            } else {
                settings.join(" · ")
            };
            let fallbacks = if routing.allow_fallbacks.unwrap_or(true) {
                "enabled"
            } else {
                "disabled"
            };
            (profile.to_string(), policy, fallbacks.to_string())
        })
        .unwrap_or_else(|| {
            (
                "default".into(),
                "OpenRouter defaults".into(),
                "enabled".into(),
            )
        });
    let cwd = std::env::current_dir()
        .ok()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "not available".into());
    let cwd = truncate_text(&cwd, popup.width.saturating_sub(18) as usize);
    let lines = vec![
        section("SESSION"),
        Line::from(""),
        value("state", session_label.to_string()),
        value("threads", threads.len().to_string()),
        value("cwd", cwd),
        value(
            "agents",
            format!(
                "{} running · {} done · {} failed",
                running, completed, failed
            ),
        ),
        help_divider(popup.width.saturating_sub(2) as usize),
        section("FOREGROUND"),
        Line::from(""),
        value("provider", provider),
        value("model", model),
        value("routing", routing_profile),
        value("policy", routing_policy),
        value("fallbacks", routing_fallbacks),
        value("context", "not reported".into()),
        help_divider(popup.width.saturating_sub(2) as usize),
        section("DAEMON"),
        Line::from(""),
        value("status", daemon_status),
        value("pid", daemon_pid),
        value("uptime", uptime),
    ];
    f.render_widget(Clear, popup);
    f.render_widget(Paragraph::new(lines).block(block), popup);
}

fn draw_conversation(
    f: &mut Frame,
    area: Rect,
    threads: &[Thread],
    focus: usize,
    scroll: usize,
    show_traces: bool,
    foreground_busy: bool,
    _foreground_activity: &str,
) {
    // ChatGPT-style: `● sender` labels; tool exec lines carry a live badge
    // (spinner while running, ✓ when done) and their output nests beneath as a
    // collapsible code block. Rows tracked in HITS for click-to-toggle.
    let mut lines: Vec<Line> = Vec::new();
    let mut hits: Vec<Hit> = Vec::new();
    let _ = focus;

    macro_rules! pushln {
        ($line:expr, $hit:expr) => {{
            lines.push($line);
            hits.push($hit);
        }};
    }

    let signature = conversation_signature(threads, area.width, show_traces, foreground_busy);
    let cached = CONVERSATION_CACHE.lock().ok().and_then(|cache| {
        cache.as_ref().and_then(|cache| {
            (cache.signature == signature).then(|| {
                let max = cache.lines.len().saturating_sub(area.height as usize);
                let top = max.saturating_sub(scroll);
                let visible = cache
                    .lines
                    .iter()
                    .skip(top)
                    .take(area.height as usize)
                    .cloned()
                    .collect();
                (visible, cache.hits.clone(), top)
            })
        })
    });
    if let Some((lines, hits, top)) = cached {
        render_conversation_view(f, area, lines, hits, top, scroll);
        return;
    }

    // A thread's tool item is "running" until its matching ToolResult arrives.
    let mut running_tool: Vec<Vec<bool>> = Vec::new();
    for t in threads.iter() {
        let mut map = vec![false; t.items.len()];
        for i in 0..t.items.len() {
            if t.items[i].kind == ItemKind::Tool {
                let has_result = t.items[i + 1..]
                    .iter()
                    .any(|x| x.kind == ItemKind::ToolResult);
                map[i] = !has_result;
            }
        }
        running_tool.push(map);
    }

    let spinner = spinner_glyph();

    let mut order: Vec<(u64, usize, usize)> = threads
        .iter()
        .enumerate()
        .flat_map(|(ti, thread)| {
            thread
                .items
                .iter()
                .enumerate()
                .map(move |(ii, item)| (item.timestamp, ti, ii))
        })
        .collect();
    order.sort_by_key(|(timestamp, ti, ii)| (*timestamp, *ti, *ii));
    let latest_conversation_timestamp = order.iter().rev().find_map(|(_, ti, ii)| {
        matches!(
            threads[*ti].items[*ii].kind,
            ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
        )
        .then_some(threads[*ti].items[*ii].timestamp)
    });

    for (_timestamp, ti, ii) in order {
        let t = &threads[ti];
        let item_kind = &t.items[ii].kind;
        let is_conversation = t.is_foreground
            && matches!(
                item_kind,
                ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
            );
        if !show_traces && !is_conversation {
            continue;
        }
        if !show_traces && !t.is_foreground && t.collapsed {
            if ii != 0 {
                continue;
            }
            let state = thread_state(t);
            let task = t.task.as_deref().unwrap_or("worker task");
            let prefix = format!("  ▸ ghost {} · {}", t.id, short_preview(task));
            let badge = format!("[{}]", state_label(state));
            let padding = (area.width as usize)
                .saturating_sub(prefix.chars().count() + badge.chars().count() + 1);
            pushln!(
                Line::from(vec![
                    Span::styled(prefix, Style::default().fg(agent_color(&t.id)),),
                    Span::raw(" ".repeat(padding)),
                    Span::styled(
                        badge,
                        Style::default()
                            .fg(match state {
                                ActivityState::Error => Color::Red,
                                ActivityState::Ready | ActivityState::Completed => Color::Green,
                                _ => Color::Yellow,
                            })
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Some((ti, ii))
            );
            continue;
        }
        let label = if t.is_foreground {
            names().conversation.clone()
        } else {
            format!("ghost {}", t.id)
        };
        let indent = if t.is_foreground { "" } else { "  " };
        let state = thread_state(t);
        let latest_reply = t
            .items
            .iter()
            .rposition(|candidate| candidate.kind == ItemKind::Reply);
        let active_style = if matches!(state, ActivityState::Ready | ActivityState::Completed) {
            Modifier::BOLD
        } else {
            Modifier::BOLD
        };
        let color = if t.is_foreground {
            Color::Green
        } else {
            agent_color(&t.id)
        };

        let item = &t.items[ii];
        let hit = Some((ti, ii));
        let body_color = if Some(item.timestamp) == latest_conversation_timestamp {
            Color::White
        } else {
            Color::Gray
        };
        let body_width = (area.width as usize)
            .saturating_sub(format!("{indent}    ").chars().count())
            .min(92);
        let active_turn = foreground_busy
            && t.is_foreground
            && match item.kind {
                ItemKind::User => {
                    t.items
                        .iter()
                        .rposition(|candidate| candidate.kind == ItemKind::User)
                        == Some(ii)
                }
                ItemKind::Reply => {
                    latest_reply == Some(ii)
                        && !t.items[ii + 1..].iter().any(|candidate| {
                            matches!(candidate.kind, ItemKind::User | ItemKind::PendingReply)
                        })
                }
                ItemKind::PendingReply => true,
                _ => false,
            };
        let body_indent = format!("{indent}    ");
        let body_indent_color = body_color;
        let running = if item.kind == ItemKind::Tool {
            item.output.is_none()
        } else {
            running_tool[ti].get(ii).copied().unwrap_or(false)
        };
        match item.kind {
            ItemKind::User => {
                let turn_suffix = item
                    .turn
                    .as_deref()
                    .map(|id| format!("  󰐖 {id}"))
                    .unwrap_or_default();
                let who = format!("{indent}● {}{turn_suffix}", names().user,);
                let mut user_header = vec![Span::styled(
                    format!("{indent} {} ", names().user),
                    Style::default()
                        .fg(Color::Black)
                        .bg(name_block_background(Color::Gray))
                        .add_modifier(Modifier::BOLD),
                )];
                let trailing = format!("{turn_suffix}  [{}]", timestamp_label(item.timestamp));
                let padding = (area.width as usize)
                    .saturating_sub(
                        Line::from(user_header.clone()).width() + Line::raw(&trailing).width(),
                    )
                    .max(1);
                user_header.push(Span::raw(" ".repeat(padding)));
                user_header.push(Span::styled(trailing, Style::default().fg(Color::Gray)));
                pushln!(Line::from(user_header), None);
                pushln!(Line::raw(""), None);
                for sub in item.text.split('\n') {
                    for chunk in wrap_text(sub.trim(), body_width) {
                        let mut row = vec![Span::styled(
                            body_indent.clone(),
                            Style::default().fg(body_indent_color),
                        )];
                        row.extend(styled_markdown(&chunk, Style::default().fg(body_color)));
                        pushln!(Line::from(row), hit);
                    }
                }
                if false {
                    for (li, sub) in item.text.split('\n').enumerate() {
                        for (ci, chunk) in wrap_text(
                            sub,
                            (area.width as usize).saturating_sub(who.chars().count() + 2),
                        )
                        .iter()
                        .enumerate()
                        {
                            let who_span = if li == 0 && ci == 0 {
                                who.clone()
                            } else {
                                " ".repeat(who.chars().count() + 2)
                            };
                            let mut row = vec![
                                Span::styled(
                                    who_span,
                                    Style::default()
                                        .fg(Color::Blue)
                                        .add_modifier(Modifier::BOLD),
                                ),
                                Span::styled("  ".to_string(), Style::default()),
                            ];
                            row.extend(styled_markdown(chunk, Style::default().fg(Color::White)));
                            pushln!(Line::from(row), hit);
                        }
                    }
                }
                pushln!(Line::raw(""), None);
            }
            ItemKind::PendingReply => {
                if !show_traces {
                    pushln!(
                        Line::from(Span::styled(
                            format!("{indent} {label} "),
                            Style::default()
                                .fg(Color::Black)
                                .bg(name_block_background(color))
                                .add_modifier(Modifier::BOLD),
                        )),
                        None
                    );
                    pushln!(Line::raw(""), None);
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}    󰔟 "),
                                Style::default().fg(Color::Yellow),
                            ),
                            Span::styled(
                                if item.turn.is_some() {
                                    "Checking information...".to_string()
                                } else {
                                    "Submitting...".to_string()
                                },
                                Style::default()
                                    .fg(Color::Gray)
                                    .add_modifier(Modifier::ITALIC),
                            ),
                        ]),
                        None
                    );
                    if let Some(progress) = worker_progress_badge(t, item.turn.as_deref()) {
                        pushln!(Line::raw(""), None);
                        pushln!(
                            Line::from(Span::styled(
                                format!("{indent}    {progress}"),
                                Style::default().fg(Color::Gray),
                            )),
                            None
                        );
                    }
                    pushln!(Line::raw(""), None);
                    continue;
                }
                let turn_suffix = item
                    .turn
                    .as_deref()
                    .map(|id| format!(" · turn {id}"))
                    .unwrap_or_default();
                let status = if item.turn.is_some() {
                    "working"
                } else {
                    "submitting"
                };
                pushln!(
                    Line::from(vec![
                        Span::styled(
                            format!(
                                "{indent}[{}] {} ",
                                timestamp_label(item.timestamp),
                                spinner_glyph()
                            ),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            format!(" {label} "),
                            Style::default()
                                .fg(Color::Black)
                                .bg(name_block_background(Color::DarkGray))
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(turn_suffix, Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("  {status}..."),
                            Style::default().fg(Color::DarkGray)
                        ),
                    ]),
                    None
                );
                pushln!(Line::raw(""), None);
            }
            ItemKind::Reply => {
                let visible_text = sanitize_reply_text(&item.text);
                let turn_suffix = item
                    .turn
                    .as_deref()
                    .map(|id| format!("  󰐖 {id}"))
                    .unwrap_or_default();
                let earlier_request = !show_traces && reply_completed_after_later_user(t, item);
                let who = format!(
                    "{indent}[{}] {} {label}{turn_suffix}",
                    timestamp_label(item.timestamp),
                    if t.is_foreground && latest_reply == Some(ii) {
                        animated_state_indicator(state)
                    } else {
                        state_indicator(state).to_string()
                    },
                );
                let mut header = vec![Span::raw(indent)];
                if active_turn {
                    header.push(Span::styled(
                        format!(" {label} "),
                        Style::default()
                            .fg(Color::Black)
                            .bg(name_block_background(color))
                            .add_modifier(Modifier::BOLD),
                    ));
                } else {
                    header.push(Span::styled(
                        format!(" {} ", label),
                        Style::default()
                            .fg(Color::Black)
                            .bg(name_block_background(color))
                            .add_modifier(active_style),
                    ));
                }
                let badges = turn_badges(t, item.turn.as_deref(), item.timestamp, show_traces);
                let metadata = if earlier_request {
                    let late = format!(
                        "󰐖 turn {} · earlier request",
                        item.turn.as_deref().unwrap_or("?")
                    );
                    if badges.is_empty() {
                        late
                    } else {
                        format!("{late} · {badges}")
                    }
                } else {
                    badges
                };
                let trailing = format!("{turn_suffix}  [{}]", timestamp_label(item.timestamp));
                let padding = (area.width as usize)
                    .saturating_sub(
                        Line::from(header.clone()).width() + Line::raw(&trailing).width(),
                    )
                    .max(1);
                header.push(Span::raw(" ".repeat(padding)));
                header.push(Span::styled(
                    trailing,
                    Style::default().fg(color).add_modifier(active_style),
                ));
                pushln!(Line::from(header), None);
                if !metadata.is_empty() {
                    pushln!(Line::raw(""), None);
                    let badge_width = (area.width as usize)
                        .saturating_sub(indent.chars().count() + 4)
                        .max(1);
                    for badge_line in wrap_text(&metadata, badge_width) {
                        pushln!(
                            Line::from(Span::styled(
                                format!("{indent}    {badge_line}"),
                                Style::default().fg(Color::Gray),
                            )),
                            None
                        );
                    }
                }
                pushln!(Line::raw(""), None);
                for sub in visible_text.split('\n') {
                    for sub_c in wrap_text(sub.trim(), body_width) {
                        let mut row = vec![Span::styled(
                            body_indent.clone(),
                            Style::default().fg(body_indent_color),
                        )];
                        row.extend(styled_markdown(&sub_c, Style::default().fg(body_color)));
                        pushln!(Line::from(row), hit);
                    }
                }
                if false {
                    for (li, sub) in item.text.split('\n').enumerate() {
                        let chunks = wrap_text(
                            sub.trim(),
                            (area.width as usize).saturating_sub(who.chars().count() + 2),
                        );
                        for (ci, sub_c) in chunks.iter().enumerate() {
                            let who_span = if li == 0 && ci == 0 {
                                who.clone()
                            } else {
                                " ".repeat(who.chars().count() + 2)
                            };
                            let mut row = if li == 0 && ci == 0 && t.is_foreground {
                                let prefix = format!(
                                    "{indent}[{}] {} ",
                                    timestamp_label(item.timestamp),
                                    state_indicator(state)
                                );
                                let mut spans = vec![Span::styled(
                                    prefix,
                                    Style::default().fg(color).add_modifier(active_style),
                                )];
                                if latest_reply == Some(ii)
                                    && matches!(
                                        state,
                                        ActivityState::Thinking
                                            | ActivityState::Working
                                            | ActivityState::Waiting
                                            | ActivityState::Composing
                                    )
                                {
                                    spans.extend(wave_label_spans(&label, color));
                                } else {
                                    spans.push(Span::styled(
                                        label.clone(),
                                        Style::default().fg(color).add_modifier(active_style),
                                    ));
                                }
                                spans.push(Span::styled(
                                    turn_suffix.clone(),
                                    Style::default().fg(color).add_modifier(active_style),
                                ));
                                spans.push(Span::styled("  ", Style::default()));
                                spans
                            } else {
                                vec![
                                    Span::styled(
                                        who_span,
                                        Style::default().fg(color).add_modifier(active_style),
                                    ),
                                    Span::styled("  ".to_string(), Style::default()),
                                ]
                            };
                            row.extend(styled_markdown(sub_c, Style::default().fg(Color::White)));
                            pushln!(Line::from(row), hit);
                        }
                    }
                }
                pushln!(Line::raw(""), None);
            }
            ItemKind::Tool => {
                let badge = if running {
                    format!(" {spinner} running")
                } else {
                    " ✓".to_string()
                };
                let badge_style = if running {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Green)
                };
                let (tool_name, arguments) = tool_parts(&item.text);
                // Completed calls stay compact even in trace mode; click their
                // header to inspect full arguments and output.
                let tool_hidden = item.hidden;
                let collapsed_summary = short_preview(&arguments);
                pushln!(
                    Line::from(vec![
                        Span::styled(format!("{indent}  󰆍 "), Style::default().fg(Color::Blue)),
                        Span::styled(
                            format!("[{}] ", timestamp_label(item.timestamp)),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(format!("{tool_name}"), Style::default().fg(color)),
                        Span::styled(badge, badge_style),
                        if tool_hidden {
                            Span::styled(
                                format!(" · {collapsed_summary} · click to expand"),
                                Style::default()
                                    .fg(Color::DarkGray)
                                    .add_modifier(Modifier::ITALIC),
                            )
                        } else {
                            Span::raw("")
                        },
                    ]),
                    hit
                );
                if !tool_hidden {
                    if let Some(target) = api_target(&arguments) {
                        pushln!(
                            Line::from(Span::styled(
                                format!("{indent}      request · {target}"),
                                Style::default().fg(Color::Cyan),
                            )),
                            hit
                        );
                    }
                    for argument_line in
                        wrap_text(&arguments, area.width.saturating_sub(8) as usize)
                    {
                        pushln!(
                            Line::from(vec![
                                Span::styled(
                                    format!("{indent}      "),
                                    Style::default().fg(Color::DarkGray)
                                ),
                                Span::styled(argument_line, Style::default().fg(Color::DarkGray)),
                            ]),
                            hit
                        );
                    }
                }
                if let Some(output) = &item.output {
                    if !tool_hidden {
                        pushln!(
                            Line::from(Span::styled(
                                format!("{indent}      output"),
                                Style::default()
                                    .fg(Color::Rgb(255, 140, 0))
                                    .add_modifier(Modifier::BOLD),
                            )),
                            hit
                        );
                        for sub in output.lines() {
                            for chunk in wrap_text(sub, area.width.saturating_sub(10) as usize) {
                                pushln!(
                                    Line::from(Span::styled(
                                        format!("{indent}        {chunk}"),
                                        Style::default()
                                            .fg(Color::Rgb(220, 223, 228))
                                            .bg(Color::Rgb(30, 32, 36)),
                                    )),
                                    hit
                                );
                            }
                        }
                    }
                }
            }
            ItemKind::ToolResult => {
                if item.hidden {
                    // Collapsed code block — clickable to expand.
                    let summary = short_preview(&item.text);
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}    ▸ "),
                                Style::default().fg(Color::DarkGray)
                            ),
                            Span::styled(
                                format!("output · {summary} — click to expand"),
                                Style::default()
                                    .fg(Color::DarkGray)
                                    .add_modifier(Modifier::ITALIC)
                            ),
                        ]),
                        hit
                    );
                } else {
                    // Markdown-style code block.
                    pushln!(
                        Line::from(vec![Span::styled(
                            format!("{indent}    output"),
                            Style::default()
                                .fg(Color::Rgb(255, 140, 0))
                                .add_modifier(Modifier::BOLD)
                        ),]),
                        hit
                    );
                    for sub in item.text.lines() {
                        for chunk in wrap_text(sub, area.width as usize - 8) {
                            pushln!(
                                Line::from(Span::styled(
                                    format!("{indent}    {chunk}"),
                                    Style::default()
                                        .fg(Color::Rgb(220, 223, 228))
                                        .bg(Color::Rgb(30, 32, 36)),
                                )),
                                hit
                            );
                        }
                    }
                    pushln!(
                        Line::from(Span::styled(
                            format!("{indent}    └─ end output"),
                            Style::default().fg(Color::DarkGray),
                        )),
                        hit
                    );
                }
            }
            ItemKind::Spawn => {
                if show_traces {
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}  󰚩 "),
                                Style::default().fg(color).add_modifier(Modifier::BOLD)
                            ),
                            Span::styled(
                                format!(
                                    "[{}] {}",
                                    timestamp_label(item.timestamp),
                                    spawn_display_label(&label, &item.text)
                                ),
                                Style::default().fg(color),
                            ),
                        ]),
                        hit
                    );
                } else {
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}    󰚩 agent spawned · "),
                                Style::default().fg(Color::Gray),
                            ),
                            Span::styled(
                                short_preview(worker_objective(&item.text)),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]),
                        hit
                    );
                }
            }
            ItemKind::SpawnResult => {
                if show_traces {
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}  󰄬 "),
                                Style::default().fg(color).add_modifier(Modifier::BOLD)
                            ),
                            Span::styled(
                                format!(
                                    "[{}] task completed · {}",
                                    timestamp_label(item.timestamp),
                                    short_preview(worker_objective(&item.text))
                                ),
                                Style::default().fg(color)
                            ),
                        ]),
                        hit
                    );
                } else {
                    pushln!(
                        Line::from(vec![
                            Span::styled(
                                format!("{indent}    󰄬 task completed · "),
                                Style::default().fg(Color::Gray),
                            ),
                            Span::styled(
                                short_preview(worker_objective(&item.text)),
                                Style::default().fg(Color::DarkGray),
                            ),
                        ]),
                        hit
                    );
                }
            }
            ItemKind::Error => {
                for sub in item.text.split('\n') {
                    let mut row = vec![
                        Span::styled(format!("{indent}  󰅙 "), Style::default().fg(Color::Red)),
                        Span::styled(
                            format!("[{}] ", timestamp_label(item.timestamp)),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ];
                    row.extend(styled_markdown(sub, Style::default().fg(Color::Red)));
                    pushln!(Line::from(row), hit);
                }
            }
            ItemKind::System => {
                let mut runtime_rendered = false;
                for sub in item.text.split('\n') {
                    let summary = if is_worker_runtime_detail(sub) {
                        if runtime_rendered {
                            continue;
                        }
                        runtime_rendered = true;
                        "󰐊 worker runtime ready".to_string()
                    } else {
                        trace_summary(sub)
                    };
                    let mut row = vec![
                        Span::styled(
                            format!("{indent}  · "),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled(
                            format!("[{}] ", timestamp_label(item.timestamp)),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ];
                    row.extend(styled_markdown(
                        &summary,
                        Style::default().fg(Color::DarkGray),
                    ));
                    pushln!(Line::from(row), hit);
                }
                if show_traces {
                    pushln!(Line::raw(""), None);
                }
            }
        }
    }

    let signature = conversation_signature(threads, area.width, show_traces, foreground_busy);
    if let Ok(mut cache) = CONVERSATION_CACHE.lock() {
        *cache = Some(ConversationCache {
            signature,
            lines: lines.clone(),
            hits: hits.clone(),
        });
    }
    render_conversation_lines(f, area, lines, hits, scroll);
}

fn render_conversation_lines(
    f: &mut Frame,
    area: Rect,
    lines: Vec<Line<'static>>,
    hits: Vec<Hit>,
    scroll: usize,
) {
    let max = lines.len().saturating_sub(area.height as usize);
    let top = max.saturating_sub(scroll);

    // Store the hitmap + view geometry for the click handler.
    if let Ok(mut guard) = HITS.lock() {
        *guard = hits.clone();
    }
    if let Ok(mut guard) = VIEW.lock() {
        *guard = (area.y, area.height, top);
    }

    f.render_widget(Paragraph::new(lines).scroll((top as u16, 0)), area);
    draw_scroll_indicator(f, area, scroll);
}

fn render_conversation_view(
    f: &mut Frame,
    area: Rect,
    lines: Vec<Line<'static>>,
    hits: Vec<Hit>,
    top: usize,
    scroll: usize,
) {
    if let Ok(mut guard) = HITS.lock() {
        *guard = hits;
    }
    if let Ok(mut guard) = VIEW.lock() {
        *guard = (area.y, area.height, top);
    }
    f.render_widget(Paragraph::new(lines), area);
    draw_scroll_indicator(f, area, scroll);
}

fn draw_scroll_indicator(f: &mut Frame, area: Rect, scroll: usize) {
    if scroll > 0 && area.width > 0 {
        let indicator = format!("↑ {scroll} lines");
        let width = indicator.chars().count().min(area.width as usize) as u16;
        let indicator_area = Rect {
            x: area.x + area.width.saturating_sub(width),
            y: area.y,
            width,
            height: 1,
        };
        f.render_widget(
            Paragraph::new(Span::styled(
                indicator,
                Style::default().fg(Color::Yellow).bg(Color::Black),
            )),
            indicator_area,
        );
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ActivityState {
    Ready,
    Completed,
    Thinking,
    Working,
    Waiting,
    Composing,
    Error,
}

fn thread_state(thread: &Thread) -> ActivityState {
    if thread
        .items
        .last()
        .map(|item| item.kind == ItemKind::Error)
        .unwrap_or(false)
    {
        return ActivityState::Error;
    }
    if thread.streaming {
        return ActivityState::Composing;
    }
    if thread
        .items
        .iter()
        .any(|item| item.kind == ItemKind::Tool && item.output.is_none())
    {
        return ActivityState::Working;
    }
    if let Some(last) = thread.items.last() {
        match last.kind {
            ItemKind::Reply | ItemKind::Tool | ItemKind::ToolResult | ItemKind::SpawnResult => {
                return ActivityState::Ready;
            }
            ItemKind::System if last.text.contains("queued") || last.text.contains("waiting") => {
                return ActivityState::Waiting;
            }
            ItemKind::System
                if last.text.contains("done")
                    || last.text.contains("exited")
                    || last.text.contains("complete") =>
            {
                return ActivityState::Completed;
            }
            ItemKind::System | ItemKind::User => return ActivityState::Thinking,
            ItemKind::PendingReply => return ActivityState::Waiting,
            ItemKind::Spawn => return ActivityState::Working,
            ItemKind::Error => return ActivityState::Error,
        }
    }
    ActivityState::Ready
}

fn agent_activity_state(state: AgentState) -> ActivityState {
    match state {
        AgentState::Created | AgentState::Starting | AgentState::Running => ActivityState::Working,
        AgentState::Waiting | AgentState::Staged => ActivityState::Waiting,
        AgentState::Completed | AgentState::Released => ActivityState::Completed,
        AgentState::Failed => ActivityState::Error,
        AgentState::Interrupted | AgentState::Terminated => ActivityState::Completed,
    }
}

fn state_label(state: ActivityState) -> &'static str {
    match state {
        ActivityState::Ready => "ready",
        ActivityState::Completed => "completed",
        ActivityState::Thinking => "thinking",
        ActivityState::Working => "working",
        ActivityState::Waiting => "waiting",
        ActivityState::Composing => "composing",
        ActivityState::Error => "error",
    }
}

fn state_indicator(state: ActivityState) -> &'static str {
    match state {
        ActivityState::Ready | ActivityState::Completed => "●",
        ActivityState::Error => "×",
        ActivityState::Thinking | ActivityState::Waiting => "◌",
        ActivityState::Working | ActivityState::Composing => "◉",
    }
}

fn animated_state_indicator(state: ActivityState) -> String {
    let active = matches!(
        state,
        ActivityState::Thinking
            | ActivityState::Working
            | ActivityState::Waiting
            | ActivityState::Composing
    );
    if !active {
        return state_indicator(state).into();
    }
    let phase = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() / 450)
        .unwrap_or(0);
    if phase % 2 == 0 {
        "●".into()
    } else {
        "○".into()
    }
}

fn wave_label_spans(label: &str, block_color: Color) -> Vec<Span<'static>> {
    label
        .chars()
        .map(|ch| {
            Span::styled(
                ch.to_string(),
                Style::default()
                    .fg(Color::Black)
                    .bg(name_block_background(block_color)),
            )
        })
        .collect()
}

/// Time-based spinner frame (animates while an agent is active).
fn spinner_glyph() -> &'static str {
    const F: [&str; 8] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"];
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    F[(ms / 80) as usize % F.len()]
}

fn short_preview(text: &str) -> String {
    let first = text.lines().next().unwrap_or("").trim();
    if first.chars().count() > 40 {
        format!("{}…", first.chars().take(40).collect::<String>())
    } else if first.is_empty() {
        "…".to_string()
    } else {
        first.to_string()
    }
}

fn compact_current_dir() -> String {
    let Ok(path) = std::env::current_dir() else {
        return "?".into();
    };
    let full = path.display().to_string();
    if full.chars().count() <= 28 {
        return full;
    }
    let parts: Vec<String> = path
        .components()
        .filter_map(|component| {
            let text = component.as_os_str().to_string_lossy();
            (!text.is_empty()).then_some(text.into_owned())
        })
        .collect();
    if parts.len() >= 2 {
        format!("…/{}/{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        truncate_text(&full, 28)
    }
}

fn truncate_text(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width <= 3 {
        return text.chars().take(width).collect();
    }
    format!("{}...", text.chars().take(width - 3).collect::<String>())
}

fn lifecycle_badge(state: AgentState) -> String {
    match state {
        AgentState::Created | AgentState::Starting => "◌ starting".into(),
        AgentState::Running => "● running".into(),
        AgentState::Waiting => "◌ waiting".into(),
        AgentState::Staged => "◌ staged".into(),
        AgentState::Completed => "✓ complete".into(),
        AgentState::Failed => "× failed".into(),
        AgentState::Interrupted => "× stopped".into(),
        AgentState::Terminated => "× killed".into(),
        AgentState::Released => "× released".into(),
    }
}

fn turn_badges(thread: &Thread, turn: Option<&str>, timestamp: u64, show_traces: bool) -> String {
    let previous_reply = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Reply && item.timestamp < timestamp)
        .map(|item| item.timestamp)
        .max()
        .unwrap_or(0);
    let in_turn = |item: &&Item| {
        item.turn.as_deref() == turn
            || (item.turn.is_none()
                && item.timestamp > previous_reply
                && item.timestamp <= timestamp)
    };
    let spawned = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Spawn && in_turn(item))
        .count();
    let completed = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::SpawnResult && in_turn(item))
        .count();
    let started_at = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::User && item.turn.as_deref() == turn)
        .map(|item| item.timestamp)
        .min()
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| {
                    item.kind == ItemKind::System
                        && item.text.starts_with("[working]")
                        && in_turn(item)
                })
                .map(|item| item.timestamp)
                .min()
        })
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::Spawn && in_turn(item))
                .map(|item| item.timestamp)
                .min()
        })
        .or_else(|| {
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::User && item.timestamp < timestamp)
                .map(|item| item.timestamp)
                .max()
        });
    let mut badges = Vec::new();
    if spawned > 0 {
        badges.push(if show_traces {
            format!("󰚩 workers started {spawned}")
        } else {
            format!("󰚩 {spawned} agents")
        });
    }
    let metrics = turn.and_then(|turn| thread.metrics.get(turn));
    let first = metrics.and_then(|metrics| metrics.first_visible_ms);
    let done = metrics.and_then(|metrics| metrics.completed_ms);
    if completed > 0 {
        badges.push(if show_traces {
            format!("󰄬 workers completed {completed}")
        } else if spawned == 0 {
            format!("󰄬 {completed} agents complete")
        } else {
            format!("󰄬 {completed} complete")
        });
    }
    if let Some(first) = first {
        if show_traces {
            badges.push(format!("󱎫 first response {:.1}s", first as f64 / 1000.0));
        }
    } else if let Some(started_at) = started_at {
        let elapsed = timestamp.saturating_sub(started_at) as f64 / 1000.0;
        if show_traces {
            badges.push(format!("󱎫 first response {:.1}s", elapsed));
        }
    }
    if let Some(done) = done {
        badges.push(if show_traces {
            format!("󰅐 response completed {:.1}s", done as f64 / 1000.0)
        } else {
            format!("󰅐 done {:.1}s", done as f64 / 1000.0)
        });
    }
    if let Some(metrics) = metrics {
        if let Some(self_usage) = &metrics.self_usage {
            let total = metrics
                .worker_usage
                .values()
                .fold(self_usage.total, |sum, usage| {
                    sum.saturating_add(usage.total)
                });
            if show_traces {
                badges.push(format!(
                    "󰍛 foreground tokens {} (prompt {} + completion {})",
                    format_count(self_usage.total),
                    format_count(self_usage.prompt),
                    format_count(self_usage.completion)
                ));
            }
            badges.push(if show_traces {
                format!("󰏪 aggregate tokens {}", format_count(total))
            } else {
                format!("󰏪 total {}", format_count(total))
            });
        }
    } else if let Some(turn) = turn.and_then(|value| value.parse::<u64>().ok()) {
        if let Some((prompt, completion, total)) = thread.usage.get(&turn) {
            if show_traces {
                badges.push(format!(
                    "󰍛 reported tokens {} (prompt {} + completion {})",
                    format_count(*total),
                    format_count(*prompt),
                    format_count(*completion)
                ));
            } else {
                badges.push(format!("󰏪 total {}", format_count(*total)));
            }
        }
    }
    badges.join(" · ")
}

fn format_count(value: impl Into<u64>) -> String {
    let value = value.into();
    if value >= 1_000_000 {
        format!("{:.1}m", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn tool_parts(text: &str) -> (String, String) {
    let text = text.trim();
    let (name, raw_args) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    let args = match serde_json::from_str::<serde_json::Value>(raw_args.trim()) {
        Ok(value) => {
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| raw_args.trim().into())
        }
        Err(_) => raw_args.trim().to_string(),
    };
    (name.to_string(), args)
}

fn api_target(arguments: &str) -> Option<String> {
    let start = arguments
        .find("https://")
        .or_else(|| arguments.find("http://"))?;
    let rest = &arguments[start..];
    let end = rest
        .char_indices()
        .find(|(_, ch)| matches!(ch, '"' | '\'' | '`' | ' ' | '\n' | '&' | ')'))
        .map(|(index, _)| index)
        .unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Wrap text at word boundaries, hard-splitting only words wider than the view.
fn wrap_text(s: &str, width: usize) -> Vec<String> {
    let width = width.max(10);
    if s.is_empty() {
        return vec![String::new()];
    }
    let mut out = Vec::new();
    let mut line = String::new();
    for word in s.split_whitespace() {
        if word.chars().count() > width {
            if !line.is_empty() {
                out.push(std::mem::take(&mut line));
            }
            let chars: Vec<char> = word.chars().collect();
            for chunk in chars.chunks(width) {
                out.push(chunk.iter().collect());
            }
        } else if line.is_empty() {
            line.push_str(word);
        } else if line.chars().count() + 1 + word.chars().count() <= width {
            line.push(' ');
            line.push_str(word);
        } else {
            out.push(std::mem::take(&mut line));
            line.push_str(word);
        }
    }
    if !line.is_empty() {
        out.push(line);
    }
    out
}

/// Render the small Markdown subset commonly returned by the assistant.
fn styled_markdown(text: &str, base: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut rest = text;
    let mut emphasis = false;
    let mut code = false;
    while !rest.is_empty() {
        let marker = if rest.starts_with("**") {
            Some((2, true))
        } else if rest.starts_with('`') {
            Some((1, false))
        } else {
            None
        };
        if let Some((len, is_emphasis)) = marker {
            if is_emphasis {
                emphasis = !emphasis;
            } else {
                code = !code;
            }
            rest = &rest[len..];
            continue;
        }
        let next = rest
            .find("**")
            .into_iter()
            .chain(rest.find('`'))
            .min()
            .unwrap_or(rest.len());
        let chunk = &rest[..next];
        let mut style = base;
        if emphasis {
            style = style.add_modifier(Modifier::BOLD);
        }
        if code {
            style = style
                .fg(Color::Rgb(255, 190, 100))
                .bg(Color::Rgb(35, 35, 35));
        }
        spans.push(Span::styled(chunk.to_string(), style));
        rest = &rest[next..];
    }
    spans
}

fn draw_input(
    f: &mut Frame,
    area: Rect,
    input: &str,
    cursor: usize,
    daemon: Option<&DaemonInfo>,
    foreground_busy: bool,
) {
    // Render the multi-line input with a visible cursor block.
    let cursor_visible = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() % 1000 < 500)
        .unwrap_or(true);
    let mut lines: Vec<Line> = Vec::new();
    let input_lines: Vec<&str> = if input.is_empty() {
        vec![""]
    } else {
        input.split('\n').collect()
    };

    let mut remaining = cursor;
    for (li, text) in input_lines.iter().enumerate() {
        let mut spans: Vec<Span> = Vec::new();
        // Prompt marker on the first line only.
        let prefix = if li == 0 { "❯ " } else { "  " };
        spans.push(Span::styled(prefix, Style::default().fg(Color::Cyan)));

        if input.is_empty() && li == 0 {
            spans.push(Span::styled(
                " ",
                if cursor_visible {
                    Style::default().bg(Color::White)
                } else {
                    Style::default()
                },
            ));
            spans.push(Span::styled(
                "Ask Tachyon anything...",
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::ITALIC),
            ));
            lines.push(Line::from(spans));
            continue;
        }

        let n = chars(text);
        let cur_on_line = if remaining <= n {
            Some(remaining)
        } else {
            None
        };
        if let Some(pos) = cur_on_line {
            // Before cursor, cursor char, after cursor.
            let before: String = text.chars().take(pos).collect();
            spans.push(Span::styled(before, Style::default().fg(Color::White)));
            if pos < n {
                let c = text.chars().nth(pos).unwrap();
                spans.push(if cursor_visible {
                    Span::styled(
                        c.to_string(),
                        Style::default().fg(Color::Black).bg(Color::White),
                    )
                } else {
                    Span::styled(c.to_string(), Style::default().fg(Color::White))
                });
                let after: String = text.chars().skip(pos + 1).collect();
                spans.push(Span::styled(after, Style::default().fg(Color::White)));
            } else {
                // Cursor at end of line.
                if cursor_visible {
                    spans.push(Span::styled(" ", Style::default().bg(Color::White)));
                }
            }
        } else {
            spans.push(Span::styled(
                text.to_string(),
                Style::default().fg(Color::White),
            ));
        }
        lines.push(Line::from(spans));
        // Move past this line's chars + the newline char itself.
        if remaining >= n + 1 {
            remaining -= n + 1;
        } else {
            remaining = 0;
        }
    }

    // Daemon hint on the bottom-right is in the statusline; keep input clean.
    let _ = daemon;
    let _ = foreground_busy;
    f.render_widget(Paragraph::new(lines), area);
}

/// Vim-style statusline below the input: a bold brand segment, a clear
/// daemon-state segment, and key bindings on the right.
fn draw_statusline(
    f: &mut Frame,
    area: Rect,
    daemon: Option<&DaemonInfo>,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &[Thread],
    config: &tachyon_util::config::Config,
    show_traces: bool,
    session_label: &str,
) {
    let (dtext, dbg) = match daemon {
        Some(info) if info.provider_ready => (" ● ".to_string(), Color::Green),
        Some(_) => (" ● ".to_string(), Color::Yellow),
        None => (" × ".to_string(), Color::Red),
    };
    let brand = Span::styled(
        " TACHYON ",
        Style::default()
            .fg(Color::Black)
            .bg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    );
    let state = Span::styled(dtext, Style::default().fg(dbg).add_modifier(Modifier::BOLD));
    let running = agent_infos
        .values()
        .filter(|agent| matches!(agent.state, AgentState::Starting | AgentState::Running))
        .count();
    let waiting = agent_infos
        .values()
        .filter(|agent| agent.state == AgentState::Waiting)
        .count();
    let failed = agent_infos
        .values()
        .filter(|agent| agent.state == AgentState::Failed)
        .count();
    let completed = agent_infos
        .values()
        .filter(|agent| {
            matches!(
                agent.state,
                AgentState::Completed | AgentState::Terminated | AgentState::Released
            )
        })
        .count();
    let mut activity_parts = Vec::new();
    if running > 0 {
        activity_parts.push(format!("{} active", running));
    }
    if waiting > 0 {
        activity_parts.push(format!("{} queued", waiting));
    }
    if failed > 0 {
        activity_parts.push(format!("{} failed", failed));
    }
    if completed > 0 {
        activity_parts.push(format!("{} done", completed));
    }
    let activity =
        (!activity_parts.is_empty()).then(|| format!(" {} ", activity_parts.join(" · ")));
    let controls = if show_traces {
        " 󰀄 AGENTS ·  HELP · 󰈈 COMPACT · 󰋼 INFO "
    } else {
        " 󰀄 AGENTS ·  HELP · 󰈈 TRACES · 󰋼 INFO "
    };
    let session = (session_label == "fresh")
        .then(|| Span::styled(" FRESH ", Style::default().fg(Color::DarkGray)));
    let cwd = Span::styled(
        format!(" 󰉋 {} ", compact_current_dir()),
        Style::default().fg(Color::DarkGray),
    );
    let context = threads
        .iter()
        .find(|thread| thread.is_foreground)
        .and_then(|thread| thread.usage.iter().max_by_key(|(turn, _)| *turn))
        .and_then(|(_, (prompt, _, _))| {
            config.model.context_length.map(|limit| {
                let percent = (*prompt as f64 / limit as f64 * 100.0).min(100.0);
                format!(" ctx:{percent:.0}% ")
            })
        });
    let context_span = context.map(|text| Span::styled(text, Style::default().fg(Color::DarkGray)));
    let activity_span =
        activity.map(|text| Span::styled(text, Style::default().fg(Color::DarkGray)));
    let left_width = brand.content.chars().count() + 2 + controls.chars().count();
    let mut right_spans = Vec::new();
    let mut right_width = 0usize;
    let mut add_right = |span: Span<'static>| {
        if !right_spans.is_empty() {
            let divider = Span::styled(" · ", Style::default().fg(Color::DarkGray));
            right_spans.push(divider.clone());
            right_width += divider.content.chars().count();
        }
        right_width += span.content.chars().count();
        right_spans.push(span);
    };
    if let Some(session) = session {
        add_right(session);
    }
    add_right(cwd);
    if let Some(context_span) = context_span {
        add_right(context_span);
    }
    if let Some(activity_span) = activity_span {
        add_right(activity_span);
    }
    add_right(state);
    let gap = area.width.saturating_sub((left_width + right_width) as u16);
    let mut line = vec![
        brand,
        Span::raw("  "),
        Span::styled(controls, Style::default().fg(Color::DarkGray)),
        Span::raw(" ".repeat(gap as usize)),
    ];
    line.extend(right_spans);
    f.render_widget(Paragraph::new(Line::from(line)), area);
}

fn draw_agent_pane(
    f: &mut Frame,
    area: Rect,
    _threads: &[Thread],
    focus: usize,
    daemon: Option<&DaemonInfo>,
    daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
    tab: PaneTab,
) {
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .style(Style::default().bg(Color::Rgb(18, 18, 22)));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(2)])
        .split(inner);
    let tab_style = |selected| {
        Style::default()
            .fg(if selected { Color::Black } else { Color::Gray })
            .bg(if selected {
                Color::Cyan
            } else {
                Color::Rgb(35, 35, 42)
            })
            .add_modifier(Modifier::BOLD)
    };
    let worker_count = agent_infos
        .keys()
        .filter(|id| id.as_str() != FOREGROUND_ID)
        .count();
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" FOREGROUND ", tab_style(tab == PaneTab::Foreground)),
            Span::raw(" "),
            Span::styled(
                format!(" AGENTS ({worker_count}) "),
                tab_style(tab == PaneTab::Agents),
            ),
        ])),
        Rect {
            x: area.x.saturating_add(2),
            y: area.y,
            width: area.width.saturating_sub(4),
            height: 1,
        },
    );

    let header = Row::new(["NAME", "STATUS", "TYPE", "AGE", "TASK"]).style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    );
    let widths = [
        Constraint::Length(18),
        Constraint::Length(16),
        Constraint::Length(16),
        Constraint::Length(9),
        Constraint::Min(16),
    ];
    let rows = match tab {
        PaneTab::Foreground => {
            let daemon_status = match daemon {
                Some(info) if info.provider_ready => "online",
                Some(_) => "online · no key",
                None => "offline",
            };
            let daemon_age = daemon_since
                .map(|since| format_duration(since.elapsed()))
                .unwrap_or_else(|| "-".into());
            let mut rows = vec![Row::new(vec![
                Cell::from("tachyond"),
                Cell::from(daemon_status),
                Cell::from("daemon"),
                Cell::from(daemon_age),
                Cell::from("daemon service"),
            ])
            .style(if focus == 0 {
                Style::default().bg(Color::Rgb(42, 42, 52))
            } else {
                Style::default()
            })];
            if let Some(info) = agent_infos.get(FOREGROUND_ID) {
                rows.push(
                    Row::new(vec![
                        Cell::from(names().conversation.clone()),
                        Cell::from(state_label(agent_activity_state(info.state))),
                        Cell::from("conversational"),
                        Cell::from(format_age(info.created_secs)),
                        Cell::from(truncate_text(&info.description, 48)),
                    ])
                    .style(if focus == 1 {
                        Style::default().bg(Color::Rgb(42, 42, 52))
                    } else {
                        Style::default()
                    }),
                );
            }
            let active_workers = agent_infos.values().any(|info| {
                info.id != FOREGROUND_ID
                    && matches!(
                        info.state,
                        AgentState::Starting | AgentState::Running | AgentState::Waiting
                    )
            });
            rows.push(Row::new(vec![
                Cell::from("Background agent"),
                Cell::from(if active_workers { "running" } else { "idle" }),
                Cell::from("coordinator"),
                Cell::from("-"),
                Cell::from(format!("{worker_count} managed workers")),
            ]));
            rows
        }
        PaneTab::Agents => {
            let ids = pane_agent_ids(agent_infos);
            if ids.is_empty() {
                vec![Row::new(vec![
                    Cell::from("No managed agents"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("Start a task to create a worker agent"),
                ])
                .style(Style::default().fg(Color::DarkGray))]
            } else {
                ids.iter()
                    .enumerate()
                    .map(|(index, id)| {
                        let info = &agent_infos[id];
                        let selected = focus == index + 2;
                        Row::new(vec![
                            Cell::from(truncate_text(&format!("ghost {id}"), 18)),
                            Cell::from(state_label(agent_activity_state(info.state))),
                            Cell::from(info.task_type.clone()),
                            Cell::from(format_age(info.created_secs)),
                            Cell::from(truncate_text(&info.description, 48)),
                        ])
                        .style(if selected {
                            Style::default().bg(Color::Rgb(42, 42, 52))
                        } else {
                            Style::default()
                        })
                    })
                    .collect()
            }
        }
    };
    f.render_widget(
        Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .style(Style::default().fg(Color::Gray).bg(Color::Rgb(18, 18, 22))),
        sections[0],
    );
    f.render_widget(
        Paragraph::new(
            "left/right tabs · click a row to focus · s stop · r restart · u resume · k kill",
        )
        .alignment(Alignment::Center)
        .style(
            Style::default()
                .fg(Color::DarkGray)
                .bg(Color::Rgb(18, 18, 22)),
        ),
        sections[1],
    );
}

#[allow(dead_code)]
/// Floating agent pane retained for reference while the table view replaces it.
fn draw_agent_pane_legacy(
    f: &mut Frame,
    area: Rect,
    _threads: &[Thread],
    focus: usize,
    daemon: Option<&DaemonInfo>,
    daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
) {
    f.render_widget(Clear, area);
    let block = Block::default()
        .title(" AGENTS ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .title_alignment(Alignment::Center)
        .style(Style::default().bg(Color::Rgb(18, 18, 22)));
    let daemon_state = match daemon {
        Some(info) if info.provider_ready => ("●", "online", Color::Green),
        Some(_) => ("●", "online · no API key", Color::Yellow),
        None => ("○", "offline", Color::Red),
    };
    let daemon_selected = focus == 0;
    let daemon_row_style = if daemon_selected {
        Style::default().bg(Color::Rgb(42, 42, 52))
    } else {
        Style::default()
    };
    let daemon_label_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(if daemon_selected {
            Modifier::BOLD
        } else {
            Modifier::empty()
        });
    let daemon_age = daemon_since
        .map(|since| format_duration(since.elapsed()))
        .unwrap_or_else(|| "-".into());
    let daemon_pid = daemon
        .map(|info| info.pid.to_string())
        .unwrap_or_else(|| "-".into());
    let mut items = vec![
        ListItem::new(agent_table_header("TARGET")),
        ListItem::new(Line::from(vec![
            Span::styled(
                if daemon_selected { "┃ " } else { "  " },
                daemon_label_style,
            ),
            Span::styled(format!("{:18}", "tachyond"), daemon_label_style),
            column_rule(),
            Span::styled(
                format!("{:^13}", format!("{} {}", daemon_state.0, daemon_state.1)),
                Style::default().fg(Color::Black).bg(daemon_state.2),
            ),
            column_rule(),
            Span::styled(
                format!("{:<21}", "daemon"),
                Style::default().fg(Color::DarkGray),
            ),
            column_rule(),
            Span::styled(format!("{:^9}", daemon_age), daemon_label_style),
            column_rule(),
            Span::styled(format!("{:^9}", daemon_pid), daemon_label_style),
            column_rule(),
            Span::styled(format!("{:^8}", ""), daemon_label_style),
            column_rule(),
            Span::styled(
                "daemon service",
                Style::default().fg(if daemon_selected {
                    Color::Gray
                } else {
                    Color::DarkGray
                }),
            ),
        ]))
        .style(daemon_row_style),
    ];
    if let Some(info) = agent_infos.get(FOREGROUND_ID) {
        let state = agent_activity_state(info.state);
        let state_color = match state {
            ActivityState::Ready | ActivityState::Completed => Color::Green,
            ActivityState::Error => Color::Red,
            _ => Color::Yellow,
        };
        let conversation_label = truncate_text(&names().conversation, 18);
        let foreground_task = truncate_text(&info.description, 36);
        items.push(
            ListItem::new(Line::from(vec![
                Span::styled(if focus == 1 { "┃ " } else { "  " }, daemon_label_style),
                Span::styled(format!("{conversation_label:<18}"), daemon_label_style),
                column_rule(),
                status_badge(
                    &format!("{} {}", animated_state_indicator(state), info.state),
                    state_color,
                ),
                column_rule(),
                Span::styled(
                    format!("{:<21}", "conversation"),
                    Style::default().fg(Color::DarkGray),
                ),
                column_rule(),
                Span::styled(
                    format!("{:^9}", format_age(info.created_secs)),
                    daemon_label_style,
                ),
                column_rule(),
                Span::styled(
                    format!(
                        "{:^9}",
                        info.pid
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "-".into())
                    ),
                    daemon_label_style,
                ),
                column_rule(),
                Span::styled(format!("{:^8}", ""), daemon_label_style),
                column_rule(),
                Span::styled(
                    foreground_task,
                    Style::default().fg(if focus == 1 {
                        Color::Gray
                    } else {
                        Color::DarkGray
                    }),
                ),
            ]))
            .style(if focus == 1 {
                Style::default().bg(Color::Rgb(42, 42, 52))
            } else {
                Style::default()
            }),
        );
    }
    let worker_count = agent_infos
        .keys()
        .filter(|id| id.as_str() != FOREGROUND_ID)
        .count();
    let active_worker_count = agent_infos
        .values()
        .filter(|info| {
            info.id != FOREGROUND_ID
                && matches!(
                    info.state,
                    AgentState::Starting | AgentState::Running | AgentState::Waiting
                )
        })
        .count();
    items.push(ListItem::new(Line::from(vec![
        Span::raw("  "),
        Span::styled(
            format!("{:<18}", "Background Coord."),
            Style::default().fg(Color::Magenta),
        ),
        column_rule(),
        status_badge(
            &format!(
                "{} {}",
                animated_state_indicator(if active_worker_count > 0 {
                    ActivityState::Working
                } else {
                    ActivityState::Ready
                }),
                if active_worker_count > 0 {
                    "running"
                } else {
                    "ready"
                }
            ),
            if active_worker_count > 0 {
                Color::Yellow
            } else {
                Color::Green
            },
        ),
        column_rule(),
        Span::styled(
            format!("{:^9}", format!("{} workers", worker_count)),
            Style::default().fg(Color::DarkGray),
        ),
    ])));
    items.push(ListItem::new(Line::from(Span::styled(
        format!("    {}", "─".repeat(area.width.saturating_sub(8) as usize)),
        Style::default().fg(Color::DarkGray),
    ))));
    items.push(ListItem::new(agent_table_header("AGENT ID")));
    let pane_ids = pane_agent_ids(agent_infos);
    if pane_ids.is_empty() {
        items.push(ListItem::new(Line::from(Span::styled(
            "  no agents",
            Style::default().fg(Color::DarkGray),
        ))));
    }
    items.extend(
        pane_ids
            .iter()
            .enumerate()
            .map(|(row, id)| {
                let info = &agent_infos[id];
                let selected = row + 2 == focus;
                let sel = if selected { "┃ " } else { "  " };
                let color = agent_color(id);
                let label = truncate_text(&format!("ghost {}", id), 18);
                let state = agent_activity_state(info.state);
                let state_style = if selected {
                    Color::Gray
                } else {
                    Color::DarkGray
                };
                let now = unix_now_secs();
                let deadline = info.stage_until_secs.or(info.lease_until_secs);
                let ttl = deadline
                    .map(|until| format!("ttl {}s", until.saturating_sub(now)))
                    .unwrap_or_else(|| "ttl -".into());
                let state_text = format!(
                    "[{}] [{}]{}{}{} {}",
                    info.state,
                    info.lifetime_class,
                    if info.retained { " [retained]" } else { "" },
                    info.turn_budget
                        .map(|budget| format!(" [turns {}/{}]", info.turns_used, budget))
                        .unwrap_or_default(),
                    if info.persistent { " [persistent]" } else { "" },
                    ttl
                );
                let age = format_age(info.created_secs);
                let pid = info
                    .pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".into());
                let task = truncate_text(&format!("{}: {}", info.task_type, info.description), 36);
                let badge = lifecycle_badge(info.state);
                let task_width = (area.width as usize)
                    .saturating_sub(97 + badge.chars().count() + 1)
                    .max(8)
                    .min(36);
                let task = truncate_text(&task, task_width);
                let policy_text = truncate_text(&state_text, 21);
                let row_style = if selected {
                    Style::default().bg(Color::Rgb(42, 42, 52))
                } else {
                    Style::default()
                };
                let label_style = Style::default()
                    .fg(if selected { color } else { Color::DarkGray })
                    .add_modifier(if selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    });
                ListItem::new(Line::from(vec![
                    Span::styled(
                        sel,
                        Style::default()
                            .fg(if selected {
                                Color::Cyan
                            } else {
                                Color::DarkGray
                            })
                            .add_modifier(if selected {
                                Modifier::BOLD
                            } else {
                                Modifier::empty()
                            }),
                    ),
                    Span::styled(format!("{label:<18}"), label_style),
                    column_rule(),
                    status_badge(state_label(state), activity_color(state)),
                    column_rule(),
                    Span::styled(
                        format!("{policy_text:<21}"),
                        Style::default().fg(state_style),
                    ),
                    column_rule(),
                    Span::styled(format!("{:^9}", age), Style::default().fg(state_style)),
                    column_rule(),
                    Span::styled(format!("{:^9}", pid), Style::default().fg(state_style)),
                    column_rule(),
                    Span::styled(
                        format!("{:^8}", if info.sandboxed { "●" } else { "" }),
                        Style::default().fg(if info.sandboxed {
                            Color::Green
                        } else {
                            state_style
                        }),
                    ),
                    column_rule(),
                    Span::styled(format!("{}", task), Style::default().fg(state_style)),
                    Span::raw(
                        " ".repeat(
                            (area.width as usize)
                                .saturating_sub(97 + task.chars().count() + badge.chars().count()),
                        ),
                    ),
                    Span::styled(
                        badge,
                        Style::default()
                            .fg(state_style)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]))
                .style(row_style)
            })
            .collect::<Vec<_>>(),
    );
    let inner = block.inner(area);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(2)])
        .split(inner);
    f.render_widget(block, area);
    let selected_item = if focus == 0 {
        1
    } else if focus == 1 {
        2
    } else {
        focus + 3
    };
    let mut list_state = ListState::default();
    list_state.select(Some(selected_item.min(items.len().saturating_sub(1))));
    f.render_stateful_widget(
        List::new(items).style(Style::default().bg(Color::Rgb(18, 18, 22))),
        sections[0],
        &mut list_state,
    );
    let selected_detail = if focus == 0 {
        format!(
            "tachyond · {} · uptime {}",
            daemon_state.1,
            daemon_since
                .map(|since| format_duration(since.elapsed()))
                .unwrap_or_else(|| "-".into())
        )
    } else if focus == 1 {
        "foreground · status managed by daemon".into()
    } else if let Some(id) = pane_ids.get(focus.saturating_sub(2)) {
        if let Some(info) = agent_infos.get(id) {
            let deadline = info.stage_until_secs.or(info.lease_until_secs);
            let ttl = deadline
                .map(|until| format!("ttl {}s", until.saturating_sub(unix_now_secs())))
                .unwrap_or_else(|| "ttl -".into());
            format!(
                "{} · {} · {} · {} · {} · age {} · pid {}",
                info.task_type,
                info.description,
                info.state,
                if info.persistent {
                    "persistent"
                } else {
                    "session"
                },
                ttl,
                format_age(info.created_secs),
                info.pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".into()),
            )
        } else {
            "agent information unavailable".into()
        }
    } else {
        "no process selected".into()
    };
    let selected_detail = truncate_text(&selected_detail, sections[1].width as usize);
    f.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                format!(
                    "{}/{} selected · {} agents · ↑/↓ select · s stop · r restart · u resume · k kill · x release · S start daemon",
                    focus.saturating_add(1),
                    pane_ids.len().saturating_add(2),
                    pane_ids.len()
                ),
                Style::default().fg(Color::DarkGray),
            )),
            Line::from(Span::styled(
                selected_detail,
                Style::default().fg(Color::DarkGray),
            )),
        ])
        .alignment(Alignment::Center)
        .style(Style::default().bg(Color::Rgb(18, 18, 22))),
        sections[1],
    );
}

// ---- input editing helpers ------------------------------------------------

/// Number of chars in a str.
fn chars(s: &str) -> usize {
    s.chars().count()
}

/// Insert a char at the cursor.
fn insert_at(s: &mut String, cursor: &mut usize, c: char) {
    let byte = char_byte_index(s, *cursor);
    s.insert(byte, c);
    *cursor += 1;
}

/// Insert a full string (paste) at the cursor.
fn paste_text(s: &mut String, cursor: &mut usize, text: &str) {
    let byte = char_byte_index(s, *cursor);
    s.insert_str(byte, text);
    *cursor += chars(text);
}

/// Delete the char before the cursor.
fn backspace_at(s: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let byte = char_byte_index(s, *cursor - 1);
    s.remove(byte);
    *cursor -= 1;
}

/// Delete the word immediately before the cursor, including preceding spaces.
fn delete_word_left(s: &mut String, cursor: &mut usize) {
    let start = *cursor;
    move_word_left(s, cursor);
    for _ in 0..start.saturating_sub(*cursor) {
        delete_at(s, cursor);
    }
}

/// Delete the char at the cursor.
fn delete_at(s: &mut String, cursor: &mut usize) {
    if *cursor >= chars(s) {
        return;
    }
    let byte = char_byte_index(s, *cursor);
    s.remove(byte);
}

/// Move the cursor one word left.
fn move_word_left(s: &str, cursor: &mut usize) {
    let mut cut = *cursor;
    if cut == 0 {
        return;
    }
    // Skip whitespace to the left, then the word to its left.
    while cut > 0 && s.chars().nth(cut - 1) == Some(' ') {
        cut -= 1;
    }
    while cut > 0 && s.chars().nth(cut - 1) != Some(' ') {
        cut -= 1;
    }
    *cursor = cut;
}

/// Move the cursor one word right.
fn move_word_right(s: &str, cursor: &mut usize) {
    let total = chars(s);
    let mut cur = *cursor;
    // Skip spaces, then skip non-space.
    while cur < total && s.chars().nth(cur) == Some(' ') {
        cur += 1;
    }
    while cur < total && s.chars().nth(cur) != Some(' ') {
        cur += 1;
    }
    *cursor = cur;
}

/// Byte index of the `ci`-th char (clamped).
fn char_byte_index(s: &str, ci: usize) -> usize {
    let mut byte = 0;
    for (i, ch) in s.char_indices() {
        if i >= ci {
            break;
        }
        byte = i + ch.len_utf8();
    }
    byte
}

// ---- subscriptions -------------------------------------------------------

fn accept_event(seen: &mut HashSet<(String, u64)>, envelope: &EventEnvelope) -> bool {
    envelope.event_id == 0 || seen.insert((envelope.session_id.clone(), envelope.event_id))
}

fn decode_interaction_event(data: &str) -> Option<InteractionEventEnvelope> {
    serde_json::from_str(data).ok()
}

fn spawn_stream_thread(agent_id: String, to_ui: mpsc::Sender<TuiEvent>) {
    std::thread::spawn(move || {
        let mut sub = match Subscription::open(&agent_id) {
            Ok(s) => s,
            Err(_) => {
                let _ = to_ui.send(TuiEvent::Ended {
                    agent_id,
                    summary: "subscribe failed (agent may be creating)".into(),
                });
                return;
            }
        };
        while let Some(resp) = sub.next() {
            match resp {
                ApiResponse::Event { stream, data } => {
                    if let Some(envelope) = decode_interaction_event(&data) {
                        if to_ui
                            .send(TuiEvent::Interaction {
                                agent_id: agent_id.clone(),
                                envelope,
                            })
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    let envelope = serde_json::from_str::<EventEnvelope>(&data).or_else(|_| {
                        serde_json::from_str::<AgentEvent>(&data).map(|kind| EventEnvelope {
                            event_id: 0,
                            session_id: agent_id.clone(),
                            conversation_id: None,
                            turn_id: None,
                            task_id: None,
                            parent_task_id: None,
                            tool_call_id: None,
                            actor: tachyon_api::types::Actor::System,
                            sequence: 0,
                            occurred_at_ms: now_seconds(),
                            kind,
                        })
                    });
                    if let Ok(envelope) = envelope {
                        if to_ui
                            .send(TuiEvent::Structured {
                                agent_id: agent_id.clone(),
                                envelope,
                            })
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    if is_structured_legacy_marker(&data) {
                        continue;
                    }
                    if to_ui
                        .send(TuiEvent::Line {
                            agent_id: agent_id.clone(),
                            stream,
                            data,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                ApiResponse::Error { message, .. } => {
                    let _ = to_ui.send(TuiEvent::Ended {
                        agent_id: agent_id.clone(),
                        summary: message,
                    });
                    return;
                }
                _ => {}
            }
        }
        let _ = to_ui.send(TuiEvent::Ended {
            agent_id,
            summary: "stream ended".into(),
        });
    });
}

fn is_structured_legacy_marker(data: &str) -> bool {
    let data = data.trim_start();
    data.starts_with("[turn:")
        && (data.contains(" [status]")
            || data.contains(" [text]")
            || data.contains(" [text-break]")
            || data.contains(" [tool:")
            || data.contains(" [tool-result:")
            || data.contains(" [agent]"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::types::Actor;

    fn envelope(event_id: u64, kind: AgentEvent) -> EventEnvelope {
        EventEnvelope {
            event_id,
            session_id: "conversation".into(),
            conversation_id: Some("conversation".into()),
            turn_id: Some("2".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: Actor::Foreground,
            sequence: event_id,
            occurred_at_ms: 1,
            kind,
        }
    }

    fn interaction(event: InteractionEvent) -> InteractionEventEnvelope {
        InteractionEventEnvelope {
            metadata: tachyon_api::InteractionMetadata {
                protocol_version: tachyon_api::INTERACTION_PROTOCOL_VERSION,
                message_id: "event-1".into(),
                correlation_id: "turn-2".into(),
                causation_id: Some("command-1".into()),
                conversation_id: FOREGROUND_ID.into(),
                turn_id: Some("2".into()),
                generation: 1,
                occurred_at_ms: 1,
            },
            event,
        }
    }

    #[test]
    fn typed_interaction_stream_projects_one_final_reply() {
        let mut thread = Thread::new_foreground();
        thread.add(ItemKind::User, "hello".into());
        thread.reserve_reply();

        apply_interaction_event(
            &mut thread,
            interaction(InteractionEvent::UserTurnAccepted {
                text: "hello".into(),
            }),
        );
        apply_interaction_event(
            &mut thread,
            interaction(InteractionEvent::ConversationDelta { text: "Hi ".into() }),
        );
        apply_interaction_event(
            &mut thread,
            interaction(InteractionEvent::ConversationFinished {
                text: "Hi there.".into(),
            }),
        );

        assert_eq!(thread.items.len(), 2);
        assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
        assert_eq!(thread.items[1].kind, ItemKind::Reply);
        assert_eq!(thread.items[1].text, "Hi there.");
        assert!(!thread.streaming);
    }

    #[test]
    fn interaction_envelope_is_decoded_before_line_fallback() {
        let wire = serde_json::to_string(&interaction(InteractionEvent::ConversationFinished {
            text: "done".into(),
        }))
        .unwrap();
        let decoded = decode_interaction_event(&wire).expect("typed interaction event");
        assert!(matches!(
            decoded.event,
            InteractionEvent::ConversationFinished { text } if text == "done"
        ));
    }

    #[test]
    fn user_echo_reconciles_the_daemon_turn_id() {
        let mut thread = Thread::new_foreground();
        thread.add(ItemKind::User, "hello".into());
        classify_line(&mut thread, "[turn:2] [user] hello");
        assert_eq!(
            thread.items.last().and_then(|item| item.turn.as_deref()),
            Some("2")
        );
    }

    #[test]
    fn reserved_reply_is_reconciled_and_filled_in_place() {
        let mut thread = Thread::new_foreground();
        thread.add(ItemKind::User, "hello".into());
        thread.reserve_reply();

        classify_line(&mut thread, "[turn:2] [user] hello");
        assert_eq!(thread.items.len(), 2);
        assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
        assert_eq!(thread.items[1].turn.as_deref(), Some("2"));
        assert_eq!(thread.items[1].kind, ItemKind::PendingReply);

        thread.add_reply_fragment("answer".into(), Some("2".into()), false);
        assert_eq!(thread.items.len(), 2);
        assert_eq!(thread.items[1].kind, ItemKind::Reply);
        assert_eq!(thread.items[1].text, "answer");
    }

    #[test]
    fn final_reply_is_idempotent_per_turn() {
        let mut thread = Thread::new_foreground();
        apply_agent_event(
            &mut thread,
            AgentEvent::Reply {
                turn: Some(2),
                text: "first".into(),
                final_reply: true,
            },
        );
        apply_agent_event(
            &mut thread,
            AgentEvent::Reply {
                turn: Some(2),
                text: "corrected".into(),
                final_reply: true,
            },
        );
        let replies = thread
            .items
            .iter()
            .filter(|item| item.kind == ItemKind::Reply)
            .collect::<Vec<_>>();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].text, "corrected");
    }

    #[test]
    fn interleaved_reply_deltas_stay_grouped_by_turn() {
        let mut thread = Thread::new_foreground();
        for (turn, text) in [(2, "slow "), (3, "hello "), (2, "answer"), (3, "there")] {
            apply_agent_event(
                &mut thread,
                AgentEvent::ReplyDelta {
                    turn: Some(turn),
                    text: text.into(),
                },
            );
        }

        let replies = thread
            .items
            .iter()
            .filter(|item| item.kind == ItemKind::Reply)
            .collect::<Vec<_>>();
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].text, "slow answer");
        assert_eq!(replies[1].text, "hello there");
    }

    #[test]
    fn late_reply_is_inserted_into_its_turn_block() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "second question".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "third question".into(), Some("3".into()));
        thread.finish_reply("third answer".into(), Some("3".into()));
        thread.finish_reply("second answer".into(), Some("2".into()));

        let timeline = thread
            .items
            .iter()
            .filter(|item| matches!(item.kind, ItemKind::User | ItemKind::Reply))
            .map(|item| (item.turn.as_deref().unwrap_or_default(), item.text.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            timeline,
            [
                ("2", "second question"),
                ("2", "second answer"),
                ("3", "third question"),
                ("3", "third answer"),
            ]
        );
    }

    #[test]
    fn worker_start_uses_its_correlated_origin_turn() {
        let mut thread = Thread::new_foreground();
        apply_agent_event(
            &mut thread,
            AgentEvent::WorkerStarted {
                turn: Some(2),
                worker_id: "worker-1".into(),
                objective: "objective".into(),
            },
        );
        let spawn = thread
            .items
            .iter()
            .find(|item| item.kind == ItemKind::Spawn)
            .expect("spawn item");
        assert_eq!(spawn.turn.as_deref(), Some("2"));
    }

    #[test]
    fn event_ids_are_deduplicated_per_session() {
        let mut seen = HashSet::new();
        let event = envelope(
            7,
            AgentEvent::Status {
                turn: Some(2),
                phase: "working".into(),
                message: String::new(),
            },
        );
        assert!(accept_event(&mut seen, &event));
        assert!(!accept_event(&mut seen, &event));

        let legacy = envelope(0, event.kind.clone());
        assert!(accept_event(&mut seen, &legacy));
        assert!(accept_event(&mut seen, &legacy));
    }

    #[test]
    fn reply_projection_removes_persisted_mojibake_dsml() {
        let damaged = "Let me check those cities.\n\n<� DSML� tool_calls>\n<� DSML� invoke name=\"spawn_agents\">\n<� DSML� parameter name=\"agents\">secret";
        assert_eq!(sanitize_reply_text(damaged), "Let me check those cities.");

        let mut thread = Thread::new_foreground();
        thread.finish_reply(damaged.into(), Some("2".into()));
        assert_eq!(thread.items[0].text, "Let me check those cities.");
    }

    #[test]
    fn reply_projection_preserves_normal_dsml_discussion() {
        assert_eq!(
            sanitize_reply_text("Explain DSML tool_calls in plain English."),
            "Explain DSML tool_calls in plain English."
        );
    }

    #[test]
    fn reply_projection_hides_heavily_corrupted_lines() {
        assert_eq!(
            sanitize_reply_text("Weather is mild.\n� � � � � � � � � �"),
            "Weather is mild."
        );
        assert_eq!(
            sanitize_reply_text("� � � � � � � �"),
            "[model output could not be decoded]"
        );
    }

    #[test]
    fn turn_latency_uses_user_acceptance_not_worker_start() {
        let mut thread = Thread::new_foreground();
        thread.items.push(Item {
            kind: ItemKind::User,
            text: "request".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 1_000,
        });
        thread.items.push(Item {
            kind: ItemKind::Spawn,
            text: "worker".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 4_000,
        });
        let badges = turn_badges(&thread, Some("2"), 6_000, true);
        assert!(badges.contains("5.0s"));
    }

    #[test]
    fn turn_badges_show_structured_latency_and_aggregate_usage() {
        let mut threads = vec![Thread::new_foreground()];
        for (event_id, kind) in [
            (
                1,
                AgentEvent::Timing {
                    turn: 2,
                    stage: "first_visible".into(),
                    elapsed_ms: 1_250,
                },
            ),
            (
                2,
                AgentEvent::Timing {
                    turn: 2,
                    stage: "completed".into(),
                    elapsed_ms: 4_500,
                },
            ),
            (
                3,
                AgentEvent::Usage {
                    turn: Some(2),
                    prompt_tokens: 1_000,
                    completion_tokens: 200,
                    total_tokens: 1_200,
                },
            ),
        ] {
            record_correlated_metrics(&mut threads, &envelope(event_id, kind));
        }
        let mut worker = envelope(
            4,
            AgentEvent::Usage {
                turn: Some(1),
                prompt_tokens: 2_000,
                completion_tokens: 300,
                total_tokens: 2_300,
            },
        );
        worker.actor = Actor::Worker {
            id: "worker-1".into(),
        };
        worker.session_id = "worker-1".into();
        worker.task_id = Some("assignment-1".into());
        record_correlated_metrics(&mut threads, &worker);
        record_correlated_metrics(&mut threads, &worker);

        let badges = turn_badges(&threads[0], Some("2"), 99_000, false);
        assert!(!badges.contains("󱎫"), "{badges}");
        assert!(badges.contains("󰅐 done 4.5s"), "{badges}");
        assert!(!badges.contains("󰍛"), "{badges}");
        assert!(badges.contains("󰏪 total 3.5k"), "{badges}");

        let trace_badges = turn_badges(&threads[0], Some("2"), 99_000, true);
        assert!(
            trace_badges.contains("󱎫 first response 1.2s"),
            "{trace_badges}"
        );
        assert!(
            trace_badges.contains("󰅐 response completed 4.5s"),
            "{trace_badges}"
        );
        assert!(
            trace_badges.contains("󰍛 foreground tokens 1.2k"),
            "{trace_badges}"
        );
        assert!(
            trace_badges.contains("󰏪 aggregate tokens 3.5k"),
            "{trace_badges}"
        );

        let mut compact_thread = Thread::new_foreground();
        compact_thread.metrics.insert(
            "3".into(),
            TurnMetrics {
                first_visible_ms: Some(4_000),
                completed_ms: Some(4_500),
                self_usage: Some(TokenTotals {
                    prompt: 900,
                    completion: 100,
                    total: 1_000,
                }),
                worker_usage: HashMap::new(),
            },
        );
        let compact_badges = turn_badges(&compact_thread, Some("3"), 99_000, false);
        assert!(!compact_badges.contains("󱎫"), "{compact_badges}");
        assert!(!compact_badges.contains("󰍛"), "{compact_badges}");
        assert!(compact_badges.contains("󰅐 done 4.5s"), "{compact_badges}");
        assert!(compact_badges.contains("󰏪 total 1.0k"), "{compact_badges}");
    }

    #[test]
    fn trace_summaries_compact_lifecycle_events() {
        assert_eq!(
            trace_summary("[timing] model_request_1_started 0ms"),
            "󰐊 model request 1 · +0ms"
        );
        assert_eq!(
            trace_summary("[timing] model_request_1_completed 3220ms"),
            "󰅐 model request 1 · 3220ms"
        );
        assert_eq!(
            trace_summary("[working] I'll check the weather"),
            "󰔟 working · I'll check the weather"
        );
    }

    #[test]
    fn late_completion_is_identified_as_an_earlier_request() {
        let mut thread = Thread::new_foreground();
        thread.items.push(Item {
            kind: ItemKind::User,
            text: "slow request".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 1_000,
        });
        thread.items.push(Item {
            kind: ItemKind::User,
            text: "foreground request".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("3".into()),
            timestamp: 2_000,
        });
        thread.items.push(Item {
            kind: ItemKind::Reply,
            text: "late result".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 3_000,
        });
        assert!(reply_completed_after_later_user(
            &thread,
            thread.items.last().unwrap()
        ));
    }
}
