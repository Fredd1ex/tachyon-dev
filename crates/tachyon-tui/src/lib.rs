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
//!   Up / Down    select adjacent turns and expand traces
//!   PageUp/Down  page within or select trace turns
//!   End          return to the latest turn
//!   Ctrl+O       toggle inline traces for the current turn
//!   y             copy the selected chat cell
//!   Ctrl+Shift+C  copy the selected or latest chat cell
//!   Ctrl+C / Esc / /exit  quit
//!
//! Slash commands: /exit, /await, /stop, /release, /replan, /kill

use std::collections::{BTreeSet, HashMap, HashSet};
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
use ratatui::widgets::block::Title;
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table,
};
use ratatui::Frame;
use ratatui::Terminal;

use tachyon_api::types::{
    Actor, AgentEvent, AgentInfo, AgentState, ApiResponse, DaemonInfo, EventEnvelope, EventStream,
    LifetimeClass, MemoryMutationKind, MemoryMutationResult, ScheduledTaskInfo, ScheduledTaskMode,
    ScheduledTaskStatus, WorkOutcome,
};
use tachyon_api::{
    InteractionEvent, InteractionEventEnvelope, BACKGROUND_ID, FOREGROUND_ID, MEMORY_ID,
};

use tachyon_client::{Client, Subscription};

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
#[derive(Clone, Debug, Hash, PartialEq)]
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
    revision: u64,
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

fn ready_earlier_turn(threads: &[Thread]) -> Option<u64> {
    let thread = threads.iter().find(|thread| thread.is_foreground)?;
    thread
        .unread_turns
        .iter()
        .filter_map(|turn| turn.parse::<u64>().ok())
        .min()
}

fn ready_notice(turn: u64) -> String {
    format!("{} response {turn} ready", icon::SUCCESS)
}

fn mark_ready_turn_seen(threads: &mut [Thread], turn: u64) {
    if let Some(thread) = threads.iter_mut().find(|thread| thread.is_foreground) {
        thread.unread_turns.remove(&turn.to_string());
    }
}

fn mark_visible_ready_turns_seen(
    threads: &mut [Thread],
    projection: &TurnProjection,
    view: &TranscriptView,
    scroll: &TranscriptScroll,
) {
    let Some(thread) = threads.iter_mut().find(|thread| thread.is_foreground) else {
        return;
    };
    let bottom = scroll.top.saturating_add(view.viewport);
    let visible = projection
        .cells
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let start = view.starts.get(*index).copied().unwrap_or(usize::MAX);
            let end = start.saturating_add(view.heights.get(*index).copied().unwrap_or(0));
            start >= scroll.top && end <= bottom
        })
        .filter_map(|(_, cell)| thread.items[cell.prompt].turn.clone())
        .collect::<Vec<_>>();
    for turn in visible {
        thread.unread_turns.remove(&turn);
    }
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
    structure_revision: u64,
    items: Vec<Item>,
    completed_turns: BTreeSet<String>,
    unread_turns: BTreeSet<String>,
    usage: HashMap<u64, (u32, u32, u32)>,
    metrics: HashMap<String, TurnMetrics>,
    metric_revisions: HashMap<String, u64>,
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
    #[serde(default)]
    memory: MemoryTurnMetrics,
    #[serde(default)]
    schedule: ScheduleTurnMetrics,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct MemoryTurnMetrics {
    saved: u32,
    forgotten: u32,
    corrected: u32,
    failed: u32,
    recalled_preferences: u32,
    recalled_history: u32,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct ScheduleTurnMetrics {
    scheduled: u32,
    #[serde(default)]
    tasks_scheduled: u32,
    cancelled: u32,
    fired: u32,
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
            structure_revision: 0,
            items: Vec::new(),
            completed_turns: BTreeSet::new(),
            unread_turns: BTreeSet::new(),
            usage: HashMap::new(),
            metrics: HashMap::new(),
            metric_revisions: HashMap::new(),
        }
    }

    fn add(&mut self, kind: ItemKind, text: String) {
        self.add_turn(kind, text, None);
    }

    fn reserve_reply(&mut self) {
        self.touch_structure();
        self.items.push(Item {
            kind: ItemKind::PendingReply,
            text: String::new(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: None,
            timestamp: now_seconds(),
            revision: self.revision,
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
                last.revision = self.revision;
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
                last.revision = self.revision;
            }
        } else {
            self.structure_revision = self.structure_revision.wrapping_add(1);
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
                        revision: self.revision,
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
                    revision: self.revision,
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
            existing.revision = self.revision;
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
            existing.revision = self.revision;
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
            revision: self.revision,
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
        self.structure_revision = self.structure_revision.wrapping_add(1);
    }

    fn finish_reply(&mut self, text: String, turn: Option<String>) {
        self.touch();
        self.streaming = false;
        if let Some(completed_turn) = turn.as_deref() {
            let newly_completed = self.completed_turns.insert(completed_turn.to_string());
            let completed_number = completed_turn.parse::<u64>().ok();
            let has_later_turn = completed_number.is_some_and(|completed_number| {
                self.items.iter().any(|item| {
                    item.kind == ItemKind::User
                        && item
                            .turn
                            .as_deref()
                            .and_then(|turn| turn.parse::<u64>().ok())
                            .is_some_and(|turn| turn > completed_number)
                })
            });
            if newly_completed && has_later_turn {
                self.unread_turns.insert(completed_turn.to_string());
            }
        }
        let text = sanitize_reply_text(&text);
        if let Some(existing) = self
            .items
            .iter_mut()
            .rev()
            .find(|item| item.kind == ItemKind::Reply && item.turn == turn)
        {
            existing.text = text;
            existing.timestamp = now_seconds();
            existing.revision = self.revision;
            self.last_activity = Instant::now();
            return;
        }
        self.add_reply_fragment(text, turn, false);
        self.streaming = false;
    }

    fn update_pending_reply_status(&mut self, turn: Option<String>, phase: &str, message: &str) {
        let Some(turn) = turn else {
            return;
        };
        let Some(index) = self.items.iter().rposition(|item| {
            item.kind == ItemKind::PendingReply && item.turn.as_deref() == Some(&turn)
        }) else {
            return;
        };
        self.touch();
        let pending = &mut self.items[index];
        if !message.trim().is_empty() {
            pending.text = message.trim().to_string();
        } else if pending.text.trim().is_empty() {
            pending.text = phase.to_string();
        }
        pending.revision = self.revision;
    }

    fn add_tool(&mut self, text: String, id: String, turn: Option<String>) {
        self.touch_structure();
        self.streaming = false;
        self.items.push(Item {
            kind: ItemKind::Tool,
            text,
            hidden: true,
            output: None,
            tool_id: Some(id),
            turn,
            timestamp: now_seconds(),
            revision: self.revision,
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
            tool.revision = self.revision;
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

    fn touch_structure(&mut self) {
        self.touch();
        self.structure_revision = self.structure_revision.wrapping_add(1);
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
    Scheduled,
    Memory,
}

const ORCHESTRATORS_TAB_LABEL: &str = " ORCHESTRATORS ";
const WINDOW_LOGO: &str = "󰘵";
const WINDOW_LOGO_BUTTON: &str = " 󰘵 ";
const INPUT_PROMPT_MARKER: &str = "❯ ";
// const INPUT_PROMPT_MARKER: &str = "⌥ ";

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClickTarget {
    TraceSummary(usize),
    Worker(usize, String),
    Item(usize, usize),
}

/// Per-render-row click target. Rows without a target are `None`.
type Hit = Option<ClickTarget>;
static HITS: std::sync::Mutex<Vec<Hit>> = std::sync::Mutex::new(Vec::new());
/// (area.y, area.height) of the last conversation render.
static VIEW: std::sync::Mutex<(u16, u16)> = std::sync::Mutex::new((0, 0));

#[derive(Clone, Debug, PartialEq)]
struct TranscriptScroll {
    top: usize,
    follow: bool,
    new_activity: bool,
    seen_latest_revision: u64,
}

impl Default for TranscriptScroll {
    fn default() -> Self {
        Self {
            top: 0,
            follow: true,
            new_activity: false,
            seen_latest_revision: 0,
        }
    }
}

impl TranscriptScroll {
    fn sync(&mut self, total: usize, viewport: usize, latest_revision: u64) {
        let max_top = total.saturating_sub(viewport);
        if self.follow {
            self.top = max_top;
            self.new_activity = false;
        } else {
            self.top = self.top.min(max_top);
            if self.seen_latest_revision != 0 && self.seen_latest_revision != latest_revision {
                self.new_activity = true;
            }
        }
        self.seen_latest_revision = latest_revision;
    }

    fn scroll_up(&mut self, rows: usize) {
        self.follow = false;
        self.top = self.top.saturating_sub(rows);
    }

    fn scroll_down(&mut self, rows: usize, total: usize, viewport: usize) {
        let max_top = total.saturating_sub(viewport);
        self.top = self.top.saturating_add(rows).min(max_top);
        if self.top == max_top {
            self.follow = true;
            self.new_activity = false;
        }
    }

    fn end(&mut self) {
        self.follow = true;
        self.new_activity = false;
    }
}

#[derive(Clone, Default)]
struct TranscriptView {
    total_height: usize,
    viewport: usize,
    turns: usize,
    anchor_turn: Option<usize>,
    starts: Vec<usize>,
    heights: Vec<usize>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct CellKey {
    prompt_timestamp: u64,
    prompt_index: usize,
}

struct CachedLayout {
    revision: CellRevision,
    variant: u8,
    layout: CellLayout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CellRevision {
    item: u64,
    metric: u64,
    worker: u64,
}

#[derive(Clone)]
struct CellLayout {
    lines: Vec<Line<'static>>,
    hits: Vec<Hit>,
}

#[derive(Default)]
struct TurnLayoutCache {
    layouts: HashMap<CellKey, CachedLayout>,
    width: Option<u16>,
    structure_revision: Option<u64>,
    #[cfg(test)]
    builds: usize,
}

impl TurnLayoutCache {
    fn reset(&mut self) {
        self.layouts.clear();
        self.width = None;
        self.structure_revision = None;
    }

    fn prepare(&mut self, width: u16, cells: &[TurnCell], structure_revision: u64) {
        if self.width != Some(width) {
            self.reset();
            self.width = Some(width);
        }
        if self.structure_revision == Some(structure_revision) {
            return;
        }
        let valid = cells.iter().map(cell_key).collect::<HashSet<_>>();
        self.layouts.retain(|key, _| valid.contains(key));
        self.structure_revision = Some(structure_revision);
    }

    fn layout<F>(
        &mut self,
        key: CellKey,
        revision: CellRevision,
        variant: u8,
        build: F,
    ) -> &CellLayout
    where
        F: FnOnce() -> CellLayout,
    {
        let stale = self
            .layouts
            .get(&key)
            .is_none_or(|cached| cached.revision != revision || cached.variant != variant);
        if stale {
            let layout = build();
            #[cfg(test)]
            {
                self.builds += 1;
            }
            self.layouts.insert(
                key.clone(),
                CachedLayout {
                    revision,
                    variant,
                    layout,
                },
            );
        }
        &self.layouts[&key].layout
    }
}

struct TurnCell {
    prompt: usize,
    items: Vec<usize>,
    prompt_timestamp: u64,
}

#[derive(Default)]
struct TurnProjection {
    structure_revision: Option<u64>,
    cells: Vec<TurnCell>,
}

impl TurnProjection {
    fn reset(&mut self) {
        self.structure_revision = None;
        self.cells.clear();
    }

    fn update(&mut self, thread: &Thread) -> bool {
        if self.structure_revision == Some(thread.structure_revision) {
            return false;
        }
        self.cells = build_turn_cells(thread);
        self.structure_revision = Some(thread.structure_revision);
        true
    }
}

fn transcript_content_height(height: u16, show_activity: bool) -> usize {
    (height as usize).saturating_sub(usize::from(show_activity && height > 1))
}

fn should_show_activity(scroll: &TranscriptScroll, total: usize, viewport: usize) -> bool {
    scroll.new_activity
        && !scroll.follow
        && viewport > 1
        && scroll.top.saturating_add(viewport) < total
}

fn foreground_thread(threads: &[Thread]) -> Option<(usize, &Thread)> {
    threads
        .iter()
        .enumerate()
        .find(|(_, thread)| thread.is_foreground)
}

fn build_turn_cells(thread: &Thread) -> Vec<TurnCell> {
    let mut cells = thread
        .items
        .iter()
        .enumerate()
        .filter(|(_, item)| item.kind == ItemKind::User)
        .map(|(prompt, item)| TurnCell {
            prompt,
            items: vec![prompt],
            prompt_timestamp: item.timestamp,
        })
        .collect::<Vec<_>>();
    let user_turns = cells
        .iter()
        .filter_map(|cell| thread.items[cell.prompt].turn.as_deref())
        .collect::<BTreeSet<_>>();
    cells.extend(
        thread
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                item.kind == ItemKind::Reply
                    && item
                        .turn
                        .as_deref()
                        .is_none_or(|turn| !user_turns.contains(turn))
            })
            .map(|(prompt, item)| TurnCell {
                prompt,
                items: vec![prompt],
                prompt_timestamp: item.timestamp,
            }),
    );
    cells.sort_by_key(|cell| cell.prompt);
    let mut by_turn = HashMap::<&str, usize>::new();
    let mut by_prompt = HashMap::<usize, usize>::new();
    for (cell, turn) in cells.iter().enumerate() {
        by_prompt.insert(turn.prompt, cell);
    }
    for (cell, turn) in cells.iter().enumerate().filter_map(|(cell, turn)| {
        thread.items[turn.prompt]
            .turn
            .as_deref()
            .map(|id| (cell, id))
    }) {
        by_turn.insert(turn, cell);
    }
    let mut current = None;
    for (index, item) in thread.items.iter().enumerate() {
        if by_prompt.contains_key(&index) {
            current = by_prompt.get(&index).copied();
            continue;
        }
        let cell = match item.turn.as_deref() {
            Some(turn) => by_turn.get(turn).copied(),
            None => current,
        };
        if let Some(cell) = cell {
            cells[cell].items.push(index);
        }
    }
    cells
}

fn cell_revision(thread: &Thread, cell: &TurnCell) -> CellRevision {
    let item_revision = cell
        .items
        .iter()
        .map(|index| thread.items[*index].revision)
        .max()
        .unwrap_or(0);
    let metric_revision = thread.items[cell.prompt]
        .turn
        .as_ref()
        .and_then(|turn| thread.metric_revisions.get(turn))
        .copied()
        .unwrap_or(0);
    CellRevision {
        item: item_revision,
        metric: metric_revision,
        worker: 0,
    }
}

fn worker_turn_revisions(threads: &[Thread]) -> HashMap<&str, u64> {
    let mut revisions = HashMap::new();
    for thread in threads.iter().filter(|thread| !thread.is_foreground) {
        for (index, item) in thread.items.iter().enumerate() {
            let Some(turn) = item.turn.as_deref() else {
                continue;
            };
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            thread.id.hash(&mut hasher);
            index.hash(&mut hasher);
            item.revision.hash(&mut hasher);
            let revision = hasher.finish();
            revisions
                .entry(turn)
                .and_modify(|combined: &mut u64| *combined ^= revision)
                .or_insert(revision);
        }
    }
    revisions
}

fn cell_key(cell: &TurnCell) -> CellKey {
    CellKey {
        prompt_timestamp: cell.prompt_timestamp,
        prompt_index: cell.prompt,
    }
}

fn latest_conversation_timestamp(thread: &Thread, cell: &TurnCell) -> u64 {
    cell.items
        .iter()
        .map(|index| &thread.items[*index])
        .filter(|item| {
            matches!(
                item.kind,
                ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
            )
        })
        .map(|item| item.timestamp)
        .max()
        .unwrap_or(0)
}

fn turn_response<'a>(thread: &'a Thread, cell: &TurnCell) -> Option<&'a Item> {
    cell.items
        .iter()
        .map(|index| &thread.items[*index])
        .find(|item| item.kind == ItemKind::Reply)
        .or_else(|| {
            cell.items
                .iter()
                .map(|index| &thread.items[*index])
                .find(|item| item.kind == ItemKind::PendingReply)
        })
}

fn toggle_trace(open_trace: &mut Option<usize>, turn: usize) {
    *open_trace = (*open_trace != Some(turn)).then_some(turn);
}

fn toggle_worker(open_worker: &mut Option<(usize, String)>, turn: usize, worker: String) {
    *open_worker = (*open_worker != Some((turn, worker.clone()))).then_some((turn, worker));
}

fn close_trace_details(
    open_trace: &mut Option<usize>,
    open_worker: &mut Option<(usize, String)>,
    scroll: &mut TranscriptScroll,
) -> bool {
    let closed = open_trace.take().is_some() | open_worker.take().is_some();
    if closed {
        scroll.end();
    }
    closed
}

fn ctrl_o_target(view: &TranscriptView, follow: bool) -> Option<usize> {
    if view.turns == 0 {
        None
    } else if follow {
        Some(view.turns - 1)
    } else {
        view.anchor_turn.map(|turn| turn.min(view.turns - 1))
    }
}

fn select_trace_turn(
    open_trace: &mut Option<usize>,
    view: &TranscriptView,
    scroll: &mut TranscriptScroll,
    direction: i8,
) {
    if view.turns == 0 {
        return;
    }
    let current = open_trace.unwrap_or_else(|| {
        if scroll.follow {
            view.turns - 1
        } else {
            view.anchor_turn.unwrap_or(view.turns - 1)
        }
    });
    let target = if open_trace.is_none() {
        current
    } else if direction < 0 {
        current.saturating_sub(1)
    } else if current + 1 < view.turns {
        current + 1
    } else {
        *open_trace = None;
        scroll.end();
        return;
    };
    *open_trace = Some(target);
    scroll.follow = false;
    scroll.new_activity = false;
    scroll.top = view.starts.get(target).copied().unwrap_or(scroll.top);
}

fn page_trace_turn(
    open_trace: &mut Option<usize>,
    view: &TranscriptView,
    scroll: &mut TranscriptScroll,
    direction: i8,
) {
    let Some(current) = open_trace.as_ref().copied() else {
        select_trace_turn(open_trace, view, scroll, direction);
        return;
    };
    let start = view.starts.get(current).copied().unwrap_or(0);
    let end = start.saturating_add(view.heights.get(current).copied().unwrap_or(0));
    if direction < 0 && scroll.top > start {
        scroll.scroll_up(view.viewport.max(1));
        scroll.top = scroll.top.max(start);
        return;
    }
    if direction > 0 && scroll.top.saturating_add(view.viewport) < end {
        scroll.top = scroll
            .top
            .saturating_add(view.viewport.max(1))
            .min(end.saturating_sub(view.viewport));
        scroll.follow = false;
        return;
    }
    select_trace_turn(open_trace, view, scroll, direction);
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
    format_elapsed(created_secs, unix_now_secs())
}

fn format_elapsed(start_secs: u64, end_secs: u64) -> String {
    format_duration(Duration::from_secs(end_secs.saturating_sub(start_secs)))
}

fn agent_duration(info: &AgentInfo) -> String {
    let end_secs = if info.state.is_terminal() {
        info.last_activity_secs.max(info.created_secs)
    } else {
        unix_now_secs()
    };
    format_elapsed(info.created_secs, end_secs)
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

fn remaining_duration(deadline_secs: u64) -> String {
    let remaining = deadline_secs.saturating_sub(unix_now_secs());
    if remaining == 0 {
        "due".into()
    } else {
        format_duration(Duration::from_secs(remaining))
    }
}

fn agent_lifetime(info: &AgentInfo) -> (String, String) {
    let retention = if info.retained {
        "retained"
    } else {
        "unretained"
    };
    let lifetime = format!("{} · {retention}", info.lifetime_class);
    if info.state.is_terminal() && info.state != AgentState::Completed {
        return (lifetime, "ended".into());
    }
    if let Some(deadline) = info.stage_until_secs {
        return (
            lifetime,
            format!("kill in {}", remaining_duration(deadline)),
        );
    }
    if let Some(deadline) = info.lease_until_secs {
        return (lifetime, format!("lease {}", remaining_duration(deadline)));
    }
    let remaining = match info.lifetime_class {
        LifetimeClass::Short => info
            .turn_budget
            .map(|budget| {
                let remaining = budget.saturating_sub(info.turns_used);
                format!(
                    "{remaining} assignment{}",
                    if remaining == 1 { "" } else { "s" }
                )
            })
            .unwrap_or_else(|| "idle cleanup".into()),
        LifetimeClass::Long => "daemon stop".into(),
        LifetimeClass::Persistent => "manual release".into(),
    };
    (lifetime, remaining)
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

fn agent_count(count: usize) -> String {
    format!("{count} agent{}", if count == 1 { "" } else { "s" })
}

fn bounded_worker_outcomes(spawned: usize, completed: usize, failed: usize) -> (usize, usize) {
    let completed = completed.min(spawned);
    let failed = failed.min(spawned.saturating_sub(completed));
    (completed, failed)
}

#[allow(dead_code)]
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
        format!("󰚩 {} running", agent_count(spawned))
    } else if completed == spawned {
        format!("󰄬 {} complete", agent_count(completed))
    } else {
        format!("󰚩 {} · 󰄬 {completed} complete", agent_count(spawned))
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
            return format!("{} {timing}", icon::DURATION);
        };
        let elapsed = elapsed
            .strip_suffix("ms")
            .and_then(|millis| millis.parse::<u64>().ok())
            .map(human_millis)
            .unwrap_or_else(|| elapsed.to_string());
        let label = stage.replace('_', " ");
        if stage.ends_with("_started") {
            return format!(
                "{} {} · +{elapsed}",
                icon::DURATION,
                label.trim_end_matches(" started")
            );
        }
        if stage.ends_with("_completed") {
            return format!(
                "{} {} · {elapsed}",
                icon::SUCCESS,
                label.trim_end_matches(" completed")
            );
        }
        return match stage {
            "ready" => format!("{} ready · +{elapsed}", icon::WAITING),
            "publication_started" => format!("{} publishing · {elapsed}", icon::RUNNING),
            "completed" => format!("{} turn completed · {elapsed}", icon::SUCCESS),
            _ => format!("{} {label} · {elapsed}", icon::DURATION),
        };
    }
    if let Some(detail) = text.strip_prefix("[working]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            format!("{} working", icon::RUNNING)
        } else {
            format!("{} working · {detail}", icon::RUNNING)
        };
    }
    if let Some(detail) = text.strip_prefix("[ready]") {
        let detail = detail.trim();
        return if detail.is_empty() {
            format!("{} ready", icon::WAITING)
        } else {
            format!("{} ready · {detail}", icon::WAITING)
        };
    }
    if let Some(detail) = text.strip_prefix("[warning]") {
        return format!("{} {}", icon::WARNING, detail.trim());
    }
    text.to_string()
}

fn human_millis(millis: u64) -> String {
    if millis < 1_000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", millis as f64 / 1_000.0)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionThread {
    id: String,
    parent: Option<String>,
    task: Option<String>,
    is_foreground: bool,
    items: Vec<SessionItem>,
    #[serde(default)]
    completed_turns: BTreeSet<String>,
    #[serde(default)]
    unread_turns: BTreeSet<String>,
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
            completed_turns: t.completed_turns.clone(),
            unread_turns: t.unread_turns.clone(),
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
        .map(|t| {
            let revision = t.items.len() as u64;
            let mut completed_turns = t.completed_turns;
            completed_turns.extend(t.metrics.iter().filter_map(|(turn, metrics)| {
                metrics.completed_ms.is_some().then(|| turn.clone())
            }));
            Thread {
                id: t.id,
                parent: t.parent,
                task: t.task,
                is_foreground: t.is_foreground,
                collapsed: !t.is_foreground,
                streaming: false,
                last_activity: Instant::now(),
                revision,
                structure_revision: 1,
                items: t
                    .items
                    .into_iter()
                    .enumerate()
                    .map(|(index, i)| {
                        let kind = kind_from_str(&i.kind);
                        let hidden =
                            i.hidden || kind == ItemKind::Tool || kind == ItemKind::ToolResult;
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
                            revision: index as u64 + 1,
                        }
                    })
                    .collect(),
                completed_turns,
                unread_turns: t.unread_turns,
                usage: HashMap::new(),
                metrics: t.metrics,
                metric_revisions: HashMap::new(),
            }
        })
        .collect();
    if threads.is_empty() {
        vec![Thread::new_foreground()]
    } else {
        threads
    }
}

fn archive_session_turns(threads: &mut [Thread], daemon_pid: Option<u32>) {
    let prefix = format!("archived:{}:", daemon_pid.unwrap_or_default());
    let archive = |turn: String| {
        if turn.starts_with("archived:") {
            turn
        } else {
            format!("{prefix}{turn}")
        }
    };
    for thread in threads {
        for item in &mut thread.items {
            item.turn = item.turn.take().map(&archive);
        }
        thread.completed_turns = std::mem::take(&mut thread.completed_turns)
            .into_iter()
            .map(&archive)
            .collect();
        thread.unread_turns.clear();
        thread.metrics = std::mem::take(&mut thread.metrics)
            .into_iter()
            .map(|(turn, metrics)| (archive(turn), metrics))
            .collect();
        thread.metric_revisions = std::mem::take(&mut thread.metric_revisions)
            .into_iter()
            .map(|(turn, revision)| (archive(turn), revision))
            .collect();
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
    let mut threads = load_session();
    if daemon_changed {
        archive_session_turns(&mut threads, previous_daemon_pid);
    }
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
    let mut scheduled_tasks: Vec<ScheduledTaskInfo> = Vec::new();
    let config = tachyon_util::config::Config::load();
    let mut daemon: Option<DaemonInfo> = None;
    let mut daemon_since: Option<Instant> = None;
    let mut input = String::new();
    let mut input_cursor: usize = 0; // char index into `input`
    let mut foreground_busy = false;
    let mut foreground_activity = "working".to_string();

    // Chat view state.
    let mut transcript_scroll = TranscriptScroll::default();
    let mut transcript_cache = TurnLayoutCache::default();
    let mut transcript_view = TranscriptView::default();
    let mut open_trace = None;
    let mut open_worker: Option<(usize, String)> = None;
    let mut turn_projection = TurnProjection::default();

    // Floating agent pane.
    let mut pane_open: bool = false;
    let mut pane_tab = PaneTab::Foreground;
    let mut commands_open: bool = false;
    let mut info_open: bool = false;

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
                                if a.id != MEMORY_ID && !subscribed.contains_key(&a.id) {
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
                    match c.scheduled_task_list() {
                        Ok(schedules) => scheduled_tasks = schedules,
                        Err(_) => scheduled_tasks.clear(),
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
                    scheduled_tasks.clear();
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
                    let envelope_turn = envelope.turn_id.clone();
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
                    apply_actor_event(&mut threads[idx], event, &actor, envelope_turn.as_deref());
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
                    foreground_busy,
                    &foreground_activity,
                    &mut transcript_scroll,
                    &mut transcript_cache,
                    &mut transcript_view,
                    open_trace,
                    open_worker.as_ref(),
                    &mut turn_projection,
                );
                mark_visible_ready_turns_seen(
                    &mut threads,
                    &turn_projection,
                    &transcript_view,
                    &transcript_scroll,
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
                        &scheduled_tasks,
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
                    open_trace,
                    transcript_scroll.follow,
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
                        yank_chat_cell(&threads, open_trace);
                    }
                    KeyCode::Char('y')
                        if !pane_open && input.is_empty() && open_trace.is_some() =>
                    {
                        yank_chat_cell(&threads, open_trace);
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        clear_history(&mut threads);
                        focus = foreground_focus(&threads);
                        reset_transcript(
                            &mut transcript_scroll,
                            &mut transcript_view,
                            &mut transcript_cache,
                            &mut open_trace,
                            &mut turn_projection,
                        );
                        open_worker = None;
                    }
                    KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        commands_open = !commands_open;
                        if commands_open {
                            pane_open = false;
                            info_open = false;
                        }
                    }
                    KeyCode::Char('?') if input.is_empty() => {
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
                        if let Some(turn) =
                            ctrl_o_target(&transcript_view, transcript_scroll.follow)
                        {
                            toggle_trace(&mut open_trace, turn);
                            open_worker = None;
                        }
                    }
                    KeyCode::Esc if commands_open => commands_open = false,
                    KeyCode::Esc
                        if close_trace_details(
                            &mut open_trace,
                            &mut open_worker,
                            &mut transcript_scroll,
                        ) => {}
                    KeyCode::Esc => break,
                    KeyCode::Tab => {
                        pane_open = !pane_open;
                        if pane_open {
                            commands_open = false;
                            info_open = false;
                        }
                    }
                    KeyCode::Up => {
                        if pane_open {
                            focus = focus.saturating_sub(1);
                        } else if input.is_empty() {
                            let previous = open_trace;
                            select_trace_turn(
                                &mut open_trace,
                                &transcript_view,
                                &mut transcript_scroll,
                                -1,
                            );
                            if open_trace != previous {
                                open_worker = None;
                            }
                        }
                    }
                    KeyCode::PageUp if !pane_open => {
                        let previous = open_trace;
                        page_trace_turn(
                            &mut open_trace,
                            &transcript_view,
                            &mut transcript_scroll,
                            -1,
                        );
                        if open_trace != previous {
                            open_worker = None;
                        }
                    }
                    KeyCode::Down => {
                        if pane_open {
                            if focus + 1 <= threads.len() {
                                focus += 1;
                            }
                        } else if input.is_empty() {
                            let previous = open_trace;
                            select_trace_turn(
                                &mut open_trace,
                                &transcript_view,
                                &mut transcript_scroll,
                                1,
                            );
                            if open_trace != previous {
                                open_worker = None;
                            }
                        }
                    }
                    KeyCode::PageDown if !pane_open => {
                        let previous = open_trace;
                        page_trace_turn(
                            &mut open_trace,
                            &transcript_view,
                            &mut transcript_scroll,
                            1,
                        );
                        if open_trace != previous {
                            open_worker = None;
                        }
                    }
                    KeyCode::End => {
                        close_trace_details(
                            &mut open_trace,
                            &mut open_worker,
                            &mut transcript_scroll,
                        );
                        transcript_scroll.end();
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
                        if pane_open
                            && matches!(pane_tab, PaneTab::Foreground | PaneTab::Agents)
                            && input.is_empty() =>
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
                    KeyCode::Char('S')
                        if pane_open
                            && matches!(pane_tab, PaneTab::Foreground | PaneTab::Agents)
                            && input.is_empty() =>
                    {
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
                                    reset_transcript(
                                        &mut transcript_scroll,
                                        &mut transcript_view,
                                        &mut transcript_cache,
                                        &mut open_trace,
                                        &mut turn_projection,
                                    );
                                    open_worker = None;
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
                        open_trace = None;
                        open_worker = None;
                        transcript_scroll.end();
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
                        pane_tab = match pane_tab {
                            PaneTab::Foreground => PaneTab::Foreground,
                            PaneTab::Agents => PaneTab::Foreground,
                            PaneTab::Scheduled => PaneTab::Agents,
                            PaneTab::Memory => PaneTab::Scheduled,
                        };
                        if pane_tab == PaneTab::Foreground && focus > 1 {
                            focus = 0;
                        }
                    }
                    KeyCode::Right if pane_open && input.is_empty() => {
                        pane_tab = match pane_tab {
                            PaneTab::Foreground => PaneTab::Agents,
                            PaneTab::Agents => PaneTab::Scheduled,
                            PaneTab::Scheduled | PaneTab::Memory => PaneTab::Memory,
                        };
                        if pane_tab == PaneTab::Agents
                            && focus < 2
                            && !pane_agent_ids(&agent_infos).is_empty()
                        {
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
                            transcript_scroll.scroll_up(8);
                        }
                        MouseEventKind::ScrollDown => {
                            transcript_scroll.scroll_down(
                                8,
                                transcript_view.total_height,
                                transcript_view.viewport,
                            );
                        }
                        MouseEventKind::Down(MouseButton::Left) => {
                            let size = terminal.size()?;
                            if m.row == size.height.saturating_sub(1) {
                                continue;
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
                                let tabs_x = pane.x.saturating_add(
                                    1 + WINDOW_LOGO_BUTTON.chars().count() as u16 + 1,
                                );
                                let orchestrators_width =
                                    ORCHESTRATORS_TAB_LABEL.chars().count() as u16;
                                let agents_x = tabs_x.saturating_add(orchestrators_width + 1);
                                let agents_width =
                                    format!(" AGENTS ({}) ", pane_agent_ids(&agent_infos).len())
                                        .chars()
                                        .count() as u16;
                                let scheduled_x = agents_x.saturating_add(agents_width + 1);
                                let scheduled_width =
                                    format!(" SCHEDULED ({}) ", scheduled_tasks.len())
                                        .chars()
                                        .count() as u16;
                                let memory_x = scheduled_x.saturating_add(scheduled_width + 1);
                                if m.row >= pane.y
                                    && m.row <= pane.y.saturating_add(1)
                                    && m.column >= tabs_x
                                    && m.column < inner_right
                                {
                                    pane_tab = if m.column < agents_x {
                                        PaneTab::Foreground
                                    } else if m.column < scheduled_x {
                                        PaneTab::Agents
                                    } else if m.column < memory_x {
                                        PaneTab::Scheduled
                                    } else {
                                        PaneTab::Memory
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
                                        PaneTab::Scheduled => {}
                                        PaneTab::Memory => {}
                                    }
                                    continue;
                                }
                            }
                            let (vy, vh) = *VIEW.lock().unwrap();
                            // Only map clicks inside the conversation area.
                            if m.row >= vy && m.row < vy + vh {
                                let row = (m.row - vy) as usize;
                                let hit = HITS.lock().unwrap().get(row).cloned().flatten();
                                if let Some(ClickTarget::TraceSummary(turn)) = hit {
                                    if let Some((_, thread)) = foreground_thread(&threads) {
                                        if let Some(turn_id) = turn_projection
                                            .cells
                                            .get(turn)
                                            .and_then(|cell| {
                                                thread.items[cell.prompt].turn.as_deref()
                                            })
                                            .and_then(|turn| turn.parse::<u64>().ok())
                                        {
                                            mark_ready_turn_seen(&mut threads, turn_id);
                                        }
                                    }
                                    toggle_trace(&mut open_trace, turn);
                                    open_worker = None;
                                    continue;
                                }
                                if let Some(ClickTarget::Worker(turn, worker)) = hit {
                                    toggle_worker(&mut open_worker, turn, worker);
                                    continue;
                                }
                                if let Some(ClickTarget::Item(ti, ii)) = hit {
                                    if let Some(thread) = threads.get_mut(ti) {
                                        if !thread.is_foreground && thread.collapsed {
                                            thread.collapsed = false;
                                            thread.touch();
                                            continue;
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
    let revision = threads[root].revision;
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
            threads[root]
                .metric_revisions
                .insert(turn.to_string(), revision);
        }
        (
            Actor::Foreground,
            AgentEvent::Usage {
                turn: Some(turn),
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..
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
            threads[root]
                .metric_revisions
                .insert(turn.to_string(), revision);
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
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (
            _,
            AgentEvent::MemoryRecalled {
                preference_count,
                history_count,
                ..
            },
        ) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let memory = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .memory;
            memory.recalled_preferences = memory.recalled_preferences.max(*preference_count);
            memory.recalled_history = memory.recalled_history.max(*history_count);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::MemoryMutation { result, .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let memory = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .memory;
            match result {
                MemoryMutationResult::Applied { kind, .. } => match kind {
                    MemoryMutationKind::Remember => memory.saved = memory.saved.saturating_add(1),
                    MemoryMutationKind::Forget => {
                        memory.forgotten = memory.forgotten.saturating_add(1)
                    }
                    MemoryMutationKind::Correct => {
                        memory.corrected = memory.corrected.saturating_add(1)
                    }
                },
                MemoryMutationResult::Rejected { .. } | MemoryMutationResult::Unavailable => {
                    memory.failed = memory.failed.saturating_add(1)
                }
                MemoryMutationResult::Ignored | MemoryMutationResult::AlreadyApplied { .. } => {}
            }
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderScheduled { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.scheduled = schedule.scheduled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ScheduledTaskCreated { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.tasks_scheduled = schedule.tasks_scheduled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderCancelled { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.cancelled = schedule.cancelled.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
        }
        (_, AgentEvent::ReminderFired { .. }) => {
            let Some(turn) = envelope.turn_id.as_ref() else {
                return;
            };
            let schedule = &mut threads[root]
                .metrics
                .entry(turn.clone())
                .or_default()
                .schedule;
            schedule.fired = schedule.fired.saturating_add(1);
            threads[root]
                .metric_revisions
                .insert(turn.clone(), revision);
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
        structure_revision: 0,
        items: Vec::new(),
        completed_turns: BTreeSet::new(),
        unread_turns: BTreeSet::new(),
        usage: HashMap::new(),
        metrics: HashMap::new(),
        metric_revisions: HashMap::new(),
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
    } else if !text.trim().is_empty() && !t.is_foreground {
        // Plain content glues onto whatever the thread was doing.
        t.add(ItemKind::System, text.to_string());
    }
}

fn accept_user_turn(thread: &mut Thread, text: &str, turn: Option<String>) {
    if let Some(index) = thread.items.iter().rposition(|item| {
        item.kind == ItemKind::User && item.turn.is_none() && item.text.trim() == text.trim()
    }) {
        thread.touch_structure();
        thread.items[index].turn = turn.clone();
        thread.items[index].revision = thread.revision;
        if let Some(pending_index) = thread.items[index + 1..]
            .iter_mut()
            .position(|item| item.kind == ItemKind::PendingReply && item.turn.is_none())
        {
            let pending = &mut thread.items[index + 1 + pending_index];
            pending.turn = turn;
            pending.revision = thread.revision;
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
            if let Some(index) = thread
                .items
                .iter()
                .rev()
                .position(|item| item.kind == ItemKind::PendingReply && item.turn == turn)
            {
                let index = thread.items.len() - 1 - index;
                thread.touch();
                let pending = &mut thread.items[index];
                pending.kind = ItemKind::Error;
                pending.text = format!("request timed out after {deadline_ms}ms");
                pending.revision = thread.revision;
            } else {
                thread.add_turn(
                    ItemKind::Error,
                    format!("request timed out after {deadline_ms}ms"),
                    turn,
                );
            }
            thread.streaming = false;
        }
        InteractionEvent::UserVisibleNotificationPublished { text } => {
            thread.add_turn(ItemKind::Reply, text, turn)
        }
    }
}

#[cfg(test)]
fn apply_agent_event(thread: &mut Thread, event: AgentEvent) {
    apply_correlated_agent_event(thread, event, None);
}

fn projected_turn(turn: Option<u64>, envelope_turn: Option<&str>) -> Option<String> {
    turn.map(|turn| turn.to_string())
        .or_else(|| envelope_turn.map(str::to_owned))
}

fn apply_correlated_agent_event(
    thread: &mut Thread,
    event: AgentEvent,
    envelope_turn: Option<&str>,
) {
    match event {
        AgentEvent::Status {
            turn,
            phase,
            message,
        } => {
            let turn = projected_turn(turn, envelope_turn);
            if matches!(phase.as_str(), "queued" | "working") {
                thread.update_pending_reply_status(turn.clone(), &phase, &message);
            }
            thread.add_turn(ItemKind::System, format!("[{phase}] {message}"), turn);
        }
        AgentEvent::ReplyDelta { turn, text } => {
            thread.add_reply_fragment(text, projected_turn(turn, envelope_turn), false);
        }
        AgentEvent::Reply {
            turn,
            text,
            final_reply: _,
        } => thread.finish_reply(text, projected_turn(turn, envelope_turn)),
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
            projected_turn(turn, envelope_turn),
        ),
        AgentEvent::Usage {
            turn: Some(turn),
            prompt_tokens,
            completion_tokens,
            total_tokens,
            ..
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
            let turn = envelope_turn.map(str::to_owned).or_else(|| {
                thread
                    .items
                    .iter()
                    .rev()
                    .find(|item| item.kind == ItemKind::Spawn && item.text.contains(&worker_id))
                    .and_then(|item| item.turn.clone())
            });
            thread.add_turn(
                ItemKind::SpawnResult,
                format!("worker {worker_id}: {objective}\n{result}"),
                turn,
            );
        }
        AgentEvent::WorkCandidate { .. } => {}
        AgentEvent::ArtifactRegistered { artifact } => thread.add_turn(
            ItemKind::System,
            format!(
                "artifact {} ({} bytes, {}): {}",
                artifact.path, artifact.size_bytes, artifact.kind, artifact.description
            ),
            None,
        ),
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
                envelope_turn.map(str::to_owned),
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
            let turn = projected_turn(turn, envelope_turn);
            thread.add_tool(format!("{name} {arguments}"), id, turn);
        }
        AgentEvent::ToolFinished { turn, id, output } => {
            thread.add_tool_result(id, output, projected_turn(turn, envelope_turn));
        }
        AgentEvent::ToolTelemetry {
            tool_name,
            duration_ms,
            success,
            truncated,
            bytes_out,
            error_code,
            ..
        } => {
            let outcome = if success { "complete" } else { "failed" };
            let truncation = if truncated { " · truncated" } else { "" };
            let error = error_code
                .map(|code| format!(" · {code}"))
                .unwrap_or_default();
            thread.add_turn(
                ItemKind::System,
                format!(
                    "[tool telemetry] {tool_name} {outcome} · {duration_ms}ms · {bytes_out} bytes{truncation}{error}"
                ),
                envelope_turn.map(str::to_owned),
            );
        }
        AgentEvent::ContextCompacted {
            epoch,
            retained_context_tokens,
            ..
        } => {
            thread.add_turn(
                ItemKind::System,
                format!(
                    "[context compacted] epoch {epoch} · {retained_context_tokens} tokens retained"
                ),
                envelope_turn.map(str::to_owned),
            );
        }
        AgentEvent::MemorySaved { .. }
        | AgentEvent::MemoryRecalled { .. }
        | AgentEvent::MemoryMutation { .. }
        | AgentEvent::ReminderScheduled { .. }
        | AgentEvent::ReminderCancelled { .. }
        | AgentEvent::ReminderFired { .. }
        | AgentEvent::ScheduledTaskCreated { .. } => {}
        AgentEvent::Error { turn, message } => {
            thread.add_turn(
                ItemKind::Error,
                message,
                projected_turn(turn, envelope_turn),
            );
        }
    }
}

fn apply_actor_event(
    thread: &mut Thread,
    mut event: AgentEvent,
    actor: &Actor,
    envelope_turn: Option<&str>,
) {
    if matches!(actor, Actor::Background) {
        match &mut event {
            AgentEvent::Reply { text, .. } => *text = format!("[Background] {text}"),
            AgentEvent::Status { message, .. } => *message = format!("[Background] {message}"),
            AgentEvent::ToolStarted { name, .. } => *name = format!("Background::{name}"),
            _ => {}
        }
    }
    apply_correlated_agent_event(thread, event, envelope_turn);
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
    let mut worker_ids: Vec<String> = agent_infos
        .keys()
        .filter(|id| id.as_str() != FOREGROUND_ID && id.as_str() != MEMORY_ID)
        .cloned()
        .collect();
    worker_ids.sort_by(|left, right| {
        let left = &agent_infos[left];
        let right = &agent_infos[right];
        agent_sort_rank(left)
            .cmp(&agent_sort_rank(right))
            .then_with(|| right.created_secs.cmp(&left.created_secs))
            .then_with(|| left.id.cmp(&right.id))
    });
    worker_ids
}

fn agent_sort_rank(info: &AgentInfo) -> u8 {
    match info.state {
        AgentState::Created | AgentState::Starting | AgentState::Running | AgentState::Staged => 0,
        AgentState::Waiting => 1,
        AgentState::Completed if info.retained => 2,
        AgentState::Failed | AgentState::Interrupted | AgentState::Terminated => 3,
        AgentState::Completed | AgentState::Released => 4,
    }
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

/// Copy the highlighted conversation cell, or the latest cell when none is selected.
fn yank_chat_cell(threads: &[Thread], selected: Option<usize>) {
    let Some(text) = selected_chat_cell_text(threads, selected) else {
        return;
    };
    // Never write status text to stderr while the alternate-screen TUI is active.
    let _ = copy_to_clipboard(&text);
}

fn selected_chat_cell_text(threads: &[Thread], selected: Option<usize>) -> Option<String> {
    let thread = threads.iter().find(|thread| thread.is_foreground)?;
    let cells = build_turn_cells(thread);
    let cell = cells.get(selected.unwrap_or_else(|| cells.len().saturating_sub(1)))?;
    let mut sections = Vec::new();
    for item in cell.items.iter().map(|index| &thread.items[*index]) {
        let label = match item.kind {
            ItemKind::User => names().user.as_str(),
            ItemKind::Reply => names().conversation.as_str(),
            _ => continue,
        };
        sections.push(format!("{label}:\n{}", item.text.trim()));
    }
    (!sections.is_empty()).then(|| sections.join("\n\n"))
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

fn reset_transcript(
    scroll: &mut TranscriptScroll,
    view: &mut TranscriptView,
    cache: &mut TurnLayoutCache,
    open_trace: &mut Option<usize>,
    projection: &mut TurnProjection,
) {
    *scroll = TranscriptScroll::default();
    *view = TranscriptView::default();
    cache.reset();
    *open_trace = None;
    projection.reset();
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

fn popup_title(label: &'static str) -> Title<'static> {
    Title::from(Line::from(vec![
        Span::styled(
            WINDOW_LOGO_BUTTON,
            Style::default().fg(Color::Black).bg(Color::White),
        ),
        Span::raw(" "),
        Span::styled(
            label,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ]))
    .alignment(Alignment::Left)
}

fn draw_command_palette(f: &mut Frame, area: Rect) {
    let popup = popup_rect(area, 68, 29);
    let block = Block::default()
        .title(popup_title(" HELP "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .style(Style::default().bg(Color::Rgb(12, 12, 15)));
    let commands = vec![
        Line::from(""),
        help_section("KEYBINDS"),
        Line::from(""),
        help_key("? / Ctrl+P", "toggle help"),
        help_key("Tab", "agents pane"),
        help_key("Ctrl+O", "toggle inline traces for the current turn"),
        help_key("y / Ctrl+Shift+C", "copy selected chat cell"),
        help_key("Shift+Enter", "insert newline"),
        help_key("Up / Down", "select a turn and expand its traces"),
        help_key("PageUp / PageDown", "page through selected traces"),
        help_key("End", "return to latest turn"),
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
        .title(popup_title(" INFO "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
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
        Line::from(""),
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

fn main_conversation_layout(
    thread: &Thread,
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
) -> CellLayout {
    let prompt = &thread.items[cell.prompt];
    if prompt.kind == ItemKind::Reply {
        return standalone_reply_layout(thread, cell, width, latest_timestamp);
    }
    let mut lines = Vec::new();
    let mut hits = Vec::new();
    let mut push = |line: Line<'static>, target: Hit| {
        lines.push(line);
        hits.push(target);
    };
    let is_latest = cell
        .items
        .iter()
        .any(|index| thread.items[*index].timestamp == latest_timestamp);
    let metadata_style = if is_latest {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let badges = turn_cell_badges(thread, cell);
    let response = turn_response(thread, cell);
    let turn_suffix = prompt
        .turn
        .as_deref()
        .map(|turn| format!("  {} {turn}", icon::TURN))
        .unwrap_or_default();
    let trailing = format!("{turn_suffix}  [{}]", timestamp_label(prompt.timestamp));
    let label = format!(" {} ", names().user);
    let mut header = vec![Span::styled(
        label,
        Style::default()
            .fg(Color::Black)
            .bg(name_block_background(Color::Gray))
            .add_modifier(Modifier::BOLD),
    )];
    let padding = (width as usize)
        .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
        .max(1);
    header.push(Span::raw(" ".repeat(padding)));
    header.push(Span::styled(trailing, metadata_style));
    push(Line::from(header), None);
    push(Line::raw(""), None);
    let body_width = width.saturating_sub(4).min(92) as usize;
    let prompt_color = if prompt.timestamp == latest_timestamp {
        Color::White
    } else {
        Color::Gray
    };
    for line in markdown_body_lines(&prompt.text, body_width, prompt_color) {
        push(line, None);
    }
    push(Line::raw(""), None);

    if let Some(response) = response {
        let response_complete = response.kind == ItemKind::Reply
            && response
                .turn
                .as_deref()
                .is_none_or(|turn| thread.completed_turns.contains(turn));
        if !response_complete {
            push(
                Line::from(Span::styled(
                    format!(" {} ", names().conversation),
                    Style::default()
                        .fg(Color::Black)
                        .bg(name_block_background(Color::Green))
                        .add_modifier(Modifier::BOLD),
                )),
                None,
            );
            push(Line::raw(""), None);
            let pending = if response.text.trim().is_empty() && active {
                pending_reply_activity(activity, response.turn.is_some())
            } else {
                pending_reply_activity(&response.text, response.turn.is_some())
            };
            push(
                Line::from(vec![
                    Span::styled("    󰔟 ", Style::default().fg(Color::Yellow)),
                    Span::styled(
                        pending,
                        Style::default()
                            .fg(Color::Gray)
                            .add_modifier(Modifier::ITALIC),
                    ),
                ]),
                None,
            );
            if let Some(progress) = turn_worker_progress(thread, cell) {
                push(Line::raw(""), None);
                push(
                    Line::from(Span::styled(
                        format!("    {progress}"),
                        Style::default().fg(Color::Gray),
                    )),
                    None,
                );
            }
        } else {
            let response_turn_suffix = response
                .turn
                .as_deref()
                .map(|turn| format!("  {} {turn}", icon::TURN))
                .unwrap_or_default();
            let mut header = vec![Span::styled(
                format!(" {} ", names().conversation),
                Style::default()
                    .fg(Color::Black)
                    .bg(name_block_background(Color::Green))
                    .add_modifier(Modifier::BOLD),
            )];
            let trailing = format!(
                "{response_turn_suffix}  [{}]",
                timestamp_label(response.timestamp)
            );
            let padding = (width as usize)
                .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
                .max(1);
            header.push(Span::raw(" ".repeat(padding)));
            header.push(Span::styled(trailing, metadata_style));
            push(Line::from(header), None);
            let metadata = badges.clone();
            if !metadata.is_empty() {
                push(Line::raw(""), None);
                for line in wrap_text(&metadata, body_width) {
                    push(
                        Line::from(Span::styled(
                            format!("    {line}"),
                            Style::default().fg(Color::Gray),
                        )),
                        None,
                    );
                }
            }
            push(Line::raw(""), None);
            let response_color = if response.timestamp == latest_timestamp {
                Color::White
            } else {
                Color::Gray
            };
            for line in markdown_body_lines(
                &sanitize_reply_text(&response.text),
                body_width,
                response_color,
            ) {
                push(line, None);
            }
        }
    }
    push(Line::raw(""), None);
    CellLayout { lines, hits }
}

fn standalone_reply_layout(
    thread: &Thread,
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
) -> CellLayout {
    let response = &thread.items[cell.prompt];
    let metadata_style = if response.timestamp == latest_timestamp {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let trailing = format!("  [{}]", timestamp_label(response.timestamp));
    let mut header = vec![Span::styled(
        format!(" {} ", names().conversation),
        Style::default()
            .fg(Color::Black)
            .bg(name_block_background(Color::Green))
            .add_modifier(Modifier::BOLD),
    )];
    if response.turn.is_some() {
        header.push(Span::styled(
            "  RESTORED",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        ));
    }
    let padding = (width as usize)
        .saturating_sub(Line::from(header.clone()).width() + Line::raw(&trailing).width())
        .max(1);
    header.push(Span::raw(" ".repeat(padding)));
    header.push(Span::styled(trailing, metadata_style));
    let mut lines = vec![Line::from(header), Line::raw("")];
    let mut hits = vec![None, None];
    let body_width = width.saturating_sub(4).min(92) as usize;
    let color = if response.timestamp == latest_timestamp {
        Color::White
    } else {
        Color::Gray
    };
    for line in markdown_body_lines(&sanitize_reply_text(&response.text), body_width, color) {
        lines.push(line);
        hits.push(None);
    }
    lines.push(Line::raw(""));
    hits.push(None);
    CellLayout { lines, hits }
}

fn turn_cell_layout(
    thread_index: usize,
    turn_index: usize,
    threads: &[Thread],
    cell: &TurnCell,
    width: u16,
    latest_timestamp: u64,
    active: bool,
    activity: &str,
    open: bool,
    open_worker: Option<&str>,
) -> CellLayout {
    let thread = &threads[thread_index];
    let content_width = width.saturating_sub(if open { 2 } else { 0 });
    let mut layout = main_conversation_layout(
        thread,
        cell,
        content_width,
        latest_timestamp,
        active,
        activity,
    );
    for hit in &mut layout.hits {
        *hit = Some(ClickTarget::TraceSummary(turn_index));
    }
    if open {
        for line in &mut layout.lines {
            line.style = line.style.bg(Color::Rgb(30, 32, 36));
        }
    }
    let mut diagnostic_items = cell
        .items
        .iter()
        .copied()
        .filter(|index| {
            !matches!(
                thread.items[*index].kind,
                ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
            )
        })
        .map(|index| (thread_index, index))
        .collect::<Vec<_>>();
    let turn = thread.items[cell.prompt].turn.as_deref();
    if let Some(turn) = turn {
        for (source_thread, worker) in threads
            .iter()
            .enumerate()
            .filter(|(_, worker)| !worker.is_foreground)
        {
            diagnostic_items.extend(
                worker
                    .items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| item.turn.as_deref() == Some(turn))
                    .map(|(index, _)| (source_thread, index)),
            );
        }
    }
    diagnostic_items
        .sort_by_key(|(source_thread, index)| threads[*source_thread].items[*index].timestamp);
    let tools = diagnostic_items
        .iter()
        .filter(|(source_thread, index)| {
            threads[*source_thread].items[*index].kind == ItemKind::Tool
        })
        .count();
    if diagnostic_items.is_empty() {
        return layout;
    }
    if !open {
        return layout;
    }
    let mut worker_traces = Vec::<WorkerTrace>::new();
    let mut model_items = Vec::new();
    let mut orchestration_items = Vec::new();
    for &(source_thread, index) in &diagnostic_items {
        let source = &threads[source_thread];
        let item = &source.items[index];
        if source_thread != thread_index {
            let worker =
                ensure_worker_trace(&mut worker_traces, &source.id, source.task.as_deref());
            worker.items.push((source_thread, index));
            continue;
        }
        match item.kind {
            ItemKind::Spawn => {
                if let Some((id, objective)) = worker_record(&item.text) {
                    ensure_worker_trace(&mut worker_traces, id, objective);
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::SpawnResult => {
                if let Some((id, objective)) = worker_record(&item.text) {
                    let worker = ensure_worker_trace(&mut worker_traces, id, objective);
                    worker.items.push((source_thread, index));
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::Error if item.text.starts_with("worker ") => {
                let (id, _) = worker_record(&item.text).expect("checked worker record");
                let worker = ensure_worker_trace(&mut worker_traces, id, None);
                worker.items.push((source_thread, index));
            }
            ItemKind::Error if item.text.starts_with("work ") => {
                let worker = worker_record(&item.text)
                    .and_then(|(id, _)| worker_traces.iter_mut().find(|worker| worker.id == id));
                if let Some(worker) = worker {
                    worker.items.push((source_thread, index));
                } else {
                    orchestration_items.push((source_thread, index));
                }
            }
            ItemKind::Error if item.text.to_ascii_lowercase().contains("review") => {
                orchestration_items.push((source_thread, index));
            }
            ItemKind::System
                if item.text.starts_with("work ")
                    || item.text.starts_with("worker release requested:") =>
            {
                orchestration_items.push((source_thread, index));
            }
            _ => model_items.push((source_thread, index)),
        }
    }
    let workers = worker_traces.len();
    let summary = trace_count_summary(diagnostic_items.len(), tools, workers);
    layout.lines.push(Line::from(Span::styled(
        summary,
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
    )));
    layout
        .hits
        .push(Some(ClickTarget::TraceSummary(turn_index)));
    if !model_items.is_empty() {
        push_trace_heading(&mut layout, icon::MODEL, "Model", "  ");
        let (timeline, remaining_model_items) = compact_model_timeline(&model_items, threads);
        let compacted = timeline.is_some();
        if let Some(timeline) = timeline {
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("    {} ", icon::DURATION),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(timeline, Style::default().fg(Color::Gray)),
            ]));
            layout.hits.push(None);
        }
        let mut previous_status = None;
        for (source_thread, index) in remaining_model_items {
            let item = &threads[source_thread].items[index];
            if item.kind == ItemKind::System {
                for line in item
                    .text
                    .lines()
                    .filter(|line| !compacted_model_line(line, compacted))
                {
                    let status = trace_summary(line);
                    if previous_status.as_deref() == Some(status.as_str()) {
                        continue;
                    }
                    previous_status = Some(status.clone());
                    layout.lines.push(Line::from(vec![
                        Span::styled("    · ", Style::default().fg(Color::DarkGray)),
                        Span::styled(status, Style::default().fg(Color::DarkGray)),
                    ]));
                    layout.hits.push(None);
                }
                continue;
            }
            previous_status = None;
            push_trace_item(
                &mut layout,
                threads,
                source_thread,
                index,
                content_width,
                "    ",
                None,
            );
        }
    }
    if !orchestration_items.is_empty() || !worker_traces.is_empty() {
        push_trace_heading(&mut layout, icon::AGENT, "Agents", "  ");
        for (source_thread, index) in orchestration_items {
            push_trace_item(
                &mut layout,
                threads,
                source_thread,
                index,
                content_width,
                "    ",
                None,
            );
        }
        for worker in worker_traces {
            let objective = worker.objective.as_deref().unwrap_or("Worker task");
            let expanded = open_worker == Some(worker.id.as_str());
            let (state, color) = worker_trace_state(&worker, threads);
            let state_icon = match state {
                "complete" => icon::SUCCESS,
                "running" => icon::RUNNING,
                _ => icon::FAILURE,
            };
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!(
                        "    {} {} ",
                        if expanded {
                            icon::EXPANDED
                        } else {
                            icon::COLLAPSED
                        },
                        icon::AGENT
                    ),
                    Style::default().fg(Color::Green),
                ),
                Span::styled(short_preview(objective), Style::default().fg(Color::Gray)),
                Span::styled(
                    format!(" · {state_icon} {state}"),
                    Style::default().fg(color),
                ),
                Span::styled(
                    format!(" · {}", worker.id.chars().take(8).collect::<String>()),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            layout
                .hits
                .push(Some(ClickTarget::Worker(turn_index, worker.id.clone())));
            if expanded {
                for (source_thread, index) in worker.items {
                    if matches!(
                        threads[source_thread].items[index].kind,
                        ItemKind::SpawnResult
                    ) {
                        continue;
                    }
                    push_trace_item(
                        &mut layout,
                        threads,
                        source_thread,
                        index,
                        content_width,
                        "      ",
                        Some(&worker.id),
                    );
                }
            }
        }
    }
    layout.lines.push(Line::raw(""));
    layout.hits.push(None);
    add_selected_rail(&mut layout);
    layout
}

fn add_selected_rail(layout: &mut CellLayout) {
    for line in &mut layout.lines {
        line.spans
            .insert(0, Span::styled("│ ", Style::default().fg(Color::DarkGray)));
    }
}

fn timing_stage(text: &str) -> Option<(&str, u64)> {
    let timing = text.strip_prefix("[timing] ")?;
    let (stage, elapsed) = timing.rsplit_once(' ')?;
    Some((stage, elapsed.strip_suffix("ms")?.parse().ok()?))
}

fn compact_timing_stage(stage: &str) -> bool {
    stage == "first_visible"
        || stage.contains("first_token")
        || stage == "completed"
        || stage.contains("routed")
        || stage.ends_with("_started")
        || stage.ends_with("_completed")
}

fn compacted_model_line(line: &str, compacted: bool) -> bool {
    if timing_stage(line).is_some_and(|(stage, _)| compact_timing_stage(stage)) {
        return true;
    }
    if !compacted {
        return false;
    }
    let lower = line.to_ascii_lowercase();
    lower.starts_with("[ready]")
        || lower.starts_with("[working]")
        || lower.contains("publication")
        || lower.contains("publish")
        || lower.contains("commit")
}

fn compact_model_timeline(
    items: &[(usize, usize)],
    threads: &[Thread],
) -> (Option<String>, Vec<(usize, usize)>) {
    let mut routed = None;
    let mut first = None;
    let mut completed = None;
    for &(thread, index) in items {
        for line in threads[thread].items[index].text.lines() {
            let Some((stage, elapsed)) = timing_stage(line) else {
                continue;
            };
            if stage == "first_visible" || stage.contains("first_token") {
                first = Some(elapsed);
            } else if stage == "completed" {
                completed = Some(elapsed);
            } else if stage.contains("routed") || stage.ends_with("_started") {
                routed.get_or_insert(elapsed);
            } else if stage.ends_with("_completed") && completed.is_none() {
                completed = Some(elapsed);
            }
        }
    }
    let mut points = Vec::new();
    if let Some(elapsed) = routed {
        points.push(format!("routed {}", human_millis(elapsed)));
    }
    if let Some(elapsed) = first {
        points.push(format!("first token {}", human_millis(elapsed)));
    }
    if let Some(elapsed) = completed {
        points.push(format!("completed {}", human_millis(elapsed)));
    }
    let has_completion = completed.is_some();
    let remaining = items
        .iter()
        .copied()
        .filter(|(thread, index)| {
            let item = &threads[*thread].items[*index];
            if has_completion && item.kind == ItemKind::System {
                return item
                    .text
                    .lines()
                    .any(|line| !compacted_model_line(line, true));
            }
            if item.kind == ItemKind::System
                && item.text.lines().all(|line| {
                    timing_stage(line).is_some_and(|(stage, _)| compact_timing_stage(stage))
                })
            {
                return false;
            }
            true
        })
        .collect();
    ((!points.is_empty()).then(|| points.join(" -> ")), remaining)
}

struct WorkerTrace {
    id: String,
    objective: Option<String>,
    items: Vec<(usize, usize)>,
}

fn worker_record(text: &str) -> Option<(&str, Option<&str>)> {
    if let Some(id) = text.strip_prefix("spawned worker ") {
        return Some((id.trim(), None));
    }
    let record = text
        .strip_prefix("worker ")
        .or_else(|| text.strip_prefix("work "))?;
    let (id, detail) = record.split_once(": ").unwrap_or((record, ""));
    let objective = detail
        .lines()
        .next()
        .map(str::trim)
        .filter(|text| !text.is_empty());
    Some((id.trim(), objective))
}

fn worker_error_detail(text: &str) -> &str {
    let Some(record) = text
        .strip_prefix("worker ")
        .or_else(|| text.strip_prefix("work "))
    else {
        return text;
    };
    let detail = record
        .split_once(": ")
        .map(|(_, detail)| detail)
        .unwrap_or(record);
    detail
        .split_once('\n')
        .map(|(_, error)| error)
        .unwrap_or(detail)
}

fn worker_error_summary(text: &str, worker_id: Option<&str>) -> String {
    let detail = worker_error_detail(text);
    let first = detail.lines().next().unwrap_or(detail).trim();
    let summary = if first.to_ascii_lowercase().contains("review failed") {
        "review failed"
    } else {
        first
    };
    worker_id
        .map(|id| summary.replace(id, "worker"))
        .unwrap_or_else(|| summary.to_string())
}

fn elide_work_id(text: &str) -> String {
    text.strip_prefix("work ")
        .and_then(|record| record.split_once(": "))
        .map(|(_, detail)| format!("work · {detail}"))
        .unwrap_or_else(|| text.to_string())
}

fn ensure_worker_trace<'a>(
    workers: &'a mut Vec<WorkerTrace>,
    id: &str,
    objective: Option<&str>,
) -> &'a mut WorkerTrace {
    if let Some(index) = workers.iter().position(|worker| worker.id == id) {
        if workers[index].objective.is_none() {
            workers[index].objective = objective.map(str::to_owned);
        }
        return &mut workers[index];
    }
    workers.push(WorkerTrace {
        id: id.to_owned(),
        objective: objective.map(str::to_owned),
        items: Vec::new(),
    });
    workers.last_mut().expect("worker was just inserted")
}

fn trace_count_summary(events: usize, tools: usize, agents: usize) -> String {
    let mut counts = Vec::new();
    if events != tools && events != agents {
        counts.push(format!(
            "{events} event{}",
            if events == 1 { "" } else { "s" }
        ));
    }
    if tools > 0 {
        counts.push(format!("{tools} tool{}", if tools == 1 { "" } else { "s" }));
    }
    if agents > 0 {
        counts.push(agent_count(agents));
    }
    format!("  trace · {}", counts.join(" · "))
}

fn push_trace_heading(layout: &mut CellLayout, icon: &str, label: &str, indent: &str) {
    layout.lines.push(Line::from(Span::styled(
        format!("{indent}{icon} {label}"),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    layout.hits.push(None);
}

fn worker_trace_state(worker: &WorkerTrace, threads: &[Thread]) -> (&'static str, Color) {
    let errors = worker.items.iter().filter_map(|(thread, item)| {
        let item = &threads[*thread].items[*item];
        (item.kind == ItemKind::Error).then_some(item.text.to_ascii_lowercase())
    });
    for error in errors {
        if error.contains("review") {
            return ("review failed", Color::Red);
        }
        return ("failed", Color::Red);
    }
    if worker.items.iter().any(|(thread, item)| {
        matches!(
            threads[*thread].items[*item].kind,
            ItemKind::SpawnResult | ItemKind::Reply
        )
    }) {
        ("complete", Color::Green)
    } else {
        ("running", Color::Yellow)
    }
}

fn push_trace_item(
    layout: &mut CellLayout,
    threads: &[Thread],
    source_thread: usize,
    index: usize,
    width: u16,
    indent: &str,
    worker_id: Option<&str>,
) {
    let source = &threads[source_thread];
    let item = &source.items[index];
    let hit = Some(ClickTarget::Item(source_thread, index));
    match item.kind {
        ItemKind::Tool => {
            let running = item.output.is_none();
            let (tool_name, arguments) = tool_parts(&item.text);
            let display_arguments = worker_id
                .map(|id| arguments.replace(id, "worker"))
                .unwrap_or_else(|| arguments.clone());
            let badge = if running {
                format!(" {} running", spinner_glyph())
            } else {
                " ✓".to_string()
            };
            let badge_style =
                Style::default().fg(if running { Color::Yellow } else { Color::Green });
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("{indent}{} ", tool_icon(&tool_name)),
                    Style::default().fg(Color::Blue),
                ),
                Span::styled(
                    format!("[{}] ", timestamp_label(item.timestamp)),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(tool_name, Style::default().fg(Color::Green)),
                Span::styled(badge, badge_style),
                if item.hidden {
                    Span::styled(
                        format!(" · {} · click to expand", short_preview(&display_arguments)),
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    )
                } else {
                    Span::raw("")
                },
            ]));
            layout.hits.push(hit.clone());
            if !item.hidden {
                if let Some(target) = api_target(&display_arguments) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    request · {target}"),
                        Style::default().fg(Color::Cyan),
                    )));
                    layout.hits.push(hit.clone());
                }
                for line in wrap_text(&display_arguments, width.saturating_sub(8) as usize) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    {line}"),
                        Style::default().fg(Color::DarkGray),
                    )));
                    layout.hits.push(hit.clone());
                }
                if let Some(output) = &item.output {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}    output"),
                        Style::default()
                            .fg(Color::Rgb(255, 140, 0))
                            .add_modifier(Modifier::BOLD),
                    )));
                    layout.hits.push(hit.clone());
                    for source in output.lines() {
                        for line in wrap_text(source, width.saturating_sub(10) as usize) {
                            layout.lines.push(Line::from(Span::styled(
                                format!("{indent}      {line}"),
                                Style::default()
                                    .fg(Color::Rgb(220, 223, 228))
                                    .bg(Color::Rgb(30, 32, 36)),
                            )));
                            layout.hits.push(hit.clone());
                        }
                    }
                }
            }
        }
        ItemKind::ToolResult => {
            let summary = short_preview(&item.text);
            layout.lines.push(Line::from(vec![
                Span::styled(
                    format!("{indent}  > "),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("output · {summary} · click to expand"),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC),
                ),
            ]));
            layout.hits.push(hit.clone());
            if !item.hidden {
                for source in item.text.lines() {
                    for line in wrap_text(source, width.saturating_sub(8) as usize) {
                        layout.lines.push(Line::from(Span::styled(
                            format!("{indent}  {line}"),
                            Style::default()
                                .fg(Color::Rgb(220, 223, 228))
                                .bg(Color::Rgb(30, 32, 36)),
                        )));
                        layout.hits.push(hit.clone());
                    }
                }
            }
        }
        ItemKind::Spawn => {
            layout.lines.push(Line::from(vec![
                Span::styled(format!("{indent}󰚩 "), Style::default().fg(Color::Green)),
                Span::styled(
                    format!(
                        "[{}] {}",
                        timestamp_label(item.timestamp),
                        spawn_display_label(&names().conversation, &item.text)
                    ),
                    Style::default().fg(Color::Green),
                ),
            ]));
            layout.hits.push(hit.clone());
        }
        ItemKind::SpawnResult => {
            layout.lines.push(Line::from(vec![
                Span::styled(format!("{indent}󰄬 "), Style::default().fg(Color::Green)),
                Span::styled(
                    format!(
                        "[{}] task completed · {}",
                        timestamp_label(item.timestamp),
                        short_preview(worker_objective(&item.text))
                    ),
                    Style::default().fg(Color::Green),
                ),
            ]));
            layout.hits.push(hit.clone());
        }
        ItemKind::Error => {
            let detail = worker_error_summary(&item.text, worker_id);
            for source in detail.lines() {
                let mut row = vec![
                    Span::styled(
                        format!("{indent}{} ", icon::FAILURE),
                        Style::default().fg(Color::Red),
                    ),
                    Span::styled(
                        format!("[{}] ", timestamp_label(item.timestamp)),
                        Style::default().fg(Color::DarkGray),
                    ),
                ];
                row.extend(styled_markdown(source, Style::default().fg(Color::Red)));
                layout.lines.push(Line::from(row));
                layout.hits.push(hit.clone());
            }
        }
        ItemKind::System => {
            for source in item.text.lines() {
                let summary = trace_summary(source);
                let summary = worker_id
                    .map(|id| summary.replace(id, "worker"))
                    .unwrap_or(summary);
                let summary = elide_work_id(&summary);
                layout.lines.push(Line::from(vec![
                    Span::styled(format!("{indent}· "), Style::default().fg(Color::DarkGray)),
                    Span::styled(
                        format!("[{}] ", timestamp_label(item.timestamp)),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(summary, Style::default().fg(Color::DarkGray)),
                ]));
                layout.hits.push(hit.clone());
            }
        }
        ItemKind::Reply if !source.is_foreground => {
            for line in item.text.lines() {
                let line = worker_id
                    .map(|id| line.replace(id, "worker"))
                    .unwrap_or_else(|| line.to_string());
                for line in wrap_text(&line, width.saturating_sub(8) as usize) {
                    layout.lines.push(Line::from(Span::styled(
                        format!("{indent}  {line}"),
                        Style::default().fg(Color::Gray),
                    )));
                    layout.hits.push(hit.clone());
                }
            }
        }
        ItemKind::User | ItemKind::PendingReply | ItemKind::Reply => {}
    }
}

fn draw_conversation(
    f: &mut Frame,
    area: Rect,
    threads: &[Thread],
    foreground_busy: bool,
    foreground_activity: &str,
    scroll: &mut TranscriptScroll,
    cache: &mut TurnLayoutCache,
    view: &mut TranscriptView,
    open_trace: Option<usize>,
    open_worker: Option<&(usize, String)>,
    projection: &mut TurnProjection,
) {
    let Some((thread_index, thread)) = foreground_thread(threads) else {
        return;
    };
    projection.update(thread);
    let cells = &projection.cells;
    cache.prepare(area.width, cells, thread.structure_revision);
    if cells.is_empty() || area.height == 0 {
        if let Ok(mut guard) = HITS.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = VIEW.lock() {
            *guard = (area.y, area.height);
        }
        *view = TranscriptView {
            viewport: area.height as usize,
            ..TranscriptView::default()
        };
        return;
    }
    let latest = cells.len() - 1;
    let latest_timestamp = latest_conversation_timestamp(thread, &cells[latest]);
    let worker_revisions = worker_turn_revisions(threads);
    let mut heights = Vec::with_capacity(cells.len());
    let mut starts = Vec::with_capacity(cells.len());
    let mut total_height = 0usize;
    for (index, cell) in cells.iter().enumerate() {
        starts.push(total_height);
        let open = open_trace == Some(index);
        let selected_worker = open_worker
            .filter(|(turn, _)| *turn == index)
            .map(|(_, worker)| worker.as_str());
        let is_latest = index == latest;
        let mut revision = cell_revision(thread, cell);
        if open {
            revision.worker = thread.items[cell.prompt]
                .turn
                .as_deref()
                .and_then(|turn| worker_revisions.get(turn))
                .copied()
                .unwrap_or(0);
            if let Some(worker) = selected_worker {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                worker.hash(&mut hasher);
                revision.worker ^= hasher.finish();
            }
        }
        let variant = u8::from(is_latest)
            | (u8::from(open) << 1)
            | (u8::from(selected_worker.is_some()) << 2);
        let height = cache
            .layout(cell_key(cell), revision, variant, || {
                turn_cell_layout(
                    thread_index,
                    index,
                    threads,
                    cell,
                    area.width,
                    latest_timestamp,
                    is_latest && foreground_busy,
                    foreground_activity,
                    open,
                    selected_worker,
                )
            })
            .lines
            .len();
        heights.push(height);
        total_height = total_height.saturating_add(height);
    }
    scroll.sync(total_height, area.height as usize, thread.revision);
    let show_activity = should_show_activity(scroll, total_height, area.height as usize);
    let viewport = transcript_content_height(area.height, show_activity);
    scroll.sync(total_height, viewport, thread.revision);
    let top = scroll.top;
    let bottom = top.saturating_add(viewport);
    let mut visible_lines = Vec::new();
    let mut visible_hits = Vec::new();
    let mut anchor_turn = None;
    for (index, cell) in cells.iter().enumerate() {
        let start = starts[index];
        let end = start.saturating_add(heights[index]);
        if end > top && start < bottom {
            anchor_turn.get_or_insert(index);
            let open = open_trace == Some(index);
            let selected_worker = open_worker
                .filter(|(turn, _)| *turn == index)
                .map(|(_, worker)| worker.as_str());
            let is_latest = index == latest;
            let mut revision = cell_revision(thread, cell);
            if open {
                revision.worker = thread.items[cell.prompt]
                    .turn
                    .as_deref()
                    .and_then(|turn| worker_revisions.get(turn))
                    .copied()
                    .unwrap_or(0);
                if let Some(worker) = selected_worker {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    worker.hash(&mut hasher);
                    revision.worker ^= hasher.finish();
                }
            }
            let variant = u8::from(is_latest)
                | (u8::from(open) << 1)
                | (u8::from(selected_worker.is_some()) << 2);
            let layout = cache.layout(cell_key(cell), revision, variant, || {
                unreachable!("height pass populated turn layout")
            });
            let skip = top.saturating_sub(start);
            let take = bottom.min(end).saturating_sub(start + skip);
            visible_lines.extend(layout.lines.iter().skip(skip).take(take).cloned());
            visible_hits.extend(layout.hits.iter().skip(skip).take(take).cloned());
        }
        if end >= bottom {
            break;
        }
    }
    if show_activity {
        visible_lines.push(Line::from(Span::styled(
            "  new activity below",
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        )));
        visible_hits.push(None);
    }
    if let Ok(mut guard) = HITS.lock() {
        *guard = visible_hits;
    }
    if let Ok(mut guard) = VIEW.lock() {
        *guard = (area.y, area.height);
    }
    f.render_widget(Paragraph::new(visible_lines), area);
    *view = TranscriptView {
        total_height,
        viewport,
        turns: cells.len(),
        anchor_turn,
        starts,
        heights,
    };
}

#[cfg(any())]
fn draw_trace_conversation_legacy(
    f: &mut Frame,
    area: Rect,
    threads: &[Thread],
    focus: usize,
    scroll: usize,
    foreground_busy: bool,
    foreground_activity: &str,
) {
    let show_traces = true;
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

    let signature = conversation_signature(
        threads,
        area.width,
        show_traces,
        foreground_busy,
        foreground_activity,
    );
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
                                pending_reply_activity(&item.text, item.turn.is_some()),
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
                let metadata = badges;
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

    let signature = conversation_signature(
        threads,
        area.width,
        show_traces,
        foreground_busy,
        foreground_activity,
    );
    if let Ok(mut cache) = CONVERSATION_CACHE.lock() {
        *cache = Some(ConversationCache {
            signature,
            lines: lines.clone(),
            hits: hits.clone(),
        });
    }
    render_conversation_lines(f, area, lines, hits, scroll);
}

#[cfg(any())]
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

#[cfg(any())]
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

#[cfg(any())]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

fn agent_pane_status(info: &AgentInfo, reviewing: bool) -> (&'static str, Color) {
    if reviewing {
        return ("reviewing", Color::Yellow);
    }
    match info.state {
        AgentState::Created | AgentState::Starting => ("starting", Color::Yellow),
        AgentState::Running => ("running", Color::Cyan),
        AgentState::Waiting => ("waiting", Color::Yellow),
        AgentState::Staged => ("staged", Color::Yellow),
        AgentState::Completed if info.retained => ("idle · retained", Color::Green),
        AgentState::Completed => ("completed", Color::Green),
        AgentState::Failed => ("failed", Color::Red),
        AgentState::Interrupted => ("stopped", Color::DarkGray),
        AgentState::Terminated => ("killed", Color::DarkGray),
        AgentState::Released => ("released", Color::DarkGray),
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

#[allow(dead_code)]
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
    let full = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .and_then(|home| {
            path.strip_prefix(home)
                .ok()
                .map(std::path::Path::to_path_buf)
        })
        .map(|relative| format!("~/{}", relative.display()))
        .unwrap_or_else(|| path.display().to_string());
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
        AgentState::Released => "✓ released".into(),
    }
}

fn pending_reply_activity(activity: &str, accepted: bool) -> String {
    let activity = activity.trim();
    if activity.is_empty() {
        return if accepted {
            "Checking information...".into()
        } else {
            "Submitting...".into()
        };
    }
    if activity.ends_with(['.', '!', '?']) {
        activity.to_string()
    } else {
        format!("{activity}...")
    }
}

fn turn_cell_badges(thread: &Thread, cell: &TurnCell) -> String {
    let mut spawned = 0;
    let mut completed = 0;
    let mut failed = 0;
    for item in cell.items.iter().map(|index| &thread.items[*index]) {
        match item.kind {
            ItemKind::Spawn => spawned += 1,
            ItemKind::SpawnResult => completed += 1,
            ItemKind::Error if item.text.starts_with("work ") => failed += 1,
            _ => {}
        }
    }
    let (completed, failed) = bounded_worker_outcomes(spawned, completed, failed);
    let mut badges = Vec::new();
    if spawned > 0 {
        badges.push(format!("󰚩 {}", agent_count(spawned)));
    }
    if completed > 0 {
        badges.push(if spawned == 0 {
            format!("󰄬 {} complete", agent_count(completed))
        } else {
            format!("󰄬 {completed} complete")
        });
    }
    if failed > 0 {
        badges.push(format!("× {failed} failed"));
    }
    let metrics = thread.items[cell.prompt]
        .turn
        .as_deref()
        .and_then(|turn| thread.metrics.get(turn));
    if let Some(done) = metrics.and_then(|metrics| metrics.completed_ms) {
        badges.push(format!("󰅐 done {:.1}s", done as f64 / 1000.0));
    }
    if let Some(metrics) = metrics {
        if let Some(self_usage) = &metrics.self_usage {
            let total = metrics
                .worker_usage
                .values()
                .fold(self_usage.total, |sum, usage| {
                    sum.saturating_add(usage.total)
                });
            badges.push(format!("{} total {}", icon::TOKENS, format_count(total)));
        }
        badges.extend(memory_badges(&metrics.memory, false));
        badges.extend(schedule_badges(&metrics.schedule, false));
    }
    badges.join(" · ")
}

fn turn_worker_progress(thread: &Thread, cell: &TurnCell) -> Option<String> {
    let mut spawned = 0;
    let mut completed = 0;
    for item in cell.items.iter().map(|index| &thread.items[*index]) {
        match item.kind {
            ItemKind::Spawn => spawned += 1,
            ItemKind::SpawnResult => completed += 1,
            _ => {}
        }
    }
    let (completed, _) = bounded_worker_outcomes(spawned, completed, 0);
    (spawned > 0).then(|| {
        if completed == 0 {
            format!("󰚩 {} running", agent_count(spawned))
        } else if completed == spawned {
            format!("󰄬 {} complete", agent_count(completed))
        } else {
            format!("󰚩 {} · 󰄬 {completed} complete", agent_count(spawned))
        }
    })
}

#[cfg_attr(not(test), allow(dead_code))]
fn turn_badges(thread: &Thread, turn: Option<&str>, timestamp: u64, show_traces: bool) -> String {
    let previous_reply = thread
        .items
        .iter()
        .filter(|item| item.kind == ItemKind::Reply && item.timestamp < timestamp)
        .map(|item| item.timestamp)
        .max()
        .unwrap_or(0);
    let in_turn = |item: &&Item| match turn {
        Some(turn) => item.turn.as_deref() == Some(turn),
        None => {
            item.turn.is_none() && item.timestamp > previous_reply && item.timestamp <= timestamp
        }
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
    let failed = thread
        .items
        .iter()
        .filter(|item| {
            item.kind == ItemKind::Error && item.text.starts_with("work ") && in_turn(item)
        })
        .count();
    let (completed, failed) = bounded_worker_outcomes(spawned, completed, failed);
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
            format!("󰚩 {}", agent_count(spawned))
        });
    }
    let metrics = turn.and_then(|turn| thread.metrics.get(turn));
    let first = metrics.and_then(|metrics| metrics.first_visible_ms);
    let done = metrics.and_then(|metrics| metrics.completed_ms);
    if completed > 0 {
        badges.push(if show_traces {
            format!("󰄬 workers completed {completed}")
        } else if spawned == 0 {
            format!("󰄬 {} complete", agent_count(completed))
        } else {
            format!("󰄬 {completed} complete")
        });
    }
    if failed > 0 {
        badges.push(if show_traces {
            format!("× workers failed {failed}")
        } else {
            format!("× {failed} failed")
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
                    "{} foreground tokens {} (prompt {} + completion {})",
                    icon::TOKENS,
                    format_count(self_usage.total),
                    format_count(self_usage.prompt),
                    format_count(self_usage.completion)
                ));
            }
            badges.push(if show_traces {
                format!("{} aggregate tokens {}", icon::TOKENS, format_count(total))
            } else {
                format!("{} total {}", icon::TOKENS, format_count(total))
            });
        }
        badges.extend(memory_badges(&metrics.memory, show_traces));
        badges.extend(schedule_badges(&metrics.schedule, show_traces));
    } else if let Some(turn) = turn.and_then(|value| value.parse::<u64>().ok()) {
        if let Some((prompt, completion, total)) = thread.usage.get(&turn) {
            if show_traces {
                badges.push(format!(
                    "{} reported tokens {} (prompt {} + completion {})",
                    icon::TOKENS,
                    format_count(*total),
                    format_count(*prompt),
                    format_count(*completion)
                ));
            } else {
                badges.push(format!("{} total {}", icon::TOKENS, format_count(*total)));
            }
        }
    }
    badges.join(" · ")
}

fn memory_badges(memory: &MemoryTurnMetrics, detailed: bool) -> Vec<String> {
    let mut badges = Vec::new();
    if memory.saved > 0 {
        badges.push(if detailed {
            format!("memory saved {}", memory.saved)
        } else {
            "memory saved".into()
        });
    }
    if memory.forgotten > 0 {
        badges.push(if detailed {
            format!("memory forgotten {}", memory.forgotten)
        } else {
            "memory forgotten".into()
        });
    }
    if memory.corrected > 0 {
        badges.push(if detailed {
            format!("memory updated {}", memory.corrected)
        } else {
            "memory updated".into()
        });
    }
    let recalled = memory
        .recalled_preferences
        .saturating_add(memory.recalled_history);
    if recalled > 0 {
        badges.push(if detailed {
            format!(
                "memory recalled {} preferences + {} history",
                memory.recalled_preferences, memory.recalled_history
            )
        } else {
            format!("memory recalled {recalled}")
        });
    }
    if memory.failed > 0 {
        badges.push("memory unchanged".into());
    }
    badges
}

fn schedule_badges(schedule: &ScheduleTurnMetrics, detailed: bool) -> Vec<String> {
    let mut badges = Vec::new();
    if schedule.scheduled > 0 {
        badges.push(if detailed {
            format!("reminders scheduled {}", schedule.scheduled)
        } else {
            "reminder scheduled".into()
        });
    }
    if schedule.tasks_scheduled > 0 {
        badges.push(if detailed {
            format!("agent tasks scheduled {}", schedule.tasks_scheduled)
        } else {
            "agent task scheduled".into()
        });
    }
    if schedule.cancelled > 0 {
        badges.push(if detailed {
            format!("reminders cancelled {}", schedule.cancelled)
        } else {
            "reminder cancelled".into()
        });
    }
    if schedule.fired > 0 {
        badges.push(if detailed {
            format!("reminders fired {}", schedule.fired)
        } else {
            "reminder fired".into()
        });
    }
    badges
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

fn tool_icon(name: &str) -> &'static str {
    let name = name.to_ascii_lowercase();
    if name.contains("browser") || name.contains("http") || name.contains("fetch") {
        icon::BROWSER
    } else if name.contains("search") || name.contains("grep") || name.contains("glob") {
        icon::SEARCH
    } else if name.contains("file") || name.contains("read") || name.contains("write") {
        icon::FILE
    } else {
        icon::TOOL
    }
}

fn flattened_list_parts(line: &str) -> Option<Vec<&str>> {
    let parts = line.split(" - ").collect::<Vec<_>>();
    (parts.len() >= 3
        && parts[1..]
            .iter()
            .all(|part| part.trim().chars().count() >= 2))
    .then_some(parts)
}

fn markdown_body_lines(text: &str, width: usize, color: Color) -> Vec<Line<'static>> {
    let base = Style::default().fg(color);
    let mut lines = Vec::new();
    for source in text.split('\n') {
        if source.trim().is_empty() {
            lines.push(Line::raw(""));
            continue;
        }
        let trimmed = source.trim();
        let explicit = ["- ", "* ", "• "]
            .into_iter()
            .find_map(|marker| trimmed.strip_prefix(marker));
        let (lead, entries) = if let Some(entry) = explicit {
            (None, vec![entry])
        } else if let Some(parts) = flattened_list_parts(trimmed) {
            (Some(parts[0]), parts[1..].to_vec())
        } else {
            (Some(trimmed), Vec::new())
        };
        if let Some(lead) = lead.filter(|lead| !lead.trim().is_empty()) {
            for chunk in wrap_text(lead, width) {
                let mut row = vec![Span::raw("    ")];
                row.extend(styled_markdown(&chunk, base));
                lines.push(Line::from(row));
            }
        }
        for entry in entries {
            let chunks = wrap_text(entry.trim(), width.saturating_sub(2));
            for (index, chunk) in chunks.into_iter().enumerate() {
                let mut row = if index == 0 {
                    vec![
                        Span::raw("    "),
                        Span::styled(format!("{} ", icon::BULLET), base),
                    ]
                } else {
                    vec![Span::raw("      ")]
                };
                row.extend(styled_markdown(&chunk, base));
                lines.push(Line::from(row));
            }
        }
    }
    lines
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
        let prefix = if li == 0 { INPUT_PROMPT_MARKER } else { "  " };
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

/// Compact contextual guidance that stays subordinate to the conversation.
fn footer_mode_text(open_trace: Option<usize>, follow: bool) -> Option<String> {
    if open_trace.is_some() {
        Some(format!(
            "TRACE    ↑↓ select · {} Pg scroll · {} Esc close · {} help",
            icon::SCROLL,
            icon::CLOSE,
            icon::HELP
        ))
    } else if !follow {
        Some(format!(
            "HISTORY    ↑↓ select · {} End live · {} help",
            icon::LIVE,
            icon::HELP
        ))
    } else {
        None
    }
}

fn status_task_count(agent_infos: &HashMap<String, AgentInfo>) -> usize {
    agent_infos
        .values()
        .filter(|agent| {
            agent.id != FOREGROUND_ID
                && agent.id != MEMORY_ID
                && agent.id != BACKGROUND_ID
                && matches!(
                    agent.state,
                    AgentState::Starting | AgentState::Running | AgentState::Waiting
                )
        })
        .count()
}

fn status_task_label(count: usize) -> Option<String> {
    match count {
        0 => None,
        1 => Some("1 task".into()),
        count => Some(format!("{count} tasks")),
    }
}

fn draw_statusline(
    f: &mut Frame,
    area: Rect,
    daemon: Option<&DaemonInfo>,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &[Thread],
    open_trace: Option<usize>,
    follow: bool,
) {
    let (daemon_label, state_color) = match daemon {
        Some(info) if info.provider_ready => (None, Color::Green),
        Some(_) => (Some("no API key"), Color::Yellow),
        None => (Some("offline"), Color::Red),
    };
    if let Some(mode) = footer_mode_text(open_trace, follow) {
        let (label, controls) = mode.split_once("    ").unwrap_or((&mode, ""));
        let sections = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Min(1), Constraint::Length(2)])
            .split(area);
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(
                    format!(" {label} "),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(controls, Style::default().fg(Color::DarkGray)),
            ])),
            sections[0],
        );
        f.render_widget(
            Paragraph::new(Span::styled("● ", Style::default().fg(state_color)))
                .alignment(Alignment::Right),
            sections[1],
        );
        return;
    }

    let task = status_task_label(status_task_count(agent_infos));
    let mut right = Vec::new();
    let mut push_right = |span: Span<'static>| {
        if !right.is_empty() {
            right.push(Span::raw("     "));
        }
        right.push(span);
    };
    if let Some(task) = task {
        push_right(Span::styled(task, Style::default().fg(Color::DarkGray)));
    }
    if let Some(ready) = ready_earlier_turn(threads).map(ready_notice) {
        push_right(Span::styled(ready, Style::default().fg(Color::Green)));
    }
    if let Some(label) = daemon_label {
        push_right(Span::styled(label, Style::default().fg(Color::DarkGray)));
    }
    push_right(Span::styled(
        format!(
            "{WINDOW_LOGO}  v{}",
            daemon
                .map(|info| info.version.as_str())
                .unwrap_or(env!("CARGO_PKG_VERSION"))
        ),
        Style::default().fg(Color::Gray),
    ));
    right.push(Span::styled(" ● ", Style::default().fg(state_color)));
    let right_width = right
        .iter()
        .map(|span| span.content.chars().count())
        .sum::<usize>() as u16;
    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(1), Constraint::Length(right_width)])
        .split(area);
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " TACHYON ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(compact_current_dir(), Style::default().fg(Color::DarkGray)),
        ])),
        sections[0],
    );
    f.render_widget(
        Paragraph::new(Line::from(right)).alignment(Alignment::Right),
        sections[1],
    );
}

fn draw_agent_pane(
    f: &mut Frame,
    area: Rect,
    _threads: &[Thread],
    focus: usize,
    daemon: Option<&DaemonInfo>,
    daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
    scheduled_tasks: &[ScheduledTaskInfo],
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
        .filter(|id| id.as_str() != FOREGROUND_ID && id.as_str() != MEMORY_ID)
        .count();
    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                WINDOW_LOGO_BUTTON,
                Style::default().fg(Color::Black).bg(Color::White),
            ),
            Span::raw(" "),
            Span::styled(
                ORCHESTRATORS_TAB_LABEL,
                tab_style(tab == PaneTab::Foreground),
            ),
            Span::raw(" "),
            Span::styled(
                format!(" AGENTS ({worker_count}) "),
                tab_style(tab == PaneTab::Agents),
            ),
            Span::raw(" "),
            Span::styled(
                format!(" SCHEDULED ({}) ", scheduled_tasks.len()),
                tab_style(tab == PaneTab::Scheduled),
            ),
            Span::raw(" "),
            Span::styled(" MEMORY ", tab_style(tab == PaneTab::Memory)),
        ])),
        Rect {
            x: area.x.saturating_add(1),
            y: area.y,
            width: area.width.saturating_sub(2),
            height: 1,
        },
    );

    let header = Row::new(["NAME", "STATUS", "POLICY", "CAPACITY", "DURATION", "TASK"]).style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    );
    let widths = [
        Constraint::Length(17),
        Constraint::Length(16),
        Constraint::Length(19),
        Constraint::Length(15),
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
                Cell::from("manual stop"),
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
                        Cell::from("daemon stop"),
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
            if let Some(info) = agent_infos.get(MEMORY_ID) {
                rows.push(Row::new(memory_service_columns(info)));
            }
            let background = daemon.map(|info| &info.background);
            let pending_reviews = background.map_or(0, |info| info.pending_reviews.len());
            let coordinator_status = match background {
                Some(info) if info.online && pending_reviews > 0 => {
                    format!("reviewing ({pending_reviews})")
                }
                Some(info) if info.online => "idle".into(),
                Some(_) => "restarting".into(),
                None => "offline".into(),
            };
            let coordinator_remaining = background
                .and_then(|info| {
                    info.pending_reviews
                        .iter()
                        .map(|review| review.deadline_ms)
                        .min()
                })
                .map(|deadline| {
                    let remaining_ms = deadline.saturating_sub(now_seconds());
                    format!(
                        "review {}",
                        format_duration(Duration::from_millis(remaining_ms))
                    )
                })
                .unwrap_or_else(|| "daemon stop".into());
            rows.push(Row::new(vec![
                Cell::from("Background Coord."),
                Cell::from(coordinator_status),
                Cell::from("daemon · supervised"),
                Cell::from(coordinator_remaining),
                Cell::from("-"),
                Cell::from(format!(
                    "semantic review · gen {} · {worker_count} workers",
                    background.map_or(0, |info| info.generation)
                )),
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
                        let reviewing = daemon.is_some_and(|daemon| {
                            daemon
                                .background
                                .pending_reviews
                                .iter()
                                .any(|review| review.worker_id == info.id)
                        });
                        let (lifetime, remaining) = agent_lifetime(info);
                        let (status, status_color) = agent_pane_status(info, reviewing);
                        Row::new(vec![
                            Cell::from(truncate_text(&format!("ghost {id}"), 17)),
                            Cell::from(status).style(Style::default().fg(status_color)),
                            Cell::from(lifetime),
                            Cell::from(remaining),
                            Cell::from(agent_duration(info)),
                            Cell::from(truncate_text(
                                &format!("{} · {}", info.task_type, info.description),
                                48,
                            )),
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
        PaneTab::Scheduled => {
            if scheduled_tasks.is_empty() {
                vec![Row::new(vec![
                    Cell::from("No scheduled tasks"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("Schedule future work from the conversation"),
                ])
                .style(Style::default().fg(Color::DarkGray))]
            } else {
                scheduled_tasks
                    .iter()
                    .map(|schedule| Row::new(scheduled_task_columns(schedule)))
                    .collect()
            }
        }
        PaneTab::Memory => vec![
            Row::new(vec![
                Cell::from("User profile"),
                Cell::from("planned"),
                Cell::from("memories.redb"),
                Cell::from("typed read/write"),
                Cell::from("-"),
                Cell::from("Durable facts, preferences, and corrections"),
            ]),
            Row::new(vec![
                Cell::from("History"),
                Cell::from("planned"),
                Cell::from("history.redb"),
                Cell::from("typed read"),
                Cell::from("-"),
                Cell::from("Conversation and task activity"),
            ]),
            Row::new(vec![
                Cell::from("Runtime"),
                Cell::from("planned"),
                Cell::from("runtime.redb"),
                Cell::from("typed read"),
                Cell::from("-"),
                Cell::from("Schedules, workers, and runtime state"),
            ]),
        ],
    };
    f.render_widget(
        Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .style(Style::default().fg(Color::Gray).bg(Color::Rgb(18, 18, 22))),
        sections[0],
    );
    f.render_widget(
        Paragraph::new(match tab {
            PaneTab::Scheduled => {
                "left/right tabs · durable scheduled work · worker appears when execution starts"
            }
            PaneTab::Memory => {
                "left/right tabs · placeholder · inspection and modification controls planned"
            }
            PaneTab::Foreground | PaneTab::Agents => {
                "left/right tabs · click a row to focus · s stop · r restart · u resume · k kill"
            }
        })
        .alignment(Alignment::Center)
        .style(
            Style::default()
                .fg(Color::DarkGray)
                .bg(Color::Rgb(18, 18, 22)),
        ),
        sections[1],
    );
}

fn scheduled_task_columns(schedule: &ScheduledTaskInfo) -> [String; 6] {
    let status = match schedule.status {
        ScheduledTaskStatus::Pending => "pending",
        ScheduledTaskStatus::Running => "running",
        ScheduledTaskStatus::Completed => "completed",
        ScheduledTaskStatus::Failed => "failed",
        ScheduledTaskStatus::Cancelled => "cancelled",
    };
    let mode = match schedule.mode {
        ScheduledTaskMode::StartAt => "start at",
        ScheduledTaskMode::FinishBy => "finish by",
    };
    let remaining_ms = schedule.due_at_ms.saturating_sub(now_seconds());
    let deadline = if remaining_ms == 0 {
        "due now".into()
    } else {
        format!(
            "in {}",
            format_duration(Duration::from_millis(remaining_ms))
        )
    };
    [
        truncate_text(&format!("schedule t{}", schedule.turn), 17),
        status.into(),
        mode.into(),
        deadline,
        format_age(schedule.created_at_ms / 1_000),
        truncate_text(&schedule.objective, 48),
    ]
}

fn memory_service_columns(info: &AgentInfo) -> [String; 6] {
    [
        "Memory".into(),
        state_label(agent_activity_state(info.state)).into(),
        "context service".into(),
        "daemon stop".into(),
        format_age(info.created_secs),
        truncate_text(&info.description, 48),
    ]
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
        .title(
            Title::from(Span::styled(
                WINDOW_LOGO_BUTTON,
                Style::default().fg(Color::Black).bg(Color::White),
            ))
            .alignment(Alignment::Left),
        )
        .title(
            Title::from(Span::styled(
                " AGENTS ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ))
            .alignment(Alignment::Center),
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
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
        .filter(|id| id.as_str() != FOREGROUND_ID && id.as_str() != MEMORY_ID)
        .count();
    let active_worker_count = agent_infos
        .values()
        .filter(|info| {
            info.id != FOREGROUND_ID
                && info.id != MEMORY_ID
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

    fn agent_info(lifetime_class: LifetimeClass) -> AgentInfo {
        AgentInfo {
            id: "worker".into(),
            task: "inspect".into(),
            state: AgentState::Running,
            pid: None,
            workspace: String::new(),
            created_secs: 0,
            retained: true,
            lease_until_secs: None,
            session_id: "worker".into(),
            lifetime_class,
            purpose: "research".into(),
            owner: "daemon".into(),
            last_activity_secs: 0,
            checkpoint_available: false,
            turns_used: 2,
            turn_budget: Some(3),
            task_type: "research".into(),
            description: "inspect".into(),
            persistent: lifetime_class == LifetimeClass::Persistent,
            sandboxed: false,
            stage_until_secs: None,
            logical_task_id: None,
            origin_turn_id: None,
            parent_task_id: None,
            tool_call_id: None,
        }
    }

    #[test]
    fn agent_lifetime_describes_budget_daemon_and_manual_policies() {
        assert_eq!(
            agent_lifetime(&agent_info(LifetimeClass::Short)),
            ("short · retained".into(), "1 assignment".into())
        );
        assert_eq!(
            agent_lifetime(&agent_info(LifetimeClass::Long)).1,
            "daemon stop"
        );
        assert_eq!(
            agent_lifetime(&agent_info(LifetimeClass::Persistent)).1,
            "manual release"
        );
    }

    #[test]
    fn terminal_agent_duration_stops_at_last_activity() {
        let mut info = agent_info(LifetimeClass::Short);
        info.created_secs = 100;
        info.last_activity_secs = 130;
        info.state = AgentState::Completed;

        assert_eq!(agent_duration(&info), "30s");
        assert_eq!(agent_pane_status(&info, false).0, "idle · retained");

        info.retained = false;
        info.state = AgentState::Terminated;
        assert_eq!(agent_duration(&info), "30s");
        assert_eq!(agent_pane_status(&info, false).0, "killed");
    }

    #[test]
    fn agent_pane_orders_actionable_and_retained_workers_first() {
        let mut active = agent_info(LifetimeClass::Short);
        active.id = "active".into();
        active.created_secs = 10;
        let mut idle = agent_info(LifetimeClass::Short);
        idle.id = "idle".into();
        idle.state = AgentState::Completed;
        let mut failed = agent_info(LifetimeClass::Short);
        failed.id = "failed".into();
        failed.state = AgentState::Failed;
        failed.retained = false;
        let infos = HashMap::from([
            (failed.id.clone(), failed),
            (idle.id.clone(), idle),
            (active.id.clone(), active),
        ]);

        assert_eq!(pane_agent_ids(&infos), ["active", "idle", "failed"]);
    }

    #[test]
    fn window_logo_button_has_balanced_internal_padding() {
        assert_eq!(WINDOW_LOGO_BUTTON, " 󰘵 ");
        assert_eq!(WINDOW_LOGO_BUTTON.chars().count(), 3);
    }

    #[test]
    fn daemon_restart_archives_turn_ids_without_discarding_chat() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        thread.completed_turns.insert("2".into());
        thread.unread_turns.insert("2".into());
        let mut threads = vec![thread];

        archive_session_turns(&mut threads, Some(42));

        assert_eq!(threads[0].items.len(), 2);
        assert!(threads[0]
            .items
            .iter()
            .all(|item| item.turn.as_deref() == Some("archived:42:2")));
        assert!(threads[0].completed_turns.contains("archived:42:2"));
        assert!(threads[0].unread_turns.is_empty());
        archive_session_turns(&mut threads, Some(43));
        assert!(threads[0]
            .items
            .iter()
            .all(|item| item.turn.as_deref() == Some("archived:42:2")));
    }

    #[test]
    fn scheduled_tab_columns_show_durable_task_state() {
        assert_eq!(ORCHESTRATORS_TAB_LABEL, " ORCHESTRATORS ");
        let columns = scheduled_task_columns(&ScheduledTaskInfo {
            id: "scheduled-task-1".into(),
            conversation_id: "foreground".into(),
            turn: 7,
            objective: "inspect release artifacts".into(),
            mode: ScheduledTaskMode::StartAt,
            created_at_ms: now_seconds(),
            due_at_ms: now_seconds().saturating_add(60_000),
            status: ScheduledTaskStatus::Pending,
            work_id: None,
        });
        assert_eq!(columns[0], "schedule t7");
        assert_eq!(columns[1], "pending");
        assert_eq!(columns[2], "start at");
        assert!(columns[3].starts_with("in "));
        assert_eq!(columns[5], "inspect release artifacts");
    }

    #[test]
    fn foreground_pane_describes_the_memory_service() {
        let mut memory = agent_info(LifetimeClass::Persistent);
        memory.id = MEMORY_ID.into();
        memory.state = AgentState::Running;
        memory.description = "Curate durable memory.".into();

        let columns = memory_service_columns(&memory);
        assert_eq!(columns[0], "Memory");
        assert_eq!(columns[1], "working");
        assert_eq!(columns[2], "context service");
        assert_eq!(columns[3], "daemon stop");
        assert_eq!(columns[5], "Curate durable memory.");
    }

    #[test]
    fn task_count_excludes_foreground() {
        let mut foreground = agent_info(LifetimeClass::Long);
        foreground.id = FOREGROUND_ID.into();
        foreground.state = AgentState::Running;
        let mut waiting = agent_info(LifetimeClass::Short);
        waiting.id = "worker-idle".into();
        waiting.state = AgentState::Waiting;
        let infos = HashMap::from([
            (foreground.id.clone(), foreground),
            (waiting.id.clone(), waiting),
        ]);

        assert_eq!(status_task_count(&infos), 1);
    }

    #[test]
    fn status_hides_zero_tasks_and_pluralizes_active_tasks() {
        assert_eq!(status_task_label(0), None);
        assert_eq!(status_task_label(1).as_deref(), Some("1 task"));
        assert_eq!(status_task_label(2).as_deref(), Some("2 tasks"));
    }

    #[test]
    fn agent_counts_use_singular_and_plural_labels() {
        assert_eq!(agent_count(1), "1 agent");
        assert_eq!(agent_count(3), "3 agents");
    }

    #[test]
    fn turn_badges_do_not_count_more_outcomes_than_started_workers() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("3".into()));
        thread.add_turn(
            ItemKind::Spawn,
            "worker fresh: lookup".into(),
            Some("3".into()),
        );
        for worker in ["old-one", "old-two", "fresh"] {
            thread.add_turn(
                ItemKind::SpawnResult,
                format!("worker {worker}: result"),
                Some("3".into()),
            );
        }
        let cell = build_turn_cells(&thread).pop().expect("turn cell");

        let badges = turn_cell_badges(&thread, &cell);
        assert!(badges.contains("󰚩 1 agent"), "{badges}");
        assert!(badges.contains("󰄬 1 complete"), "{badges}");
        assert!(!badges.contains("3 complete"), "{badges}");
    }

    #[test]
    fn released_lifecycle_uses_success_marker() {
        assert_eq!(lifecycle_badge(AgentState::Released), "✓ released");
    }

    #[test]
    fn pending_reply_uses_activity_with_safe_fallback() {
        assert_eq!(
            pending_reply_activity("using search", true),
            "using search..."
        );
        assert_eq!(pending_reply_activity("", true), "Checking information...");
        assert_eq!(pending_reply_activity("   ", false), "Submitting...");
    }

    #[test]
    fn main_conversation_is_always_expanded_in_turn_cells() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "first answer".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "second question".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
        let cells = build_turn_cells(&thread);
        let latest = latest_conversation_timestamp(&thread, &cells[1]);
        for cell in &cells {
            let layout = turn_cell_layout(
                0,
                0,
                std::slice::from_ref(&thread),
                cell,
                80,
                latest,
                false,
                "working",
                false,
                None,
            );
            let text = layout
                .lines
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(text.contains(&names().user));
            assert!(text.contains(&names().conversation));
            assert!(!text.contains("> first question"));
            assert!(!text.contains("trace ·"));
        }
    }

    #[test]
    fn selected_chat_cell_copy_includes_user_and_reply() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "first answer".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "second question".into(), Some("3".into()));
        thread.add_turn(ItemKind::Reply, "second answer".into(), Some("3".into()));
        let threads = vec![thread];

        let expected = format!(
            "{}:\nfirst question\n\n{}:\nfirst answer",
            names().user,
            names().conversation
        );
        assert_eq!(
            selected_chat_cell_text(&threads, Some(0)).as_deref(),
            Some(expected.as_str())
        );
        assert!(selected_chat_cell_text(&threads, None)
            .expect("latest chat cell")
            .contains("second answer"));
    }

    #[test]
    fn page_navigation_scrolls_one_viewport_and_end_follows_latest() {
        let mut scroll = TranscriptScroll::default();
        scroll.sync(100, 20, 1);
        assert_eq!(scroll.top, 80);
        scroll.scroll_up(20);
        assert_eq!(scroll.top, 60);
        assert!(!scroll.follow);
        scroll.scroll_down(20, 100, 20);
        assert_eq!(scroll.top, 80);
        assert!(scroll.follow);
        scroll.scroll_up(1);
        scroll.end();
        scroll.sync(120, 20, 2);
        assert_eq!(scroll.top, 100);
    }

    #[test]
    fn arrow_selection_opens_one_turn_and_collapses_the_previous_one() {
        let view = TranscriptView {
            total_height: 30,
            viewport: 10,
            turns: 3,
            anchor_turn: Some(2),
            starts: vec![0, 10, 20],
            heights: vec![10, 10, 10],
        };
        let mut scroll = TranscriptScroll::default();
        scroll.sync(30, 10, 1);
        let mut open = None;

        select_trace_turn(&mut open, &view, &mut scroll, -1);
        assert_eq!(open, Some(2));
        assert_eq!(scroll.top, 20);
        assert!(!scroll.follow);

        select_trace_turn(&mut open, &view, &mut scroll, -1);
        assert_eq!(open, Some(1));
        assert_eq!(scroll.top, 10);

        select_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(open, Some(2));
        select_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(open, None);
        assert!(scroll.follow);
    }

    #[test]
    fn page_keys_scroll_inside_a_tall_selected_trace_before_moving_turns() {
        let view = TranscriptView {
            total_height: 50,
            viewport: 10,
            turns: 2,
            anchor_turn: Some(0),
            starts: vec![0, 40],
            heights: vec![40, 10],
        };
        let mut scroll = TranscriptScroll {
            top: 0,
            follow: false,
            ..TranscriptScroll::default()
        };
        let mut open = Some(0);

        page_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(open, Some(0));
        assert_eq!(scroll.top, 10);
        page_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(scroll.top, 20);
        page_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(scroll.top, 30);
        page_trace_turn(&mut open, &view, &mut scroll, 1);
        assert_eq!(open, Some(1));
        assert_eq!(scroll.top, 40);
    }

    #[test]
    fn detached_height_growth_preserves_top_anchor() {
        let mut scroll = TranscriptScroll::default();
        scroll.sync(100, 20, 10);
        scroll.scroll_up(30);
        assert_eq!(scroll.top, 50);
        scroll.sync(140, 19, 11);
        assert_eq!(scroll.top, 50);
        assert!(scroll.new_activity);
    }

    #[test]
    fn turn_response_prefers_reply_over_stale_pending() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        thread.add_turn(
            ItemKind::PendingReply,
            "stale pending".into(),
            Some("2".into()),
        );
        let cells = build_turn_cells(&thread);
        let response = turn_response(&thread, &cells[0]).expect("response");
        assert_eq!(response.kind, ItemKind::Reply);
        assert_eq!(response.text, "answer");
    }

    #[test]
    fn turn_grouping_maps_out_of_order_correlated_items_in_two_passes() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
        thread.add_turn(
            ItemKind::SpawnResult,
            "late first result".into(),
            Some("2".into()),
        );
        thread.add_turn(
            ItemKind::SpawnResult,
            "unknown result".into(),
            Some("99".into()),
        );
        let cells = build_turn_cells(&thread);
        assert!(cells[0].items.contains(&2));
        assert!(!cells[1].items.contains(&2));
        assert!(!cells.iter().any(|cell| cell.items.contains(&3)));
    }

    #[test]
    fn activity_indicator_reserves_content_height_and_reaches_final_line() {
        let viewport = transcript_content_height(20, true);
        assert_eq!(viewport, 19);
        let mut scroll = TranscriptScroll::default();
        scroll.sync(25, viewport, 1);
        scroll.scroll_up(1);
        scroll.sync(30, viewport, 2);
        assert_eq!(scroll.top, 5);
        scroll.scroll_down(usize::MAX, 30, viewport);
        assert_eq!(scroll.top, 11);
        assert!(scroll.follow);
    }

    #[test]
    fn activity_indicator_requires_unseen_rows_below_viewport() {
        let scroll = TranscriptScroll {
            top: 10,
            follow: false,
            new_activity: true,
            seen_latest_revision: 2,
        };
        assert!(!should_show_activity(&scroll, 30, 20));
        assert!(should_show_activity(&scroll, 31, 20));

        let mut quiet = scroll.clone();
        quiet.new_activity = false;
        assert!(!should_show_activity(&quiet, 31, 20));
    }

    #[test]
    fn turn_cache_retains_one_layout_per_turn_and_clears_old_width() {
        let mut thread = Thread::new_foreground();
        for index in 0..200 {
            thread.add_turn(
                ItemKind::User,
                format!("question {index}"),
                Some(index.to_string()),
            );
        }
        let cells = build_turn_cells(&thread);
        let mut cache = TurnLayoutCache::default();
        cache.prepare(80, &cells, thread.structure_revision);
        for cell in &cells {
            cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
                CellLayout {
                    lines: vec![Line::raw("cell")],
                    hits: vec![None],
                }
            });
        }
        assert_eq!(cache.layouts.len(), 200);
        cache.layout(
            cell_key(&cells[0]),
            CellRevision {
                item: u64::MAX,
                metric: u64::MAX,
                worker: u64::MAX,
            },
            0,
            || CellLayout {
                lines: vec![Line::raw("updated")],
                hits: vec![None],
            },
        );
        assert_eq!(cache.layouts.len(), 200);
        cache.prepare(81, &cells, thread.structure_revision);
        assert!(cache.layouts.is_empty());
    }

    #[test]
    fn turn_projection_survives_stream_mutations_without_rebuilding() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
        let mut projection = TurnProjection::default();
        assert!(projection.update(&thread));
        let revision = cell_revision(&thread, &projection.cells[0]);
        thread.add_reply_fragment("delta".into(), Some("2".into()), false);
        assert!(!projection.update(&thread));
        assert_ne!(cell_revision(&thread, &projection.cells[0]), revision);
    }

    #[test]
    fn traces_default_hidden_and_conversation_cell_is_click_target() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        thread.add_turn(
            ItemKind::System,
            "raw diagnostic detail".into(),
            Some("2".into()),
        );
        let cells = build_turn_cells(&thread);
        let latest = latest_conversation_timestamp(&thread, &cells[0]);
        let collapsed = turn_cell_layout(
            0,
            0,
            std::slice::from_ref(&thread),
            &cells[0],
            80,
            latest,
            false,
            "",
            false,
            None,
        );
        let collapsed_text = collapsed
            .lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!collapsed_text.contains("raw diagnostic detail"));
        assert!(!collapsed_text.contains("trace ·"));
        assert!(collapsed
            .hits
            .iter()
            .any(|hit| *hit == Some(ClickTarget::TraceSummary(0))));
        let open = turn_cell_layout(
            0,
            0,
            std::slice::from_ref(&thread),
            &cells[0],
            80,
            latest,
            false,
            "",
            true,
            None,
        );
        assert!(open
            .lines
            .iter()
            .any(|line| line.to_string().contains("trace ·")));
        assert!(open
            .lines
            .iter()
            .any(|line| line.to_string().contains("raw diagnostic detail")));
    }

    #[test]
    fn conversation_cell_without_traces_is_clickable_and_highlighted() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        let cell = build_turn_cells(&thread).pop().expect("chat cell");
        let layout = turn_cell_layout(
            0,
            0,
            std::slice::from_ref(&thread),
            &cell,
            80,
            latest_conversation_timestamp(&thread, &cell),
            false,
            "",
            true,
            None,
        );

        assert!(layout
            .hits
            .iter()
            .all(|hit| *hit == Some(ClickTarget::TraceSummary(0))));
        assert!(layout
            .lines
            .iter()
            .all(|line| line.style.bg == Some(Color::Rgb(30, 32, 36))));
    }

    #[test]
    fn inactive_turn_metadata_is_dimmed_and_latest_metadata_is_green() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "old question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "old answer".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "new question".into(), Some("3".into()));
        thread.add_turn(ItemKind::Reply, "new answer".into(), Some("3".into()));
        thread.items[0].timestamp = 1;
        thread.items[1].timestamp = 2;
        thread.items[2].timestamp = 3;
        thread.items[3].timestamp = 4;
        let cells = build_turn_cells(&thread);

        let old = main_conversation_layout(&thread, &cells[0], 80, 4, false, "");
        let latest = main_conversation_layout(&thread, &cells[1], 80, 4, false, "");

        assert_eq!(
            old.lines[0].spans.last().unwrap().style.fg,
            Some(Color::DarkGray)
        );
        assert_eq!(
            latest.lines[0].spans.last().unwrap().style.fg,
            Some(Color::Green)
        );
    }

    #[test]
    fn correlated_standalone_reply_is_marked_restored() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::Reply, "recovered answer".into(), Some("2".into()));
        let cell = build_turn_cells(&thread).pop().expect("standalone reply");

        let layout = main_conversation_layout(
            &thread,
            &cell,
            80,
            latest_conversation_timestamp(&thread, &cell),
            false,
            "",
        );

        assert!(layout.lines[0].to_string().contains("RESTORED"));
    }

    #[test]
    fn resting_turn_layout_matches_main_conversation_lines() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        thread.add_turn(ItemKind::System, "diagnostic".into(), Some("2".into()));
        let cells = build_turn_cells(&thread);
        let latest = latest_conversation_timestamp(&thread, &cells[0]);
        let expected = main_conversation_layout(&thread, &cells[0], 80, latest, false, "");
        let resting = turn_cell_layout(
            0,
            0,
            std::slice::from_ref(&thread),
            &cells[0],
            80,
            latest,
            false,
            "",
            false,
            None,
        );
        assert_eq!(resting.lines, expected.lines);
    }

    #[test]
    fn selected_trace_orders_and_indents_model_and_worker_hierarchy() {
        let worker_id = "worker-123456789-secret";
        let mut threads = vec![Thread::new_foreground()];
        threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
        threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        threads[0].add_turn(
            ItemKind::System,
            "[timing] completed 1048ms".into(),
            Some("2".into()),
        );
        threads[0].add_turn(
            ItemKind::Spawn,
            format!("worker {worker_id}: Check release evidence"),
            Some("2".into()),
        );
        threads[0].add_turn(
            ItemKind::Error,
            format!("work {worker_id}: Check release evidence\nreview failed: stale evidence"),
            Some("2".into()),
        );
        let worker = find_or_create_thread(&mut threads, worker_id, false, None);
        threads[worker].add_tool(
            "agent_browser {\"action\":\"get\"}".into(),
            "tool-1".into(),
            Some("2".into()),
        );
        threads[worker].add_turn(ItemKind::Reply, "Evidence checked".into(), Some("2".into()));

        let cells = build_turn_cells(&threads[0]);
        let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
        let lines = turn_cell_layout(
            0,
            0,
            &threads,
            &cells[0],
            100,
            latest,
            false,
            "",
            true,
            Some(worker_id),
        )
        .lines
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
        let model = lines
            .iter()
            .position(|line| line.contains("Model"))
            .unwrap();
        let agents = lines
            .iter()
            .position(|line| line.contains("Agents"))
            .unwrap();
        let worker_heading = lines
            .iter()
            .position(|line| line.contains("Check release evidence"))
            .unwrap();
        let worker_tool = lines
            .iter()
            .position(|line| line.contains("agent_browser"))
            .unwrap();
        assert!(model < agents && agents < worker_heading && worker_heading < worker_tool);
        assert!(lines[worker_heading].contains(icon::EXPANDED));
        assert!(lines[worker_tool].contains("      "));
        assert!(lines.iter().any(|line| line.contains("review failed")));
        assert!(lines.iter().any(|line| line.contains("1.0s")));
        assert!(!lines.iter().any(|line| line.contains(worker_id)));
        assert!(lines.iter().any(|line| line.contains("worker-1")));
    }

    #[test]
    fn trace_summary_counts_are_singular_plural_and_non_repetitive() {
        assert_eq!(trace_count_summary(1, 1, 0), "  trace · 1 tool");
        assert_eq!(trace_count_summary(1, 0, 1), "  trace · 1 agent");
        assert_eq!(
            trace_count_summary(5, 2, 1),
            "  trace · 5 events · 2 tools · 1 agent"
        );
        assert_eq!(
            trace_count_summary(6, 1, 2),
            "  trace · 6 events · 1 tool · 2 agents"
        );
    }

    #[test]
    fn trace_timing_uses_human_duration() {
        assert_eq!(
            trace_summary("[timing] completed 1048ms"),
            format!("{} turn completed · 1.0s", icon::SUCCESS)
        );
        assert_eq!(
            trace_summary("[timing] ready 420ms"),
            format!("{} ready · +420ms", icon::WAITING)
        );
    }

    #[test]
    fn semantic_icon_mapping_detects_tool_families() {
        assert_eq!(tool_icon("agent_browser"), icon::BROWSER);
        assert_eq!(tool_icon("web_search"), icon::SEARCH);
        assert_eq!(tool_icon("read_file"), icon::FILE);
        assert_eq!(tool_icon("shell"), icon::TOOL);
        for value in [
            icon::MODEL,
            icon::AGENT,
            icon::DURATION,
            icon::TOKENS,
            icon::RUNNING,
            icon::WAITING,
            icon::SUCCESS,
            icon::WARNING,
            icon::FAILURE,
            icon::COLLAPSED,
            icon::EXPANDED,
        ] {
            assert!(!value.is_empty());
        }
    }

    #[test]
    fn model_timeline_compacts_completed_lifecycle_noise() {
        let mut thread = Thread::new_foreground();
        for text in [
            "[timing] model_request_1_started 0ms",
            "[ready] provider ready",
            "[working] generating",
            "[timing] first_visible 6100ms",
            "[timing] publication_started 42000ms",
            "commit complete",
            "[timing] completed 42800ms",
        ] {
            thread.add(ItemKind::System, text.into());
        }
        thread.add(ItemKind::Error, "provider warning".into());
        thread.add_tool("search {}".into(), "tool-1".into(), None);
        let items = (0..thread.items.len())
            .map(|index| (0, index))
            .collect::<Vec<_>>();
        let (timeline, remaining) = compact_model_timeline(&items, &[thread]);
        assert_eq!(
            timeline.as_deref(),
            Some("routed 0ms -> first token 6.1s -> completed 42.8s")
        );
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn closed_worker_is_one_row_and_still_explains_outcome() {
        let id = "worker-123456789";
        let mut threads = vec![Thread::new_foreground()];
        threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
        threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        threads[0].add_turn(
            ItemKind::Spawn,
            format!("worker {id}: Inspect release evidence"),
            Some("2".into()),
        );
        let worker = find_or_create_thread(&mut threads, id, false, None);
        threads[worker].add_turn(ItemKind::Reply, "done".into(), Some("2".into()));
        let cells = build_turn_cells(&threads[0]);
        let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
        let layout = turn_cell_layout(
            0, 0, &threads, &cells[0], 100, latest, false, "", true, None,
        );
        let rows = layout
            .lines
            .iter()
            .filter(|line| line.to_string().contains("Inspect release evidence"))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].to_string().contains("complete"));
        assert!(!layout
            .lines
            .iter()
            .any(|line| line.to_string() == "complete"));
        assert!(layout
            .hits
            .iter()
            .any(|hit| { matches!(hit, Some(ClickTarget::Worker(0, worker)) if worker == id) }));
    }

    #[test]
    fn worker_detail_toggle_keeps_only_one_worker_open() {
        let mut open = None;
        toggle_worker(&mut open, 0, "one".into());
        assert_eq!(open, Some((0, "one".into())));
        toggle_worker(&mut open, 0, "two".into());
        assert_eq!(open, Some((0, "two".into())));
        toggle_worker(&mut open, 0, "two".into());
        assert_eq!(open, None);
    }

    #[test]
    fn worker_normal_details_elide_ids_and_summarize_review_errors() {
        let id = "worker-123456789-secret";
        let summary = worker_error_summary(
            &format!("work {id}: objective\nreview failed: stale evidence from task-77"),
            Some(id),
        );
        assert_eq!(summary, "review failed");
        assert!(!summary.contains(id));
        assert_eq!(
            elide_work_id("work task-77: validating release"),
            "work · validating release"
        );
    }

    #[test]
    fn markdown_bullets_hang_and_preserve_blank_paragraphs() {
        let lines = markdown_body_lines(
            "- first entry wraps onto another line\n\nSummary - alpha item - beta item",
            16,
            Color::White,
        );
        let text = lines.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert!(text[0].contains(icon::BULLET));
        assert!(text.iter().any(|line| line.starts_with("      ")));
        assert_eq!(text.iter().filter(|line| line.is_empty()).count(), 1);
        assert_eq!(
            text.iter()
                .filter(|line| line.contains(icon::BULLET))
                .count(),
            3
        );
        assert!(text.iter().any(|line| line.trim() == "Summary"));
    }

    #[test]
    fn escape_end_reset_helper_closes_nested_selection_and_follows() {
        let mut trace = Some(2);
        let mut worker = Some((2, "worker".into()));
        let mut scroll = TranscriptScroll {
            follow: false,
            new_activity: true,
            ..TranscriptScroll::default()
        };
        assert!(close_trace_details(&mut trace, &mut worker, &mut scroll));
        assert_eq!(trace, None);
        assert_eq!(worker, None);
        assert!(scroll.follow);
        assert!(!close_trace_details(&mut trace, &mut worker, &mut scroll));
    }

    #[test]
    fn footer_trace_mode_is_concise_and_contextual() {
        assert_eq!(footer_mode_text(None, true), None);
        assert_eq!(
            footer_mode_text(Some(1), false),
            Some(format!(
                "TRACE    ↑↓ select · {} Pg scroll · {} Esc close · {} help",
                icon::SCROLL,
                icon::CLOSE,
                icon::HELP
            ))
        );
        assert_eq!(
            footer_mode_text(None, false),
            Some(format!(
                "HISTORY    ↑↓ select · {} End live · {} help",
                icon::LIVE,
                icon::HELP
            ))
        );
    }

    #[test]
    fn selected_worker_changes_the_single_cached_turn_variant() {
        let mut cache = TurnLayoutCache::default();
        let key = CellKey {
            prompt_timestamp: 1,
            prompt_index: 0,
        };
        let revision = CellRevision {
            item: 1,
            metric: 1,
            worker: 1,
        };
        cache.layout(key.clone(), revision, 2, || CellLayout {
            lines: vec![Line::raw("closed")],
            hits: vec![None],
        });
        cache.layout(key, revision, 6, || CellLayout {
            lines: vec![Line::raw("worker open")],
            hits: vec![None],
        });
        assert_eq!(cache.layouts.len(), 1);
        assert_eq!(cache.builds, 2);
    }

    #[test]
    fn selected_turn_nests_correlated_worker_tools() {
        let mut threads = vec![Thread::new_foreground()];
        threads[0].add_turn(ItemKind::User, "question".into(), Some("2".into()));
        threads[0].add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        let worker = find_or_create_thread(&mut threads, "worker-123456", false, None);
        threads[worker].add_tool(
            "agent_browser {\"action\":\"get\"}".into(),
            "tool-1".into(),
            Some("2".into()),
        );
        let cells = build_turn_cells(&threads[0]);
        let latest = latest_conversation_timestamp(&threads[0], &cells[0]);
        let closed = turn_cell_layout(0, 0, &threads, &cells[0], 80, latest, false, "", true, None);
        assert!(!closed
            .lines
            .iter()
            .any(|line| line.to_string().contains("agent_browser")));
        let open = turn_cell_layout(
            0,
            0,
            &threads,
            &cells[0],
            80,
            latest,
            false,
            "",
            true,
            Some("worker-123456"),
        );
        assert!(open
            .lines
            .iter()
            .any(|line| line.to_string().contains("agent_browser")));
        assert!(worker_turn_revisions(&threads).contains_key("2"));
    }

    #[test]
    fn trace_toggle_keeps_at_most_one_drawer_open() {
        let mut open = None;
        toggle_trace(&mut open, 0);
        assert_eq!(open, Some(0));
        toggle_trace(&mut open, 0);
        assert_eq!(open, None);
        toggle_trace(&mut open, 1);
        assert_eq!(open, Some(1));
        toggle_trace(&mut open, 2);
        assert_eq!(open, Some(2));
    }

    #[test]
    fn ctrl_o_targets_viewport_anchor_or_live_turn() {
        let view = TranscriptView {
            turns: 4,
            anchor_turn: Some(1),
            ..TranscriptView::default()
        };
        assert_eq!(ctrl_o_target(&view, false), Some(1));
        assert_eq!(ctrl_o_target(&view, true), Some(3));
    }

    #[test]
    fn turn_cache_invalidates_only_changed_streaming_turn() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "done".into(), Some("2".into()));
        thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
        let mut projection = TurnProjection::default();
        projection.update(&thread);
        let mut cache = TurnLayoutCache::default();
        cache.prepare(80, &projection.cells, thread.structure_revision);
        for cell in &projection.cells {
            cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
                CellLayout {
                    lines: vec![Line::raw("cell")],
                    hits: vec![None],
                }
            });
        }
        assert_eq!(cache.builds, 2);
        thread.add_reply_fragment("delta".into(), Some("3".into()), false);
        assert!(!projection.update(&thread));
        for cell in &projection.cells {
            cache.layout(cell_key(cell), cell_revision(&thread, cell), 0, || {
                CellLayout {
                    lines: vec![Line::raw("cell")],
                    hits: vec![None],
                }
            });
        }
        assert_eq!(cache.builds, 3);
    }

    #[test]
    fn metric_revision_is_not_masked_by_a_newer_item_revision() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "question".into(), Some("2".into()));
        thread.add_turn(ItemKind::Reply, "answer".into(), Some("2".into()));
        thread.metric_revisions.insert("2".into(), 1);
        let cell = &build_turn_cells(&thread)[0];
        let before = cell_revision(&thread, cell);
        assert!(before.item > before.metric);

        thread.metric_revisions.insert("2".into(), 2);
        let after = cell_revision(&thread, cell);
        assert_ne!(before, after);
    }

    #[test]
    fn clear_reset_discards_unified_view_projection_and_cache() {
        let mut scroll = TranscriptScroll::default();
        scroll.scroll_up(1);
        let mut view = TranscriptView {
            total_height: 10,
            viewport: 5,
            turns: 1,
            anchor_turn: Some(0),
            starts: vec![0],
            heights: vec![10],
        };
        let mut cache = TurnLayoutCache::default();
        cache.width = Some(80);
        let mut open_trace = Some(0);
        let mut projection = TurnProjection {
            structure_revision: Some(2),
            cells: Vec::new(),
        };
        reset_transcript(
            &mut scroll,
            &mut view,
            &mut cache,
            &mut open_trace,
            &mut projection,
        );
        assert_eq!(scroll, TranscriptScroll::default());
        assert_eq!(view.total_height, 0);
        assert!(cache.layouts.is_empty());
        assert_eq!(cache.width, None);
        assert_eq!(open_trace, None);
        assert_eq!(projection.structure_revision, None);
    }

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
    fn scheduled_notification_is_projected_as_standalone_reply() {
        let mut thread = Thread::new_foreground();
        let mut event = interaction(InteractionEvent::UserVisibleNotificationPublished {
            text: "Your coffee is ready.".into(),
        });
        event.metadata.message_id = "reminder-delivery-reminder-1".into();
        event.metadata.correlation_id = "reminder-1".into();
        event.metadata.turn_id = None;
        apply_interaction_event(&mut thread, event);

        assert_eq!(thread.items.len(), 1);
        assert_eq!(thread.items[0].kind, ItemKind::Reply);
        assert_eq!(thread.items[0].text, "Your coffee is ready.");
        assert_eq!(thread.items[0].turn, None);
        let cells = build_turn_cells(&thread);
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].prompt, 0);
        let layout = main_conversation_layout(
            &thread,
            &cells[0],
            80,
            latest_conversation_timestamp(&thread, &cells[0]),
            false,
            "",
        );
        assert!(!layout.lines[0].to_string().contains("RESTORED"));
    }

    #[test]
    fn reminder_schedule_badges_report_committed_changes() {
        assert_eq!(
            schedule_badges(
                &ScheduleTurnMetrics {
                    scheduled: 1,
                    tasks_scheduled: 0,
                    cancelled: 0,
                    fired: 0,
                },
                false,
            ),
            ["reminder scheduled"]
        );
        assert_eq!(
            schedule_badges(
                &ScheduleTurnMetrics {
                    scheduled: 0,
                    tasks_scheduled: 0,
                    cancelled: 1,
                    fired: 0,
                },
                false,
            ),
            ["reminder cancelled"]
        );
        assert_eq!(
            schedule_badges(
                &ScheduleTurnMetrics {
                    tasks_scheduled: 1,
                    ..ScheduleTurnMetrics::default()
                },
                false,
            ),
            ["agent task scheduled"]
        );
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
    fn memory_lifecycle_events_render_beside_token_usage_without_trace_rows() {
        let mut threads = vec![Thread::new_foreground()];
        for (event_id, kind) in [
            (
                1,
                AgentEvent::Usage {
                    turn: Some(3),
                    prompt_tokens: 10,
                    completion_tokens: 5,
                    total_tokens: 15,
                    context_tokens: 10,
                    context_window: Some(100),
                },
            ),
            (
                2,
                AgentEvent::MemoryRecalled {
                    turn: Some(3),
                    preference_count: 1,
                    history_count: 4,
                },
            ),
            (
                3,
                AgentEvent::MemoryMutation {
                    turn: Some(3),
                    result: MemoryMutationResult::Applied {
                        kind: MemoryMutationKind::Remember,
                        memory_id: "preference-1".into(),
                        replaced_memory_id: None,
                    },
                },
            ),
        ] {
            record_correlated_metrics(
                &mut threads,
                &EventEnvelope {
                    event_id,
                    session_id: FOREGROUND_ID.into(),
                    conversation_id: Some(FOREGROUND_ID.into()),
                    turn_id: Some("3".into()),
                    task_id: None,
                    parent_task_id: None,
                    tool_call_id: None,
                    actor: Actor::Foreground,
                    sequence: event_id,
                    occurred_at_ms: event_id,
                    kind,
                },
            );
        }
        let thread = &threads[0];
        assert!(thread.items.is_empty());
        let metrics = &thread.metrics["3"];
        let mut badges = vec![format!(
            "{} total {}",
            icon::TOKENS,
            format_count(metrics.self_usage.as_ref().unwrap().total)
        )];
        badges.extend(memory_badges(&metrics.memory, false));
        let badges = badges.join(" · ");
        assert!(badges.contains("total 15"));
        assert!(badges.contains("memory saved"));
        assert!(badges.contains("memory recalled 5"));
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
    fn turn_status_updates_only_its_existing_pending_reply() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
        thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

        apply_agent_event(
            &mut thread,
            AgentEvent::Status {
                turn: Some(3),
                phase: "queued".into(),
                message: "Earlier answer is still running; this turn will respond in context."
                    .into(),
            },
        );
        apply_agent_event(
            &mut thread,
            AgentEvent::Status {
                turn: Some(2),
                phase: "working".into(),
                message: "using context".into(),
            },
        );
        apply_agent_event(
            &mut thread,
            AgentEvent::Status {
                turn: Some(3),
                phase: "working".into(),
                message: String::new(),
            },
        );

        assert_eq!(thread.items[1].text, "using context");
        assert_eq!(
            thread.items[3].text,
            "Earlier answer is still running; this turn will respond in context."
        );
        assert_eq!(
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::System)
                .count(),
            3,
            "status traces remain available"
        );

        let cell = build_turn_cells(&thread).pop().expect("latest turn");
        let layout = main_conversation_layout(&thread, &cell, 100, u64::MAX, true, "working");
        let rendered = layout
            .lines
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("Earlier answer is still running"));
        assert!(!rendered.contains("working..."));
    }

    #[test]
    fn streamed_acknowledgement_stays_pending_until_conversation_finishes() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
        thread.add_reply_fragment("Let me check that for you.".into(), Some("2".into()), false);
        thread.add_turn(ItemKind::User, "tell me a joke".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

        let cells = build_turn_cells(&thread);
        let pending = main_conversation_layout(&thread, &cells[0], 100, u64::MAX, false, "working");
        let pending_text = pending
            .lines
            .iter()
            .map(Line::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(pending_text.contains("󰔟"));
        assert_eq!(ready_earlier_turn(&[thread]), None);

        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
        thread.add_reply_fragment("Checking now.".into(), Some("2".into()), false);
        thread.add_turn(ItemKind::User, "tell me a joke".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));
        thread.finish_reply("Here is the answer.".into(), Some("2".into()));

        assert!(thread.completed_turns.contains("2"));
        assert_eq!(ready_earlier_turn(&[thread]), Some(2));
    }

    #[test]
    fn dismissed_ready_turn_is_not_restored_by_replayed_completion() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "slow request".into(), Some("2".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("2".into()));
        thread.add_turn(ItemKind::User, "new request".into(), Some("3".into()));
        thread.finish_reply("finished".into(), Some("2".into()));
        mark_ready_turn_seen(std::slice::from_mut(&mut thread), 2);
        thread.finish_reply("finished".into(), Some("2".into()));

        assert_eq!(ready_earlier_turn(&[thread]), None);
        assert_eq!(
            ready_notice(2),
            format!("{} response 2 ready", icon::SUCCESS)
        );
    }

    #[test]
    fn untyped_foreground_output_does_not_leak_into_model_trace() {
        let mut thread = Thread::new_foreground();
        classify_line(&mut thread, "**Temperature:** raw worker evidence");
        assert!(thread.items.is_empty());
    }

    #[test]
    fn typed_tool_telemetry_projects_into_the_correlated_trace() {
        let mut thread = Thread::new_foreground();
        apply_correlated_agent_event(
            &mut thread,
            AgentEvent::ToolTelemetry {
                tool_name: "grep".into(),
                call_id: Some("call-1".into()),
                duration_ms: 38,
                success: true,
                truncated: false,
                bytes_out: 420,
                error_code: None,
                identity: tachyon_api::types::ToolTelemetryIdentity {
                    task_id: Some("task-1".into()),
                    work_id: Some("work-1".into()),
                    generation: Some(1),
                    assignment: Some(1),
                    attempt_id: None,
                },
            },
            Some("2"),
        );

        assert_eq!(thread.items.len(), 1);
        assert_eq!(thread.items[0].kind, ItemKind::System);
        assert_eq!(thread.items[0].turn.as_deref(), Some("2"));
        assert!(thread.items[0]
            .text
            .contains("grep complete · 38ms · 420 bytes"));
    }

    #[test]
    fn conversation_finished_replaces_turn_reservation_once() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(ItemKind::User, "first".into(), Some("2".into()));
        thread.add_turn(
            ItemKind::PendingReply,
            "queued status".into(),
            Some("2".into()),
        );
        thread.add_turn(ItemKind::User, "second".into(), Some("3".into()));
        thread.add_turn(ItemKind::PendingReply, String::new(), Some("3".into()));

        apply_interaction_event(
            &mut thread,
            interaction(InteractionEvent::ConversationFinished {
                text: "first answer".into(),
            }),
        );
        apply_interaction_event(
            &mut thread,
            interaction(InteractionEvent::ConversationFinished {
                text: "corrected answer".into(),
            }),
        );

        assert_eq!(thread.items.len(), 4);
        assert_eq!(thread.items[1].kind, ItemKind::Reply);
        assert_eq!(thread.items[1].text, "corrected answer");
        assert_eq!(thread.items[3].kind, ItemKind::PendingReply);
        assert_eq!(
            thread
                .items
                .iter()
                .filter(|item| item.kind == ItemKind::Reply && item.turn.as_deref() == Some("2"))
                .count(),
            1
        );
    }

    #[test]
    fn conversation_finish_replaces_the_acknowledgement_timestamp() {
        let mut thread = Thread::new_foreground();
        thread.add_reply_fragment("Checking now.".into(), Some("2".into()), false);
        thread.items[0].timestamp = 1;
        thread.finish_reply("Finished.".into(), Some("2".into()));

        assert!(thread.items[0].timestamp > 1);
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
        apply_correlated_agent_event(
            &mut thread,
            AgentEvent::WorkerStarted {
                turn: Some(2),
                worker_id: "worker-1".into(),
                objective: "objective".into(),
            },
            Some("3"),
        );
        let spawn = thread
            .items
            .iter()
            .find(|item| item.kind == ItemKind::Spawn)
            .expect("spawn item");
        assert_eq!(spawn.turn.as_deref(), Some("2"));
    }

    #[test]
    fn overlapping_work_results_increment_only_their_envelope_turn() {
        let mut thread = Thread::new_foreground();
        for worker_id in ["worker-1", "worker-2", "worker-3"] {
            thread.add_turn(
                ItemKind::Spawn,
                format!("worker {worker_id}: turn two"),
                Some("2".into()),
            );
        }
        thread.add_turn(
            ItemKind::Spawn,
            "worker worker-4: turn three".into(),
            Some("3".into()),
        );

        let mut seen = HashSet::new();
        for (event_id, turn, worker_id) in [(1, "2", "worker-1"), (2, "3", "worker-4")] {
            let mut result = envelope(
                event_id,
                AgentEvent::WorkResult {
                    result: tachyon_api::types::WorkResult {
                        work_id: worker_id.into(),
                        objective: format!("work for turn {turn}"),
                        generation: 1,
                        assignment: 1,
                        outcome: WorkOutcome::Completed {
                            result: "done".into(),
                            artifacts: Vec::new(),
                            context: String::new(),
                            suggested_reuse: false,
                        },
                    },
                },
            );
            result.turn_id = Some(turn.into());
            assert!(accept_event(&mut seen, &result));
            let actor = result.actor.clone();
            let envelope_turn = result.turn_id.clone();
            apply_actor_event(&mut thread, result.kind, &actor, envelope_turn.as_deref());
        }

        assert_eq!(
            turn_badges(&thread, Some("2"), u64::MAX, false),
            "󰚩 3 agents · 󰄬 1 complete"
        );
        assert_eq!(
            turn_badges(&thread, Some("3"), u64::MAX, false),
            "󰚩 1 agent · 󰄬 1 complete"
        );
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
            revision: 1,
        });
        thread.items.push(Item {
            kind: ItemKind::Spawn,
            text: "worker".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 4_000,
            revision: 2,
        });
        let badges = turn_badges(&thread, Some("2"), 6_000, true);
        assert!(badges.contains("5.0s"));
    }

    #[test]
    fn turn_badges_surface_partial_structured_worker_failures() {
        let mut thread = Thread::new_foreground();
        thread.add_turn(
            ItemKind::Spawn,
            "worker one: inspect".into(),
            Some("2".into()),
        );
        thread.add_turn(
            ItemKind::Spawn,
            "worker two: inspect".into(),
            Some("2".into()),
        );
        thread.add_turn(
            ItemKind::SpawnResult,
            "worker one: inspect\ndone".into(),
            Some("2".into()),
        );
        thread.add_turn(
            ItemKind::Error,
            "work two: inspect\nfailed".into(),
            Some("2".into()),
        );

        let badges = turn_badges(&thread, Some("2"), u64::MAX, false);
        assert!(badges.contains("󰄬 1 complete"), "{badges}");
        assert!(badges.contains("× 1 failed"), "{badges}");
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
                    context_tokens: 1_000,
                    context_window: Some(10_000),
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
                context_tokens: 2_000,
                context_window: Some(10_000),
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
        assert!(
            badges.contains(&format!("{} total 3.5k", icon::TOKENS)),
            "{badges}"
        );

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
            trace_badges.contains(&format!("{} foreground tokens 1.2k", icon::TOKENS)),
            "{trace_badges}"
        );
        assert!(
            trace_badges.contains(&format!("{} aggregate tokens 3.5k", icon::TOKENS)),
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
                memory: MemoryTurnMetrics::default(),
                schedule: ScheduleTurnMetrics::default(),
            },
        );
        let compact_badges = turn_badges(&compact_thread, Some("3"), 99_000, false);
        assert!(!compact_badges.contains("󱎫"), "{compact_badges}");
        assert!(compact_badges.contains("󰅐 done 4.5s"), "{compact_badges}");
        assert!(
            compact_badges.contains(&format!("{} total 1.0k", icon::TOKENS)),
            "{compact_badges}"
        );
    }

    #[test]
    fn trace_summaries_compact_lifecycle_events() {
        assert_eq!(
            trace_summary("[timing] model_request_1_started 0ms"),
            format!("{} model request 1 · +0ms", icon::DURATION)
        );
        assert_eq!(
            trace_summary("[timing] model_request_1_completed 3220ms"),
            format!("{} model request 1 · 3.2s", icon::SUCCESS)
        );
        assert_eq!(
            trace_summary("[working] I'll check the weather"),
            format!("{} working · I'll check the weather", icon::RUNNING)
        );
    }

    #[test]
    fn visible_late_completion_clears_its_ready_notice() {
        let mut thread = Thread::new_foreground();
        thread.items.push(Item {
            kind: ItemKind::User,
            text: "slow request".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 1_000,
            revision: 1,
        });
        thread.items.push(Item {
            kind: ItemKind::User,
            text: "foreground request".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("3".into()),
            timestamp: 2_000,
            revision: 2,
        });
        thread.items.push(Item {
            kind: ItemKind::PendingReply,
            text: "Still checking that for you.".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("3".into()),
            timestamp: 2_000,
            revision: 2,
        });
        thread.items.push(Item {
            kind: ItemKind::Reply,
            text: "late result".into(),
            hidden: false,
            output: None,
            tool_id: None,
            turn: Some("2".into()),
            timestamp: 3_000,
            revision: 3,
        });
        thread.completed_turns.insert("2".into());
        thread.unread_turns.insert("2".into());
        let mut threads = vec![thread];
        assert_eq!(ready_earlier_turn(&threads), Some(2));
        let mut projection = TurnProjection::default();
        projection.update(&threads[0]);
        let view = TranscriptView {
            total_height: 10,
            viewport: 10,
            turns: 2,
            anchor_turn: Some(0),
            starts: vec![0, 5],
            heights: vec![5, 5],
        };
        mark_visible_ready_turns_seen(
            &mut threads,
            &projection,
            &view,
            &TranscriptScroll::default(),
        );
        assert_eq!(ready_earlier_turn(&threads), None);
    }
}
