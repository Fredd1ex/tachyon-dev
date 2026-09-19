//! Agent transcript storage and revision invalidation, independent of input routing.
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;
use tachyon_api::FOREGROUND_ID;

use super::super::{now_seconds, turn_activity};
use super::items::{Item, ItemKind};
use super::metrics::TurnMetrics;

pub(in crate::app) fn find_or_create_thread(
    threads: &mut Vec<Thread>,
    id: &str,
    is_foreground: bool,
    task: Option<String>,
) -> usize {
    if let Some(idx) = threads.iter().position(|thread| thread.id == id) {
        return idx;
    }
    let mut thread = Thread::new_foreground();
    thread.id = id.into();
    thread.parent = (!is_foreground).then(|| FOREGROUND_ID.into());
    thread.task = task;
    thread.is_foreground = is_foreground;
    thread.collapsed = !is_foreground;
    threads.push(thread);
    threads.len() - 1
}

pub(in crate::app) struct Thread {
    pub(in crate::app) history_len: usize,
    pub(in crate::app) history_label: Option<String>,
    pub(in crate::app) session_started: u64,
    pub(in crate::app) hide_history: bool,
    pub(in crate::app) id: String,
    pub(in crate::app) parent: Option<String>,
    pub(in crate::app) task: Option<String>,
    pub(in crate::app) is_foreground: bool,
    pub(in crate::app) collapsed: bool,
    pub(in crate::app) streaming: bool,
    pub(in crate::app) last_activity: Instant,
    pub(in crate::app) revision: u64,
    pub(in crate::app) structure_revision: u64,
    pub(in crate::app) items: Vec<Item>,
    pub(in crate::app) completed_turns: BTreeSet<String>,
    pub(in crate::app) unread_turns: BTreeSet<String>,
    pub(in crate::app) usage: HashMap<u64, (u32, u32, u32)>,
    pub(in crate::app) metrics: HashMap<String, TurnMetrics>,
    pub(in crate::app) metric_revisions: HashMap<String, u64>,
    pub(in crate::app) activity: turn_activity::Activity,
    pub(in crate::app) checklist: Option<(String, String)>,
}

impl Thread {
    pub(in crate::app) fn new_foreground() -> Self {
        Thread {
            history_len: 0,
            history_label: None,
            session_started: now_seconds(),
            hide_history: false,
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
            activity: turn_activity::Activity::default(),
            checklist: None,
        }
    }

    pub(in crate::app) fn add(&mut self, kind: ItemKind, text: String) {
        self.add_turn(kind, text, None);
    }

    pub(in crate::app) fn reserve_reply(&mut self) {
        self.touch_structure();
        self.items.push(Item {
            attention: None,
            work: None,
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

    pub(in crate::app) fn add_turn(&mut self, kind: ItemKind, text: String, turn: Option<String>) {
        self.touch();
        self.streaming = kind == ItemKind::Reply;
        self.last_activity = Instant::now();
        let timestamp = now_seconds();
        if kind == ItemKind::ToolResult {
            if let Some(last) = self.items[self.history_len..]
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
        // Glue consecutive fragments, but never glue a notice to a model reply.
        let can_glue = self.items.last().is_some_and(|last| {
            last.kind == kind
                && last.turn == turn
                && match kind {
                    ItemKind::ToolResult | ItemKind::Tool | ItemKind::System => true,
                    ItemKind::Reply => last.attention.is_none(),
                    _ => false,
                }
        });
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
            let hidden = kind == ItemKind::ToolResult;
            self.items.push(Item {
                attention: None,
                work: None,
                kind,
                text,
                hidden,
                output: None,
                tool_id: None,
                turn,
                timestamp,
                revision: self.revision,
            });
        }
    }

    pub(in crate::app) fn add_reply_fragment(
        &mut self,
        text: String,
        turn: Option<String>,
        line_break: bool,
    ) {
        turn_activity::reply(
            self,
            turn,
            turn_activity::ReplyUpdate::Delta { text, line_break },
        );
    }

    pub(in crate::app) fn finish_reply(&mut self, text: String, turn: Option<String>) {
        turn_activity::reply(self, turn, turn_activity::ReplyUpdate::Finished(text));
    }

    pub(in crate::app) fn update_pending_reply_status(
        &mut self,
        turn: Option<String>,
        phase: &str,
        message: &str,
    ) {
        turn_activity::reply(
            self,
            turn,
            turn_activity::ReplyUpdate::Status { phase, message },
        );
    }

    pub(in crate::app) fn add_tool(&mut self, text: String, id: String, turn: Option<String>) {
        self.touch_structure();
        self.streaming = false;
        self.items.push(Item {
            kind: ItemKind::Tool,
            attention: None,
            work: None,
            text,
            hidden: true,
            output: None,
            tool_id: Some(id),
            turn,
            timestamp: now_seconds(),
            revision: self.revision,
        });
    }

    pub(in crate::app) fn add_tool_result(
        &mut self,
        id: String,
        text: String,
        turn: Option<String>,
    ) {
        self.touch();
        if let Some(tool) = self.items.iter_mut().rev().find(|item| {
            item.kind == ItemKind::Tool
                && item.tool_id.as_deref() == Some(id.as_str())
                && item.turn == turn
        }) {
            // Terminal snapshots supersede live output; replay replaces the payload.
            if tool.work.is_some() {
                return;
            }
            tool.output = Some(text);
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

    pub(in crate::app) fn touch(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }

    pub(in crate::app) fn touch_structure(&mut self) {
        self.touch();
        self.structure_revision = self.structure_revision.wrapping_add(1);
    }
}
