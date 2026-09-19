//! Live-only state. Transcript items remain the source of truth for publication;
//! monitoring never infers running tools from saved trace text or tool arguments.
use super::*;
use std::collections::BTreeMap;

pub(super) enum ReplyUpdate<'a> {
    Status { phase: &'a str, message: &'a str },
    Delta { text: String, line_break: bool },
    Finished(String),
    Recovered(String),
    Failed(String),
}

pub(super) fn reply(thread: &mut Thread, turn: Option<String>, update: ReplyUpdate<'_>) {
    let recovered = matches!(&update, ReplyUpdate::Recovered(_));
    let terminal = turn.as_ref().is_some_and(|turn| {
        thread.completed_turns.contains(turn)
            || thread
                .metrics
                .get(turn)
                .is_some_and(|m| m.ended_at_ms.is_some())
    });
    match update {
        ReplyUpdate::Status { phase, message } => {
            if terminal || !matches!(phase, "working" | "queued") || turn.is_none() {
                return;
            }
            // Publication wins even if an old reservation survived recovery.
            if thread
                .items
                .iter()
                .any(|i| i.turn == turn && i.kind == ItemKind::Reply)
            {
                return;
            }
            let Some(index) = thread
                .items
                .iter()
                .rposition(|i| i.kind == ItemKind::PendingReply && i.turn == turn)
            else {
                return;
            };
            if index < thread.history_len {
                return;
            }
            let message = message.trim();
            let generic = |s: &str| {
                matches!(
                    s.trim().trim_end_matches('.').to_ascii_lowercase().as_str(),
                    "" | "working"
                        | "working on that"
                        | "working on your request"
                        | "queued"
                        | "thinking"
                        | "checking information"
                        | "submitting"
                )
            };
            let current = &thread.items[index].text;
            if generic(message) && !generic(current) {
                return;
            }
            let text = if message.is_empty() { phase } else { message };
            if current == text {
                return;
            }
            thread.touch();
            thread.items[index].text = text.to_owned();
            thread.items[index].revision = thread.revision;
        }
        ReplyUpdate::Delta { text, line_break } => {
            if !terminal {
                publish(thread, text, turn, false, line_break);
            }
        }
        ReplyUpdate::Finished(text) | ReplyUpdate::Recovered(text) => {
            if !recovered
                && turn.as_ref().is_some_and(|turn| {
                    thread
                        .metrics
                        .get(turn)
                        .is_some_and(|m| m.failure.is_some())
                })
            {
                return;
            }
            if let Some(turn) = &turn {
                thread.activity.finish_turn(turn);
                if recovered {
                    thread.metrics.entry(turn.clone()).or_default().failure = None;
                }
            }
            publish(
                thread,
                sanitize_reply_text(&text),
                turn.clone(),
                true,
                false,
            );
            if recovered {
                if let Some(turn) = turn {
                    thread.metric_revisions.insert(turn, thread.revision);
                }
            }
        }
        ReplyUpdate::Failed(message) => {
            if turn
                .as_ref()
                .is_some_and(|turn| thread.completed_turns.contains(turn))
            {
                return;
            }
            if let Some(turn) = &turn {
                thread.activity.finish_turn(turn);
                thread.completed_turns.insert(turn.clone());
                thread.touch();
                thread.metrics.entry(turn.clone()).or_default().failure = Some(message.clone());
                thread
                    .metric_revisions
                    .insert(turn.clone(), thread.revision);
            }
            if let Some(index) = thread
                .items
                .iter()
                .rposition(|i| i.kind == ItemKind::PendingReply && i.turn == turn)
            {
                thread.touch();
                let item = &mut thread.items[index];
                item.kind = ItemKind::Error;
                item.text = message;
                item.revision = thread.revision;
            } else {
                thread.add_turn(ItemKind::Error, message, turn);
            }
            thread.streaming = false;
        }
    }
}

/// Cached, cell-width-aware plain status layout. Bound hostile/accidental large
/// status payloads independently of transcript storage and timer overlays.
pub(super) fn status_lines(
    text: &str,
    width: u16,
    marker: &str,
    style: Style,
) -> Vec<Line<'static>> {
    if width == 0 {
        return vec![Line::raw("")];
    }
    let indent = Span::raw(marker).width();
    let marker = if indent < width as usize { marker } else { "" };
    let indent = Span::raw(marker).width();
    let budget = (width as usize - indent).min(92);
    let bounded: String = text.chars().take(4096).collect();
    let mut rows = Vec::new();
    let mut line = String::new();
    let mut used = 0;
    for word in bounded.split_whitespace() {
        let word_width = Span::raw(word).width();
        if used > 0 && used + 1 + word_width > budget {
            rows.push(std::mem::take(&mut line));
            used = 0;
        }
        if used > 0 {
            line.push(' ');
            used += 1;
        }
        for ch in word.chars().filter(|ch| !ch.is_control()) {
            let ch_width = Span::raw(ch.to_string()).width();
            if used > 0 && used + ch_width > budget {
                rows.push(std::mem::take(&mut line));
                used = 0;
            }
            if ch_width > budget {
                line.push('?');
                used += 1;
            } else {
                line.push(ch);
                used += ch_width;
            }
        }
        if rows.len() >= 256 {
            break;
        }
    }
    if !line.is_empty() {
        rows.push(line);
    }
    if bounded.len() < text.len() || rows.len() >= 256 {
        rows.truncate(255);
        rows.push(".".repeat(budget.min(3)));
    }
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let prefix = if index == 0 {
                marker.to_owned()
            } else {
                " ".repeat(indent)
            };
            Line::from(vec![
                Span::styled(prefix, Style::default().fg(Color::Yellow)),
                Span::styled(row, style),
            ])
        })
        .collect()
}

// Both transports publish through the same slot. A delta replaces provisional
// text; a final replaces accumulated deltas. Neither appends to an acknowledgement.
fn publish(
    thread: &mut Thread,
    text: String,
    turn: Option<String>,
    final_reply: bool,
    line_break: bool,
) {
    thread.touch();
    thread.streaming = !final_reply;
    thread.last_activity = Instant::now();
    if final_reply {
        if let Some(turn) = &turn {
            let newly_completed = thread.completed_turns.insert(turn.clone());
            if newly_completed
                && thread.items.iter().any(|item| {
                    item.kind == ItemKind::User
                        && item.turn.as_deref().is_some_and(|t| later_turn(t, turn))
                })
            {
                thread.unread_turns.insert(turn.clone());
            }
        }
    }
    let existing = thread
        .items
        .iter()
        .rposition(|item| {
            item.kind == ItemKind::Reply && item.turn == turn && item.attention.is_none()
        })
        .or_else(|| {
            thread
                .items
                .iter()
                .rposition(|item| item.kind == ItemKind::PendingReply && item.turn == turn)
        });
    if let Some(index) = existing {
        let item = &mut thread.items[index];
        if final_reply || item.kind == ItemKind::PendingReply {
            item.text = text;
            item.timestamp = now_seconds();
        } else {
            if line_break {
                item.text.push('\n');
            }
            item.text.push_str(&text);
        }
        item.kind = ItemKind::Reply;
        item.revision = thread.revision;
        return;
    }
    let index = turn
        .as_deref()
        .and_then(|turn| {
            thread
                .items
                .iter()
                .position(|item| item.turn.as_deref().is_some_and(|t| later_turn(t, turn)))
        })
        .unwrap_or(thread.items.len());
    thread.items.insert(
        index,
        Item {
            attention: None,
            work: None,
            kind: ItemKind::Reply,
            text,
            hidden: false,
            output: None,
            tool_id: None,
            turn,
            timestamp: now_seconds(),
            revision: thread.revision,
        },
    );
    thread.structure_revision = thread.structure_revision.wrapping_add(1);
}

pub(super) fn busy(thread: &Thread) -> bool {
    thread.items[thread.history_len..].iter().any(|item| {
        matches!(
            item.kind,
            ItemKind::User | ItemKind::PendingReply | ItemKind::Reply
        ) && item.turn.as_ref().is_none_or(|turn| {
            !thread.completed_turns.contains(turn)
                && !thread
                    .metrics
                    .get(turn)
                    .is_some_and(|m| m.ended_at_ms.is_some())
        })
    })
}

// Caps include terminal tombstones. On overflow, suppress rather than evict and
// accidentally resurrect an old call. No unbounded arguments/output are retained.
const MAX_SCOPES: usize = 256;
const MAX_CALLS: usize = 128;

type ScopeKey = (String, String, String, Option<String>, Option<Assignment>);

#[derive(Default)]
pub(super) struct Activity {
    scopes: BTreeMap<ScopeKey, Scope>,
    assignments: BTreeMap<(String, String), (Assignment, bool)>,
    closed_workers: BTreeSet<(String, String)>,
    overflowed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Assignment {
    generation: u64,
    assignment: u64,
    attempt: Option<String>,
}

#[derive(Default)]
struct Scope {
    calls: BTreeMap<String, Option<String>>,
    closed: bool,
}

impl Activity {
    pub(super) fn work_ids<'a>(&'a self, turn: &'a str) -> impl Iterator<Item = &'a str> {
        self.assignments
            .keys()
            .filter(move |(t, _)| t == turn)
            .map(|(_, work)| work.as_str())
    }

    pub(super) fn saturated(&self) -> bool {
        self.overflowed
            || self.closed_workers.len() >= MAX_SCOPES
            || self.assignments.len() >= MAX_SCOPES
    }

    pub(super) fn finish_turn(&mut self, turn: &str) {
        // Once events have been dropped, freeing capacity cannot make their
        // missing tombstones trustworthy again.
        self.overflowed = self.saturated();
        // Completed turns are rejected by record() using transcript state, so
        // their tombstones can be discarded without admitting delayed starts.
        self.scopes.retain(|(t, ..), _| t != turn);
        self.closed_workers.retain(|(t, _)| t != turn);
        self.assignments.retain(|(t, _), _| t != turn);
    }

    pub(super) fn finish_actor(&mut self, actor: &str) -> BTreeSet<String> {
        let mut changed = BTreeSet::new();
        for ((turn, a, _, task, assignment), scope) in &mut self.scopes {
            if a == actor {
                if let (Some(task), Some(assignment)) = (task, assignment) {
                    if let Some((current, closed)) =
                        self.assignments.get_mut(&(turn.clone(), task.clone()))
                    {
                        if current == assignment {
                            *closed = true;
                        }
                    }
                }
                changed.insert(turn.clone());
                if self.closed_workers.len() < MAX_SCOPES {
                    self.closed_workers.insert((turn.clone(), a.clone()));
                }
                scope.closed = true;
                scope.calls.clear();
            }
        }
        if self.saturated() {
            changed.extend(self.scopes.keys().map(|(turn, ..)| turn.clone()));
        }
        changed
    }

    fn label(value: &str) -> String {
        value
            .chars()
            .take(128)
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.'))
            .take(32)
            .collect()
    }

    pub(super) fn record(&mut self, e: &EventEnvelope) {
        let Some(turn) = &e.turn_id else { return };
        let actor = match &e.actor {
            Actor::Foreground => FOREGROUND_ID,
            Actor::Worker { id } => id,
            _ => return,
        };
        if turn.len() > 512
            || actor.len() > 512
            || e.session_id.len() > 512
            || e.task_id.as_ref().is_some_and(|task| task.len() > 512)
        {
            return;
        }
        if self.saturated() {
            return;
        }
        let fenced = match &e.kind {
            AgentEvent::ToolStarted {
                identity: Some(identity),
                ..
            }
            | AgentEvent::ToolFinished {
                identity: Some(identity),
                ..
            } => {
                let (Some(work), Some(generation), Some(assignment)) =
                    (&identity.work_id, identity.generation, identity.assignment)
                else {
                    return;
                };
                // Host correlation and the producer fence must agree; do not
                // infer a missing work ID from a child-local turn or arguments.
                if e.task_id.as_ref() != Some(work)
                    || identity.task_id.as_ref().is_some_and(|task| task != work)
                {
                    return;
                }
                Some((
                    work.clone(),
                    Assignment {
                        generation,
                        assignment,
                        attempt: identity.attempt_id.clone(),
                    },
                ))
            }
            AgentEvent::WorkResult { result } => Some((
                result.work_id.clone(),
                Assignment {
                    generation: result.generation,
                    assignment: result.assignment,
                    attempt: result.attempt_id.clone(),
                },
            )),
            _ => None,
        };
        if let Some((work, incoming)) = &fenced {
            if work.len() > 512
                || incoming.attempt.as_ref().is_some_and(|id| id.len() > 512)
                || e.task_id.as_ref().is_some_and(|task| task != work)
            {
                return;
            }
            let key = (turn.clone(), work.clone());
            if let Some((current, closed)) = self.assignments.get(&key) {
                let order = (incoming.generation, incoming.assignment)
                    .cmp(&(current.generation, current.assignment));
                if order.is_lt() || (order.is_eq() && (incoming != current || *closed)) {
                    return;
                }
            }
            if self
                .assignments
                .get(&key)
                .is_none_or(|(current, _)| current != incoming)
            {
                // Advancing the fence supersedes old calls, including legacy
                // calls whose assignment cannot be established.
                self.scopes
                    .retain(|(t, _, _, task, _), _| t != turn || task.as_ref() != Some(work));
                self.assignments
                    .insert(key.clone(), (incoming.clone(), false));
            }
            if matches!(e.kind, AgentEvent::WorkResult { .. }) {
                self.assignments.get_mut(&key).unwrap().1 = true;
            }
        } else if self
            .closed_workers
            .contains(&(turn.clone(), actor.to_owned()))
            || e.task_id
                .as_ref()
                .is_some_and(|task| self.assignments.contains_key(&(turn.clone(), task.clone())))
        {
            return;
        }
        if let AgentEvent::WorkerCompleted { worker_id, .. } = &e.kind {
            if worker_id.len() > 512 {
                return;
            }
            self.closed_workers
                .insert((turn.clone(), worker_id.clone()));
            for ((t, a, _, _, assignment), scope) in &mut self.scopes {
                if t == turn && a == worker_id && assignment.is_none() {
                    scope.closed = true;
                    scope.calls.clear();
                }
            }
            return;
        }
        if let AgentEvent::WorkResult { result } = &e.kind {
            if result.work_id.len() > 512 {
                return;
            }
            for ((t, a, _, task, assignment), scope) in &mut self.scopes {
                if t == turn
                    && (assignment.is_none()
                        || assignment.as_ref() == fenced.as_ref().map(|(_, fence)| fence))
                    && (task.as_deref() == Some(&result.work_id)
                        || (matches!(e.actor, Actor::Worker { .. })
                            && a == actor
                            && task.is_none()))
                {
                    scope.closed = true;
                    scope.calls.clear();
                }
            }
            if matches!(e.actor, Actor::Worker { .. }) && self.scopes.len() < MAX_SCOPES {
                let key = (
                    turn.clone(),
                    actor.to_owned(),
                    e.session_id.clone(),
                    None,
                    None,
                );
                self.scopes.insert(
                    key,
                    Scope {
                        closed: true,
                        ..Default::default()
                    },
                );
            }
            // A result before a start must also leave a tombstone.
        } else if matches!(
            e.kind,
            AgentEvent::Reply { .. }
                | AgentEvent::Error { .. }
                | AgentEvent::WorkerReleaseRequested { .. }
        ) {
            for ((t, a, session, task, assignment), scope) in &mut self.scopes {
                if t == turn
                    && assignment.is_none()
                    && a == actor
                    && session == &e.session_id
                    && (e.task_id.is_none() || task == &e.task_id)
                {
                    scope.closed = true;
                    scope.calls.clear();
                }
            }
            if e.task_id.is_none() {
                self.closed_workers.insert((turn.clone(), actor.to_owned()));
            }
        } else if !matches!(
            e.kind,
            AgentEvent::ToolStarted { .. }
                | AgentEvent::ToolFinished { .. }
                | AgentEvent::Reply { .. }
                | AgentEvent::Error { .. }
                | AgentEvent::WorkerReleaseRequested { .. }
        ) {
            return;
        }
        let task = match &e.kind {
            AgentEvent::WorkResult { result } => Some(result.work_id.clone()),
            _ => e.task_id.clone(),
        };
        let key = (
            turn.clone(),
            actor.to_owned(),
            e.session_id.clone(),
            task,
            fenced.map(|(_, fence)| fence),
        );
        // Identity fields are opaque, never truncated (which would alias calls).
        // Refuse oversized identities to keep monitoring memory bounded in bytes.
        if key.0.len() > 512
            || key.1.len() > 512
            || key.2.len() > 512
            || key.3.as_ref().is_some_and(|task| task.len() > 512)
        {
            return;
        }
        if self.scopes.len() >= MAX_SCOPES && !self.scopes.contains_key(&key) {
            self.overflowed = true;
            return;
        }
        let scope = self.scopes.entry(key).or_default();
        if scope.closed {
            return;
        }
        match &e.kind {
            AgentEvent::ToolStarted { id, name, .. } => {
                if id.len() > 512 {
                    return;
                }
                if scope.calls.len() >= MAX_CALLS && !scope.calls.contains_key(id) {
                    scope.closed = true;
                    scope.calls.clear();
                    return;
                }
                scope
                    .calls
                    .entry(id.clone())
                    .or_insert_with(|| Some(Self::label(name)));
            }
            AgentEvent::ToolFinished { id, .. } => {
                if id.len() > 512 {
                    return;
                }
                if scope.calls.len() >= MAX_CALLS && !scope.calls.contains_key(id) {
                    scope.closed = true;
                    scope.calls.clear();
                } else {
                    scope.calls.insert(id.clone(), None);
                }
            }
            _ => {
                scope.closed = true;
                scope.calls.clear();
            }
        }
    }

    pub(super) fn compact_summary(&self, turn: &str) -> String {
        if self.saturated() {
            return String::new();
        }
        let total: usize = self
            .scopes
            .iter()
            .filter(|((t, ..), scope)| t == turn && !scope.closed)
            .map(|(_, scope)| scope.calls.values().flatten().count())
            .sum();
        if total == 0 {
            return String::new();
        }
        format!("{total} {}", if total == 1 { "tool" } else { "tools" })
    }

    pub(super) fn summary(&self, turn: &str, actor: Option<&str>) -> String {
        if self.saturated() {
            return String::new();
        }
        let mut labels = Vec::new();
        let mut count = 0;
        for foreground in [true, false] {
            for ((t, a, _, _, _), scope) in &self.scopes {
                if (a == FOREGROUND_ID) != foreground
                    || t != turn
                    || scope.closed
                    || actor.is_some_and(|actor| actor != a)
                {
                    continue;
                }
                for name in scope.calls.values().flatten() {
                    count += 1;
                    if labels.len() < 3 {
                        let who = if foreground {
                            "foreground".into()
                        } else {
                            Self::label(a)
                        };
                        labels.push(format!("{who}: {name}"));
                    }
                }
            }
        }
        if count > labels.len() {
            labels.push(format!("+{} tools", count - labels.len()));
        }
        labels.join(" | ")
    }
}

pub(super) fn record(thread: &mut Thread, envelope: &EventEnvelope) {
    let Some(turn) = &envelope.turn_id else {
        return;
    };
    if thread.completed_turns.contains(turn)
        || thread
            .metrics
            .get(turn)
            .is_some_and(|m| m.ended_at_ms.is_some())
    {
        thread.activity.finish_turn(turn);
        return;
    }
    if !matches!(
        envelope.kind,
        AgentEvent::ToolStarted { .. }
            | AgentEvent::ToolFinished { .. }
            | AgentEvent::Reply { .. }
            | AgentEvent::Error { .. }
            | AgentEvent::WorkerCompleted { .. }
            | AgentEvent::WorkResult { .. }
            | AgentEvent::WorkerReleaseRequested { .. }
    ) {
        return;
    }
    let saturated = thread.activity.saturated();
    thread.activity.record(envelope);
    if !saturated && thread.activity.saturated() {
        for turn in thread.metrics.keys() {
            thread
                .metric_revisions
                .insert(turn.clone(), thread.revision);
        }
    }
    thread
        .metric_revisions
        .insert(turn.clone(), thread.revision);
}

#[cfg(test)]
#[path = "turn_activity/tests.rs"]
mod tests;
