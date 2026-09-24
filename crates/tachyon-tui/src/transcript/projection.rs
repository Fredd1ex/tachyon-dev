//! Conversation cell projection and revision identity.
use crate::app::model::items::{Item, ItemKind};
use crate::app::model::thread::Thread;
use crate::app::model::TranscriptScroll;
use crate::app::transcript_cache::{CellKey, CellRevision, TurnCell};
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

pub(in crate::app) fn transcript_content_height(height: u16, show_activity: bool) -> usize {
    (height as usize).saturating_sub(usize::from(show_activity && height > 1))
}

pub(in crate::app) fn should_show_activity(
    scroll: &TranscriptScroll,
    total: usize,
    viewport: usize,
) -> bool {
    scroll.new_activity
        && !scroll.follow
        && viewport > 1
        && scroll.top.saturating_add(viewport) < total
}

pub(in crate::app) fn foreground_thread(threads: &[Thread]) -> Option<(usize, &Thread)> {
    threads
        .iter()
        .enumerate()
        .find(|(_, thread)| thread.is_foreground)
}

pub(in crate::app) fn build_turn_cells(thread: &Thread) -> Vec<TurnCell> {
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
                matches!(item.kind, ItemKind::Reply | ItemKind::PendingReply)
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
    if thread.hide_history {
        cells.retain(|cell| cell.prompt >= thread.history_len);
    }
    cells
}

pub(in crate::app) fn cell_revision(thread: &Thread, cell: &TurnCell) -> CellRevision {
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

pub(in crate::app) fn worker_turn_revisions(threads: &[Thread]) -> HashMap<&str, u64> {
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

pub(in crate::app) fn cell_key(cell: &TurnCell) -> CellKey {
    CellKey {
        prompt_timestamp: cell.prompt_timestamp,
        prompt_index: cell.prompt,
    }
}

pub(in crate::app) fn latest_conversation_timestamp(thread: &Thread, cell: &TurnCell) -> u64 {
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

pub(in crate::app) fn turn_response<'a>(thread: &'a Thread, cell: &TurnCell) -> Option<&'a Item> {
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
