//! Explicit, generation-fenced daemon history paging. No transcript filesystem I/O.
mod loading;
use crate::app::{session_archive, ItemKind, Thread};
pub(in crate::app) use loading::Navigator;
use std::{collections::HashSet, io, time::Duration};
use tachyon_api::{ApiRequest, ApiResponse, HistoryEntry, HistoryKind, HistoryRole, FOREGROUND_ID};

#[derive(Default)]
pub(in crate::app) struct Cursor {
    before: Option<u64>,
    seen: HashSet<String>,
}

pub(in crate::app) struct PageRequest {
    until: u64,
}
pub(in crate::app) struct LoadedPage {
    before: u64,
    entries: Vec<HistoryEntry>,
}

impl Cursor {
    pub(in crate::app) fn observe(&mut self, entries: &[HistoryEntry]) {
        if self.before.is_none() {
            // Include the boundary millisecond: snapshot history may split it.
            self.before = entries
                .iter()
                .map(|e| e.occurred_at_ms.saturating_add(1))
                .min();
        }
        self.seen.extend(entries.iter().map(|e| e.event_id.clone()));
    }

    pub(in crate::app) fn request(&self, _latest: bool, older: bool) -> PageRequest {
        PageRequest {
            until: if older {
                self.before.unwrap_or(u64::MAX)
            } else {
                0
            },
        }
    }

    pub(in crate::app) fn install(&mut self, page: LoadedPage, threads: &mut Vec<Thread>) {
        let thread = &mut threads[0];
        let mut prefix = Thread::new_foreground();
        for entry in page.entries {
            if !self.seen.insert(entry.event_id)
                || entry.kind != HistoryKind::Conversation
                || entry.conversation_id != FOREGROUND_ID
            {
                continue;
            }
            let turn = entry
                .turn_id
                .as_deref()
                .map(|t| session_archive::conversation_turn(&entry.conversation_id, t));
            let kind = if entry.role == HistoryRole::User {
                ItemKind::User
            } else {
                ItemKind::Reply
            };
            if turn.is_some()
                && thread
                    .items
                    .iter()
                    .any(|i| i.turn == turn && i.kind == kind)
            {
                continue;
            }
            prefix.add_turn(kind, entry.text, turn.clone());
            prefix.items.last_mut().unwrap().timestamp = entry.occurred_at_ms / 1000;
            if entry.role == HistoryRole::Assistant {
                if let Some(turn) = turn {
                    thread.completed_turns.insert(turn);
                }
            }
        }
        thread.touch_structure();
        thread.history_len += prefix.items.len();
        thread.items.splice(0..0, prefix.items);
        self.before = Some(page.before);
    }
}

impl PageRequest {
    pub(in crate::app) fn load(self) -> io::Result<Option<LoadedPage>> {
        if self.until == 0 {
            return Ok(None);
        }
        let mut client =
            tachyon_client::Client::connect().map_err(|e| io::Error::other(e.to_string()))?;
        let mut windows = vec![(0, self.until)];
        // HistoryQuery is ascending and timestamp-only. Split saturated windows,
        // newest first, never advance past a partially retrieved timestamp.
        while let Some((since_ms, until_ms)) = windows.pop() {
            let response = client
                .request(
                    &ApiRequest::HistoryQuery {
                        since_ms,
                        until_ms,
                        limit: 1000,
                    },
                    Duration::from_secs(5),
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
            let ApiResponse::History { entries } = response else {
                return Err(io::Error::other("unexpected history response"));
            };
            if entries.len() == 1000 {
                if until_ms - since_ms <= 1 {
                    return Err(io::Error::other(
                        "history timestamp exceeds daemon page limit; event-ID cursor required",
                    ));
                }
                let middle = since_ms + (until_ms - since_ms) / 2;
                windows.push((since_ms, middle));
                windows.push((middle, until_ms));
            } else if !entries.is_empty() {
                return Ok(Some(LoadedPage {
                    before: since_ms,
                    entries,
                }));
            }
        }
        Ok(None)
    }
}
