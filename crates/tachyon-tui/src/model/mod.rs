//! Transcript records, accounting, and viewport state.
pub(super) mod items;
pub(super) mod metrics;
pub(super) mod thread;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct TranscriptScroll {
    pub(super) top: usize,
    pub(super) follow: bool,
    pub(super) new_activity: bool,
    pub(super) seen_latest_revision: u64,
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
    pub(super) fn sync(&mut self, total: usize, viewport: usize, latest_revision: u64) {
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

    pub(super) fn scroll_up(&mut self, rows: usize) {
        self.follow = false;
        self.top = self.top.saturating_sub(rows);
    }

    pub(super) fn scroll_down(&mut self, rows: usize, total: usize, viewport: usize) {
        let max_top = total.saturating_sub(viewport);
        self.top = self.top.saturating_add(rows).min(max_top);
        if self.top == max_top {
            self.end();
        }
    }

    pub(super) fn end(&mut self) {
        self.follow = true;
        self.new_activity = false;
    }
}
