//! Local visit snapshots. The directory is the visit index; each file ends in a
//! fixed-width page-offset table, so reading a page never reads other pages.
use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const TURNS_PER_PAGE: usize = 32;
const MAGIC: &[u8; 8] = b"TUIVIS01";

pub(super) struct Visits {
    root: PathBuf,
    own: String,
    selected: Option<(String, u64)>,
    saved: Vec<(String, u64)>,
    pending: HashMap<String, u64>,
}

impl Visits {
    pub(super) fn open(data: &Path) -> io::Result<Self> {
        let root = data.join("tui-visits");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)?;
        let legacy = data.join("tui-session.json");
        let imported = root.join("00000000000000000000-legacy.visit");
        if legacy.exists() && !imported.exists() {
            // Deterministic destination makes interruption/retry idempotent. The
            // source remains untouched, including old archived PID identities.
            let list: Vec<SessionThread> = serde_json::from_reader(File::open(&legacy)?)?;
            write_snapshot(&imported, list)?;
        }
        let mut random = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        random[6] = (random[6] & 15) | 64;
        random[8] = (random[8] & 63) | 128;
        let hex: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let uuid = format!(
            "{}-{}-{}-{}-{}",
            &hex[..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..]
        );
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let own = format!("{started:020}-{uuid}.visit");
        let mut visits = Self {
            root,
            own,
            selected: None,
            saved: Vec::new(),
            pending: HashMap::new(),
        };
        // An older, still-open TUI may have checkpointed after a newer visit
        // closed. Inherit recovery state by checkpoint time, not open time.
        let mut checkpoint = None;
        for entry in fs::read_dir(&visits.root)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().ends_with(".visit") {
                let candidate = (entry.metadata()?.modified()?, entry.path());
                if checkpoint.as_ref().is_none_or(|best| candidate > *best) {
                    checkpoint = Some(candidate);
                }
            }
        }
        if let Some((_, previous)) = checkpoint {
            visits.pending = read_pending(&previous)?;
        }
        visits.save(&[Thread::new_foreground()])?;
        Ok(visits)
    }

    pub(super) fn save(&mut self, threads: &[Thread]) -> io::Result<()> {
        let revisions: Vec<_> = threads
            .iter()
            .filter(|t| !t.id.starts_with("visit:"))
            .map(|t| (t.id.clone(), t.revision))
            .collect();
        if revisions != self.saved {
            write_snapshot_pending(
                &self.root.join(&self.own),
                session_snapshot(threads),
                &self.pending,
            )?;
            self.saved = revisions;
        }
        Ok(())
    }

    pub(super) fn recovery(&self) -> Vec<(String, u64)> {
        self.pending
            .iter()
            .map(|(key, time)| (key.clone(), *time))
            .collect()
    }

    pub(super) fn observe(&mut self, event: &InteractionEventEnvelope) {
        let Some(turn) = event.metadata.turn_id.as_deref() else {
            return;
        };
        let key = conversation_turn(&event.metadata.conversation_id, turn);
        match event.event {
            InteractionEvent::UserTurnAccepted { .. } => {
                if !self.pending.contains_key(&key) {
                    self.pending.insert(key, event.metadata.occurred_at_ms);
                    self.saved.clear();
                }
            }
            InteractionEvent::ConversationFinished { .. } => self.recovered(&key),
            _ => {}
        }
    }

    pub(super) fn recovered(&mut self, key: &str) {
        if self.pending.remove(key).is_some() {
            self.saved.clear();
        }
    }

    // Bounded memory directory-index traversal. No archive contents are opened
    // here, and no ever-growing in-memory list of visit metadata is retained.
    fn neighbor(&self, key: &str, older: bool) -> io::Result<Option<String>> {
        let mut best: Option<String> = None;
        for entry in fs::read_dir(&self.root)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".visit") || name == self.own || name.as_str() >= self.own.as_str() {
                continue;
            }
            if (older && name.as_str() < key && best.as_ref().is_none_or(|b| name > *b))
                || (!older && name.as_str() > key && best.as_ref().is_none_or(|b| name < *b))
            {
                best = Some(name);
            }
        }
        Ok(best)
    }

    pub(super) fn latest(&mut self, threads: &mut Vec<Thread>) -> io::Result<()> {
        let previous = self.selected.take();
        match self.page(threads, true) {
            Ok(true) => Ok(()),
            result => {
                self.selected = previous;
                result.map(|_| ())
            }
        }
    }

    pub(super) fn toggle(&mut self, threads: &mut Vec<Thread>) -> io::Result<()> {
        toggle_history(threads);
        if foreground_thread(threads).is_some_and(|(_, thread)| !thread.hide_history) {
            self.latest(threads)?;
        }
        Ok(())
    }

    pub(super) fn select(
        &mut self,
        threads: &mut Vec<Thread>,
        selected: &mut Option<usize>,
        view: &mut TranscriptView,
        scroll: &mut TranscriptScroll,
        cache: &mut TurnLayoutCache,
        projection: &mut TurnProjection,
        direction: i8,
    ) -> io::Result<()> {
        let thread = &threads[0];
        projection.update(thread);
        let history = projection
            .cells
            .iter()
            .take_while(|c| c.prompt < thread.history_len)
            .count();
        let older = direction < 0;
        let boundary = !thread.hide_history
            && history > 0
            && if older {
                *selected == Some(0)
            } else {
                *selected == Some(history - 1)
            };
        // End and mouse navigation can leave an older page beside current turns.
        // Re-enter history at its newest turn rather than skipping newer pages.
        let entering = !thread.hide_history
            && history > 0
            && older
            && (*selected == Some(history)
                || (selected.is_none() && scroll.follow && history == projection.cells.len()));
        if entering {
            self.latest(threads)?;
        }
        if entering || (boundary && self.page(threads, older)?) {
            reset_transcript(scroll, view, cache, selected, projection);
            projection.update(&threads[0]);
            let history = projection
                .cells
                .iter()
                .take_while(|c| c.prompt < threads[0].history_len)
                .count();
            *selected = Some(if older { history - 1 } else { 0 });
            scroll.follow = false;
        } else {
            select_trace_turn(selected, view, scroll, direction);
        }
        Ok(())
    }

    pub(super) fn page(&mut self, threads: &mut Vec<Thread>, older: bool) -> io::Result<bool> {
        let mut selected = self.selected.clone();
        loop {
            let (mut key, mut page) = selected.clone().unwrap_or((self.own.clone(), 0));
            if selected.is_some() && older && page > 0 {
                page -= 1;
            } else if selected.is_some() && !older && page + 1 < page_count(&self.root.join(&key))?
            {
                page += 1;
            } else {
                loop {
                    let Some(next) = self.neighbor(&key, older)? else {
                        return Ok(false);
                    };
                    key = next;
                    let count = page_count(&self.root.join(&key))?;
                    if count > 0 {
                        page = if older { count - 1 } else { 0 };
                        break;
                    }
                }
            }
            let list = read_page(&self.root.join(&key), page)?;
            if !list.iter().any(|thread| {
                thread.is_foreground
                    && thread
                        .items
                        .iter()
                        .any(|item| item.kind == "user" || item.kind == "reply")
            }) {
                selected = Some((key, page));
                continue;
            }
            let mut timestamp = key
                .split('-')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
                / 1_000_000;
            if timestamp == 0 {
                timestamp = list
                    .iter()
                    .flat_map(|t| &t.items)
                    .map(|i| i.timestamp)
                    .filter(|time| *time > 0)
                    .min()
                    .unwrap_or(0);
                if timestamp < 10_000_000_000 {
                    timestamp *= 1000;
                }
            }
            let label = format!("Previous session {}", date_label(timestamp));
            install_page(threads, list, &key, label);
            self.selected = Some((key, page));
            return Ok(true);
        }
    }
}

fn write_snapshot(path: &Path, list: Vec<SessionThread>) -> io::Result<()> {
    write_snapshot_pending(path, list, &HashMap::new())
}

fn write_snapshot_pending(
    path: &Path,
    list: Vec<SessionThread>,
    pending: &HashMap<String, u64>,
) -> io::Result<()> {
    // A page is bounded in conversation turns, not bytes: one large response or
    // trace still costs its actual size. Partition once per changed active save.
    let foreground = list.iter().find(|t| t.is_foreground);
    let mut turn_pages = HashMap::new();
    let mut item_pages = Vec::new();
    let mut turns = 0usize;
    let mut page = 0usize;
    if let Some(thread) = foreground {
        let user_turns: HashSet<_> = thread
            .items
            .iter()
            .filter(|i| i.kind == "user")
            .filter_map(|i| i.turn.as_deref())
            .collect();
        for item in &thread.items {
            if item.kind == "user"
                || (item.kind == "reply"
                    && item.turn.as_deref().is_none_or(|t| !user_turns.contains(t)))
            {
                page = turns / TURNS_PER_PAGE;
                turns += 1;
            }
            if let Some(turn) = &item.turn {
                if item.kind == "user" {
                    turn_pages.insert(turn.clone(), page);
                } else {
                    turn_pages.entry(turn.clone()).or_insert(page);
                }
            }
            item_pages.push(page);
        }
    }
    let count = if list.iter().all(|t| t.items.is_empty()) {
        0
    } else {
        page + 1
    };
    let mut pages: Vec<Vec<SessionThread>> = (0..count).map(|_| Vec::new()).collect();
    for mut thread in list {
        let items = std::mem::take(&mut thread.items);
        let mut partitions: Vec<Vec<SessionItem>> = (0..count).map(|_| Vec::new()).collect();
        for (index, item) in items.into_iter().enumerate() {
            let target = item
                .turn
                .as_ref()
                .and_then(|t| turn_pages.get(t))
                .copied()
                .unwrap_or_else(|| {
                    if thread.is_foreground {
                        item_pages[index]
                    } else {
                        0
                    }
                });
            partitions[target].push(item);
        }
        for (index, items) in partitions.into_iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            let turns: HashSet<_> = items.iter().filter_map(|i| i.turn.as_ref()).collect();
            pages[index].push(SessionThread {
                id: thread.id.clone(),
                parent: thread.parent.clone(),
                task: thread.task.clone(),
                is_foreground: thread.is_foreground,
                completed_turns: thread
                    .completed_turns
                    .iter()
                    .filter(|t| turns.contains(t))
                    .cloned()
                    .collect(),
                unread_turns: BTreeSet::new(),
                metrics: thread
                    .metrics
                    .iter()
                    .filter(|(t, _)| turns.contains(t))
                    .map(|(t, m)| (t.clone(), m.clone()))
                    .collect(),
                items,
            });
        }
    }
    // Concurrent legacy imports can target the same destination, even within
    // one process. Never truncate another writer's temporary file.
    static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let serial = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temporary = path.with_extension(format!("{}.{serial}.tmp", std::process::id()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
        {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        let mut offsets = Vec::new();
        for page in pages {
            offsets.push(file.stream_position()?);
            serde_json::to_writer(&mut file, &page)?;
            file.write_all(b"\n")?;
        }
        offsets.push(file.stream_position()?);
        serde_json::to_writer(&mut file, pending)?;
        for offset in offsets {
            file.write_all(&offset.to_le_bytes())?;
        }
        file.write_all(&(count as u64).to_le_bytes())?;
        file.write_all(MAGIC)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().unwrap())?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn index(file: &mut File) -> io::Result<(u64, u64)> {
    let len = file.metadata()?.len();
    if len < 24 {
        return Err(io::Error::other("truncated visit archive"));
    }
    file.seek(SeekFrom::End(-16))?;
    let mut footer = [0; 16];
    file.read_exact(&mut footer)?;
    if &footer[8..] != MAGIC {
        return Err(io::Error::other("unknown visit archive format"));
    }
    let count = u64::from_le_bytes(footer[..8].try_into().unwrap());
    let start = count
        .checked_add(1)
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(16))
        .and_then(|n| len.checked_sub(n))
        .ok_or_else(|| io::Error::other("invalid visit page index"))?;
    Ok((count, start))
}

fn page_count(path: &Path) -> io::Result<u64> {
    index(&mut File::open(path)?).map(|(count, _)| count)
}

fn read_pending(path: &Path) -> io::Result<HashMap<String, u64>> {
    let mut file = File::open(path)?;
    let (count, start) = index(&mut file)?;
    file.seek(SeekFrom::Start(start + count * 8))?;
    let mut offset = [0; 8];
    file.read_exact(&mut offset)?;
    let offset = u64::from_le_bytes(offset);
    if offset > start {
        return Err(io::Error::other("invalid continuation metadata offset"));
    }
    file.seek(SeekFrom::Start(offset))?;
    Ok(serde_json::from_reader(file.take(start - offset))?)
}

fn read_page(path: &Path, page: u64) -> io::Result<Vec<SessionThread>> {
    let mut file = File::open(path)?;
    let (count, start) = index(&mut file)?;
    if page >= count {
        return Err(io::Error::other("visit page out of range"));
    }
    file.seek(SeekFrom::Start(start + page * 8))?;
    let mut offsets = [0; 16];
    file.read_exact(&mut offsets)?;
    let begin = u64::from_le_bytes(offsets[..8].try_into().unwrap());
    let end = u64::from_le_bytes(offsets[8..].try_into().unwrap());
    if begin > end || end > start {
        return Err(io::Error::other("invalid visit page offsets"));
    }
    file.seek(SeekFrom::Start(begin))?;
    Ok(serde_json::from_reader(BufReader::new(
        file.take(end - begin),
    ))?)
}

fn install_page(threads: &mut Vec<Thread>, list: Vec<SessionThread>, key: &str, label: String) {
    threads.retain(|t| !t.id.starts_with("visit:"));
    for thread in threads.iter_mut() {
        thread.items.drain(..thread.history_len);
        thread.history_len = 0;
        thread.metrics.retain(|id, _| !id.starts_with("visit:"));
        thread
            .completed_turns
            .retain(|id| !id.starts_with("visit:"));
        thread
            .metric_revisions
            .retain(|id, _| !id.starts_with("visit:"));
        thread.touch_structure();
    }
    for mut old in restore_session(list) {
        let prefix = format!("visit:{key}:");
        let mut uncorrelated = 0;
        for item in &mut old.items {
            if item.kind == ItemKind::User {
                uncorrelated += 1;
            }
            item.turn = Some(format!(
                "{prefix}{}",
                item.turn
                    .as_deref()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("local-{uncorrelated}"))
            ));
        }
        old.metrics = old
            .metrics
            .into_iter()
            .map(|(id, value)| (format!("{prefix}{id}"), value))
            .collect();
        old.completed_turns = old
            .completed_turns
            .into_iter()
            .map(|id| format!("{prefix}{id}"))
            .collect();
        old.unread_turns.clear();
        old.history_len = old.items.len();
        if old.is_foreground {
            let live = threads.iter_mut().find(|t| t.is_foreground).unwrap();
            old.items.append(&mut live.items);
            live.items = old.items;
            live.history_len = old.history_len;
            live.history_label = Some(label.clone());
            live.metrics.extend(old.metrics);
            live.completed_turns.extend(old.completed_turns);
            live.touch_structure();
        } else {
            old.id = format!("{prefix}{}", old.id);
            threads.push(old);
        }
    }
}

pub(super) fn display_turn(mut turn: &str) -> &str {
    if let Some(rest) = turn.strip_prefix("visit:") {
        turn = rest.split_once(':').map(|(_, id)| id).unwrap_or(rest);
    }
    while let Some(rest) = turn.strip_prefix("archived:") {
        turn = rest.split_once(':').map(|(_, id)| id).unwrap_or(rest);
    }
    if turn.starts_with("conversation:") {
        turn = turn.rsplit_once(':').map(|(_, turn)| turn).unwrap_or(turn);
    }
    turn
}

pub(super) fn conversation_turn(conversation: &str, turn: &str) -> String {
    format!("conversation:{conversation}:{turn}")
}

#[cfg(test)]
fn pending_continuations(threads: &[Thread]) -> Vec<(String, u64)> {
    let Some((_, thread)) = foreground_thread(threads) else {
        return Vec::new();
    };
    let mut pending = HashMap::<String, u64>::new();
    for item in &thread.items[..thread.history_len] {
        let Some(turn) = item.turn.as_ref() else {
            continue;
        };
        if thread.completed_turns.contains(turn) {
            continue;
        }
        let Some((_, original)) = turn.strip_prefix("visit:").and_then(|s| s.split_once(':'))
        else {
            continue;
        };
        // Legacy numeric turns have no conversation identity. Never guess using
        // text or a daemon PID, which can attach a different conversation reply.
        if original.starts_with("conversation:") {
            pending
                .entry(original.to_owned())
                .and_modify(|time| *time = (*time).min(item.timestamp))
                .or_insert(item.timestamp);
        }
    }
    pending.into_iter().collect()
}

pub(super) fn recover_continuations(
    pending: &[(String, u64)],
    to_ui: &mpsc::Sender<TuiEvent>,
) -> Result<(), String> {
    use tachyon_api::types::ApiRequest;
    let mut client = Client::connect().map_err(|e| e.to_string())?;
    recover_pages(
        pending,
        now_seconds().saturating_add(1),
        |since_ms, until_ms, limit| {
            let response = client
                .request(
                    &ApiRequest::HistoryQuery {
                        since_ms,
                        until_ms,
                        limit,
                    },
                    Duration::from_secs(5),
                )
                .map_err(|e| e.to_string())?;
            let ApiResponse::History { entries } = response else {
                return Err("unexpected history response".into());
            };
            Ok(entries)
        },
        |entry| {
            to_ui
                .send(TuiEvent::Recovered(entry))
                .map_err(|e| e.to_string())
        },
    )
}

fn recover_pages(
    pending: &[(String, u64)],
    until: u64,
    mut query: impl FnMut(u64, u64, u32) -> Result<Vec<tachyon_api::types::HistoryEntry>, String>,
    mut publish: impl FnMut(tachyon_api::types::HistoryEntry) -> Result<(), String>,
) -> Result<(), String> {
    use tachyon_api::types::{HistoryKind, HistoryRole};
    let since = pending
        .iter()
        .map(|(_, time)| *time)
        .min()
        .unwrap_or(now_seconds())
        .saturating_sub(60_000);
    let mut windows = vec![(since, until)];
    while let Some((since_ms, until_ms)) = windows.pop() {
        let limit = if until_ms.saturating_sub(since_ms) <= 1 {
            1000
        } else {
            256
        };
        let entries = query(since_ms, until_ms, limit)?;
        if entries.len() >= limit as usize {
            if until_ms.saturating_sub(since_ms) <= 1 {
                return Err("history timestamp exceeds the API page limit; continuation recovery incomplete".into());
            }
            let middle = since_ms + (until_ms - since_ms) / 2;
            windows.push((middle, until_ms));
            windows.push((since_ms, middle));
            continue;
        }
        for entry in entries {
            if entry.kind != HistoryKind::Conversation || entry.role != HistoryRole::Assistant {
                continue;
            }
            let Some(turn) = entry.turn_id.as_deref() else {
                continue;
            };
            let key = conversation_turn(&entry.conversation_id, turn);
            if pending.iter().any(|(wanted, _)| *wanted == key) {
                publish(entry)?;
            }
        }
    }
    Ok(())
}

pub(super) fn date_label(ms: u64) -> String {
    if ms == 0 {
        return "legacy (date unknown)".into();
    }
    // Gregorian civil date from Unix days, UTC (no locale/timezone dependency).
    let days = (ms / 86_400_000) as i64 + 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let minutes = ms / 60_000;
    format!(
        "{day:02}-{month:02}-{year:04} {:02}:{:02}",
        minutes / 60 % 24,
        minutes % 60
    )
}

pub(super) fn separator(label: &str, width: u16) -> Line<'static> {
    let width = width as usize;
    let mut label = label.to_owned();
    while Line::raw(&label).width() > width.saturating_sub(2) {
        if label.pop().is_none() {
            break;
        }
    }
    let label = if width >= 2 {
        format!(" {label} ")
    } else {
        String::new()
    };
    let remaining = width.saturating_sub(Line::raw(&label).width());
    let style = Style::default()
        .fg(Color::DarkGray)
        .add_modifier(Modifier::DIM)
        .remove_modifier(Modifier::BOLD);
    Line::from(Span::styled(
        format!(
            "{}{label}{}",
            " ".repeat(remaining / 2),
            " ".repeat(remaining - remaining / 2)
        ),
        style,
    ))
    .style(style)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = PathBuf::from("/tmp/opencode").join(format!(
                "tui-visits-{}-{}-{}",
                std::process::id(),
                now_seconds(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn conversation(count: usize) -> Vec<Thread> {
        let mut thread = Thread::new_foreground();
        for index in 0..count {
            let turn = conversation_turn("test", &index.to_string());
            thread.add_turn(
                ItemKind::User,
                format!("question {index}"),
                Some(turn.clone()),
            );
            thread.finish_reply(format!("answer {index}"), Some(turn));
        }
        vec![thread]
    }

    #[test]
    fn centered_separators_fit_unicode_cells_and_date_both_sessions() {
        let label = "Previous session 08-07-2026 12:34";
        let line = separator(label, 80).to_string();
        let (left, right) = line.split_once(&format!(" {label} ")).unwrap();
        assert!(left.chars().all(|c| c == ' '));
        assert!(right.chars().all(|c| c == ' '));
        assert!(left.chars().count().abs_diff(right.chars().count()) <= 1);
        for width in 0..90 {
            assert_eq!(
                separator("Previous session 界 e\u{301} 08-07-2026 12:34", width).width(),
                width as usize
            );
        }
        let mut threads = conversation(1);
        threads[0].session_started = 1_783_468_800_000 + 45_296_000;
        install_page(
            &mut threads,
            session_snapshot(&conversation(1)),
            "old",
            label.into(),
        );
        for hidden in [false, true] {
            threads[0].hide_history = hidden;
            let cells = build_turn_cells(&threads[0]);
            assert_eq!(cells.len(), if hidden { 1 } else { 2 });
            for (n, cell) in cells.iter().enumerate() {
                let layout = turn_cell_layout(0, n, &threads, cell, 80, 0, false, "", false, None);
                if hidden {
                    assert!(!layout
                        .lines
                        .iter()
                        .any(|line| line.to_string().contains("session")));
                    continue;
                }
                let expected = if cell.prompt < threads[0].history_len {
                    label
                } else {
                    "Current session 08-07-2026 12:34"
                };
                assert!(layout.lines[1].to_string().contains(expected));
                let separator = &layout.lines[1];
                for style in std::iter::once(separator.style)
                    .chain(separator.spans.iter().map(|span| span.style))
                {
                    assert_eq!(style.fg, Some(Color::DarkGray));
                    assert!(style.add_modifier.contains(Modifier::DIM));
                    assert!(style.sub_modifier.contains(Modifier::BOLD));
                }
                let mut buffer = ratatui::buffer::Buffer::empty(Rect::new(0, 0, 80, 1));
                ratatui::widgets::Widget::render(
                    Paragraph::new(separator.clone()).style(
                        Style::default()
                            .fg(Color::White)
                            .add_modifier(Modifier::BOLD),
                    ),
                    buffer.area,
                    &mut buffer,
                );
                for cell in &buffer.content {
                    assert_eq!(cell.fg, Color::DarkGray);
                    assert!(cell.modifier.contains(Modifier::DIM));
                    assert!(!cell.modifier.contains(Modifier::BOLD));
                }
                assert!(layout.lines[0].to_string().is_empty());
                assert!(layout.lines[2].to_string().is_empty());
                assert!(layout.hits[..3].iter().all(Option::is_none));
            }
        }
    }

    #[test]
    fn session_labels_require_visible_history() {
        for width in [1, 12, 80] {
            for archived in [false, true] {
                for current in [false, true] {
                    let mut threads = conversation(usize::from(current));
                    threads[0].session_started = 1_783_468_800_000;
                    if archived {
                        install_page(
                            &mut threads,
                            session_snapshot(&conversation(1)),
                            "old",
                            "Previous session 08-07-2026 00:00".into(),
                        );
                    }
                    let mut terminal =
                        ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, 40))
                            .unwrap();
                    let mut cache = TurnLayoutCache::default();
                    let mut projection = TurnProjection::default();
                    let mut view = TranscriptView::default();
                    let mut scroll = TranscriptScroll::default();
                    for hidden in [false, true, false] {
                        if threads[0].hide_history != hidden {
                            toggle_history(&mut threads);
                        }
                        let mut selected = None;
                        reset_transcript(
                            &mut scroll,
                            &mut view,
                            &mut cache,
                            &mut selected,
                            &mut projection,
                        );
                        terminal
                            .draw(|f| {
                                draw_conversation(
                                    f,
                                    f.area(),
                                    &threads,
                                    false,
                                    "",
                                    &mut scroll,
                                    &mut cache,
                                    &mut view,
                                    None,
                                    None,
                                    &mut projection,
                                );
                            })
                            .unwrap();
                        let screen: String = terminal
                            .backend()
                            .buffer()
                            .content
                            .iter()
                            .map(|cell| cell.symbol())
                            .collect();
                        assert!(!screen.contains('─'));
                        if width == 80 {
                            for label in ["Previous session", "Current session"] {
                                assert_eq!(screen.contains(label), archived && !hidden);
                            }
                        }
                        if !current && (!archived || hidden) {
                            assert_eq!(view.total_height, 0);
                            assert!(screen.trim().is_empty());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn arrows_cross_pages_without_skips_and_live_render_reuses_history() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        first.save(&conversation(67)).unwrap();
        let mut visits = Visits::open(&directory.0).unwrap();
        let mut threads = conversation(1);
        threads[0].items[0].text = "CURRENT".into();
        visits.latest(&mut threads).unwrap();
        let mut projection = TurnProjection::default();
        let mut cache = TurnLayoutCache::default();
        let mut view = TranscriptView::default();
        let mut scroll = TranscriptScroll::default();
        let mut selected = None;
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap();
        let check_copy = |threads: &[Thread], selected, question: &str| {
            let answer = question.strip_prefix("question ").unwrap_or("0");
            let mut copied = None;
            assert!(handle_copy_key(
                event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
                MouseCapture::default(),
                false,
                "",
                threads,
                selected,
                |text| {
                    copied = Some(text.to_owned());
                    true
                },
            ));
            assert_eq!(
                copied,
                Some(format!(
                    "{}:\n{question}\n\n{}:\nanswer {answer}",
                    names().user,
                    names().conversation
                ))
            );
        };
        let mut draw = |threads: &[Thread],
                        selected,
                        scroll: &mut TranscriptScroll,
                        cache: &mut TurnLayoutCache,
                        view: &mut TranscriptView,
                        projection: &mut TurnProjection| {
            terminal
                .draw(|f| {
                    draw_conversation(
                        f,
                        f.area(),
                        threads,
                        false,
                        "",
                        scroll,
                        cache,
                        view,
                        selected,
                        None,
                        projection,
                    )
                })
                .unwrap();
        };
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        // Start with current, then visit every archived turn in reverse order.
        for expected in std::iter::once("CURRENT".to_owned())
            .chain((0..67).rev().map(|n| format!("question {n}")))
        {
            visits
                .select(
                    &mut threads,
                    &mut selected,
                    &mut view,
                    &mut scroll,
                    &mut cache,
                    &mut projection,
                    -1,
                )
                .unwrap();
            draw(
                &threads,
                selected,
                &mut scroll,
                &mut cache,
                &mut view,
                &mut projection,
            );
            let n = selected.unwrap();
            assert_eq!(threads[0].items[projection.cells[n].prompt].text, expected);
            check_copy(&threads, selected, &expected);
            assert!(scroll.top <= view.starts[n]);
            assert!(scroll.top + view.viewport > view.starts[n]);
            assert!(threads[0].history_len <= TURNS_PER_PAGE * 2);
        }
        for expected in (1..67)
            .map(|n| format!("question {n}"))
            .chain(std::iter::once("CURRENT".to_owned()))
        {
            visits
                .select(
                    &mut threads,
                    &mut selected,
                    &mut view,
                    &mut scroll,
                    &mut cache,
                    &mut projection,
                    1,
                )
                .unwrap();
            draw(
                &threads,
                selected,
                &mut scroll,
                &mut cache,
                &mut view,
                &mut projection,
            );
            assert_eq!(
                threads[0].items[projection.cells[selected.unwrap()].prompt].text,
                expected
            );
            check_copy(&threads, selected, &expected);
        }
        selected = None;
        threads[0].add_turn(ItemKind::User, "stream".into(), Some("stream".into()));
        threads[0].add_reply_fragment("first".into(), Some("stream".into()), false);
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        for _ in 0..3 {
            let builds = cache.builds;
            threads[0].add_reply_fragment(" delta".into(), Some("stream".into()), false);
            draw(
                &threads,
                selected,
                &mut scroll,
                &mut cache,
                &mut view,
                &mut projection,
            );
            assert_eq!(cache.builds, builds + 1);
        }
        let before = serde_json::to_vec(&session_snapshot(&threads)).unwrap();
        visits.toggle(&mut threads).unwrap();
        reset_transcript(
            &mut scroll,
            &mut view,
            &mut cache,
            &mut selected,
            &mut projection,
        );
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        assert!(scroll.follow);
        assert!(selected.is_none());
        assert_eq!(projection.cells.len(), 2);
        check_copy(&threads, Some(0), "CURRENT");
        let mut copied = None;
        assert!(handle_copy_key(
            event::KeyEvent::new(
                KeyCode::Char('C'),
                KeyModifiers::CONTROL | KeyModifiers::SHIFT
            ),
            MouseCapture(true),
            false,
            "",
            &threads,
            None,
            |text| {
                copied = Some(text.to_owned());
                true
            },
        ));
        assert_eq!(
            copied,
            Some(format!(
                "{}:\nstream\n\n{}:\nfirst delta delta delta",
                names().user,
                names().conversation
            ))
        );
        assert!(projection
            .cells
            .iter()
            .all(|c| c.prompt >= threads[0].history_len));
        assert_eq!(
            serde_json::to_vec(&session_snapshot(&threads)).unwrap(),
            before
        );
        threads = vec![Thread::new_foreground()];
        threads[0].session_started = 1_783_468_800_000;
        reset_transcript(
            &mut scroll,
            &mut view,
            &mut cache,
            &mut selected,
            &mut projection,
        );
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        assert_eq!(view.total_height, 0);
        assert_eq!(view.turns, 0);
        visits.latest(&mut threads).unwrap();
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        let builds = cache.builds;
        threads[0].add_turn(
            ItemKind::User,
            "first current turn".into(),
            Some("fresh".into()),
        );
        draw(
            &threads,
            selected,
            &mut scroll,
            &mut cache,
            &mut view,
            &mut projection,
        );
        assert_eq!(cache.builds, builds + 1);
        let screen: String = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect();
        assert!(screen.contains("Current session 08-07-2026 00:00"));
    }

    #[test]
    fn two_turn_protocol_pending_checkpoint_and_paged_copy() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        let events = crate::tests::two_turn_fixture();
        // Put the two fixture turns on opposite sides of a real archive page boundary.
        let mut threads = conversation(TURNS_PER_PAGE - 1);
        for (step, event) in events.iter().enumerate() {
            first.observe(event);
            let mut projected = event.clone();
            projected.metadata.turn_id = event
                .metadata
                .turn_id
                .as_deref()
                .map(|turn| conversation_turn(&event.metadata.conversation_id, turn));
            apply_interaction_event(&mut threads[0], projected);
            for item in &mut threads[0].items {
                item.timestamp = 100;
            }
            first.save(&threads).unwrap();
            if step == 4 {
                let expected = vec![(conversation_turn("fixture", "2"), 100)];
                assert_eq!(first.recovery(), expected);
                assert_eq!(
                    read_pending(&first.root.join(&first.own)).unwrap(),
                    expected.into_iter().collect()
                );
                let page = read_page(&first.root.join(&first.own), 0).unwrap();
                assert_eq!(page[0].items.last().unwrap().text, "Checking α\n\n");
            }
        }
        assert!(first.recovery().is_empty());
        assert_eq!(page_count(&first.root.join(&first.own)).unwrap(), 2);
        let expected = [
            format!("{}:\n  first α\n\n\n{}:\n# Corrected α\n\n- one\n\n```text\n  exact  \n```\n\n", names().user, names().conversation),
            format!("{}:\nsecond 界\r\n\n\n{}:\n  **second** 界\r\n\r\n```rust\r\n    ready();  \r\n```\r\n", names().user, names().conversation),
        ];
        let mut visits = Visits::open(&directory.0).unwrap();
        let mut restored = vec![Thread::new_foreground()];
        visits.latest(&mut restored).unwrap();
        let mut terminal = Terminal::new(ratatui::backend::TestBackend::new(100, 60)).unwrap();
        let mut cache = TurnLayoutCache::default();
        let mut projection = TurnProjection::default();
        let mut view = TranscriptView::default();
        let mut scroll = TranscriptScroll::default();
        for (index, selected) in [(1, 0), (0, TURNS_PER_PAGE - 1), (1, 0)] {
            if index == 0 {
                assert!(visits.page(&mut restored, true).unwrap());
            } else if visits.selected.as_ref().unwrap().1 == 0 {
                assert!(visits.page(&mut restored, false).unwrap());
            }
            let mut open = Some(selected);
            reset_transcript(
                &mut scroll,
                &mut view,
                &mut cache,
                &mut open,
                &mut projection,
            );
            for redraw in 0..2 {
                let builds = cache.builds;
                terminal
                    .draw(|f| {
                        draw_conversation(
                            f,
                            f.area(),
                            &restored,
                            false,
                            "",
                            &mut scroll,
                            &mut cache,
                            &mut view,
                            Some(selected),
                            None,
                            &mut projection,
                        );
                    })
                    .unwrap();
                if redraw == 1 {
                    assert_eq!(cache.builds, builds);
                }
                assert_eq!(
                    selected_chat_cell_text(&restored, Some(selected)),
                    Some(expected[index].clone())
                );
                let mut copied = Vec::new();
                assert!(handle_copy_key(
                    event::KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
                    MouseCapture::default(),
                    false,
                    "",
                    &restored,
                    Some(selected),
                    |text| {
                        copied.push(text.to_owned());
                        true
                    },
                ));
                assert_eq!(copied, [expected[index].clone()]);
            }
        }
    }

    #[test]
    fn pending_history_publishes_independent_finals_without_foreground_events() {
        use tachyon_api::types::{HistoryEntry, HistoryKind, HistoryRole};
        let events = crate::tests::two_turn_fixture();
        let entries: Vec<_> = events
            .iter()
            .filter_map(|event| {
                let InteractionEvent::ConversationFinished { text } = &event.event else {
                    return None;
                };
                Some(HistoryEntry {
                    event_id: event.metadata.message_id.clone(),
                    kind: HistoryKind::Conversation,
                    conversation_id: event.metadata.conversation_id.clone(),
                    turn_id: event.metadata.turn_id.clone(),
                    occurred_at_ms: event.metadata.occurred_at_ms,
                    role: HistoryRole::Assistant,
                    text: text.clone(),
                    task_id: None,
                    task_state: None,
                })
            })
            .collect();
        let pending = [
            (conversation_turn("fixture", "2"), 100),
            (conversation_turn("fixture", "3"), 101),
        ];
        for available in 1..=entries.len() {
            let mut published = Vec::new();
            recover_pages(
                &pending,
                200,
                |since, until, limit| {
                    assert_eq!((since, until, limit), (0, 200, 256));
                    let mut page = entries[..available].to_vec();
                    for (kind, role) in [
                        (HistoryKind::Task, HistoryRole::Assistant),
                        (HistoryKind::Conversation, HistoryRole::Notification),
                        (HistoryKind::Conversation, HistoryRole::User),
                    ] {
                        let mut noise = entries[0].clone();
                        noise.kind = kind;
                        noise.role = role;
                        noise.text = "RAW EVIDENCE OR NOTIFICATION".into();
                        page.push(noise);
                    }
                    Ok(page)
                },
                |entry| {
                    published.push(entry);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(published, entries[..available]);
            assert_eq!(published[0].turn_id.as_deref(), Some("3"));
            if available == 3 {
                assert_eq!(published[1].event_id, "finish-2");
                assert_eq!(published[2].event_id, "correct-2");
                assert_ne!(published[1].text, published[2].text);
            }
        }
    }

    #[test]
    fn end_then_up_without_current_turns_returns_to_latest_archive() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        first.save(&conversation(67)).unwrap();
        let mut visits = Visits::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        visits.latest(&mut threads).unwrap();
        visits.page(&mut threads, true).unwrap();
        assert_eq!(visits.selected.as_ref().unwrap().1, 1);
        let mut selected = None;
        let mut view = TranscriptView::default();
        let mut scroll = TranscriptScroll::default();
        scroll.end();
        let mut cache = TurnLayoutCache::default();
        let mut projection = TurnProjection::default();
        visits
            .select(
                &mut threads,
                &mut selected,
                &mut view,
                &mut scroll,
                &mut cache,
                &mut projection,
                -1,
            )
            .unwrap();
        assert_eq!(visits.selected.as_ref().unwrap().1, 2);
        assert_eq!(
            threads[0].items[projection.cells[selected.unwrap()].prompt].text,
            "question 66"
        );
        assert!(threads[0].history_len <= TURNS_PER_PAGE * 2);
    }

    #[test]
    fn live_unmatched_tool_output_does_not_mutate_loaded_history() {
        let mut previous = conversation(1);
        previous[0].add_tool("old tool".into(), "old-id".into(), Some("0".into()));
        let mut threads = vec![Thread::new_foreground()];
        install_page(
            &mut threads,
            session_snapshot(&previous),
            "old",
            "previous".into(),
        );
        threads[0].add_tool_result("new-id".into(), "live output".into(), Some("1".into()));
        assert!(threads[0].items[2].output.is_none());
        let saved = session_snapshot(&threads);
        assert_eq!(saved[0].items.len(), 1);
        assert_eq!(saved[0].items[0].text, "live output");
    }

    #[test]
    fn visit_dates_follow_each_filename_and_unselected_pages_are_not_decoded() {
        let directory = Directory::new();
        let mut visits = Visits::open(&directory.0).unwrap();
        for ms in [1_783_468_800_000u64, 1_783_555_200_000] {
            let path = visits
                .root
                .join(format!("{:020}-dated.visit", ms * 1_000_000));
            write_snapshot(&path, session_snapshot(&conversation(100))).unwrap();
            // Break the first page's JSON; reading the last page must still work.
            OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .write_all(b"!")
                .unwrap();
        }
        let mut threads = vec![Thread::new_foreground()];
        visits.latest(&mut threads).unwrap();
        assert!(threads[0]
            .history_label
            .as_ref()
            .unwrap()
            .contains("09-07-2026 00:00"));
        visits.selected.as_mut().unwrap().1 = 0;
        visits.page(&mut threads, true).unwrap();
        assert!(threads[0]
            .history_label
            .as_ref()
            .unwrap()
            .contains("08-07-2026 00:00"));
        assert_eq!(threads[0].history_len, 8);
    }

    #[test]
    fn failed_navigation_keeps_the_displayed_page_cursor() {
        let directory = Directory::new();
        let mut visits = Visits::open(&directory.0).unwrap();
        let mut blank = vec![Thread::new_foreground()];
        blank[0].add(ItemKind::System, "no conversation".into());
        write_snapshot(
            &visits.root.join("00000000000000000001-empty.visit"),
            session_snapshot(&blank),
        )
        .unwrap();
        write_snapshot(
            &visits.root.join("00000000000000000002-visible.visit"),
            session_snapshot(&conversation(1)),
        )
        .unwrap();
        let mut threads = vec![Thread::new_foreground()];
        visits.latest(&mut threads).unwrap();
        let selected = visits.selected.clone();
        assert!(!visits.page(&mut threads, true).unwrap());
        assert_eq!(visits.selected, selected);
        assert!(!visits.page(&mut threads, false).unwrap());
        assert_eq!(visits.selected, selected);
        fs::write(
            visits.root.join("00000000000000000000-broken.visit"),
            b"bad",
        )
        .unwrap();
        assert!(visits.page(&mut threads, true).is_err());
        assert_eq!(visits.selected, selected);
        assert_eq!(threads[0].items[0].text, "question 0");
    }

    #[test]
    fn concurrent_snapshot_writers_use_independent_temporary_files() {
        let directory = Directory::new();
        let path = directory.0.join("legacy.visit");
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let path = &path;
                let barrier = &barrier;
                scope.spawn(move || {
                    let snapshot = session_snapshot(&conversation(100));
                    barrier.wait();
                    write_snapshot(path, snapshot).unwrap();
                });
            }
        });
        assert_eq!(page_count(&path).unwrap(), 4);
        assert_eq!(read_page(&path, 0).unwrap()[0].items.len(), 64);
        assert_eq!(read_page(&path, 3).unwrap()[0].items.len(), 8);
    }

    #[test]
    fn identical_submissions_bind_fifo_without_reusing_archived_or_accepted_turns() {
        let mut threads = vec![Thread::new_foreground()];
        install_page(
            &mut threads,
            session_snapshot(&conversation(1)),
            "old",
            "previous".into(),
        );
        let thread = &mut threads[0];
        for _ in 0..2 {
            thread.add(ItemKind::User, "question 0".into());
            thread.reserve_reply();
        }
        let first = conversation_turn("same-daemon", "1");
        let second = conversation_turn("same-daemon", "2");
        accept_user_turn(thread, "question 0", Some(first.clone()));
        accept_user_turn(thread, "question 0", Some(first.clone()));
        assert!(thread.items[4].turn.is_none());
        accept_user_turn(thread, "question 0", Some(second.clone()));
        assert_eq!(thread.items[2].turn.as_ref(), Some(&first));
        assert_eq!(thread.items[3].turn.as_ref(), Some(&first));
        assert_eq!(thread.items[4].turn.as_ref(), Some(&second));
        assert_eq!(thread.items[5].turn.as_ref(), Some(&second));
        assert!(thread.items[0]
            .turn
            .as_ref()
            .unwrap()
            .starts_with("visit:old:"));
    }

    #[test]
    fn reopen_is_a_new_visit_without_copying_history_into_it() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        let first_threads = conversation(2);
        first.save(&first_threads).unwrap();
        let original = fs::read(first.root.join(&first.own)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        let mut second = Visits::open(&directory.0).unwrap();
        assert_ne!(first.own, second.own);
        let mut threads = vec![Thread::new_foreground()];
        second.latest(&mut threads).unwrap();
        assert_eq!(threads[0].history_len, 4);
        threads[0].add_turn(ItemKind::User, "new visit".into(), Some("9".into()));
        second.save(&threads).unwrap();
        let own = read_page(&second.root.join(&second.own), 0).unwrap();
        assert_eq!(own[0].items.len(), 1);
        assert_eq!(own[0].items[0].text, "new visit");
        assert_eq!(original, fs::read(first.root.join(&first.own)).unwrap());
        std::thread::sleep(Duration::from_millis(2));
        let mut third = Visits::open(&directory.0).unwrap();
        let mut reopened = vec![Thread::new_foreground()];
        third.latest(&mut reopened).unwrap();
        assert_eq!(reopened[0].items.len(), 1);
        third.save(&reopened).unwrap();
        assert_eq!(page_count(&third.root.join(&third.own)).unwrap(), 0);
        assert!(third.page(&mut reopened, true).unwrap());
        assert_eq!(reopened[0].items.len(), 4);
    }

    #[test]
    fn daemon_identity_change_and_hidden_checkpoint_preserve_all_visits() {
        let directory = Directory::new();
        let pid = directory.0.join("tui-daemon.pid");
        fs::write(&pid, b"42").unwrap();
        let mut first = Visits::open(&directory.0).unwrap();
        first.save(&conversation(67)).unwrap();
        let original = fs::read(first.root.join(&first.own)).unwrap();
        // Simulate a changed legacy daemon marker, without starting a daemon.
        fs::write(&pid, b"99").unwrap();
        let mut second = Visits::open(&directory.0).unwrap();
        let mut threads = conversation(1);
        second.latest(&mut threads).unwrap();
        second.toggle(&mut threads).unwrap();
        assert!(threads[0].hide_history);
        second.save(&threads).unwrap();
        assert_eq!(fs::read(first.root.join(&first.own)).unwrap(), original);
        let mut third = Visits::open(&directory.0).unwrap();
        let mut loaded = vec![Thread::new_foreground()];
        let mut turns = 0;
        while third.page(&mut loaded, true).unwrap() {
            turns += build_turn_cells(&loaded[0]).len();
            assert!(loaded[0].history_len <= TURNS_PER_PAGE * 2);
        }
        assert_eq!(turns, 68);
        assert_eq!(fs::read(first.root.join(&first.own)).unwrap(), original);
        assert_eq!(fs::read_dir(&first.root).unwrap().count(), 3);
    }

    #[test]
    fn legacy_import_preserves_current_and_pid_archived_data_once() {
        let directory = Directory::new();
        let mut threads = conversation(2);
        threads[0].items[0].turn = Some("archived:42:1".into());
        threads[0].items[1].turn = Some("archived:42:1".into());
        let legacy = serde_json::to_vec(&session_snapshot(&threads)).unwrap();
        fs::write(directory.0.join("tui-session.json"), &legacy).unwrap();
        let mut first = Visits::open(&directory.0).unwrap();
        let imported_path = first.root.join("00000000000000000000-legacy.visit");
        let imported = fs::read(&imported_path).unwrap();
        let mut loaded = vec![Thread::new_foreground()];
        first.latest(&mut loaded).unwrap();
        assert_eq!(loaded[0].history_len, 4);
        let _second = Visits::open(&directory.0).unwrap();
        assert_eq!(fs::read(imported_path).unwrap(), imported);
        assert_eq!(
            fs::read(directory.0.join("tui-session.json")).unwrap(),
            legacy
        );
        assert_eq!(
            display_turn(loaded[0].items[0].turn.as_deref().unwrap()),
            "1"
        );
    }

    #[test]
    fn atomic_snapshot_ignores_interrupted_temporary_and_independent_writers() {
        let directory = Directory::new();
        let mut a = Visits::open(&directory.0).unwrap();
        let mut b = Visits::open(&directory.0).unwrap();
        a.save(&conversation(1)).unwrap();
        b.save(&conversation(3)).unwrap();
        fs::write(
            a.root.join(&a.own).with_extension("interrupted.tmp"),
            b"partial json",
        )
        .unwrap();
        assert_eq!(
            read_page(&a.root.join(&a.own), 0).unwrap()[0].items.len(),
            2
        );
        assert_eq!(
            read_page(&b.root.join(&b.own), 0).unwrap()[0].items.len(),
            6
        );
        let before = fs::metadata(a.root.join(&a.own))
            .unwrap()
            .modified()
            .unwrap();
        a.save(&conversation(1)).unwrap();
        assert_eq!(
            fs::metadata(a.root.join(&a.own))
                .unwrap()
                .modified()
                .unwrap(),
            before
        );
    }

    #[test]
    fn indexed_pages_bound_old_turns_and_reach_the_oldest_of_thousand_visits() {
        let directory = Directory::new();
        let visits = Visits::open(&directory.0).unwrap();
        // An unreadable oldest archive proves startup does not deserialize it.
        fs::write(
            visits.root.join("00000000000000000000-broken.visit"),
            b"bad",
        )
        .unwrap();
        let source = directory.0.join("source");
        write_snapshot(&source, session_snapshot(&conversation(100))).unwrap();
        assert_eq!(page_count(&source).unwrap(), 4);
        assert_eq!(read_page(&source, 0).unwrap()[0].items.len(), 64);
        assert_eq!(read_page(&source, 3).unwrap()[0].items.len(), 8);
        for n in 0..1000 {
            fs::hard_link(
                &source,
                visits.root.join(format!("{n:020}-synthetic.visit")),
            )
            .unwrap();
        }
        let mut visits = Visits::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        visits.latest(&mut threads).unwrap();
        assert_eq!(threads[0].history_len, 8);
        assert!(visits
            .selected
            .as_ref()
            .unwrap()
            .0
            .contains("00999-synthetic"));
        visits.selected = Some((format!("{:020}-synthetic.visit", 1), 0));
        visits.page(&mut threads, true).unwrap();
        assert_eq!(
            visits.selected.as_ref().unwrap().0,
            "00000000000000000000-synthetic.visit"
        );
        assert!(threads[0].history_len <= TURNS_PER_PAGE * 2);
    }

    #[test]
    fn hide_is_nondestructive_and_current_selection_indexes_stay_valid() {
        let mut threads = conversation(1);
        threads[0].items[0].text = "CURRENT".into();
        install_page(
            &mut threads,
            session_snapshot(&conversation(3)),
            "old",
            "Previous session 11-09-2026 00:00".into(),
        );
        let before = serde_json::to_vec(&session_snapshot(&threads)).unwrap();
        let mut projection = TurnProjection::default();
        projection.update(&threads[0]);
        assert_eq!(projection.cells.len(), 4);
        toggle_history(&mut threads);
        projection.update(&threads[0]);
        assert_eq!(projection.cells.len(), 1);
        assert_eq!(projection.cells[0].prompt, 6);
        assert!(selected_chat_cell_text(&threads, Some(0))
            .unwrap()
            .contains("CURRENT"));
        let layout = turn_cell_layout(
            0,
            0,
            &threads,
            &projection.cells[0],
            80,
            0,
            false,
            "",
            true,
            None,
        );
        assert!(layout
            .hits
            .iter()
            .flatten()
            .any(|hit| matches!(hit, ClickTarget::TraceSummary(0))));
        assert_eq!(foreground_focus(&threads), 1);
        toggle_history(&mut threads);
        projection.update(&threads[0]);
        assert_eq!(projection.cells.len(), 4);
        assert_eq!(
            serde_json::to_vec(&session_snapshot(&threads)).unwrap(),
            before
        );
        let mut scroll = TranscriptScroll::default();
        scroll.follow = false;
        let mut view = TranscriptView::default();
        let mut cache = TurnLayoutCache::default();
        let mut selected = Some(3);
        reset_transcript(
            &mut scroll,
            &mut view,
            &mut cache,
            &mut selected,
            &mut projection,
        );
        assert!(scroll.follow);
        assert!(selected.is_none());
    }

    #[test]
    fn separators_are_dated_unclickable_and_old_layouts_survive_live_append() {
        assert_eq!(date_label(1_783_468_800_000), "08-07-2026 00:00");
        assert_eq!(
            date_label(1_783_468_800_000 + 45_296_000),
            "08-07-2026 12:34"
        );
        let mut threads = conversation(1);
        install_page(
            &mut threads,
            session_snapshot(&conversation(3)),
            "old",
            "Previous session 11-09-2026 00:00".into(),
        );
        let mut projection = TurnProjection::default();
        projection.update(&threads[0]);
        let mut cache = TurnLayoutCache::default();
        cache.prepare(80, &projection.cells, threads[0].structure_revision);
        for (n, cell) in projection.cells.iter().enumerate() {
            cache.layout(cell_key(cell), cell_revision(&threads[0], cell), 0, || {
                turn_cell_layout(0, n, &threads, cell, 80, 0, false, "", false, None)
            });
        }
        let first = &cache.layouts[&cell_key(&projection.cells[0])].layout;
        assert!(first.lines[1]
            .to_string()
            .contains("Previous session 11-09-2026 00:00"));
        assert!(first.hits[0].is_none());
        assert!(first.lines[0].to_string().is_empty());
        assert!(first.lines[2].to_string().is_empty());
        assert!(first.hits[..3].iter().all(Option::is_none));
        assert!(!first
            .lines
            .iter()
            .any(|line| line.to_string().contains("visit:old:")));
        let builds = cache.builds;
        threads[0].add_turn(ItemKind::User, "streaming".into(), Some("new".into()));
        threads[0].add_reply_fragment("delta".into(), Some("new".into()), false);
        projection.update(&threads[0]);
        cache.prepare(80, &projection.cells, threads[0].structure_revision);
        for (n, cell) in projection.cells.iter().enumerate() {
            cache.layout(cell_key(cell), cell_revision(&threads[0], cell), 0, || {
                turn_cell_layout(0, n, &threads, cell, 80, 0, false, "", false, None)
            });
        }
        assert_eq!(cache.builds, builds + 1);
        assert_eq!(cache.layouts.len(), 5);
    }

    #[test]
    fn continuation_keeps_old_identity_and_never_fills_a_stale_turn() {
        let mut previous = conversation(1);
        previous[0].completed_turns.clear();
        let mut threads = vec![Thread::new_foreground()];
        install_page(
            &mut threads,
            session_snapshot(&previous),
            "old",
            "previous".into(),
        );
        assert_eq!(pending_continuations(&threads).len(), 1);
        let old = threads[0].items[1].text.clone();
        let turn = conversation_turn("test", "0");
        threads[0].finish_reply("continued final".into(), Some(turn.clone()));
        threads[0].add_reply_fragment("stale queued delta".into(), Some(turn.clone()), false);
        threads[0].finish_reply("continued final".into(), Some(turn));
        threads[0].finish_reply(
            "different conversation".into(),
            Some(conversation_turn("other", "0")),
        );
        assert_eq!(threads[0].items[1].text, old);
        assert_eq!(threads[0].items.len(), 4);
        assert_eq!(threads[0].items[2].text, "continued final");
        assert_eq!(session_snapshot(&threads)[0].items.len(), 2);
    }

    #[test]
    fn outstanding_identity_survives_empty_visits_and_pages_without_the_prompt() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        first.pending.insert(conversation_turn("test", "0"), 123);
        first.save(&conversation(100)).unwrap();
        let second = Visits::open(&directory.0).unwrap();
        assert_eq!(
            second.recovery(),
            vec![(conversation_turn("test", "0"), 123)]
        );
        let mut third = Visits::open(&directory.0).unwrap();
        assert_eq!(third.recovery(), second.recovery());
        let mut threads = vec![Thread::new_foreground()];
        third.latest(&mut threads).unwrap();
        assert!(!threads[0].items.iter().any(|i| i.text == "question 0"));
        third.recovered(&conversation_turn("test", "0"));
        third.save(&threads).unwrap();
        let fourth = Visits::open(&directory.0).unwrap();
        assert!(fourth.recovery().is_empty());
    }

    #[test]
    fn typed_recovery_splits_full_pages_and_matches_conversation_not_turn_number() {
        use tachyon_api::types::{HistoryEntry, HistoryKind, HistoryRole};
        let entries: Vec<_> = (0..600)
            .map(|n| HistoryEntry {
                event_id: format!("event-{n}"),
                kind: HistoryKind::Conversation,
                conversation_id: if n == 555 { "wanted" } else { "other" }.into(),
                turn_id: Some("7".into()),
                occurred_at_ms: n,
                role: HistoryRole::Assistant,
                text: format!("reply {n}"),
                task_id: None,
                task_state: None,
            })
            .collect();
        let mut queries = 0;
        let mut recovered = Vec::new();
        recover_pages(
            &[(conversation_turn("wanted", "7"), 0)],
            600,
            |since, until, limit| {
                queries += 1;
                Ok(entries
                    .iter()
                    .filter(|e| e.occurred_at_ms >= since && e.occurred_at_ms < until)
                    .take(limit as usize)
                    .cloned()
                    .collect())
            },
            |entry| {
                recovered.push(entry);
                Ok(())
            },
        )
        .unwrap();
        assert!(queries > 1);
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].text, "reply 555");
        assert!(recover_pages(
            &[(conversation_turn("wanted", "7"), 0)],
            1,
            |_, _, _| Ok(vec![entries[0].clone(); 1000]),
            |_| Ok(())
        )
        .is_err());
    }

    #[test]
    fn blank_worker_only_visit_does_not_obscure_previous_conversation() {
        let directory = Directory::new();
        let mut first = Visits::open(&directory.0).unwrap();
        first.save(&conversation(1)).unwrap();
        let mut second = Visits::open(&directory.0).unwrap();
        let mut blank = vec![Thread::new_foreground()];
        let worker = find_or_create_thread(&mut blank, "worker", false, None);
        blank[worker].add(ItemKind::System, "worker exit".into());
        second.save(&blank).unwrap();
        let mut third = Visits::open(&directory.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        third.latest(&mut threads).unwrap();
        assert_eq!(threads[0].items[0].text, "question 0");
    }
}
