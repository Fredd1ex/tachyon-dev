//! Deterministic notices and operator receipts. No model or conversation turns.
use super::*;
use std::collections::BTreeMap;
#[cfg(test)]
use std::fs::{self, File, OpenOptions};
#[cfg(test)]
use std::io::Write;
#[cfg(test)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use tachyon_api::attention::{Attention, AttentionAcknowledgement, AttentionFrameMetadata};
use tachyon_api::todo::TodoScope;
use tachyon_api::types::{ApiRequest, HistoryEntry, HistoryKind, HistoryRole};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct Notice {
    pub event_id: String,
    pub frame: AttentionFrameMetadata,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct Receipts {
    seen: BTreeMap<String, AttentionFrameMetadata>,
    visible: BTreeSet<String>,
    confirmed: BTreeSet<String>,
    records: BTreeMap<String, Attention>,
}

#[derive(Clone)]
pub(super) struct State {
    data: Receipts,
    #[cfg(test)]
    path: PathBuf,
    dirty: bool,
    generation: u64,
    in_flight: bool,
    attempted: Option<Instant>,
}

pub(super) enum ResultEvent {
    Displayed(Vec<(TodoScope, String)>),
    Command(Result<(Vec<Attention>, String), String>),
}

pub(super) fn publication(envelope: &InteractionEventEnvelope) -> Option<HistoryEntry> {
    let InteractionEvent::UserVisibleNotificationPublished { text } = &envelope.event else {
        return None;
    };
    let frame = envelope.metadata.attention.clone()?;
    if envelope.metadata.turn_id.is_some() {
        return None;
    }
    Some(HistoryEntry {
        attention: Some(frame),
        event_id: envelope.metadata.message_id.clone(),
        kind: HistoryKind::Conversation,
        conversation_id: envelope.metadata.conversation_id.clone(),
        turn_id: None,
        occurred_at_ms: envelope.metadata.occurred_at_ms,
        role: HistoryRole::Notification,
        text: text.clone(),
        task_id: None,
        task_state: None,
    })
}

fn append(thread: &mut Thread, text: String, notice: Option<Notice>, timestamp: u64) {
    thread.touch_structure();
    // Do not glue overlays onto a model reply or change its streaming state.
    thread.items.push(Item {
        attention: notice,
        work: None,
        kind: ItemKind::Reply,
        text,
        hidden: false,
        output: None,
        tool_id: None,
        turn: None,
        timestamp,
        revision: thread.revision,
    });
}

impl State {
    pub(super) fn open(_root: &Path) -> io::Result<Self> {
        #[cfg(test)]
        let path = _root.join("tui-attention.json");
        // Durable bodies and display receipts belong to the daemon. Old local files
        // remain untouched and cannot suppress recovered canonical notifications.
        let data = Receipts::default();
        Ok(Self {
            data,
            #[cfg(test)]
            path,
            dirty: false,
            generation: 0,
            in_flight: false,
            attempted: None,
        })
    }

    #[cfg(test)]
    pub(super) fn save(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let temp = self
            .path
            .with_extension(format!("{}.tmp", std::process::id()));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&temp)?;
        serde_json::to_writer(&mut file, &self.data)?;
        file.flush()?;
        file.sync_all()?;
        fs::rename(temp, &self.path)?;
        File::open(self.path.parent().unwrap())?.sync_all()?;
        self.dirty = false;
        Ok(())
    }

    pub(super) fn receive(&mut self, entry: HistoryEntry, threads: &mut Vec<Thread>) -> bool {
        if entry.kind != HistoryKind::Conversation
            || entry.role != HistoryRole::Notification
            || entry.turn_id.is_some()
        {
            return false;
        }
        let Some(frame) = entry.attention else {
            return false;
        };
        if self.data.seen.contains_key(&entry.event_id) {
            return false;
        }
        let exists = threads.iter().flat_map(|t| &t.items).any(|item| {
            item.attention
                .as_ref()
                .is_some_and(|n| n.event_id == entry.event_id)
        });
        self.data.seen.insert(entry.event_id.clone(), frame.clone());
        self.dirty = true;
        self.generation += 1;
        if !exists {
            let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
            append(
                &mut threads[idx],
                entry.text,
                Some(Notice {
                    event_id: entry.event_id,
                    frame,
                }),
                entry.occurred_at_ms,
            );
        }
        !exists
    }

    pub(super) fn visible(&mut self, threads: &[Thread], hits: &[Hit], covered: bool) -> bool {
        if covered {
            return false;
        }
        let mut changed = false;
        for hit in hits.iter().flatten() {
            let ClickTarget::Attention(ti, ii) = hit else {
                continue;
            };
            let Some(item) = threads.get(*ti).and_then(|t| t.items.get(*ii)) else {
                continue;
            };
            let Some(notice) = &item.attention else {
                continue;
            };
            if item.hidden {
                continue;
            }
            self.data
                .seen
                .entry(notice.event_id.clone())
                .or_insert_with(|| notice.frame.clone());
            for id in &notice.frame.ids {
                changed |= self.data.visible.insert(id.clone());
            }
        }
        self.dirty |= changed;
        if changed {
            self.generation += 1;
            self.attempted = None;
        }
        changed
    }

    fn pending(&self) -> Vec<(TodoScope, String)> {
        let mut pending = BTreeMap::new();
        for frame in self.data.seen.values() {
            for id in &frame.ids {
                if self.data.visible.contains(id) && !self.data.confirmed.contains(id) {
                    pending.insert(id.clone(), frame.scope.clone());
                }
            }
        }
        pending
            .into_iter()
            .take(32)
            .map(|(id, scope)| (scope, id))
            .collect()
    }

    pub(super) fn retry(&mut self, out: &mpsc::Sender<TuiEvent>) {
        if self.in_flight
            || self
                .attempted
                .is_some_and(|at| at.elapsed() < Duration::from_secs(5))
        {
            return;
        }
        self.attempted = Some(Instant::now());
        let pending = self.pending();
        if pending.is_empty() {
            return;
        }
        self.in_flight = true;
        let out = out.clone();
        std::thread::spawn(move || {
            let confirmed = Client::connect().map_or_else(
                |_| Vec::new(),
                |mut client| {
                    display(pending, |request| {
                        client
                            .request(request, Duration::from_secs(5))
                            .map_err(|e| e.to_string())
                    })
                },
            );
            let _ = out.send(TuiEvent::Attention(ResultEvent::Displayed(confirmed)));
        });
    }

    pub(super) fn complete(&mut self, event: ResultEvent, threads: &mut Vec<Thread>) {
        match event {
            ResultEvent::Displayed(records) => {
                self.in_flight = false;
                for (scope, id) in records {
                    if self.scope(&id).as_ref() == Some(&scope) {
                        if self.data.confirmed.insert(id) {
                            self.dirty = true;
                            self.generation += 1;
                        }
                    }
                }
            }
            ResultEvent::Command(result) => {
                let text = match result {
                    Ok((records, text)) => {
                        for record in records {
                            self.data.records.insert(record.id.clone(), record);
                            self.dirty = true;
                            self.generation += 1;
                        }
                        text
                    }
                    Err(error) => format!("Attention: {error}"),
                };
                let idx = find_or_create_thread(threads, FOREGROUND_ID, true, None);
                append(&mut threads[idx], text, None, now_seconds());
            }
        }
    }

    fn scope(&self, id: &str) -> Option<TodoScope> {
        self.data
            .records
            .get(id)
            .map(|r| r.scope.clone())
            .or_else(|| {
                self.data
                    .seen
                    .values()
                    .find(|frame| frame.ids.iter().any(|known| known == id))
                    .map(|frame| frame.scope.clone())
            })
    }

    pub(super) fn command(&self, text: &str) -> Option<Result<Vec<ApiRequest>, String>> {
        let parts: Vec<_> = text.split_whitespace().collect();
        if !matches!(parts.first(), Some(&"attention" | &"ack")) {
            return None;
        }
        Some(self.requests(&parts))
    }

    fn requests(&self, parts: &[&str]) -> Result<Vec<ApiRequest>, String> {
        match parts {
            ["ack", id] => {
                let scope = self.scope(id).ok_or(
                    "unknown attention ID; use /attention [conversation|campaign|work <id>] first",
                )?;
                Ok(vec![ApiRequest::AttentionAcknowledge {
                    scope,
                    id: (*id).into(),
                    phase: AttentionAcknowledgement::Acknowledged,
                }])
            }
            ["attention", rest @ ..] => {
                let scopes = match rest {
                    [] => {
                        // This is the host's global foreground scope, not the current model turn.
                        let mut scopes = vec![TodoScope::Conversation {
                            id: FOREGROUND_ID.into(),
                        }];
                        for frame in self.data.seen.values() {
                            if !scopes.contains(&frame.scope) {
                                scopes.push(frame.scope.clone());
                            }
                        }
                        scopes
                    }
                    ["conversation", id] => vec![TodoScope::Conversation { id: (*id).into() }],
                    ["campaign", id] => vec![TodoScope::Campaign {
                        campaign_id: (*id).into(),
                    }],
                    ["work", id] => vec![TodoScope::Work {
                        work_id: (*id).into(),
                    }],
                    _ => return Err("usage: /attention [conversation|campaign|work <id>]".into()),
                };
                Ok(scopes
                    .into_iter()
                    .map(|scope| ApiRequest::AttentionList {
                        scope,
                        after: None,
                        limit: 100,
                    })
                    .collect())
            }
            _ => Err("usage: /ack <id>".into()),
        }
    }
}

fn display(
    pending: Vec<(TodoScope, String)>,
    mut request: impl FnMut(&ApiRequest) -> Result<ApiResponse, String>,
) -> Vec<(TodoScope, String)> {
    let mut confirmed = Vec::new();
    for (scope, id) in pending {
        let req = ApiRequest::AttentionAcknowledge {
            scope: scope.clone(),
            id: id.clone(),
            phase: AttentionAcknowledgement::Displayed,
        };
        match request(&req) {
            Ok(ApiResponse::AttentionAcknowledged { attention })
                if attention.id == id
                    && attention.scope == scope
                    && attention.displayed_at_ms.is_some() =>
            {
                confirmed.push((scope, id))
            }
            _ => break,
        }
    }
    confirmed
}

pub(super) fn execute(
    requests: Vec<ApiRequest>,
    mut request: impl FnMut(&ApiRequest) -> Result<ApiResponse, String>,
) -> Result<(Vec<Attention>, String), String> {
    let mut records = Vec::new();
    for mut req in requests {
        loop {
            match request(&req)? {
                ApiResponse::AttentionAcknowledged { attention } => {
                    let ApiRequest::AttentionAcknowledge { scope, id, .. } = &req else {
                        return Err("unexpected acknowledgment".into());
                    };
                    if &attention.id != id
                        || &attention.scope != scope
                        || attention.acknowledged_at_ms.is_none()
                    {
                        return Err("mismatched acknowledgment".into());
                    }
                    let text = format!("Acknowledged {}. No work action was taken.", attention.id);
                    return Ok((vec![attention], text));
                }
                ApiResponse::AttentionList { snapshot } => {
                    let ApiRequest::AttentionList { scope, after, .. } = &mut req else {
                        return Err("unexpected list".into());
                    };
                    if snapshot.records.iter().any(|r| &r.scope != scope) {
                        return Err("mismatched attention scope".into());
                    }
                    records.extend(snapshot.records);
                    if let Some(cursor) = snapshot.next_cursor {
                        *after = Some(cursor);
                    } else {
                        break;
                    }
                }
                _ => return Err("unexpected attention response".into()),
            }
        }
    }
    let text = if records.is_empty() {
        "No attention records in the selected scopes.".into()
    } else {
        records.iter().map(|r| format!("{}: {:?} {:?}; accepted={}, delivered={}, displayed={}, acknowledged={}; work={} campaign={}",
            r.id, r.severity, r.category, r.accepted_at_ms,
            r.delivered_at_ms.map_or("unknown".into(), |n| n.to_string()),
            r.displayed_at_ms.map_or("unknown".into(), |n| n.to_string()),
            r.acknowledged_at_ms.map_or("unknown".into(), |n| n.to_string()),
            r.work_id.as_deref().unwrap_or("none"), r.campaign_id.as_deref().unwrap_or("none")))
            .collect::<Vec<_>>().join("\n")
    };
    Ok((records, text))
}

pub(super) fn recover(
    client: &mut Client,
    mut publish: impl FnMut(tachyon_api::types::HistoryEntry) -> Result<(), String>,
) -> Result<(), String> {
    // History has time-window pagination, not an event cursor. Split full windows
    // rather than treating the newest bounded page as complete recovery.
    session_archive::history_pages(
        0,
        now_seconds().saturating_add(1),
        |since_ms, until_ms, limit| match client
            .request(
                &ApiRequest::HistoryQuery {
                    since_ms,
                    until_ms,
                    limit,
                },
                Duration::from_secs(5),
            )
            .map_err(|e| e.to_string())?
        {
            ApiResponse::History { entries } => Ok(entries),
            _ => Err("unexpected history response".into()),
        },
        |entry| {
            if entry.attention.is_some() {
                publish(entry)?;
            }
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let path = PathBuf::from("/tmp/opencode").join(format!(
                "attention-tui-{}-{}-{}",
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

    fn event() -> InteractionEventEnvelope {
        let mut metadata =
            tachyon_api::InteractionMetadata::new("frame:published", "frame", FOREGROUND_ID, 123);
        metadata.attention = Some(AttentionFrameMetadata {
            scope: TodoScope::Campaign {
                campaign_id: "campaign-source-not-foreground".into(),
            },
            ids: vec!["attention-one".into(), "attention-two".into()],
        });
        InteractionEventEnvelope {
            metadata,
            event: InteractionEvent::UserVisibleNotificationPublished {
                text: "Work failed (2 items). Review attention for details.".into(),
            },
        }
    }

    fn record(id: &str, scope: TodoScope) -> Attention {
        use tachyon_api::attention::{AttentionCategory, AttentionSeverity};
        Attention {
            id: id.into(),
            command_id: "not-the-frame".into(),
            cause_id: "cause".into(),
            scope,
            work_id: None,
            campaign_id: None,
            generation: 1,
            instruction_revision: 0,
            category: AttentionCategory::WorkFailed,
            severity: AttentionSeverity::Urgent,
            accepted_at_ms: 1,
            delivered_at_ms: Some(2),
            displayed_at_ms: None,
            acknowledged_at_ms: None,
        }
    }

    fn render(
        threads: &[Thread],
        scroll: &mut TranscriptScroll,
        width: u16,
        height: u16,
    ) -> (TranscriptView, String) {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let mut view = TranscriptView::default();
        terminal
            .draw(|f| {
                draw_conversation(
                    f,
                    f.area(),
                    threads,
                    false,
                    "",
                    scroll,
                    &mut TurnLayoutCache::default(),
                    &mut view,
                    None,
                    None,
                    &mut TurnProjection::default(),
                )
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let text = (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        (view, text)
    }

    #[test]
    fn receipt_is_not_display_and_duplicate_frames_do_not_mutate_model_state() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        threads[0].streaming = true;
        let entry = publication(&event()).unwrap();
        assert!(state.receive(entry.clone(), &mut threads));
        let revision = threads[0].revision;
        assert!(!state.receive(entry, &mut threads));
        assert_eq!(threads[0].revision, revision);
        assert_eq!(threads[0].items.len(), 1);
        assert!(threads[0].items[0].turn.is_none());
        assert!(threads[0].streaming);
        assert!(threads[0].completed_turns.is_empty());
        assert!(threads[0].usage.is_empty());
        assert!(threads[0].metrics.is_empty());
        assert!(state.pending().is_empty());

        let (view, text) = render(&threads, &mut TranscriptScroll::default(), 90, 12);
        assert!(text.contains("Work failed (2 items)"));
        assert!(!state.visible(&threads, &view.attention_hits, true));
        assert!(state.pending().is_empty());
        assert!(state.visible(&threads, &view.attention_hits, false));
        assert_eq!(state.pending().len(), 2);
        assert!(state
            .pending()
            .iter()
            .all(|(scope, _)| scope == &event().metadata.attention.unwrap().scope));
        assert!(!state.visible(&threads, &view.attention_hits, false));
        threads[0].finish_reply("model result".into(), None);
        assert_eq!(
            threads[0].items[0].text,
            "Work failed (2 items). Review attention for details."
        );
    }

    #[test]
    fn notice_preserves_existing_completion_nodes_badges_and_metrics() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        let turn = "conversation:model:1";
        for (kind, text) in [
            (ItemKind::User, "question"),
            (ItemKind::Spawn, "worker fresh: lookup"),
            (ItemKind::SpawnResult, "worker fresh: result"),
        ] {
            threads[0].add_turn(kind, text.into(), Some(turn.into()));
        }
        threads[0].finish_reply("answer".into(), Some(turn.into()));
        threads[0].metrics.insert(
            turn.into(),
            TurnMetrics {
                completed_ms: Some(4500),
                self_usage: Some(TokenTotals {
                    prompt: 1000,
                    completion: 200,
                    total: 1200,
                }),
                ..Default::default()
            },
        );
        let cell = build_turn_cells(&threads[0]).remove(0);
        let badges = turn_cell_badges(&threads[0], &cell);
        assert!(badges.contains("1 complete"));
        assert!(badges.contains("done 4.5s"));
        assert!(badges.contains("total 1.2k"));
        let metrics = serde_json::to_value(&threads[0].metrics).unwrap();
        let completed = threads[0].completed_turns.clone();
        let nodes = threads[0]
            .items
            .iter()
            .map(|i| (i.text.clone(), i.turn.clone()))
            .collect::<Vec<_>>();
        let entry = publication(&event()).unwrap();
        assert!(state.receive(entry.clone(), &mut threads));
        assert!(!state.receive(entry, &mut threads));
        assert_eq!(turn_cell_badges(&threads[0], &cell), badges);
        assert_eq!(serde_json::to_value(&threads[0].metrics).unwrap(), metrics);
        assert_eq!(threads[0].completed_turns, completed);
        assert!(!threads[0].streaming);
        assert_eq!(threads[0].items.len(), nodes.len() + 1);
        for (item, (text, turn)) in threads[0].items.iter().zip(nodes) {
            assert_eq!(item.text, text);
            assert_eq!(item.turn, turn);
        }
        let (_, text) = render(&threads, &mut TranscriptScroll::default(), 110, 25);
        assert!(text.contains("1 complete"), "{text}");
        assert!(text.contains("total 1.2k"), "{text}");
        assert!(text.contains("Work failed (2 items)"), "{text}");
    }

    #[test]
    fn scroll_header_only_and_zero_width_are_not_display() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        threads[0].add_turn(
            ItemKind::User,
            "question".into(),
            Some("conversation:model:1".into()),
        );
        threads[0].finish_reply(
            (0..50).map(|n| format!("answer line {n}\n")).collect(),
            Some("conversation:model:1".into()),
        );
        state.receive(publication(&event()).unwrap(), &mut threads);
        let mut scroll = TranscriptScroll {
            follow: false,
            ..TranscriptScroll::default()
        };
        let (view, text) = render(&threads, &mut scroll, 90, 8);
        assert!(!text.contains("Work failed"));
        assert!(!state.visible(&threads, &view.attention_hits, false));
        for width in 0..=4 {
            // The existing body renderer has four leading spaces. Padding is not display.
            let (view, _) = render(&threads, &mut TranscriptScroll::default(), width, 8);
            assert!(!state.visible(&threads, &view.attention_hits, false));
        }
        scroll.end();
        let (view, text) = render(&threads, &mut scroll, 90, 8);
        assert!(text.contains("Work failed"));
        assert!(state.visible(&threads, &view.attention_hits, false));

        let mut standalone = vec![Thread::new_foreground()];
        state.receive(
            {
                let mut e = publication(&event()).unwrap();
                e.event_id = "another".into();
                e
            },
            &mut standalone,
        );
        let (view, text) = render(
            &standalone,
            &mut TranscriptScroll {
                follow: false,
                ..TranscriptScroll::default()
            },
            90,
            2,
        );
        assert!(!text.contains("Work failed"));
        assert!(view.attention_hits.is_empty());
    }

    #[test]
    fn archive_reopen_and_replay_keep_one_notice_and_retry_only_displayed_ids() {
        let dir = Directory::new();
        let entry = publication(&event()).unwrap();
        {
            let mut visits = session_archive::Visits::open(&dir.0).unwrap();
            let mut state = State::open(&dir.0).unwrap();
            let mut threads = vec![Thread::new_foreground()];
            threads[0].add(ItemKind::User, "uncorrelated user before notice".into());
            state.receive(entry.clone(), &mut threads);
            visits.save(&threads).unwrap();
            state.save().unwrap();
        }
        let mut visits = session_archive::Visits::open(&dir.0).unwrap();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        visits.latest(&mut threads).unwrap();
        assert!(!state.receive(entry.clone(), &mut threads));
        assert_eq!(
            threads[0]
                .items
                .iter()
                .filter(|i| i.attention.is_some())
                .count(),
            1
        );
        threads[0].hide_history = true;
        let (view, _) = render(&threads, &mut TranscriptScroll::default(), 90, 20);
        assert!(!state.visible(&threads, &view.attention_hits, false));
        assert!(state.pending().is_empty());
        threads[0].hide_history = false;
        let (view, text) = render(&threads, &mut TranscriptScroll::default(), 90, 20);
        assert!(text.contains("Work failed"));
        assert!(state.visible(&threads, &view.attention_hits, false));
        assert_eq!(state.pending().len(), 2);
        state.complete(ResultEvent::Displayed(vec![]), &mut threads);
        assert_eq!(state.pending().len(), 2);
        state.complete(ResultEvent::Displayed(state.pending()), &mut threads);
        state.save().unwrap();
        let mut state = State::open(&dir.0).unwrap();
        assert!(state.pending().is_empty());
        assert!(!state.receive(entry, &mut threads));
        assert!(state.visible(&threads, &view.attention_hits, false));
        // The daemon makes repeated display receipts idempotent after reconnect.
        assert_eq!(state.pending().len(), 2);
    }

    #[test]
    fn metadata_absence_keeps_legacy_shape_and_never_claims_display() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut envelope = event();
        envelope.metadata.attention = None;
        assert!(publication(&envelope).is_none());
        assert!(!serde_json::to_string(&envelope)
            .unwrap()
            .contains("\"attention\""));
        let mut thread = Thread::new_foreground();
        apply_interaction_event(&mut thread, envelope);
        let threads = vec![thread];
        let snapshot = serde_json::to_string(&session_snapshot(&threads)).unwrap();
        assert!(!snapshot.contains("\"attention\""));
        let restored = restore_session(serde_json::from_str(&snapshot).unwrap());
        let (view, _) = render(&restored, &mut TranscriptScroll::default(), 90, 20);
        assert!(!state.visible(&restored, &view.attention_hits, false));
        assert!(state.pending().is_empty());
    }

    #[test]
    fn explicit_ack_uses_known_host_scope_and_does_not_imply_display() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        assert!(state.requests(&["ack", "unknown"]).is_err());
        state.receive(publication(&event()).unwrap(), &mut threads);
        let scope = event().metadata.attention.unwrap().scope;
        let requests = state.requests(&["ack", "attention-two"]).unwrap();
        assert!(
            matches!(&requests[0], ApiRequest::AttentionAcknowledge { scope: s, id, phase: AttentionAcknowledgement::Acknowledged } if s == &scope && id == "attention-two")
        );
        let mut record = record("attention-two", scope.clone());
        record.acknowledged_at_ms = Some(3);
        let result = execute(requests, |_| {
            Ok(ApiResponse::AttentionAcknowledged {
                attention: record.clone(),
            })
        })
        .unwrap();
        assert!(result.1.contains("No work action"));
        state.complete(ResultEvent::Command(Ok(result)), &mut threads);
        assert!(state.pending().is_empty());
        assert!(state.data.visible.is_empty());
        assert!(state
            .requests(&["attention", "campaign", "explicit-scope"])
            .is_ok());
        assert!(state.requests(&["attention", "guess"]).is_err());
    }

    #[test]
    fn display_requests_are_scoped_bounded_and_retry_after_partial_failure() {
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let mut threads = vec![Thread::new_foreground()];
        for n in 0..40 {
            let mut entry = publication(&event()).unwrap();
            entry.event_id = format!("frame-{n}");
            entry.attention.as_mut().unwrap().ids = vec![format!("attention-{n:02}")];
            state.receive(entry, &mut threads);
        }
        let (view, _) = render(&threads, &mut TranscriptScroll::default(), 90, 250);
        state.visible(&threads, &view.attention_hits, false);
        assert_eq!(state.pending().len(), 32);
        let mut calls = 0;
        let confirmed = display(state.pending(), |request| {
            let ApiRequest::AttentionAcknowledge {
                scope,
                id,
                phase: AttentionAcknowledgement::Displayed,
            } = request
            else {
                panic!("not a display request")
            };
            assert_eq!(scope, &event().metadata.attention.unwrap().scope);
            calls += 1;
            if calls == 3 {
                return Err("offline".into());
            }
            let mut attention = record(id, scope.clone());
            attention.displayed_at_ms = Some(5);
            assert!(attention.acknowledged_at_ms.is_none());
            Ok(ApiResponse::AttentionAcknowledged { attention })
        });
        assert_eq!(confirmed.len(), 2);
        state.complete(ResultEvent::Displayed(confirmed), &mut threads);
        assert_eq!(state.pending()[0].1, "attention-02");
        for _ in 0..2 {
            let confirmed = display(state.pending(), |request| {
                let ApiRequest::AttentionAcknowledge {
                    scope,
                    id,
                    phase: AttentionAcknowledgement::Displayed,
                } = request
                else {
                    panic!()
                };
                let mut attention = record(id, scope.clone());
                attention.displayed_at_ms = Some(6);
                Ok(ApiResponse::AttentionAcknowledged { attention })
            });
            state.complete(ResultEvent::Displayed(confirmed), &mut threads);
        }
        assert!(state.pending().is_empty());
    }

    #[test]
    fn list_paginates_typed_records_without_displaying_them_or_accepting_stale_partial_results() {
        use tachyon_api::attention::AttentionSnapshot;
        use tachyon_api::operational_events::OperationalWatermark;
        let dir = Directory::new();
        let mut state = State::open(&dir.0).unwrap();
        let scope = TodoScope::Work {
            work_id: "explicit-work".into(),
        };
        let mut calls = 0;
        let requests = || {
            vec![ApiRequest::AttentionList {
                scope: scope.clone(),
                after: None,
                limit: 100,
            }]
        };
        let result = execute(requests(), |request| {
            let ApiRequest::AttentionList {
                scope: selected,
                after,
                limit: 100,
            } = request
            else {
                panic!()
            };
            assert_eq!(selected, &scope);
            assert_eq!(
                after.as_deref(),
                if calls == 0 {
                    None
                } else {
                    Some("opaque-cursor")
                }
            );
            calls += 1;
            Ok(ApiResponse::AttentionList {
                snapshot: AttentionSnapshot {
                    records: vec![record(&format!("record-{calls}"), scope.clone())],
                    next_cursor: (calls == 1).then(|| "opaque-cursor".into()),
                    watermark: OperationalWatermark {
                        instance_id: "test".into(),
                        sequence: 1,
                    },
                },
            })
        })
        .unwrap();
        assert_eq!(calls, 2);
        assert_eq!(result.0.len(), 2);
        assert!(result.1.contains("displayed=unknown"));
        let mut threads = vec![Thread::new_foreground()];
        state.complete(ResultEvent::Command(Ok(result)), &mut threads);
        assert_eq!(state.scope("record-2"), Some(scope.clone()));
        let (view, _) = render(&threads, &mut TranscriptScroll::default(), 90, 20);
        assert!(!state.visible(&threads, &view.attention_hits, false));
        assert!(state.pending().is_empty());
        let mut first = true;
        assert!(execute(requests(), |_| {
            if !first {
                return Err("stale snapshot cursor".into());
            }
            first = false;
            Ok(ApiResponse::AttentionList {
                snapshot: AttentionSnapshot {
                    records: vec![record("partial", scope.clone())],
                    next_cursor: Some("stale".into()),
                    watermark: OperationalWatermark {
                        instance_id: "test".into(),
                        sequence: 1,
                    },
                },
            })
        })
        .is_err());
        assert!(state.scope("partial").is_none());
    }
}
