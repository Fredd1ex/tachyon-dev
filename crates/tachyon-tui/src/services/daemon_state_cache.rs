//! Disposable read models. Daemon records, revisions and cursors remain authoritative.
use std::io::{self, BufRead, BufReader, Read};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::{mpsc::Sender, Arc, Condvar, Mutex};
use std::time::Duration;

use tachyon_api::monitor::{MonitorQuery, MonitorScope, MonitorSnapshot, Observed};
use tachyon_api::operational_events::{OperationalBatch, OperationalChange, OperationalWatermark};
use tachyon_api::todo::{Todo, TodoCursor, TodoResponse, TodoScope};
use tachyon_api::transport::write_request;
use tachyon_api::types::{ApiRequest, ApiResponse};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Query {
    Todos {
        scope: TodoScope,
        cursor: Option<TodoCursor>,
        // Exact UI turn binding participates in cancellation, even for the same scope.
        turn: Option<String>,
    },
    Resources {
        after: Option<String>,
    },
}

#[derive(Default)]
pub(crate) struct CurrentConversation(Option<(u64, String)>);

impl CurrentConversation {
    pub fn observe(&mut self, agent: &str, metadata: &tachyon_api::InteractionMetadata) -> bool {
        if agent == tachyon_api::FOREGROUND_ID
            && !metadata.conversation_id.is_empty()
            && self
                .0
                .as_ref()
                .is_none_or(|(at, _)| metadata.occurred_at_ms >= *at)
        {
            let changed = self
                .0
                .as_ref()
                .is_none_or(|(_, id)| id != &metadata.conversation_id);
            self.0 = Some((metadata.occurred_at_ms, metadata.conversation_id.clone()));
            return changed;
        }
        false
    }

    pub fn scope(&self) -> Option<TodoScope> {
        self.0
            .as_ref()
            .map(|(_, id)| TodoScope::Conversation { id: id.clone() })
    }
}

#[derive(Clone, Default)]
pub(crate) struct View {
    pub query: Option<Query>,
    pub rows: String,
    pub stale: bool,
    pub next_todo: Option<TodoCursor>,
    pub next_resource: Option<String>,
    pub checklist: String,
    pub todo_revision: Option<u64>,
}

impl View {
    pub fn global_checklist(&self) -> String {
        if !matches!(
            &self.query,
            Some(Query::Todos {
                scope: TodoScope::Conversation { .. },
                ..
            })
        ) {
            return String::new();
        }
        if self.stale {
            format!("Global checklist unknown / stale\n{}", self.checklist)
        } else if self.todo_revision.is_none() {
            "Global conversation checklist unknown (loading)".into()
        } else {
            self.checklist.clone()
        }
    }
}

#[derive(Default)]
struct Todos {
    records: Vec<Todo>,
    revision: u64,
    watermark: Option<OperationalWatermark>,
    next: Option<TodoCursor>,
    continued: bool,
}

impl Todos {
    fn snapshot(&mut self, response: TodoResponse, scope: &TodoScope) -> io::Result<()> {
        let TodoResponse::List {
            todos,
            scope_revision,
            watermark,
            next_cursor,
        } = response
        else {
            return Err(io::Error::other("expected todo snapshot"));
        };
        if todos.len() > 100
            || self.watermark.as_ref().is_some_and(|old| {
                old.instance_id == watermark.instance_id
                    && (watermark.sequence < old.sequence || scope_revision < self.revision)
            })
            || todos
                .iter()
                .any(|t| &t.scope != scope || t.schema_version != 1 || t.revision == 0)
            || todos
                .iter()
                .map(|t| &t.id)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != todos.len()
            || todos
                .windows(2)
                .any(|pair| (pair[0].order_key, &pair[0].id) >= (pair[1].order_key, &pair[1].id))
        {
            return Err(io::Error::other("invalid todo snapshot"));
        }
        self.records = todos;
        self.revision = scope_revision;
        self.watermark = Some(watermark);
        self.next = next_cursor;
        Ok(())
    }

    // Validate the entire batch before committing it, including records outside this page.
    fn batch(&mut self, batch: OperationalBatch, scope: &TodoScope) -> io::Result<bool> {
        let old = self
            .watermark
            .as_ref()
            .ok_or_else(|| io::Error::other("no snapshot"))?;
        if batch.watermark.instance_id != old.instance_id || batch.watermark.sequence < old.sequence
        {
            return Err(io::Error::other("feed epoch/cursor gap"));
        }
        let mut records = self.records.clone();
        let mut revision = self.revision;
        let mut sequence = old.sequence;
        for event in batch.events {
            if event.schema_version != 1
                || event.scope != *scope
                || event.watermark.instance_id != old.instance_id
                || event.watermark.sequence > batch.watermark.sequence
            {
                return Err(io::Error::other("invalid feed event"));
            }
            let (todo, added) = match event.change {
                OperationalChange::AttentionChanged { .. } => {
                    sequence = sequence.max(event.watermark.sequence);
                    continue;
                }
                OperationalChange::TodoAdded { todo } => (todo, true),
                OperationalChange::TodoUpdated { todo } => (todo, false),
            };
            if event.watermark.sequence <= sequence
                && event.scope_revision == revision
                && records.iter().any(|record| record == &todo)
            {
                continue;
            }
            if event.watermark.sequence <= sequence
                || Some(event.scope_revision) != revision.checked_add(1)
                || todo.scope != *scope
                || todo.schema_version != 1
            {
                return Err(io::Error::other("todo revision gap"));
            }
            if let Some(record) = records.iter_mut().find(|t| t.id == todo.id) {
                if Some(todo.revision) != record.revision.checked_add(1)
                    || todo.order_key != record.order_key
                {
                    return Err(io::Error::other("record revision gap"));
                }
                *record = todo;
            } else if added
                && todo.revision == 1
                && self.next.is_none()
                && records.len() < 100
                && records
                    .last()
                    .is_none_or(|last| todo.order_key > last.order_key)
            {
                records.push(todo);
            } else {
                // Page membership/cursors are snapshot-bound. Refresh rather than invent one.
                return Err(io::Error::other("page changed"));
            }
            revision = event.scope_revision;
            sequence = event.watermark.sequence;
        }
        let changed = revision != self.revision;
        if changed && (self.continued || self.next.is_some()) {
            return Err(io::Error::other("page cursor stale"));
        }
        self.records = records;
        self.revision = revision;
        self.watermark = Some(batch.watermark);
        Ok(changed)
    }

    fn view(&self, scope: &TodoScope) -> View {
        let mut rows = format!("{scope:?} | revision {}\n", self.revision);
        if self.records.is_empty() {
            rows.push_str("No todos in this scope.\n");
        }
        for todo in &self.records {
            rows.push_str(&format!(
                "{}  {:?}  r{}  {}\n",
                todo.id,
                todo.status,
                todo.revision,
                clean(&todo.title)
            ));
        }
        View {
            rows,
            next_todo: self.next.clone(),
            checklist: self.checklist(scope),
            todo_revision: Some(self.revision),
            ..View::default()
        }
    }

    fn checklist(&self, scope: &TodoScope) -> String {
        use tachyon_api::todo::TodoStatus;
        if self.records.is_empty() {
            return String::new();
        }
        let label = match scope {
            TodoScope::Conversation { id } => format!(
                "Global conversation checklist ({}) - not turn-linked",
                clean(id)
            ),
            TodoScope::Work { work_id } => format!("Work checklist ({})", clean(work_id)),
            TodoScope::Campaign { campaign_id } => {
                format!("Campaign checklist ({})", clean(campaign_id))
            }
        };
        let mut rows = vec![label];
        for todo in self.records.iter().take(3) {
            let status = match todo.status {
                TodoStatus::Pending => "[ ] pending",
                TodoStatus::InProgress => "[>] active",
                TodoStatus::Blocked => "[!] blocked",
                TodoStatus::Completed => "[x] complete",
                TodoStatus::Cancelled => "[-] cancelled",
            };
            rows.push(format!("{status}  {}", clean(&todo.title)));
        }
        let extra = self.records.len().saturating_sub(3);
        if extra > 0 || self.next.is_some() {
            rows.push(format!(
                "+{extra}{} more (TODO for pages)",
                if self.next.is_some() { " or more" } else { "" }
            ));
        }
        rows.join("\n")
    }
}

fn clean(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

// Bound the frame before deserialization, not just the number of decoded rows.
fn read_response(reader: &mut impl BufRead) -> io::Result<ApiResponse> {
    const MAX_FRAME: u64 = 16 * 1024 * 1024;
    let mut frame = Vec::new();
    reader.take(MAX_FRAME + 1).read_until(b'\n', &mut frame)?;
    if frame.len() as u64 > MAX_FRAME || frame.last() != Some(&b'\n') {
        return Err(io::Error::other("invalid or oversized operational frame"));
    }
    serde_json::from_slice(&frame).map_err(io::Error::other)
}

fn observed(value: &Observed<tachyon_api::monitor::Decimal>) -> String {
    match value {
        Observed::Unknown => "unknown".into(),
        Observed::Known(n) => n.0.to_string(),
    }
}

fn resource_view(snapshot: &MonitorSnapshot, previous: &View) -> View {
    let Some(p) = &snapshot.payload else {
        return View {
            stale: true,
            ..previous.clone()
        };
    };
    let mut rows = format!(
        "Host | sample {} | {}:{}\nCampaign-ledger coverage (not all chat usage)\n",
        p.durable.sampled_at_ms, snapshot.version.epoch, snapshot.version.sequence
    );
    match &p.durable.inference {
        Observed::Unknown => rows.push_str("Inference: unknown\n"),
        Observed::Known(i) => rows.push_str(&format!("Tokens final {} / provisional {} / reserved {}\nCost micro-USD final {} / provisional {} / reserved {}\nUnknown reports {}\n", i.final_usage.tokens.0, i.provisional_usage.tokens.0, i.unresolved_reserved.tokens.0, i.final_usage.cost_micro_usd.0, i.provisional_usage.cost_micro_usd.0, i.unresolved_reserved.cost_micro_usd.0, i.unknown_reports.0)),
    }
    match &p.durable.funding {
        Observed::Unknown => rows.push_str("Funding: unknown\n"),
        Observed::Known(f) => {
            for (name, pool) in [("Work", &f.work), ("Verification", &f.verification)] {
                rows.push_str(&format!("{name} budget tokens authorized {} committed {} available {}\n{name} budget micro-USD authorized {} committed {} available {}\n", pool.authorized.tokens.0, pool.committed.tokens.0, pool.available.tokens.0, pool.authorized.cost_micro_usd.0, pool.committed.cost_micro_usd.0, pool.available.cost_micro_usd.0));
            }
            rows.push_str(&format!(
                "Debt tokens {} micro-USD {} paused ledgers {}\n",
                f.debt.tokens.0,
                f.debt.cost_micro_usd.0,
                observed(&f.paused_ledgers)
            ));
        }
    }
    rows.push_str(&format!(
        "Native charged wall ms (not utilization): CPU {} GPU {}\n",
        p.durable.native_jobs.cpu_charged_wall_ms.0, p.durable.native_jobs.gpu_charged_wall_ms.0
    ));
    match &p.durable.retained_storage {
        Observed::Unknown => rows.push_str("Retained storage: unknown\n"),
        Observed::Known(s) => rows.push_str(&format!(
            "Logical retained bytes (not disk): ready {} reserved {} limit {}\n",
            s.ready_bytes.0,
            s.reserved_bytes.0,
            observed(&s.limit_bytes)
        )),
    }
    for c in p.capacities.iter().take(6) {
        rows.push_str(&format!(
            "{:?}: held {} / limit {} queued {} unresolved {}\n",
            c.resource,
            c.held.0,
            c.limit.0,
            c.queued.0,
            observed(&c.unresolved)
        ));
    }
    rows.push_str(&format!(
        "Registered {} (sample {})\n",
        p.registered.total.0, p.registered.sampled_at_ms
    ));
    for e in p.registered.entries.iter().take(100) {
        rows.push_str(&format!(
            "{} {:?} {} pid {}\n",
            clean(&e.id),
            e.role,
            clean(&e.state),
            e.pid.map_or_else(|| "unknown".into(), |p| p.to_string())
        ));
    }
    View {
        rows,
        stale: snapshot.stale.is_some(),
        next_resource: p.registered.next_after.clone(),
        ..View::default()
    }
}

#[derive(Default)]
struct State {
    generation: u64,
    query: Option<Query>,
    socket: Option<UnixStream>,
    latest: Option<(u64, View)>,
    notified: bool,
    stopped: bool,
}

/// One lazy worker and one replaceable result, never a queue of model-token updates.
pub(crate) struct Worker {
    state: Arc<(Mutex<State>, Condvar)>,
    started: bool,
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            state: Arc::new((Mutex::new(State::default()), Condvar::new())),
            started: false,
        }
    }
}

impl Worker {
    pub fn select(&mut self, query: Option<Query>, out: &Sender<super::TuiEvent>) {
        let (lock, wake) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.query == query {
            return;
        }
        state.generation += 1;
        state.query = query;
        state.latest = None;
        if let Some(socket) = state.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        wake.notify_one();
        if !self.started && state.query.is_some() {
            self.started = true;
            let shared = self.state.clone();
            let out = out.clone();
            std::thread::spawn(move || {
                run(shared, out, || {
                    UnixStream::connect(tachyon_util::daemon::socket_path())
                })
            });
        }
    }

    pub fn take(&self) -> Option<View> {
        let mut state = self.state.0.lock().unwrap();
        state.notified = false;
        let (generation, view) = state.latest.take()?;
        if generation != state.generation {
            return None;
        }
        // Adopt recovery's effective cursor atomically with delivering it to the UI.
        state.query = view.query.clone();
        Some(view)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let mut state = self.state.0.lock().unwrap();
        state.stopped = true;
        if let Some(socket) = state.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        self.state.1.notify_one();
    }
}

fn run(
    shared: Arc<(Mutex<State>, Condvar)>,
    out: Sender<super::TuiEvent>,
    connect: impl Fn() -> io::Result<UnixStream>,
) {
    let (lock, wake) = &*shared;
    loop {
        let mut state = lock.lock().unwrap();
        while state.query.is_none() && !state.stopped {
            state = wake.wait(state).unwrap();
        }
        if state.stopped {
            return;
        }
        let generation = state.generation;
        let mut query = state.query.clone().unwrap();
        drop(state);
        let mut view = View {
            rows: "Waiting for daemon data (unknown).".into(),
            ..View::default()
        };
        let mut todos = Todos::default();
        loop {
            let publish = |view: &View| {
                let mut state = lock.lock().unwrap();
                if state.stopped || state.generation != generation {
                    return false;
                }
                let mut view = view.clone();
                view.query = Some(query.clone());
                state.latest = Some((generation, view));
                if !state.notified {
                    state.notified = true;
                    let _ = out.send(super::TuiEvent::Operational);
                }
                true
            };
            let active = || {
                let state = lock.lock().unwrap();
                !state.stopped && state.generation == generation
            };
            let result = (|| -> io::Result<()> {
                if !active() {
                    return Ok(());
                }
                let mut socket = connect()?;
                socket.set_write_timeout(Some(Duration::from_secs(2)))?;
                socket.set_read_timeout(Some(Duration::from_secs(5)))?;
                {
                    let mut state = lock.lock().unwrap();
                    if state.stopped || state.generation != generation {
                        return Ok(());
                    }
                    state.socket = Some(socket.try_clone()?);
                }
                let mut reader = BufReader::new(socket.try_clone()?);
                match &query {
                    Query::Todos { scope, cursor, .. } => {
                        write_request(
                            &mut socket,
                            &ApiRequest::TodoSnapshot {
                                scope: scope.clone(),
                                limit: Some(100),
                                cursor: cursor.clone(),
                            },
                        )?;
                        let ApiResponse::Todo { response } = read_response(&mut reader)? else {
                            return Err(io::Error::other("snapshot unavailable"));
                        };
                        if !active() {
                            return Ok(());
                        }
                        todos.continued = cursor.is_some();
                        todos.snapshot(response, scope)?;
                        view = todos.view(scope);
                        if !publish(&view) {
                            return Ok(());
                        }
                        write_request(
                            &mut socket,
                            &ApiRequest::OperationalSubscribe {
                                scope: scope.clone(),
                                after: todos.watermark.clone().unwrap(),
                            },
                        )?;
                        // Idle push subscriptions must not resnapshot on a timer.
                        socket.set_read_timeout(None)?;
                        loop {
                            let ApiResponse::OperationalBatch { batch } =
                                read_response(&mut reader)?
                            else {
                                return Err(io::Error::other("feed unavailable"));
                            };
                            if !active() {
                                return Ok(());
                            }
                            if todos.batch(batch, scope)? {
                                view = todos.view(scope);
                                if !publish(&view) {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Query::Resources { after } => {
                        let query = MonitorQuery {
                            scope: MonitorScope::Host,
                            after: after.clone(),
                            limit: 100,
                        };
                        // Subscribe without a version returns the current latest snapshot, then updates.
                        write_request(
                            &mut socket,
                            &ApiRequest::MonitorSubscribe {
                                query: query.clone(),
                                after: None,
                            },
                        )?;
                        let mut version = None;
                        loop {
                            let ApiResponse::Monitor { snapshot } = read_response(&mut reader)?
                            else {
                                return Err(io::Error::other("monitor unavailable"));
                            };
                            if !active() {
                                return Ok(());
                            }
                            if snapshot.query != query {
                                return Err(io::Error::other("monitor query mismatch"));
                            }
                            socket.set_read_timeout(None)?;
                            if version.as_ref().is_some_and(
                                |v: &tachyon_api::monitor::MonitorVersion| {
                                    v.epoch == snapshot.version.epoch
                                        && v.sequence >= snapshot.version.sequence
                                },
                            ) {
                                continue;
                            }
                            version = Some(snapshot.version.clone());
                            view = resource_view(&snapshot, &view);
                            if !publish(&view) {
                                return Ok(());
                            }
                        }
                    }
                }
            })();
            if result.is_err() {
                view.stale = true;
                view.next_todo = None;
                view.next_resource = None;
                if !publish(&view) {
                    break;
                }
                // Cursors are revision-bound; recovery always starts at the first page.
                if let Query::Todos { cursor, .. } = &mut query {
                    *cursor = None;
                }
            }
            let mut state = lock.lock().unwrap();
            state.socket = None;
            if state.stopped {
                return;
            }
            if state.generation != generation {
                break;
            }
            state = wake.wait_timeout(state, Duration::from_secs(1)).unwrap().0;
            if state.stopped {
                return;
            }
            if state.generation != generation {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::monitor::*;
    use tachyon_api::operational_events::OperationalEvent;
    use tachyon_api::todo::{TodoActor, TodoStatus};
    use tachyon_api::transport::{read_request, write_response};

    fn scope() -> TodoScope {
        TodoScope::Conversation {
            id: "actual-random-session".into(),
        }
    }
    fn watermark(sequence: u64) -> OperationalWatermark {
        OperationalWatermark {
            instance_id: "database".into(),
            sequence,
        }
    }
    fn todo(id: u64) -> Todo {
        Todo {
            schema_version: 1,
            id: id.to_string(),
            scope: scope(),
            title: format!("Task {id}"),
            description: String::new(),
            status: TodoStatus::Pending,
            order_key: id,
            revision: 1,
            created_ms: 1,
            updated_ms: 1,
            created_by: TodoActor {
                source: "operator".into(),
                actor: "uid:1".into(),
            },
            updated_by: TodoActor {
                source: "operator".into(),
                actor: "uid:1".into(),
            },
        }
    }
    fn snapshot(records: Vec<Todo>, revision: u64) -> TodoResponse {
        TodoResponse::List {
            todos: records,
            scope_revision: revision,
            watermark: watermark(revision),
            next_cursor: None,
        }
    }
    fn batch(record: Todo, revision: u64) -> OperationalBatch {
        OperationalBatch {
            events: vec![OperationalEvent {
                schema_version: 1,
                watermark: watermark(revision),
                scope: scope(),
                scope_revision: revision,
                occurred_at_ms: revision,
                change: if record.revision == 1 {
                    OperationalChange::TodoAdded { todo: record }
                } else {
                    OperationalChange::TodoUpdated { todo: record }
                },
            }],
            watermark: watermark(revision),
        }
    }
    fn monitor(payload: Option<MonitorPayload>) -> MonitorSnapshot {
        MonitorSnapshot {
            query: MonitorQuery {
                scope: MonitorScope::Host,
                after: None,
                limit: 100,
            },
            version: MonitorVersion {
                epoch: "daemon".into(),
                sequence: 1,
            },
            payload,
            stale: None,
        }
    }

    #[test]
    fn checklist_is_host_status_titles_bounded_and_empty_is_absent() {
        let mut cache = Todos::default();
        cache.snapshot(snapshot(vec![], 0), &scope()).unwrap();
        assert!(cache.view(&scope()).checklist.is_empty());
        let statuses = [
            TodoStatus::Pending,
            TodoStatus::InProgress,
            TodoStatus::Blocked,
            TodoStatus::Completed,
            TodoStatus::Cancelled,
        ];
        for status in statuses {
            let mut record = todo(1);
            record.title = "Real title\nno injected row".into();
            record.status = status;
            cache.snapshot(snapshot(vec![record], 1), &scope()).unwrap();
            let text = cache.view(&scope()).checklist;
            assert_eq!(text.lines().count(), 2);
            assert!(text.contains("Real title no injected row"));
            assert!(!text.contains('%'));
            assert!(text.contains("not turn-linked"));
        }
        cache
            .snapshot(snapshot((1..=8).map(todo).collect(), 8), &scope())
            .unwrap();
        let text = cache.view(&scope()).checklist;
        assert_eq!(text.lines().count(), 5);
        assert!(text.contains("Task 3"));
        assert!(!text.contains("Task 4"));
        assert!(text.contains("+5 more"));
    }

    #[test]
    fn todo_error_is_unknown_not_a_successful_empty_snapshot() {
        let (client, mut server) = UnixStream::pair().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let worker = Worker::default();
        worker.state.0.lock().unwrap().query = Some(Query::Todos {
            scope: scope(),
            cursor: None,
            turn: Some("selected".into()),
        });
        let shared = worker.state.clone();
        let (out, receiver) = std::sync::mpsc::channel();
        let connection = Mutex::new(Some(client));
        let join = std::thread::spawn(move || {
            run(shared, out, || {
                connection
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| io::Error::other("offline"))
            })
        });
        let mut reader = BufReader::new(server.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::TodoSnapshot { .. }
        ));
        write_response(
            &mut server,
            &ApiResponse::TodoError {
                error: tachyon_api::todo::TodoError::AuthorityDenied,
            },
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let view = worker.take().unwrap();
        assert!(view.stale);
        assert_eq!(view.todo_revision, None);
        assert!(!view.rows.contains("No todos"));
        drop(worker);
        join.join().unwrap();
    }

    #[test]
    fn stale_snapshot_cannot_regress_revision_or_watermark() {
        let mut cache = Todos::default();
        cache
            .snapshot(snapshot(vec![todo(1)], 4), &scope())
            .unwrap();
        let before = cache.view(&scope()).rows;
        assert!(cache.snapshot(snapshot(vec![], 3), &scope()).is_err());
        let mut stale = snapshot(vec![], 5);
        if let TodoResponse::List { watermark, .. } = &mut stale {
            watermark.sequence = 2;
        }
        assert!(cache.snapshot(stale, &scope()).is_err());
        assert_eq!(cache.view(&scope()).rows, before);
    }

    #[test]
    fn same_scope_different_cell_rejects_pending_generation() {
        let mut worker = Worker::default();
        worker.started = true; // exercise selection without contacting a daemon
        let (out, _) = std::sync::mpsc::channel();
        let query = |turn: &str| {
            Some(Query::Todos {
                scope: scope(),
                cursor: None,
                turn: Some(turn.into()),
            })
        };
        worker.select(query("old"), &out);
        let old = worker.state.0.lock().unwrap().generation;
        worker.select(query("new"), &out);
        worker.state.0.lock().unwrap().latest = Some((
            old,
            View {
                query: query("old"),
                checklist: "Old checklist".into(),
                ..Default::default()
            },
        ));
        assert!(worker.take().is_none());
        let generation = worker.state.0.lock().unwrap().generation;
        worker.select(query("new"), &out);
        assert_eq!(worker.state.0.lock().unwrap().generation, generation);
    }

    #[test]
    fn snapshot_race_replay_and_exact_duplicates_keep_daemon_revisions() {
        let mut cache = Todos::default();
        cache
            .snapshot(snapshot(vec![todo(1)], 1), &scope())
            .unwrap();
        assert!(!cache.batch(batch(todo(1), 1), &scope()).unwrap());
        let mut update = todo(1);
        update.revision = 2;
        update.title = "Changed after snapshot".into();
        let mut replay = batch(update.clone(), 2);
        replay.events.push(replay.events[0].clone());
        assert!(cache.batch(replay.clone(), &scope()).unwrap());
        assert!(!cache.batch(replay, &scope()).unwrap());
        assert_eq!(cache.records, vec![update]);
        assert_eq!(cache.revision, 2);
        assert_eq!(cache.watermark, Some(watermark(2)));
    }

    #[test]
    fn negative_future_out_of_order_and_epoch_gaps_never_clobber() {
        let mut cache = Todos::default();
        cache
            .snapshot(snapshot(vec![todo(1)], 1), &scope())
            .unwrap();
        let before = cache.view(&scope()).rows;
        let mut invalid = vec![batch(todo(2), 3), batch(todo(2), 0)];
        let mut wrong_record = todo(1);
        wrong_record.revision = 9;
        invalid.push(batch(wrong_record, 2));
        let mut wrong_epoch = batch(todo(2), 2);
        wrong_epoch.watermark.instance_id = "new database".into();
        invalid.push(wrong_epoch);
        let mut reversed = batch(todo(2), 2);
        reversed.events.push(batch(todo(3), 1).events.remove(0));
        invalid.push(reversed);
        for value in invalid {
            assert!(cache.batch(value, &scope()).is_err());
            assert_eq!(cache.view(&scope()).rows, before);
            assert_eq!(cache.watermark, Some(watermark(1)));
        }
    }

    #[test]
    fn bounded_page_never_accumulates_more_than_one_hundred_records() {
        let mut cache = Todos::default();
        cache
            .snapshot(snapshot((1..=100).map(todo).collect(), 100), &scope())
            .unwrap();
        assert!(cache.batch(batch(todo(101), 101), &scope()).is_err());
        assert_eq!(cache.records.len(), 100);
        assert!(cache
            .snapshot(snapshot((1..=101).map(todo).collect(), 101), &scope())
            .is_err());
        assert_eq!(cache.records.len(), 100);
        cache
            .snapshot(snapshot(vec![todo(101)], 101), &scope())
            .unwrap();
        assert_eq!(cache.records.len(), 1);
    }

    #[test]
    fn page_cursor_is_verbatim_and_mutation_requests_resnapshot_without_clobbering() {
        let cursor = TodoCursor {
            version: 1,
            instance_id: "database".into(),
            scope: scope(),
            filter: Default::default(),
            scope_revision: 1,
            after_order_key: 1,
            after_id: "1".into(),
        };
        let mut page = snapshot(vec![todo(1)], 1);
        if let TodoResponse::List { next_cursor, .. } = &mut page {
            *next_cursor = Some(cursor.clone());
        }
        let mut cache = Todos::default();
        cache.snapshot(page, &scope()).unwrap();
        assert_eq!(cache.view(&scope()).next_todo, Some(cursor.clone()));
        let mut update = todo(1);
        update.revision = 2;
        assert!(cache.batch(batch(update, 2), &scope()).is_err());
        assert_eq!(cache.records, vec![todo(1)]);
        assert_eq!(cache.revision, 1);
        assert_eq!(cache.view(&scope()).next_todo, Some(cursor));
    }

    #[test]
    fn final_continuation_page_invalidates_on_any_mutation() {
        for change in [
            batch(todo(3), 3),
            {
                let mut updated = todo(2);
                updated.revision = 2;
                batch(updated, 3)
            },
            {
                let mut hidden = todo(1);
                hidden.revision = 2;
                batch(hidden, 3)
            },
        ] {
            let mut cache = Todos {
                continued: true,
                ..Default::default()
            };
            cache
                .snapshot(snapshot(vec![todo(2)], 2), &scope())
                .unwrap();
            assert!(cache.batch(change, &scope()).is_err());
            assert_eq!(cache.records, vec![todo(2)]);
            assert_eq!(cache.revision, 2);
        }
    }

    #[test]
    fn consuming_recovery_adopts_first_page_before_selecting_the_same_next_cursor() {
        let mut worker = Worker::default();
        worker.started = true;
        let next = Query::Todos {
            turn: None,
            scope: scope(),
            cursor: Some(TodoCursor {
                version: 1,
                instance_id: "database".into(),
                scope: scope(),
                filter: Default::default(),
                scope_revision: 1,
                after_order_key: 1,
                after_id: "1".into(),
            }),
        };
        let first = Query::Todos {
            turn: None,
            scope: scope(),
            cursor: None,
        };
        {
            let mut state = worker.state.0.lock().unwrap();
            state.query = Some(next.clone());
            state.latest = Some((
                0,
                View {
                    query: Some(first.clone()),
                    ..Default::default()
                },
            ));
        }
        let (out, _) = std::sync::mpsc::channel();
        // The event loop selects before it drains notifications. Recovery must survive it.
        worker.select(Some(next.clone()), &out);
        assert_eq!(worker.take().unwrap().query, Some(first.clone()));
        worker.select(Some(first), &out);
        assert_eq!(worker.state.0.lock().unwrap().generation, 0);
        worker.select(Some(next), &out);
        assert_eq!(worker.state.0.lock().unwrap().generation, 1);
    }

    #[test]
    fn operational_frame_is_bounded_before_json_decoding() {
        let mut input = BufReader::new(io::repeat(b' '));
        assert!(read_response(&mut input)
            .unwrap_err()
            .to_string()
            .contains("oversized"));
        assert!(read_response(&mut &b"{}"[..]).is_err());
    }

    #[test]
    fn switching_query_cancels_idle_monitor_and_never_delivers_old_rows() {
        let (client, mut server) = UnixStream::pair().unwrap();
        let (new_client, mut new_server) = UnixStream::pair().unwrap();
        for server in [&server, &new_server] {
            server
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
        }
        let mut worker = Worker::default();
        worker.started = true;
        worker.state.0.lock().unwrap().query = Some(Query::Resources { after: None });
        let shared = worker.state.clone();
        let (out, receiver) = std::sync::mpsc::channel();
        let sender = out.clone();
        let connections = Mutex::new(std::collections::VecDeque::from([client, new_client]));
        let (done, ended) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            run(shared, sender, || {
                connections
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or_else(|| io::Error::other("no connection"))
            });
            done.send(()).unwrap();
        });
        let mut reader = BufReader::new(server.try_clone().unwrap());
        read_request(&mut reader).unwrap();
        write_response(
            &mut server,
            &ApiResponse::Monitor {
                snapshot: monitor(Some(MonitorPayload {
                    durable: Durable::default(),
                    capacities: vec![],
                    registered: Registered::default(),
                })),
            },
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        // Leave the old result pending, then change both view kind and scope.
        worker.select(
            Some(Query::Todos {
                turn: None,
                scope: scope(),
                cursor: None,
            }),
            &out,
        );
        assert!(worker.take().is_none());
        let mut reader = BufReader::new(new_server.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::TodoSnapshot { .. }
        ));
        write_response(
            &mut new_server,
            &ApiResponse::Todo {
                response: snapshot(vec![todo(1)], 1),
            },
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let rows = worker.take().unwrap().rows;
        assert!(rows.contains("Task 1"));
        assert!(!rows.contains("Host"));
        worker.select(None, &out);
        assert!(worker.take().is_none());
        drop(worker);
        ended
            .recv_timeout(Duration::from_secs(2))
            .expect("worker did not cancel");
        join.join().unwrap();
    }

    #[test]
    fn hiding_or_changing_scope_discards_pending_old_results() {
        let mut worker = Worker::default();
        let (out, _) = std::sync::mpsc::channel();
        {
            let mut state = worker.state.0.lock().unwrap();
            state.query = Some(Query::Resources { after: None });
            state.latest = Some((
                0,
                View {
                    rows: "old Host data".into(),
                    ..Default::default()
                },
            ));
            state.notified = true;
        }
        worker.select(None, &out);
        assert!(worker.take().is_none());
        assert!(!worker.started);
        assert!(!worker.state.0.lock().unwrap().notified);
    }

    #[test]
    fn absent_unknown_zero_and_stale_remain_distinct() {
        let absent = resource_view(&monitor(None), &View::default());
        assert!(absent.stale);
        assert!(absent.rows.is_empty());
        assert_eq!(observed(&Observed::Unknown), "unknown");
        assert_eq!(observed(&Observed::Known(Decimal(0))), "0");
        let live = resource_view(
            &monitor(Some(MonitorPayload {
                durable: Durable::default(),
                capacities: vec![],
                registered: Registered::default(),
            })),
            &View::default(),
        );
        assert!(live.rows.contains("Inference: unknown"));
        assert!(live.rows.contains("CPU 0 GPU 0"));
        let stale = resource_view(&monitor(None), &live);
        assert!(stale.stale);
        assert_eq!(stale.rows, live.rows);
        assert!(!live.stale);
    }

    #[test]
    fn worker_is_lazy_and_hidden_selection_never_connects() {
        let mut worker = Worker::default();
        let (out, receiver) = std::sync::mpsc::channel();
        worker.select(None, &out);
        assert!(!worker.started);
        assert!(worker.take().is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn current_scope_requires_live_foreground_metadata_and_rejects_older_sessions() {
        let mut current = CurrentConversation::default();
        assert_eq!(current.scope(), None);
        let old = tachyon_api::InteractionMetadata::new("a", "b", "old-random-id", 1);
        current.observe("worker", &old);
        assert_eq!(current.scope(), None);
        current.observe(tachyon_api::FOREGROUND_ID, &old);
        let latest = tachyon_api::InteractionMetadata::new("c", "d", "new-random-id", 2);
        current.observe(tachyon_api::FOREGROUND_ID, &latest);
        current.observe(tachyon_api::FOREGROUND_ID, &old);
        assert_eq!(
            current.scope(),
            Some(TodoScope::Conversation {
                id: "new-random-id".into()
            })
        );
    }

    #[test]
    fn socket_snapshot_subscribe_coalesces_and_cancel_interrupts_read() {
        let (client, mut server) = UnixStream::pair().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let worker = Worker::default();
        worker.state.0.lock().unwrap().query = Some(Query::Todos {
            turn: None,
            scope: scope(),
            cursor: None,
        });
        let shared = worker.state.clone();
        let (out, receiver) = std::sync::mpsc::channel();
        let connection = Mutex::new(Some(client));
        let join = std::thread::spawn(move || {
            run(shared, out, || {
                connection
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| io::Error::other("one connection only"))
            })
        });
        let mut reader = BufReader::new(server.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::TodoSnapshot {
                limit: Some(100),
                cursor: None,
                ..
            }
        ));
        write_response(
            &mut server,
            &ApiResponse::Todo {
                response: snapshot(vec![todo(1)], 1),
            },
        )
        .unwrap();
        assert_eq!(
            read_request(&mut reader).unwrap(),
            ApiRequest::OperationalSubscribe {
                scope: scope(),
                after: watermark(1)
            }
        );
        for revision in 2..=100 {
            write_response(
                &mut server,
                &ApiResponse::OperationalBatch {
                    batch: batch(todo(revision), revision),
                },
            )
            .unwrap();
        }
        // Wait for the final value without draining the single notification.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if worker
                .state
                .0
                .lock()
                .unwrap()
                .latest
                .as_ref()
                .is_some_and(|(_, v)| v.rows.contains("revision 100\n"))
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(matches!(
            receiver.try_recv().unwrap(),
            super::super::TuiEvent::Operational
        ));
        assert!(receiver.try_recv().is_err());
        assert!(worker.take().unwrap().rows.contains("revision 100\n"));
        drop(worker);
        join.join().unwrap();
    }

    #[test]
    fn monitor_disconnect_publishes_stale_last_payload() {
        let (client, mut server) = UnixStream::pair().unwrap();
        let worker = Worker::default();
        worker.state.0.lock().unwrap().query = Some(Query::Resources { after: None });
        let shared = worker.state.clone();
        let (out, receiver) = std::sync::mpsc::channel();
        let connection = Mutex::new(Some(client));
        let join = std::thread::spawn(move || {
            run(shared, out, || {
                connection
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| io::Error::other("disconnected"))
            })
        });
        let mut reader = BufReader::new(server.try_clone().unwrap());
        assert!(matches!(
            read_request(&mut reader).unwrap(),
            ApiRequest::MonitorSubscribe { after: None, .. }
        ));
        write_response(
            &mut server,
            &ApiResponse::Monitor {
                snapshot: monitor(Some(MonitorPayload {
                    durable: Durable::default(),
                    capacities: vec![],
                    registered: Registered::default(),
                })),
            },
        )
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let live = worker.take().unwrap();
        assert!(!live.stale);
        server.shutdown(Shutdown::Both).unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let stale = worker.take().unwrap();
        assert!(stale.stale);
        assert_eq!(stale.rows, live.rows);
        drop(worker);
        join.join().unwrap();
    }
}
