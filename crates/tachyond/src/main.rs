#![forbid(unsafe_code)]

mod allocation_policy;
mod history_store;
mod messaging;
mod monitor;
mod runtime_store;
use tachyond::artifact_store;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

use chrono::{Datelike, Duration as ChronoDuration, Local, LocalResult, TimeZone};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use tachyon_api::types::{
    AgentEvent as StructuredAgentEvent, AgentInfo, AgentState, ApiRequest, ApiResponse,
    BackgroundCoordinatorInfo, BackgroundScheduleAction, BackgroundScheduleDecision,
    BackgroundScheduleRequest, DaemonInfo, EventEnvelope, EventStream, LifecycleRecommendation,
    LifetimeClass, MemoryMutationResult, MemoryRecallItem, MemoryRecallKind, PendingWorkReviewInfo,
    ScheduleDay, WorkOutcome, WorkRequest, WorkResult, WorkReviewContext, WorkReviewDecision,
    WorkReviewFailure, WorkReviewRecommendation, WorkReviewRequest, PROTO_VERSION,
};
use tachyon_api::{InteractionCommand, BACKGROUND_ID, FOREGROUND_ID, MEMORY_ID};
use tachyon_util::guard;

use crate::history_store::HistoryStore;
use crate::messaging::{
    acknowledge_reminder_notification, emit_schedule_event, encode_interaction_command,
    encode_reminder_notification, encode_scheduled_task_notification, persist_interaction_history,
    project_pending_history, stream_agent, stream_work, work_attention_notification,
};
use crate::runtime_store::{RuntimeStore, RuntimeTaskRecord};
use tachyon_memory::{MemoryMutationSource, MemoryStore};

const DEFAULT_BACKGROUND_REVIEW_TIMEOUT_SECS: u64 = 20;

/// One streamed event, broadcast to subscribers.
#[derive(Clone)]
struct AgentEvent {
    stream: EventStream,
    data: String,
}

/// A running agent: metadata, the ghost child, and subscribers.
struct Task {
    info: AgentInfo,
    depends_on: Vec<String>,
    process: Option<Child>,
    stdin: Option<Arc<Mutex<std::process::ChildStdin>>>,
    subs: Vec<mpsc::Sender<AgentEvent>>,
    generation: u64,
    assignment: u64,
    warm: bool,
    ready: bool,
    owner: Option<String>,
    last_used_secs: u64,
    control_socket: Option<String>,
    terminal_usage: Option<String>,
    terminal_result: Option<String>,
}

struct WorkRecord {
    request: WorkRequest,
    fingerprint: String,
    worker_id: String,
    info: AgentInfo,
    review: Option<PendingReview>,
    terminal_result: Option<String>,
    subs: Vec<mpsc::Sender<AgentEvent>>,
}

#[derive(Clone)]
struct PendingReview {
    request: WorkReviewRequest,
    started: std::time::Instant,
}

enum CoordinatorRequest {
    WorkReview(WorkReviewRequest),
    Schedule {
        request: BackgroundScheduleRequest,
        response: mpsc::SyncSender<BackgroundScheduleDecision>,
    },
}

struct Registry {
    service_shutdown: Arc<AtomicBool>,
    monitor: Option<Arc<monitor::Monitor>>,
    tasks: HashMap<String, Task>,
    works: HashMap<String, WorkRecord>,
    foreground_id: Option<String>,
    coordinator_tx: Option<mpsc::SyncSender<CoordinatorRequest>>,
    background_online: bool,
    background_generation: u64,
    runtime_store: Option<Arc<RuntimeStore>>,
    history_store: Option<Arc<HistoryStore>>,
    memory_store: Option<Arc<MemoryStore>>,
    campaigns: Option<Arc<runtime_store::campaign_launch::CampaignService>>,
    memory_started_secs: u64,
    memory_last_activity_secs: u64,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            service_shutdown: Arc::new(AtomicBool::new(false)),
            monitor: None,
            tasks: HashMap::new(),
            works: HashMap::new(),
            foreground_id: None,
            coordinator_tx: None,
            background_online: false,
            background_generation: 0,
            runtime_store: None,
            history_store: None,
            memory_store: None,
            campaigns: None,
            memory_started_secs: unix_now(),
            memory_last_activity_secs: 0,
        }
    }
}

impl Registry {
    fn next_id(&self) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{:02x}{:08x}", 0xA0, (n as u64) & 0xffff_ffff)
    }

    fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    fn get_mut(&mut self, id: &str) -> Option<&mut Task> {
        self.tasks.get_mut(id)
    }

    fn sorted(&self) -> Vec<AgentInfo> {
        let mut agents: Vec<AgentInfo> = self.tasks.values().map(|t| t.info.clone()).collect();
        agents.push(memory_agent_info(
            self.memory_started_secs,
            self.memory_last_activity_secs,
        ));
        agents.sort_by(|a, b| b.created_secs.cmp(&a.created_secs));
        agents
    }
}

fn memory_agent_info(created_secs: u64, last_activity_secs: u64) -> AgentInfo {
    AgentInfo {
        id: MEMORY_ID.into(),
        task: "Curate durable memory and support context compaction.".into(),
        state: AgentState::Running,
        pid: None,
        workspace: tachyon_util::daemon::databases_dir().display().to_string(),
        created_secs,
        retained: true,
        lease_until_secs: None,
        session_id: MEMORY_ID.into(),
        lifetime_class: LifetimeClass::Persistent,
        purpose: "memory".into(),
        owner: "daemon".into(),
        last_activity_secs,
        checkpoint_available: false,
        turns_used: 0,
        turn_budget: None,
        task_type: "memory".into(),
        description: "curated memory service".into(),
        persistent: true,
        sandboxed: false,
        stage_until_secs: None,
        logical_task_id: None,
        origin_turn_id: None,
        parent_task_id: None,
        tool_call_id: None,
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn schedule_due_at_ms(
    now_ms: u64,
    delay_seconds: Option<u64>,
    local_time: Option<&str>,
    day: Option<ScheduleDay>,
) -> Result<u64, String> {
    match (delay_seconds, local_time, day) {
        (Some(delay), None, None) if (1..=31_536_000).contains(&delay) => {
            Ok(now_ms.saturating_add(delay.saturating_mul(1000)))
        }
        (None, Some(local_time), Some(day)) => {
            let (hour, minute) = local_time
                .split_once(':')
                .ok_or_else(|| "local time must use 24-hour HH:MM format".to_string())?;
            let hour = hour
                .parse::<u32>()
                .map_err(|_| "local time has an invalid hour".to_string())?;
            let minute = minute
                .parse::<u32>()
                .map_err(|_| "local time has an invalid minute".to_string())?;
            if hour > 23 || minute > 59 {
                return Err("local time must use 24-hour HH:MM format".into());
            }
            let now = Local
                .timestamp_millis_opt(now_ms as i64)
                .single()
                .ok_or_else(|| "current local time is unavailable".to_string())?;
            let offset_days = match day {
                ScheduleDay::Next | ScheduleDay::Today => 0,
                ScheduleDay::Tomorrow => 1,
            };
            let date = now
                .date_naive()
                .checked_add_signed(ChronoDuration::days(offset_days))
                .ok_or_else(|| "scheduled date is out of range".to_string())?;
            let resolve = |date: chrono::NaiveDate| -> Result<chrono::DateTime<Local>, String> {
                match Local.with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0)
                {
                    LocalResult::Single(value) => Ok(value),
                    LocalResult::Ambiguous(first, _) => Ok(first),
                    LocalResult::None => Err("local time does not exist in this timezone".into()),
                }
            };
            let mut candidate = resolve(date)?;
            if day == ScheduleDay::Next && candidate.timestamp_millis() <= now_ms as i64 {
                let tomorrow = date
                    .checked_add_signed(ChronoDuration::days(1))
                    .ok_or_else(|| "scheduled date is out of range".to_string())?;
                candidate = resolve(tomorrow)?;
            }
            if candidate.timestamp_millis() <= now_ms as i64 {
                return Err("scheduled time is already in the past".into());
            }
            Ok(candidate.timestamp_millis() as u64)
        }
        _ => Err("provide either delay_seconds or local_time with day".into()),
    }
}

#[allow(clippy::too_many_arguments)]
fn work_fingerprint(
    task: &str,
    cwd: &Option<String>,
    depends_on: &[String],
    lifetime_class: LifetimeClass,
    purpose: &str,
    origin_turn_id: &Option<String>,
    parent_task_id: &Option<String>,
    tool_call_id: &Option<String>,
    deadline_ms: &Option<u64>,
) -> String {
    serde_json::to_string(&(
        task,
        cwd,
        depends_on,
        lifetime_class,
        purpose,
        origin_turn_id,
        parent_task_id,
        tool_call_id,
        deadline_ms,
    ))
    .unwrap_or_default()
}

fn existing_work(
    registry: &Registry,
    work_id: &str,
    fingerprint: &str,
) -> Result<Option<AgentInfo>, String> {
    let Some(work) = registry.works.get(work_id) else {
        return Ok(None);
    };
    if work.fingerprint != fingerprint {
        return Err(format!(
            "work id {work_id} was already used for a different request"
        ));
    }
    Ok(Some(work.info.clone()))
}

static DAEMON_EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1 << 63);

fn reap_warm_workers(registry: &Arc<Mutex<Registry>>) {
    let configured_ttl = std::env::var("TACHYON_WARM_AGENT_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    let now = unix_now();
    let cutoff = configured_ttl.map(|ttl| now.saturating_sub(ttl));
    let mut released = Vec::new();
    let mut staged = Vec::new();
    let mut children = Vec::new();
    let mut supervisor_sockets = Vec::new();
    let mut pids = Vec::new();
    {
        let mut reg = registry.lock().unwrap();
        for task in reg.tasks.values_mut() {
            let lease_expired = task
                .info
                .lease_until_secs
                .is_some_and(|deadline| deadline <= now);
            let safety_expired = cutoff.is_some_and(|limit| {
                !task.info.retained && task.last_used_secs > 0 && task.last_used_secs <= limit
            });
            let budget_expired = task
                .info
                .turn_budget
                .is_some_and(|budget| task.info.turns_used >= budget);
            if !task.warm {
                continue;
            }
            let expired = lease_expired || safety_expired || budget_expired;
            if matches!(task.info.state, AgentState::Waiting | AgentState::Completed) && expired {
                task.info.state = AgentState::Staged;
                task.info.stage_until_secs = Some(now.saturating_add(60));
                staged.push(task.info.clone());
                continue;
            }
            if task.info.state != AgentState::Staged
                || !task
                    .info
                    .stage_until_secs
                    .is_some_and(|deadline| deadline <= now)
            {
                continue;
            }
            task.generation = task.generation.wrapping_add(1);
            if let Some(child) = task.process.take() {
                children.push(child);
            } else if let Some(socket) = task.control_socket.as_deref() {
                supervisor_sockets.push(socket.to_string());
            } else if let Some(pid) = task.info.pid {
                pids.push(pid);
            }
            task.stdin = None;
            task.info.state = AgentState::Released;
            task.info.pid = None;
            for tx in task.subs.drain(..) {
                let _ = tx.send(AgentEvent {
                    stream: EventStream::Exit,
                    data: "warm worker retired after idle timeout".into(),
                });
            }
            released.push(task.info.clone());
        }
    }
    for mut child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    for socket in supervisor_sockets {
        let _ = supervisor_command(&socket, "signal\tterm\n");
    }
    for pid in pids {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    for info in staged {
        persist_task(
            registry,
            &info,
            "Agent staged before automatic termination.",
        );
    }
    for info in released {
        persist_task(registry, &info, "Warm worker retired after idle timeout.");
        cleanup_workspace(&info);
    }
}

fn persist_task(registry: &Arc<Mutex<Registry>>, info: &AgentInfo, note: &str) {
    if info.id == FOREGROUND_ID {
        return;
    }
    let (runtime_store, runtime_task) = {
        let reg = registry.lock().unwrap();
        let task = reg.tasks.get(&info.id);
        (
            reg.runtime_store.clone(),
            RuntimeTaskRecord {
                schema_version: 1,
                updated_at_ms: unix_now_ms(),
                info: info.clone(),
                depends_on: task.map(|task| task.depends_on.clone()).unwrap_or_default(),
                generation: task.map(|task| task.generation).unwrap_or_default(),
                assignment: task.map(|task| task.assignment).unwrap_or_default(),
                warm: task.is_some_and(|task| task.warm),
                ready: task.is_some_and(|task| task.ready),
                owner: task.and_then(|task| task.owner.clone()),
                last_used_secs: task.map(|task| task.last_used_secs).unwrap_or_default(),
                control_socket: task.and_then(|task| task.control_socket.clone()),
                terminal_usage: task.and_then(|task| task.terminal_usage.clone()),
                terminal_result: task.and_then(|task| task.terminal_result.clone()),
            },
        )
    };
    let Some(store) = runtime_store else {
        eprintln!("tachyond: runtime store missing for task {}", info.id);
        return;
    };
    if let Err(error) = store.persist_task_transition(&runtime_task, note, None) {
        eprintln!("tachyond: runtime write {}: {error}", info.id);
    }
}

fn scheduled_result_text(data: &str) -> (String, bool) {
    let result = serde_json::from_str::<EventEnvelope>(data)
        .ok()
        .and_then(|event| match event.kind {
            StructuredAgentEvent::WorkResult { result } => Some(result),
            _ => None,
        });
    let Some(result) = result else {
        return (data.to_string(), false);
    };
    match result.outcome {
        WorkOutcome::Completed { result, .. } => (result.clone(), false),
        WorkOutcome::Blocked { reason } => (format!("Blocked: {reason}"), true),
        WorkOutcome::Failed { message } => (format!("Failed: {message}"), true),
        WorkOutcome::Cancelled { reason } => (format!("Cancelled: {reason}"), true),
        WorkOutcome::TimedOut { deadline_ms } => {
            (format!("Timed out at deadline {deadline_ms}."), true)
        }
    }
}

fn execute_scheduled_task(
    registry: Arc<Mutex<Registry>>,
    store: Arc<RuntimeStore>,
    task: tachyon_api::ScheduledTaskInfo,
) {
    let work_id = task
        .work_id
        .clone()
        .unwrap_or_else(|| format!("scheduled-work-{}", task.id));
    let deadline_ms = match task.mode {
        tachyon_api::ScheduledTaskMode::FinishBy => task.due_at_ms,
        tachyon_api::ScheduledTaskMode::StartAt => {
            unix_now_ms().saturating_add(worker_result_timeout().as_millis() as u64)
        }
    };
    if deadline_ms <= unix_now_ms() {
        let _ = store.store_scheduled_task_result(
            &task.id,
            "The task deadline passed before execution could begin.",
            true,
        );
        return;
    }
    let request = ApiRequest::BackgroundDelegate {
        task: task.objective.clone(),
        cwd: None,
        depends_on: Vec::new(),
        lifetime_class: LifetimeClass::Short,
        purpose: "scheduled_task".into(),
        logical_task_id: Some(work_id.clone()),
        origin_turn_id: Some(task.turn.to_string()),
        parent_task_id: None,
        tool_call_id: Some(task.id.clone()),
        deadline_ms: Some(deadline_ms),
    };
    if let ApiResponse::Error { message, .. } = dispatch(&request, &registry) {
        let _ = store.store_scheduled_task_result(&task.id, &message, true);
        return;
    }
    loop {
        let result = registry
            .lock()
            .unwrap()
            .works
            .get(&work_id)
            .and_then(|work| work.terminal_result.clone());
        if let Some(result) = result {
            let (text, failed) = scheduled_result_text(&result);
            if let Err(error) = store.store_scheduled_task_result(&task.id, &text, failed) {
                eprintln!("tachyond: store scheduled task result {}: {error}", task.id);
            }
            return;
        }
        if unix_now_ms() > deadline_ms.saturating_add(1_000) {
            let _ = store.store_scheduled_task_result(
                &task.id,
                "The scheduled task did not complete before its deadline.",
                true,
            );
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn run_reminder_scheduler(registry: Arc<Mutex<Registry>>, shutdown: Arc<AtomicBool>) {
    if let Some(store) = registry.lock().unwrap().runtime_store.clone() {
        if let Err(error) = store.recover_scheduled_tasks() {
            eprintln!("tachyond: recover scheduled tasks: {error}");
        }
    }
    while !shutdown.load(Ordering::SeqCst) {
        let (store, user_connected) = {
            let registry = registry.lock().unwrap();
            (
                registry.runtime_store.clone(),
                registry
                    .tasks
                    .get(FOREGROUND_ID)
                    .is_some_and(|foreground| !foreground.subs.is_empty()),
            )
        };
        if let Some(store) = store {
            // Best-effort compact transition only; durable questions remain available
            // through typed CLI input even with no connected Conversation.
            let attention = store
                .attention_notifications
                .lock()
                .unwrap()
                .drain(..)
                .collect::<Vec<_>>();
            if user_connected {
                for q in attention {
                    if let Ok(command) = work_attention_notification(&q) {
                        if let Ok(input) = task_input(&registry, FOREGROUND_ID) {
                            let _ = write_task_input(input, FOREGROUND_ID, &command);
                        }
                    }
                }
            }
            match store.claim_ready_scheduled_tasks(unix_now_ms(), 8) {
                Ok(tasks) => {
                    for task in tasks {
                        let task_registry = Arc::clone(&registry);
                        let task_store = Arc::clone(&store);
                        std::thread::spawn(move || {
                            execute_scheduled_task(task_registry, task_store, task)
                        });
                    }
                }
                Err(error) => eprintln!("tachyond: poll scheduled tasks: {error}"),
            }
            if user_connected {
                match store.claim_scheduled_task_notifications(unix_now_ms(), 8) {
                    Ok(notifications) => {
                        for (task, result) in notifications {
                            let delivery = encode_scheduled_task_notification(&task, &result)
                                .and_then(|command| {
                                    task_input(&registry, FOREGROUND_ID).and_then(|input| {
                                        write_task_input(input, FOREGROUND_ID, &command)
                                    })
                                });
                            if let Err(error) = delivery {
                                eprintln!("tachyond: deliver scheduled task {}: {error}", task.id);
                            }
                        }
                    }
                    Err(error) => eprintln!("tachyond: poll scheduled task results: {error}"),
                }
                match store.claim_due_reminders(unix_now_ms(), 32) {
                    Ok(reminders) => {
                        for reminder in reminders {
                            let delivery =
                                encode_reminder_notification(&reminder).and_then(|command| {
                                    task_input(&registry, FOREGROUND_ID).and_then(|input| {
                                        write_task_input(input, FOREGROUND_ID, &command)
                                    })
                                });
                            if let Err(error) = delivery {
                                eprintln!("tachyond: deliver reminder {}: {error}", reminder.id);
                                if let Err(release_error) =
                                    store.release_reminder_delivery(&reminder.id)
                                {
                                    eprintln!(
                                        "tachyond: release reminder {}: {release_error}",
                                        reminder.id
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => eprintln!("tachyond: poll reminders: {error}"),
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

fn emit_memory_event(
    registry: &Arc<Mutex<Registry>>,
    conversation_id: String,
    turn_id: Option<String>,
    kind: StructuredAgentEvent,
) {
    registry.lock().unwrap().memory_last_activity_secs = unix_now();
    let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let envelope = EventEnvelope {
        event_id: sequence,
        session_id: MEMORY_ID.into(),
        conversation_id: Some(conversation_id),
        turn_id,
        task_id: None,
        parent_task_id: None,
        tool_call_id: None,
        actor: tachyon_api::Actor::System,
        sequence,
        occurred_at_ms: unix_now_ms(),
        kind,
    };
    if let Ok(data) = serde_json::to_string(&envelope) {
        push_event(registry, FOREGROUND_ID, EventStream::Stdout, &data);
    }
}

fn coordinate_schedule(
    registry: &Arc<Mutex<Registry>>,
    action: BackgroundScheduleAction,
) -> Result<BackgroundScheduleAction, String> {
    let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let request = BackgroundScheduleRequest {
        request_id: format!("schedule-command-{sequence}"),
        action: action.clone(),
    };
    let coordinator = registry
        .lock()
        .unwrap()
        .coordinator_tx
        .clone()
        .ok_or_else(|| "background coordinator unavailable".to_string())?;
    let (response_tx, response_rx) = mpsc::sync_channel(1);
    coordinator
        .try_send(CoordinatorRequest::Schedule {
            request: request.clone(),
            response: response_tx,
        })
        .map_err(|_| "background coordinator queue unavailable".to_string())?;
    let decision = response_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| "background coordinator schedule decision timed out".to_string())?;
    if decision.request_id != request.request_id || decision.action != action {
        return Err("background coordinator returned a mismatched schedule decision".into());
    }
    if !decision.approved {
        return Err(decision.reason);
    }
    Ok(decision.action)
}

fn history_recall_window(query: &str, now_ms: u64) -> (u64, u64) {
    const DAY_MS: u64 = 86_400_000;
    let query = query.to_ascii_lowercase();
    let today = now_ms / DAY_MS;
    if query.contains("yesterday") {
        return (
            today.saturating_sub(1) * DAY_MS,
            today.saturating_mul(DAY_MS),
        );
    }
    if query.contains("today") {
        return (today.saturating_mul(DAY_MS), now_ms.saturating_add(1));
    }
    let weekdays = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ];
    if query.contains("last ") {
        if let Some(target) = weekdays
            .iter()
            .position(|weekday| query.contains(&format!("last {weekday}")))
        {
            // 1970-01-01 was Thursday. Weekday indices use Monday = 0.
            let current = ((today + 3) % 7) as usize;
            let mut days_back = (current + 7 - target) % 7;
            if days_back == 0 {
                days_back = 7;
            }
            let day = today.saturating_sub(days_back as u64);
            return (day * DAY_MS, day.saturating_add(1) * DAY_MS);
        }
    }
    if query.contains("last week") || query.contains("recently") {
        return (now_ms.saturating_sub(7 * DAY_MS), now_ms.saturating_add(1));
    }
    (0, now_ms.saturating_add(1))
}

fn recall_task_history(
    store: &RuntimeStore,
    query: &str,
    limit: usize,
) -> Result<Vec<MemoryRecallItem>, String> {
    let query_tokens = recall_tokens(query);
    let mut ranked = store
        .list_tasks()?
        .into_iter()
        .map(|record| {
            let searchable = format!(
                "{} {} {}",
                record.info.task, record.info.purpose, record.info.description
            );
            let candidate_tokens = recall_tokens(&searchable);
            let score = query_tokens
                .iter()
                .filter(|token| candidate_tokens.contains(*token))
                .count();
            (score, record)
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| {
                right
                    .info
                    .last_activity_secs
                    .cmp(&left.info.last_activity_secs)
            })
            .then_with(|| left.info.id.cmp(&right.info.id))
    });
    Ok(ranked
        .into_iter()
        .take(limit)
        .map(|(_, record)| MemoryRecallItem {
            kind: MemoryRecallKind::TaskHistory,
            memory_id: None,
            descriptor: None,
            text: format!(
                "{} [{}]: {}",
                record.info.id, record.info.state, record.info.task
            ),
            occurred_at_ms: record
                .info
                .last_activity_secs
                .max(record.info.created_secs)
                .saturating_mul(1000),
        })
        .collect())
}

fn recall_tokens(text: &str) -> std::collections::BTreeSet<String> {
    const STOP_WORDS: [&str; 17] = [
        "about", "could", "doing", "from", "have", "history", "last", "past", "please", "remember",
        "that", "this", "what", "when", "were", "with", "worked",
    ];
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|token| token.len() > 2 && !STOP_WORDS.contains(&token.as_str()))
        .collect()
}

fn handle_context_compaction(registry: &Arc<Mutex<Registry>>, agent_id: &str, data: &str) {
    let Ok(envelope) = serde_json::from_str::<EventEnvelope>(data) else {
        return;
    };
    if let StructuredAgentEvent::ContextCompacted {
        request_id,
        retained_context_tokens,
        ..
    } = &envelope.kind
    {
        let store = registry.lock().unwrap().runtime_store.clone();
        if let Some(store) = store {
            if let Err(error) = store.complete_context_compaction(
                agent_id,
                request_id,
                *retained_context_tokens,
                envelope.occurred_at_ms,
            ) {
                eprintln!("tachyond: complete context compaction: {error}");
            }
        }
        return;
    }
    let StructuredAgentEvent::Usage {
        context_tokens,
        context_window: Some(context_window),
        ..
    } = envelope.kind
    else {
        return;
    };
    let (store, generation, assignment) = {
        let registry = registry.lock().unwrap();
        let Some(task) = registry.tasks.get(agent_id) else {
            return;
        };
        if agent_id != FOREGROUND_ID && !task.warm {
            return;
        }
        (
            registry.runtime_store.clone(),
            task.generation,
            task.assignment,
        )
    };
    let Some(store) = store else { return };
    let command = match store.observe_context_usage(
        agent_id,
        generation,
        assignment,
        envelope.event_id,
        context_tokens,
        context_window,
        envelope.occurred_at_ms,
    ) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("tachyond: schedule context compaction: {error}");
            return;
        }
    };
    let Some(command) = command else { return };
    let encoded = match serde_json::to_string(&command) {
        Ok(encoded) => encoded,
        Err(error) => {
            eprintln!("tachyond: encode context compaction: {error}");
            return;
        }
    };
    if let Err(error) =
        task_input(registry, agent_id).and_then(|input| write_task_input(input, agent_id, &encoded))
    {
        eprintln!("tachyond: deliver context compaction to {agent_id}: {error}");
    }
}

fn restore_runtime_tasks(registry: &Arc<Mutex<Registry>>) -> Result<(), String> {
    let store = registry
        .lock()
        .unwrap()
        .runtime_store
        .clone()
        .ok_or_else(|| "runtime store unavailable".to_string())?;
    let records = store.list_tasks()?;
    let mut reg = registry.lock().unwrap();
    for record in records {
        if record.info.id == FOREGROUND_ID {
            continue;
        }
        // Only persistent sessions are eligible for daemon restart recovery.
        // Short and long workers remain historical records and are not revived.
        if record.info.lifetime_class != LifetimeClass::Persistent {
            continue;
        }
        let mut info = record.info;
        if matches!(info.state, AgentState::Running | AgentState::Starting)
            || (info.state == AgentState::Completed && info.retained)
        {
            info.state = AgentState::Created;
        }
        if info
            .stage_until_secs
            .is_some_and(|deadline| deadline > unix_now())
        {
            info.state = AgentState::Staged;
        }
        info.pid = None;
        let fallback_control_socket = format!("{}/.tachyon/agent.sock", info.workspace);
        reg.tasks.entry(info.id.clone()).or_insert_with(|| Task {
            info,
            depends_on: record.depends_on,
            process: None,
            stdin: None,
            subs: Vec::new(),
            generation: record.generation,
            assignment: record.assignment,
            warm: record.warm,
            ready: false,
            owner: record.owner,
            last_used_secs: record.last_used_secs,
            control_socket: record.control_socket.or(Some(fallback_control_socket)),
            terminal_usage: record.terminal_usage,
            terminal_result: record.terminal_result,
        });
    }
    Ok(())
}

fn dependencies_satisfied(registry: &Registry, depends_on: &[String]) -> bool {
    depends_on.iter().all(|id| {
        registry
            .tasks
            .get(id)
            .is_some_and(|task| task.info.state == AgentState::Completed)
    })
}

fn normalized_task_type(purpose: &str) -> String {
    purpose.trim().to_ascii_lowercase()
}

#[derive(Clone)]
enum TaskInput {
    Pipe(Arc<Mutex<std::process::ChildStdin>>),
    Supervisor(String),
}

fn task_input(registry: &Arc<Mutex<Registry>>, id: &str) -> Result<TaskInput, String> {
    let registry = registry.lock().unwrap();
    let task = registry
        .tasks
        .get(id)
        .ok_or_else(|| format!("no such agent: {id}"))?;
    if let Some(socket) = &task.control_socket {
        Ok(TaskInput::Supervisor(socket.clone()))
    } else if let Some(stdin) = &task.stdin {
        Ok(TaskInput::Pipe(Arc::clone(stdin)))
    } else {
        Err(format!("agent {id}: not writable (no chat)"))
    }
}

fn write_task_input(input: TaskInput, id: &str, text: &str) -> Result<(), String> {
    let line = format!("{text}\n");
    match input {
        TaskInput::Pipe(stdin) => stdin
            .lock()
            .map_err(|_| format!("agent {id}: input lock poisoned"))?
            .write_all(line.as_bytes())
            .map_err(|error| format!("agent {id}: {error}")),
        TaskInput::Supervisor(socket) => {
            let mut stream = UnixStream::connect(socket)
                .map_err(|error| format!("agent {id}: supervisor: {error}"))?;
            stream
                .write_all(format!("input\t{line}").as_bytes())
                .map_err(|error| format!("agent {id}: supervisor: {error}"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn claim_reusable_worker(
    registry: &mut Registry,
    task: &str,
    purpose: &str,
    lifetime_class: LifetimeClass,
    cwd: Option<&String>,
    depends_on: &[String],
    logical_task_id: &Option<String>,
    origin_turn_id: &Option<String>,
    parent_task_id: &Option<String>,
    tool_call_id: &Option<String>,
) -> Option<AgentInfo> {
    let selected = tachyon_util::daemon::selected_workspace(cwd?).ok()?;
    if !dependencies_satisfied(registry, depends_on) {
        return None;
    }
    let task_type = if purpose.trim().is_empty() {
        "general".into()
    } else {
        normalized_task_type(purpose)
    };
    let candidate = registry.tasks.values_mut().find(|candidate| {
        candidate.warm
            && tachyon_util::daemon::selected_workspace(&candidate.info.workspace)
                .ok()
                .as_ref()
                == Some(&selected)
            && candidate.info.retained
            && candidate.info.owner == "background"
            && candidate.info.state == AgentState::Completed
            && candidate.info.lifetime_class == lifetime_class
            && candidate.info.task_type == task_type
            && candidate.ready
            && (candidate.stdin.is_some() || candidate.control_socket.is_some())
    })?;
    let now = unix_now();
    candidate.info.task = task.to_string();
    candidate.info.state = AgentState::Running;
    candidate.info.purpose = if purpose.trim().is_empty() {
        task.to_string()
    } else {
        purpose.to_string()
    };
    candidate.info.task_type = task_type;
    candidate.info.description = task.to_string();
    candidate.info.last_activity_secs = now;
    candidate.info.logical_task_id = logical_task_id.clone();
    candidate.info.origin_turn_id = origin_turn_id.clone();
    candidate.info.parent_task_id = parent_task_id.clone();
    candidate.info.tool_call_id = tool_call_id.clone();
    candidate.assignment = candidate.assignment.wrapping_add(1);
    candidate.depends_on = depends_on.to_vec();
    candidate.last_used_secs = now;
    candidate.terminal_usage = None;
    candidate.terminal_result = None;
    candidate.ready = false;
    Some(candidate.info.clone())
}

fn deliver_task(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    task: &str,
    persistent: bool,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let result = task_input(registry, id).and_then(|input| write_task_input(input, id, task));
        if result.is_ok() || !persistent || std::time::Instant::now() >= deadline {
            return result;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

fn deliver_work(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    request: &WorkRequest,
    persistent: bool,
) -> Result<(), String> {
    let encoded = serde_json::to_string(request).map_err(|error| error.to_string())?;
    deliver_task(registry, id, &encoded, persistent)
}

/// Recreate workers for durable non-terminal tasks only after their prerequisites
/// are complete. The task remains visible as Created/Starting/Failed while this
/// synchronous spawn attempt is in progress.
fn start_ready_tasks(registry: &Arc<Mutex<Registry>>) {
    loop {
        let (candidate, generation, assignment) = {
            let mut reg = registry.lock().unwrap();
            let Some(id) = reg.tasks.values().find_map(|task| {
                if matches!(task.info.state, AgentState::Created | AgentState::Waiting)
                    && dependencies_satisfied(&reg, &task.depends_on)
                {
                    Some(task.info.id.clone())
                } else {
                    None
                }
            }) else {
                return;
            };
            let task = reg.tasks.get_mut(&id).unwrap();
            task.info.state = AgentState::Starting;
            task.generation = task.generation.wrapping_add(1);
            (task.info.clone(), task.generation, task.assignment)
        };
        persist_task(registry, &candidate, "Restoring agent.");

        if candidate.persistent {
            let socket = std::path::Path::new(&candidate.workspace).join(".tachyon/agent.sock");
            if socket.exists() {
                let info = {
                    let mut reg = registry.lock().unwrap();
                    let task = reg.tasks.get_mut(&candidate.id).unwrap();
                    if task.generation != generation || task.assignment != assignment {
                        continue;
                    }
                    task.info.state = AgentState::Running;
                    task.info.pid = std::fs::read_to_string(format!("{}.pid", socket.display()))
                        .ok()
                        .and_then(|pid| pid.trim().parse().ok());
                    task.info.clone()
                };
                persist_task(registry, &info, "Reattached persistent agent supervisor.");
                let pump_reg = Arc::clone(registry);
                let id = candidate.id.clone();
                std::thread::spawn(move || pump_agent(&pump_reg, &id, generation));
                continue;
            }
        }

        let result = std::fs::create_dir_all(&candidate.workspace).and_then(|_| {
            if candidate.retained {
                if candidate.persistent {
                    spawn_supervised_ghost(&candidate.id, &candidate.workspace)
                } else {
                    spawn_warm_ghost(&candidate.id, &candidate.workspace)
                }
            } else {
                spawn_ghost(&candidate.id, &candidate.task, &candidate.workspace)
            }
        });
        match result {
            Ok(mut child) => {
                let stdin = child.stdin.take().map(|stdin| Arc::new(Mutex::new(stdin)));
                let mut child = Some(child);
                let info = {
                    let mut reg = registry.lock().unwrap();
                    let Some(task) = reg.tasks.get_mut(&candidate.id) else {
                        if let Some(mut child) = child {
                            let _ = child.kill();
                            let _ = child.wait();
                        }
                        continue;
                    };
                    if task.generation != generation || task.assignment != assignment {
                        drop(reg);
                        if let Some(mut child) = child {
                            let _ = child.kill();
                            let _ = child.wait();
                        }
                        continue;
                    }
                    task.process = child.take();
                    task.stdin = stdin;
                    task.info.pid = task.process.as_ref().map(Child::id);
                    task.info.state = AgentState::Running;
                    task.info.clone()
                };
                let work_request = candidate.logical_task_id.as_ref().and_then(|work_id| {
                    let mut reg = registry.lock().unwrap();
                    let work = reg.works.get_mut(work_id)?;
                    work.request.generation = generation;
                    work.request.assignment = assignment;
                    Some(work.request.clone())
                });
                if let Some(request) = &work_request {
                    collect_worker_result(
                        Arc::clone(registry),
                        request.work_id.clone(),
                        FOREGROUND_ID.into(),
                    );
                }
                if candidate.retained {
                    let delivery = if let Some(request) = &work_request {
                        deliver_work(registry, &candidate.id, request, candidate.persistent)
                    } else {
                        deliver_task(
                            registry,
                            &candidate.id,
                            &candidate.task,
                            candidate.persistent,
                        )
                    };
                    if let Err(error) = delivery {
                        eprintln!("tachyond: failed to restore task input: {error}");
                    }
                }
                persist_task(registry, &info, "Agent restored.");
                let pump_reg = Arc::clone(registry);
                let id = candidate.id.clone();
                std::thread::spawn(move || pump_agent(&pump_reg, &id, generation));
            }
            Err(error) => {
                eprintln!("tachyond: failed to restore {}: {error}", candidate.id);
                let info = {
                    let mut reg = registry.lock().unwrap();
                    let task = reg.tasks.get_mut(&candidate.id).unwrap();
                    if task.generation == generation && task.assignment == assignment {
                        task.info.state = AgentState::Failed;
                    }
                    task.info.clone()
                };
                persist_task(registry, &info, &format!("restore failed: {error}"));
            }
        }
    }
}

/// Fan a line out to every subscriber of an agent. Drops dead subscribers.
fn correlate_event(data: &str, info: &AgentInfo) -> String {
    let Ok(mut envelope) = serde_json::from_str::<EventEnvelope>(data) else {
        return data.to_string();
    };
    envelope.task_id = info.logical_task_id.clone().or(envelope.task_id);
    envelope.parent_task_id = info.parent_task_id.clone().or(envelope.parent_task_id);
    if let StructuredAgentEvent::ArtifactRegistered { artifact } = &mut envelope.kind {
        artifact.task_id = info.logical_task_id.clone().or(artifact.task_id.take());
    }
    if matches!(
        envelope.kind,
        StructuredAgentEvent::Usage { .. }
            | StructuredAgentEvent::ToolTelemetry { .. }
            | StructuredAgentEvent::ArtifactRegistered { .. }
            | StructuredAgentEvent::WorkerCompleted { .. }
            | StructuredAgentEvent::WorkCandidate { .. }
            | StructuredAgentEvent::WorkResult { .. }
    ) {
        envelope.turn_id = info.origin_turn_id.clone().or(envelope.turn_id);
        envelope.tool_call_id = info.tool_call_id.clone().or(envelope.tool_call_id);
    }
    serde_json::to_string(&envelope).unwrap_or_else(|_| data.to_string())
}

fn result_envelope(worker_id: &str, result: WorkResult) -> EventEnvelope {
    let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    EventEnvelope {
        event_id: sequence,
        session_id: worker_id.to_string(),
        conversation_id: None,
        turn_id: None,
        task_id: Some(result.work_id.clone()),
        parent_task_id: None,
        tool_call_id: None,
        actor: tachyon_api::Actor::System,
        sequence,
        occurred_at_ms: unix_now_ms(),
        kind: StructuredAgentEvent::WorkResult { result },
    }
}

fn handle_work_candidate(
    registry: &Arc<Mutex<Registry>>,
    worker_id: &str,
    envelope: EventEnvelope,
) {
    let StructuredAgentEvent::WorkCandidate { mut candidate } = envelope.kind else {
        return;
    };
    if let Some(timing) = candidate.timing.as_mut() {
        timing.review_ms = None;
    }
    let completed = matches!(candidate.outcome, WorkOutcome::Completed { .. });
    if !completed {
        let terminal = result_envelope(worker_id, candidate);
        if let Ok(data) = serde_json::to_string(&terminal) {
            push_event(registry, worker_id, EventStream::Stdout, &data);
        }
        return;
    }
    let (request, coordinator_tx) = {
        let mut reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(worker_id) else {
            return;
        };
        let valid_actor = matches!(
            &envelope.actor,
            tachyon_api::Actor::Worker { id } if id == worker_id
        );
        if envelope.session_id != worker_id
            || !valid_actor
            || task.generation != candidate.generation
            || task.assignment != candidate.assignment
            || task.info.logical_task_id.as_deref() != Some(candidate.work_id.as_str())
        {
            return;
        }
        let task_info = task.info.clone();
        let coordinator_generation = reg.background_generation;
        let review_id = format!(
            "review:{}:{}:{}:{}",
            candidate.work_id, candidate.generation, candidate.assignment, envelope.event_id
        );
        let deadline_ms = background_review_deadline_ms(unix_now_ms(), background_review_timeout());
        let Some(work) = reg.works.get_mut(&candidate.work_id) else {
            return;
        };
        if work.terminal_result.is_some()
            || work.review.is_some()
            || work.request.objective != candidate.objective
        {
            return;
        }
        let request = WorkReviewRequest {
            review_id,
            coordinator_generation,
            candidate,
            worker: WorkReviewContext {
                worker_id: worker_id.to_string(),
                current_lifetime_class: task_info.lifetime_class,
                turns_used: task_info.turns_used,
                turn_budget: task_info.turn_budget,
                purpose: task_info.purpose,
            },
            deadline_ms,
        };
        work.review = Some(PendingReview {
            request: request.clone(),
            started: std::time::Instant::now(),
        });
        (request, reg.coordinator_tx.clone())
    };
    let queued = coordinator_tx.as_ref().is_some_and(|tx| {
        tx.try_send(CoordinatorRequest::WorkReview(request.clone()))
            .is_ok()
    });
    if !queued {
        fail_pending_review(
            registry,
            &request,
            WorkReviewFailure::CoordinatorUnavailable,
            "background coordinator unavailable",
        );
        return;
    }
    let timeout_registry = Arc::clone(registry);
    std::thread::spawn(move || {
        let delay = request.deadline_ms.saturating_sub(unix_now_ms());
        std::thread::sleep(std::time::Duration::from_millis(delay));
        fail_pending_review(
            &timeout_registry,
            &request,
            WorkReviewFailure::TimedOut,
            "background review timed out",
        );
    });
}

fn fail_pending_review(
    registry: &Arc<Mutex<Registry>>,
    request: &WorkReviewRequest,
    failure: WorkReviewFailure,
    message: &str,
) {
    let candidate = {
        let mut reg = registry.lock().unwrap();
        let Some(work) = reg.works.get_mut(&request.candidate.work_id) else {
            return;
        };
        let matches = work
            .review
            .as_ref()
            .is_some_and(|review| review.request.review_id == request.review_id);
        if !matches || work.terminal_result.is_some() {
            return;
        }
        let review = work.review.take().unwrap();
        let mut candidate = request.candidate.clone();
        candidate
            .timing
            .get_or_insert_with(Default::default)
            .review_ms = Some(review.started.elapsed().as_millis() as u64);
        candidate
    };
    let result = WorkResult {
        outcome: WorkOutcome::Failed {
            message: format!("{message}: {failure:?}"),
        },
        ..candidate
    };
    let envelope = result_envelope(&request.worker.worker_id, result);
    if let Ok(data) = serde_json::to_string(&envelope) {
        push_event(
            registry,
            &request.worker.worker_id,
            EventStream::Stdout,
            &data,
        );
    }
}

fn apply_work_review(registry: &Arc<Mutex<Registry>>, decision: WorkReviewDecision) {
    apply_work_review_at(registry, decision, unix_now_ms(), std::time::Instant::now());
}

fn apply_work_review_at(
    registry: &Arc<Mutex<Registry>>,
    decision: WorkReviewDecision,
    now_ms: u64,
    measured_at: std::time::Instant,
) {
    let (request, lifecycle) = {
        let mut reg = registry.lock().unwrap();
        let Some(work) = reg.works.get_mut(&decision.work_id) else {
            return;
        };
        let Some(review) = work.review.as_ref() else {
            return;
        };
        let request = &review.request;
        if request.review_id != decision.review_id
            || request.coordinator_generation != decision.coordinator_generation
            || request.candidate.generation != decision.generation
            || request.candidate.assignment != decision.assignment
            || request.deadline_ms <= now_ms
            || work.terminal_result.is_some()
        {
            return;
        }
        let mut request = request.clone();
        request
            .candidate
            .timing
            .get_or_insert_with(Default::default)
            .review_ms = measured_at
            .checked_duration_since(review.started)
            .map(|elapsed| elapsed.as_millis() as u64);
        work.review = None;
        let lifecycle = match &decision.recommendation {
            WorkReviewRecommendation::Accept { lifecycle } => Some(lifecycle.clone()),
            _ => None,
        };
        if let Some(LifecycleRecommendation::Retain { lifetime_class }) = lifecycle.as_ref() {
            if request.worker.current_lifetime_class != *lifetime_class {
                if let Some(task) = reg.tasks.get_mut(&request.worker.worker_id) {
                    task.info.lifetime_class = *lifetime_class;
                    task.info.turn_budget = None;
                }
            }
        }
        (request, lifecycle)
    };
    let result = match decision.recommendation {
        WorkReviewRecommendation::Accept { .. } => request.candidate.clone(),
        WorkReviewRecommendation::Rework { revised_objective } => WorkResult {
            outcome: WorkOutcome::Failed {
                message: revised_objective.map_or_else(
                    || format!("background review requested rework: {}", decision.rationale),
                    |objective| {
                        format!(
                            "background review requested rework ({objective}): {}",
                            decision.rationale
                        )
                    },
                ),
            },
            ..request.candidate.clone()
        },
        WorkReviewRecommendation::Inconclusive { failure } => WorkResult {
            outcome: WorkOutcome::Failed {
                message: format!(
                    "background review was inconclusive ({failure:?}): {}",
                    decision.rationale
                ),
            },
            ..request.candidate.clone()
        },
    };
    let envelope = result_envelope(&request.worker.worker_id, result);
    if let Ok(data) = serde_json::to_string(&envelope) {
        push_event(
            registry,
            &request.worker.worker_id,
            EventStream::Stdout,
            &data,
        );
    }
    let Some(lifecycle) = lifecycle else { return };
    match lifecycle {
        LifecycleRecommendation::KeepCurrent => {}
        LifecycleRecommendation::Release => {
            let _ = release_worker(registry, &request.worker.worker_id);
        }
        LifecycleRecommendation::Retain { lifetime_class } => {
            if request.worker.current_lifetime_class != lifetime_class {
                let _ = retain_worker(
                    registry,
                    &request.worker.worker_id,
                    None,
                    Some(lifetime_class),
                );
            }
        }
    }
}

// Only bounded bookkeeping lives on the event owner. Jobs retain the source
// workspace and the original subscribers/correlation even if the worker exits
// or is reassigned before publication finishes.
struct ArtifactJob {
    info: AgentInfo,
    generation: u64,
    assignment: u64,
    envelope: EventEnvelope,
    subscribers: Vec<mpsc::Sender<AgentEvent>>,
}

type ArtifactRetention = HashMap<String, (usize, Option<AgentInfo>)>;

fn artifact_retention() -> &'static Mutex<ArtifactRetention> {
    static RETENTION: OnceLock<Mutex<ArtifactRetention>> = OnceLock::new();
    RETENTION.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Drop for ArtifactJob {
    fn drop(&mut self) {
        let cleanup = {
            let mut retained = artifact_retention().lock().unwrap();
            let Some((count, _)) = retained.get_mut(&self.info.workspace) else {
                return;
            };
            *count -= 1;
            if *count == 0 {
                retained
                    .remove(&self.info.workspace)
                    .and_then(|(_, info)| info)
            } else {
                None
            }
        };
        if let Some(info) = cleanup {
            cleanup_workspace(&info);
        }
    }
}

impl ArtifactJob {
    fn emit(&self, envelope: &EventEnvelope) {
        let data = correlate_event(&serde_json::to_string(envelope).unwrap(), &self.info);
        for tx in &self.subscribers {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: data.clone(),
            });
        }
        log_event(&self.info.id, &EventStream::Stdout, &data);
    }

    fn finish(&self, mut envelope: EventEnvelope) {
        let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        envelope.event_id = sequence;
        envelope.sequence = sequence;
        envelope.session_id = format!("{}:artifact-publication", self.envelope.session_id);
        envelope.actor = tachyon_api::Actor::System;
        envelope.occurred_at_ms = unix_now_ms();
        self.emit(&envelope);
    }
}

fn artifact_publisher() -> &'static mpsc::SyncSender<ArtifactJob> {
    static PUBLISHER: OnceLock<mpsc::SyncSender<ArtifactJob>> = OnceLock::new();
    PUBLISHER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<ArtifactJob>(32);
        std::thread::spawn(move || {
            for job in rx {
                let data = serde_json::to_string(&job.envelope).unwrap();
                if let Some(published) =
                    artifact_store::publish_event(&data, &job.info, job.generation, job.assignment)
                {
                    if let Ok(envelope) = serde_json::from_str(&published) {
                        job.finish(envelope);
                    }
                }
            }
        });
        tx
    })
}

fn queue_artifact(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    mut envelope: EventEnvelope,
    bytes: usize,
    publisher: &mpsc::SyncSender<ArtifactJob>,
) {
    let StructuredAgentEvent::ArtifactRegistered { artifact } = &mut envelope.kind else {
        return;
    };
    let job = {
        let reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(id) else {
            return;
        };
        let stale = artifact.generation.is_some_and(|v| v != task.generation)
            || artifact.assignment.is_some_and(|v| v != task.assignment)
            || (task.assignment > 0
                && (artifact.generation != Some(task.generation)
                    || artifact.assignment != Some(task.assignment)));
        // An old or unfenced event must not be attributed to a reused worker's new Work.
        if stale {
            return;
        }
        artifact.task_id = task.info.logical_task_id.clone();
        artifact.work_id = task.info.logical_task_id.clone();
        artifact.generation = Some(task.generation);
        artifact.assignment = Some(task.assignment);
        artifact.publication = if bytes > 32 * 1024 {
            tachyon_api::types::ArtifactPublication::Failed {
                reason: "stale or oversized artifact event".into(),
            }
        } else {
            tachyon_api::types::ArtifactPublication::Pending
        };
        artifact_retention()
            .lock()
            .unwrap()
            .entry(task.info.workspace.clone())
            .or_default()
            .0 += 1;
        let mut subscribers = task.subs.clone();
        if let Some(work) = task
            .info
            .logical_task_id
            .as_ref()
            .and_then(|id| reg.works.get(id))
        {
            subscribers.extend(work.subs.iter().cloned());
        }
        ArtifactJob {
            info: task.info.clone(),
            generation: task.generation,
            assignment: task.assignment,
            envelope,
            subscribers,
        }
    };
    // Emit before enqueueing: the worker never waits on the event owner or
    // sends back through its bounded actor channel, and cannot overtake pending.
    job.emit(&job.envelope);
    if matches!(&job.envelope.kind, StructuredAgentEvent::ArtifactRegistered { artifact }
        if matches!(artifact.publication, tachyon_api::types::ArtifactPublication::Failed { .. }))
    {
        return;
    }
    if let Err(error) = publisher.try_send(job) {
        let job = match error {
            mpsc::TrySendError::Full(job) | mpsc::TrySendError::Disconnected(job) => job,
        };
        let mut envelope = job.envelope.clone();
        if let StructuredAgentEvent::ArtifactRegistered { artifact } = &mut envelope.kind {
            artifact.publication = tachyon_api::types::ArtifactPublication::Failed {
                reason: "artifact publication queue unavailable or full; retry with a new ID"
                    .into(),
            };
        }
        job.finish(envelope);
    }
}

fn push_event(registry: &Arc<Mutex<Registry>>, id: &str, stream: EventStream, data: &str) {
    if stream == EventStream::Stdout {
        if let Ok(envelope) = serde_json::from_str::<EventEnvelope>(data) {
            if matches!(
                envelope.kind,
                StructuredAgentEvent::ArtifactRegistered { .. }
            ) {
                queue_artifact(registry, id, envelope, data.len(), artifact_publisher());
                return;
            }
            if matches!(envelope.kind, StructuredAgentEvent::WorkCandidate { .. }) {
                let info = registry
                    .lock()
                    .unwrap()
                    .tasks
                    .get(id)
                    .map(|task| task.info.clone());
                let correlated = info
                    .as_ref()
                    .map_or_else(|| data.to_string(), |info| correlate_event(data, info));
                if let Ok(envelope) = serde_json::from_str::<EventEnvelope>(&correlated) {
                    log_event(id, &stream, &correlated);
                    handle_work_candidate(registry, id, envelope);
                }
                return;
            }
        } else if decode_structured_event(data)
            .is_some_and(|event| matches!(event, StructuredAgentEvent::WorkCandidate { .. }))
        {
            return;
        }
    }
    let mut retire: Option<(Option<u32>, Option<String>, AgentInfo)> = None;
    let mut terminal_info = None;
    let completed;
    let correlated_data;
    {
        let mut reg = registry.lock().unwrap();
        let Some(info) = reg.tasks.get(id).map(|task| task.info.clone()) else {
            return;
        };
        correlated_data = correlate_event(data, &info);
        let structured = decode_structured_event(&correlated_data);
        let legacy_completed = structured
            .as_ref()
            .is_some_and(|event| matches!(event, StructuredAgentEvent::WorkerCompleted { .. }));
        let work_result = structured.as_ref().and_then(|event| match event {
            StructuredAgentEvent::WorkResult { result } => Some(result.clone()),
            _ => None,
        });
        if let Some(result) = &work_result {
            let valid = reg.works.get(&result.work_id).is_some_and(|work| {
                work.worker_id == id
                    && work.request.generation == result.generation
                    && work.request.assignment == result.assignment
                    && work.terminal_result.is_none()
            }) && reg.tasks.get(id).is_some_and(|task| {
                task.generation == result.generation
                    && task.assignment == result.assignment
                    && task.info.logical_task_id.as_deref() == Some(result.work_id.as_str())
            });
            if !valid {
                return;
            }
        }
        completed = legacy_completed
            || work_result
                .as_ref()
                .is_some_and(|result| matches!(result.outcome, WorkOutcome::Completed { .. }));
        let terminal = legacy_completed || work_result.is_some();
        let ready = structured.as_ref().is_some_and(|event| {
            matches!(event, StructuredAgentEvent::Status { phase, message, .. } if phase == "ready" && message == "idle")
        });
        if let Some(task) = reg.tasks.get_mut(id) {
            if structured
                .as_ref()
                .is_some_and(|event| matches!(event, StructuredAgentEvent::Usage { .. }))
            {
                task.terminal_usage = Some(correlated_data.clone());
            }
            if ready && task.info.state == AgentState::Completed {
                task.ready = true;
            }
            if terminal {
                task.ready = false;
                task.info.last_activity_secs = unix_now();
                task.last_used_secs = unix_now();
                if completed {
                    task.info.state = AgentState::Completed;
                    task.info.turns_used = task.info.turns_used.saturating_add(1);
                    task.info.retained = true;
                    let budget_exhausted = task
                        .info
                        .turn_budget
                        .is_some_and(|budget| task.info.turns_used >= budget);
                    if budget_exhausted {
                        task.info.retained = false;
                        task.info.state = AgentState::Released;
                        task.generation = task.generation.wrapping_add(1);
                        let pid = task.info.pid.take();
                        task.stdin = None;
                        retire = Some((pid, task.control_socket.clone(), task.info.clone()));
                    }
                } else {
                    task.info.state = AgentState::Failed;
                    task.info.retained = false;
                    task.generation = task.generation.wrapping_add(1);
                    let pid = task.info.pid.take();
                    task.stdin = None;
                    retire = Some((pid, task.control_socket.clone(), task.info.clone()));
                }
                task.terminal_result = Some(correlated_data.clone());
                terminal_info = Some(task.info.clone());
            }
            task.subs.retain(|tx| {
                tx.send(AgentEvent {
                    stream,
                    data: correlated_data.clone(),
                })
                .is_ok()
            });
        }
        if let Some(result) = &work_result {
            let current_info = reg.tasks.get(id).map(|task| task.info.clone());
            if let Some(work) = reg.works.get_mut(&result.work_id) {
                if let Some(info) = current_info {
                    work.info = info;
                }
                work.terminal_result = Some(correlated_data.clone());
                for tx in work.subs.drain(..) {
                    let _ = tx.send(AgentEvent {
                        stream,
                        data: correlated_data.clone(),
                    });
                }
            }
        }
    }
    if let Some((pid, control_socket, info)) = retire {
        if let Some(socket) = control_socket {
            let _ = supervisor_command(&socket, "signal\tterm\n");
        } else if let Some(pid) = pid {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
        let note = if info.state == AgentState::Released {
            "Short worker released after reaching its three-turn budget."
        } else {
            "Worker terminated after an unsuccessful terminal outcome."
        };
        persist_task(registry, &info, note);
        if info.state == AgentState::Released {
            cleanup_workspace(&info);
        }
    } else if let Some(info) = terminal_info {
        persist_task(
            registry,
            &info,
            "Work assignment reached a terminal outcome.",
        );
    }
    handle_context_compaction(registry, id, &correlated_data);
    persist_interaction_history(registry, &correlated_data);
    acknowledge_reminder_notification(registry, &correlated_data);
    log_event(id, &stream, &correlated_data);
    if completed {
        start_ready_tasks(registry);
    }
}

/// Append an event to the structured JSON-lines log used by `tachyon logs`,
/// GUIs, and debugging. Best-effort; never blocks or fails the agent.
fn log_event(id: &str, stream: &EventStream, data: &str) {
    // stream serializes to a JSON string like "stdout"; embed without
    // double-encoding.
    let stream_json = serde_json::to_string(stream).unwrap_or_else(|_| "\"stdout\"".into());
    let line = format!(
        "{{\"ts\":{},\"agent\":\"{}\",\"stream\":{},\"data\":{}}}\n",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
        id,
        stream_json,
        serde_json::to_string(data).unwrap_or_default(),
    );
    let _ = event_logger().try_send(line);
}

fn event_logger() -> &'static mpsc::SyncSender<String> {
    static LOGGER: OnceLock<mpsc::SyncSender<String>> = OnceLock::new();
    LOGGER.get_or_init(|| {
        let (tx, rx) = mpsc::sync_channel::<String>(4096);
        let path = tachyon_util::daemon::logs_dir().join("events.jsonl");
        std::thread::spawn(move || {
            let Some(parent) = path.parent() else { return };
            if std::fs::create_dir_all(parent).is_err() {
                return;
            }
            let Ok(file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            else {
                return;
            };
            let mut writer = BufWriter::new(file);
            while let Ok(line) = rx.recv() {
                if writer.write_all(line.as_bytes()).is_err() {
                    return;
                }
                let _ = writer.flush();
            }
        });
        tx
    })
}

/// Mark an agent terminal and notify all subscribers with an Exit event.
fn finish_agent(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    generation: u64,
    state: AgentState,
    data: String,
) {
    let unfinished_work = {
        let reg = registry.lock().unwrap();
        reg.tasks.get(id).and_then(|task| {
            let work_id = task.info.logical_task_id.as_ref()?;
            let work = reg.works.get(work_id)?;
            (task.generation == generation
                && task.assignment == work.request.assignment
                && work.request.generation == generation
                && work.terminal_result.is_none())
            .then(|| work.request.clone())
        })
    };
    if let Some(request) = unfinished_work {
        let sequence = DAEMON_EVENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let result = WorkResult {
            work_id: request.work_id.clone(),
            candidate_refs: None,
            final_context: None,
            attempt_id: request.attempt.as_ref().map(|a| a.id.clone()),
            evidence: Default::default(),
            instruction_revision: None,
            timing: None,
            objective: request.objective.clone(),
            generation: request.generation,
            assignment: request.assignment,
            outcome: WorkOutcome::Failed {
                message: format!("worker exited without a terminal result: {data}"),
            },
        };
        let envelope = EventEnvelope {
            event_id: sequence,
            session_id: id.to_string(),
            conversation_id: None,
            turn_id: None,
            task_id: Some(request.work_id),
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::Actor::System,
            sequence,
            occurred_at_ms: unix_now_ms(),
            kind: StructuredAgentEvent::WorkResult { result },
        };
        if let Ok(encoded) = serde_json::to_string(&envelope) {
            push_event(registry, id, EventStream::Stdout, &encoded);
        }
        return;
    }
    let is_foreground = {
        let reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(id) else {
            return;
        };
        task.info.id == FOREGROUND_ID
    };
    let mut reg = registry.lock().unwrap();
    let Some(task) = reg.tasks.get_mut(id) else {
        return;
    };
    if task.generation != generation {
        return;
    }
    let state = if matches!(
        task.info.state,
        AgentState::Terminated | AgentState::Released
    ) {
        task.info.state
    } else {
        state
    };
    task.info.state = state;
    task.info.pid = None;
    let info = task.info.clone();

    // Clean up the (dead) process handle and drain pipes we still hold.
    task.process = None;

    let ev = AgentEvent {
        stream: EventStream::Exit,
        data: data.clone(),
    };
    for tx in task.subs.drain(..) {
        let _ = tx.send(ev.clone());
    }
    drop(reg);

    persist_task(registry, &info, &data);
    if state == AgentState::Completed {
        start_ready_tasks(registry);
    }

    // Wipe the agent's workspace on completion/failure so no files leak. Only
    // wipe workspace-autogenerated dirs (under workspaces/<id>), never the
    // foreground's shared dir. `TACHYON_KEEP_WORKSPACES=1` disables cleanup
    // for debugging.
    if !is_foreground {
        cleanup_workspace(&info);
    }
}

fn main() -> std::process::ExitCode {
    if let Some(code) = guard::guard_or_exit_code() {
        return std::process::ExitCode::from(code as u8);
    }

    if let Err(e) = tachyon_util::daemon::ensure_layout() {
        eprintln!("tachyond: failed to create data directories: {e}");
        return std::process::ExitCode::FAILURE;
    }

    // Enforce a single daemon instance via an exclusive flock.
    let _lock = match tachyon_util::daemon::Lock::try_acquire() {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            eprintln!("tachyond: another daemon instance is already running");
            return std::process::ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("tachyond: failed to acquire single-instance lock: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };

    if let Err(error) =
        tachyon_util::daemon::managed_agent_root(&tachyon_util::config::Config::load())
            .and_then(|root| tachyon_util::daemon::ensure_managed_agent_root(&root))
    {
        eprintln!("tachyond: managed root unavailable: {error}; continuing without it; managed worker requests will retry provisioning");
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))
    {
        eprintln!("tachyond: failed to register SIGTERM handler: {e}");
    }
    if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))
    {
        eprintln!("tachyond: failed to register SIGINT handler: {e}");
    }

    let runtime_store = match RuntimeStore::open(&tachyon_util::daemon::runtime_database_path()) {
        Ok(store) => Arc::new(store),
        Err(error) => {
            eprintln!("tachyond: runtime store unavailable: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    if let Err(error) = artifact_store::initialize_retained(runtime_store.retained.clone()) {
        eprintln!("tachyond: retained artifact storage unavailable: {error}");
        return std::process::ExitCode::FAILURE;
    }
    let history_store = match HistoryStore::open(&tachyon_util::daemon::history_database_path()) {
        Ok(store) => Some(Arc::new(store)),
        Err(error) => {
            eprintln!("tachyond: history store unavailable; projections will queue: {error}");
            None
        }
    };
    let memory_store = match MemoryStore::open(tachyon_util::daemon::memories_database_path()) {
        Ok(store) => Arc::new(store),
        Err(error) => {
            eprintln!("tachyond: memories store unavailable: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let pid = std::process::id();
    if let Err(e) = tachyon_util::daemon::write_pid(pid) {
        eprintln!("tachyond: failed to write pid file: {e}");
        return std::process::ExitCode::FAILURE;
    }

    let campaigns = match runtime_store::campaign_launch::CampaignService::new(
        runtime_store.clone(),
        tachyon_util::daemon::data_dir(),
    ) {
        Ok(service) => Arc::new(service),
        Err(error) => {
            eprintln!("tachyond: campaign recovery: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let reg = Arc::new(Mutex::new(Registry {
        service_shutdown: shutdown.clone(),
        campaigns: Some(campaigns.clone()),
        runtime_store: Some(runtime_store.clone()),
        history_store,
        memory_store: Some(memory_store),
        ..Registry::default()
    }));
    let (coordinator_tx, coordinator_rx) = mpsc::sync_channel(64);
    reg.lock().unwrap().coordinator_tx = Some(coordinator_tx);
    let background_registry = Arc::clone(&reg);
    let background_shutdown = Arc::clone(&shutdown);
    let background = std::thread::spawn(move || {
        supervise_background(background_registry, coordinator_rx, background_shutdown)
    });
    if let Err(error) = project_pending_history(&reg) {
        eprintln!("tachyond: replay history outbox: {error}");
    }
    if let Err(error) = restore_runtime_tasks(&reg) {
        eprintln!("tachyond: runtime restore failed: {error}");
        tachyon_util::daemon::clear_pid();
        return std::process::ExitCode::FAILURE;
    }
    start_ready_tasks(&reg);

    let (monitor, monitor_thread) =
        monitor::Monitor::start(Arc::downgrade(&reg), runtime_store, shutdown.clone());
    reg.lock().unwrap().monitor = Some(monitor.clone());

    // Spawn the foreground runtime.
    {
        let foreground_started = unix_now();
        match spawn_foreground() {
            Ok((id, child, stdin)) => {
                // The foreground isn't a normal agent list entry; it's the
                // conversation host. Still stored in tasks so subscriptions
                // work uniformly.
                let info = AgentInfo {
                    id: id.clone(),
                    task: "(foreground)".into(),
                    state: AgentState::Running,
                    pid: Some(child.id()),
                    workspace: ".".into(),
                    created_secs: foreground_started,
                    retained: false,
                    lease_until_secs: None,
                    session_id: id.clone(),
                    lifetime_class: Default::default(),
                    purpose: "foreground".into(),
                    owner: "daemon".into(),
                    last_activity_secs: foreground_started,
                    checkpoint_available: false,
                    turns_used: 0,
                    turn_budget: None,
                    task_type: "foreground".into(),
                    description: "conversation coordinator".into(),
                    persistent: false,
                    sandboxed: false,
                    stage_until_secs: None,
                    logical_task_id: None,
                    origin_turn_id: None,
                    parent_task_id: None,
                    tool_call_id: None,
                };
                let recovered: Vec<String> = {
                    let mut guard = reg.lock().unwrap();
                    guard.foreground_id = Some(id.clone());
                    guard.tasks.insert(
                        id.clone(),
                        Task {
                            info,
                            depends_on: Vec::new(),
                            process: Some(child),
                            stdin: Some(Arc::new(Mutex::new(stdin))),
                            subs: Vec::new(),
                            generation: 0,
                            assignment: 0,
                            warm: false,
                            ready: false,
                            owner: None,
                            last_used_secs: 0,
                            control_socket: None,
                            terminal_usage: None,
                            terminal_result: None,
                        },
                    );
                    guard
                        .tasks
                        .values()
                        .filter(|task| task.info.persistent && task.info.id != id)
                        .map(|task| {
                            format!(
                                "[daemon:recovered] session={} type={} task={} state={}",
                                task.info.session_id,
                                task.info.task_type,
                                task.info.description,
                                task.info.state
                            )
                        })
                        .collect()
                };
                for message in recovered {
                    if let Ok(input) = task_input(&reg, &id) {
                        let _ = write_task_input(input, &id, &message);
                    }
                }
                let reg2 = Arc::clone(&reg);
                let id2 = id.clone();
                std::thread::spawn(move || pump_agent(&reg2, &id2, 0));
                eprintln!("tachyond: foreground running ({id})");
            }
            Err(e) => eprintln!("tachyond: failed to start foreground: {e}"),
        }
    }

    let reminder_registry = Arc::clone(&reg);
    let reminder_shutdown = Arc::clone(&shutdown);
    std::thread::spawn(move || run_reminder_scheduler(reminder_registry, reminder_shutdown));

    let server_reg = Arc::clone(&reg);
    let server_shutdown = Arc::clone(&shutdown);
    let server = std::thread::spawn(move || {
        if let Err(e) = run_ipc_server(server_reg, server_shutdown) {
            eprintln!("tachyond: ipc server: {e}");
        }
    });

    let reaper_reg = Arc::clone(&reg);
    let reaper_shutdown = Arc::clone(&shutdown);
    std::thread::spawn(move || {
        while !reaper_shutdown.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_secs(15));
            reap_warm_workers(&reaper_reg);
        }
    });

    println!("tachyond: running (pid {pid})");

    while !shutdown.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    monitor.stop();
    let _ = monitor_thread.join();
    shutdown_tasks(&reg);
    campaigns.shutdown();
    let _ = background.join();
    tachyon_util::daemon::clear_pid();
    let _ = std::fs::remove_file(tachyon_util::daemon::socket_path());
    println!("tachyond: stopped");
    let _ = server.join();
    std::process::ExitCode::SUCCESS
}

fn shutdown_tasks(registry: &Arc<Mutex<Registry>>) {
    let mut tasks = Vec::new();
    {
        let mut reg = registry.lock().unwrap();
        for task in reg.tasks.values_mut() {
            tasks.push((task.info.lifetime_class, task.info.pid, task.process.take()));
        }
    }
    for (lifetime_class, pid, mut child) in tasks {
        // Persistent supervisors are intentionally orphaned. Their socket, PID,
        // workspace, and checkpoint let the next daemon instance reattach.
        if survives_daemon_shutdown(lifetime_class) {
            continue;
        }
        if let Some(process) = child.as_mut() {
            let _ = process.kill();
        } else if let Some(pid) = pid {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
        if let Some(process) = child.as_mut() {
            let _ = process.wait();
        }
    }
}

fn survives_daemon_shutdown(lifetime_class: LifetimeClass) -> bool {
    lifetime_class == LifetimeClass::Persistent
}

/// Run the Unix socket server until shutdown is requested.
fn run_ipc_server(
    registry: Arc<Mutex<Registry>>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let socket = tachyon_util::daemon::socket_path();
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    listener.set_nonblocking(true)?;

    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, registry) {
                        eprintln!("tachyond: connection: {e}");
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Handle a client connection: serve multiple requests until the client
/// closes. `AgentSubscribe` / `ForegroundSubscribe` change the connection to
/// a streaming mode and take it over until the stream ends.
fn handle_connection(stream: UnixStream, registry: Arc<Mutex<Registry>>) -> std::io::Result<()> {
    #[cfg(not(target_os = "linux"))]
    let uid = nix::unistd::geteuid().as_raw();
    #[cfg(target_os = "linux")]
    let uid =
        nix::sys::socket::getsockopt(&stream, nix::sys::socket::sockopt::PeerCredentials)?.uid();
    if uid != nix::unistd::geteuid().as_raw() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "daemon IPC requires the same user",
        ));
    }
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        let req = match read_request(&mut reader) {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(_) => return write_response(&mut writer, &ApiResponse::error("bad request")),
        };

        if let ApiRequest::MonitorGet { query } | ApiRequest::MonitorSubscribe { query, .. } = &req
        {
            let (monitor, shutdown) = {
                let registry = registry.lock().unwrap();
                (registry.monitor.clone(), registry.service_shutdown.clone())
            };
            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
            let subscribe = matches!(req, ApiRequest::MonitorSubscribe { .. });
            monitor::serve(&mut writer, monitor, query.clone(), subscribe)?;
            if subscribe || shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
            continue;
        }
        if let ApiRequest::AgentSubscribe { id } = &req {
            return stream_agent(&mut writer, id, registry);
        }
        if let ApiRequest::WorkSubscribe { work_id } = &req {
            return stream_work(&mut writer, work_id, registry);
        }
        if let ApiRequest::ForegroundSubscribe = &req {
            return stream_agent(&mut writer, FOREGROUND_ID, registry);
        }

        if matches!(
            req,
            ApiRequest::Todo(_)
                | ApiRequest::TodoSnapshot { .. }
                | ApiRequest::OperationalSubscribe { .. }
        ) {
            let (store, shutdown) = {
                let registry = registry.lock().unwrap();
                (
                    registry.runtime_store.clone(),
                    registry.service_shutdown.clone(),
                )
            };
            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(store) = store else {
                write_service_response(
                    &mut writer,
                    &ApiResponse::TodoError {
                        error: tachyon_api::todo::TodoError::Storage {
                            message: "runtime store unavailable".into(),
                        },
                    },
                    &shutdown,
                )?;
                continue;
            };
            let (scope, request) = match &req {
                ApiRequest::Todo(request) => (request.scope().clone(), Some(request.clone())),
                ApiRequest::TodoSnapshot {
                    scope,
                    limit,
                    cursor,
                } => (
                    scope.clone(),
                    Some(tachyon_api::todo::TodoRequest::List {
                        scope: scope.clone(),
                        filter: Default::default(),
                        limit: *limit,
                        cursor: cursor.clone(),
                    }),
                ),
                ApiRequest::OperationalSubscribe { scope, .. } => (scope.clone(), None),
                _ => unreachable!(),
            };
            let facade = store.todos(runtime_store::todo::TodoAuthority::Bound {
                scope,
                actor: tachyon_api::todo::TodoActor {
                    source: "operator".into(),
                    actor: format!("uid:{uid}"),
                },
            });
            let facade = match facade {
                Ok(facade) => facade,
                Err(error) => {
                    write_service_response(
                        &mut writer,
                        &ApiResponse::TodoError { error },
                        &shutdown,
                    )?;
                    continue;
                }
            };
            if let Some(request) = request {
                let response = match facade.execute(request) {
                    Ok(response) => ApiResponse::Todo { response },
                    Err(error) => ApiResponse::TodoError { error },
                };
                write_service_response(&mut writer, &response, &shutdown)?;
                continue;
            }
            if let ApiRequest::OperationalSubscribe { mut after, .. } = req {
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    match facade.operational_batch(&after) {
                        Ok(batch) => {
                            let idle = batch.watermark == after;
                            after = batch.watermark.clone();
                            write_service_response(
                                &mut writer,
                                &ApiResponse::OperationalBatch { batch },
                                &shutdown,
                            )?;
                            if idle {
                                std::thread::sleep(std::time::Duration::from_millis(250));
                            }
                        }
                        Err(error) => {
                            return write_service_response(
                                &mut writer,
                                &ApiResponse::TodoError { error },
                                &shutdown,
                            )
                        }
                    }
                }
            }
        }

        let resp = dispatch(&req, &registry);
        if write_response(&mut writer, &resp).is_err() {
            return Ok(());
        }
    }
}

fn read_request<R: BufRead>(reader: &mut R) -> std::io::Result<ApiRequest> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "closed",
        ));
    }
    serde_json::from_str(line.trim()).map_err(std::io::Error::other)
}

fn write_response<W: Write>(writer: &mut W, resp: &ApiResponse) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(resp).map_err(std::io::Error::other)?;
    buf.push(b'\n');
    writer.write_all(&buf)?;
    writer.flush()
}

/// Only the new services use cancellable writes; transcript transports are unchanged.
fn write_service_response(
    writer: &mut UnixStream,
    resp: &ApiResponse,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    use std::io::ErrorKind;
    use std::time::{Duration, Instant};
    let mut bytes = serde_json::to_vec(resp).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    let previous = writer.write_timeout()?;
    let deadline = Instant::now() + Duration::from_secs(1);
    let result = (|| {
        let mut remaining = bytes.as_slice();
        while !remaining.is_empty() {
            if shutdown.load(Ordering::Acquire) {
                return Err(std::io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "service stopped",
                ));
            }
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                return Err(std::io::Error::new(
                    ErrorKind::TimedOut,
                    "service write deadline",
                ));
            }
            writer.set_write_timeout(Some(wait.min(Duration::from_millis(250))))?;
            match writer.write(remaining) {
                Ok(0) => return Err(std::io::Error::from(ErrorKind::WriteZero)),
                Ok(n) => remaining = &remaining[n..],
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    })();
    let restored = writer.set_write_timeout(previous);
    result.and(restored)
}

/// Dispatch a request to the registry.
fn dispatch(req: &ApiRequest, registry: &Arc<Mutex<Registry>>) -> ApiResponse {
    use ApiRequest::*;
    match req {
        MonitorGet { .. } | MonitorSubscribe { .. } => {
            ApiResponse::error("monitor API requires authenticated operator connection")
        }
        Todo(_) | TodoSnapshot { .. } | OperationalSubscribe { .. } => {
            ApiResponse::error("todo API requires authenticated connection metadata")
        }
        CampaignIntegrationSnapshot { .. } | CampaignIntegrate { .. } => {
            let store = registry.lock().unwrap().runtime_store.clone();
            store
                .ok_or_else(|| "runtime store unavailable".to_string())
                .and_then(|s| s.integration_request(req, &tachyon_util::daemon::data_dir()))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignReconcile {
            id,
            receipt,
            unisolated_development,
            confirm_authoritative,
        } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| {
                    s.reconcile(id, receipt, *unisolated_development, *confirm_authoritative)
                })
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignInspect { id } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.inspect(id))
                .unwrap_or_else(ApiResponse::error)
        }
        LocalRetentionSet { id, archived } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.retention(id, *archived))
                .unwrap_or_else(ApiResponse::error)
        }
        LocalRetentionGet { id } => {
            let store = registry.lock().unwrap().runtime_store.clone();
            store
                .ok_or_else(|| "runtime store unavailable".to_string())
                .and_then(|s| s.retention_get(id))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignRecover {
            id,
            unisolated_development,
        } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.recover(id, *unisolated_development))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignAttentionList { .. } | CampaignAttentionAnswer { .. } => {
            let store = registry.lock().unwrap().runtime_store.clone();
            store
                .ok_or_else(|| "runtime store unavailable".to_string())
                .and_then(|s| s.attention_request(req))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignAcceptanceGet(_) | CampaignAcceptanceDecide(_) => {
            let store = registry.lock().unwrap().runtime_store.clone();
            store
                .ok_or_else(|| "runtime store unavailable".to_string())
                .and_then(|s| s.acceptance_request(req))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignProgress { id } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.progress(id))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignRun {
            manifest,
            unisolated_development,
        } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.run(manifest, *unisolated_development))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignCancel { id } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.cancel(id))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignResume {
            id,
            unisolated_development,
        } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.resume(id, *unisolated_development))
                .unwrap_or_else(ApiResponse::error)
        }
        CampaignContinue {
            id,
            request,
            unisolated_development,
        } => {
            let service = registry.lock().unwrap().campaigns.clone();
            service
                .ok_or_else(|| "campaign service unavailable".to_string())
                .and_then(|s| s.continue_work(id, request, *unisolated_development))
                .unwrap_or_else(ApiResponse::error)
        }
        ArtifactGet { .. } | ArtifactRead { .. } | ArtifactList { .. } => {
            artifact_store::query(req).unwrap_or_else(ApiResponse::error)
        }
        ResearchCreate { .. }
        | ResearchGet { .. }
        | ResearchList { .. }
        | CampaignCreate { .. }
        | CampaignGet { .. }
        | CampaignList { .. } => {
            // IPC runs on a connection thread. Never wait for redb's writer
            // while holding the registry lock or routing through an actor.
            let store = registry.lock().unwrap().runtime_store.clone();
            match store {
                Some(store) => store
                    .research_request(req)
                    .unwrap_or_else(ApiResponse::error),
                None => ApiResponse::error("runtime store unavailable"),
            }
        }
        DaemonStatus => {
            let background = {
                let reg = registry.lock().unwrap();
                BackgroundCoordinatorInfo {
                    online: reg.background_online,
                    generation: reg.background_generation,
                    pending_reviews: reg
                        .works
                        .values()
                        .filter_map(|work| {
                            let review = work.review.as_ref()?;
                            Some(PendingWorkReviewInfo {
                                work_id: review.request.candidate.work_id.clone(),
                                worker_id: review.request.worker.worker_id.clone(),
                                deadline_ms: review.request.deadline_ms,
                            })
                        })
                        .collect(),
                }
            };
            ApiResponse::DaemonStatus {
                info: DaemonInfo {
                    pid: std::process::id(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    proto_version: PROTO_VERSION.into(),
                    provider_ready: tachyon_util::config::Config::load().provider_ready(),
                    socket: tachyon_util::daemon::socket_path().display().to_string(),
                    background,
                },
            }
        }
        HistoryQuery {
            since_ms,
            until_ms,
            limit,
        } => {
            if since_ms >= until_ms {
                return ApiResponse::error("history range must have since_ms < until_ms");
            }
            let history = registry.lock().unwrap().history_store.clone();
            let Some(history) = history else {
                return ApiResponse::error("history store unavailable");
            };
            match history.activity_between(*since_ms, *until_ms, (*limit).clamp(1, 1000) as usize) {
                Ok(entries) => ApiResponse::History { entries },
                Err(error) => ApiResponse::error(format!("history query failed: {error}")),
            }
        }
        MemoryRecall {
            query,
            conversation_id,
            turn,
            include_history,
            max_items,
            max_chars,
        } => {
            let item_limit = (*max_items).clamp(1, 32) as usize;
            let char_limit = (*max_chars).clamp(256, 12_000) as usize;
            let (memory, history, runtime) = {
                let registry = registry.lock().unwrap();
                (
                    registry.memory_store.clone(),
                    registry.history_store.clone(),
                    registry.runtime_store.clone(),
                )
            };
            let Some(memory) = memory else {
                return ApiResponse::error("memory store unavailable");
            };
            let now_ms = unix_now_ms();
            let preference_limit = item_limit.div_ceil(2).min(6);
            let preferences = match memory.recall_preferences(query, now_ms, preference_limit) {
                Ok(records) => records,
                Err(error) => {
                    return ApiResponse::error(format!("memory recall failed: {error}"));
                }
            };
            let recall_history = *include_history;
            let task_entries = if recall_history {
                let Some(runtime) = runtime else {
                    return ApiResponse::error("runtime store unavailable");
                };
                match recall_task_history(&runtime, query, item_limit + 1) {
                    Ok(entries) => entries,
                    Err(error) => {
                        return ApiResponse::error(format!("task history recall failed: {error}"));
                    }
                }
            } else {
                Vec::new()
            };
            let history_entries = if recall_history {
                let Some(history) = history else {
                    return ApiResponse::error("history store unavailable");
                };
                let (start_ms, end_ms) = history_recall_window(query, now_ms);
                match history.recall_between(start_ms, end_ms, query, item_limit + 1) {
                    Ok(entries) => entries,
                    Err(error) => {
                        return ApiResponse::error(format!("history recall failed: {error}"));
                    }
                }
            } else {
                Vec::new()
            };

            let mut candidates = preferences
                .into_iter()
                .map(|record| MemoryRecallItem {
                    kind: MemoryRecallKind::Preference,
                    memory_id: Some(record.id),
                    descriptor: Some(tachyon_api::types::MemoryDescriptor {
                        kind: record.kind,
                        namespace: record.namespace,
                        relation: record.predicate,
                        scope: record.scope,
                        cardinality: record.cardinality,
                        topics: record.topics,
                    }),
                    text: record.value,
                    occurred_at_ms: record.updated_at_ms,
                })
                .collect::<Vec<_>>();
            let history_entries = history_entries
                .into_iter()
                .map(|entry| MemoryRecallItem {
                    kind: MemoryRecallKind::History,
                    memory_id: None,
                    descriptor: None,
                    text: format!("{:?}: {}", entry.role, entry.text),
                    occurred_at_ms: entry.occurred_at_ms,
                })
                .collect::<Vec<_>>();
            let historical_candidates = task_entries.len().max(history_entries.len());
            for index in 0..historical_candidates {
                if let Some(item) = task_entries.get(index) {
                    candidates.push(item.clone());
                }
                if let Some(item) = history_entries.get(index) {
                    candidates.push(item.clone());
                }
            }

            let mut items = Vec::new();
            let mut used_chars = 0;
            let mut truncated = candidates.len() > item_limit;
            for mut item in candidates {
                if items.len() >= item_limit || used_chars >= char_limit {
                    truncated = true;
                    break;
                }
                let available = char_limit - used_chars;
                let item_chars = item.text.chars().count();
                if item_chars > available {
                    item.text = item.text.chars().take(available).collect();
                    truncated = true;
                }
                used_chars += item.text.chars().count();
                items.push(item);
            }
            let actual_preferences = items
                .iter()
                .filter(|item| item.kind == MemoryRecallKind::Preference)
                .count() as u32;
            let actual_history = items
                .iter()
                .filter(|item| item.kind == MemoryRecallKind::History)
                .chain(
                    items
                        .iter()
                        .filter(|item| item.kind == MemoryRecallKind::TaskHistory),
                )
                .count() as u32;
            if !items.is_empty() {
                emit_memory_event(
                    registry,
                    conversation_id.clone(),
                    Some(turn.to_string()),
                    StructuredAgentEvent::MemoryRecalled {
                        turn: Some(*turn),
                        preference_count: actual_preferences,
                        history_count: actual_history,
                    },
                );
            }
            ApiResponse::MemoryRecall { items, truncated }
        }
        MemoryMutate {
            intent,
            source_event_id,
            conversation_id,
            turn,
            occurred_at_ms,
        } => {
            let memory = registry.lock().unwrap().memory_store.clone();
            let Some(memory) = memory else {
                return ApiResponse::MemoryMutation {
                    result: MemoryMutationResult::Unavailable,
                };
            };
            let result = match memory.apply_intent(
                intent,
                MemoryMutationSource {
                    event_id: source_event_id,
                    conversation_id,
                    turn_id: *turn,
                    occurred_at_ms: *occurred_at_ms,
                },
            ) {
                Ok(result) => result,
                Err(error) => MemoryMutationResult::Rejected {
                    reason: error.to_string(),
                },
            };
            if !matches!(result, MemoryMutationResult::Ignored) {
                emit_memory_event(
                    registry,
                    conversation_id.clone(),
                    Some(turn.to_string()),
                    StructuredAgentEvent::MemoryMutation {
                        turn: Some(*turn),
                        result: result.clone(),
                    },
                );
            }
            ApiResponse::MemoryMutation { result }
        }
        ReminderCreate {
            source_event_id,
            conversation_id,
            turn,
            text,
            delay_seconds,
            local_time,
            day,
            created_at_ms,
        } => {
            let text = text.trim();
            if text.is_empty() || text.chars().count() > 500 {
                return ApiResponse::error("reminder text must contain 1 to 500 characters");
            }
            let now_ms = unix_now_ms();
            let due_at_ms =
                match schedule_due_at_ms(now_ms, *delay_seconds, local_time.as_deref(), *day) {
                    Ok(due_at_ms) => due_at_ms,
                    Err(error) => return ApiResponse::error(error),
                };
            let store = registry.lock().unwrap().runtime_store.clone();
            let Some(store) = store else {
                return ApiResponse::error("runtime store unavailable");
            };
            let action = BackgroundScheduleAction::Create {
                source_event_id: source_event_id.clone(),
                conversation_id: conversation_id.clone(),
                turn: *turn,
                text: text.to_string(),
                delay_seconds: *delay_seconds,
                local_time: local_time.clone(),
                day: *day,
                created_at_ms: *created_at_ms,
            };
            if let Err(error) = coordinate_schedule(registry, action) {
                return ApiResponse::error(format!("schedule coordination failed: {error}"));
            }
            let id = format!("reminder-{created_at_ms}-{turn}");
            match store.create_reminder(
                &id,
                source_event_id,
                conversation_id,
                *turn,
                text,
                now_ms,
                due_at_ms,
            ) {
                Ok(reminder) => {
                    emit_schedule_event(
                        registry,
                        conversation_id.clone(),
                        *turn,
                        StructuredAgentEvent::ReminderScheduled {
                            turn: Some(*turn),
                            reminder_id: reminder.id.clone(),
                            due_at_ms: reminder.due_at_ms,
                        },
                    );
                    ApiResponse::Reminder { reminder }
                }
                Err(error) => ApiResponse::error(format!("create reminder failed: {error}")),
            }
        }
        ReminderList => {
            if let Err(error) = coordinate_schedule(registry, BackgroundScheduleAction::List) {
                return ApiResponse::error(format!("schedule coordination failed: {error}"));
            }
            let store = registry.lock().unwrap().runtime_store.clone();
            let Some(store) = store else {
                return ApiResponse::error("runtime store unavailable");
            };
            match store.active_reminders() {
                Ok(reminders) => ApiResponse::Reminders { reminders },
                Err(error) => ApiResponse::error(format!("list reminders failed: {error}")),
            }
        }
        ReminderCancel {
            id,
            conversation_id,
            turn,
        } => {
            if let Err(error) = coordinate_schedule(
                registry,
                BackgroundScheduleAction::Cancel { id: id.clone() },
            ) {
                return ApiResponse::error(format!("schedule coordination failed: {error}"));
            }
            let store = registry.lock().unwrap().runtime_store.clone();
            let Some(store) = store else {
                return ApiResponse::error("runtime store unavailable");
            };
            match store.cancel_reminder(id) {
                Ok(reminder) => {
                    emit_schedule_event(
                        registry,
                        conversation_id.clone(),
                        *turn,
                        StructuredAgentEvent::ReminderCancelled {
                            turn: Some(*turn),
                            reminder_id: reminder.id.clone(),
                        },
                    );
                    ApiResponse::Reminder { reminder }
                }
                Err(error) => ApiResponse::error(format!("cancel reminder failed: {error}")),
            }
        }
        ScheduledTaskCreate {
            source_event_id,
            conversation_id,
            turn,
            objective,
            mode,
            delay_seconds,
            local_time,
            day,
            created_at_ms,
        } => {
            let objective = objective.trim();
            if objective.is_empty() || objective.chars().count() > 4_000 {
                return ApiResponse::error("task objective must contain 1 to 4000 characters");
            }
            let now_ms = unix_now_ms();
            let due_at_ms =
                match schedule_due_at_ms(now_ms, *delay_seconds, local_time.as_deref(), *day) {
                    Ok(due_at_ms) => due_at_ms,
                    Err(error) => return ApiResponse::error(error),
                };
            let action = BackgroundScheduleAction::CreateTask {
                source_event_id: source_event_id.clone(),
                conversation_id: conversation_id.clone(),
                turn: *turn,
                objective: objective.to_string(),
                mode: *mode,
                delay_seconds: *delay_seconds,
                local_time: local_time.clone(),
                day: *day,
                created_at_ms: *created_at_ms,
            };
            if let Err(error) = coordinate_schedule(registry, action) {
                return ApiResponse::error(format!("schedule coordination failed: {error}"));
            }
            let store = registry.lock().unwrap().runtime_store.clone();
            let Some(store) = store else {
                return ApiResponse::error("runtime store unavailable");
            };
            let id = format!("scheduled-task-{created_at_ms}-{turn}");
            match store.create_scheduled_task(
                &id,
                source_event_id,
                conversation_id,
                *turn,
                objective,
                *mode,
                now_ms,
                due_at_ms,
            ) {
                Ok(schedule) => {
                    emit_schedule_event(
                        registry,
                        conversation_id.clone(),
                        *turn,
                        StructuredAgentEvent::ScheduledTaskCreated {
                            turn: Some(*turn),
                            schedule_id: schedule.id.clone(),
                            due_at_ms: schedule.due_at_ms,
                            mode: schedule.mode,
                        },
                    );
                    ApiResponse::ScheduledTask { schedule }
                }
                Err(error) => ApiResponse::error(format!("create scheduled task failed: {error}")),
            }
        }
        ScheduledTaskList => {
            let store = registry.lock().unwrap().runtime_store.clone();
            let Some(store) = store else {
                return ApiResponse::error("runtime store unavailable");
            };
            match store.scheduled_tasks() {
                Ok(schedules) => ApiResponse::ScheduledTasks { schedules },
                Err(error) => ApiResponse::error(format!("list scheduled tasks failed: {error}")),
            }
        }
        AgentStart {
            task,
            cwd,
            depends_on,
            lifetime_class,
            purpose,
            logical_task_id,
            origin_turn_id,
            parent_task_id,
            tool_call_id,
            deadline_ms,
        }
        | BackgroundDelegate {
            task,
            cwd,
            depends_on,
            lifetime_class,
            purpose,
            logical_task_id,
            origin_turn_id,
            parent_task_id,
            tool_call_id,
            deadline_ms,
        } => {
            let delegated_by_background = matches!(req, BackgroundDelegate { .. });
            let cwd = match cwd.as_deref().filter(|path| !path.is_empty()) {
                Some(path) => match tachyon_util::daemon::selected_workspace(path) {
                    Ok(path) => Some(path.to_string_lossy().into_owned()),
                    Err(error) => return ApiResponse::error(error),
                },
                None => None,
            };
            let work_identity = if delegated_by_background {
                let Some(work_id) = logical_task_id.clone() else {
                    return ApiResponse::error(
                        "background delegation requires a stable logical_task_id",
                    );
                };
                Some((
                    work_id,
                    work_fingerprint(
                        task,
                        &cwd,
                        depends_on,
                        *lifetime_class,
                        purpose,
                        origin_turn_id,
                        parent_task_id,
                        tool_call_id,
                        deadline_ms,
                    ),
                ))
            } else {
                None
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let mut reg = registry.lock().unwrap();
            if let Some((work_id, fingerprint)) = &work_identity {
                match existing_work(&reg, work_id, fingerprint) {
                    Ok(Some(info)) => return ApiResponse::Agent { info },
                    Ok(None) => {}
                    Err(error) => return ApiResponse::error(error),
                }
            }
            if delegated_by_background {
                if let Some(info) = claim_reusable_worker(
                    &mut reg,
                    task,
                    purpose,
                    *lifetime_class,
                    cwd.as_ref(),
                    depends_on,
                    logical_task_id,
                    origin_turn_id,
                    parent_task_id,
                    tool_call_id,
                ) {
                    let id = info.id.clone();
                    let worker = &reg.tasks[&id];
                    let assignment = worker.assignment;
                    let generation = worker.generation;
                    let (work_id, fingerprint) = work_identity.clone().expect("delegated work id");
                    let request = WorkRequest {
                        context_refs: vec![],
                        constraints: None,
                        attempt: None,
                        work_id: work_id.clone(),
                        objective: task.clone(),
                        generation,
                        assignment,
                        deadline_ms: deadline_ms.unwrap_or_else(|| {
                            unix_now_ms().saturating_add(worker_result_timeout().as_millis() as u64)
                        }),
                        lifetime_class: *lifetime_class,
                    };
                    reg.works.insert(
                        work_id.clone(),
                        WorkRecord {
                            request: request.clone(),
                            fingerprint,
                            worker_id: id.clone(),
                            info: info.clone(),
                            review: None,
                            terminal_result: None,
                            subs: Vec::new(),
                        },
                    );
                    drop(reg);
                    if let Err(error) = deliver_work(registry, &id, &request, info.persistent) {
                        if let Some(worker) = registry.lock().unwrap().tasks.get_mut(&id) {
                            if worker.assignment == assignment
                                && worker.info.state == AgentState::Running
                            {
                                worker.info.state = AgentState::Failed;
                                worker.ready = false;
                            }
                        }
                        finish_agent(
                            registry,
                            &id,
                            generation,
                            AgentState::Failed,
                            format!("work delivery failed: {error}"),
                        );
                        return ApiResponse::error(error);
                    }
                    collect_worker_result(Arc::clone(registry), work_id, FOREGROUND_ID.into());
                    persist_task(
                        registry,
                        &info,
                        "Retained worker reused for a new assignment.",
                    );
                    eprintln!("tachyond: agent {id} reused: {task}");
                    return ApiResponse::Agent { info };
                }
            }
            let id = reg.next_id();
            let workspace = match cwd {
                Some(c) => c,
                None => match tachyon_util::daemon::managed_agent_root(
                    &tachyon_util::config::Config::load(),
                )
                .and_then(|root| tachyon_util::daemon::provision_managed_workspace(&root, &id))
                {
                    Ok(path) => path.to_string_lossy().into_owned(),
                    Err(error) => return ApiResponse::error(error),
                },
            };
            let warm = true;
            let state = if dependencies_satisfied(&reg, depends_on) {
                AgentState::Starting
            } else {
                AgentState::Waiting
            };
            let control_socket = (*lifetime_class == LifetimeClass::Persistent)
                .then(|| format!("{workspace}/.tachyon/agent.sock"));
            let mut info = AgentInfo {
                id: id.clone(),
                task: task.clone(),
                state,
                pid: None,
                workspace: workspace.clone(),
                created_secs: now,
                retained: warm,
                lease_until_secs: None,
                session_id: id.clone(),
                lifetime_class: *lifetime_class,
                purpose: if purpose.is_empty() {
                    task.clone()
                } else {
                    purpose.clone()
                },
                owner: if warm {
                    if delegated_by_background {
                        "background".into()
                    } else {
                        "foreground".into()
                    }
                } else {
                    "user".into()
                },
                last_activity_secs: now,
                checkpoint_available: false,
                turns_used: 0,
                turn_budget: (*lifetime_class == LifetimeClass::Short).then_some(3),
                task_type: if purpose.is_empty() {
                    "general".into()
                } else {
                    normalized_task_type(purpose)
                },
                description: task.clone(),
                persistent: *lifetime_class == LifetimeClass::Persistent,
                sandboxed: false,
                stage_until_secs: None,
                logical_task_id: logical_task_id.clone(),
                origin_turn_id: origin_turn_id.clone(),
                parent_task_id: parent_task_id.clone(),
                tool_call_id: tool_call_id.clone(),
            };
            reg.tasks.insert(
                id.clone(),
                Task {
                    info: info.clone(),
                    depends_on: depends_on.clone(),
                    process: None,
                    stdin: None,
                    subs: Vec::new(),
                    generation: 0,
                    assignment: 0,
                    warm,
                    ready: false,
                    owner: None,
                    last_used_secs: 0,
                    control_socket,
                    terminal_usage: None,
                    terminal_result: None,
                },
            );
            let work_request = work_identity.clone().map(|(work_id, fingerprint)| {
                let request = WorkRequest {
                    context_refs: vec![],
                    constraints: None,
                    attempt: None,
                    work_id: work_id.clone(),
                    objective: task.clone(),
                    generation: 0,
                    assignment: 0,
                    deadline_ms: deadline_ms.unwrap_or_else(|| {
                        unix_now_ms().saturating_add(worker_result_timeout().as_millis() as u64)
                    }),
                    lifetime_class: *lifetime_class,
                };
                reg.works.insert(
                    work_id,
                    WorkRecord {
                        request: request.clone(),
                        fingerprint,
                        worker_id: id.clone(),
                        info: info.clone(),
                        review: None,
                        terminal_result: None,
                        subs: Vec::new(),
                    },
                );
                request
            });
            drop(reg);
            if state == AgentState::Waiting {
                persist_task(registry, &info, "Agent waiting for dependencies.");
                return ApiResponse::Agent { info };
            }

            let spawned = std::fs::create_dir_all(&workspace).and_then(|_| {
                if *lifetime_class == LifetimeClass::Persistent {
                    spawn_supervised_ghost(&id, &workspace)
                } else {
                    spawn_warm_ghost(&id, &workspace)
                }
            });

            match spawned {
                Ok(mut spawned_child) => {
                    let stdin = spawned_child
                        .stdin
                        .take()
                        .map(|stdin| Arc::new(Mutex::new(stdin)));
                    let mut child = Some(spawned_child);
                    let installed = {
                        let mut reg = registry.lock().unwrap();
                        reg.tasks.get_mut(&id).is_some_and(|worker| {
                            if worker.info.state != AgentState::Starting
                                || worker.generation != 0
                                || worker.assignment != 0
                            {
                                return false;
                            }
                            worker.info.pid = child.as_ref().map(Child::id);
                            worker.info.state = AgentState::Running;
                            worker.process = child.take();
                            worker.stdin = stdin;
                            info = worker.info.clone();
                            true
                        })
                    };
                    if !installed {
                        if let Some(mut child) = child {
                            let _ = child.kill();
                            let _ = child.wait();
                        }
                        return ApiResponse::error("agent lifecycle changed while spawning");
                    }
                }
                Err(error) => {
                    eprintln!("tachyond: failed to spawn ghost: {error}");
                    if let Some(worker) = registry.lock().unwrap().tasks.get_mut(&id) {
                        worker.info.state = AgentState::Failed;
                        info = worker.info.clone();
                    }
                    finish_agent(
                        registry,
                        &id,
                        0,
                        AgentState::Failed,
                        format!("ghost failed to start: {error}"),
                    );
                    persist_task(registry, &info, &format!("Agent start failed: {error}"));
                    return ApiResponse::Agent { info };
                }
            }

            if delegated_by_background {
                collect_worker_result(
                    Arc::clone(registry),
                    work_request
                        .as_ref()
                        .expect("delegated work request")
                        .work_id
                        .clone(),
                    FOREGROUND_ID.into(),
                );
            }
            {
                let pump_registry = Arc::clone(registry);
                let id = id.clone();
                std::thread::spawn(move || pump_agent(&pump_registry, &id, 0));
            }
            let delivery = if let Some(request) = &work_request {
                deliver_work(registry, &id, request, info.persistent)
            } else {
                deliver_task(registry, &id, task, info.persistent)
            };
            if let Err(error) = delivery {
                let pid = {
                    let mut reg = registry.lock().unwrap();
                    reg.tasks.get_mut(&id).and_then(|worker| {
                        if worker.generation != 0 || worker.assignment != 0 {
                            return None;
                        }
                        worker.info.state = AgentState::Failed;
                        worker.ready = false;
                        worker.stdin = None;
                        worker.info.pid
                    })
                };
                if let Some(pid) = pid {
                    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
                }
                finish_agent(
                    registry,
                    &id,
                    0,
                    AgentState::Failed,
                    format!("work delivery failed: {error}"),
                );
                return ApiResponse::error(error);
            }

            persist_task(registry, &info, "Agent started.");
            if let Some(request) = &work_request {
                if let Some(work) = registry.lock().unwrap().works.get_mut(&request.work_id) {
                    work.info = info.clone();
                }
            }
            eprintln!("tachyond: agent {id} started: {task}");
            ApiResponse::Agent { info }
        }
        AgentList => ApiResponse::Agents {
            agents: registry.lock().unwrap().sorted(),
        },
        AgentStatus { id } => match id {
            Some(id) => {
                let reg = registry.lock().unwrap();
                match reg.get(id) {
                    Some(t) => ApiResponse::Agent {
                        info: t.info.clone(),
                    },
                    None => ApiResponse::error(format!("no such agent: {id}")),
                }
            }
            None => ApiResponse::Agents {
                agents: registry.lock().unwrap().sorted(),
            },
        },
        AgentCat { id } => {
            let reg = registry.lock().unwrap();
            match reg.get(id) {
                Some(t) => ApiResponse::Agent {
                    info: t.info.clone(),
                },
                None => ApiResponse::error(format!("no such agent: {id}")),
            }
        }
        AgentLogs { id, lines, .. } => {
            let n = (*lines).max(1) as usize;
            // Replay the structured event log filtered to this agent.
            // Works regardless of whether the agent is still registered
            // (e.g. after a daemon restart or a finished agent).
            let path = tachyon_util::daemon::logs_dir().join("events.jsonl");
            let mut out: Vec<String> = Vec::new();
            if let Ok(contents) = std::fs::read_to_string(path) {
                for ln in contents.lines() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(ln) {
                        if v.get("agent").and_then(|a| a.as_str()) == Some(id.as_str()) {
                            if let Some(data) = v.get("data").and_then(|d| d.as_str()) {
                                out.push(data.to_string());
                            }
                        }
                    }
                }
            }
            let start = out.len().saturating_sub(n);
            ApiResponse::Logs {
                id: id.clone(),
                lines: out[start..].to_vec(),
            }
        }
        AgentStop { id } => lifecycle(
            &registry,
            id,
            Signal::SIGTERM,
            AgentState::Terminated,
            "terminated by user",
        ),
        AgentAwait { id } => {
            let reg = registry.lock().unwrap();
            match reg.get(id) {
                Some(task) => ApiResponse::Agent {
                    info: task.info.clone(),
                },
                None => ApiResponse::error(format!("no such agent: {id}")),
            }
        }
        AgentRelease { id } => release_worker(&registry, id),
        AgentStage { id, ttl_secs } => stage_worker(&registry, id, *ttl_secs),
        AgentRetain {
            id,
            lease_until_secs,
            lifetime_class,
        } => retain_worker(&registry, id, *lease_until_secs, *lifetime_class),
        AgentReplan { id, task } => replace_worker(&registry, id, Some(task)),
        AgentInterrupt { id } => lifecycle(
            &registry,
            id,
            Signal::SIGINT,
            AgentState::Interrupted,
            "interrupted by user",
        ),
        AgentKill { id } => lifecycle(
            &registry,
            id,
            Signal::SIGKILL,
            AgentState::Terminated,
            "killed",
        ),
        AgentRestart { id } | AgentResume { id } => replace_worker(&registry, id, None),
        AgentExec {
            id,
            command: _command,
        } => {
            let reg = registry.lock().unwrap();
            if reg.get(id).is_some() {
                ApiResponse::Exec {
                    id: id.clone(),
                    exit_code: None,
                    stdout: String::new(),
                    stderr: "exec not yet wired (harness not built)".into(),
                }
            } else {
                ApiResponse::error(format!("no such agent: {id}"))
            }
        }
        AgentAttach { id } => {
            let reg = registry.lock().unwrap();
            match reg.get(id) {
                Some(t) => ApiResponse::Attach {
                    id: id.clone(),
                    output: format!(
                        "agent {} is {:?}; stream with the TUI",
                        t.info.id, t.info.state
                    ),
                },
                None => ApiResponse::error(format!("no such agent: {id}")),
            }
        }
        AgentChat { id, text } => match deliver_task(registry, id, text, false) {
            Ok(()) => ApiResponse::Chat { id: id.clone() },
            Err(e) => ApiResponse::error(e),
        },
        ForegroundChat { text, cwd } => {
            let cwd = match cwd {
                Some(path) => match tachyon_util::daemon::selected_workspace(path) {
                    Ok(path) => Some(path.to_string_lossy().into_owned()),
                    Err(error) => return ApiResponse::error(error),
                },
                None => None,
            };
            let command = encode_interaction_command(
                InteractionCommand::AcceptUserTurn { text: text.clone() },
                None,
                None,
                None,
                cwd,
            );
            match command.and_then(|command| deliver_task(registry, FOREGROUND_ID, &command, false))
            {
                Ok(()) => ApiResponse::Chat {
                    id: FOREGROUND_ID.into(),
                },
                Err(e) => ApiResponse::error(e),
            }
        }
        Top => ApiResponse::Agents {
            agents: registry.lock().unwrap().sorted(),
        },
        AgentSubscribe { .. } | WorkSubscribe { .. } => {
            ApiResponse::error("unreachable: handled in handle_connection")
        }
        ForegroundSubscribe => ApiResponse::error("unreachable: handled in handle_connection"),
    }
}

/// Signal an agent's process and set the daemon-authoritative lifecycle state.
fn lifecycle(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    signal: Signal,
    state: AgentState,
    reason: &str,
) -> ApiResponse {
    let mut reg = registry.lock().unwrap();
    if reg.foreground_id.as_deref() == Some(id) {
        return ApiResponse::error("the foreground is not a controllable worker");
    }
    match reg.get_mut(id) {
        Some(t) => {
            if let Some(socket) = t.control_socket.as_deref() {
                let command = match signal {
                    Signal::SIGINT => "signal\tint\n",
                    Signal::SIGKILL => "signal\tkill\n",
                    _ => "signal\tterm\n",
                };
                let _ = supervisor_command(socket, command);
            } else if let Some(pid) = t.info.pid {
                if let Err(error) = kill(Pid::from_raw(pid as i32), signal) {
                    eprintln!("tachyond: signal agent {id} ({pid}): {error}");
                }
            }
            t.generation = t.generation.wrapping_add(1);
            t.info.state = state;
            t.info.pid = None;
            t.info.retained = false;
            t.info.lease_until_secs = None;
            t.stdin = None;
            let info = t.info.clone();
            let ev = AgentEvent {
                stream: EventStream::Exit,
                data: reason.to_string(),
            };
            t.subs.retain(|tx| tx.send(ev.clone()).is_ok());
            drop(reg);
            persist_task(registry, &info, reason);
            ApiResponse::Agent { info }
        }
        None => ApiResponse::error(format!("no such agent: {id}")),
    }
}

#[cfg(test)]
fn lifecycle_signal(state: AgentState) -> Signal {
    match state {
        AgentState::Terminated => Signal::SIGTERM,
        AgentState::Interrupted => Signal::SIGINT,
        _ => Signal::SIGKILL,
    }
}

/// Replace a worker process while retaining its durable task identity and
/// metadata. A generation prevents the old process pump from changing the
/// state of the newly-created worker after a restart/resume.
fn replace_worker(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    objective: Option<&str>,
) -> ApiResponse {
    let candidate = {
        let mut reg = registry.lock().unwrap();
        if reg.foreground_id.as_deref() == Some(id) {
            return ApiResponse::error("the foreground is not a controllable worker");
        }
        let Some(task) = reg.get_mut(id) else {
            return ApiResponse::error(format!("no such agent: {id}"));
        };
        let old_pid = task.info.pid;
        task.generation = task.generation.wrapping_add(1);
        task.terminal_usage = None;
        task.terminal_result = None;
        if let Some(objective) = objective {
            task.info.task = objective.to_string();
        }
        task.info.state = AgentState::Starting;
        task.info.pid = None;
        task.stdin = None;
        (task.info.clone(), task.generation, task.assignment, old_pid)
    };
    let (info, generation, assignment, old_pid) = candidate;
    if let Some(pid) = old_pid {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    }
    persist_task(registry, &info, "Recreating agent worker.");

    let result = std::fs::create_dir_all(&info.workspace)
        .and_then(|_| spawn_ghost(&info.id, &info.task, &info.workspace));
    match result {
        Ok(mut child) => {
            let pid = child.id();
            let stdin = child.stdin.take().map(|stdin| Arc::new(Mutex::new(stdin)));
            let mut child = Some(child);
            let installed = {
                let mut reg = registry.lock().unwrap();
                reg.tasks.get_mut(id).and_then(|task| {
                    if task.generation != generation || task.assignment != assignment {
                        return None;
                    }
                    task.process = child.take();
                    task.stdin = stdin;
                    task.info.state = AgentState::Running;
                    task.info.pid = Some(pid);
                    Some(task.info.clone())
                })
            };
            let Some(info) = installed else {
                if let Some(mut child) = child {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                return ApiResponse::error("agent lifecycle changed while recreating");
            };
            persist_task(registry, &info, "Agent worker recreated.");
            let pump_reg = Arc::clone(registry);
            let pump_id = id.to_string();
            std::thread::spawn(move || pump_agent(&pump_reg, &pump_id, generation));
            ApiResponse::Agent { info }
        }
        Err(error) => {
            let info = {
                let mut reg = registry.lock().unwrap();
                let task = reg.tasks.get_mut(id).unwrap();
                task.info.state = AgentState::Failed;
                task.info.pid = None;
                task.info.clone()
            };
            persist_task(
                registry,
                &info,
                &format!("worker recreation failed: {error}"),
            );
            ApiResponse::Agent { info }
        }
    }
}

/// Explicitly release a worker. Detach its process before waiting so no daemon
/// registry lock is held while a child or filesystem operation can block.
fn release_worker(registry: &Arc<Mutex<Registry>>, id: &str) -> ApiResponse {
    let (info, mut process, pid, control_socket) = {
        let mut reg = registry.lock().unwrap();
        if reg.foreground_id.as_deref() == Some(id) {
            return ApiResponse::error("the foreground is not a controllable worker");
        }
        let Some(task) = reg.get_mut(id) else {
            return ApiResponse::error(format!("no such agent: {id}"));
        };
        task.generation = task.generation.wrapping_add(1);
        let pid = task.info.pid;
        let control_socket = task.control_socket.clone();
        task.info.state = AgentState::Released;
        task.info.pid = None;
        task.info.retained = false;
        task.info.lease_until_secs = None;
        task.stdin = None;
        let process = task.process.take();
        let info = task.info.clone();
        let event = AgentEvent {
            stream: EventStream::Exit,
            data: "released by user".into(),
        };
        task.subs.retain(|tx| tx.send(event.clone()).is_ok());
        (info, process, pid, control_socket)
    };

    if let Some(child) = process.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    } else if let Some(socket) = &control_socket {
        let _ = supervisor_command(socket, "signal\tterm\n");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::path::Path::new(socket).exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        if std::path::Path::new(socket).exists() {
            if let Some(pid) = pid {
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
            }
        }
    } else if let Some(pid) = pid {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    persist_task(registry, &info, "Agent released.");
    cleanup_workspace(&info);
    ApiResponse::Agent { info }
}

fn retain_worker(
    registry: &Arc<Mutex<Registry>>,
    id: &str,
    lease_until_secs: Option<u64>,
    lifetime_class: Option<LifetimeClass>,
) -> ApiResponse {
    let reclassify = {
        let mut reg = registry.lock().unwrap();
        if reg.foreground_id.as_deref() == Some(id) {
            return ApiResponse::error("the foreground is not a retainable worker");
        }
        let Some(task) = reg.get_mut(id) else {
            return ApiResponse::error(format!("no such agent: {id}"));
        };
        let target = lifetime_class.unwrap_or(task.info.lifetime_class);
        let crosses_persistent_boundary =
            task.info.persistent != (target == LifetimeClass::Persistent);
        if crosses_persistent_boundary && task.info.state != AgentState::Completed {
            return ApiResponse::error(
                "lifetime promotion or demotion involving persistent requires an idle completed worker",
            );
        }
        task.info.retained = true;
        task.info.lease_until_secs = lease_until_secs;
        task.info.stage_until_secs = None;
        task.info.lifetime_class = target;
        task.info.turn_budget = (target == LifetimeClass::Short).then_some(3);
        if lifetime_class.is_some() {
            task.info.turns_used = 0;
        }
        task.last_used_secs = unix_now();
        if !crosses_persistent_boundary {
            let info = task.info.clone();
            drop(reg);
            persist_task(registry, &info, "Worker retention policy updated.");
            return ApiResponse::Agent { info };
        }
        let old_pid = task.info.pid;
        let old_socket = task.control_socket.clone();
        task.generation = task.generation.wrapping_add(1);
        let generation = task.generation;
        let assignment = task.assignment;
        task.info.persistent = target == LifetimeClass::Persistent;
        task.info.state = AgentState::Starting;
        task.info.pid = None;
        task.stdin = None;
        task.process = None;
        task.ready = false;
        task.control_socket = (target == LifetimeClass::Persistent)
            .then(|| format!("{}/.tachyon/agent.sock", task.info.workspace));
        (
            task.info.clone(),
            target,
            generation,
            assignment,
            old_pid,
            old_socket,
        )
    };
    let (mut info, target, generation, assignment, old_pid, old_socket) = reclassify;
    if let Some(socket) = old_socket {
        let _ = supervisor_command(&socket, "signal\tterm\n");
    } else if let Some(pid) = old_pid {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    let spawned = std::fs::create_dir_all(&info.workspace).and_then(|_| {
        if target == LifetimeClass::Persistent {
            spawn_supervised_ghost(id, &info.workspace)
        } else {
            spawn_warm_ghost(id, &info.workspace)
        }
    });
    match spawned {
        Ok(mut child) => {
            let stdin = child.stdin.take().map(|stdin| Arc::new(Mutex::new(stdin)));
            let mut child = Some(child);
            let installed = {
                let mut reg = registry.lock().unwrap();
                reg.tasks.get_mut(id).is_some_and(|task| {
                    if task.generation != generation || task.assignment != assignment {
                        return false;
                    }
                    task.info.pid = child.as_ref().map(Child::id);
                    task.info.state = AgentState::Completed;
                    task.process = child.take();
                    task.stdin = stdin;
                    info = task.info.clone();
                    true
                })
            };
            if !installed {
                if let Some(mut child) = child {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                return ApiResponse::error("worker lifecycle changed while reclassifying");
            }
            persist_task(registry, &info, "Worker lifetime policy reclassified.");
            let pump_registry = Arc::clone(registry);
            let worker_id = id.to_string();
            std::thread::spawn(move || pump_agent(&pump_registry, &worker_id, generation));
            ApiResponse::Agent { info }
        }
        Err(error) => {
            let info = {
                let mut reg = registry.lock().unwrap();
                let task = reg.tasks.get_mut(id).unwrap();
                if task.generation == generation && task.assignment == assignment {
                    task.info.state = AgentState::Failed;
                }
                task.info.clone()
            };
            persist_task(
                registry,
                &info,
                &format!("worker lifetime reclassification failed: {error}"),
            );
            ApiResponse::Agent { info }
        }
    }
}

fn stage_worker(registry: &Arc<Mutex<Registry>>, id: &str, ttl_secs: u64) -> ApiResponse {
    let mut reg = registry.lock().unwrap();
    if reg.foreground_id.as_deref() == Some(id) {
        return ApiResponse::error("the foreground cannot be staged");
    }
    match reg.get_mut(id) {
        Some(task) => {
            task.info.state = AgentState::Staged;
            task.info.retained = false;
            task.info.stage_until_secs = Some(unix_now().saturating_add(ttl_secs));
            let info = task.info.clone();
            let event = AgentEvent {
                stream: EventStream::Stage,
                data: format!("staged for termination in {ttl_secs}s"),
            };
            task.subs.retain(|tx| tx.send(event.clone()).is_ok());
            drop(reg);
            persist_task(registry, &info, "Agent staged for delayed termination.");
            ApiResponse::Agent { info }
        }
        None => ApiResponse::error(format!("no such agent: {id}")),
    }
}

fn cleanup_workspace(info: &AgentInfo) {
    {
        let mut retained = artifact_retention().lock().unwrap();
        if let Some((_, cleanup)) = retained.get_mut(&info.workspace) {
            *cleanup = Some(info.clone());
            return;
        }
    }
    let keep = std::env::var("TACHYON_KEEP_WORKSPACES")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    if !keep && info.id != FOREGROUND_ID {
        let root = tachyon_util::daemon::workspaces_dir();
        let workspace = std::path::Path::new(&info.workspace);
        if workspace.starts_with(root) {
            let _ = std::fs::remove_dir_all(workspace);
        }
    }
}

fn supervisor_command(socket: &str, command: &str) -> std::io::Result<()> {
    let mut stream = UnixStream::connect(socket)?;
    stream.write_all(command.as_bytes())
}

/// Resolve the ghost binary path.
fn ghost_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("TACHYON_GHOST_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join("ghost")))
        .unwrap_or_else(|| "ghost".into())
}

/// Resolve the standalone user-facing Conversation runtime.
fn foreground_path() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("TACHYON_FOREGROUND_BIN") {
        return path.into();
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("tachyon-foreground")))
        .unwrap_or_else(|| "tachyon-foreground".into())
}

fn background_path() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("TACHYON_BACKGROUND_BIN") {
        return path.into();
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("tachyon-background")))
        .unwrap_or_else(|| "tachyon-background".into())
}

fn spawn_background() -> std::io::Result<Child> {
    Command::new(background_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

fn write_review_request(
    stdin: &mut std::process::ChildStdin,
    request: &WorkReviewRequest,
) -> std::io::Result<()> {
    serde_json::to_writer(&mut *stdin, request).map_err(std::io::Error::other)?;
    stdin.write_all(b"\n")?;
    stdin.flush()
}

fn write_schedule_request(
    stdin: &mut std::process::ChildStdin,
    request: &BackgroundScheduleRequest,
) -> std::io::Result<()> {
    serde_json::to_writer(&mut *stdin, request).map_err(std::io::Error::other)?;
    stdin.write_all(b"\n")?;
    stdin.flush()
}

fn current_review_request(
    registry: &Arc<Mutex<Registry>>,
    request: &WorkReviewRequest,
) -> Option<WorkReviewRequest> {
    registry
        .lock()
        .unwrap()
        .works
        .get(&request.candidate.work_id)
        .and_then(|work| work.review.as_ref())
        .filter(|review| review.request.review_id == request.review_id)
        .map(|review| review.request.clone())
}

fn supervise_background(
    registry: Arc<Mutex<Registry>>,
    rx: mpsc::Receiver<CoordinatorRequest>,
    shutdown: Arc<AtomicBool>,
) {
    let mut generation = 0_u64;
    let schedule_responses = Arc::new(Mutex::new(HashMap::<
        String,
        mpsc::SyncSender<BackgroundScheduleDecision>,
    >::new()));
    while !shutdown.load(Ordering::SeqCst) {
        let mut child = match spawn_background() {
            Ok(child) => child,
            Err(error) => {
                eprintln!("tachyond: failed to start {BACKGROUND_ID}: {error}");
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }
        };
        generation = generation.wrapping_add(1);
        let pending = {
            let mut reg = registry.lock().unwrap();
            reg.background_generation = generation;
            reg.works
                .values_mut()
                .filter_map(|work| {
                    let review = work.review.as_mut()?;
                    review.request.coordinator_generation = generation;
                    Some(review.request.clone())
                })
                .collect::<Vec<_>>()
        };
        let Some(mut stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            continue;
        };
        registry.lock().unwrap().background_online = true;
        if let Some(stdout) = child.stdout.take() {
            let decision_registry = Arc::clone(&registry);
            let schedule_responses = Arc::clone(&schedule_responses);
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if let Ok(decision) = serde_json::from_str::<BackgroundScheduleDecision>(&line)
                    {
                        if let Some(response) = schedule_responses
                            .lock()
                            .unwrap()
                            .remove(&decision.request_id)
                        {
                            let _ = response.send(decision);
                        }
                        continue;
                    }
                    match serde_json::from_str::<WorkReviewDecision>(&line) {
                        Ok(decision) => apply_work_review(&decision_registry, decision),
                        Err(error) => {
                            eprintln!("tachyond: invalid {BACKGROUND_ID} decision: {error}")
                        }
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            std::thread::spawn(move || {
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    eprintln!("{BACKGROUND_ID}: {line}");
                }
            });
        }
        let mut restart = false;
        for request in pending {
            if write_review_request(&mut stdin, &request).is_err() {
                restart = true;
                break;
            }
        }
        while !restart && !shutdown.load(Ordering::SeqCst) {
            match rx.recv_timeout(std::time::Duration::from_millis(50)) {
                Ok(command) => match command {
                    CoordinatorRequest::WorkReview(request) => {
                        if let Some(request) = current_review_request(&registry, &request) {
                            if write_review_request(&mut stdin, &request).is_err() {
                                restart = true;
                            }
                        }
                    }
                    CoordinatorRequest::Schedule { request, response } => {
                        schedule_responses
                            .lock()
                            .unwrap()
                            .insert(request.request_id.clone(), response);
                        if write_schedule_request(&mut stdin, &request).is_err() {
                            schedule_responses
                                .lock()
                                .unwrap()
                                .remove(&request.request_id);
                            restart = true;
                        }
                    }
                },
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    registry.lock().unwrap().background_online = false;
                    return;
                }
            }
            if child.try_wait().ok().flatten().is_some() {
                restart = true;
            }
        }
        if shutdown.load(Ordering::SeqCst) {
            registry.lock().unwrap().background_online = false;
            let _ = child.kill();
            let _ = child.wait();
            return;
        }
        registry.lock().unwrap().background_online = false;
        let _ = child.kill();
        let _ = child.wait();
        schedule_responses.lock().unwrap().clear();
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

fn supervisor_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("TACHYON_AGENT_SUPERVISOR_BIN") {
        return p.into();
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join("tachyon-agent-supervisor")))
        .unwrap_or_else(|| "tachyon-agent-supervisor".into())
}

fn spawn_supervised_ghost(id: &str, workspace: &str) -> std::io::Result<Child> {
    let socket = std::path::Path::new(workspace).join(".tachyon/agent.sock");
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Command::new(supervisor_path())
        .args([
            "--socket",
            &socket.display().to_string(),
            "--ghost",
            &ghost_path().display().to_string(),
            "--id",
            id,
            "--cwd",
            workspace,
        ])
        .current_dir(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Spawn the background Ghost harness for a worker agent. Workers are **ephemeral**:
/// one-shot `--task` mode that runs the autonomous loop and exits when done.
/// The worker's `--cwd` is its dedicated workspace (the jail root); ghost's
/// Local backend confines all commands there.
fn spawn_ghost(id: &str, task: &str, workspace: &str) -> std::io::Result<Child> {
    Command::new(ghost_path())
        .arg("--task")
        .arg(task)
        .arg("--role")
        .arg("worker")
        .arg("--agent-id")
        .arg(id)
        .arg("--cwd")
        .arg(workspace)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

fn spawn_warm_ghost(id: &str, workspace: &str) -> std::io::Result<Child> {
    Command::new(ghost_path())
        .arg("--chat")
        .arg("--role")
        .arg("worker")
        .arg("--agent-id")
        .arg(id)
        .arg("--cwd")
        .arg(workspace)
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

/// Spawn the standalone Conversational Agent. Returns
/// (id, child, stdin).
fn spawn_foreground() -> std::io::Result<(String, Child, std::process::ChildStdin)> {
    let mut cmd = foreground_command();

    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no stdin"))?;
    let id = FOREGROUND_ID.to_string();
    Ok((id, child, stdin))
}

fn foreground_command() -> Command {
    let mut cmd = Command::new(foreground_path());
    cmd.arg("--agent-id")
        .arg(FOREGROUND_ID)
        .arg("--new-session");

    // The Conversational Agent's workspace should be the directory the daemon
    // was launched from (the user's project when they ran `tachyon` there).
    if let Ok(cwd) = std::env::current_dir() {
        cmd.arg("--cwd").arg(&cwd).current_dir(&cwd);
    }

    cmd
}

/// Run on a thread: pump a ghost's stdout/stderr to subscribers and finish the
/// agent when the process exits. For the foreground, `[daemon:spawn] <task>`
/// lines are intercepted: a worker is spawned, its [agent] answer is collected,
/// and `[daemon:result] <answer>` is fed back to the foreground's stdin.
fn pump_agent(registry: &Arc<Mutex<Registry>>, id: &str, generation: u64) {
    let is_foreground = id == FOREGROUND_ID;

    // Take the process (for wait) but leave the stdin handle in the task so
    // `write_stdin` (AgentChat / ForegroundChat) can keep writing to it.
    let (child, persistent, control_socket) = {
        let mut reg = registry.lock().unwrap();
        match reg.tasks.get_mut(id) {
            Some(task) if task.generation == generation => (
                task.process.take(),
                task.info.persistent,
                task.control_socket.clone(),
            ),
            None => return,
            Some(_) => return,
        }
    };
    if persistent {
        let Some(socket) = control_socket else { return };
        pump_supervisor(registry, id, generation, &socket);
        if let Some(mut child) = child {
            let code = child.wait().ok().and_then(|s| s.code());
            let state = if code == Some(0) {
                AgentState::Completed
            } else {
                AgentState::Failed
            };
            finish_agent(registry, id, generation, state, "supervisor exited".into());
        }
        return;
    }
    let mut child = match child {
        Some(c) => c,
        None => return,
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // stdout pump: route spawn requests for the foreground.
    let regs = Arc::clone(registry);
    let id_out = id.to_string();
    let is_foreground = is_foreground;
    let h_out = std::thread::spawn(move || {
        let Some(stream) = stdout else { return };
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let trimmed = line.trim_end_matches(['\r', '\n']).to_string();
                    if is_foreground {
                        if let Some(rest) = trimmed.strip_prefix("[daemon:spawn]") {
                            handle_foreground_spawn(&regs, rest.trim(), &id_out);
                            continue;
                        }
                    }
                    push_event(&regs, &id_out, EventStream::Stdout, &trimmed);
                }
            }
        }
    });

    let regs = Arc::clone(registry);
    let id_err = id.to_string();
    let h_err = std::thread::spawn(move || drain_stream(regs, id_err, stderr, EventStream::Stderr));

    let _ = h_out.join();
    let _ = h_err.join();

    let code = child.wait().ok().and_then(|s| s.code());
    let state = registry
        .lock()
        .unwrap()
        .tasks
        .get(id)
        .map(|task| task.info.state)
        .filter(|state| {
            matches!(
                state,
                AgentState::Released
                    | AgentState::Waiting
                    | AgentState::Completed
                    | AgentState::Terminated
            )
        })
        .unwrap_or_else(|| {
            if code == Some(0) {
                AgentState::Completed
            } else {
                AgentState::Failed
            }
        });
    let msg = match code {
        Some(c) => format!("exited ({c})"),
        None => "exited".into(),
    };
    finish_agent(registry, id, generation, state, msg);
}

fn pump_supervisor(registry: &Arc<Mutex<Registry>>, id: &str, generation: u64, socket: &str) {
    let mut stream = None;
    for _ in 0..100 {
        match UnixStream::connect(socket) {
            Ok(connection) => {
                stream = Some(connection);
                break;
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
        }
    }
    let Some(stream) = stream else {
        push_event(
            registry,
            id,
            EventStream::Stderr,
            "persistent supervisor unavailable",
        );
        return;
    };
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if registry
                    .lock()
                    .unwrap()
                    .tasks
                    .get(id)
                    .is_none_or(|task| task.generation != generation)
                {
                    return;
                }
                let trimmed = line.trim_end();
                let Some((kind, data)) = trimmed.split_once('\t') else {
                    continue;
                };
                let stream = match kind {
                    "stderr" => EventStream::Stderr,
                    "exit" => EventStream::Exit,
                    _ => EventStream::Stdout,
                };
                if stream == EventStream::Stdout
                    && decode_structured_event(data).is_some_and(|event| {
                        matches!(event, StructuredAgentEvent::Status { phase, message, .. } if phase == "ready" && message == "idle")
                    })
                {
                    if let Some(task) = registry.lock().unwrap().tasks.get_mut(id) {
                        if task.generation == generation {
                            task.info.state = AgentState::Completed;
                            task.info.retained = true;
                        }
                    }
                }
                push_event(registry, id, stream, data);
                if stream == EventStream::Exit {
                    break;
                }
            }
        }
    }
}

/// The foreground requested `spawn_agent`. Spawn a worker, wait for it to
/// answer, and feed `[daemon:result] <answer>` back to the foreground's stdin.
fn handle_foreground_spawn(registry: &Arc<Mutex<Registry>>, spec: &str, foreground_id: &str) {
    let task = spec.to_string();

    // Keep one warm worker per foreground. Its Ghost conversation and
    // IPython namespace survive between related delegated tasks.
    let reusable = {
        let mut reg = registry.lock().unwrap();
        let id = reg
            .tasks
            .values()
            .find(|candidate| {
                candidate.warm
                    && candidate.owner.as_deref() == Some(foreground_id)
                    && matches!(
                        candidate.info.state,
                        AgentState::Waiting | AgentState::Completed
                    )
                    && candidate.info.pid.is_some()
                    && candidate.stdin.is_some()
            })
            .map(|candidate| candidate.info.id.clone());
        if let Some(id) = &id {
            if let Some(candidate) = reg.tasks.get_mut(id) {
                candidate.info.task = task.clone();
                candidate.info.state = AgentState::Running;
                candidate.last_used_secs = unix_now();
                candidate.assignment = candidate.assignment.wrapping_add(1);
                candidate.ready = false;
            }
        }
        id
    };
    if let Some(worker_id) = reusable {
        if let Err(error) = deliver_task(registry, &worker_id, &task, false) {
            push_event(
                registry,
                foreground_id,
                EventStream::Stdout,
                &format!("[daemon] reuse failed: {error}"),
            );
            return;
        }
        push_event(
            registry,
            foreground_id,
            EventStream::Stdout,
            &format!("[daemon] reusing warm worker {worker_id}"),
        );
        collect_legacy_worker_result(Arc::clone(registry), worker_id, foreground_id.to_string());
        return;
    }

    // Spawn and register a worker agent (ghost one-shot, jailed to a
    // per-worker workspace).
    let (id, now, ws_str) = {
        let reg = registry.lock().unwrap();
        let id = reg.next_id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let ws_str = tachyon_util::daemon::workspaces_dir()
            .join(&id)
            .display()
            .to_string();
        (id, now, ws_str)
    };
    let worker_id: String =
        match std::fs::create_dir_all(&ws_str).and_then(|_| spawn_warm_ghost(&id, &ws_str)) {
            Ok(mut c) => {
                let stdin = c.stdin.take().map(|stdin| Arc::new(Mutex::new(stdin)));
                let info = AgentInfo {
                    id: id.clone(),
                    task: task.clone(),
                    state: AgentState::Running,
                    pid: Some(c.id()),
                    workspace: ws_str.clone(),
                    created_secs: now,
                    retained: true,
                    lease_until_secs: None,
                    session_id: id.clone(),
                    lifetime_class: tachyon_api::types::LifetimeClass::Long,
                    purpose: task.clone(),
                    owner: foreground_id.to_string(),
                    last_activity_secs: now,
                    checkpoint_available: false,
                    turns_used: 0,
                    turn_budget: None,
                    task_type: "research".into(),
                    description: task.clone(),
                    persistent: false,
                    sandboxed: false,
                    stage_until_secs: None,
                    logical_task_id: None,
                    origin_turn_id: None,
                    parent_task_id: None,
                    tool_call_id: None,
                };
                registry.lock().unwrap().tasks.insert(
                    id.clone(),
                    Task {
                        info,
                        depends_on: Vec::new(),
                        process: Some(c),
                        stdin,
                        subs: Vec::new(),
                        generation: 0,
                        assignment: 0,
                        warm: true,
                        ready: false,
                        owner: Some(foreground_id.to_string()),
                        last_used_secs: now,
                        control_socket: None,
                        terminal_usage: None,
                        terminal_result: None,
                    },
                );
                id
            }
            Err(e) => {
                push_event(
                    registry,
                    foreground_id,
                    EventStream::Stdout,
                    &format!("[daemon] spawn failed: {e}"),
                );
                return;
            }
        };
    if let Err(error) = deliver_task(registry, &worker_id, &task, false) {
        push_event(
            registry,
            foreground_id,
            EventStream::Stdout,
            &format!("[daemon] task delivery failed: {error}"),
        );
        return;
    }
    push_event(
        registry,
        foreground_id,
        EventStream::Stdout,
        &format!("[daemon] spawned worker {worker_id} for: {task}"),
    );
    if let Some(info) = registry
        .lock()
        .unwrap()
        .tasks
        .get(&worker_id)
        .map(|task| task.info.clone())
    {
        persist_task(registry, &info, "Agent started by foreground.");
    }

    // Subscribe before pumping so a fast worker's final event cannot be lost.
    collect_legacy_worker_result(
        Arc::clone(registry),
        worker_id.clone(),
        foreground_id.to_string(),
    );

    // Start pumping the worker's stdout/stderr to its subscribers.
    let pump_reg = Arc::clone(registry);
    let pump_id = worker_id.clone();
    std::thread::spawn(move || pump_agent(&pump_reg, &pump_id, 0));
}

fn collect_worker_result(registry: Arc<Mutex<Registry>>, work_id: String, foreground_id: String) {
    let (rx, request, worker_id, scheduled) = {
        let mut reg = registry.lock().unwrap();
        let Some(work) = reg.works.get(&work_id) else {
            return;
        };
        let request = work.request.clone();
        let worker_id = work.worker_id.clone();
        let scheduled = work.info.purpose == "scheduled_task";
        let Some(rx) = reg.subscribe_work(&work_id) else {
            return;
        };
        (rx, request, worker_id, scheduled)
    };
    std::thread::spawn(move || {
        let remaining_ms = request.deadline_ms.saturating_sub(unix_now_ms());
        let event = match rx.recv_timeout(std::time::Duration::from_millis(remaining_ms.max(1))) {
            Ok(event) => Some(event),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if registry
                    .lock()
                    .unwrap()
                    .works
                    .get(&request.work_id)
                    .is_some_and(|work| work.review.is_some())
                {
                    rx.recv().ok()
                } else {
                    let result = WorkResult {
                        candidate_refs: None,
                        final_context: None,
                        attempt_id: request.attempt.as_ref().map(|a| a.id.clone()),
                        instruction_revision: None,
                        work_id: request.work_id.clone(),
                        evidence: Default::default(),
                        timing: None,
                        objective: request.objective.clone(),
                        generation: request.generation,
                        assignment: request.assignment,
                        outcome: WorkOutcome::TimedOut {
                            deadline_ms: request.deadline_ms,
                        },
                    };
                    let envelope = result_envelope(&worker_id, result);
                    if let Ok(data) = serde_json::to_string(&envelope) {
                        push_event(&registry, &worker_id, EventStream::Stdout, &data);
                    }
                    None
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => None,
        };
        let Some(event) = event else { return };
        if scheduled {
            return;
        }
        let Ok(envelope) = serde_json::from_str::<EventEnvelope>(&event.data) else {
            return;
        };
        let StructuredAgentEvent::WorkResult { result } = &envelope.kind else {
            return;
        };
        if !matches!(result.outcome, WorkOutcome::Completed { .. }) {
            return;
        }
        let reply = encode_interaction_command(
            InteractionCommand::PublishBackgroundUpdate {
                event: envelope.clone(),
            },
            envelope
                .tool_call_id
                .clone()
                .or_else(|| envelope.task_id.clone())
                .or_else(|| envelope.turn_id.clone()),
            Some(format!("event-{}", envelope.event_id)),
            envelope.turn_id.clone(),
            None,
        )
        .unwrap_or_else(|_| format!("[daemon:evidence] {}", event.data));
        let _ = deliver_task(&registry, &foreground_id, &reply, false);
    });
}

fn collect_legacy_worker_result(
    registry: Arc<Mutex<Registry>>,
    worker_id: String,
    foreground_id: String,
) {
    let rx = registry.lock().unwrap().subscribe(&worker_id);
    std::thread::spawn(move || {
        let mut answer: Option<String> = None;
        let mut buf: Vec<String> = Vec::new();
        let mut timed_out = false;
        if let Some(rx) = rx {
            let deadline = std::time::Instant::now() + worker_result_timeout();
            loop {
                let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now())
                else {
                    timed_out = true;
                    break;
                };
                let ev = match rx.recv_timeout(remaining) {
                    Ok(event) => event,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        timed_out = true;
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                match ev.stream {
                    EventStream::Stdout => {
                        let data = ev.data.clone();
                        buf.push(data.clone());
                        if let Some(StructuredAgentEvent::WorkerCompleted { .. }) =
                            decode_structured_event(&data)
                        {
                            answer = Some(data);
                            break;
                        }
                    }
                    EventStream::Exit => break,
                    _ => {}
                }
            }
        }
        if timed_out {
            push_event(
                &registry,
                &foreground_id,
                EventStream::Stderr,
                &format!("worker {worker_id} timed out waiting for a result"),
            );
            return;
        }
        let reply = match answer {
            Some(a) => match serde_json::from_str::<EventEnvelope>(&a) {
                Ok(event) => encode_interaction_command(
                    InteractionCommand::PublishBackgroundUpdate {
                        event: event.clone(),
                    },
                    event
                        .tool_call_id
                        .clone()
                        .or_else(|| event.task_id.clone())
                        .or_else(|| event.turn_id.clone()),
                    Some(format!("event-{}", event.event_id)),
                    event.turn_id.clone(),
                    None,
                )
                .unwrap_or_else(|_| format!("[daemon:evidence] {a}")),
                Err(_) => format!("[daemon:evidence] {a}"),
            },
            None => format!(
                "[daemon:evidence] worker ended without a WorkerCompleted event:\n{}",
                buf.join("\n")
            ),
        };
        let _ = deliver_task(&registry, &foreground_id, &reply, false);
    });
}

fn worker_result_timeout() -> std::time::Duration {
    let seconds = std::env::var("TACHYON_WORKER_RESULT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(120)
        .max(1);
    std::time::Duration::from_secs(seconds)
}

fn background_review_timeout() -> std::time::Duration {
    let configured = std::env::var("TACHYON_BACKGROUND_REVIEW_TIMEOUT_SECS").ok();
    background_review_timeout_from(configured.as_deref())
}

fn background_review_timeout_from(configured: Option<&str>) -> std::time::Duration {
    let seconds = configured
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_BACKGROUND_REVIEW_TIMEOUT_SECS)
        .max(1);
    std::time::Duration::from_secs(seconds)
}

fn background_review_deadline_ms(now_ms: u64, timeout: std::time::Duration) -> u64 {
    now_ms.saturating_add(timeout.as_millis() as u64)
}

fn decode_structured_event(data: &str) -> Option<StructuredAgentEvent> {
    serde_json::from_str::<EventEnvelope>(data)
        .map(|envelope| envelope.kind)
        .or_else(|_| serde_json::from_str::<StructuredAgentEvent>(data))
        .ok()
}

/// Read a stream to EOF, relaying every line as an event.
fn drain_stream(
    registry: Arc<Mutex<Registry>>,
    id: String,
    stream: Option<impl std::io::Read + Send + 'static>,
    event: EventStream,
) {
    let Some(stream) = stream else { return };
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => push_event(&registry, &id, event, line.trim_end()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_store::HistoryProjection;
    use tachyon_api::types::{MemoryIntent, MemoryMutationKind};
    use tachyon_api::{HistoryKind, HistoryRole};

    #[test]
    fn research_campaign_dispatch_is_inert_and_requires_store() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let registry = Arc::new(Mutex::new(Registry {
            coordinator_tx: Some(tx),
            ..Registry::default()
        }));
        let request = ApiRequest::ResearchCreate {
            command_id: "r".into(),
            title: "Research".into(),
            objective: "Do not execute".into(),
        };
        assert!(dispatch(&request, &registry)
            .as_error()
            .unwrap()
            .contains("unavailable"));
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        registry.lock().unwrap().runtime_store = Some(store.clone());
        registry.lock().unwrap().campaigns = Some(Arc::new(
            runtime_store::campaign_launch::CampaignService::new(store.clone(), dir.path().into())
                .unwrap(),
        ));
        let ApiResponse::Research { research } = dispatch(&request, &registry) else {
            panic!()
        };
        let request = ApiRequest::CampaignCreate {
            command_id: "c".into(),
            research_id: research.id,
            title: "Campaign".into(),
            objective: "Do not execute".into(),
        };
        let ApiResponse::Campaign { campaign } = dispatch(&request, &registry) else {
            panic!()
        };
        assert_eq!(campaign.status, tachyon_api::types::CampaignStatus::Draft);
        assert!(store.campaign_ledger(&campaign.id).unwrap().is_none());
        for archived in [true, false] {
            assert!(matches!(dispatch(&ApiRequest::LocalRetentionSet {
                id: campaign.id.clone(), archived,
            }, &registry), ApiResponse::LocalRetention { archived: actual, .. } if actual == archived));
            assert!(matches!(dispatch(&ApiRequest::LocalRetentionGet {
                id: campaign.id.clone(),
            }, &registry), ApiResponse::LocalRetention { archived: actual, .. } if actual == archived));
        }
        assert!(matches!(
            dispatch(&ApiRequest::CampaignGet { id: campaign.id }, &registry),
            ApiResponse::Campaign { .. }
        ));
        assert!(
            matches!(dispatch(&ApiRequest::ResearchList { after: None, limit: 10 }, &registry), ApiResponse::ResearchList { records, .. } if records.len() == 1)
        );
        assert!(
            matches!(dispatch(&ApiRequest::CampaignList { research_id: None, after: None, limit: 10 }, &registry), ApiResponse::CampaignList { campaigns, .. } if campaigns.len() == 1)
        );
        let reg = registry.lock().unwrap();
        assert!(reg.tasks.is_empty());
        assert!(reg.works.is_empty());
        assert!(reg.foreground_id.is_none());
        assert!(rx.try_recv().is_err());
        assert!(store.list_tasks().unwrap().is_empty());
        assert!(store.scheduled_tasks().unwrap().is_empty());
    }

    #[test]
    fn operational_subscription_stops_with_connected_reader_and_releases_resources() {
        use tachyon_api::todo::{TodoActor, TodoFilter, TodoRequest, TodoResponse, TodoScope};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let shutdown = Arc::new(AtomicBool::new(false));
        let scope = TodoScope::Conversation {
            id: "shutdown-feed".into(),
        };
        let TodoResponse::List { watermark, .. } = store
            .todos(runtime_store::todo::TodoAuthority::Bound {
                scope: scope.clone(),
                actor: TodoActor {
                    source: "operator".into(),
                    actor: "test".into(),
                },
            })
            .unwrap()
            .execute(TodoRequest::List {
                scope: scope.clone(),
                filter: TodoFilter::default(),
                limit: None,
                cursor: None,
            })
            .unwrap()
        else {
            panic!()
        };
        let registry = Arc::new(Mutex::new(Registry {
            runtime_store: Some(store.clone()),
            service_shutdown: shutdown.clone(),
            ..Default::default()
        }));
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let serving = registry.clone();
        let (done, ended) = mpsc::channel();
        let handler = std::thread::spawn(move || {
            done.send(handle_connection(server, serving)).unwrap();
        });
        writeln!(
            client,
            "{}",
            serde_json::to_string(&ApiRequest::OperationalSubscribe {
                scope,
                after: watermark,
            })
            .unwrap()
        )
        .unwrap();
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(
            matches!(serde_json::from_str::<ApiResponse>(&line).unwrap(), ApiResponse::OperationalBatch { batch } if batch.events.is_empty())
        );
        // The idle feed must observe shutdown without reacquiring the registry.
        let guard = registry.lock().unwrap();
        shutdown.store(true, Ordering::Release);
        let result = ended.recv_timeout(std::time::Duration::from_secs(1));
        drop(guard);
        if result.is_err() {
            client.shutdown(std::net::Shutdown::Both).unwrap();
        }
        handler.join().unwrap();
        result.unwrap().unwrap();
        assert_eq!(Arc::strong_count(&store), 2);
        assert_eq!(Arc::strong_count(&registry), 1);
        assert_eq!(Arc::strong_count(&shutdown), 2);
        loop {
            line.clear();
            if reader.read_line(&mut line).unwrap() == 0 {
                break;
            }
        }
    }

    #[test]
    fn service_writes_cancel_and_deadline_without_changing_other_transports() {
        use std::io::Read;
        for cancel in [false, true] {
            let (mut client, mut server) = UnixStream::pair().unwrap();
            nix::sys::socket::setsockopt(&server, nix::sys::socket::sockopt::SndBuf, &1024usize)
                .unwrap();
            client
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            let shutdown = Arc::new(AtomicBool::new(false));
            let token = shutdown.clone();
            let (done, ended) = mpsc::channel();
            let handler = std::thread::spawn(move || {
                let result = write_service_response(
                    &mut server,
                    &ApiResponse::error("x".repeat(128 * 1024)),
                    &token,
                );
                done.send((result, server.write_timeout().unwrap()))
                    .unwrap();
            });
            // Confirm a partial frame, then leave the socket connected and non-reading.
            client.read_exact(&mut [0u8; 1]).unwrap();
            if cancel {
                shutdown.store(true, Ordering::Release);
            }
            let result = ended.recv_timeout(std::time::Duration::from_secs(2));
            if result.is_err() {
                client.shutdown(std::net::Shutdown::Both).unwrap();
            }
            handler.join().unwrap();
            let (result, timeout) = result.unwrap();
            assert_eq!(
                result.unwrap_err().kind(),
                if cancel {
                    std::io::ErrorKind::ConnectionAborted
                } else {
                    std::io::ErrorKind::TimedOut
                }
            );
            assert_eq!(timeout, None);
        }
    }

    #[test]
    fn research_campaign_ipc_uses_existing_typed_transport() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(Mutex::new(Registry {
            runtime_store: Some(Arc::new(
                RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap(),
            )),
            ..Registry::default()
        }));
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let handler = std::thread::spawn(move || handle_connection(server, registry));
        let request = ApiRequest::ResearchCreate {
            command_id: "ipc".into(),
            title: "IPC".into(),
            objective: "Metadata".into(),
        };
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut original = None;
        for _ in 0..2 {
            let mut bytes = serde_json::to_vec(&request).unwrap();
            bytes.push(b'\n');
            client.write_all(&bytes).unwrap();
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let response: ApiResponse = serde_json::from_str(&line).unwrap();
            assert!(matches!(response, ApiResponse::Research { .. }));
            if let Some(original) = &original {
                assert_eq!(&line, original);
            }
            original = Some(line);
        }
        drop(reader);
        drop(client);
        handler.join().unwrap().unwrap();
    }

    #[test]
    fn todo_ipc_snapshot_mutation_replay_and_live_feed() {
        use tachyon_api::todo::*;
        fn exchange(
            client: &mut UnixStream,
            reader: &mut BufReader<UnixStream>,
            req: ApiRequest,
        ) -> ApiResponse {
            let mut bytes = serde_json::to_vec(&req).unwrap();
            bytes.push(b'\n');
            client.write_all(&bytes).unwrap();
            receive(reader)
        }
        fn receive(reader: &mut BufReader<UnixStream>) -> ApiResponse {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            serde_json::from_str(&line).unwrap()
        }
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(Mutex::new(Registry {
            runtime_store: Some(Arc::new(
                RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap(),
            )),
            ..Registry::default()
        }));
        let connect = || {
            let (client, server) = UnixStream::pair().unwrap();
            client
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let registry = registry.clone();
            let handler = std::thread::spawn(move || handle_connection(server, registry));
            let reader = BufReader::new(client.try_clone().unwrap());
            (client, reader, handler)
        };
        let (mut client, mut reader, handler) = connect();
        let scope = TodoScope::Conversation {
            id: "public-chat".into(),
        };
        let ApiResponse::Todo {
            response:
                TodoResponse::List {
                    watermark: initial,
                    todos,
                    ..
                },
        } = exchange(
            &mut client,
            &mut reader,
            ApiRequest::TodoSnapshot {
                scope: scope.clone(),
                limit: None,
                cursor: None,
            },
        )
        else {
            panic!()
        };
        assert!(todos.is_empty());
        let add = ApiRequest::Todo(TodoRequest::Add {
            scope: scope.clone(),
            command_id: "public-add".into(),
            expected_revision: 0,
            title: "task".into(),
            description: String::new(),
        });
        let ApiResponse::Todo { response: first } = exchange(&mut client, &mut reader, add.clone())
        else {
            panic!()
        };
        let TodoResponse::Mutation {
            todo, watermark, ..
        } = &first
        else {
            panic!()
        };
        assert_eq!(
            todo.created_by,
            TodoActor {
                source: "operator".into(),
                actor: format!("uid:{}", nix::unistd::geteuid())
            }
        );
        let ApiResponse::Todo { response: replay } = exchange(&mut client, &mut reader, add) else {
            panic!()
        };
        assert_eq!(first, replay);
        for scope in [
            TodoScope::Work {
                work_id: "missing".into(),
            },
            TodoScope::Campaign {
                campaign_id: "missing".into(),
            },
        ] {
            assert!(matches!(
                exchange(
                    &mut client,
                    &mut reader,
                    ApiRequest::TodoSnapshot {
                        scope,
                        limit: None,
                        cursor: None
                    }
                ),
                ApiResponse::TodoError {
                    error: TodoError::AuthorityDenied
                }
            ));
        }
        let (mut feed, mut feed_reader, feed_handler) = connect();
        let ApiResponse::OperationalBatch { batch } = exchange(
            &mut feed,
            &mut feed_reader,
            ApiRequest::OperationalSubscribe {
                scope: scope.clone(),
                after: initial,
            },
        ) else {
            panic!()
        };
        assert_eq!(batch.events.len(), 1);
        assert_eq!(&batch.watermark, watermark);
        let ApiResponse::Todo {
            response: TodoResponse::Mutation {
                watermark: updated, ..
            },
        } = exchange(
            &mut client,
            &mut reader,
            ApiRequest::Todo(TodoRequest::Update {
                scope: scope.clone(),
                command_id: "public-update".into(),
                id: todo.id.clone(),
                expected_revision: 1,
                title: None,
                description: None,
                status: Some(TodoStatus::Completed),
            }),
        )
        else {
            panic!()
        };
        loop {
            let ApiResponse::OperationalBatch { batch } = receive(&mut feed_reader) else {
                panic!()
            };
            if batch.events.is_empty() {
                continue;
            }
            assert_eq!(batch.watermark, updated);
            assert_eq!(batch.events.len(), 1);
            break;
        }
        let mut stale = updated;
        stale.instance_id = uuid::Uuid::new_v4().to_string();
        let (mut bad, mut bad_reader, bad_handler) = connect();
        assert!(matches!(
            exchange(
                &mut bad,
                &mut bad_reader,
                ApiRequest::OperationalSubscribe {
                    scope,
                    after: stale
                }
            ),
            ApiResponse::TodoError {
                error: TodoError::CursorStale
            }
        ));
        bad_handler.join().unwrap().unwrap();
        drop(feed_reader);
        drop(feed);
        assert!(feed_handler.join().unwrap().is_err());
        drop(reader);
        drop(client);
        handler.join().unwrap().unwrap();
    }
    #[test]
    fn absolute_local_deadlines_distinguish_today_tomorrow_and_next() {
        let now = Local
            .with_ymd_and_hms(2026, 1, 10, 13, 0, 0)
            .single()
            .unwrap();
        let now_ms = now.timestamp_millis() as u64;
        assert!(schedule_due_at_ms(now_ms, None, Some("12:00"), Some(ScheduleDay::Today)).is_err());

        let next_ms =
            schedule_due_at_ms(now_ms, None, Some("12:00"), Some(ScheduleDay::Next)).unwrap();
        let next = Local.timestamp_millis_opt(next_ms as i64).single().unwrap();
        assert_eq!(next.date_naive().to_string(), "2026-01-11");
        assert_eq!(next.format("%H:%M").to_string(), "12:00");

        let tomorrow_ms =
            schedule_due_at_ms(now_ms, None, Some("08:00"), Some(ScheduleDay::Tomorrow)).unwrap();
        let tomorrow = Local
            .timestamp_millis_opt(tomorrow_ms as i64)
            .single()
            .unwrap();
        assert_eq!(tomorrow.date_naive().to_string(), "2026-01-11");
        assert_eq!(tomorrow.format("%H:%M").to_string(), "08:00");
    }

    #[test]
    fn interaction_executable_resolution_preserves_names_and_overrides() {
        let exe = std::env::current_exe().unwrap();
        if let Ok(mode) = std::env::var("TACHYON_TEST_INTERACTION_RESOLUTION") {
            for (actual, name) in [
                (foreground_path(), "tachyon-foreground"),
                (background_path(), "tachyon-background"),
            ] {
                let expected = if mode == "sibling" {
                    exe.parent().unwrap().join(name)
                } else {
                    std::path::PathBuf::from(format!("/test overrides/{name}"))
                };
                assert_eq!(actual, expected);
            }
            assert_eq!(foreground_command().get_program(), foreground_path());
            return;
        }
        // Isolate environment changes from parallel host tests; launch only this test.
        for mode in ["sibling", "override"] {
            let mut command = Command::new(&exe);
            command
                .args([
                    "--exact",
                    "tests::interaction_executable_resolution_preserves_names_and_overrides",
                ])
                .env("TACHYON_TEST_INTERACTION_RESOLUTION", mode)
                .env_remove("TACHYON_FOREGROUND_BIN")
                .env_remove("TACHYON_BACKGROUND_BIN");
            if mode == "override" {
                command
                    .env(
                        "TACHYON_FOREGROUND_BIN",
                        "/test overrides/tachyon-foreground",
                    )
                    .env(
                        "TACHYON_BACKGROUND_BIN",
                        "/test overrides/tachyon-background",
                    );
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "{output:?}");
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
    }

    #[test]
    fn daemon_launches_foreground_with_a_fresh_checkpoint() {
        let command = foreground_command();
        let args = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(&args[..3], &["--agent-id", FOREGROUND_ID, "--new-session"]);
    }

    pub(super) fn task(id: &str, state: AgentState) -> Task {
        Task {
            info: AgentInfo {
                id: id.into(),
                task: id.into(),
                state,
                pid: None,
                workspace: String::new(),
                created_secs: 0,
                retained: false,
                lease_until_secs: None,
                session_id: id.into(),
                lifetime_class: Default::default(),
                purpose: id.into(),
                owner: "test".into(),
                last_activity_secs: 0,
                checkpoint_available: false,
                turns_used: 0,
                turn_budget: None,
                task_type: "test".into(),
                description: id.into(),
                persistent: false,
                sandboxed: false,
                stage_until_secs: None,
                logical_task_id: None,
                origin_turn_id: None,
                parent_task_id: None,
                tool_call_id: None,
            },
            depends_on: Vec::new(),
            process: None,
            stdin: None,
            subs: Vec::new(),
            generation: 0,
            assignment: 0,
            warm: false,
            ready: false,
            owner: None,
            last_used_secs: 0,
            control_socket: None,
            terminal_usage: None,
            terminal_result: None,
        }
    }

    fn review_registry(
        lifetime_class: LifetimeClass,
    ) -> (Arc<Mutex<Registry>>, mpsc::Receiver<CoordinatorRequest>) {
        let (coordinator_tx, coordinator_rx) = mpsc::sync_channel(4);
        let mut registry = Registry {
            background_generation: 7,
            coordinator_tx: Some(coordinator_tx),
            ..Registry::default()
        };
        let mut worker = task("worker", AgentState::Running);
        worker.warm = true;
        worker.info.retained = true;
        worker.info.lifetime_class = lifetime_class;
        worker.info.turns_used = 2;
        worker.info.turn_budget = Some(3);
        worker.info.logical_task_id = Some("work-1".into());
        let info = worker.info.clone();
        registry.tasks.insert("worker".into(), worker);
        registry.works.insert(
            "work-1".into(),
            WorkRecord {
                request: WorkRequest {
                    context_refs: vec![],
                    constraints: None,
                    attempt: None,
                    work_id: "work-1".into(),
                    objective: "inspect".into(),
                    generation: 0,
                    assignment: 0,
                    deadline_ms: unix_now_ms() + 60_000,
                    lifetime_class,
                },
                fingerprint: "fingerprint".into(),
                worker_id: "worker".into(),
                info,
                review: None,
                terminal_result: None,
                subs: Vec::new(),
            },
        );
        (Arc::new(Mutex::new(registry)), coordinator_rx)
    }

    fn completed_candidate() -> EventEnvelope {
        EventEnvelope {
            event_id: 1,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: None,
            task_id: Some("work-1".into()),
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::Actor::Worker {
                id: "worker".into(),
            },
            sequence: 1,
            occurred_at_ms: unix_now_ms(),
            kind: StructuredAgentEvent::WorkCandidate {
                candidate: WorkResult {
                    attempt_id: None,
                    work_id: "work-1".into(),
                    candidate_refs: None,
                    final_context: None,
                    instruction_revision: None,
                    evidence: Default::default(),
                    timing: None,
                    objective: "inspect".into(),
                    generation: 0,
                    assignment: 0,
                    outcome: WorkOutcome::Completed {
                        result: "verified evidence".into(),
                        artifacts: Vec::new(),
                        context: "context".into(),
                        suggested_reuse: true,
                    },
                },
            },
        }
    }

    #[test]
    fn dependencies_require_completed_tasks() {
        let mut registry = Registry::default();
        registry
            .tasks
            .insert("done".into(), task("done", AgentState::Completed));
        registry
            .tasks
            .insert("running".into(), task("running", AgentState::Running));

        assert!(dependencies_satisfied(&registry, &["done".into()]));
        assert!(!dependencies_satisfied(&registry, &["running".into()]));
        assert!(!dependencies_satisfied(&registry, &["missing".into()]));
    }

    #[test]
    fn work_idempotency_reuses_matching_requests_and_rejects_conflicts() {
        let mut registry = Registry::default();
        let info = task("worker", AgentState::Running).info;
        registry.works.insert(
            "work-1".into(),
            WorkRecord {
                request: WorkRequest {
                    context_refs: vec![],
                    constraints: None,
                    attempt: None,
                    work_id: "work-1".into(),
                    objective: "inspect".into(),
                    generation: 0,
                    assignment: 0,
                    deadline_ms: 100,
                    lifetime_class: LifetimeClass::Short,
                },
                fingerprint: "same".into(),
                worker_id: "worker".into(),
                info: info.clone(),
                review: None,
                terminal_result: None,
                subs: Vec::new(),
            },
        );
        assert_eq!(
            existing_work(&registry, "work-1", "same")
                .unwrap()
                .unwrap()
                .id,
            info.id
        );
        assert!(existing_work(&registry, "work-1", "different").is_err());
        assert!(existing_work(&registry, "work-2", "same")
            .unwrap()
            .is_none());
    }

    #[test]
    fn successful_warm_worker_keeps_completed_task_state() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("worker", AgentState::Running);
        worker.warm = true;
        worker.ready = true;
        worker.info.lifetime_class = LifetimeClass::Long;
        registry
            .lock()
            .unwrap()
            .tasks
            .insert("worker".into(), worker);
        let completion = serde_json::to_string(&StructuredAgentEvent::WorkerCompleted {
            worker_id: "worker".into(),
            objective: "objective".into(),
            result: "result".into(),
            artifacts: Vec::new(),
            context: String::new(),
            suggested_reuse: true,
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &completion);

        let registry = registry.lock().unwrap();
        let worker = &registry.tasks["worker"];
        assert_eq!(worker.info.state, AgentState::Completed);
        assert!(worker.info.retained);
        assert_eq!(worker.info.turns_used, 1);
        assert!(!worker.ready);
    }

    #[test]
    fn only_idle_readiness_enables_reuse() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        registry
            .lock()
            .unwrap()
            .tasks
            .insert("worker".into(), task("worker", AgentState::Completed));
        for (message, expected) in [("startup", false), ("idle", true)] {
            let status = serde_json::to_string(&StructuredAgentEvent::Status {
                turn: None,
                phase: "ready".into(),
                message: message.into(),
            })
            .unwrap();
            push_event(&registry, "worker", EventStream::Stdout, &status);
            assert_eq!(registry.lock().unwrap().tasks["worker"].ready, expected);
        }
    }

    #[test]
    fn typed_pool_claim_is_atomic_and_clears_old_assignment() {
        let directory = tempfile::tempdir().unwrap();
        let cwd = directory.path().to_string_lossy().into_owned();
        let mut registry = Registry::default();
        let mut worker = task("worker", AgentState::Completed);
        worker.info.workspace = cwd.clone();
        worker.warm = true;
        worker.ready = true;
        worker.info.retained = true;
        worker.info.owner = "background".into();
        worker.info.lifetime_class = LifetimeClass::Long;
        worker.info.task_type = "research".into();
        worker.control_socket = Some("/tmp/test-worker.sock".into());
        worker.terminal_usage = Some("old usage".into());
        worker.terminal_result = Some("old result".into());
        registry.tasks.insert("worker".into(), worker);

        let unrelated = tempfile::tempdir().unwrap();
        let unrelated = unrelated.path().to_string_lossy().into_owned();
        for selected in [None, Some(&unrelated)] {
            assert!(claim_reusable_worker(
                &mut registry,
                "wrong workspace",
                "Research",
                LifetimeClass::Long,
                selected,
                &[],
                &None,
                &None,
                &None,
                &None,
            )
            .is_none());
            assert!(registry.tasks["worker"].ready);
            assert_eq!(registry.tasks["worker"].info.state, AgentState::Completed);
        }

        // A different spelling of the same canonical identity is compatible.
        let cwd = format!("{cwd}/.");

        let claimed = claim_reusable_worker(
            &mut registry,
            "new objective",
            "Research",
            LifetimeClass::Long,
            Some(&cwd),
            &[],
            &Some("task-2".into()),
            &Some("2".into()),
            &None,
            &Some("call-2".into()),
        )
        .expect("compatible retained worker should be claimed");
        assert_eq!(claimed.id, "worker");
        assert_eq!(claimed.state, AgentState::Running);
        assert_eq!(claimed.logical_task_id.as_deref(), Some("task-2"));
        assert_eq!(claimed.task_type, "research");
        assert!(registry.tasks["worker"].terminal_usage.is_none());
        assert!(registry.tasks["worker"].terminal_result.is_none());

        assert!(claim_reusable_worker(
            &mut registry,
            "another objective",
            "Research",
            LifetimeClass::Long,
            Some(&cwd),
            &[],
            &None,
            &None,
            &None,
            &None,
        )
        .is_none());
    }

    #[test]
    fn managed_root_startup_provisioning_is_idempotent_and_honors_configuration() {
        use tachyon_util::daemon::{
            ensure_managed_agent_root, managed_agent_root, provision_managed_workspace,
        };
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("nested/custom-agents");
        let config = tachyon_util::config::Config {
            managed_agent_root: Some(root.clone()),
            ..Default::default()
        };
        let configured = managed_agent_root(&config).unwrap();
        assert_eq!(configured, root);
        assert!(!root.exists());
        let ensured = ensure_managed_agent_root(&configured).unwrap();
        assert_eq!(ensured, root.canonicalize().unwrap());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        let workspace = provision_managed_workspace(&root, "worker-1").unwrap();
        for path in ["research/notes.md", "artifacts/report.md"] {
            std::fs::write(workspace.join(path), "keep").unwrap();
        }
        assert_eq!(ensure_managed_agent_root(&configured).unwrap(), ensured);
        for path in ["research/notes.md", "artifacts/report.md"] {
            assert_eq!(
                std::fs::read_to_string(workspace.join(path)).unwrap(),
                "keep"
            );
        }
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    }

    #[test]
    fn managed_root_startup_provisioning_reports_invalid_and_file_paths() {
        use std::path::PathBuf;
        use tachyon_util::daemon::{ensure_managed_agent_root, managed_agent_root};
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, "keep").unwrap();
        for root in [
            PathBuf::from("relative"),
            PathBuf::from("/"),
            file.clone(),
            file.join("nested"),
        ] {
            let config = tachyon_util::config::Config {
                managed_agent_root: Some(root.clone()),
                ..Default::default()
            };
            let error = managed_agent_root(&config)
                .and_then(|root| ensure_managed_agent_root(&root))
                .unwrap_err();
            if root.is_absolute() && root.parent().is_some() {
                assert!(
                    error.contains("managed root provisioning failed"),
                    "{error}"
                );
            } else {
                assert!(error.contains("absolute non-root directory"), "{error}");
                assert!(ensure_managed_agent_root(&root).is_err());
            }
        }
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "keep");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn workspace_validation_and_managed_provisioning_are_scoped_and_distinct() {
        use tachyon_util::daemon::{
            managed_agent_root, provision_managed_workspace, selected_workspace,
        };
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("Agents");
        let config = tachyon_util::config::Config {
            managed_agent_root: Some(root.clone()),
            ..Default::default()
        };
        assert_eq!(managed_agent_root(&config).unwrap(), root);
        assert!(!root.exists());
        let local = selected_workspace(directory.path().to_str().unwrap()).unwrap();
        assert!(!local.join("research").exists());
        assert!(!root.exists());
        let workspace = provision_managed_workspace(&root, "worker-1").unwrap();
        assert_eq!(
            workspace.parent(),
            Some(root.canonicalize().unwrap().as_path())
        );
        assert!(workspace.join("research").is_dir());
        assert!(workspace.join("artifacts").is_dir());
        std::fs::write(workspace.join("artifacts/keep.txt"), "keep").unwrap();
        assert!(provision_managed_workspace(&root, "worker-1")
            .unwrap_err()
            .contains("managed workspace provisioning failed"));
        assert_eq!(
            std::fs::read_to_string(workspace.join("artifacts/keep.txt")).unwrap(),
            "keep"
        );
        assert!(provision_managed_workspace(&root, "../escape")
            .unwrap_err()
            .contains("invalid managed workspace id"));
        assert!(selected_workspace("relative")
            .unwrap_err()
            .contains("absolute path"));
        assert!(selected_workspace("/")
            .unwrap_err()
            .contains("filesystem root"));
        assert!(selected_workspace(root.join("missing").to_str().unwrap())
            .unwrap_err()
            .contains("workspace unavailable"));
        assert!(
            selected_workspace(workspace.join("artifacts/keep.txt").to_str().unwrap())
                .unwrap_err()
                .contains("not a directory")
        );
        let file = directory.path().join("file");
        std::fs::write(&file, "file").unwrap();
        assert!(provision_managed_workspace(&file, "worker-2")
            .unwrap_err()
            .contains("managed root provisioning failed"));
        #[cfg(unix)]
        {
            let alias = directory.path().join("alias");
            std::os::unix::fs::symlink(&workspace, &alias).unwrap();
            assert_eq!(
                selected_workspace(alias.to_str().unwrap()).unwrap(),
                workspace
            );
            std::os::unix::fs::symlink(&workspace, root.join("worker-3")).unwrap();
            assert!(provision_managed_workspace(&root, "worker-3").is_err());
        }
    }

    #[test]
    fn foreground_workspace_errors_are_not_delivery_errors() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let directory = tempfile::tempdir().unwrap();
        for (cwd, expected) in [
            (
                Some("relative".into()),
                "workspace selection must be an absolute path",
            ),
            (
                Some(directory.path().to_string_lossy().into_owned()),
                "no such agent",
            ),
            (None, "no such agent"),
        ] {
            let response = dispatch(
                &ApiRequest::ForegroundChat {
                    text: "hello".into(),
                    cwd,
                },
                &registry,
            );
            let ApiResponse::Error { message, .. } = response else {
                panic!("expected error");
            };
            assert!(message.contains(expected), "{message}");
        }
        assert!(registry.lock().unwrap().tasks.is_empty());
    }

    #[test]
    fn short_workers_are_released_after_three_completed_assignments() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("short", AgentState::Running);
        worker.warm = true;
        worker.info.retained = true;
        worker.info.lifetime_class = LifetimeClass::Short;
        worker.info.turn_budget = Some(3);
        registry
            .lock()
            .unwrap()
            .tasks
            .insert("short".into(), worker);
        let completion = serde_json::to_string(&StructuredAgentEvent::WorkerCompleted {
            worker_id: "short".into(),
            objective: "work".into(),
            result: "done".into(),
            artifacts: Vec::new(),
            context: String::new(),
            suggested_reuse: true,
        })
        .unwrap();

        for expected_turns in 1..=3 {
            push_event(&registry, "short", EventStream::Stdout, &completion);
            let reg = registry.lock().unwrap();
            let worker = &reg.tasks["short"];
            assert_eq!(worker.info.turns_used, expected_turns);
            if expected_turns < 3 {
                assert_eq!(worker.info.state, AgentState::Completed);
                assert!(worker.info.retained);
            } else {
                assert_eq!(worker.info.state, AgentState::Released);
                assert!(!worker.info.retained);
            }
        }
    }

    #[test]
    fn review_timing_is_separate_from_execution_for_accept_rework_and_no_provider() {
        for recommendation in [
            WorkReviewRecommendation::Accept {
                lifecycle: LifecycleRecommendation::KeepCurrent,
            },
            WorkReviewRecommendation::Rework {
                revised_objective: None,
            },
            WorkReviewRecommendation::Inconclusive {
                failure: WorkReviewFailure::ModelUnavailable,
            },
        ] {
            let (registry, _review_rx) = review_registry(LifetimeClass::Long);
            let rx = registry.lock().unwrap().subscribe_work("work-1").unwrap();
            let mut candidate = completed_candidate();
            let StructuredAgentEvent::WorkCandidate { candidate: result } = &mut candidate.kind
            else {
                unreachable!()
            };
            result.timing = Some(tachyon_api::WorkTiming {
                execution_ms: Some(1000),
                inference_ms: Some(600),
                tool_ms: Some(300),
                review_ms: Some(99999),
            });
            handle_work_candidate(&registry, "worker", candidate);
            let review = registry.lock().unwrap().works["work-1"]
                .review
                .clone()
                .unwrap();
            assert_eq!(
                review.request.candidate.timing.as_ref().unwrap().review_ms,
                None
            );
            let request = review.request;
            apply_work_review_at(
                &registry,
                WorkReviewDecision {
                    review_id: request.review_id,
                    coordinator_generation: request.coordinator_generation,
                    work_id: request.candidate.work_id,
                    generation: request.candidate.generation,
                    assignment: request.candidate.assignment,
                    recommendation: recommendation.clone(),
                    rationale: "fixture".into(),
                },
                request.deadline_ms - 1,
                review.started + std::time::Duration::from_millis(250),
            );
            let envelope: EventEnvelope = serde_json::from_str(&rx.recv().unwrap().data).unwrap();
            assert_eq!(envelope.task_id.as_deref(), Some("work-1"));
            let StructuredAgentEvent::WorkResult { result } = envelope.kind else {
                unreachable!()
            };
            assert_eq!((result.generation, result.assignment), (0, 0));
            assert_eq!(
                matches!(result.outcome, WorkOutcome::Completed { .. }),
                matches!(recommendation, WorkReviewRecommendation::Accept { .. })
            );
            assert_eq!(
                result.timing,
                Some(tachyon_api::WorkTiming {
                    execution_ms: Some(1000),
                    inference_ms: Some(600),
                    tool_ms: Some(300),
                    review_ms: Some(250),
                })
            );
        }
    }

    #[test]
    fn unavailable_coordinator_records_only_observed_review_wait() {
        let (registry, _review_rx) = review_registry(LifetimeClass::Long);
        registry.lock().unwrap().coordinator_tx = None;
        let rx = registry.lock().unwrap().subscribe_work("work-1").unwrap();
        handle_work_candidate(&registry, "worker", completed_candidate());
        let envelope: EventEnvelope = serde_json::from_str(&rx.recv().unwrap().data).unwrap();
        let StructuredAgentEvent::WorkResult { result } = envelope.kind else {
            unreachable!()
        };
        assert!(matches!(result.outcome, WorkOutcome::Failed { .. }));
        let timing = result.timing.unwrap();
        assert!(timing.review_ms.is_some());
        assert_eq!(timing.execution_ms, None);
        assert_eq!(timing.inference_ms, None);
        assert_eq!(timing.tool_ms, None);
    }

    #[test]
    fn completed_candidate_requires_a_fenced_review_before_becoming_terminal() {
        let (registry, _review_rx) = review_registry(LifetimeClass::Short);
        let rx = registry.lock().unwrap().subscribe_work("work-1").unwrap();
        handle_work_candidate(&registry, "worker", completed_candidate());
        assert!(rx.try_recv().is_err());

        let request = registry.lock().unwrap().works["work-1"]
            .review
            .as_ref()
            .unwrap()
            .request
            .clone();
        apply_work_review(
            &registry,
            WorkReviewDecision {
                review_id: request.review_id.clone(),
                coordinator_generation: request.coordinator_generation - 1,
                work_id: request.candidate.work_id.clone(),
                generation: request.candidate.generation,
                assignment: request.candidate.assignment,
                recommendation: WorkReviewRecommendation::Accept {
                    lifecycle: LifecycleRecommendation::Retain {
                        lifetime_class: LifetimeClass::Long,
                    },
                },
                rationale: "stale".into(),
            },
        );
        assert!(rx.try_recv().is_err());
        assert!(registry.lock().unwrap().works["work-1"]
            .terminal_result
            .is_none());

        apply_work_review(
            &registry,
            WorkReviewDecision {
                review_id: request.review_id,
                coordinator_generation: request.coordinator_generation,
                work_id: request.candidate.work_id,
                generation: request.candidate.generation,
                assignment: request.candidate.assignment,
                recommendation: WorkReviewRecommendation::Accept {
                    lifecycle: LifecycleRecommendation::Retain {
                        lifetime_class: LifetimeClass::Long,
                    },
                },
                rationale: "sufficient evidence".into(),
            },
        );
        let event = rx.recv().unwrap();
        let envelope: EventEnvelope = serde_json::from_str(&event.data).unwrap();
        assert!(matches!(
            envelope.kind,
            StructuredAgentEvent::WorkResult {
                result: WorkResult {
                    outcome: WorkOutcome::Completed { .. },
                    ..
                }
            }
        ));
        let reg = registry.lock().unwrap();
        assert_eq!(reg.tasks["worker"].info.state, AgentState::Completed);
        assert_eq!(reg.tasks["worker"].info.lifetime_class, LifetimeClass::Long);
        assert_eq!(reg.tasks["worker"].info.turn_budget, None);
        assert!(reg.tasks["worker"].info.retained);
    }

    #[test]
    fn review_after_old_timeout_before_current_deadline_is_accepted_and_fenced() {
        let (registry, _review_rx) = review_registry(LifetimeClass::Short);
        let rx = registry.lock().unwrap().subscribe_work("work-1").unwrap();
        handle_work_candidate(&registry, "worker", completed_candidate());

        let started_ms = 1_000;
        let deadline_ms =
            background_review_deadline_ms(started_ms, background_review_timeout_from(None));
        let request = {
            let mut reg = registry.lock().unwrap();
            let review = reg
                .works
                .get_mut("work-1")
                .unwrap()
                .review
                .as_mut()
                .unwrap();
            review.request.deadline_ms = deadline_ms;
            review.request.clone()
        };
        assert_eq!(deadline_ms, 21_000);

        let mut decision = WorkReviewDecision {
            review_id: request.review_id.clone(),
            coordinator_generation: request.coordinator_generation,
            work_id: request.candidate.work_id.clone(),
            generation: request.candidate.generation,
            assignment: request.candidate.assignment + 1,
            recommendation: WorkReviewRecommendation::Accept {
                lifecycle: LifecycleRecommendation::KeepCurrent,
            },
            rationale: "sufficient evidence".into(),
        };
        apply_work_review_at(
            &registry,
            decision.clone(),
            started_ms + 10_001,
            std::time::Instant::now(),
        );
        assert!(rx.try_recv().is_err());
        assert!(registry.lock().unwrap().works["work-1"].review.is_some());

        decision.assignment = request.candidate.assignment;
        apply_work_review_at(
            &registry,
            decision,
            started_ms + 10_001,
            std::time::Instant::now(),
        );
        let event = rx.recv().unwrap();
        let envelope: EventEnvelope = serde_json::from_str(&event.data).unwrap();
        assert!(matches!(
            envelope.kind,
            StructuredAgentEvent::WorkResult {
                result: WorkResult {
                    outcome: WorkOutcome::Completed { .. },
                    ..
                }
            }
        ));
        assert!(registry.lock().unwrap().works["work-1"].review.is_none());
    }

    #[test]
    fn background_review_timeout_default_and_override_are_stable() {
        assert_eq!(
            background_review_timeout_from(None),
            std::time::Duration::from_secs(20)
        );
        assert_eq!(
            background_review_timeout_from(Some("8")),
            std::time::Duration::from_secs(8)
        );
    }

    #[test]
    fn rework_decision_fails_closed_without_publishing_candidate_evidence() {
        let (registry, _review_rx) = review_registry(LifetimeClass::Long);
        let rx = registry.lock().unwrap().subscribe_work("work-1").unwrap();
        handle_work_candidate(&registry, "worker", completed_candidate());
        let request = registry.lock().unwrap().works["work-1"]
            .review
            .as_ref()
            .unwrap()
            .request
            .clone();
        let decision = WorkReviewDecision {
            review_id: request.review_id,
            coordinator_generation: request.coordinator_generation,
            work_id: request.candidate.work_id,
            generation: request.candidate.generation,
            assignment: request.candidate.assignment,
            recommendation: WorkReviewRecommendation::Rework {
                revised_objective: Some("collect primary evidence".into()),
            },
            rationale: "only secondary evidence was supplied".into(),
        };
        apply_work_review(&registry, decision.clone());
        apply_work_review(&registry, decision);

        let event = rx.recv().unwrap();
        assert!(rx.try_recv().is_err());
        let envelope: EventEnvelope = serde_json::from_str(&event.data).unwrap();
        let StructuredAgentEvent::WorkResult { result } = envelope.kind else {
            panic!("expected terminal work result");
        };
        assert!(matches!(result.outcome, WorkOutcome::Failed { .. }));
        assert!(!event.data.contains("verified evidence"));
        let reg = registry.lock().unwrap();
        assert_eq!(reg.tasks["worker"].info.state, AgentState::Failed);
        assert_eq!(reg.tasks["worker"].generation, 1);
    }

    #[test]
    fn timed_out_work_fails_and_fences_the_worker_generation() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("worker", AgentState::Running);
        worker.warm = true;
        worker.info.retained = true;
        worker.info.logical_task_id = Some("work-timeout".into());
        let info = worker.info.clone();
        let request = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: "work-timeout".into(),
            objective: "inspect".into(),
            generation: 0,
            assignment: 0,
            deadline_ms: 100,
            lifetime_class: LifetimeClass::Long,
        };
        {
            let mut reg = registry.lock().unwrap();
            reg.tasks.insert("worker".into(), worker);
            reg.works.insert(
                request.work_id.clone(),
                WorkRecord {
                    request: request.clone(),
                    fingerprint: "fingerprint".into(),
                    worker_id: "worker".into(),
                    info,
                    review: None,
                    terminal_result: None,
                    subs: Vec::new(),
                },
            );
        }
        let timeout = serde_json::to_string(&StructuredAgentEvent::WorkResult {
            result: WorkResult {
                attempt_id: None,
                evidence: Default::default(),
                candidate_refs: None,
                final_context: None,
                instruction_revision: None,
                timing: None,
                work_id: request.work_id,
                objective: request.objective,
                generation: request.generation,
                assignment: request.assignment,
                outcome: WorkOutcome::TimedOut {
                    deadline_ms: request.deadline_ms,
                },
            },
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &timeout);

        let reg = registry.lock().unwrap();
        let worker = &reg.tasks["worker"];
        assert_eq!(worker.info.state, AgentState::Failed);
        assert!(!worker.info.retained);
        assert_eq!(worker.generation, 1);
        assert!(reg.works["work-timeout"].terminal_result.is_some());
    }

    #[test]
    fn only_persistent_workers_survive_daemon_shutdown() {
        assert!(!survives_daemon_shutdown(LifetimeClass::Short));
        assert!(!survives_daemon_shutdown(LifetimeClass::Long));
        assert!(survives_daemon_shutdown(LifetimeClass::Persistent));
    }

    #[test]
    fn agent_list_includes_memory_service_once() {
        let agents = Registry::default().sorted();
        assert_eq!(
            agents.iter().filter(|agent| agent.id == MEMORY_ID).count(),
            1
        );
        assert_eq!(agents[0].task_type, "memory");
    }

    #[test]
    fn typed_memory_mutation_commits_before_reporting_success() {
        let directory = tempfile::tempdir().unwrap();
        let memory = Arc::new(MemoryStore::open(directory.path().join("memories.redb")).unwrap());
        let registry = Arc::new(Mutex::new(Registry {
            memory_store: Some(Arc::clone(&memory)),
            ..Registry::default()
        }));
        let response = dispatch(
            &ApiRequest::MemoryMutate {
                intent: MemoryIntent::Remember {
                    descriptor: tachyon_api::types::MemoryDescriptor::default(),
                    value: "likes pickles".into(),
                },
                source_event_id: "message-1".into(),
                conversation_id: "conversation-1".into(),
                turn: 1,
                occurred_at_ms: 123,
            },
            &registry,
        );
        assert!(matches!(
            response,
            ApiResponse::MemoryMutation {
                result: MemoryMutationResult::Applied {
                    kind: MemoryMutationKind::Remember,
                    ..
                }
            }
        ));
        let records = memory.list().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, "likes pickles");
    }

    #[test]
    fn memory_recall_combines_preferences_and_prompted_history() {
        let directory = tempfile::tempdir().unwrap();
        let memory = Arc::new(MemoryStore::open(directory.path().join("memories.redb")).unwrap());
        memory
            .apply_intent(
                &MemoryIntent::Remember {
                    descriptor: tachyon_api::types::MemoryDescriptor::default(),
                    value: "prefers concise Rust answers".into(),
                },
                MemoryMutationSource {
                    event_id: "preference",
                    conversation_id: "conversation-1",
                    turn_id: 1,
                    occurred_at_ms: 100,
                },
            )
            .unwrap();
        let history = Arc::new(HistoryStore::open(&directory.path().join("history.redb")).unwrap());
        history
            .apply(&HistoryProjection {
                schema_version: 1,
                event_id: "history".into(),
                kind: HistoryKind::Conversation,
                conversation_id: "conversation-1".into(),
                turn_id: Some("2".into()),
                occurred_at_ms: 200,
                role: HistoryRole::Assistant,
                text: "Implemented the Rust database layer.".into(),
                task_id: None,
                task_state: None,
            })
            .unwrap();
        let registry = Arc::new(Mutex::new(Registry {
            memory_store: Some(memory),
            history_store: Some(history),
            runtime_store: Some(Arc::new(
                RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap(),
            )),
            ..Registry::default()
        }));
        let mut prior_task = task("prior-task", AgentState::Completed);
        prior_task.info.task = "Build the Rust database layer".into();
        let prior_info = prior_task.info.clone();
        registry
            .lock()
            .unwrap()
            .tasks
            .insert(prior_info.id.clone(), prior_task);
        persist_task(&registry, &prior_info, "completed");
        let response = dispatch(
            &ApiRequest::MemoryRecall {
                query: "What did we work on in the Rust history?".into(),
                conversation_id: "conversation-1".into(),
                turn: 3,
                include_history: true,
                max_items: 4,
                max_chars: 1000,
            },
            &registry,
        );
        let ApiResponse::MemoryRecall { items, truncated } = response else {
            panic!("expected memory recall response");
        };
        assert!(!truncated);
        assert_eq!(items.len(), 3);
        assert!(items
            .iter()
            .any(|item| item.kind == MemoryRecallKind::Preference));
        assert!(items
            .iter()
            .any(|item| item.kind == MemoryRecallKind::History));
        assert!(items
            .iter()
            .any(|item| item.kind == MemoryRecallKind::TaskHistory));

        let response = dispatch(
            &ApiRequest::MemoryRecall {
                query: "concise Rust answers".into(),
                conversation_id: "conversation-1".into(),
                turn: 4,
                include_history: false,
                max_items: 4,
                max_chars: 1000,
            },
            &registry,
        );
        let ApiResponse::MemoryRecall { items, .. } = response else {
            panic!("expected memory recall response");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].kind, MemoryRecallKind::Preference);
    }

    #[test]
    fn relative_history_windows_are_bounded_to_the_requested_day() {
        const DAY_MS: u64 = 86_400_000;
        assert_eq!(
            history_recall_window("what did we do yesterday?", 10 * DAY_MS + 123),
            (9 * DAY_MS, 10 * DAY_MS)
        );
    }

    #[test]
    fn reminder_dispatch_commits_lists_and_cancels() {
        let directory = tempfile::tempdir().unwrap();
        let (coordinator_tx, coordinator_rx) = mpsc::sync_channel(4);
        std::thread::spawn(move || {
            while let Ok(command) = coordinator_rx.recv() {
                if let CoordinatorRequest::Schedule { request, response } = command {
                    let _ = response.send(BackgroundScheduleDecision {
                        request_id: request.request_id,
                        action: request.action,
                        approved: true,
                        reason: "validated".into(),
                    });
                }
            }
        });
        let registry = Arc::new(Mutex::new(Registry {
            coordinator_tx: Some(coordinator_tx),
            runtime_store: Some(Arc::new(
                RuntimeStore::open(&directory.path().join("runtime.redb")).unwrap(),
            )),
            ..Registry::default()
        }));
        let response = dispatch(
            &ApiRequest::ReminderCreate {
                source_event_id: "source-1".into(),
                conversation_id: FOREGROUND_ID.into(),
                turn: 1,
                text: "Your coffee is ready.".into(),
                delay_seconds: Some(60),
                local_time: None,
                day: None,
                created_at_ms: 123,
            },
            &registry,
        );
        let ApiResponse::Reminder { reminder } = response else {
            panic!("expected reminder response");
        };
        assert_eq!(reminder.text, "Your coffee is ready.");
        assert!(reminder.due_at_ms > reminder.created_at_ms);

        let ApiResponse::Reminders { reminders } = dispatch(&ApiRequest::ReminderList, &registry)
        else {
            panic!("expected reminders response");
        };
        assert_eq!(reminders, [reminder.clone()]);

        let ApiResponse::Reminder { reminder } = dispatch(
            &ApiRequest::ReminderCancel {
                id: reminder.id.clone(),
                conversation_id: FOREGROUND_ID.into(),
                turn: 2,
            },
            &registry,
        ) else {
            panic!("expected cancelled reminder response");
        };
        assert_eq!(
            reminder.status,
            tachyon_api::types::ReminderStatus::Cancelled
        );
    }

    #[test]
    fn persistent_workers_restore_from_runtime_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("runtime.redb");
        {
            let registry = Arc::new(Mutex::new(Registry {
                runtime_store: Some(Arc::new(RuntimeStore::open(&path).unwrap())),
                ..Registry::default()
            }));
            let mut worker = task("persistent", AgentState::Running);
            worker.info.lifetime_class = LifetimeClass::Persistent;
            worker.info.persistent = true;
            worker.info.retained = true;
            worker.generation = 4;
            worker.assignment = 3;
            registry
                .lock()
                .unwrap()
                .tasks
                .insert("persistent".into(), worker);
            let info = registry.lock().unwrap().tasks["persistent"].info.clone();
            persist_task(&registry, &info, "running");
        }

        let registry = Arc::new(Mutex::new(Registry {
            runtime_store: Some(Arc::new(RuntimeStore::open(&path).unwrap())),
            ..Registry::default()
        }));
        restore_runtime_tasks(&registry).unwrap();
        let guard = registry.lock().unwrap();
        let restored = &guard.tasks["persistent"];
        assert_eq!(restored.info.state, AgentState::Created);
        assert_eq!(restored.generation, 4);
        assert_eq!(restored.assignment, 3);
        assert!(restored.process.is_none());
    }

    #[test]
    fn completed_worker_can_be_demoted_to_a_fresh_short_budget() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("worker", AgentState::Completed);
        worker.info.lifetime_class = LifetimeClass::Long;
        worker.info.turns_used = 9;
        registry
            .lock()
            .unwrap()
            .tasks
            .insert("worker".into(), worker);
        let response = retain_worker(&registry, "worker", None, Some(LifetimeClass::Short));
        let ApiResponse::Agent { info } = response else {
            panic!("expected agent response");
        };
        assert_eq!(info.lifetime_class, LifetimeClass::Short);
        assert_eq!(info.turn_budget, Some(3));
        assert_eq!(info.turns_used, 0);
    }

    #[test]
    fn lifecycle_states_map_to_expected_unix_signals() {
        assert_eq!(lifecycle_signal(AgentState::Terminated), Signal::SIGTERM);
        assert_eq!(lifecycle_signal(AgentState::Interrupted), Signal::SIGINT);
    }

    #[test]
    fn worker_completion_is_enriched_with_request_correlation() {
        let mut worker = task("research", AgentState::Running);
        worker.info.logical_task_id = Some("task-7".into());
        worker.info.origin_turn_id = Some("7".into());
        worker.info.parent_task_id = Some("task-parent".into());
        worker.info.tool_call_id = Some("call-7".into());
        let event = EventEnvelope {
            event_id: 1,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: None,
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::types::Actor::Worker {
                id: "worker".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: StructuredAgentEvent::WorkerCompleted {
                worker_id: "worker".into(),
                objective: "research".into(),
                result: "result".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            },
        };

        let enriched: EventEnvelope = serde_json::from_str(&correlate_event(
            &serde_json::to_string(&event).unwrap(),
            &worker.info,
        ))
        .unwrap();
        assert_eq!(enriched.task_id.as_deref(), Some("task-7"));
        assert_eq!(enriched.turn_id.as_deref(), Some("7"));
        assert_eq!(enriched.parent_task_id.as_deref(), Some("task-parent"));
        assert_eq!(enriched.tool_call_id.as_deref(), Some("call-7"));
    }

    #[test]
    fn artifact_queue_is_bounded_retains_source_and_preserves_causal_identity() {
        use tachyon_api::types::{ArtifactPublication, ArtifactRegistration};
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            storage.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        std::fs::write(workspace.path().join("report.txt"), "abc").unwrap();
        let store = artifact_store::ArtifactStore::open(storage.path()).unwrap();
        let mut worker = task("worker", AgentState::Running);
        worker.info.workspace = workspace.path().to_string_lossy().into_owned();
        worker.info.logical_task_id = Some("original-work".into());
        worker.info.origin_turn_id = Some("original-turn".into());
        worker.generation = 1;
        worker.assignment = 1;
        let info = worker.info.clone();
        let mut reg = Registry::default();
        reg.tasks.insert("worker".into(), worker);
        let events = reg.subscribe("worker").unwrap();
        let registry = Arc::new(Mutex::new(reg));
        let envelope = EventEnvelope {
            event_id: 100,
            sequence: 100,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: None,
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::Actor::Worker {
                id: "worker".into(),
            },
            occurred_at_ms: 1,
            kind: StructuredAgentEvent::ArtifactRegistered {
                artifact: ArtifactRegistration {
                    id: "first".into(),
                    path: "report.txt".into(),
                    kind: "report".into(),
                    description: "test".into(),
                    size_bytes: 3,
                    sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                        .into(),
                    task_id: Some("forged".into()),
                    work_id: Some("forged".into()),
                    generation: Some(1),
                    assignment: Some(1),
                    attempt_id: None,
                    publication: ArtifactPublication::Ready {
                        version: "forged".into(),
                    },
                },
            },
        };
        // No consumer is running: neither admission nor overflow may wait on it.
        let (tx, rx) = mpsc::sync_channel(1);
        queue_artifact(&registry, "worker", envelope.clone(), 1024, &tx);
        queue_artifact(&registry, "worker", envelope.clone(), 1024, &tx);
        let next = || {
            serde_json::from_str::<EventEnvelope>(
                &events
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap()
                    .data,
            )
            .unwrap()
        };
        for _ in 0..2 {
            assert!(
                matches!(next().kind, StructuredAgentEvent::ArtifactRegistered { artifact }
                if artifact.publication == ArtifactPublication::Pending)
            );
        }
        assert!(
            matches!(next().kind, StructuredAgentEvent::ArtifactRegistered { artifact }
            if matches!(artifact.publication, ArtifactPublication::Failed { .. }))
        );
        cleanup_workspace(&info);
        assert!(artifact_retention()
            .lock()
            .unwrap()
            .get(&info.workspace)
            .unwrap()
            .1
            .is_some());
        assert!(workspace.path().join("report.txt").exists());
        {
            let mut reg = registry.lock().unwrap();
            let worker = reg.tasks.get_mut("worker").unwrap();
            worker.assignment = 2;
            worker.info.logical_task_id = Some("replacement-work".into());
            worker.info.origin_turn_id = Some("replacement-turn".into());
        }
        queue_artifact(&registry, "worker", envelope.clone(), 1024, &tx);
        let mut unfenced = envelope;
        if let StructuredAgentEvent::ArtifactRegistered { artifact } = &mut unfenced.kind {
            artifact.generation = None;
            artifact.assignment = None;
        }
        queue_artifact(&registry, "worker", unfenced, 1024, &tx);
        assert!(events.try_recv().is_err());
        let job = rx.recv().unwrap();
        let handle = std::thread::spawn(move || {
            let mut completed = job.envelope.clone();
            if let StructuredAgentEvent::ArtifactRegistered { artifact } = &mut completed.kind {
                *artifact = store
                    .register(
                        "original-work",
                        std::path::Path::new(&job.info.workspace),
                        artifact.clone(),
                    )
                    .unwrap();
            }
            job.finish(completed);
        });
        let ready = next();
        assert_eq!(ready.session_id, "worker:artifact-publication");
        assert_eq!(ready.task_id.as_deref(), Some("original-work"));
        assert_eq!(ready.turn_id.as_deref(), Some("original-turn"));
        assert!(matches!(ready.actor, tachyon_api::Actor::System));
        assert!(
            matches!(ready.kind, StructuredAgentEvent::ArtifactRegistered { artifact }
            if artifact.work_id.as_deref() == Some("original-work")
                && matches!(artifact.publication, ArtifactPublication::Ready { .. }))
        );
        handle.join().unwrap();
        assert!(!artifact_retention()
            .lock()
            .unwrap()
            .contains_key(&info.workspace));
    }

    #[test]
    fn artifact_registration_uses_daemon_owned_task_identity() {
        let mut worker = task("research", AgentState::Running);
        worker.info.logical_task_id = Some("task-7".into());
        let event = EventEnvelope {
            event_id: 1,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: None,
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::types::Actor::Worker {
                id: "worker".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: StructuredAgentEvent::ArtifactRegistered {
                artifact: tachyon_api::types::ArtifactRegistration {
                    id: "artifact-1".into(),
                    publication: Default::default(),
                    path: "report.txt".into(),
                    kind: "report".into(),
                    description: "Report".into(),
                    size_bytes: 3,
                    sha256: "abc".into(),
                    task_id: None,
                    work_id: Some("work-1".into()),
                    generation: Some(2),
                    assignment: Some(3),
                    attempt_id: None,
                },
            },
        };

        let enriched: EventEnvelope = serde_json::from_str(&correlate_event(
            &serde_json::to_string(&event).unwrap(),
            &worker.info,
        ))
        .unwrap();
        assert_eq!(enriched.task_id.as_deref(), Some("task-7"));
        let StructuredAgentEvent::ArtifactRegistered { artifact } = enriched.kind else {
            panic!("expected artifact registration");
        };
        assert_eq!(artifact.task_id.as_deref(), Some("task-7"));
        assert_eq!(artifact.work_id.as_deref(), Some("work-1"));
    }

    #[test]
    fn tool_telemetry_keeps_local_call_and_gains_daemon_correlation() {
        let mut worker = task("research", AgentState::Running);
        worker.info.logical_task_id = Some("task-7".into());
        worker.info.origin_turn_id = Some("7".into());
        worker.info.parent_task_id = Some("task-parent".into());
        worker.info.tool_call_id = Some("delegation-call".into());
        let event = EventEnvelope {
            event_id: 1,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: None,
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::types::Actor::Worker {
                id: "worker".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: StructuredAgentEvent::ToolTelemetry {
                tool_name: "read".into(),
                call_id: Some("model-call".into()),
                duration_ms: 4,
                success: true,
                truncated: false,
                bytes_out: 12,
                error_code: None,
                identity: tachyon_api::types::ToolTelemetryIdentity {
                    task_id: None,
                    work_id: Some("work-7".into()),
                    generation: Some(1),
                    assignment: Some(2),
                    attempt_id: None,
                },
            },
        };

        let enriched: EventEnvelope = serde_json::from_str(&correlate_event(
            &serde_json::to_string(&event).unwrap(),
            &worker.info,
        ))
        .unwrap();
        assert_eq!(enriched.task_id.as_deref(), Some("task-7"));
        assert_eq!(enriched.turn_id.as_deref(), Some("7"));
        assert_eq!(enriched.parent_task_id.as_deref(), Some("task-parent"));
        assert_eq!(enriched.tool_call_id.as_deref(), Some("delegation-call"));
        assert!(matches!(
            enriched.kind,
            StructuredAgentEvent::ToolTelemetry { call_id: Some(call_id), .. }
                if call_id == "model-call"
        ));
    }

    #[test]
    fn worker_usage_is_correlated_and_replayed_before_completion() {
        let mut registry = Registry::default();
        let mut worker = task("worker", AgentState::Running);
        worker.info.logical_task_id = Some("task-7".into());
        worker.info.origin_turn_id = Some("7".into());
        worker.info.tool_call_id = Some("call-7".into());
        registry.tasks.insert("worker".into(), worker);
        let registry = Arc::new(Mutex::new(registry));
        let usage = EventEnvelope {
            event_id: 1,
            session_id: "worker".into(),
            conversation_id: None,
            turn_id: Some("1".into()),
            task_id: None,
            parent_task_id: None,
            tool_call_id: None,
            actor: tachyon_api::types::Actor::Worker {
                id: "worker".into(),
            },
            sequence: 1,
            occurred_at_ms: 1,
            kind: StructuredAgentEvent::Usage {
                turn: Some(1),
                prompt_tokens: 100,
                completion_tokens: 20,
                total_tokens: 120,
                context_tokens: 100,
                context_window: Some(1_000),
            },
        };
        push_event(
            &registry,
            "worker",
            EventStream::Stdout,
            &serde_json::to_string(&usage).unwrap(),
        );

        let rx = registry.lock().unwrap().subscribe("worker").unwrap();
        let replayed = rx.recv().unwrap();
        let envelope: EventEnvelope = serde_json::from_str(&replayed.data).unwrap();
        assert_eq!(envelope.turn_id.as_deref(), Some("7"));
        assert_eq!(envelope.task_id.as_deref(), Some("task-7"));
        assert_eq!(envelope.tool_call_id.as_deref(), Some("call-7"));
        assert!(matches!(envelope.kind, StructuredAgentEvent::Usage { .. }));
    }

    #[test]
    fn staging_sets_a_visible_termination_deadline() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        registry
            .lock()
            .unwrap()
            .tasks
            .insert("worker".into(), task("worker", AgentState::Waiting));
        let response = stage_worker(&registry, "worker", 60);
        let ApiResponse::Agent { info } = response else {
            panic!("expected staged agent response");
        };
        assert_eq!(info.state, AgentState::Staged);
        assert!(info.stage_until_secs.is_some());
    }
}
