#![forbid(unsafe_code)]

mod memory;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use tachyon_api::types::{
    AgentEvent as StructuredAgentEvent, AgentInfo, AgentState, ApiRequest, ApiResponse,
    BackgroundCoordinatorInfo, DaemonInfo, EventEnvelope, EventStream, LifecycleRecommendation,
    LifetimeClass, PendingWorkReviewInfo, WorkOutcome, WorkRequest, WorkResult, WorkReviewContext,
    WorkReviewDecision, WorkReviewFailure, WorkReviewRecommendation, WorkReviewRequest,
    PROTO_VERSION,
};
use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionMetadata, BACKGROUND_ID,
    FOREGROUND_ID,
};
use tachyon_memory::{TaskDocument, TaskMetadata, TaskState};
use tachyon_util::guard;

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
}

struct Registry {
    tasks: HashMap<String, Task>,
    works: HashMap<String, WorkRecord>,
    foreground_id: Option<String>,
    review_tx: Option<mpsc::SyncSender<WorkReviewRequest>>,
    background_online: bool,
    background_generation: u64,
    memory: Option<memory::MemoryClient>,
    memory_path: std::path::PathBuf,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            works: HashMap::new(),
            foreground_id: None,
            review_tx: None,
            background_online: false,
            background_generation: 0,
            memory: None,
            memory_path: tachyon_util::daemon::runtime_dir().join("memory.sock"),
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

    fn subscribe(&mut self, id: &str) -> Option<mpsc::Receiver<AgentEvent>> {
        let (tx, rx) = mpsc::channel();
        let task = self.tasks.get_mut(id)?;
        if let Some(usage) = &task.terminal_usage {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: usage.clone(),
            });
        }
        if let Some(result) = &task.terminal_result {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: result.clone(),
            });
        }
        task.subs.push(tx);
        Some(rx)
    }

    fn subscribe_work(&mut self, work_id: &str) -> Option<mpsc::Receiver<AgentEvent>> {
        let (tx, rx) = mpsc::channel();
        let work = self.works.get_mut(work_id)?;
        if let Some(result) = &work.terminal_result {
            let _ = tx.send(AgentEvent {
                stream: EventStream::Stdout,
                data: result.clone(),
            });
        } else {
            work.subs.push(tx);
        }
        Some(rx)
    }

    fn sorted(&self) -> Vec<AgentInfo> {
        let mut agents: Vec<AgentInfo> = self.tasks.values().map(|t| t.info.clone()).collect();
        agents.sort_by(|a, b| b.created_secs.cmp(&a.created_secs));
        agents
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

static INTERACTION_COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static DAEMON_EVENT_SEQUENCE: AtomicU64 = AtomicU64::new(1 << 63);

fn encode_interaction_command(
    command: InteractionCommand,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    turn_id: Option<String>,
) -> Result<String, String> {
    let sequence = INTERACTION_COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let message_id = format!("interaction-command-{sequence}");
    let mut metadata = InteractionMetadata::new(
        &message_id,
        correlation_id.unwrap_or_else(|| message_id.clone()),
        FOREGROUND_ID,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0),
    );
    metadata.causation_id = causation_id;
    metadata.turn_id = turn_id;
    serde_json::to_string(&InteractionCommandEnvelope { metadata, command })
        .map_err(|error| format!("encode interaction command: {error}"))
}

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

fn memory_state(state: AgentState) -> Option<TaskState> {
    match state {
        AgentState::Running | AgentState::Created | AgentState::Starting => {
            Some(TaskState::Running)
        }
        AgentState::Waiting => Some(TaskState::Waiting),
        AgentState::Staged => Some(TaskState::Waiting),
        AgentState::Completed => Some(TaskState::Completed),
        AgentState::Failed | AgentState::Interrupted => Some(TaskState::Failed),
        AgentState::Terminated => Some(TaskState::Terminated),
        AgentState::Released => Some(TaskState::Released),
    }
}

fn task_document(info: &AgentInfo, depends_on: &[String], note: &str) -> Option<TaskDocument> {
    Some(TaskDocument {
        metadata: TaskMetadata {
            id: info.id.clone(),
            kind: "agent".into(),
            objective: info.task.clone(),
            state: memory_state(info.state)?,
            depends_on: depends_on.to_vec(),
            created_at: info.created_secs.to_string(),
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs().to_string())
                .unwrap_or_else(|_| info.created_secs.to_string()),
            sensitivity: "normal".into(),
        },
        body: format!(
            "# Agent\n\nTask: {}\n\nWorkspace: {}\n\nState: {}\n\nSession: {}\n\nLifetime: {}\n\nRetained: {}\n\nLeaseUntil: {}\n\nPurpose: {}\n\nOwner: {}\n\nLastActivity: {}\n\nCheckpoint: {}\n\nTurns: {}\n\nTurnBudget: {}\n\nTaskType: {}\n\nDescription: {}\n\nPersistent: {}\n\nStageUntil: {}\n\nLogicalTask: {}\n\nOriginTurn: {}\n\nParentTask: {}\n\nToolCall: {}\n\n{}",
            info.task,
            info.workspace,
            info.state,
            info.session_id,
            info.lifetime_class,
            info.retained,
            info.lease_until_secs.map(|v| v.to_string()).unwrap_or_default(),
            info.purpose,
            info.owner,
            info.last_activity_secs,
            info.checkpoint_available,
            info.turns_used,
            info.turn_budget.map(|v| v.to_string()).unwrap_or_default(),
            info.task_type,
            info.description,
            info.persistent,
            info.stage_until_secs.map(|v| v.to_string()).unwrap_or_default(),
            info.logical_task_id.as_deref().unwrap_or_default(),
            info.origin_turn_id.as_deref().unwrap_or_default(),
            info.parent_task_id.as_deref().unwrap_or_default(),
            info.tool_call_id.as_deref().unwrap_or_default(),
            note
        ),
    })
}

fn persist_task(registry: &Arc<Mutex<Registry>>, info: &AgentInfo, note: &str) {
    if info.id == FOREGROUND_ID {
        return;
    }
    let (client, path) = {
        let reg = registry.lock().unwrap();
        (reg.memory.clone(), reg.memory_path.clone())
    };
    let Some(client) = client else { return };
    let depends_on = registry
        .lock()
        .unwrap()
        .tasks
        .get(&info.id)
        .map(|task| task.depends_on.clone())
        .unwrap_or_default();
    let Some(document) = task_document(info, &depends_on, note) else {
        return;
    };
    if let Err(error) = client.write_task(&document) {
        eprintln!(
            "tachyond: memory write {} ({}): {error}",
            info.id,
            path.display()
        );
    }
}

fn restore_memory_tasks(registry: &Arc<Mutex<Registry>>) {
    let client = registry.lock().unwrap().memory.clone();
    let Some(client) = client else { return };
    let documents = match client.list_tasks() {
        Ok(documents) => documents,
        Err(error) => {
            eprintln!("tachyond: memory restore unavailable: {error}");
            return;
        }
    };
    let mut reg = registry.lock().unwrap();
    for document in documents {
        if document.metadata.id == FOREGROUND_ID {
            continue;
        }
        let workspace = document
            .body
            .lines()
            .find_map(|line| line.strip_prefix("Workspace: "))
            .unwrap_or("")
            .to_string();
        let field = |name: &str| {
            document
                .body
                .lines()
                .find_map(|line| line.strip_prefix(&format!("{name}: ")))
                .unwrap_or("")
        };
        let lifetime_class =
            serde_json::from_str::<LifetimeClass>(&format!("\"{}\"", field("Lifetime")))
                .unwrap_or_default();
        // Only persistent sessions are eligible for daemon restart recovery.
        // Short and long workers remain historical records and are not revived.
        if lifetime_class != LifetimeClass::Persistent {
            continue;
        }
        let mut state = match document.metadata.state {
            TaskState::Completed => AgentState::Completed,
            TaskState::Failed => AgentState::Failed,
            TaskState::Terminated => AgentState::Terminated,
            TaskState::Released => AgentState::Released,
            TaskState::Waiting => AgentState::Waiting,
            TaskState::Ready | TaskState::Running | TaskState::Paused => AgentState::Created,
        };
        let created_secs = document.metadata.created_at.parse().unwrap_or(0);
        let depends_on = document.metadata.depends_on.clone();
        let retained = field("Retained") == "true";
        if state == AgentState::Completed && retained {
            state = AgentState::Created;
        }
        let lease_until_secs = field("LeaseUntil").parse().ok();
        let turns_used = field("Turns").parse().unwrap_or(0);
        let turn_budget = field("TurnBudget").parse().ok();
        let stage_until_secs = field("StageUntil").parse().ok();
        if stage_until_secs.is_some_and(|deadline| deadline > unix_now()) {
            state = AgentState::Staged;
        }
        let info = AgentInfo {
            id: document.metadata.id.clone(),
            task: document.metadata.objective.clone(),
            state,
            pid: None,
            workspace,
            created_secs,
            retained,
            lease_until_secs,
            session_id: {
                let value = field("Session");
                if value.is_empty() {
                    document.metadata.id.clone()
                } else {
                    value.into()
                }
            },
            lifetime_class,
            purpose: field("Purpose").to_string(),
            owner: field("Owner").to_string(),
            last_activity_secs: field("LastActivity").parse().unwrap_or(created_secs),
            checkpoint_available: false,
            turns_used,
            turn_budget,
            task_type: field("TaskType").to_string(),
            description: field("Description").to_string(),
            persistent: lifetime_class == LifetimeClass::Persistent,
            sandboxed: false,
            stage_until_secs,
            logical_task_id: (!field("LogicalTask").is_empty())
                .then(|| field("LogicalTask").to_string()),
            origin_turn_id: (!field("OriginTurn").is_empty())
                .then(|| field("OriginTurn").to_string()),
            parent_task_id: (!field("ParentTask").is_empty())
                .then(|| field("ParentTask").to_string()),
            tool_call_id: (!field("ToolCall").is_empty()).then(|| field("ToolCall").to_string()),
        };
        let task_owner = info.owner.clone();
        let task_activity = info.last_activity_secs;
        let control_socket = format!("{}/.tachyon/agent.sock", info.workspace);
        reg.tasks.entry(info.id.clone()).or_insert_with(|| Task {
            info,
            depends_on,
            process: None,
            stdin: None,
            subs: Vec::new(),
            generation: 0,
            assignment: 0,
            warm: retained,
            ready: false,
            owner: Some(task_owner),
            last_used_secs: task_activity,
            control_socket: Some(control_socket),
            terminal_usage: None,
            terminal_result: None,
        });
    }
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
    if cwd.is_some_and(|cwd| !cwd.is_empty()) || !dependencies_satisfied(registry, depends_on) {
        return None;
    }
    let task_type = if purpose.trim().is_empty() {
        "general".into()
    } else {
        normalized_task_type(purpose)
    };
    let candidate = registry.tasks.values_mut().find(|candidate| {
        candidate.warm
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
                    work.request.deadline_ms =
                        unix_now_ms().saturating_add(worker_result_timeout().as_millis() as u64);
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
    let StructuredAgentEvent::WorkCandidate { candidate } = envelope.kind else {
        return;
    };
    let completed = matches!(candidate.outcome, WorkOutcome::Completed { .. });
    if !completed {
        let terminal = result_envelope(worker_id, candidate);
        if let Ok(data) = serde_json::to_string(&terminal) {
            push_event(registry, worker_id, EventStream::Stdout, &data);
        }
        return;
    }
    let (request, review_tx) = {
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
        });
        (request, reg.review_tx.clone())
    };
    let queued = review_tx
        .as_ref()
        .is_some_and(|tx| tx.try_send(request.clone()).is_ok());
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
        work.review = None;
        request.candidate.clone()
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
    apply_work_review_at(registry, decision, unix_now_ms());
}

fn apply_work_review_at(
    registry: &Arc<Mutex<Registry>>,
    decision: WorkReviewDecision,
    now_ms: u64,
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
        let request = request.clone();
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

fn push_event(registry: &Arc<Mutex<Registry>>, id: &str, stream: EventStream, data: &str) {
    if stream == EventStream::Stdout {
        if let Ok(envelope) = serde_json::from_str::<EventEnvelope>(data) {
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
    let (workspace, is_foreground) = {
        let reg = registry.lock().unwrap();
        let Some(task) = reg.tasks.get(id) else {
            return;
        };
        (task.info.workspace.clone(), task.info.id == FOREGROUND_ID)
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
    let keep = std::env::var("TACHYON_KEEP_WORKSPACES")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    if !keep && !is_foreground {
        let ws_root = tachyon_util::daemon::workspaces_dir();
        let workspace = std::path::Path::new(&workspace);
        if workspace.starts_with(&ws_root) {
            let _ = std::fs::remove_dir_all(workspace);
        }
    }
}

// Subscribe path: `subscribe()` hands back an mpsc receiver while still
// holding the task in the map, so keep the borrow short.
fn subscribe_unlocked(reg: &mut Registry, id: &str) -> Option<mpsc::Receiver<AgentEvent>> {
    reg.subscribe(id)
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

    let shutdown = Arc::new(AtomicBool::new(false));
    if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))
    {
        eprintln!("tachyond: failed to register SIGTERM handler: {e}");
    }
    if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))
    {
        eprintln!("tachyond: failed to register SIGINT handler: {e}");
    }

    let pid = std::process::id();
    if let Err(e) = tachyon_util::daemon::write_pid(pid) {
        eprintln!("tachyond: failed to write pid file: {e}");
        return std::process::ExitCode::FAILURE;
    }

    let memory_socket = tachyon_util::daemon::runtime_dir().join("memory.sock");
    let mut memory = match spawn_memory() {
        Ok(child) => {
            eprintln!("tachyond: memory service started");
            Some(child)
        }
        Err(e) => {
            eprintln!("tachyond: failed to start memory service: {e}");
            None
        }
    };

    let memory_client =
        match memory::MemoryClient::connect(&memory_socket, std::time::Duration::from_secs(3)) {
            Ok(client) => Some(client),
            Err(error) => {
                eprintln!(
                    "tachyond: memory unavailable at {}: {error}",
                    memory_socket.display()
                );
                None
            }
        };
    let reg = Arc::new(Mutex::new(Registry {
        memory: memory_client,
        memory_path: memory_socket.clone(),
        ..Registry::default()
    }));
    let (review_tx, review_rx) = mpsc::sync_channel(64);
    reg.lock().unwrap().review_tx = Some(review_tx);
    let background_registry = Arc::clone(&reg);
    let background_shutdown = Arc::clone(&shutdown);
    let background = std::thread::spawn(move || {
        supervise_background(background_registry, review_rx, background_shutdown)
    });
    restore_memory_tasks(&reg);
    start_ready_tasks(&reg);

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

    shutdown_tasks(&reg);
    let _ = background.join();
    if let Some(child) = memory.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
    let _ = std::fs::remove_file(tachyon_util::daemon::runtime_dir().join("memory.sock"));
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
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        let req = match read_request(&mut reader) {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(_) => return write_response(&mut writer, &ApiResponse::error("bad request")),
        };

        if let ApiRequest::AgentSubscribe { id } = &req {
            return stream_agent(&mut writer, id, registry);
        }
        if let ApiRequest::WorkSubscribe { work_id } = &req {
            return stream_work(&mut writer, work_id, registry);
        }
        if let ApiRequest::ForegroundSubscribe = &req {
            return stream_agent(&mut writer, FOREGROUND_ID, registry);
        }

        let resp = dispatch(&req, &registry);
        if write_response(&mut writer, &resp).is_err() {
            return Ok(());
        }
    }
}

/// Stream an agent's events to a subscriber until the agent ends.
fn stream_agent(
    writer: &mut UnixStream,
    id: &str,
    registry: Arc<Mutex<Registry>>,
) -> std::io::Result<()> {
    let rx = {
        let mut reg = registry.lock().unwrap();
        match subscribe_unlocked(&mut reg, id) {
            Some(rx) => rx,
            None => {
                return write_response(writer, &ApiResponse::error(format!("no such agent: {id}")))
            }
        }
    };

    while let Ok(ev) = rx.recv() {
        let resp = ApiResponse::Event {
            stream: ev.stream,
            data: ev.data,
        };
        if write_response(writer, &resp).is_err() {
            break;
        }
    }
    Ok(())
}

fn stream_work(
    writer: &mut UnixStream,
    work_id: &str,
    registry: Arc<Mutex<Registry>>,
) -> std::io::Result<()> {
    let rx = {
        let mut reg = registry.lock().unwrap();
        match reg.subscribe_work(work_id) {
            Some(rx) => rx,
            None => {
                return write_response(
                    writer,
                    &ApiResponse::error(format!("no such work: {work_id}")),
                )
            }
        }
    };
    while let Ok(event) = rx.recv() {
        if write_response(
            writer,
            &ApiResponse::Event {
                stream: event.stream,
                data: event.data,
            },
        )
        .is_err()
        {
            break;
        }
    }
    Ok(())
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

/// Dispatch a request to the registry.
fn dispatch(req: &ApiRequest, registry: &Arc<Mutex<Registry>>) -> ApiResponse {
    use ApiRequest::*;
    match req {
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
        } => {
            let delegated_by_background = matches!(req, BackgroundDelegate { .. });
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
                        cwd,
                        depends_on,
                        *lifetime_class,
                        purpose,
                        origin_turn_id,
                        parent_task_id,
                        tool_call_id,
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
                        work_id: work_id.clone(),
                        objective: task.clone(),
                        generation,
                        assignment,
                        deadline_ms: unix_now_ms()
                            .saturating_add(worker_result_timeout().as_millis() as u64),
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
                Some(c) if !c.is_empty() => c.clone(),
                _ => tachyon_util::daemon::workspaces_dir()
                    .join(&id)
                    .display()
                    .to_string(),
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
                    work_id: work_id.clone(),
                    objective: task.clone(),
                    generation: 0,
                    assignment: 0,
                    deadline_ms: unix_now_ms()
                        .saturating_add(worker_result_timeout().as_millis() as u64),
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
        ForegroundChat { text } => {
            let command = encode_interaction_command(
                InteractionCommand::AcceptUserTurn { text: text.clone() },
                None,
                None,
                None,
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
    rx: mpsc::Receiver<WorkReviewRequest>,
    shutdown: Arc<AtomicBool>,
) {
    let mut generation = 0_u64;
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
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
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
                Ok(request) => {
                    if let Some(request) = current_review_request(&registry, &request) {
                        if write_review_request(&mut stdin, &request).is_err() {
                            restart = true;
                        }
                    }
                }
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
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

/// Resolve the supervised Memory service binary.
fn memory_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("TACHYON_MEMORY_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join("tachyon-memory")))
        .unwrap_or_else(|| "tachyon-memory".into())
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

/// Spawn the Markdown-first Memory service under Tachyond supervision.
fn spawn_memory() -> std::io::Result<Child> {
    let root = tachyon_util::daemon::data_dir().join("memory");
    let socket = tachyon_util::daemon::runtime_dir().join("memory.sock");
    Command::new(memory_path())
        .arg("--root")
        .arg(root)
        .arg("--socket")
        .arg(socket)
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
    let mut cmd = Command::new(foreground_path());
    cmd.arg("--agent-id")
        .arg(FOREGROUND_ID)
        .arg("--new-session");

    // The Conversational Agent's workspace should be the directory the daemon
    // was launched from (the user's project when they ran `tachyon` there).
    if let Ok(cwd) = std::env::current_dir() {
        cmd.arg("--cwd").arg(&cwd).current_dir(&cwd);
    }

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
    let (rx, request, worker_id) = {
        let mut reg = registry.lock().unwrap();
        let Some(work) = reg.works.get(&work_id) else {
            return;
        };
        let request = work.request.clone();
        let worker_id = work.worker_id.clone();
        let Some(rx) = reg.subscribe_work(&work_id) else {
            return;
        };
        (rx, request, worker_id)
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
                        work_id: request.work_id.clone(),
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

    #[test]
    fn foreground_commands_are_versioned_and_preserve_multiline_turns() {
        let wire = encode_interaction_command(
            InteractionCommand::AcceptUserTurn {
                text: "line one\nline two".into(),
            },
            Some("request-1".into()),
            None,
            Some("4".into()),
        )
        .unwrap();
        assert!(!wire.contains('\n'));
        let decoded: InteractionCommandEnvelope = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            decoded.metadata.protocol_version,
            tachyon_api::INTERACTION_PROTOCOL_VERSION
        );
        assert_eq!(decoded.metadata.correlation_id, "request-1");
        assert_eq!(decoded.metadata.turn_id.as_deref(), Some("4"));
        assert!(matches!(
            decoded.command,
            InteractionCommand::AcceptUserTurn { text } if text == "line one\nline two"
        ));
    }

    fn task(id: &str, state: AgentState) -> Task {
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
    ) -> (Arc<Mutex<Registry>>, mpsc::Receiver<WorkReviewRequest>) {
        let (review_tx, review_rx) = mpsc::sync_channel(4);
        let mut registry = Registry {
            background_generation: 7,
            review_tx: Some(review_tx),
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
        (Arc::new(Mutex::new(registry)), review_rx)
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
                    work_id: "work-1".into(),
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
        let mut registry = Registry::default();
        let mut worker = task("worker", AgentState::Completed);
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

        let claimed = claim_reusable_worker(
            &mut registry,
            "new objective",
            "Research",
            LifetimeClass::Long,
            None,
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
            None,
            &[],
            &None,
            &None,
            &None,
            &None,
        )
        .is_none());
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
    fn typed_work_result_is_terminal_once_and_replayed_by_work_id() {
        let registry = Arc::new(Mutex::new(Registry::default()));
        let mut worker = task("worker", AgentState::Running);
        worker.warm = true;
        worker.info.retained = true;
        worker.info.logical_task_id = Some("work-1".into());
        let info = worker.info.clone();
        let request = WorkRequest {
            work_id: "work-1".into(),
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
        let result = WorkResult {
            work_id: request.work_id.clone(),
            objective: request.objective.clone(),
            generation: 0,
            assignment: 0,
            outcome: WorkOutcome::Completed {
                result: "first".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: true,
            },
        };
        let event = serde_json::to_string(&StructuredAgentEvent::WorkResult {
            result: result.clone(),
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &event);

        let duplicate = serde_json::to_string(&StructuredAgentEvent::WorkResult {
            result: WorkResult {
                outcome: WorkOutcome::Completed {
                    result: "second".into(),
                    artifacts: Vec::new(),
                    context: String::new(),
                    suggested_reuse: true,
                },
                ..result
            },
        })
        .unwrap();
        push_event(&registry, "worker", EventStream::Stdout, &duplicate);

        let mut reg = registry.lock().unwrap();
        assert_eq!(reg.tasks["worker"].info.turns_used, 1);
        let replay = reg.subscribe_work("work-1").unwrap();
        let replayed = replay.recv().unwrap();
        assert!(replayed.data.contains("first"));
        assert!(!replayed.data.contains("second"));
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
        apply_work_review_at(&registry, decision.clone(), started_ms + 10_001);
        assert!(rx.try_recv().is_err());
        assert!(registry.lock().unwrap().works["work-1"].review.is_some());

        decision.assignment = request.candidate.assignment;
        apply_work_review_at(&registry, decision, started_ms + 10_001);
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
    fn waiting_maps_to_waiting_memory_state() {
        assert_eq!(memory_state(AgentState::Waiting), Some(TaskState::Waiting));
    }

    #[test]
    fn lifecycle_states_map_to_expected_unix_signals() {
        assert_eq!(lifecycle_signal(AgentState::Terminated), Signal::SIGTERM);
        assert_eq!(lifecycle_signal(AgentState::Interrupted), Signal::SIGINT);
    }

    #[test]
    fn lifecycle_terminal_states_are_preserved_in_memory_mapping() {
        assert_eq!(
            memory_state(AgentState::Completed),
            Some(TaskState::Completed)
        );
        assert_eq!(memory_state(AgentState::Failed), Some(TaskState::Failed));
        assert_eq!(
            memory_state(AgentState::Interrupted),
            Some(TaskState::Failed)
        );
        assert_eq!(
            memory_state(AgentState::Terminated),
            Some(TaskState::Terminated)
        );
        assert_eq!(
            memory_state(AgentState::Released),
            Some(TaskState::Released)
        );
        assert!(AgentState::Released.is_terminal());
    }

    #[test]
    fn persisted_task_contains_session_policy_metadata() {
        let mut worker = task("research", AgentState::Waiting);
        worker.info.session_id = "session-research".into();
        worker.info.retained = true;
        worker.info.purpose = "research".into();
        worker.info.turn_budget = Some(3);
        worker.info.logical_task_id = Some("task-7".into());
        worker.info.origin_turn_id = Some("7".into());
        worker.info.parent_task_id = Some("task-parent".into());
        worker.info.tool_call_id = Some("call-7".into());
        let document = task_document(&worker.info, &[], "retained").expect("document");
        assert!(document.body.contains("Session: session-research"));
        assert!(document.body.contains("Retained: true"));
        assert!(document.body.contains("Purpose: research"));
        assert!(document.body.contains("TurnBudget: 3"));
        assert!(document.body.contains("LogicalTask: task-7"));
        assert!(document.body.contains("OriginTurn: 7"));
        assert!(document.body.contains("ParentTask: task-parent"));
        assert!(document.body.contains("ToolCall: call-7"));
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
