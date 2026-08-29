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
    AgentEvent as StructuredAgentEvent, AgentInfo, AgentState, ApiRequest, ApiResponse, DaemonInfo,
    EventEnvelope, EventStream, LifetimeClass, PROTO_VERSION,
};
use tachyon_api::{
    InteractionCommand, InteractionCommandEnvelope, InteractionMetadata, FOREGROUND_ID,
};
use tachyon_memory::{TaskDocument, TaskMetadata, TaskState};
use tachyon_util::guard;

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

struct Registry {
    tasks: HashMap<String, Task>,
    foreground_id: Option<String>,
    memory: Option<memory::MemoryClient>,
    memory_path: std::path::PathBuf,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            tasks: HashMap::new(),
            foreground_id: None,
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

static INTERACTION_COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
    if purpose.trim().is_empty()
        || !matches!(
            lifetime_class,
            LifetimeClass::Long | LifetimeClass::Persistent
        )
        || cwd.is_some_and(|cwd| !cwd.is_empty())
        || !dependencies_satisfied(registry, depends_on)
    {
        return None;
    }
    let candidate = registry.tasks.values_mut().find(|candidate| {
        candidate.warm
            && candidate.info.retained
            && candidate.info.owner == "background"
            && candidate.info.state == AgentState::Completed
            && candidate.info.lifetime_class == lifetime_class
            && candidate.ready
            && (candidate.stdin.is_some() || candidate.control_socket.is_some())
    })?;
    let now = unix_now();
    candidate.info.task = task.to_string();
    candidate.info.state = AgentState::Running;
    candidate.info.purpose = purpose.to_string();
    candidate.info.task_type = normalized_task_type(purpose);
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
                if candidate.retained {
                    if let Err(error) = deliver_task(
                        registry,
                        &candidate.id,
                        &candidate.task,
                        candidate.persistent,
                    ) {
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
    if matches!(
        envelope.kind,
        StructuredAgentEvent::Usage { .. } | StructuredAgentEvent::WorkerCompleted { .. }
    ) {
        envelope.turn_id = info.origin_turn_id.clone().or(envelope.turn_id);
        envelope.tool_call_id = info.tool_call_id.clone().or(envelope.tool_call_id);
    }
    serde_json::to_string(&envelope).unwrap_or_else(|_| data.to_string())
}

fn push_event(registry: &Arc<Mutex<Registry>>, id: &str, stream: EventStream, data: &str) {
    let mut retire: Option<(Option<u32>, AgentInfo)> = None;
    let mut completed = false;
    let mut correlated_data = data.to_string();
    if let Some(task) = registry.lock().unwrap().tasks.get_mut(id) {
        correlated_data = correlate_event(data, &task.info);
        let structured = decode_structured_event(&correlated_data);
        completed = structured
            .as_ref()
            .is_some_and(|event| matches!(event, StructuredAgentEvent::WorkerCompleted { .. }));
        let ready = structured.as_ref().is_some_and(|event| {
            matches!(event, StructuredAgentEvent::Status { phase, message, .. } if phase == "ready" && message == "idle")
        });
        if structured
            .as_ref()
            .is_some_and(|event| matches!(event, StructuredAgentEvent::Usage { .. }))
        {
            task.terminal_usage = Some(correlated_data.clone());
        }
        if ready {
            task.ready = true;
        }
        if task.warm && completed {
            task.ready = false;
            task.info.state = AgentState::Completed;
            task.info.last_activity_secs = unix_now();
            task.info.turns_used = task.info.turns_used.saturating_add(1);
            task.last_used_secs = unix_now();
            if task.info.lifetime_class == LifetimeClass::Short {
                task.info.retained = false;
                task.info.state = AgentState::Released;
                let pid = task.info.pid;
                task.info.pid = None;
                task.stdin = None;
                retire = Some((pid, task.info.clone()));
            } else {
                task.info.retained = true;
            }
        }
        if completed {
            task.terminal_result = Some(correlated_data.clone());
        }
        task.subs.retain(|tx| {
            tx.send(AgentEvent {
                stream,
                data: correlated_data.clone(),
            })
            .is_ok()
        });
    }
    if let Some((pid, info)) = retire {
        if let Some(pid) = pid {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
        persist_task(
            registry,
            &info,
            "Short worker released after completing its task.",
        );
        cleanup_workspace(&info);
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
            tasks.push((task.control_socket.clone(), task.process.take()));
        }
    }
    for (socket, mut child) in tasks {
        if let Some(socket) = socket {
            let _ = supervisor_command(&socket, "signal\tterm\n");
        } else if let Some(process) = child.as_mut() {
            let _ = process.kill();
        }
        if let Some(process) = child.as_mut() {
            let _ = process.wait();
        }
    }
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
        DaemonStatus => ApiResponse::DaemonStatus {
            info: DaemonInfo {
                pid: std::process::id(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                proto_version: PROTO_VERSION.into(),
                provider_ready: tachyon_util::config::Config::load().provider_ready(),
                socket: tachyon_util::daemon::socket_path().display().to_string(),
            },
        },
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
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            let mut reg = registry.lock().unwrap();
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
                    let assignment = reg.tasks[&id].assignment;
                    drop(reg);
                    if let Err(error) = deliver_task(registry, &id, task, info.persistent) {
                        if let Some(worker) = registry.lock().unwrap().tasks.get_mut(&id) {
                            if worker.assignment == assignment
                                && worker.info.state == AgentState::Running
                            {
                                worker.info.state = AgentState::Failed;
                                worker.ready = false;
                            }
                        }
                        return ApiResponse::error(error);
                    }
                    collect_worker_result(Arc::clone(registry), id.clone(), FOREGROUND_ID.into());
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
                retained: warm && *lifetime_class != LifetimeClass::Short,
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
                turn_budget: (*lifetime_class == LifetimeClass::Short).then_some(1),
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
                    push_event(registry, &id, EventStream::Exit, "ghost failed to start");
                    persist_task(registry, &info, &format!("Agent start failed: {error}"));
                    return ApiResponse::Agent { info };
                }
            }

            if delegated_by_background {
                collect_worker_result(Arc::clone(registry), id.clone(), FOREGROUND_ID.into());
            }
            {
                let pump_registry = Arc::clone(registry);
                let id = id.clone();
                std::thread::spawn(move || pump_agent(&pump_registry, &id, 0));
            }
            if let Err(error) = deliver_task(registry, &id, task, info.persistent) {
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
                return ApiResponse::error(error);
            }

            persist_task(registry, &info, "Agent started.");
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
        AgentSubscribe { .. } => ApiResponse::error("unreachable: handled in handle_connection"),
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
    let (info, mut process) = {
        let mut reg = registry.lock().unwrap();
        if reg.foreground_id.as_deref() == Some(id) {
            return ApiResponse::error("the foreground is not a controllable worker");
        }
        let Some(task) = reg.get_mut(id) else {
            return ApiResponse::error(format!("no such agent: {id}"));
        };
        task.generation = task.generation.wrapping_add(1);
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
        (info, process)
    };

    if let Some(child) = process.as_mut() {
        let _ = child.kill();
        let _ = child.wait();
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
    let mut reg = registry.lock().unwrap();
    if reg.foreground_id.as_deref() == Some(id) {
        return ApiResponse::error("the foreground is not a retainable worker");
    }
    match reg.get_mut(id) {
        Some(task) => {
            task.info.retained = true;
            if task.info.state == AgentState::Staged {
                task.info.state = AgentState::Waiting;
            }
            task.info.lease_until_secs = lease_until_secs;
            task.info.stage_until_secs = None;
            if let Some(class) = lifetime_class {
                task.info.lifetime_class = class;
                task.info.turn_budget = (class == LifetimeClass::Short).then_some(1);
                task.info.turns_used = 0;
            }
            task.last_used_secs = unix_now();
            ApiResponse::Agent {
                info: task.info.clone(),
            }
        }
        None => ApiResponse::error(format!("no such agent: {id}")),
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
        collect_worker_result(Arc::clone(registry), worker_id, foreground_id.to_string());
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
    collect_worker_result(
        Arc::clone(registry),
        worker_id.clone(),
        foreground_id.to_string(),
    );

    // Start pumping the worker's stdout/stderr to its subscribers.
    let pump_reg = Arc::clone(registry);
    let pump_id = worker_id.clone();
    std::thread::spawn(move || pump_agent(&pump_reg, &pump_id, 0));
}

fn collect_worker_result(registry: Arc<Mutex<Registry>>, worker_id: String, foreground_id: String) {
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
        .unwrap_or(60)
        .max(1);
    std::time::Duration::from_secs(seconds)
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
        worker.info.task_type = "coding".into();
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
