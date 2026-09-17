//! Host-authoritative worker requests and daemon service calls.

use tachyon_api::transport::Connection;
use tachyon_api::types::{
    AgentEvent, ApiRequest, ApiResponse, EventStream, LifetimeClass, WorkOutcome,
};
use tachyon_api::types::{
    MemoryIntent, MemoryMutationResult, MemoryRecallItem, ReminderInfo, ScheduleDay,
    ScheduledTaskInfo, ScheduledTaskMode,
};
use tachyon_api::InteractionMetadata;
use tachyon_model::ToolCall;

use super::{
    input::decode_event,
    streaming::{emit_event, session_id},
};

pub(super) async fn recall_for_turn(
    conversation_id: &str,
    turn: u64,
    query: &str,
    include_history: bool,
) -> Result<(Vec<MemoryRecallItem>, bool), String> {
    let conversation_id = conversation_id.to_string();
    let query = query.to_string();
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::MemoryRecall {
                query,
                conversation_id,
                turn,
                include_history,
                max_items: 12,
                max_chars: 6000,
            })
            .map_err(|error| error.to_string())?
        {
            ApiResponse::MemoryRecall { items, truncated } => Ok((items, truncated)),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected memory recall response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

pub(super) async fn mutate_memory_for_turn(
    metadata: &InteractionMetadata,
    turn: u64,
    intent: MemoryIntent,
) -> Result<MemoryMutationResult, String> {
    let metadata = metadata.clone();
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::MemoryMutate {
                intent,
                source_event_id: metadata.message_id,
                conversation_id: metadata.conversation_id,
                turn,
                occurred_at_ms: metadata.occurred_at_ms,
            })
            .map_err(|error| error.to_string())?
        {
            ApiResponse::MemoryMutation { result } => Ok(result),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected memory mutation response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

pub(super) async fn create_reminder(
    metadata: &InteractionMetadata,
    turn: u64,
    text: String,
    delay_seconds: Option<u64>,
    local_time: Option<String>,
    day: Option<ScheduleDay>,
) -> Result<ReminderInfo, String> {
    let metadata = metadata.clone();
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::ReminderCreate {
                source_event_id: format!(
                    "{}:{}:{}",
                    metadata.message_id, metadata.occurred_at_ms, turn
                ),
                conversation_id: metadata.conversation_id,
                turn,
                text,
                delay_seconds,
                local_time,
                day,
                created_at_ms: metadata.occurred_at_ms,
            })
            .map_err(|error| error.to_string())?
        {
            ApiResponse::Reminder { reminder } => Ok(reminder),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected reminder create response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

pub(super) async fn create_scheduled_task(
    metadata: &InteractionMetadata,
    turn: u64,
    objective: String,
    mode: ScheduledTaskMode,
    delay_seconds: Option<u64>,
    local_time: Option<String>,
    day: Option<ScheduleDay>,
) -> Result<ScheduledTaskInfo, String> {
    let metadata = metadata.clone();
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::ScheduledTaskCreate {
                source_event_id: format!(
                    "{}:{}:{}:task",
                    metadata.message_id, metadata.occurred_at_ms, turn
                ),
                conversation_id: metadata.conversation_id,
                turn,
                objective,
                mode,
                delay_seconds,
                local_time,
                day,
                created_at_ms: metadata.occurred_at_ms,
            })
            .map_err(|error| error.to_string())?
        {
            ApiResponse::ScheduledTask { schedule } => Ok(schedule),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected scheduled task response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

pub(super) async fn list_reminders() -> Result<Vec<ReminderInfo>, String> {
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::ReminderList)
            .map_err(|error| error.to_string())?
        {
            ApiResponse::Reminders { reminders } => Ok(reminders),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected reminder list response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

pub(super) async fn cancel_reminder(
    metadata: &InteractionMetadata,
    turn: u64,
    id: String,
) -> Result<ReminderInfo, String> {
    let conversation_id = metadata.conversation_id.clone();
    tokio::task::spawn_blocking(move || {
        let socket = tachyon_util::daemon::socket_path();
        let mut client = Connection::connect(&socket).map_err(|error| error.to_string())?;
        match client
            .exchange(&ApiRequest::ReminderCancel {
                id,
                conversation_id,
                turn,
            })
            .map_err(|error| error.to_string())?
        {
            ApiResponse::Reminder { reminder } => Ok(reminder),
            ApiResponse::Error { message, .. } => Err(message),
            other => Err(format!("unexpected reminder cancel response: {other:?}")),
        }
    })
    .await
    .map_err(|error| error.to_string())?
}

struct DelegationCorrelation {
    logical_task_id: String,
    origin_turn_id: Option<String>,
    parent_task_id: Option<String>,
    tool_call_id: Option<String>,
}

fn delegation_correlation(
    tool_call: &ToolCall,
    turn: Option<u64>,
    batch_index: Option<usize>,
) -> DelegationCorrelation {
    let session_id = session_id();
    let origin_turn_id = turn.map(|turn| turn.to_string());
    let suffix = batch_index
        .map(|index| format!("-{index}"))
        .unwrap_or_default();
    DelegationCorrelation {
        logical_task_id: format!(
            "{session_id}:{}:{}{suffix}",
            origin_turn_id.as_deref().unwrap_or("task"),
            tool_call.id
        ),
        origin_turn_id,
        parent_task_id: None,
        tool_call_id: (!tool_call.id.is_empty()).then(|| tool_call.id.clone()),
    }
}

/// Prepare both delegation forms from host-owned turn context, not model cwd arguments.
pub(super) fn delegation_requests(
    tc: &ToolCall,
    turn: Option<u64>,
    cwd: Option<String>,
) -> Result<Vec<ApiRequest>, String> {
    let value = serde_json::from_str::<serde_json::Value>(&tc.arguments).unwrap_or_default();
    let fanout = tc.name == "spawn_agents";
    let tasks = if fanout {
        let tasks = value
            .get("tasks")
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
            .filter(|tasks| !tasks.is_empty())
            .ok_or("spawn_agents requires a non-empty tasks array")?;
        if tasks.len() > 8 {
            return Err("spawn_agents accepts at most 8 tasks; group related objectives".into());
        }
        tasks
    } else {
        vec![arg(&tc.arguments, "task")]
    };
    let lifetime_class = value
        .get("lifetime_class")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(LifetimeClass::Short);
    let purpose = if fanout {
        ""
    } else {
        value
            .get("purpose")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
    };
    Ok(tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| {
            let correlation = delegation_correlation(tc, turn, fanout.then_some(index));
            ApiRequest::BackgroundDelegate {
                task,
                cwd: cwd.clone(),
                depends_on: Vec::new(),
                lifetime_class,
                purpose: purpose.to_string(),
                logical_task_id: Some(correlation.logical_task_id),
                origin_turn_id: correlation.origin_turn_id,
                parent_task_id: correlation.parent_task_id,
                tool_call_id: correlation.tool_call_id,
                deadline_ms: None,
            }
        })
        .collect())
}

/// Ask Tachyond to create a worker and wait for its terminal result.
/// The worker's live output remains available to TUI subscribers by its ID.
pub(super) fn spawn_via_daemon(request: ApiRequest) -> Result<String, String> {
    let ApiRequest::BackgroundDelegate {
        task,
        logical_task_id: Some(work_id),
        origin_turn_id,
        ..
    } = &request
    else {
        return Err("invalid worker delegation request".into());
    };
    let origin_turn = origin_turn_id
        .as_deref()
        .and_then(|turn| turn.parse::<u64>().ok());
    let socket = tachyon_util::daemon::socket_path();
    let mut client = Connection::connect(&socket).map_err(|e| e.to_string())?;
    let response = client.exchange(&request).map_err(|e| e.to_string())?;
    let id = match response {
        ApiResponse::Agent { info } => info.id,
        ApiResponse::Error { message, .. } => return Err(message),
        other => return Err(format!("unexpected spawn response: {other:?}")),
    };
    emit_event(AgentEvent::WorkerStarted {
        turn: origin_turn,
        worker_id: id.clone(),
        objective: task.to_string(),
    });

    let mut stream = Connection::connect(&socket).map_err(|e| e.to_string())?;
    stream
        .send(&ApiRequest::WorkSubscribe {
            work_id: work_id.clone(),
        })
        .map_err(|e| e.to_string())?;
    loop {
        match stream.recv().map_err(|error| error.to_string())? {
            ApiResponse::Event {
                stream: EventStream::Stdout,
                data,
            } => {
                if let Some(AgentEvent::WorkResult { result }) = decode_event(&data) {
                    return match result.outcome {
                        WorkOutcome::Completed { result, .. } => {
                            println!("[worker:result] {id} {result}");
                            Ok(result)
                        }
                        WorkOutcome::TimedOut { .. } => {
                            Err(format!("worker {id} timed out waiting for a result"))
                        }
                        WorkOutcome::Failed { message } => Err(format!("worker {id}: {message}")),
                        WorkOutcome::Blocked { reason } => {
                            Err(format!("worker {id} blocked: {reason}"))
                        }
                        WorkOutcome::Cancelled { reason } => {
                            Err(format!("worker {id} cancelled: {reason}"))
                        }
                    };
                }
            }
            ApiResponse::Event {
                stream: EventStream::Exit,
                data,
            } => {
                return Err(format!("worker {id} exited without a work result: {data}"));
            }
            ApiResponse::Error { message, .. } => return Err(message),
            _ => {}
        }
    }
}

pub(super) fn arg(args: &str, key: &str) -> String {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(args) {
        if let Some(v) = json.get(key) {
            if let serde_json::Value::String(s) = v {
                return s.clone();
            }
        }
    }
    String::new()
}
