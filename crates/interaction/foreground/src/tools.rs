//! Native contextual tool dispatch, validation, and per-turn service state.

use futures_util::future::join_all;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tachyon_api::types::{
    ApiRequest, LifetimeClass, MemoryCardinality, MemoryDescriptor, MemoryIntent, MemoryKind,
    MemoryMutationResult, ReminderInfo, ReminderStatus, ScheduleDay, ScheduledTaskMode,
    ScheduledTaskStatus,
};
use tachyon_api::{InteractionMetadata, TaskIntent};
use tachyon_model::{ToolCall, ToolSpec};

use crate::delegation::{
    cancel_reminder, create_reminder, create_scheduled_task, delegation_requests, list_reminders,
    mutate_memory_for_turn, recall_for_turn, spawn_via_daemon,
};
use crate::runtime::AgentRole;

pub(super) fn task_intents(call: &ToolCall) -> Vec<TaskIntent> {
    let value = serde_json::from_str::<serde_json::Value>(&call.arguments).unwrap_or_default();
    let lifetime_class = value
        .get("lifetime_class")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or(LifetimeClass::Short);
    match call.name.as_str() {
        "spawn_agent" => value
            .get("task")
            .and_then(|value| value.as_str())
            .filter(|task| !task.trim().is_empty())
            .map(|task| {
                vec![TaskIntent {
                    objective: task.into(),
                    purpose: value
                        .get("purpose")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .into(),
                    lifetime_class,
                }]
            })
            .unwrap_or_default(),
        "spawn_agents" => value
            .get("tasks")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str())
            .filter(|task| !task.trim().is_empty())
            .map(|task| TaskIntent {
                objective: task.into(),
                purpose: String::new(),
                lifetime_class,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) struct ToolOutput {
    pub(super) text: String,
    pub(super) succeeded: bool,
}

#[derive(Clone)]
pub(super) struct MemoryToolContext {
    pub(super) metadata: InteractionMetadata,
    pub(super) turn: u64,
    pub(super) recalled_ids: Arc<Mutex<BTreeSet<String>>>,
    pub(super) recall_used: Arc<AtomicBool>,
    pub(super) mutation_used: Arc<AtomicBool>,
    pub(super) mutation_succeeded: Arc<Mutex<Option<bool>>>,
}

#[derive(Clone)]
pub(super) struct ScheduleToolContext {
    pub(super) metadata: InteractionMetadata,
    pub(super) turn: u64,
    pub(super) listed_ids: Arc<Mutex<BTreeSet<String>>>,
    pub(super) list_used: Arc<AtomicBool>,
    pub(super) mutation_used: Arc<AtomicBool>,
    pub(super) mutation_succeeded: Arc<Mutex<Option<bool>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryToolArgs {
    action: String,
    query: Option<String>,
    include_history: Option<bool>,
    target_ids: Option<Vec<String>>,
    value: Option<String>,
    kind: Option<MemoryKind>,
    namespace: Option<String>,
    relation: Option<String>,
    scope: Option<String>,
    cardinality: Option<MemoryCardinality>,
    #[serde(default)]
    topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleToolArgs {
    action: String,
    text: Option<String>,
    objective: Option<String>,
    delay_seconds: Option<u64>,
    local_time: Option<String>,
    day: Option<ScheduleDay>,
    id: Option<String>,
}

pub(super) fn available_tools_for_context(
    tools: &[ToolSpec],
    memory_context: Option<&MemoryToolContext>,
    schedule_context: Option<&ScheduleToolContext>,
) -> Vec<ToolSpec> {
    let mut tools = tools.to_vec();
    if let Some(context) = memory_context {
        if context.mutation_succeeded.lock().unwrap().is_some() {
            tools.retain(|tool| tool.name != "memory");
        } else if context.recall_used.load(Ordering::Acquire) {
            if let Some(memory) = tools.iter_mut().find(|tool| tool.name == "memory") {
                memory.parameters["properties"]["action"]["enum"] =
                    serde_json::json!(["remember", "forget", "correct"]);
                if let Some(actions) = memory.parameters["oneOf"].as_array_mut() {
                    actions.retain(|action| {
                        action["properties"]["action"]["const"].as_str() != Some("recall")
                    });
                }
                memory.description = "Apply at most one durable memory change using the prior recall result. Use exact recalled IDs for forget or correct.".into();
            }
        }
    }
    if let Some(context) = schedule_context {
        if context.mutation_succeeded.lock().unwrap().is_some() {
            tools.retain(|tool| tool.name != "schedule");
        } else if context.list_used.load(Ordering::Acquire) {
            if let Some(schedule) = tools.iter_mut().find(|tool| tool.name == "schedule") {
                schedule.parameters["properties"]["action"]["enum"] =
                    serde_json::json!(["create", "cancel", "start_at", "finish_by"]);
                if let Some(actions) = schedule.parameters["oneOf"].as_array_mut() {
                    actions.retain(|action| {
                        action["properties"]["action"]["const"].as_str() != Some("list")
                    });
                }
            }
        }
    }
    tools
}

impl ToolOutput {
    fn success(text: String) -> Self {
        Self {
            text,
            succeeded: true,
        }
    }

    fn failure(text: String) -> Self {
        Self {
            text,
            succeeded: false,
        }
    }
}

pub(super) async fn run_tool(
    tc: &ToolCall,
    role: AgentRole,
    delegation_allowed: bool,
    turn: Option<u64>,
    memory_batch_valid: bool,
    memory_context: Option<MemoryToolContext>,
    schedule_batch_valid: bool,
    schedule_context: Option<ScheduleToolContext>,
    cwd: Option<String>,
) -> ToolOutput {
    match role.allows_tool(&tc.name) {
        Ok(true) => {}
        Ok(false) => {
            return ToolOutput::failure(format!(
                "{} is not available to the {role:?} role",
                tc.name
            ))
        }
        Err(error) => return ToolOutput::failure(error),
    }
    match tc.name.as_str() {
        "memory" => {
            if !memory_batch_valid {
                return ToolOutput::failure(
                    "Use at most one memory action per model step; recall first, then use its result in the next step.".into(),
                );
            }
            let Some(context) = memory_context else {
                return ToolOutput::failure("Memory is unavailable outside a user turn.".into());
            };
            run_memory_tool(&tc.arguments, context).await
        }
        "schedule" => {
            if !schedule_batch_valid {
                return ToolOutput::failure(
                    "Use at most one schedule action per model step; list first, then cancel in the next step."
                        .into(),
                );
            }
            let Some(context) = schedule_context else {
                return ToolOutput::failure(
                    "Scheduling is unavailable outside a user turn.".into(),
                );
            };
            run_schedule_tool(&tc.arguments, context).await
        }
        "spawn_agent" | "spawn_agents" => {
            if !delegation_allowed {
                return ToolOutput::failure("Delegation has already been used for this turn. Synthesize an answer from the worker results already received; do not spawn another worker.".into());
            }
            let requests = match delegation_requests(tc, turn, cwd) {
                Ok(requests) => requests,
                Err(error) => return ToolOutput::failure(error),
            };
            let tasks = requests
                .iter()
                .filter_map(|request| match request {
                    ApiRequest::BackgroundDelegate { task, .. } => Some(task.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let jobs = requests
                .into_iter()
                .map(|request| tokio::task::spawn_blocking(move || spawn_via_daemon(request)));
            let results = join_all(jobs).await;
            let mut outcomes: Vec<_> = results
                .into_iter()
                .map(|result| match result {
                    Ok(Ok(answer)) => ToolOutput::success(answer),
                    Ok(Err(error)) => ToolOutput::failure(format!("worker failed: {error}")),
                    Err(error) => ToolOutput::failure(format!("worker task failed: {error}")),
                })
                .collect();
            if tc.name == "spawn_agent" {
                outcomes.remove(0)
            } else {
                compose_fanout_output(&tasks, outcomes)
            }
        }
        other => ToolOutput::failure(format!("unknown tool: {other}")),
    }
}

async fn run_memory_tool(arguments: &str, context: MemoryToolContext) -> ToolOutput {
    let args: MemoryToolArgs = match serde_json::from_str(arguments) {
        Ok(args) => args,
        Err(error) => {
            return ToolOutput::failure(format!("Invalid memory action: {error}"));
        }
    };
    if args.action == "recall" {
        if context
            .recall_used
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return ToolOutput::failure(
                "Memory was already recalled for this turn. Use the prior result and answer now."
                    .into(),
            );
        }
        if args.target_ids.is_some()
            || args.value.is_some()
            || args.kind.is_some()
            || args.namespace.is_some()
            || args.relation.is_some()
            || args.scope.is_some()
            || args.cardinality.is_some()
            || !args.topics.is_empty()
        {
            return ToolOutput::failure("Recall accepts only action and query.".into());
        }
        let Some(query) = args.query.map(|query| query.trim().to_string()) else {
            return ToolOutput::failure("Recall requires a query.".into());
        };
        if query.is_empty() || query.chars().count() > 1000 {
            return ToolOutput::failure("Recall query must contain 1 to 1000 characters.".into());
        }
        let (items, truncated) = match recall_for_turn(
            &context.metadata.conversation_id,
            context.turn,
            &query,
            args.include_history.unwrap_or(false),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                return ToolOutput::failure(format!("Memory recall unavailable: {error}"))
            }
        };
        {
            let mut recalled_ids = context.recalled_ids.lock().unwrap();
            recalled_ids.extend(items.iter().filter_map(|item| item.memory_id.clone()));
        }
        return ToolOutput::success(
            serde_json::json!({
                "status": "ok",
                "items": items,
                "truncated": truncated,
            })
            .to_string(),
        );
    }

    if args.query.is_some() || args.include_history.is_some() {
        return ToolOutput::failure("Only recall accepts query or include_history.".into());
    }
    *context.mutation_succeeded.lock().unwrap() = Some(false);
    let intent = match memory_intent_from_tool(args, &context.recalled_ids) {
        Ok(intent) => intent,
        Err(error) => return ToolOutput::failure(error),
    };
    if context
        .mutation_used
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return ToolOutput::failure("Only one memory change is allowed per user turn.".into());
    }
    let result = mutate_memory_for_turn(&context.metadata, context.turn, intent)
        .await
        .unwrap_or(MemoryMutationResult::Unavailable);
    let succeeded = matches!(
        result,
        MemoryMutationResult::Applied { .. } | MemoryMutationResult::AlreadyApplied { .. }
    );
    *context.mutation_succeeded.lock().unwrap() = Some(succeeded);
    let output = serde_json::json!({
        "authoritative": true,
        "result": result,
        "instruction": if succeeded {
            "The durable memory change succeeded and may be confirmed naturally."
        } else {
            "No durable memory change was made; do not claim success."
        }
    })
    .to_string();
    if succeeded {
        ToolOutput::success(output)
    } else {
        ToolOutput::failure(output)
    }
}

async fn run_schedule_tool(arguments: &str, context: ScheduleToolContext) -> ToolOutput {
    let proposed_action = serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|value| {
            value
                .get("action")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        });
    if proposed_action
        .as_deref()
        .is_some_and(|action| matches!(action, "create" | "cancel" | "start_at" | "finish_by"))
    {
        *context.mutation_succeeded.lock().unwrap() = Some(false);
    }
    let args: ScheduleToolArgs = match serde_json::from_str(arguments) {
        Ok(args) => args,
        Err(error) => return ToolOutput::failure(format!("Invalid schedule action: {error}")),
    };
    match args.action.as_str() {
        "list" => {
            if args.text.is_some()
                || args.objective.is_some()
                || args.delay_seconds.is_some()
                || args.local_time.is_some()
                || args.day.is_some()
                || args.id.is_some()
            {
                return ToolOutput::failure("Schedule list accepts only action.".into());
            }
            if context
                .list_used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return ToolOutput::failure(
                    "Reminders were already listed for this turn. Use that result now.".into(),
                );
            }
            match list_reminders().await {
                Ok(reminders) => {
                    context
                        .listed_ids
                        .lock()
                        .unwrap()
                        .extend(reminders.iter().map(|reminder| reminder.id.clone()));
                    ToolOutput::success(
                        serde_json::json!({ "status": "ok", "reminders": reminders }).to_string(),
                    )
                }
                Err(error) => ToolOutput::failure(format!("Reminder list unavailable: {error}")),
            }
        }
        "create" => {
            if args.id.is_some() || args.objective.is_some() {
                return ToolOutput::failure(
                    "Schedule create accepts reminder text, not an agent objective.".into(),
                );
            }
            let Some(text) = args.text.map(|text| text.trim().to_string()) else {
                return ToolOutput::failure("Schedule create requires alert text.".into());
            };
            if text.is_empty() || text.chars().count() > 500 {
                return ToolOutput::failure(
                    "Reminder text must contain 1 to 500 characters.".into(),
                );
            }
            let timing = match (args.delay_seconds, args.local_time, args.day) {
                (Some(delay_seconds), None, None) if (1..=31_536_000).contains(&delay_seconds) => {
                    (Some(delay_seconds), None, None)
                }
                (None, Some(local_time), Some(day)) if !local_time.trim().is_empty() => {
                    (None, Some(local_time), Some(day))
                }
                _ => {
                    return ToolOutput::failure(
                        "Schedule create requires either delay_seconds or local_time with day."
                            .into(),
                    )
                }
            };
            if context
                .mutation_used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return ToolOutput::failure(
                    "Only one reminder change is allowed per user turn.".into(),
                );
            }
            let result = create_reminder(
                &context.metadata,
                context.turn,
                text,
                timing.0,
                timing.1,
                timing.2,
            )
            .await;
            finish_schedule_mutation(&context, result, ReminderStatus::Pending)
        }
        "start_at" | "finish_by" => {
            if args.id.is_some() || args.text.is_some() {
                return ToolOutput::failure(
                    "Scheduled agent work accepts an objective and timing, not reminder text or an ID."
                        .into(),
                );
            }
            let Some(objective) = args.objective.map(|value| value.trim().to_string()) else {
                return ToolOutput::failure("Scheduled agent work requires an objective.".into());
            };
            if objective.is_empty() || objective.chars().count() > 4_000 {
                return ToolOutput::failure(
                    "Task objective must contain 1 to 4000 characters.".into(),
                );
            }
            let timing = match (args.delay_seconds, args.local_time, args.day) {
                (Some(delay), None, None) if (1..=31_536_000).contains(&delay) => {
                    (Some(delay), None, None)
                }
                (None, Some(time), Some(day)) if !time.trim().is_empty() => {
                    (None, Some(time), Some(day))
                }
                _ => return ToolOutput::failure(
                    "Scheduled agent work requires either delay_seconds or local_time with day."
                        .into(),
                ),
            };
            if context
                .mutation_used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return ToolOutput::failure(
                    "Only one schedule change is allowed per user turn.".into(),
                );
            }
            let mode = if args.action == "start_at" {
                ScheduledTaskMode::StartAt
            } else {
                ScheduledTaskMode::FinishBy
            };
            let result = create_scheduled_task(
                &context.metadata,
                context.turn,
                objective,
                mode,
                timing.0,
                timing.1,
                timing.2,
            )
            .await;
            let succeeded = result
                .as_ref()
                .is_ok_and(|task| task.status == ScheduledTaskStatus::Pending);
            *context.mutation_succeeded.lock().unwrap() = Some(succeeded);
            match result {
                Ok(task) if succeeded => ToolOutput::success(
                    serde_json::json!({
                        "authoritative": true,
                        "scheduled_task": task,
                        "instruction": "The scheduled task is durably committed and may be confirmed naturally."
                    })
                    .to_string(),
                ),
                Ok(task) => ToolOutput::failure(
                    serde_json::json!({
                        "authoritative": true,
                        "scheduled_task": task,
                        "instruction": "The task did not reach pending state; do not claim success."
                    })
                    .to_string(),
                ),
                Err(error) => ToolOutput::failure(format!(
                    "Scheduled task creation failed; do not claim success: {error}"
                )),
            }
        }
        "cancel" => {
            if args.text.is_some()
                || args.objective.is_some()
                || args.delay_seconds.is_some()
                || args.local_time.is_some()
                || args.day.is_some()
            {
                return ToolOutput::failure("Schedule cancel accepts only action and ID.".into());
            }
            let Some(id) = args.id.filter(|id| !id.trim().is_empty()) else {
                return ToolOutput::failure("Schedule cancel requires an ID.".into());
            };
            if !context.listed_ids.lock().unwrap().contains(&id) {
                return ToolOutput::failure(
                    "Cancel requires an exact reminder ID returned by list earlier in this turn."
                        .into(),
                );
            }
            if context
                .mutation_used
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return ToolOutput::failure(
                    "Only one reminder change is allowed per user turn.".into(),
                );
            }
            let result = cancel_reminder(&context.metadata, context.turn, id).await;
            finish_schedule_mutation(&context, result, ReminderStatus::Cancelled)
        }
        _ => ToolOutput::failure(
            "Schedule action must be create, list, cancel, start_at, or finish_by.".into(),
        ),
    }
}

fn finish_schedule_mutation(
    context: &ScheduleToolContext,
    result: Result<ReminderInfo, String>,
    expected_status: ReminderStatus,
) -> ToolOutput {
    let succeeded = result
        .as_ref()
        .is_ok_and(|reminder| reminder.status == expected_status);
    *context.mutation_succeeded.lock().unwrap() = Some(succeeded);
    match result {
        Ok(reminder) if succeeded => ToolOutput::success(
            serde_json::json!({
                "authoritative": true,
                "reminder": reminder,
                "instruction": "The reminder change is committed and may be confirmed naturally."
            })
            .to_string(),
        ),
        Ok(reminder) => ToolOutput::failure(
            serde_json::json!({
                "authoritative": true,
                "reminder": reminder,
                "instruction": "The reminder change did not reach the requested state; do not claim success."
            })
            .to_string(),
        ),
        Err(error) => ToolOutput::failure(format!(
            "Reminder change failed; do not claim success: {error}"
        )),
    }
}

fn memory_intent_from_tool(
    args: MemoryToolArgs,
    recalled_ids: &Arc<Mutex<BTreeSet<String>>>,
) -> Result<MemoryIntent, String> {
    let descriptor = || -> Result<MemoryDescriptor, String> {
        let namespace = args
            .namespace
            .clone()
            .filter(|value| valid_memory_field(value, 100))
            .ok_or_else(|| "Memory namespace must contain 1 to 100 characters.".to_string())?;
        let relation = args
            .relation
            .clone()
            .filter(|value| valid_memory_field(value, 100))
            .ok_or_else(|| "Memory relation must contain 1 to 100 characters.".to_string())?;
        let scope = args
            .scope
            .clone()
            .filter(|value| valid_memory_field(value, 100))
            .ok_or_else(|| "Memory scope must contain 1 to 100 characters.".to_string())?;
        if args.topics.len() > 8
            || args
                .topics
                .iter()
                .any(|topic| !valid_memory_field(topic, 50))
        {
            return Err(
                "Memory topics must contain at most 8 values of 1 to 50 characters.".into(),
            );
        }
        Ok(MemoryDescriptor {
            kind: args
                .kind
                .ok_or_else(|| "Memory kind is required.".to_string())?,
            namespace,
            relation,
            scope,
            cardinality: args
                .cardinality
                .ok_or_else(|| "Memory cardinality is required.".to_string())?,
            topics: args.topics.clone(),
        })
    };
    let value = || {
        args.value
            .clone()
            .filter(|value| valid_memory_field(value, 1000))
            .ok_or_else(|| "Memory value must contain 1 to 1000 characters.".to_string())
    };
    match args.action.as_str() {
        "remember" => {
            if args.target_ids.is_some() {
                return Err("Remember does not accept target IDs.".into());
            }
            Ok(MemoryIntent::Remember {
                descriptor: descriptor()?,
                value: value()?,
            })
        }
        "forget" => {
            if args.value.is_some()
                || args.kind.is_some()
                || args.namespace.is_some()
                || args.relation.is_some()
                || args.scope.is_some()
                || args.cardinality.is_some()
                || !args.topics.is_empty()
            {
                return Err("Forget accepts only action and recalled target IDs.".into());
            }
            Ok(MemoryIntent::Forget {
                target_ids: validated_memory_targets(args.target_ids, recalled_ids)?,
            })
        }
        "correct" => Ok(MemoryIntent::Correct {
            target_ids: validated_memory_targets(args.target_ids, recalled_ids)?,
            descriptor: descriptor()?,
            value: value()?,
        }),
        _ => Err("Memory action must be recall, remember, forget, or correct.".into()),
    }
}

fn validated_memory_targets(
    target_ids: Option<Vec<String>>,
    recalled_ids: &Arc<Mutex<BTreeSet<String>>>,
) -> Result<Vec<String>, String> {
    let target_ids = target_ids
        .filter(|ids| !ids.is_empty() && ids.len() <= 8)
        .ok_or_else(|| {
            "Forget and correct require 1 to 8 target IDs from a prior recall in this turn."
                .to_string()
        })?;
    let unique = target_ids.iter().collect::<BTreeSet<_>>();
    let recalled = recalled_ids.lock().unwrap();
    if unique.len() != target_ids.len() || target_ids.iter().any(|id| !recalled.contains(id)) {
        return Err(
            "Every target ID must be unique and returned by a prior recall in this turn.".into(),
        );
    }
    Ok(target_ids)
}

fn valid_memory_field(value: &str, max_chars: usize) -> bool {
    let count = value.trim().chars().count();
    count > 0 && count <= max_chars
}

fn compose_fanout_output(tasks: &[String], outcomes: Vec<ToolOutput>) -> ToolOutput {
    let succeeded = outcomes.iter().filter(|outcome| outcome.succeeded).count();
    let status = if succeeded == tasks.len() {
        "complete"
    } else if succeeded == 0 {
        "failed"
    } else {
        "partial"
    };
    let mut evidence = Vec::new();
    let mut failures = Vec::new();
    let mut outcomes = outcomes.into_iter();
    for (index, objective) in tasks.iter().enumerate() {
        let outcome = outcomes.next().unwrap_or_else(|| {
            ToolOutput::failure("worker returned no outcome for this objective".into())
        });
        let outcome_status = if outcome.succeeded {
            "succeeded"
        } else {
            "failed"
        };
        let entry = format!(
            "Objective {} [{outcome_status}]: {}\n{}",
            index + 1,
            objective,
            outcome.text
        );
        if outcome.succeeded {
            evidence.push(entry);
        } else {
            failures.push(entry);
        }
    }

    let mut sections = vec![format!(
        "Coverage: {status} ({succeeded}/{} objectives succeeded).",
        tasks.len()
    )];
    if !evidence.is_empty() {
        sections.push(format!("Valid evidence:\n{}", evidence.join("\n\n")));
    }
    if !failures.is_empty() {
        sections.push(format!(
            "Failed objectives (not evidence):\n{}",
            failures.join("\n\n")
        ));
    }
    ToolOutput {
        text: sections.join("\n\n"),
        succeeded: succeeded > 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turns::durable_turn_messages;
    use tachyon_api::FOREGROUND_ID;

    fn interaction_metadata() -> tachyon_api::InteractionMetadata {
        tachyon_api::InteractionMetadata::new("command-1", "turn-1", FOREGROUND_ID, 1)
    }
    #[test]
    fn delegation_calls_produce_provider_neutral_task_intents() {
        let call = ToolCall {
            id: "call-1".into(),
            name: "spawn_agents".into(),
            arguments: serde_json::json!({
                "tasks": ["London weather", "Tokyo weather"],
                "lifetime_class": "short"
            })
            .to_string(),
        };
        let intents = task_intents(&call);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].objective, "London weather");
        assert_eq!(intents[1].objective, "Tokyo weather");
        assert_eq!(intents[0].lifetime_class, LifetimeClass::Short);
    }

    #[test]
    fn memory_mutations_require_valid_shapes_and_recalled_targets() {
        let recalled = Arc::new(Mutex::new(BTreeSet::from(["preference-1".into()])));
        let remember: MemoryToolArgs = serde_json::from_str(
            r#"{"action":"remember","value":"likes pizza","kind":"preference","namespace":"personal.food","relation":"likes","scope":"global","cardinality":"many","topics":["pizza"]}"#,
        )
        .unwrap();
        assert!(matches!(
            memory_intent_from_tool(remember, &recalled),
            Ok(MemoryIntent::Remember { .. })
        ));

        let forget: MemoryToolArgs =
            serde_json::from_str(r#"{"action":"forget","target_ids":["preference-1"]}"#).unwrap();
        assert!(matches!(
            memory_intent_from_tool(forget, &recalled),
            Ok(MemoryIntent::Forget { .. })
        ));

        let invented: MemoryToolArgs =
            serde_json::from_str(r#"{"action":"forget","target_ids":["invented"]}"#).unwrap();
        assert!(memory_intent_from_tool(invented, &recalled).is_err());
        assert!(durable_turn_messages("question".into(), "answer".into())
            .iter()
            .all(|message| !message.plain().contains("preference-1")));
    }

    #[test]
    fn conversation_advertises_and_authorizes_contextual_services() {
        let tools = AgentRole::Conversation.tools(false).unwrap();
        assert!(tools.iter().any(|tool| tool.name == "memory"));
        assert!(tools.iter().any(|tool| tool.name == "schedule"));
        assert!(AgentRole::Conversation.allows_tool("memory").unwrap());
        assert!(AgentRole::Conversation.allows_tool("schedule").unwrap());
        assert!(serde_json::from_str::<MemoryToolArgs>(
            r#"{"action":"recall","query":"relevant food preferences","include_history":false}"#
        )
        .is_ok());
        assert!(serde_json::from_str::<MemoryToolArgs>(
            r#"{"action":"recall","query":"preferences","unexpected":true}"#
        )
        .is_err());
        assert!(serde_json::from_str::<ScheduleToolArgs>(
            r#"{"action":"create","text":"Your coffee is ready.","delay_seconds":60}"#
        )
        .is_ok());
    }

    #[test]
    fn memory_recall_is_removed_after_one_contextual_lookup() {
        let context = MemoryToolContext {
            metadata: interaction_metadata(),
            turn: 1,
            recalled_ids: Arc::new(Mutex::new(BTreeSet::new())),
            recall_used: Arc::new(AtomicBool::new(true)),
            mutation_used: Arc::new(AtomicBool::new(false)),
            mutation_succeeded: Arc::new(Mutex::new(None)),
        };
        let tools = available_tools_for_context(
            &AgentRole::Conversation.tools(false).unwrap(),
            Some(&context),
            None,
        );
        let memory = tools.iter().find(|tool| tool.name == "memory").unwrap();
        assert!(!memory.parameters["properties"]["action"]["enum"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("recall")));
        assert_eq!(memory.parameters["oneOf"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn contextual_filters_preserve_registry_order_and_unaffected_schemas() {
        let primary = AgentRole::Conversation.tools(false).unwrap();
        let memory = MemoryToolContext {
            metadata: interaction_metadata(),
            turn: 1,
            recalled_ids: Arc::new(Mutex::new(BTreeSet::new())),
            recall_used: Arc::new(AtomicBool::new(true)),
            mutation_used: Arc::new(AtomicBool::new(false)),
            mutation_succeeded: Arc::new(Mutex::new(None)),
        };
        let schedule = ScheduleToolContext {
            metadata: interaction_metadata(),
            turn: 1,
            listed_ids: Arc::new(Mutex::new(BTreeSet::new())),
            list_used: Arc::new(AtomicBool::new(true)),
            mutation_used: Arc::new(AtomicBool::new(false)),
            mutation_succeeded: Arc::new(Mutex::new(None)),
        };
        for (memory_done, schedule_done, names) in [
            (
                None,
                None,
                vec!["spawn_agent", "spawn_agents", "memory", "schedule"],
            ),
            (
                Some(true),
                None,
                vec!["spawn_agent", "spawn_agents", "schedule"],
            ),
            (
                None,
                Some(false),
                vec!["spawn_agent", "spawn_agents", "memory"],
            ),
            (Some(false), Some(true), vec!["spawn_agent", "spawn_agents"]),
        ] {
            *memory.mutation_succeeded.lock().unwrap() = memory_done;
            *schedule.mutation_succeeded.lock().unwrap() = schedule_done;
            let actual = available_tools_for_context(&primary, Some(&memory), Some(&schedule));
            assert_eq!(
                actual
                    .iter()
                    .map(|tool| tool.name.as_str())
                    .collect::<Vec<_>>(),
                names
            );
            for (actual, expected) in actual[..2].iter().zip(&primary[..2]) {
                assert_eq!(actual.description, expected.description);
                assert_eq!(actual.parameters, expected.parameters);
            }
            if let Some(tool) = actual.iter().find(|tool| tool.name == "schedule") {
                let mut expected = primary[3].parameters.clone();
                expected["properties"]["action"]["enum"] =
                    serde_json::json!(["create", "cancel", "start_at", "finish_by"]);
                expected["oneOf"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|action| action["properties"]["action"]["const"] != "list");
                assert_eq!(tool.parameters, expected);
                assert_eq!(tool.description, primary[3].description);
            }
            if let Some(tool) = actual.iter().find(|tool| tool.name == "memory") {
                let mut expected = primary[2].parameters.clone();
                expected["properties"]["action"]["enum"] =
                    serde_json::json!(["remember", "forget", "correct"]);
                expected["oneOf"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|action| action["properties"]["action"]["const"] != "recall");
                assert_eq!(tool.parameters, expected);
            }
        }
    }

    #[test]
    fn fanout_output_preserves_partial_coverage_and_separates_failures() {
        let tasks = vec![
            "verify package release".into(),
            "inspect deployment status".into(),
            "check security advisory".into(),
        ];
        let output = compose_fanout_output(
            &tasks,
            vec![
                ToolOutput::success("release evidence".into()),
                ToolOutput::failure("worker timed out".into()),
                ToolOutput::success("advisory evidence".into()),
            ],
        );

        assert!(output.succeeded);
        assert!(output
            .text
            .contains("Coverage: partial (2/3 objectives succeeded)."));
        let evidence = output.text.find("Valid evidence:").unwrap();
        let failures = output
            .text
            .find("Failed objectives (not evidence):")
            .unwrap();
        assert!(evidence < failures);
        assert!(output.text[..failures].contains("verify package release"));
        assert!(output.text[..failures].contains("check security advisory"));
        assert!(!output.text[..failures].contains("worker timed out"));
        assert!(output.text[failures..].contains("inspect deployment status"));
        assert!(output.text[failures..].contains("worker timed out"));
    }
}
