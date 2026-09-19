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
    pub(super) web_usage: Option<tachyon_api::web::WebUsage>,
    pub(super) text: String,
    pub(super) succeeded: bool,
    pub(super) task_outcomes: Vec<TaskOutcome>,
}

/// Deterministic host projection. The daemon retains the original typed result.
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WebOutcome {
    pub(super) tool_call_id: String,
    pub(super) query: String,
    pub(super) freshness: String,
    pub(super) evidence_notice: String,
    pub(super) omitted: usize,
    pub(super) result: tachyon_api::web::WebResult,
}

impl WebOutcome {
    fn bounded(
        call: &ToolCall,
        request: &tachyon_api::web::WebRequest,
        result: tachyon_api::web::WebResult,
    ) -> Self {
        let mut out = Self {
            tool_call_id: call.id.clone(),
            query: match request {
                tachyon_api::web::WebRequest::Search { query, .. } => query.chars().take(512).collect(),
                tachyon_api::web::WebRequest::Fetch { .. } => "Known URL retrieval; see requested_urls".into(),
            },
            freshness: "unknown".into(),
            evidence_notice: "Model-mediated report, not raw pages or automatically source-verified. Partial/unverified reports do not confirm retrieval. host_observed_at is receipt time, not publication time. Offsets refer only to original provider text, never this projection; source_index is provider metadata, not a verified source. Full evidence retained in daemon record.".into(),
            omitted: 0,
            result,
        };
        assert!(out.fit(8192), "validated web envelope fits tool budget");
        out
    }

    pub(super) fn fit(&mut self, budget: usize) -> bool {
        // Typed citations already contain the known annotation fields. Do not
        // spend model context on a second copy of potentially large excerpts.
        self.omitted += self.result.annotations.len();
        self.result.annotations.clear();
        while !json_fits(self, budget) {
            self.omitted += 1;
            if !self.result.answer.is_empty() {
                self.result.answer = self
                    .result
                    .answer
                    .chars()
                    .take(self.result.answer.chars().count() / 2)
                    .collect();
                for citation in &mut self.result.citations {
                    citation.start_index = None;
                    citation.end_index = None;
                }
                continue;
            }
            // Reduce the largest excerpt first so one large source cannot evict
            // the titles and excerpts of every other source.
            if let Some(text) = self
                .result
                .citations
                .iter_mut()
                .filter_map(|c| c.excerpt.as_mut())
                .filter(|s| !s.is_empty())
                .max_by_key(|s| s.len())
            {
                *text = text.chars().take(text.chars().count() / 2).collect();
                continue;
            }
            if let Some(text) = self
                .result
                .citations
                .iter_mut()
                .filter_map(|c| c.title.as_mut())
                .filter(|s| !s.is_empty())
                .max_by_key(|s| s.len())
            {
                *text = text.chars().take(text.chars().count() / 2).collect();
                continue;
            }
            if self.result.requested_urls.pop().is_some() {
                continue;
            }
            if self.result.citations.pop().is_some() {
                continue;
            }
            if !self.query.is_empty() {
                self.query = self
                    .query
                    .chars()
                    .take(self.query.chars().count() / 2)
                    .collect();
                continue;
            }
            if !self.result.notice.is_empty() {
                self.result.notice = self
                    .result
                    .notice
                    .chars()
                    .take(self.result.notice.chars().count() / 2)
                    .collect();
                continue;
            }
            return false;
        }
        true
    }
}

/// Native delegation results, not fields recovered from arbitrary worker prose.
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TaskOutcome {
    pub(super) objective: String,
    pub(super) result: Option<String>,
    pub(super) completed_scopes: Option<Vec<String>>,
    pub(super) failure_reason: Option<String>,
    #[serde(default)]
    pub(super) evidence: tachyon_api::types::WorkEvidence,
}

#[derive(serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerEvidence {
    pub(super) task_outcomes: Vec<TaskOutcome>,
    #[serde(default)]
    pub(super) omitted: usize,
}

pub(super) const MAX_WORKER_EVIDENCE_BYTES: usize = 1024 * 1024;

/// Count encoded bytes without allocating a serialized copy of a raw report.
pub(super) fn json_fits(value: &impl serde::Serialize, budget: usize) -> bool {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0 {
                return Err(std::io::ErrorKind::FileTooLarge.into());
            }
            self.0 -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(budget), value).is_ok()
}

impl TaskOutcome {
    pub(super) fn from_work_result(work: &tachyon_api::WorkResult) -> Self {
        use tachyon_api::types::WorkOutcome;
        let (result, failure_reason) = match &work.outcome {
            WorkOutcome::Completed { result, .. } => (Some(result.clone()), None),
            WorkOutcome::Failed { message } => (None, Some(message.clone())),
            WorkOutcome::Blocked { reason } | WorkOutcome::Cancelled { reason } => {
                (None, Some(reason.clone()))
            }
            WorkOutcome::TimedOut { .. } => {
                (None, Some("The lookup exceeded its deadline.".into()))
            }
        };
        Self {
            objective: work.objective.clone(),
            result,
            completed_scopes: None,
            failure_reason,
            evidence: work.evidence.clone(),
        }
    }
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
    pub(super) fn account_web_usage(
        &self,
        usage: &mut tachyon_model::TokenUsage,
        seen: &mut BTreeSet<String>,
    ) {
        if let Some(receipt) = &self.web_usage {
            if let (Some(id), Some(input), Some(output)) = (
                &receipt.receipt_id,
                receipt.input_tokens,
                receipt.output_tokens,
            ) {
                if receipt.valid() && seen.insert(id.clone()) {
                    *usage += tachyon_model::TokenUsage {
                        prompt_tokens: input.min(u64::from(u32::MAX)) as u32,
                        completion_tokens: output.min(u64::from(u32::MAX)) as u32,
                        total_tokens: input.saturating_add(output).min(u64::from(u32::MAX)) as u32,
                        ..Default::default()
                    };
                }
            }
        }
    }
    fn success(text: String) -> Self {
        Self {
            web_usage: None,
            text,
            succeeded: true,
            task_outcomes: Vec::new(),
        }
    }

    fn failure(text: String) -> Self {
        Self {
            web_usage: None,
            text,
            succeeded: false,
            task_outcomes: Vec::new(),
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
    metadata: Option<&InteractionMetadata>,
) -> ToolOutput {
    match role.allows_tool(&tc.name, metadata) {
        Ok(true) => {}
        Ok(false) => {
            return ToolOutput::failure(format!(
                "{} is not available to the {role:?} role",
                tc.name
            ));
        }
        Err(error) => return ToolOutput::failure(error),
    }
    match tc.name.as_str() {
        "websearch" | "webfetch" => {
            let Some(metadata) = metadata
                .filter(|m| turn.is_some() && !m.conversation_id.is_empty() && m.turn_id.is_some())
            else {
                return ToolOutput::failure(
                    "Web retrieval requires a current conversation turn.".into(),
                );
            };
            let request = match serde_json::from_str(&tc.arguments)
                .map_err(|_| "invalid JSON")
                .and_then(|v| tachyon_api::web::WebRequest::from_tool_input(&tc.name, v))
            {
                Ok(request) => request,
                _ => return ToolOutput::failure("Invalid web request kind or payload.".into()),
            };
            let command = tachyon_api::web::WebCommand {
                command_id: tc.id.clone(),
                caller_id: metadata.conversation_id.clone(),
                tool_call_id: tc.id.clone(),
                turn_id: metadata.turn_id.clone().unwrap(),
                request_id: tc.id.clone(),
                request: request.clone(),
            };
            if let Err(error) = command.validate() {
                return ToolOutput::failure(error.into());
            }
            match crate::delegation::web_for_turn(metadata.clone(), command).await {
                Ok(result) => {
                    if !result.usage.valid() { return ToolOutput::failure("Invalid web usage receipt.".into()); }
                    let succeeded = !matches!(result.status, tachyon_api::web::WebStatus::Failed);
                    ToolOutput { web_usage: Some(result.usage.clone()), text: serde_json::to_string(&WebOutcome::bounded(tc, &request, result)).unwrap(), succeeded, task_outcomes: vec![] }
                }
                Err(error) => ToolOutput::failure(serde_json::json!({"error":error,"freshness":"unknown","instruction":"Retrieval not confirmed; do not claim verification."}).to_string()),
            }
        }
        "campaign" => {
            let Some(metadata) =
                metadata.filter(|m| turn.is_some() && !m.conversation_id.is_empty())
            else {
                return ToolOutput::failure(
                    "Campaign access requires a current conversation turn.".into(),
                );
            };
            let request =
                serde_json::from_str::<tachyon_api::conversation_campaign::Request>(&tc.arguments)
                    .map_err(|e| e.to_string())
                    .and_then(|r| {
                        r.validate()?;
                        Ok(r)
                    });
            let result = match request {
                Ok(request) => {
                    crate::delegation::campaign_for_turn(metadata.clone(), request).await
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(response) => ToolOutput::success(response.to_string()),
                Err(error) => ToolOutput::failure(serde_json::json!({"error":error,"instruction":"No change confirmed; do not claim Done."}).to_string()),
            }
        }
        "todo" => {
            let Some(metadata) = metadata.filter(|_| turn.is_some()) else {
                return todo_failure(tachyon_api::todo::TodoError::AuthorityDenied);
            };
            run_todo_tool(&tc.arguments, metadata).await
        }
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
            let requests = match delegation_requests(
                tc,
                turn,
                metadata.and_then(|metadata| metadata.cwd.clone()),
            ) {
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
            let outcomes: Vec<_> = results
                .into_iter()
                .map(|result| match result {
                    Ok(Ok(work)) => ToolOutput {
                        web_usage: None,
                        text: String::new(),
                        succeeded: true,
                        task_outcomes: vec![TaskOutcome::from_work_result(&work)],
                    },
                    Ok(Err(error)) => ToolOutput::failure(format!("worker failed: {error}")),
                    Err(error) => ToolOutput::failure(format!("worker task failed: {error}")),
                })
                .collect();
            compose_fanout_output(&tasks, outcomes)
        }
        other => ToolOutput::failure(format!("unknown tool: {other}")),
    }
}

fn todo_failure(error: tachyon_api::todo::TodoError) -> ToolOutput {
    ToolOutput::failure(serde_json::json!({"error":error}).to_string())
}

async fn run_todo_tool(arguments: &str, metadata: &InteractionMetadata) -> ToolOutput {
    use tachyon_api::todo::{TodoError, TodoRequest, TodoScope};
    // Replace only the identity-free selector, then let the shared strict typed
    // request reject unknown fields and fields belonging to other operations.
    let request = (|| {
        let mut value: serde_json::Value =
            serde_json::from_str(arguments).map_err(|error| TodoError::Invalid {
                message: error.to_string(),
            })?;
        let object = value.as_object_mut().ok_or_else(|| TodoError::Invalid {
            message: "todo arguments must be an object".into(),
        })?;
        if object
            .get("scope")
            .is_some_and(|scope| scope.as_str() != Some("current_conversation"))
        {
            return Err(TodoError::AuthorityDenied);
        }
        if metadata.conversation_id.trim().is_empty() {
            return Err(TodoError::AuthorityDenied);
        }
        let scope = TodoScope::Conversation {
            id: metadata.conversation_id.clone(),
        };
        object.insert("scope".into(), serde_json::to_value(&scope).unwrap());
        let request: TodoRequest =
            serde_json::from_value(value).map_err(|error| TodoError::Invalid {
                message: error.to_string(),
            })?;
        if let TodoRequest::List {
            cursor: Some(cursor),
            ..
        } = &request
        {
            if cursor.scope != scope {
                return Err(TodoError::AuthorityDenied);
            }
        }
        Ok(request)
    })();
    let request = match request {
        Ok(request) => request,
        Err(error) => return todo_failure(error),
    };
    match crate::delegation::todo_for_turn(request).await {
        Ok(response) => ToolOutput::success(serde_json::to_string(&response).unwrap()),
        Err(error) => todo_failure(error),
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
                return ToolOutput::failure(format!("Memory recall unavailable: {error}"));
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
                    );
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
    let mut outcomes = outcomes.into_iter();
    let mut task_outcomes = Vec::new();
    for objective in tasks {
        let outcome = outcomes.next().unwrap_or_else(|| {
            ToolOutput::failure("worker returned no outcome for this objective".into())
        });
        if !outcome.task_outcomes.is_empty() {
            task_outcomes.extend(outcome.task_outcomes);
            continue;
        }
        task_outcomes.push(TaskOutcome {
            objective: objective.clone(),
            result: outcome.succeeded.then(|| outcome.text.clone()),
            completed_scopes: None,
            failure_reason: (!outcome.succeeded).then_some(outcome.text),
            evidence: Default::default(),
        });
    }
    ToolOutput {
        web_usage: None,
        text: serde_json::to_string(&WorkerEvidence {
            task_outcomes: task_outcomes.clone(),
            omitted: 0,
        })
        .expect("native worker evidence is serializable"),
        succeeded: task_outcomes.iter().any(|outcome| outcome.result.is_some()),
        task_outcomes,
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::turns::durable_turn_messages;
    use tachyon_api::FOREGROUND_ID;

    fn interaction_metadata() -> tachyon_api::InteractionMetadata {
        tachyon_api::InteractionMetadata::new("command-1", "turn-1", FOREGROUND_ID, 1)
    }

    #[test]
    fn web_usage_receipts_deduplicate_tokens_and_preserve_unknown_cost() {
        let mut out = ToolOutput::success("report".into());
        out.web_usage = Some(tachyon_api::web::WebUsage {
            receipt_id: Some("host-receipt".into()),
            input_tokens: Some(10),
            output_tokens: Some(2),
            cost_micro_usd: None,
        });
        let mut usage = tachyon_model::TokenUsage::default();
        let mut seen = BTreeSet::new();
        out.account_web_usage(&mut usage, &mut seen);
        out.account_web_usage(&mut usage, &mut seen);
        assert_eq!(
            (
                usage.prompt_tokens,
                usage.completion_tokens,
                usage.total_tokens
            ),
            (10, 2, 12)
        );
        assert_eq!(out.web_usage.as_ref().unwrap().cost_micro_usd, None);
        out.web_usage.as_mut().unwrap().cost_micro_usd = Some(7001);
        out.account_web_usage(&mut usage, &mut seen);
        assert_eq!(usage.total_tokens, 12);
        out.web_usage = Some(tachyon_api::web::WebUsage {
            receipt_id: Some("unknown".into()),
            ..Default::default()
        });
        out.account_web_usage(&mut usage, &mut seen);
        assert!(!seen.contains("unknown"));
        let mut new_turn = BTreeSet::new();
        out.web_usage = Some(tachyon_api::web::WebUsage {
            receipt_id: Some("fresh-root-receipt".into()),
            input_tokens: Some(3),
            output_tokens: Some(1),
            cost_micro_usd: Some(100),
        });
        out.account_web_usage(&mut usage, &mut new_turn);
        assert_eq!(usage.total_tokens, 16);
    }

    pub(crate) fn web_result() -> tachyon_api::web::WebResult {
        tachyon_api::web::WebResult {
            usage: Default::default(),
            answer: "A model-mediated summary.".into(),
            citations: vec![tachyon_api::web::Citation {
                url: "https://arxiv.org/html/2401.00001".into(),
                title: Some("Paper".into()),
                excerpt: Some("Source snippet".into()),
                source_index: Some(1),
                start_index: None,
                end_index: None,
            }],
            annotations: vec![],
            status: tachyon_api::web::WebStatus::Grounded,
            notice: "Grounded does not establish per-URL success.".into(),
            host_observed_at: 42,
            observed_search_uses: Some(1),
            observed_fetch_uses: None,
            requested_urls: vec![],
        }
    }

    #[tokio::test]
    async fn web_disabled_invalid_and_retries_preserve_authority() {
        let mut cfg = tachyon_util::config::Config::default();
        cfg.web.enabled = false;
        let primary = AgentRole::Conversation
            .primary(&tachyon_orchestrator::registry::builtin(), &cfg)
            .unwrap();
        assert!(!primary
            .tools
            .iter()
            .any(|tool| matches!(tool.name.as_str(), "websearch" | "webfetch")));
        let mut metadata = interaction_metadata();
        metadata.turn_id = Some("turn-1".into());
        let call = ToolCall {
            id: "native-1".into(),
            name: "websearch".into(),
            arguments: r#"{"query":"current facts"}"#.into(),
        };
        {
            assert!(!AgentRole::Conversation
                .tools(false, Some(&metadata))
                .unwrap()
                .iter()
                .any(|t| t.name.starts_with("web")));
            assert!(
                !run_tool(
                    &call,
                    AgentRole::Conversation,
                    true,
                    Some(1),
                    true,
                    None,
                    true,
                    None,
                    Some(&metadata)
                )
                .await
                .succeeded
            );
        }
        metadata.web_availability = Some(tachyon_api::interaction::WebAvailability {
            available: true,
            reason: None,
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        crate::delegation::WEB_SERVICE
            .scope(
                Arc::new(move |metadata, command| {
                    captured.lock().unwrap().push((metadata, command));
                    Ok(web_result())
                }),
                async {
                    for arguments in [
                        r#"{"kind":"fetch","urls":["https://arxiv.org/pdf/2401.00001"]}"#,
                        r#"{"kind":"search","query":"x","model":"invented"}"#,
                        r#"{"kind":"search","query":"x","caller_id":"other"}"#,
                    ] {
                        let invalid = ToolCall {
                            arguments: arguments.into(),
                            ..call.clone()
                        };
                        assert!(
                            !run_tool(
                                &invalid,
                                AgentRole::Conversation,
                                true,
                                Some(1),
                                true,
                                None,
                                true,
                                None,
                                Some(&metadata)
                            )
                            .await
                            .succeeded
                        );
                    }
                    assert!(calls.lock().unwrap().is_empty());
                    for _ in 0..2 {
                        assert!(
                            run_tool(
                                &call,
                                AgentRole::Conversation,
                                true,
                                Some(1),
                                true,
                                None,
                                true,
                                None,
                                Some(&metadata)
                            )
                            .await
                            .succeeded
                        );
                    }
                },
            )
            .await;
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0], calls[1]);
        assert_eq!(calls[0].0, metadata);
        assert_eq!(calls[0].1.command_id, call.id);
    }

    #[tokio::test]
    async fn web_failure_diagnostic_survives_foreground_tool_result() {
        let mut metadata = interaction_metadata();
        metadata.turn_id = Some("turn-1".into());
        metadata.web_availability = Some(tachyon_api::interaction::WebAvailability {
            available: true,
            reason: None,
        });
        let call = ToolCall {
            id: "rejected".into(),
            name: "websearch".into(),
            arguments: r#"{"query":"fixture"}"#.into(),
        };
        let diagnostic = "web retrieval failed: provider HTTP 400; request id fixture-request-400; spend may be unknown";
        crate::delegation::WEB_SERVICE
            .scope(Arc::new(move |_, _| Err(diagnostic.into())), async {
                let result = run_tool(
                    &call,
                    AgentRole::Conversation,
                    true,
                    Some(1),
                    true,
                    None,
                    true,
                    None,
                    Some(&metadata),
                )
                .await;
                assert!(!result.succeeded);
                assert!(result.text.contains(diagnostic));
                assert!(result.web_usage.is_none());
            })
            .await;
    }

    #[test]
    fn review_synthesis_shares_space_across_web_results() {
        use tachyon_model::{ChatMessage, Content, Role};
        let mut messages = vec![ChatMessage::new(Role::User, "Compare these four sources")];
        for index in 0..4 {
            let call = ToolCall {
                id: format!("web-{index}"),
                name: "websearch".into(),
                arguments: String::new(),
            };
            let request = tachyon_api::web::WebRequest::Search {
                query: format!("source {index}"),
                domains: None,
                max_results: 3,
            };
            let mut result = web_result();
            result.answer = "report prose ".repeat(1200);
            result.citations[0].url = format!("https://example.org/source-{index}");
            result.citations[0].title = Some(format!("Observed title {index}"));
            result.citations[0].excerpt = Some(format!("Observed excerpt {index}"));
            result.citations[0].source_index = Some(index);
            result.citations[0].start_index = Some(42);
            result.citations[0].end_index = Some(84);
            result.status = tachyon_api::web::WebStatus::Partial;
            let outcome = WebOutcome::bounded(&call, &request, result);
            assert!(json_fits(&outcome, 8192));
            let output = serde_json::to_string(&outcome).unwrap();
            messages.push(ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(call.clone())],
            });
            messages.push(ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: call.id,
                    output,
                }],
            });
        }
        let brief =
            serde_json::to_value(crate::model::SynthesisBrief::from_messages(&messages, None))
                .unwrap();
        assert_eq!(
            brief["web"].as_array().unwrap().len(),
            4,
            "share prose budget rather than discard later sources and their citations"
        );
        assert!(json_fits(&brief, 8192));
        for (index, outcome) in brief["web"].as_array().unwrap().iter().enumerate() {
            assert_eq!(outcome["result"]["status"], "partial");
            assert_eq!(
                outcome["result"]["citations"][0]["url"],
                format!("https://example.org/source-{index}")
            );
            assert_eq!(
                outcome["result"]["citations"][0]["title"],
                format!("Observed title {index}")
            );
            assert_eq!(
                outcome["result"]["citations"][0]["excerpt"],
                format!("Observed excerpt {index}")
            );
            assert_eq!(outcome["result"]["citations"][0]["source_index"], index);
            assert!(outcome["result"]["citations"][0]["start_index"].is_null());
            assert!(outcome["result"]["citations"][0]["end_index"].is_null());
            assert_eq!(outcome["result"]["annotations"], serde_json::json!([]));
        }
    }

    #[test]
    fn web_projection_prioritizes_citations_and_bounds_encoded_bytes() {
        let call = ToolCall {
            id: "native-1".into(),
            name: "websearch".into(),
            arguments: String::new(),
        };
        let request = tachyon_api::web::WebRequest::Search {
            query: "latest paper".into(),
            domains: None,
            max_results: 3,
        };
        let mut result = web_result();
        result.answer = "\\\"".repeat(16000);
        let out = WebOutcome::bounded(&call, &request, result);
        assert!(json_fits(&out, 8192));
        assert_eq!(out.result.citations.len(), 1);
        assert!(out.omitted > 0);
        assert_eq!(out.freshness, "unknown");
        let mut messages = vec![tachyon_model::ChatMessage::new(
            tachyon_model::Role::User,
            "latest paper",
        )];
        messages.push(tachyon_model::ChatMessage {
            role: tachyon_model::Role::Assistant,
            content: vec![tachyon_model::Content::ToolCall(call)],
        });
        messages.push(tachyon_model::ChatMessage {
            role: tachyon_model::Role::Tool,
            content: vec![tachyon_model::Content::ToolResult {
                id: "native-1".into(),
                output: serde_json::to_string(&WebOutcome::bounded(
                    &ToolCall {
                        id: "native-1".into(),
                        name: "websearch".into(),
                        arguments: String::new(),
                    },
                    &request,
                    web_result(),
                ))
                .unwrap(),
            }],
        });
        let brief =
            serde_json::to_value(crate::model::SynthesisBrief::from_messages(&messages, None))
                .unwrap();
        assert_eq!(brief["web"][0]["tool_call_id"], "native-1");
        messages.remove(1);
        let brief =
            serde_json::to_value(crate::model::SynthesisBrief::from_messages(&messages, None))
                .unwrap();
        assert_eq!(brief["web"], serde_json::json!([]));
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
        let tools = AgentRole::Conversation.tools(false, None).unwrap();
        assert!(tools.iter().any(|tool| tool.name == "memory"));
        assert!(tools.iter().any(|tool| tool.name == "schedule"));
        assert!(AgentRole::Conversation.allows_tool("memory", None).unwrap());
        assert!(AgentRole::Conversation
            .allows_tool("schedule", None)
            .unwrap());
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
            &AgentRole::Conversation.tools(false, None).unwrap(),
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
        let primary = AgentRole::Conversation.tools(false, None).unwrap();
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
        for (memory_done, schedule_done, mut names) in [
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
            names.push("todo");
            names.push("campaign");
            *memory.mutation_succeeded.lock().unwrap() = memory_done;
            *schedule.mutation_succeeded.lock().unwrap() = schedule_done;
            let actual = available_tools_for_context(&primary, Some(&memory), Some(&schedule));
            assert_eq!(
                actual.last().unwrap().parameters,
                primary.last().unwrap().parameters
            );
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

    #[tokio::test]
    async fn todo_rejects_model_authority_and_invalid_mutations_before_transport() {
        use serde_json::json;
        let service = Arc::new(|_: tachyon_api::todo::TodoRequest| -> Result<tachyon_api::todo::TodoResponse, tachyon_api::todo::TodoError> {
            panic!("invalid arguments must not reach a daemon")
        });
        crate::delegation::TODO_SERVICE.scope(service, async {
            for arguments in [
                json!({"operation":"list","scope":"current_work"}),
                json!({"operation":"list","scope":"current_campaign"}),
                json!({"operation":"list","scope":{"kind":"conversation","id":"other"}}),
                json!({"operation":"list","campaign_id":"guessed"}),
                json!({"operation":"list","actor":"operator","role":"host"}),
                json!({"operation":"add","title":"missing revision","command_id":"x"}),
                json!({"operation":"add","title":"missing command","expected_revision":0}),
                json!({"operation":"update","id":"x","status":"completed","command_id":"x"}),
                json!({"operation":"list","title":"not a list field"}),
                json!({"operation":"list","cursor":{"version":1,"instance_id":"x","scope":{"kind":"conversation","id":"other"},"filter":{"status":null,"ids":null},"scope_revision":0,"after_order_key":0,"after_id":"x"}}),
            ] {
                let output = run_todo_tool(&arguments.to_string(), &interaction_metadata()).await;
                assert!(!output.succeeded, "{arguments}");
                assert!(serde_json::from_str::<serde_json::Value>(&output.text).unwrap()["error"].is_object());
            }
        }).await;
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
        let evidence: WorkerEvidence = serde_json::from_str(&output.text).unwrap();
        assert_eq!(evidence.task_outcomes.len(), 3);
        assert_eq!(
            evidence.task_outcomes[0].result.as_deref(),
            Some("release evidence")
        );
        assert_eq!(evidence.task_outcomes[1].result, None);
        assert_eq!(
            evidence.task_outcomes[1].failure_reason.as_deref(),
            Some("worker timed out")
        );
        assert_eq!(
            evidence.task_outcomes[2].result.as_deref(),
            Some("advisory evidence")
        );
    }

    #[tokio::test]
    async fn campaign_native_service_preserves_host_origin_and_rejects_model_authority() {
        use serde_json::json;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let service = Arc::new(move |origin, request| {
            captured.lock().unwrap().push((origin, request));
            Ok(json!({"status":"accepted","accepted_revision":2}))
        });
        crate::delegation::CAMPAIGN_SERVICE.scope(service, async {
            let metadata = interaction_metadata();
            for args in [
                json!({"operation":"list","actor":"root"}),
                json!({"operation":"list","origin":{"conversation_id":"other"}}),
                json!({"operation":"create","objective":"invented"}),
                json!({"operation":"resize","campaign_id":"linked","max_running":10}),
                json!({"operation":"steer","campaign_id":"linked","work_id":"exact","command_id":"x","instructions":"stop"}),
                json!({"operation":"cancel","campaign_id":"linked","work_id":"exact","generation":1}),
                json!({"operation":"status","campaign_id":"linked","limit":33}),
            ] {
                let call = ToolCall { id: "provider-call".into(), name: "campaign".into(), arguments: args.to_string() };
                assert!(!run_tool(&call, AgentRole::Conversation, true, Some(1), true, None, true, None, Some(&metadata)).await.succeeded);
            }
            assert!(calls.lock().unwrap().is_empty());
            let call = ToolCall { id: "provider-call".into(), name: "campaign".into(), arguments: json!({"operation":"steer","campaign_id":"linked","work_id":"exact","command_id":"separate-command","expected_revision":1,"instructions":"stop the requested branch"}).to_string() };
            assert!(!run_tool(&call, AgentRole::Conversation, true, None, true, None, true, None, Some(&metadata)).await.succeeded);
            let output = run_tool(&call, AgentRole::Conversation, true, Some(1), true, None, true, None, Some(&metadata)).await;
            assert!(output.succeeded);
            assert_eq!(serde_json::from_str::<serde_json::Value>(&output.text).unwrap()["status"], "accepted");
            let calls = calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, metadata);
            assert!(matches!(&calls[0].1, tachyon_api::conversation_campaign::Request::Steer { command_id, work_id, .. } if command_id == "separate-command" && work_id == "exact"));
        }).await;
    }
}
