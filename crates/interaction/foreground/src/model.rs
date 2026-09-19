#![forbid(unsafe_code)]

//! Foreground model adapters for conversation policy.

use tachyon_model::{ChatMessage, Completion, Model, ModelError, Role, TokenUsage, ToolSpec};
use tachyon_orchestrator::conversation::policy::{
    Answerability, InteractionDecision, InteractionIntake, ANSWERABILITY_PROMPT,
    CLASSIFICATION_PROMPT,
};
use tachyon_orchestrator::conversation::prompt::{SYNTHESIS_EVIDENCE_GUIDANCE, SYNTHESIS_PROMPT};

const DEFAULT_POLICY_CONTEXT_CHARS: usize = 8_000;
const ANSWERABILITY_TOOL_NAME: &str = "submit_answerability";
const DEFAULT_SYNTHESIS_RESPONSE_BYTES: usize = 1024 * 1024;
const SYNTHESIS_INTERRUPTION: &str = "\n\nThe response was interrupted. Treat this as a partial answer; I couldn't finish verifying the full requested scope.";

/// Allow a direct response or delegation while filtering accidental protocol
/// markup from the user-facing stream.
pub(super) async fn chat_with_delegation(
    model: &Model,
    messages: &[ChatMessage],
    tools: &[ToolSpec],
    publish_direct: bool,
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> std::result::Result<Completion, ModelError> {
    let mut visible = VisibleResponseStream::default();
    let mut buffered = String::new();
    let mut relay = |delta: &str| {
        if let Some(delta) = visible.push(delta) {
            buffered.push_str(&delta);
        }
    };
    let completion = model.chat(messages, Some(tools), &mut relay).await;
    drop(relay);
    if let Some(delta) = visible.finish() {
        buffered.push_str(&delta);
    }
    if completion
        .as_ref()
        .is_ok_and(|completion| should_publish_direct(completion, publish_direct))
        && !visible.protocol_blocked
        && !buffered.is_empty()
    {
        on_delta(&buffered);
    }
    completion
}

fn should_publish_direct(completion: &Completion, publish_direct: bool) -> bool {
    publish_direct && completion.tool_calls.is_empty()
}

#[derive(Default)]
struct VisibleResponseStream {
    pending: String,
    protocol_blocked: bool,
}

impl VisibleResponseStream {
    fn push(&mut self, delta: &str) -> Option<String> {
        if self.protocol_blocked {
            return None;
        }
        self.pending.push_str(delta);
        const MARKERS: [&str; 4] = ["<DSML", "<｜DSML", "</DSML", "</｜DSML"];
        if let Some(marker) = MARKERS
            .iter()
            .filter_map(|marker| self.pending.find(marker))
            .min()
        {
            let safe = self.pending[..marker].to_string();
            self.pending.clear();
            self.protocol_blocked = true;
            return (!safe.is_empty()).then_some(safe);
        }
        // Retain only a possible marker prefix, including across SSE frames.
        // Ordinary angle brackets and mentions of DSML are still normal text.
        if let Some(start) = self
            .pending
            .char_indices()
            .map(|(index, _)| index)
            .find(|&index| {
                MARKERS
                    .iter()
                    .any(|marker| marker.starts_with(&self.pending[index..]))
            })
        {
            let safe = self.pending[..start].to_string();
            self.pending.drain(..start);
            return (!safe.is_empty()).then_some(safe);
        }
        Some(std::mem::take(&mut self.pending))
    }

    fn finish(&mut self) -> Option<String> {
        if self.protocol_blocked {
            return None;
        }
        (!self.pending.is_empty()).then(|| std::mem::take(&mut self.pending))
    }
}

/// Classifies a new message independently of the tool-enabled conversation.
pub(super) async fn classify(
    model: &Model,
    active_turn: &str,
    incoming: &str,
) -> tachyon_model::Result<(InteractionDecision, Option<String>, TokenUsage)> {
    let context = format!("Active turn:\n{active_turn}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, CLASSIFICATION_PROMPT),
        ChatMessage::new(Role::User, context),
    ];
    let mut relay = |_text: &str| {};
    let completion = model.chat(&messages, None, &mut relay).await?;
    let intake = InteractionIntake::parse(&completion.text);
    Ok(match intake {
        Some(intake) => (
            intake.decision,
            Some(intake.acknowledgement),
            completion.usage,
        ),
        None => (
            InteractionDecision::WaitForActiveTurn,
            None,
            completion.usage,
        ),
    })
}

/// Decide whether an active follow-up already has enough evidence to answer.
/// This is deliberately separate from the tool-enabled Conversation loop so a
/// known answer cannot accidentally trigger another worker lookup.
pub(super) async fn assess_answerability(
    model: &Model,
    context: &str,
    incoming: &str,
) -> tachyon_model::Result<(Answerability, TokenUsage, bool)> {
    let input =
        format!("Existing conversation and evidence:\n{context}\n\nIncoming message:\n{incoming}");
    let messages = vec![
        ChatMessage::new(Role::System, ANSWERABILITY_PROMPT),
        ChatMessage::new(Role::User, input),
    ];
    let mut relay = |_text: &str| {};
    let completion = model
        .chat_requiring_tool(
            &messages,
            &[answerability_tool()],
            (ANSWERABILITY_TOOL_NAME, "outcome"),
            &mut relay,
        )
        .await?;
    let answerability = parse_answerability_tool_call(&completion);
    Ok((
        answerability.unwrap_or(Answerability::NeedsNewWork),
        completion.usage,
        answerability.is_none(),
    ))
}

fn answerability_tool() -> ToolSpec {
    ToolSpec::new(
        ANSWERABILITY_TOOL_NAME,
        "Submit the internal answerability classification.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "outcome": {
                    "type": "string",
                    "enum": ["AnswerFromContext", "NeedsNewWork"]
                }
            },
            "required": ["outcome"],
            "additionalProperties": false
        }),
    )
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AnswerabilityToolOutput {
    outcome: Answerability,
}

fn parse_answerability_tool_call(completion: &Completion) -> Option<Answerability> {
    if !completion.text.trim().is_empty() || completion.tool_calls.len() != 1 {
        return None;
    }
    let call = &completion.tool_calls[0];
    if call.name != ANSWERABILITY_TOOL_NAME {
        return None;
    }
    serde_json::from_str::<AnswerabilityToolOutput>(&call.arguments)
        .ok()
        .map(|output| output.outcome)
}

#[derive(Debug)]
pub(super) struct SynthesisFailure {
    pub(super) published_prefix: String,
    pub(super) usage: TokenUsage,
}

impl SynthesisFailure {
    pub(super) fn into_response(self, fallback: impl FnOnce() -> String) -> String {
        if self.published_prefix.is_empty() {
            fallback()
        } else {
            format!("{}{SYNTHESIS_INTERRUPTION}", self.published_prefix)
        }
    }
}

/// Tool-free synthesis streams filtered text immediately. Failures retain the
/// exact published prefix so finalization cannot retract or duplicate it.
pub(super) async fn synthesize_spoken_response(
    model: &Model,
    conversation: &[ChatMessage],
    selected_index: Option<usize>,
    on_delta: &mut (dyn FnMut(&str) + Send),
) -> Result<(String, TokenUsage), SynthesisFailure> {
    let evidence = synthesis_context(conversation, selected_index);
    let messages = vec![
        ChatMessage::new(
            Role::System,
            format!("{SYNTHESIS_PROMPT}\n\n{SYNTHESIS_EVIDENCE_GUIDANCE}"),
        ),
        ChatMessage::new(Role::User, evidence),
    ];
    let max_response_bytes = std::env::var("TACHYON_SYNTHESIS_RESPONSE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.clamp(16 * 1024, 16 * 1024 * 1024))
        .unwrap_or(DEFAULT_SYNTHESIS_RESPONSE_BYTES);
    let mut visible = VisibleResponseStream::default();
    let mut published = String::new();
    let completion = model
        .chat_bounded(&messages, max_response_bytes, &mut |delta| {
            if let Some(text) = visible.push(delta) {
                let text = if published.is_empty() {
                    text.trim_start()
                } else {
                    &text
                };
                if !text.is_empty() {
                    published.push_str(text);
                    on_delta(text);
                }
            }
        })
        .await;
    let usage = completion
        .as_ref()
        .map(|completion| completion.usage)
        .unwrap_or_default();
    if visible.protocol_blocked
        || !visible.pending.is_empty()
        || published.is_empty()
        || !completion.as_ref().is_ok_and(|completion| {
            completion.tool_calls.is_empty()
                && completion
                    .finish_reason
                    .as_deref()
                    .is_none_or(|reason| reason == "stop")
        })
    {
        return Err(SynthesisFailure {
            published_prefix: published,
            usage,
        });
    }
    Ok((published, usage))
}

/// Project model history into policy context without system instructions,
/// tool protocol, or provider-specific serialization.
pub(super) fn policy_context(messages: &[ChatMessage]) -> String {
    let visible = messages
        .iter()
        .filter(|message| matches!(message.role, Role::User | Role::Assistant))
        .filter_map(|message| {
            let text = message.plain();
            (!text.trim().is_empty()).then_some((message.role, text))
        })
        .collect::<Vec<_>>();
    let rendered = visible
        .into_iter()
        .map(|(role, text)| format!("{role:?}: {text}"))
        .collect::<Vec<_>>()
        .join("\n");
    bounded_policy_text(&rendered)
}

pub(super) fn bounded_policy_text(text: &str) -> String {
    bounded_text(text, policy_context_chars())
}

#[derive(Default, serde::Serialize)]
pub(super) struct SynthesisBrief {
    request: String,
    user_scope_context: Vec<String>,
    accepted_follow_up_context: Vec<SynthesisOutcome>,
    completed: Vec<SynthesisOutcome>,
    limitations: Vec<SynthesisOutcome>,
    web: Vec<crate::tools::WebOutcome>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    recalled_preferences: Vec<tachyon_api::types::MemoryRecallItem>,
    omitted: usize,
}

#[derive(serde::Serialize)]
struct SynthesisOutcome {
    objective: String,
    result: Option<String>,
    result_suffix: Option<String>,
    failure_reason: Option<String>,
    answer_omitted_bytes: usize,
    diagnostic_tools_withheld: usize,
    diagnostic_tools_omitted: u64,
    diagnostic_tools_truncated: usize,
    observed_invocations: Option<u64>,
    evidence_refs: Vec<String>,
}

impl SynthesisOutcome {
    fn project(outcome: crate::tools::TaskOutcome, budget: usize) -> Self {
        let mut projected = Self {
            objective: bounded_text(&outcome.objective, 400),
            result: None,
            result_suffix: None,
            failure_reason: outcome
                .failure_reason
                .map(|reason| bounded_text(&reason, 300)),
            answer_omitted_bytes: 0,
            diagnostic_tools_withheld: outcome.evidence.tools.len(),
            diagnostic_tools_omitted: outcome.evidence.omitted,
            diagnostic_tools_truncated: outcome
                .evidence
                .tools
                .iter()
                .filter(|tool| tool.output["truncated"] == true)
                .count(),
            observed_invocations: outcome.evidence.observed_invocations,
            evidence_refs: Vec::new(),
        };
        let answer_budget = if outcome
            .evidence
            .tools
            .iter()
            .any(|tool| tool.call_id.is_some())
        {
            budget.saturating_sub(128)
        } else {
            budget
        };
        if projected.failure_reason.is_none() {
            if let Some(result) = outcome.result.filter(|text| !text.trim().is_empty()) {
                projected.answer_omitted_bytes = result.len();
                projected.result = Some(result.clone());
                if crate::tools::json_fits(&projected, answer_budget) {
                    projected.answer_omitted_bytes = 0;
                } else {
                    // Preserve complete paragraphs (including tables and citation clauses),
                    // never manufacture a sentence from an arbitrary character prefix.
                    projected.result = None;
                    let overhead = serde_json::to_vec(&projected).unwrap().len();
                    let prefix_budget = overhead + answer_budget.saturating_sub(overhead) * 3 / 4;
                    for paragraph in result.split("\n\n") {
                        let previous = projected.result.clone();
                        let candidate = match &previous {
                            Some(text) => format!("{text}\n\n{paragraph}"),
                            None => paragraph.to_string(),
                        };
                        projected.result = Some(candidate);
                        if !crate::tools::json_fits(&projected, prefix_budget) {
                            projected.result = previous;
                            break;
                        }
                    }
                    let prefix_len = projected.result.as_ref().map_or(0, String::len);
                    let remainder = &result[prefix_len..];
                    for paragraph in remainder.trim_start_matches('\n').rsplit("\n\n") {
                        let previous = projected.result_suffix.clone();
                        projected.result_suffix = Some(match &previous {
                            Some(text) => format!("{paragraph}\n\n{text}"),
                            None => paragraph.to_string(),
                        });
                        if !crate::tools::json_fits(&projected, answer_budget) {
                            projected.result_suffix = previous;
                            break;
                        }
                    }
                    projected.answer_omitted_bytes = result.len()
                        - prefix_len
                        - projected.result_suffix.as_ref().map_or(0, String::len);
                }
            }
        }
        // References are navigation, not proof. Raw arguments, stdout and control
        // envelopes stay in the retained WorkResult, not the answer-model payload.
        for tool in outcome.evidence.tools {
            if let Some(reference) = tool.call_id {
                projected.evidence_refs.push(reference);
                if !crate::tools::json_fits(&projected, budget) {
                    projected.evidence_refs.pop();
                    break;
                }
            }
        }
        projected
    }
}

impl SynthesisBrief {
    pub(super) fn from_messages(messages: &[ChatMessage], selected_index: Option<usize>) -> Self {
        let request_index = messages
            .iter()
            .rposition(|message| message.role == Role::User)
            .unwrap_or(0);
        let request = messages
            .get(request_index)
            .map(ChatMessage::plain)
            .unwrap_or_default();
        let budget = policy_context_chars().clamp(1000, 32000);
        let mut brief = Self {
            request: bounded_text(&request, 2000),
            ..Self::default()
        };
        let mut outcomes = Vec::new();
        // Reserve room for the omission count even with escaped input characters.
        let fits = |brief: &Self| crate::tools::json_fits(brief, budget - 32);
        while !crate::tools::json_fits(&brief, budget / 3) {
            brief.request = bounded_text(&brief.request, brief.request.chars().count() / 2);
        }
        // Only the caller's host-selected attachment is evidence. JSON-shaped
        // user text never establishes provenance.
        let selected_index = selected_index.filter(|index| *index < request_index);
        let resolved_objectives = messages[request_index..]
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|content| match content {
                tachyon_model::Content::ToolCall(call) => Some(crate::tools::task_intents(call)),
                _ => None,
            })
            .flatten()
            .map(|task| task.objective)
            .collect::<Vec<_>>()
            .join(" ");
        let scope_terms = format!("{request} {resolved_objectives}")
            .split_whitespace()
            .filter(|word| {
                !matches!(
                    word.to_ascii_lowercase()
                        .trim_matches(|c: char| !c.is_alphanumeric()),
                    "request" | "question" | "please" | "tell" | "show" | "give" | "answer"
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        for (index, message) in messages[..request_index]
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, message)| message.role == Role::User)
        {
            let text = message.plain();
            if Some(index) == selected_index {
                continue;
            }
            if brief.user_scope_context.len() < 4
                && crate::turns::evidence_matches(&scope_terms, &text)
            {
                brief.user_scope_context.push(text);
                if !fits(&brief) || !crate::tools::json_fits(&brief.user_scope_context, budget / 4)
                {
                    brief.user_scope_context.pop();
                    brief.omitted = brief.omitted.saturating_add(1);
                }
            }
        }
        brief.user_scope_context.reverse();
        // Only matched successful native recall from this request is durable user context.
        let mut recall_ids = std::collections::BTreeSet::new();
        for message in &messages[request_index..] {
            for content in &message.content {
                match content {
                    tachyon_model::Content::ToolCall(call)
                        if message.role == Role::Assistant && call.name == "memory" =>
                    {
                        if serde_json::from_str::<serde_json::Value>(&call.arguments)
                            .is_ok_and(|args| args["action"] == "recall")
                        {
                            recall_ids.insert(call.id.as_str());
                        }
                    }
                    tachyon_model::Content::ToolResult { id, output }
                        if message.role == Role::Tool && recall_ids.remove(id.as_str()) =>
                    {
                        #[derive(serde::Deserialize)]
                        struct Recall {
                            status: String,
                            items: Vec<tachyon_api::types::MemoryRecallItem>,
                            truncated: bool,
                        }
                        if output.len() > crate::tools::MAX_WORKER_EVIDENCE_BYTES {
                            brief.omitted += 1;
                            continue;
                        }
                        let Ok(recall) = serde_json::from_str::<Recall>(output) else {
                            continue;
                        };
                        if recall.status != "ok" {
                            continue;
                        }
                        brief.omitted += usize::from(recall.truncated);
                        for item in recall.items {
                            if item.kind != tachyon_api::types::MemoryRecallKind::Preference {
                                continue;
                            }
                            brief.recalled_preferences.push(item);
                            if !fits(&brief)
                                || !crate::tools::json_fits(&brief.recalled_preferences, budget / 4)
                            {
                                brief.recalled_preferences.pop();
                                brief.omitted += 1;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(index) = selected_index {
            let selected_text = messages[index].plain();
            if selected_text.len() > crate::tools::MAX_WORKER_EVIDENCE_BYTES {
                brief.omitted += 1;
            } else if let Ok(selected) =
                serde_json::from_str::<crate::tools::WorkerEvidence>(&selected_text)
            {
                brief.omitted = brief.omitted.saturating_add(selected.omitted);
                for outcome in selected.task_outcomes {
                    outcomes.push((true, outcome));
                }
            } else {
                brief.omitted = brief.omitted.saturating_add(1);
            }
        }
        // Only native delegation envelopes with matching calls belong to this request.
        let calls = messages[request_index..]
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|content| match content {
                tachyon_model::Content::ToolCall(call)
                    if matches!(call.name.as_str(), "spawn_agent" | "spawn_agents") =>
                {
                    Some(call.id.as_str())
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        for output in messages[request_index..]
            .iter()
            .filter(|message| message.role == Role::Tool)
            .flat_map(|message| &message.content)
            .filter_map(|content| match content {
                tachyon_model::Content::ToolResult { id, output }
                    if calls.contains(id.as_str()) =>
                {
                    Some(output)
                }
                _ => None,
            })
        {
            // Bound parsing allocations too, before deserializing retained raw output.
            if output.len() > crate::tools::MAX_WORKER_EVIDENCE_BYTES {
                brief.omitted = brief.omitted.saturating_add(1);
                continue;
            }
            let Ok(envelope) = serde_json::from_str::<crate::tools::WorkerEvidence>(output) else {
                brief.omitted = brief.omitted.saturating_add(1);
                continue;
            };
            brief.omitted = brief.omitted.saturating_add(envelope.omitted);
            for outcome in envelope.task_outcomes {
                outcomes.push((false, outcome));
            }
        }
        let web_calls = messages[request_index..]
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .flat_map(|m| &m.content)
            .filter_map(|content| match content {
                tachyon_model::Content::ToolCall(call)
                    if matches!(call.name.as_str(), "websearch" | "webfetch") =>
                {
                    Some(call.id.as_str())
                }
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let mut web_outcomes = Vec::new();
        let mut web_ids = std::collections::BTreeSet::new();
        for message in messages[request_index..]
            .iter()
            .filter(|m| m.role == Role::Tool)
        {
            for content in &message.content {
                if let tachyon_model::Content::ToolResult { id, output } = content {
                    if web_calls.contains(id.as_str()) && output.len() <= 8192 {
                        if let Ok(outcome) =
                            serde_json::from_str::<crate::tools::WebOutcome>(output)
                        {
                            if outcome.tool_call_id == *id && web_ids.insert(id) {
                                web_outcomes.push(outcome);
                            }
                        }
                    }
                }
            }
        }
        let available = (budget - 32).saturating_sub(serde_json::to_vec(&brief).unwrap().len());
        let share = available / (web_outcomes.len() + outcomes.len()).max(1);
        for mut outcome in web_outcomes {
            if outcome.fit(share.saturating_sub(1)) {
                brief.web.push(outcome);
            } else {
                brief.omitted += 1;
            }
        }
        let available = (budget - 32).saturating_sub(serde_json::to_vec(&brief).unwrap().len());
        let share = available / outcomes.len().max(1);
        for (historical, outcome) in outcomes {
            let projected = SynthesisOutcome::project(outcome, share.saturating_sub(1));
            let target = if projected.failure_reason.is_some()
                || (projected.result.is_none() && projected.result_suffix.is_none())
            {
                &mut brief.limitations
            } else if historical {
                &mut brief.accepted_follow_up_context
            } else {
                &mut brief.completed
            };
            if crate::tools::json_fits(&projected, share.saturating_sub(1)) {
                target.push(projected);
            } else {
                brief.omitted += 1;
            }
        }
        brief
    }

    pub(super) fn safe_fallback(&self) -> String {
        for outcome in &self.limitations {
            if let Some(reason) = &outcome.failure_reason {
                let reason = reason.strip_prefix("worker failed: ").unwrap_or(reason);
                if let Some(seconds) = reason
                    .strip_prefix("The lookup exceeded ")
                    .and_then(|rest| rest.split_once(" seconds.").map(|(seconds, _)| seconds))
                    .and_then(|seconds| seconds.parse::<u64>().ok())
                {
                    return format!(
                        "The lookup exceeded {seconds} seconds. I couldn't verify enough information to answer that reliably."
                    );
                }
                if reason.starts_with("The lookup exceeded its time limit.")
                    || reason.starts_with("The lookup exceeded its deadline.")
                {
                    return "The lookup exceeded its time limit. I couldn't verify enough information to answer that reliably.".into();
                }
            }
        }
        if self.completed.is_empty() && self.accepted_follow_up_context.is_empty() {
            "I couldn't verify enough information to answer that reliably.".into()
        } else if self.limitations.is_empty()
            && self.omitted == 0
            && self
                .completed
                .iter()
                .chain(&self.accepted_follow_up_context)
                .all(|outcome| outcome.answer_omitted_bytes == 0)
        {
            "I received results, but couldn't reliably summarize them. I can't provide a verified answer yet.".into()
        } else {
            "I received only partial results and couldn't reliably summarize them. I can't confirm the full requested scope yet.".into()
        }
    }
}

fn synthesis_context(messages: &[ChatMessage], selected_index: Option<usize>) -> String {
    serde_json::to_string(&SynthesisBrief::from_messages(messages, selected_index))
        .expect("synthesis brief is serializable")
}

fn policy_context_chars() -> usize {
    std::env::var("TACHYON_POLICY_CONTEXT_CHARS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 1_000)
        .unwrap_or(DEFAULT_POLICY_CONTEXT_CHARS)
}

fn bounded_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let head = max_chars * 3 / 4;
    let tail = max_chars - head;
    let start = text.chars().take(head).collect::<String>();
    let end = text
        .chars()
        .rev()
        .take(tail)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    format!("{start}\n[context truncated]\n{end}")
}

#[cfg(test)]
#[path = "test_provider.rs"]
pub(super) mod test_provider;

#[cfg(test)]
mod tests {
    use super::{
        answerability_tool, parse_answerability_tool_call, policy_context, should_publish_direct,
        synthesis_context, VisibleResponseStream, ANSWERABILITY_TOOL_NAME,
    };
    use tachyon_model::{ChatMessage, Completion, Content, Role, TokenUsage, ToolCall};
    use tachyon_orchestrator::conversation::policy::Answerability;

    fn native_evidence(result: &str) -> Vec<ChatMessage> {
        vec![
            ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "spawn_agent".into(),
                    arguments: "{}".into(),
                })],
            },
            ChatMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    id: "call-1".into(),
                    output: serde_json::to_string(&crate::tools::WorkerEvidence {
                        omitted: 0,
                        task_outcomes: vec![crate::tools::TaskOutcome {
                            objective: "inspect release".into(),
                            result: Some(result.into()),
                            completed_scopes: None,
                            failure_reason: None,
                            evidence: Default::default(),
                        }],
                    })
                    .unwrap(),
                }],
            },
        ]
    }

    #[tokio::test]
    async fn synthesis_retains_only_matched_current_recall_with_override_policy_over_http() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for (action, status, matched, current, text, retained) in [
                (
                    "recall",
                    "ok",
                    true,
                    true,
                    "Prefer vegetarian meals and bullet lists.",
                    true,
                ),
                (
                    "remember",
                    "ok",
                    true,
                    true,
                    "MUTATION IS NOT RECALL",
                    false,
                ),
                ("recall", "error", true, true, "FAILED RECALL", false),
                ("recall", "ok", false, true, "UNMATCHED", false),
                ("recall", "ok", true, false, "OLD REQUEST", false),
            ] {
                let recall = vec![
                    ChatMessage {
                        role: Role::Assistant,
                        content: vec![Content::ToolCall(ToolCall {
                            id: "memory-call".into(),
                            name: "memory".into(),
                            arguments: serde_json::json!({"action":action}).to_string(),
                        })],
                    },
                    ChatMessage {
                        role: Role::Tool,
                        content: vec![Content::ToolResult {
                            id: if matched { "memory-call" } else { "other" }.into(),
                            output: serde_json::json!({"status":status,"items":[{
                            "kind":"preference", "memory_id":"p1", "text":text, "occurred_at_ms":1
                        }],"truncated":false})
                            .to_string(),
                        }],
                    },
                ];
                let mut messages = vec![];
                if !current {
                    messages.extend(recall.clone());
                }
                messages.push(ChatMessage::new(
                    Role::User,
                    "Recommend dinner in one sentence, not bullets.",
                ));
                if current {
                    messages.extend(recall);
                }
                messages.extend(native_evidence("Lentil soup is available."));
                let model = provider.model.clone();
                let mut relay = |_: &str| {};
                let future = super::synthesize_spoken_response(&model, &messages, None, &mut relay);
                tokio::pin!(future);
                let request = tokio::select! {
                    _ = &mut future => panic!("missing synthesis request"),
                    request = provider.requests.recv() => request.unwrap(),
                };
                let system = request.body["messages"][0]["content"].as_str().unwrap();
                assert!(system.contains(
                    "Use relevant recalled_preferences for recommendations and format constraints"
                ));
                assert!(
                    system.contains("explicit current instructions override stored preferences")
                );
                assert!(system.contains("never invent one"));
                let context = request.body["messages"][1]["content"].as_str().unwrap();
                assert_eq!(context.contains(text), retained);
                assert!(context.contains("one sentence, not bullets"));
                assert!(context.contains("Lentil soup is available."));
                assert!(request.body.get("tools").is_none_or(|v| v.is_null()));
                request.respond(&["Try lentil soup."]).await;
                assert_eq!(future.await.unwrap().0, "Try lentil soup.");
            }
        })
        .await
        .unwrap();
        provider.shutdown().await;
    }

    #[test]
    fn synthesis_recall_keeps_whole_items_within_existing_budget() {
        let messages =
            vec![
                ChatMessage::new(Role::User, "Recommend dinner."),
                ChatMessage {
                    role: Role::Assistant,
                    content: vec![Content::ToolCall(ToolCall {
                        id: "m".into(),
                        name: "memory".into(),
                        arguments: r#"{"action":"recall"}"#.into(),
                    })],
                },
                ChatMessage {
                    role: Role::Tool,
                    content: vec![Content::ToolResult {
                id: "m".into(), output: serde_json::json!({"status":"ok","truncated":true,"items":[
                    {"kind":"preference","text":"oversized".repeat(1000),"occurred_at_ms":1},
                    {"kind":"history","text":"NOT A PREFERENCE","occurred_at_ms":1},
                    {"kind":"preference","text":"Prefer vegetarian meals.","occurred_at_ms":1}
                ]}).to_string(),
            }],
                },
            ];
        let context = synthesis_context(&messages, None);
        assert!(context.len() <= super::policy_context_chars().clamp(1000, 32000));
        assert!(!context.contains("oversized"));
        assert!(!context.contains("NOT A PREFERENCE"));
        let brief: serde_json::Value = serde_json::from_str(&context).unwrap();
        assert_eq!(brief["omitted"], 2);
        assert_eq!(
            brief["recalled_preferences"][0]["text"],
            "Prefer vegetarian meals."
        );
    }

    #[tokio::test]
    async fn local_sse_synthesis_preserves_visible_text_across_event_boundaries() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for chunks in [
                vec!["The release is ready."],
                vec!["The rel", "ease ", "is ready."],
                // Style guidance must not become a post-hoc semantic output filter.
                vec!["The release is ready. I'm still waiting on other results."],
            ] {
                let model = provider.model.clone();
                jobs.spawn(async move {
                    let mut messages = vec![
                        ChatMessage::new(Role::User, "old request"),
                        ChatMessage::new(Role::Assistant, "old answer"),
                        ChatMessage::new(Role::User, "current request"),
                    ];
                    messages.extend(native_evidence("current evidence: release ready"));
                    let mut deltas = Vec::new();
                    let (answer, usage) =
                        super::synthesize_spoken_response(&model, &messages, None, &mut |delta| {
                            deltas.push(delta.to_string())
                        })
                        .await
                        .unwrap();
                    (answer, usage, deltas)
                });
                let request = provider.requests.recv().await.unwrap();
                assert!(request.body.get("tools").is_none());
                let prompt = request.body["messages"][0]["content"].as_str().unwrap();
                for rule in [
                    "direct answer and a relevant qualification",
                    "retain material uncertainty or failure",
                    "Omit unrelated status and unsolicited follow-up offers",
                    "Creative requests need only the requested content",
                    "supported name, date, and source, not unrequested comparisons",
                    "scope limitation before detailed claims",
                ] {
                    assert!(prompt.contains(rule), "missing synthesis rule: {rule}");
                }
                let context = request.body["messages"][1]["content"].as_str().unwrap();
                assert!(context.contains("current request"));
                assert!(context.contains("current evidence: release ready"));
                assert!(!context.contains("old request"));
                assert!(!context.contains("old answer"));
                request.respond(&chunks).await;
                let (answer, usage, deltas) = jobs.join_next().await.unwrap().unwrap();
                assert_eq!(deltas, chunks);
                assert_eq!(deltas.concat(), answer);
                assert_eq!(answer, chunks.concat());
                assert_eq!(usage.total_tokens, 16);
            }
            assert!(provider.requests.try_recv().is_err());
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("local synthesis stream deadlocked");
    }

    #[tokio::test]
    async fn local_sse_protocol_markup_is_not_published_across_event_boundaries() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for chunks in [
                vec!["Private plan <DSML>hidden</DSML>"],
                vec!["Private plan <D", "S", "ML>hidden</DSML>"],
            ] {
                let model = provider.model.clone();
                jobs.spawn(async move {
                    let mut visible = String::new();
                    let completion = super::chat_with_delegation(
                        &model,
                        &[ChatMessage::new(Role::User, "request")],
                        &[],
                        true,
                        &mut |delta| visible.push_str(delta),
                    )
                    .await
                    .unwrap();
                    (completion, visible)
                });
                provider
                    .requests
                    .recv()
                    .await
                    .unwrap()
                    .respond(&chunks)
                    .await;
                let (completion, visible) = jobs.join_next().await.unwrap().unwrap();
                assert_eq!(completion.text, chunks.concat());
                assert_eq!(completion.finish_reason.as_deref(), Some("stop"));
                assert!(completion.tool_calls.is_empty());
                assert!(
                    visible.is_empty(),
                    "even the buffered planning prefix stays private"
                );
            }
            assert!(provider.requests.try_recv().is_err());
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("local protocol stream deadlocked");
    }

    fn answerability_completion(arguments: &str) -> Completion {
        Completion {
            text: String::new(),
            tool_calls: vec![ToolCall {
                id: "classification-1".into(),
                name: ANSWERABILITY_TOOL_NAME.into(),
                arguments: arguments.into(),
            }],
            usage: TokenUsage::default(),
            finish_reason: Some("tool_calls".into()),
        }
    }

    #[test]
    fn answerability_tool_schema_is_strict() {
        let tool = answerability_tool();
        assert_eq!(tool.name, ANSWERABILITY_TOOL_NAME);
        assert_eq!(tool.parameters["required"], serde_json::json!(["outcome"]));
        assert_eq!(tool.parameters["additionalProperties"], false);
        assert_eq!(
            tool.parameters["properties"]["outcome"]["enum"],
            serde_json::json!(["AnswerFromContext", "NeedsNewWork"])
        );
    }

    #[test]
    fn answerability_tool_parser_accepts_only_exact_structured_output() {
        assert_eq!(
            parse_answerability_tool_call(&answerability_completion(
                r#"{"outcome":"AnswerFromContext"}"#
            )),
            Some(Answerability::AnswerFromContext)
        );
        for arguments in [
            r#"{"outcome":"answer_from_context"}"#,
            r#"{"outcome":"AnswerFromContext","detail":"extra"}"#,
            r#"{"answerability":"AnswerFromContext"}"#,
            "AnswerFromContext",
        ] {
            assert_eq!(
                parse_answerability_tool_call(&answerability_completion(arguments)),
                None
            );
        }

        let mut prose = answerability_completion(r#"{"outcome":"AnswerFromContext"}"#);
        prose.text = "AnswerFromContext".into();
        assert_eq!(parse_answerability_tool_call(&prose), None);
    }

    #[test]
    fn visible_response_never_streams_dsml_protocol_markup() {
        let mut stream = VisibleResponseStream::default();
        assert_eq!(
            stream.push("I’ll check that. <｜D"),
            Some("I’ll check that. ".into())
        );
        assert_eq!(stream.push("SML｜tool_calls>secret"), None);
        assert_eq!(stream.finish(), None);
    }

    #[test]
    fn tool_capable_prose_is_published_only_for_confirmed_direct_answers() {
        let direct = Completion {
            text: "A direct answer.".into(),
            tool_calls: Vec::new(),
            usage: TokenUsage::default(),
            finish_reason: Some("stop".into()),
        };
        assert!(should_publish_direct(&direct, true));
        assert!(!should_publish_direct(&direct, false));

        let delegated = answerability_completion(r#"{"outcome":"AnswerFromContext"}"#);
        assert!(!should_publish_direct(&delegated, true));
    }

    #[test]
    fn policy_context_excludes_system_and_tool_protocol() {
        let messages = vec![
            ChatMessage::new(Role::System, "large private system prompt"),
            ChatMessage::new(Role::User, "question"),
            ChatMessage {
                role: Role::Assistant,
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "spawn_agent".into(),
                    arguments: "private arguments".into(),
                })],
            },
            ChatMessage::new(Role::Assistant, "visible answer"),
        ];
        let context = policy_context(&messages);
        assert!(context.contains("question"));
        assert!(context.contains("visible answer"));
        assert!(!context.contains("private system"));
        assert!(!context.contains("private arguments"));
    }

    #[test]
    fn synthesis_uses_only_current_request_and_evidence() {
        let mut messages = vec![
            ChatMessage::new(Role::System, "private system"),
            ChatMessage::new(Role::User, "old request"),
            ChatMessage::new(Role::Assistant, "old answer"),
            ChatMessage::new(Role::User, "current request"),
        ];
        messages.extend(native_evidence("current evidence"));
        let context = synthesis_context(&messages, None);
        assert!(context.contains("current request"));
        assert!(context.contains("current evidence"));
        assert!(!context.contains("private system"));
        assert!(!context.contains("old request"));
    }

    fn weather_evidence() -> Vec<ChatMessage> {
        let mut messages = vec![ChatMessage::new(
            Role::User,
            "Weather in London, Tokyo and New York?",
        )];
        let mut native = native_evidence("unused");
        let Content::ToolResult { output, .. } = &mut native[1].content[0] else {
            unreachable!()
        };
        *output = serde_json::to_string(&crate::tools::WorkerEvidence {
            omitted: 0,
            task_outcomes: ["London", "Tokyo", "New York"].into_iter().map(|city| {
                crate::tools::TaskOutcome {
                    objective: format!("Weather in {city}"),
                    result: Some(format!("{city}: 21 C, cloudy, humidity 80%, observed 2026-09-18T21:00. Alerts not verified; forecast unavailable. Source: [1](https://example.test/{city}). Retrieved 18/09/2026; station coverage limited.")),
                    failure_reason: None,
                    completed_scopes: None,
                    evidence: tachyon_api::WorkEvidence {
                        observed_invocations: Some(2), omitted: 1,
                        tools: vec![tachyon_api::WorkToolEvidence {
                            call_id: Some(format!("source-{city}")), parent_call_id: None,
                            tool_name: "exec".into(),
                            arguments: serde_json::json!({"code":"PRIVATE_CONTROL"}),
                            output: serde_json::json!({"content":"PRIVATE_STDOUT".repeat(1260), "truncated":true}),
                        }],
                    },
                }
            }).collect(),
        }).unwrap();
        messages.extend(native);
        messages
    }

    #[tokio::test]
    async fn synthesis_projects_all_weather_results_at_the_http_boundary() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let model = provider.model.clone();
        let mut job = tokio::spawn(async move {
            super::synthesize_spoken_response(&model, &weather_evidence(), None, &mut |_| {}).await
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let request = provider.requests.recv().await.unwrap();
            let payload = request.body.to_string();
            assert!(!payload.contains("PRIVATE_CONTROL"));
            assert!(!payload.contains("PRIVATE_STDOUT"));
            let context = synthesis_context(&weather_evidence(), None);
            assert!(context.len() < 8000);
            let value: serde_json::Value = serde_json::from_str(&context).unwrap();
            assert_eq!(value["completed"].as_array().unwrap().len(), 3);
            assert_eq!(value["omitted"], 0);
            for outcome in value["completed"].as_array().unwrap() {
                let text = outcome["result"].as_str().unwrap();
                assert!(text.contains("2026-09-18T21:00"));
                assert!(text.contains("18/09/2026"));
                assert!(text.contains("https://example.test/"));
                assert_eq!(outcome["answer_omitted_bytes"], 0);
                assert_eq!(outcome["diagnostic_tools_omitted"], 1);
                assert_eq!(outcome["diagnostic_tools_truncated"], 1);
                assert_eq!(outcome["evidence_refs"].as_array().unwrap().len(), 1);
            }
            let sent = request.body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["content"].as_str() == Some(context.as_str()));
            assert!(sent, "HTTP request must contain the projected brief");
            request.respond(&["Local scripted answer."]).await;
            assert_eq!(
                (&mut job).await.unwrap().unwrap().0,
                "Local scripted answer."
            );
        })
        .await;
        job.abort();
        provider.shutdown().await;
        result.unwrap();
    }

    #[test]
    fn synthesis_fair_windows_keep_citations_and_do_not_invent_missing_facts() {
        let mut messages = weather_evidence();
        let Content::ToolResult { output, .. } = &mut messages[2].content[0] else {
            unreachable!()
        };
        let mut envelope: crate::tools::WorkerEvidence = serde_json::from_str(output).unwrap();
        envelope.task_outcomes[0].result = Some(format!(
            "London: 19 C at 2026-09-18T13:00 [1]. Alerts unverified.\n\n{}\n\n[1] https://example.test/london (18/09/2026). Station coverage limited.",
            "A long indivisible paragraph. ".repeat(400)
        ));
        envelope.task_outcomes[1].failure_reason =
            Some("worker failed: The lookup exceeded 120 seconds. PRIVATE_CONTROL".into());
        envelope.task_outcomes[1].result = Some("FAILURE_BODY_MUST_NOT_BECOME_FACT".into());
        *output = serde_json::to_string(&envelope).unwrap();
        let context = synthesis_context(&messages, None);
        let brief = super::SynthesisBrief::from_messages(&messages, None);
        assert!(context.len() < 8000);
        assert_eq!(brief.completed.len(), 2);
        assert!(brief.completed[0].result.as_ref().unwrap().contains("19 C"));
        assert!(brief.completed[0]
            .result_suffix
            .as_ref()
            .unwrap()
            .contains("https://example.test/london"));
        assert!(brief.completed[0].answer_omitted_bytes > 9000);
        assert_eq!(brief.completed[1].answer_omitted_bytes, 0);
        assert!(!context.contains("FAILURE_BODY_MUST_NOT_BECOME_FACT"));
        assert!(brief
            .safe_fallback()
            .contains("lookup exceeded 120 seconds"));
        assert!(!brief.safe_fallback().contains("PRIVATE_CONTROL"));
        assert!(!brief.safe_fallback().contains("browser"));

        for text in ["", &"uncited ".repeat(2000)] {
            let mut messages = vec![ChatMessage::new(Role::User, "latest news")];
            messages.extend(native_evidence(text));
            let brief = super::SynthesisBrief::from_messages(&messages, None);
            assert!(brief.completed.is_empty());
            assert!(brief.limitations[0].evidence_refs.is_empty());
            assert!(!brief.safe_fallback().contains("release"));
        }
    }

    #[test]
    fn synthesis_retains_scope_corrections_and_canonical_sources_not_failures() {
        let mut messages = vec![
            ChatMessage::new(Role::User, "What is the recipe for soup?"),
            ChatMessage::new(Role::Assistant, "unrelated private answer"),
            ChatMessage::new(
                Role::User,
                "For the package release, use the stable channel, not preview.",
            ),
            ChatMessage::new(Role::Assistant, "private planning"),
            ChatMessage::new(Role::User, "Summarize the package release."),
        ];
        let work: tachyon_api::WorkResult = serde_json::from_value(serde_json::json!({
            "work_id": "work-1", "objective": "inspect package release", "generation": 0, "assignment": 0,
            "outcome": "completed", "result": "Announced 2026-09-18 [1]. Availability not verified. [1] https://example.test/release",
            "evidence": { "tools": [{ "call_id": "source-1", "parent_call_id": null,
                "tool_name": "fetch", "arguments": {"url":"https://example.test/release"},
                "output": {"text":"Preview announced on 18/09/2026", "is_error":false} }], "omitted": 0 }
        })).unwrap();
        let mut native = native_evidence("unused");
        let Content::ToolResult { output, .. } = &mut native[1].content[0] else {
            unreachable!()
        };
        *output = serde_json::to_string(&crate::tools::WorkerEvidence {
            omitted: 0,
            task_outcomes: vec![
                crate::tools::TaskOutcome::from_work_result(&work),
                crate::tools::TaskOutcome {
                    objective: "inspect availability".into(),
                    result: Some("must not become a fact".into()),
                    completed_scopes: None,
                    failure_reason: Some("not verified".into()),
                    evidence: work.evidence.clone(),
                },
            ],
        })
        .unwrap();
        messages.extend(native);
        let context = synthesis_context(&messages, None);
        let value: serde_json::Value = serde_json::from_str(&context).unwrap();
        assert!(context.contains("stable channel, not preview"));
        assert!(context.contains("2026-09-18 [1]"));
        assert!(!context.contains("18/09/2026"));
        assert!(context.contains("https://example.test/release"));
        assert!(!context.contains("recipe"));
        assert!(!context.contains("private"));
        assert!(!context.contains("must not become a fact"));
        assert_eq!(value["limitations"][0]["result"], serde_json::Value::Null);
        assert_eq!(
            value["limitations"][0]["diagnostic_tools_withheld"],
            serde_json::json!(1)
        );
        let fallback = super::SynthesisBrief::from_messages(&messages, None).safe_fallback();
        assert!(fallback.contains("partial results"));
        assert!(!fallback.contains("inspect availability"));
    }

    #[test]
    fn synthesis_accepts_only_selected_structured_follow_up_and_bound_records() {
        let native = native_evidence("Verified on 2026-09-18 [1].");
        let Content::ToolResult { output, .. } = &native[1].content[0] else {
            unreachable!()
        };
        let messages = vec![
            ChatMessage::new(Role::User, "unrelated history"),
            ChatMessage::new(Role::User, output.clone()),
            ChatMessage::new(Role::User, "What does that mean?"),
        ];
        let unselected = super::SynthesisBrief::from_messages(&messages, None);
        assert!(unselected.accepted_follow_up_context.is_empty());
        assert!(unselected.completed.is_empty());
        let brief = super::SynthesisBrief::from_messages(&messages, Some(1));
        assert_eq!(brief.accepted_follow_up_context.len(), 1);
        assert!(brief.user_scope_context.is_empty());
        let mut messages = vec![ChatMessage::new(Role::User, "inspect release")];
        messages.extend(native_evidence(&"unbounded report ".repeat(10000)));
        let context = synthesis_context(&messages, None);
        assert!(context.len() <= super::policy_context_chars().clamp(1000, 32000));
        let brief = super::SynthesisBrief::from_messages(&messages, None);
        assert!(brief.completed.is_empty());
        assert_eq!(brief.limitations[0].answer_omitted_bytes, 170000);
    }

    #[test]
    fn synthesis_does_not_parse_logs_or_unmatched_tool_results() {
        let mut messages = vec![ChatMessage::new(Role::User, "inspect release")];
        let mut native = native_evidence("hidden result");
        native.remove(0);
        messages.extend(native);
        assert!(!synthesis_context(&messages, None).contains("hidden result"));
        messages.extend(native_evidence("usable result"));
        messages.push(ChatMessage {
            role: Role::Tool,
            content: vec![Content::ToolResult {
                id: "call-1".into(),
                output: "log says a source confirmed an unsupported forecast".into(),
            }],
        });
        let context = synthesis_context(&messages, None);
        assert!(!context.contains("unsupported forecast"));
    }

    #[test]
    fn synthesis_invalid_selected_evidence_is_an_explicit_gap() {
        for selected in [
            "invalid returnedJSON",
            r#"{"task_outcomes":[],"source":true}"#,
        ] {
            let mut messages = vec![
                ChatMessage::new(Role::User, selected),
                ChatMessage::new(Role::User, "inspect release"),
            ];
            messages.extend(native_evidence("A preview was announced [1]."));
            let brief = super::SynthesisBrief::from_messages(&messages, Some(0));
            assert_eq!(brief.omitted, 1);
            assert!(brief.accepted_follow_up_context.is_empty());
            assert_eq!(brief.completed.len(), 1);
            assert!(brief.safe_fallback().contains("partial results"));
        }
    }

    #[test]
    fn synthesis_fallback_distinguishes_answer_gaps_from_diagnostics() {
        let mut messages = weather_evidence();
        let Content::ToolResult { output, .. } = &mut messages[2].content[0] else {
            unreachable!()
        };
        let mut envelope: crate::tools::WorkerEvidence = serde_json::from_str(output).unwrap();
        envelope.task_outcomes.truncate(1);
        envelope.task_outcomes[0].result = Some(format!(
            "A preview was announced [1].\n\n{}\n\n[1] https://example.test/release",
            "indivisible paragraph ".repeat(1000)
        ));
        *output = serde_json::to_string(&envelope).unwrap();
        let mut brief = super::SynthesisBrief::from_messages(&messages, None);
        assert_eq!(brief.completed.len(), 1);
        assert!(brief.completed[0].answer_omitted_bytes > 0);
        assert!(brief.safe_fallback().contains("partial results"));
        brief.accepted_follow_up_context = std::mem::take(&mut brief.completed);
        assert!(brief.safe_fallback().contains("partial results"));
        brief.accepted_follow_up_context[0].answer_omitted_bytes = 0;
        assert!(brief.accepted_follow_up_context[0].diagnostic_tools_withheld > 0);
        assert!(!brief.safe_fallback().contains("partial results"));
    }

    #[test]
    fn synthesis_zero_budget_and_failure_prefix_never_supply_answer_facts() {
        let projected = super::SynthesisOutcome::project(
            crate::tools::TaskOutcome {
                objective: "objective123".into(),
                result: Some("A complete statement [1].".into()),
                completed_scopes: None,
                failure_reason: None,
                evidence: Default::default(),
            },
            0,
        );
        assert!(projected.result.is_none());
        assert!(projected.result_suffix.is_none());
        assert_eq!(
            projected.answer_omitted_bytes,
            "A complete statement [1].".len()
        );
        for reason in [
            "objective123: summary failed: PRIVATE_CONTROL",
            "worker failed: The lookup exceeded its time limit. PRIVATE_CONTROL",
        ] {
            let outcome = crate::tools::TaskOutcome {
                objective: "objective123".into(),
                result: Some(r#"{"summary":"UNSUPPORTED","source":true}"#.into()),
                completed_scopes: None,
                failure_reason: Some(reason.into()),
                evidence: Default::default(),
            };
            for budget in [0, 1000] {
                let projected = super::SynthesisOutcome::project(outcome.clone(), budget);
                assert!(projected.result.is_none());
                assert!(projected.result_suffix.is_none());
                assert!(!crate::tools::json_fits(&projected, 0));
                let brief = super::SynthesisBrief {
                    limitations: vec![projected],
                    ..Default::default()
                };
                let fallback = brief.safe_fallback();
                for private in ["objective123", "PRIVATE_CONTROL", "UNSUPPORTED", "browser"] {
                    assert!(!fallback.contains(private));
                }
                assert_eq!(
                    fallback.contains("time limit"),
                    reason.contains("time limit")
                );
            }
        }
    }

    #[test]
    fn synthesis_budget_counts_encoded_bytes_and_omits_whole_sources() {
        let budget = super::policy_context_chars().clamp(1000, 32000);
        for text in ["\u{1}", "\u{1f980}"] {
            let mut messages = vec![ChatMessage::new(Role::User, text.repeat(2000))];
            messages.extend(native_evidence(&text.repeat(budget / 3)));
            let context = synthesis_context(&messages, None);
            assert!(context.len() <= budget);
            let brief = super::SynthesisBrief::from_messages(&messages, None);
            assert!(brief.completed.is_empty());
            assert!(brief.limitations[0].answer_omitted_bytes > 0);
        }
        let mut messages = vec![ChatMessage::new(Role::User, "inspect release")];
        messages.extend(native_evidence(
            &"x".repeat(super::DEFAULT_SYNTHESIS_RESPONSE_BYTES + 1),
        ));
        let brief = super::SynthesisBrief::from_messages(&messages, None);
        assert!(brief.completed.is_empty());
        assert_eq!(brief.omitted, 1);
    }

    #[test]
    fn synthesis_resolved_objective_retains_latest_scope_correction() {
        let mut messages = vec![
            ChatMessage::new(Role::User, "Use project North, not South."),
            ChatMessage::new(Role::Assistant, "scope acknowledged"),
            ChatMessage::new(Role::User, "Actually, use project South."),
            ChatMessage::new(Role::Assistant, "updated scope"),
            ChatMessage::new(Role::User, "What is its deployment status?"),
        ];
        let mut native = native_evidence("Deployment was announced; availability unverified.");
        let Content::ToolCall(call) = &mut native[0].content[0] else {
            unreachable!()
        };
        call.arguments =
            serde_json::json!({"task":"Verify deployment status for project South"}).to_string();
        messages.extend(native);
        let brief = super::SynthesisBrief::from_messages(&messages, None);
        assert_eq!(
            brief.user_scope_context.last().map(String::as_str),
            Some("Actually, use project South.")
        );
    }

    #[tokio::test]
    async fn output_observers_follow_tasks_across_models_and_cancellation() {
        let mut first = super::test_provider::LocalProvider::start().await;
        let mut second = super::test_provider::LocalProvider::start().await;
        let (observed, mut observations) = tokio::sync::mpsc::unbounded_channel();
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let models = [first.model.clone(), second.model.clone()];
            let observer = observed.clone();
            jobs.spawn(tachyon_model::with_output_observer(async move {
                for model in models {
                    model.chat(&[ChatMessage::new(Role::User, "answer")], None, &mut |_| {})
                        .await.unwrap();
                }
            }, move || { observer.send(1).unwrap(); }));
            let model = second.model.clone();
            let cancelled = jobs.spawn(tachyon_model::with_output_observer(async move {
                model.chat(&[ChatMessage::new(Role::User, "cancel")], None, &mut |_| {})
                    .await.unwrap();
            }, move || { observed.send(2).unwrap(); }));
            let mut held = second.requests.recv().await.unwrap();
            held.start_stream().await;
            held.send_delta(serde_json::json!({"content":"pending"})).await;
            assert_eq!(observations.recv().await, Some(2));
            cancelled.abort();
            assert!(jobs.join_next().await.unwrap().unwrap_err().is_cancelled());
            drop(held);
            first.requests.recv().await.unwrap().respond(&["one", " two"]).await;
            assert_eq!(observations.recv().await, Some(1));
            second.requests.recv().await.unwrap().respond(&["three", " four"]).await;
            assert_eq!(observations.recv().await, Some(1));
            jobs.join_next().await.unwrap().unwrap();
            assert!(observations.try_recv().is_err());

            // Existing chat callers still accept bare EOF and are not opted into
            // the synthesis reader's cap or terminal-marker requirement.
            let model = first.model.clone();
            jobs.spawn(async move {
                let completion = model.chat(&[ChatMessage::new(Role::User, "legacy")], None, &mut |_| {})
                    .await.unwrap();
                assert!(completion.text.len() > super::DEFAULT_SYNTHESIS_RESPONSE_BYTES);
            });
            let mut request = first.requests.recv().await.unwrap();
            request.start_stream().await;
            request.send_delta(serde_json::json!({"content":"x".repeat(super::DEFAULT_SYNTHESIS_RESPONSE_BYTES + 1)})).await;
            drop(request);
            jobs.join_next().await.unwrap().unwrap();
            assert!(observations.try_recv().is_err());
        }).await;
        jobs.shutdown().await;
        first.shutdown().await;
        second.shutdown().await;
        result.expect("observer isolation or cancellation deadlocked");
    }

    #[tokio::test]
    async fn synthesis_first_chunk_is_visible_before_provider_releases_the_rest() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let model = provider.model.clone();
        let (tx, mut deltas) = tokio::sync::mpsc::unbounded_channel();
        let mut job = tokio::spawn(async move {
            super::synthesize_spoken_response(
                &model,
                &[ChatMessage::new(Role::User, "Explain the release status.")],
                None,
                &mut |delta| {
                    tx.send(delta.to_string()).unwrap();
                },
            )
            .await
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let mut request = provider.requests.recv().await.unwrap();
            request.start_stream().await;
            request
                .send_delta(
                    serde_json::json!({"content":"The preview was announced on 2026-09-18 [1]."}),
                )
                .await;
            let first = deltas.recv().await.unwrap();
            assert_eq!(first, "The preview was announced on 2026-09-18 [1].");
            assert!(
                !job.is_finished(),
                "first answer must not wait for completion"
            );
            request
                .send_delta(serde_json::json!({"content":" Availability is not verified."}))
                .await;
            request.finish_completion("stop").await;
            let (answer, _) = (&mut job).await.unwrap().unwrap();
            let second = deltas.recv().await.unwrap();
            assert_eq!(answer, format!("{first}{second}"));
            assert!(deltas.recv().await.is_none());
        })
        .await;
        job.abort();
        provider.shutdown().await;
        result.expect("synthesis buffered its first chunk behind the provider barrier");
    }

    #[tokio::test]
    async fn synthesis_failures_preserve_published_prefix_without_replay_or_retraction() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for failure in [
                "transport",
                "eof",
                "protocol",
                "incomplete_marker",
                "tool_call",
                "length",
                "provider_error",
                "malformed",
            ] {
                let model = provider.model.clone();
                let (tx, mut deltas) = tokio::sync::mpsc::unbounded_channel();
                jobs.spawn(async move {
                    super::synthesize_spoken_response(
                        &model,
                        &[ChatMessage::new(Role::User, "Explain the release status.")],
                        None,
                        &mut |delta| {
                            tx.send(delta.to_string()).unwrap();
                        },
                    )
                    .await
                });
                let mut request = provider.requests.recv().await.unwrap();
                if failure == "transport" {
                    request.start_truncated_stream().await;
                } else {
                    request.start_stream().await;
                }
                let prefix = "A preview was announced on 18/09/2026 [1]. ";
                request
                    .send_delta(serde_json::json!({"content":prefix}))
                    .await;
                assert_eq!(deltas.recv().await.unwrap(), prefix);
                match failure {
                    "transport" | "eof" => drop(request),
                    "protocol" => {
                        for frame in ["<", "｜D", "SM", "L｜tool_calls>private report"] {
                            request
                                .send_delta(serde_json::json!({"content":frame}))
                                .await;
                        }
                        request.finish_completion("stop").await;
                    }
                    "incomplete_marker" => {
                        request
                            .send_delta(serde_json::json!({"content":"<D"}))
                            .await;
                        request.finish_completion("stop").await;
                    }
                    "tool_call" => {
                        request
                            .send_delta(serde_json::json!({"tool_calls":[{
                                "index":0, "id":"unexpected", "type":"function",
                                "function":{"name":"spawn_agent", "arguments":"private report"}
                            }]}))
                            .await;
                        request.finish_completion("tool_calls").await;
                    }
                    "length" => request.finish_completion("length").await,
                    "provider_error" => request.fail_stream(false).await,
                    "malformed" => request.fail_stream(true).await,
                    _ => unreachable!(),
                }
                let failure = jobs.join_next().await.unwrap().unwrap().unwrap_err();
                assert_eq!(failure.published_prefix, prefix);
                assert!(
                    deltas.recv().await.is_none(),
                    "must not replay the prefix or leak protocol"
                );
                let final_text =
                    failure.into_response(|| panic!("must not replace a published prefix"));
                assert_eq!(
                    final_text,
                    format!("{prefix}{}", super::SYNTHESIS_INTERRUPTION)
                );
                assert_eq!(final_text.matches(super::SYNTHESIS_INTERRUPTION).count(), 1);
                assert!(!final_text.contains("private report"));
            }
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("interrupted synthesis did not terminate");
    }

    #[tokio::test]
    async fn synthesis_failure_before_publication_uses_safe_brief_fallback() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for protocol in [false, true] {
                let model = provider.model.clone();
                jobs.spawn(async move {
                    let mut messages = vec![ChatMessage::new(Role::User, "current request")];
                    messages.extend(native_evidence("raw private report"));
                    let failure =
                        super::synthesize_spoken_response(&model, &messages, None, &mut |_| {
                            panic!("nothing usable should be published")
                        })
                        .await
                        .unwrap_err();
                    assert!(failure.published_prefix.is_empty());
                    let brief = super::SynthesisBrief::from_messages(&messages, None);
                    let fallback = brief.safe_fallback();
                    assert_eq!(failure.into_response(|| brief.safe_fallback()), fallback);
                    assert!(!fallback.contains("raw private report"));
                });
                let request = provider.requests.recv().await.unwrap();
                if protocol {
                    request.respond(&["<D", "SML>private report"]).await;
                } else {
                    request.respond_error().await;
                }
                jobs.join_next().await.unwrap().unwrap();
            }
        })
        .await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("failed synthesis did not terminate");
    }

    #[tokio::test]
    async fn detailed_synthesis_exceeds_four_thousand_bytes_but_default_memory_is_bounded() {
        let mut provider = super::test_provider::LocalProvider::start().await;
        let mut jobs = tokio::task::JoinSet::new();
        let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            for oversized in [false, true] {
                let model = provider.model.clone();
                let (tx, mut deltas) = tokio::sync::mpsc::unbounded_channel();
                jobs.spawn(async move {
                    super::synthesize_spoken_response(&model,
                        &[ChatMessage::new(Role::User, "Provide a detailed explanation, preserving dates and citations.")],
                        None,
                        &mut |delta| { tx.send(delta.to_string()).unwrap(); }).await
                });
                let mut request = provider.requests.recv().await.unwrap();
                let mut visible = String::new();
                request.start_stream().await;
                let prefix = "DSML is a protocol name; 3 < 5 is an inequality.\n";
                request.send_delta(serde_json::json!({"content":prefix})).await;
                visible.push_str(&deltas.recv().await.unwrap());
                let detail = if oversized {
                    "x".repeat(super::DEFAULT_SYNTHESIS_RESPONSE_BYTES + 1)
                } else {
                    "The preview was announced on 2026-09-18 [1]; availability remains unverified.\n".repeat(100)
                };
                assert!(detail.len() > 4000);
                if oversized {
                    let _ = request.try_send_delta(serde_json::json!({"content":detail})).await;
                    // No terminal frame: the byte bound must stop reading by itself.
                    let failure = jobs.join_next().await.unwrap().unwrap().unwrap_err();
                    assert_eq!(failure.published_prefix, visible);
                    assert!(failure.published_prefix.len() < super::DEFAULT_SYNTHESIS_RESPONSE_BYTES);
                    assert!(failure.into_response(|| panic!("published prefix"))
                        .ends_with(super::SYNTHESIS_INTERRUPTION));
                } else {
                    request.send_delta(serde_json::json!({"content":detail})).await;
                    // Some providers use only [DONE]; bounded chat still rejects bare EOF.
                    request.finish_stream().await;
                    let (answer, _) = jobs.join_next().await.unwrap().unwrap().unwrap();
                    while let Some(delta) = deltas.recv().await { visible.push_str(&delta); }
                    assert_eq!(answer, visible);
                    assert_eq!(answer, format!("{prefix}{detail}"));
                    assert!(answer.len() > 4000);
                }
            }
        }).await;
        jobs.shutdown().await;
        provider.shutdown().await;
        result.expect("detailed or byte-bounded synthesis did not terminate");
    }
}

pub(super) fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let mut cp = s.chars();
        let cut: String = cp.by_ref().take(n).collect();
        format!("{cut}…")
    }
}
