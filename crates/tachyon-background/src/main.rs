#![forbid(unsafe_code)]

use std::process::ExitCode;

use serde::Deserialize;
use serde_json::json;
use tachyon_api::{
    LifecycleRecommendation, LifetimeClass, WorkOutcome, WorkReviewDecision, WorkReviewFailure,
    WorkReviewRecommendation, WorkReviewRequest,
};
use tachyon_model::{ChatMessage, Model, ModelConfig, Role, ToolSpec};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const REVIEW_TOOL: &str = "submit_work_review";
const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 10;
const MAX_REVIEW_INPUT_CHARS: usize = 32_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelReview {
    decision: ModelDecision,
    lifecycle: Option<ModelLifecycle>,
    revised_objective: Option<String>,
    rationale: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ModelDecision {
    Accept,
    Rework,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ModelLifecycle {
    KeepCurrent,
    Release,
    RetainShort,
    RetainLong,
    RetainPersistent,
}

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tachyon-background: tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run())
}

async fn run() -> ExitCode {
    let config = tachyon_util::config::Config::load();
    let background = config.background_config();
    let persona = background.persona.clone();
    let model = model_from_config(&config, &background).ok();
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
    eprintln!("tachyon-background: ready");
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let request = match serde_json::from_str::<WorkReviewRequest>(&line) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("tachyon-background: invalid review request: {error}");
                continue;
            }
        };
        let decision = review(&request, model.as_ref(), persona.as_deref()).await;
        let encoded = match serde_json::to_vec(&decision) {
            Ok(encoded) => encoded,
            Err(error) => {
                eprintln!("tachyon-background: encode review decision: {error}");
                return ExitCode::FAILURE;
            }
        };
        if stdout.write_all(&encoded).await.is_err()
            || stdout.write_all(b"\n").await.is_err()
            || stdout.flush().await.is_err()
        {
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

async fn review(
    request: &WorkReviewRequest,
    model: Option<&Model>,
    persona: Option<&str>,
) -> WorkReviewDecision {
    let invalid = request.review_id.trim().is_empty()
        || request.candidate.work_id.trim().is_empty()
        || request.candidate.objective.trim().is_empty()
        || request.worker.worker_id.trim().is_empty()
        || request.deadline_ms <= unix_now_ms()
        || !matches!(
            &request.candidate.outcome,
            WorkOutcome::Completed { result, .. } if !result.trim().is_empty()
        )
        || serde_json::to_string(request)
            .map(|input| input.chars().count() > MAX_REVIEW_INPUT_CHARS)
            .unwrap_or(true);
    if invalid {
        return inconclusive(
            request,
            WorkReviewFailure::InvalidRequest,
            "invalid review request",
        );
    }
    let Some(model) = model else {
        return inconclusive(
            request,
            WorkReviewFailure::ModelUnavailable,
            "background model unavailable",
        );
    };
    let prompt = tachyon_orchestrator::background::prompt::review_system_prompt(
        tachyon_orchestrator::background::prompt::PromptContext { persona },
    );
    let input = serde_json::to_string(request).unwrap_or_default();
    let messages = vec![
        ChatMessage::new(Role::System, prompt),
        ChatMessage::new(Role::User, input),
    ];
    let tool = review_tool();
    let remaining_ms = request.deadline_ms.saturating_sub(unix_now_ms()).max(1);
    let configured_ms = review_timeout().as_millis() as u64;
    let mut relay = |_delta: &str| {};
    let completion = match tokio::time::timeout(
        std::time::Duration::from_millis(remaining_ms.min(configured_ms)),
        model.chat_requiring_tool(
            &messages,
            std::slice::from_ref(&tool),
            (REVIEW_TOOL, "decision"),
            &mut relay,
        ),
    )
    .await
    {
        Err(_) => return inconclusive(request, WorkReviewFailure::TimedOut, "review timed out"),
        Ok(Err(error)) => {
            eprintln!("tachyon-background: provider review failed: {error}");
            return inconclusive(
                request,
                WorkReviewFailure::ProviderError,
                "review provider failed",
            );
        }
        Ok(Ok(completion)) => completion,
    };
    if completion
        .finish_reason
        .as_deref()
        .is_some_and(|reason| matches!(reason, "length" | "max_tokens"))
    {
        return inconclusive(
            request,
            WorkReviewFailure::TruncatedOutput,
            "review output was truncated",
        );
    }
    let [call] = completion.tool_calls.as_slice() else {
        return inconclusive(
            request,
            WorkReviewFailure::MalformedOutput,
            "review did not return exactly one decision",
        );
    };
    if call.name != REVIEW_TOOL {
        return inconclusive(
            request,
            WorkReviewFailure::MalformedOutput,
            "review returned an unexpected tool",
        );
    }
    let parsed = match serde_json::from_str::<ModelReview>(&call.arguments) {
        Ok(parsed) if !parsed.rationale.trim().is_empty() => parsed,
        _ => {
            return inconclusive(
                request,
                WorkReviewFailure::MalformedOutput,
                "review arguments were malformed",
            )
        }
    };
    let recommendation = match parsed.decision {
        ModelDecision::Accept => {
            let Some(lifecycle) = parsed.lifecycle.map(lifecycle_recommendation) else {
                return inconclusive(
                    request,
                    WorkReviewFailure::MalformedOutput,
                    "accepted review omitted lifecycle",
                );
            };
            WorkReviewRecommendation::Accept { lifecycle }
        }
        ModelDecision::Rework => WorkReviewRecommendation::Rework {
            revised_objective: parsed
                .revised_objective
                .filter(|objective| !objective.trim().is_empty()),
        },
    };
    decision(request, recommendation, parsed.rationale)
}

fn model_from_config(
    config: &tachyon_util::config::Config,
    agent: &tachyon_util::config::AgentConfig,
) -> Result<Model, String> {
    let api_key = config.resolve_key("openrouter").ok_or_else(|| {
        "api key not configured (set OPENROUTER_API_KEY and restart the daemon)".to_string()
    })?;
    let configured = agent.model(&config.model);
    Ok(Model::new(ModelConfig {
        base_url: config.provider_base_url(),
        api_key,
        model: configured
            .name
            .clone()
            .unwrap_or_else(|| tachyon_util::config::Config::default_model().into()),
        temperature: configured.temperature.unwrap_or(0.0),
        max_completion_tokens: configured.max_completion_tokens,
        context_length: configured.context_length,
        parallel_tool_calls: false,
        reasoning: configured.reasoning,
        routing: config.provider_routing(),
        debug: std::env::var("TACHYON_DEBUG").is_ok_and(|value| value == "1" || value == "true"),
        debug_log: Some(tachyon_util::daemon::logs_dir().join("debug-http.log")),
    }))
}

fn review_tool() -> ToolSpec {
    ToolSpec::new(
        REVIEW_TOOL,
        "Submit one advisory semantic review of the supplied worker result.",
        json!({
            "type": "object",
            "properties": {
                "decision": { "type": "string", "enum": ["accept", "rework"] },
                "lifecycle": {
                    "type": "string",
                    "enum": ["keep_current", "release", "retain_short", "retain_long", "retain_persistent"]
                },
                "revised_objective": { "type": "string" },
                "rationale": { "type": "string", "minLength": 1, "maxLength": 2000 }
            },
            "required": ["decision", "rationale"],
            "additionalProperties": false
        }),
    )
}

fn lifecycle_recommendation(lifecycle: ModelLifecycle) -> LifecycleRecommendation {
    match lifecycle {
        ModelLifecycle::KeepCurrent => LifecycleRecommendation::KeepCurrent,
        ModelLifecycle::Release => LifecycleRecommendation::Release,
        ModelLifecycle::RetainShort => LifecycleRecommendation::Retain {
            lifetime_class: LifetimeClass::Short,
        },
        ModelLifecycle::RetainLong => LifecycleRecommendation::Retain {
            lifetime_class: LifetimeClass::Long,
        },
        ModelLifecycle::RetainPersistent => LifecycleRecommendation::Retain {
            lifetime_class: LifetimeClass::Persistent,
        },
    }
}

fn decision(
    request: &WorkReviewRequest,
    recommendation: WorkReviewRecommendation,
    rationale: impl Into<String>,
) -> WorkReviewDecision {
    WorkReviewDecision {
        review_id: request.review_id.clone(),
        coordinator_generation: request.coordinator_generation,
        work_id: request.candidate.work_id.clone(),
        generation: request.candidate.generation,
        assignment: request.candidate.assignment,
        recommendation,
        rationale: rationale.into(),
    }
}

fn inconclusive(
    request: &WorkReviewRequest,
    failure: WorkReviewFailure,
    rationale: impl Into<String>,
) -> WorkReviewDecision {
    decision(
        request,
        WorkReviewRecommendation::Inconclusive { failure },
        rationale,
    )
}

fn review_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("TACHYON_BACKGROUND_REVIEW_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_REVIEW_TIMEOUT_SECS)
            .max(1),
    )
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::{WorkResult, WorkReviewContext};

    fn request(outcome: WorkOutcome) -> WorkReviewRequest {
        WorkReviewRequest {
            review_id: "review-1".into(),
            coordinator_generation: 2,
            candidate: WorkResult {
                work_id: "work-1".into(),
                objective: "verify release".into(),
                generation: 3,
                assignment: 4,
                outcome,
            },
            worker: WorkReviewContext {
                worker_id: "worker-1".into(),
                current_lifetime_class: LifetimeClass::Short,
                turns_used: 1,
                turn_budget: Some(3),
                purpose: "research".into(),
            },
            deadline_ms: unix_now_ms() + 5_000,
        }
    }

    #[tokio::test]
    async fn invalid_non_completed_candidate_fails_closed_without_a_model() {
        let decision = review(
            &request(WorkOutcome::TimedOut { deadline_ms: 1 }),
            None,
            None,
        )
        .await;
        assert!(matches!(
            decision.recommendation,
            WorkReviewRecommendation::Inconclusive {
                failure: WorkReviewFailure::InvalidRequest
            }
        ));
    }

    #[test]
    fn lifecycle_values_map_to_advisory_recommendations() {
        assert_eq!(
            lifecycle_recommendation(ModelLifecycle::RetainPersistent),
            LifecycleRecommendation::Retain {
                lifetime_class: LifetimeClass::Persistent
            }
        );
        assert_eq!(
            lifecycle_recommendation(ModelLifecycle::Release),
            LifecycleRecommendation::Release
        );
    }

    #[test]
    fn model_review_rejects_unknown_fields() {
        assert!(serde_json::from_str::<ModelReview>(
            r#"{"decision":"accept","lifecycle":"keep_current","rationale":"ok","extra":true}"#
        )
        .is_err());
    }
}
