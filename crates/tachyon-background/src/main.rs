#![forbid(unsafe_code)]

use std::{future::Future, process::ExitCode, sync::Arc};

use serde::Deserialize;
use serde_json::json;
use tachyon_api::{
    LifecycleRecommendation, LifetimeClass, WorkOutcome, WorkReviewDecision, WorkReviewFailure,
    WorkReviewRecommendation, WorkReviewRequest,
};
use tachyon_model::{ChatMessage, Model, ModelConfig, Role, ToolSpec};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;

const REVIEW_TOOL: &str = "submit_work_review";
const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 20;
const MAX_REVIEW_INPUT_CHARS: usize = 32_000;
const MAX_CONCURRENT_REVIEWS: usize = 4;

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
    let persona = background.persona.clone().map(Arc::<str>::from);
    let model = model_from_config(&config, &background).ok().map(Arc::new);
    eprintln!("tachyon-background: ready");
    let result = process_reviews(
        tokio::io::stdin(),
        tokio::io::BufWriter::new(tokio::io::stdout()),
        move |request| {
            let model = model.clone();
            let persona = persona.clone();
            async move { review(&request, model.as_deref(), persona.as_deref()).await }
        },
    )
    .await;
    if let Err(error) = result {
        eprintln!("tachyon-background: review stream failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn process_reviews<R, W, F, Fut>(
    reader: R,
    mut writer: W,
    review_fn: F,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: Fn(WorkReviewRequest) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = WorkReviewDecision> + Send + 'static,
{
    let mut lines = tokio::io::BufReader::new(reader).lines();
    let mut reviews = JoinSet::new();
    loop {
        if reviews.len() == MAX_CONCURRENT_REVIEWS {
            write_next_decision(&mut reviews, &mut writer).await?;
            continue;
        }
        tokio::select! {
            result = reviews.join_next(), if !reviews.is_empty() => {
                write_joined_decision(result, &mut writer).await?;
            }
            line = lines.next_line() => {
                let Some(line) = line? else { break };
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
                let review_fn = review_fn.clone();
                reviews.spawn(async move { review_fn(request).await });
            }
        }
    }
    while !reviews.is_empty() {
        write_next_decision(&mut reviews, &mut writer).await?;
    }
    Ok(())
}

async fn write_next_decision<W: AsyncWrite + Unpin>(
    reviews: &mut JoinSet<WorkReviewDecision>,
    writer: &mut W,
) -> std::io::Result<()> {
    let result = reviews
        .join_next()
        .await
        .ok_or_else(|| std::io::Error::other("review task set was empty"))?;
    write_joined_decision(Some(result), writer).await
}

async fn write_joined_decision<W: AsyncWrite + Unpin>(
    result: Option<Result<WorkReviewDecision, tokio::task::JoinError>>,
    writer: &mut W,
) -> std::io::Result<()> {
    let decision = result
        .ok_or_else(|| std::io::Error::other("review task set was empty"))?
        .map_err(std::io::Error::other)?;
    let encoded = serde_json::to_vec(&decision).map_err(std::io::Error::other)?;
    writer.write_all(&encoded).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
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
    let timeout = review_execution_timeout(request.deadline_ms, unix_now_ms(), review_timeout());
    let mut relay = |_delta: &str| {};
    let completion = match tokio::time::timeout(
        timeout,
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
    let configured = std::env::var("TACHYON_BACKGROUND_REVIEW_TIMEOUT_SECS").ok();
    review_timeout_from(configured.as_deref())
}

fn review_timeout_from(configured: Option<&str>) -> std::time::Duration {
    let seconds = configured
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_REVIEW_TIMEOUT_SECS)
        .max(1);
    std::time::Duration::from_secs(seconds)
}

fn review_execution_timeout(
    deadline_ms: u64,
    started_ms: u64,
    configured: std::time::Duration,
) -> std::time::Duration {
    let remaining_ms = deadline_ms.saturating_sub(started_ms).max(1);
    std::time::Duration::from_millis(remaining_ms.min(configured.as_millis() as u64))
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
    use std::collections::HashSet;
    use tachyon_api::{WorkResult, WorkReviewContext};
    use tokio::sync::{mpsc, Semaphore};

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

    #[tokio::test]
    async fn review_stream_runs_with_a_small_bound_and_preserves_correlation() {
        let (service, client) = tokio::io::duplex(16_384);
        let (service_reader, service_writer) = tokio::io::split(service);
        let (client_reader, mut client_writer) = tokio::io::split(client);
        let gate = Arc::new(Semaphore::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let service_gate = Arc::clone(&gate);
        let service = tokio::spawn(process_reviews(
            service_reader,
            service_writer,
            move |request| {
                let gate = Arc::clone(&service_gate);
                let started_tx = started_tx.clone();
                async move {
                    started_tx.send(request.review_id.clone()).unwrap();
                    gate.acquire().await.unwrap().forget();
                    inconclusive(&request, WorkReviewFailure::ProviderError, "test")
                }
            },
        ));

        let expected = (0..MAX_CONCURRENT_REVIEWS + 2)
            .map(|index| format!("review-{index}"))
            .collect::<HashSet<_>>();
        for review_id in &expected {
            let mut request = request(WorkOutcome::Completed {
                result: "verified".into(),
                artifacts: Vec::new(),
                context: String::new(),
                suggested_reuse: false,
            });
            request.review_id = review_id.clone();
            client_writer
                .write_all(&serde_json::to_vec(&request).unwrap())
                .await
                .unwrap();
            client_writer.write_all(b"\n").await.unwrap();
        }
        client_writer.shutdown().await.unwrap();

        for _ in 0..MAX_CONCURRENT_REVIEWS {
            started_rx.recv().await.unwrap();
        }
        tokio::task::yield_now().await;
        assert!(started_rx.try_recv().is_err());

        gate.add_permits(expected.len());
        service.await.unwrap().unwrap();
        let mut output = tokio::io::BufReader::new(client_reader).lines();
        let mut actual = HashSet::new();
        while let Some(line) = output.next_line().await.unwrap() {
            let decision: WorkReviewDecision = serde_json::from_str(&line).unwrap();
            assert_eq!(decision.work_id, "work-1");
            assert_eq!(decision.generation, 3);
            assert_eq!(decision.assignment, 4);
            actual.insert(decision.review_id);
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn expired_completed_candidate_fails_closed_without_a_model() {
        let mut request = request(WorkOutcome::Completed {
            result: "verified".into(),
            artifacts: Vec::new(),
            context: String::new(),
            suggested_reuse: false,
        });
        request.deadline_ms = unix_now_ms().saturating_sub(1);
        let decision = review(&request, None, None).await;
        assert!(matches!(
            decision.recommendation,
            WorkReviewRecommendation::Inconclusive {
                failure: WorkReviewFailure::InvalidRequest
            }
        ));
    }

    #[test]
    fn review_deadline_allows_twenty_seconds_and_execution_uses_the_remainder() {
        assert_eq!(
            review_timeout_from(None),
            std::time::Duration::from_secs(20)
        );
        assert_eq!(
            review_timeout_from(Some("7")),
            std::time::Duration::from_secs(7)
        );
        assert_eq!(
            review_execution_timeout(20_000, 11_000, review_timeout_from(None)),
            std::time::Duration::from_secs(9)
        );
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
