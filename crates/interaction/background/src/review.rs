use serde::Deserialize;
#[cfg(test)]
use serde_json::json;
use tachyon_api::{
    LifecycleRecommendation, LifetimeClass, WorkOutcome, WorkReviewDecision, WorkReviewFailure,
    WorkReviewRecommendation, WorkReviewRequest,
};
use tachyon_model::{ChatMessage, Model, Role, ToolSpec};

use tachyon_orchestrator::agents::coordinator::tools::REVIEW_TOOL;
use tachyon_orchestrator::registry::{
    self, HostLane, InvocationContext, InvocationKind, Registry, RoleId,
};
const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 20;
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

pub(super) async fn review(
    request: &WorkReviewRequest,
    model: Option<&Model>,
    persona: Option<&str>,
) -> WorkReviewDecision {
    review_with_registry(request, model, persona, &registry::builtin()).await
}

async fn review_with_registry(
    request: &WorkReviewRequest,
    model: Option<&Model>,
    persona: Option<&str>,
    registry: &Registry<'_>,
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
    let rendered = match registry
        .resolve(
            RoleId::Coordinator,
            HostLane::Background,
            InvocationKind::Review,
        )
        .and_then(|resolved| {
            resolved.render(InvocationContext {
                identity: None,
                persona,
            })
        }) {
        Ok(rendered) => rendered,
        Err(error) => {
            return inconclusive(
                request,
                WorkReviewFailure::ModelUnavailable,
                error.to_string(),
            )
        }
    };
    if rendered.output_visibility != registry::OutputVisibility::Internal
        || rendered.tools.len() != 1
        || rendered.tools[0].name != REVIEW_TOOL
    {
        return inconclusive(
            request,
            WorkReviewFailure::ModelUnavailable,
            "role registry: invalid review visibility or tool selection",
        );
    }
    let messages = review_messages(request, rendered.prompt);
    let tools = rendered
        .tools
        .into_iter()
        .map(|schema| ToolSpec::new(schema.name, schema.description, schema.parameters))
        .collect::<Vec<_>>();
    let timeout = review_execution_timeout(request.deadline_ms, unix_now_ms(), review_timeout());
    let mut relay = |_delta: &str| {};
    let completion = match tokio::time::timeout(
        timeout,
        model.chat_requiring_tool(&messages, &tools, (REVIEW_TOOL, "decision"), &mut relay),
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

fn review_messages(request: &WorkReviewRequest, prompt: String) -> Vec<ChatMessage> {
    vec![
        ChatMessage::new(Role::System, prompt),
        ChatMessage::new(
            Role::User,
            serde_json::to_string(request).unwrap_or_default(),
        ),
    ]
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
    use tachyon_api::{WorkResult, WorkReviewContext};

    fn request(outcome: WorkOutcome) -> WorkReviewRequest {
        WorkReviewRequest {
            review_id: "review-1".into(),
            coordinator_generation: 2,
            candidate: WorkResult {
                attempt_id: None,
                candidate_refs: None,
                final_context: None,
                instruction_revision: None,
                work_id: "work-1".into(),
                evidence: Default::default(),
                timing: None,
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
    async fn registry_failures_never_contact_provider_and_preserve_identity() {
        use tachyon_orchestrator::registry::{InvocationBinding, RegistryError, RoleDescriptor};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let model = local_model(listener.local_addr().unwrap());
        let role = tachyon_orchestrator::agents::coordinator::definition();
        const BROKEN: &[InvocationBinding] = &[InvocationBinding {
            kind: InvocationKind::Review,
            prompt: |_| Err(RegistryError::MissingConversationIdentity),
            tools: || panic!("must not render tools"),
        }];
        let request = request(WorkOutcome::Completed {
            result: "verified".into(),
            artifacts: vec![],
            context: String::new(),
            suggested_reuse: false,
        });
        for roles in [
            vec![],
            vec![RoleDescriptor {
                enabled: false,
                ..role
            }],
            vec![RoleDescriptor {
                host_lane: HostLane::Foreground,
                ..role
            }],
            vec![RoleDescriptor {
                invocations: &[],
                ..role
            }],
            vec![RoleDescriptor {
                invocations: BROKEN,
                ..role
            }],
        ] {
            let registry = Registry::new(&roles).unwrap();
            let result = review_with_registry(&request, Some(&model), None, &registry).await;
            assert_eq!(result.review_id, request.review_id);
            assert_eq!(
                result.coordinator_generation,
                request.coordinator_generation
            );
            assert_eq!(result.work_id, request.candidate.work_id);
            assert_eq!(result.generation, request.candidate.generation);
            assert_eq!(result.assignment, request.candidate.assignment);
            assert!(matches!(
                result.recommendation,
                WorkReviewRecommendation::Inconclusive {
                    failure: WorkReviewFailure::ModelUnavailable
                }
            ));
            assert!(result.rationale.starts_with("role registry:"));
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
            assert_eq!(
                review_with_registry(&request, None, None, &registry)
                    .await
                    .rationale,
                "background model unavailable"
            );
        }
    }

    fn local_model(address: std::net::SocketAddr) -> Model {
        Model::new(tachyon_model::ModelConfig {
            base_url: format!("http://{address}"),
            api_key: "local-test-only".into(),
            model: "scripted-test-model".into(),
            temperature: 0.0,
            max_completion_tokens: Some(128),
            context_length: Some(4096),
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        })
    }

    #[tokio::test]
    async fn local_review_uses_only_registry_review_schema_and_preserves_decision() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        for fresh_lookup in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let model = local_model(listener.local_addr().unwrap());
            let mut request = request(WorkOutcome::Completed {
                result: "verified".into(),
                artifacts: vec![],
                context: String::new(),
                suggested_reuse: false,
            });
            if fresh_lookup {
                request.candidate.objective = "Retrieve fresh source evidence".into();
            }
            let server = async {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                let mut length = 0;
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let schema = tachyon_orchestrator::agents::coordinator::tools::review_tool();
                assert_eq!(
                    body["tools"],
                    json!([{"type":"function", "function": {
                        "name": schema.name, "description": schema.description, "parameters": schema.parameters
                    }}])
                );
                let prompt = registry::builtin()
                    .resolve(
                        RoleId::Coordinator,
                        HostLane::Background,
                        InvocationKind::Review,
                    )
                    .unwrap()
                    .render(InvocationContext {
                        identity: None,
                        persona: Some("fixture persona"),
                    })
                    .unwrap()
                    .prompt;
                assert_eq!(body["messages"][0]["content"], prompt);
                let received: WorkReviewRequest =
                    serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
                assert_eq!(received.candidate.evidence, request.candidate.evidence);
                assert_eq!(received.candidate.evidence.observed_invocations, None);
                let arguments = if fresh_lookup {
                    "{\"decision\":\"rework\",\"rationale\":\"fresh source evidence missing\"}"
                } else {
                    "{\"decision\":\"accept\",\"lifecycle\":\"keep_current\",\"rationale\":\"verified\"}"
                };
                let body = format!(
                    "data: {}\n\ndata: [DONE]\n\n",
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"review-call","type":"function","function":{"name":REVIEW_TOOL,"arguments":arguments}}]},"finish_reason":"tool_calls"}]})
                );
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            };
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let (result, ()) = tokio::join!(
                    review(&request, Some(&model), Some("fixture persona")),
                    server
                );
                result
            })
            .await
            .unwrap();
            if fresh_lookup {
                assert_eq!(
                    result,
                    decision(
                        &request,
                        WorkReviewRecommendation::Rework {
                            revised_objective: None
                        },
                        "fresh source evidence missing"
                    )
                );
            } else {
                assert_eq!(
                    result,
                    decision(
                        &request,
                        WorkReviewRecommendation::Accept {
                            lifecycle: LifecycleRecommendation::KeepCurrent
                        },
                        "verified"
                    )
                );
            }
        }
    }

    #[tokio::test]
    async fn missing_model_preserves_review_identity_without_fabricating_inference() {
        let request = request(WorkOutcome::Completed {
            result: "fixture evidence".into(),
            artifacts: Vec::new(),
            context: String::new(),
            suggested_reuse: false,
        });
        let decision = review(&request, None, None).await;
        assert_eq!(decision.review_id, request.review_id);
        assert_eq!(
            decision.coordinator_generation,
            request.coordinator_generation
        );
        assert_eq!(decision.work_id, request.candidate.work_id);
        assert_eq!(decision.generation, request.candidate.generation);
        assert_eq!(decision.assignment, request.candidate.assignment);
        assert!(matches!(
            decision.recommendation,
            WorkReviewRecommendation::Inconclusive {
                failure: WorkReviewFailure::ModelUnavailable,
            }
        ));
        assert!(request.candidate.timing.is_none());
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
    fn review_message_preserves_structured_hostcall_results_as_untrusted_data() {
        let mut request = request(WorkOutcome::Completed {
            result: "fixture.lua:2".into(),
            artifacts: Vec::new(),
            context: String::new(),
            suggested_reuse: false,
        });
        let tools = &mut request.candidate.evidence.tools;
        tools.push(tachyon_api::types::WorkToolEvidence {
            call_id: Some("python-1-2".into()),
            parent_call_id: Some("cell-0".into()),
            tool_name: "read".into(),
            arguments: json!({"path":"fixture.lua","offset":2,"limit":1}),
            output: json!({"content":"return 'fixture'", "is_error":false, "truncated":false, "metadata":{"path":"fixture.lua"}}),
        });
        tools.push(tachyon_api::types::WorkToolEvidence {
            call_id: Some("cell-1".into()),
            parent_call_id: None,
            tool_name: "ipython".into(),
            arguments: json!({"code":"raise ValueError('fixture')"}),
            output: json!({"content":"ValueError: fixture", "is_error":true, "truncated":false, "metadata":{"exit_code":1}}),
        });
        request.candidate.evidence.omitted = 2;
        request.candidate.evidence.observed_invocations = Some(4);
        let rendered = registry::builtin()
            .resolve(
                RoleId::Coordinator,
                HostLane::Background,
                InvocationKind::Review,
            )
            .unwrap()
            .render(InvocationContext::default())
            .unwrap();
        let messages = review_messages(&request, rendered.prompt);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, Role::System);
        assert_eq!(messages[1].role, Role::User);
        let tachyon_model::Content::Text(input) = &messages[1].content[0] else {
            panic!("expected review input");
        };
        let received: WorkReviewRequest = serde_json::from_str(input).unwrap();
        assert_eq!(received, request);
        assert_eq!(
            received.candidate.evidence.tools[1].output["is_error"],
            true
        );
        assert!(input.len() < MAX_REVIEW_INPUT_CHARS);
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
