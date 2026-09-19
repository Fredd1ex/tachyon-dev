use tachyon_api::campaign_oversight::*;
use tachyon_model::{ChatMessage, Model, Role};
use tachyon_orchestrator::{
    agents::campaign::assessment::{prepare, validate_completion},
    capabilities::Capability,
    registry::{self, Registry},
};

/// Grants are host policy, never fields decoded from the request. No executable
/// tools are passed to the model, even when the read capabilities are granted.
pub(super) async fn assess_campaign(
    request: &CampaignAssessmentRequest,
    model: Option<&Model>,
    grants: &[Capability],
    registry: &Registry<'_>,
) -> CampaignAssessmentResponse {
    let result = async {
        let prepared = prepare(request, grants, registry)?;
        let model = model.ok_or(CampaignAssessmentError::ModelUnavailable)?;
        let messages = [
            ChatMessage::new(Role::System, prepared.system),
            ChatMessage::new(Role::User, prepared.input),
        ];
        let completion = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            model.chat(&messages, None, &mut |_| {}),
        )
        .await
        .map_err(|_| CampaignAssessmentError::TimedOut)?
        .map_err(|_| CampaignAssessmentError::ProviderError)?;
        validate_completion(
            &completion.text,
            !completion.tool_calls.is_empty(),
            completion.finish_reason.as_deref(),
            &prepared.snapshot,
        )
    }
    .await;
    CampaignAssessmentResponse {
        request_id: request.request_id.clone(),
        campaign_id: request.campaign_id.clone(),
        revision: request.revision,
        result,
    }
}

pub(super) async fn dispatch(
    request: CampaignAssessmentRequest,
    model: Option<&Model>,
) -> CampaignAssessmentResponse {
    assess_campaign(
        &request,
        model,
        &[Capability::Todo, Capability::Monitor],
        &registry::builtin(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::{
        monitor::{MonitorQuery, MonitorScope, MonitorSnapshot, MonitorVersion},
        operational_events::OperationalWatermark,
        todo::{TodoResponse, TodoScope, TodoStatus as ApiStatus},
    };
    use tachyon_orchestrator::{
        agents::campaign::assessment::{snapshot, validate_assessment},
        registry::{HostLane, InvocationContext, InvocationKind, RoleId},
    };

    fn request() -> CampaignAssessmentRequest {
        CampaignAssessmentRequest {
            triggers: vec![],
            evidence_total: 0,
            evidence: vec![],
            kind: CampaignRequestKind::CampaignAssessment,
            request_id: "assessment-1".into(),
            campaign_id: "c".into(),
            revision: 7,
            objective_summary: "Ship".into(),
            todo_scope: TodoScope::Campaign {
                campaign_id: "c".into(),
            },
            todos: TodoResponse::List {
                todos: vec![],
                scope_revision: 0,
                watermark: OperationalWatermark {
                    instance_id: "epoch".into(),
                    sequence: 0,
                },
                next_cursor: None,
            },
            monitor: MonitorSnapshot {
                query: MonitorQuery {
                    scope: MonitorScope::Campaign {
                        campaign_id: "c".into(),
                    },
                    after: None,
                    limit: 20,
                },
                version: MonitorVersion {
                    epoch: "epoch".into(),
                    sequence: 0,
                },
                payload: None,
                stale: None,
            },
        }
    }

    #[tokio::test]
    async fn grants_registry_scope_and_missing_model_fail_closed() {
        let mut request = request();
        let grants = [Capability::Todo, Capability::Monitor];
        for grant in [&[][..], &grants[..1]] {
            assert_eq!(
                assess_campaign(&request, None, grant, &registry::builtin())
                    .await
                    .result
                    .unwrap_err(),
                CampaignAssessmentError::MissingCapabilities
            );
        }
        assert_eq!(
            assess_campaign(&request, None, &grants, &Registry::new(&[]).unwrap())
                .await
                .result
                .unwrap_err(),
            CampaignAssessmentError::RegistryUnavailable
        );
        let response = dispatch(request.clone(), None).await;
        assert_eq!(response.request_id, "assessment-1");
        assert_eq!(response.revision, 7);
        assert_eq!(
            response.result.unwrap_err(),
            CampaignAssessmentError::ModelUnavailable
        );
        request.monitor.query.scope = MonitorScope::Host;
        assert_eq!(
            dispatch(request, None).await.result.unwrap_err(),
            CampaignAssessmentError::InvalidRequest
        );
    }

    #[test]
    fn projection_and_assessment_are_bounded_and_typed() {
        let mut request = request();
        let projected = snapshot(&request).unwrap();
        assert!(projected.resources.final_tokens.is_none());
        let valid = r#"{"summary":"Evidence missing","findings":[],"refs":[],"blockers":[],"attention":"insufficient_evidence"}"#;
        assert!(validate_assessment(valid, &projected).is_ok());
        assert!(validate_completion(valid, false, Some("stop"), &projected).is_ok());
        assert!(validate_completion(valid, false, None, &projected).is_ok());
        for (tools, finish) in [
            (true, Some("stop")),
            (false, Some("length")),
            (false, Some("tool_calls")),
        ] {
            assert!(matches!(
                validate_completion(valid, tools, finish, &projected),
                Err(CampaignAssessmentError::MalformedOutput)
            ));
        }
        assert!(validate_assessment(
            &valid.replace("\"refs\":[]", "\"refs\":[\"invented\"]"),
            &projected
        )
        .is_err());
        assert!(validate_assessment(
            &valid.replace("\"attention\":", "\"allocate\":true,\"attention\":"),
            &projected
        )
        .is_err());
        request.objective_summary = "x".repeat(4097);
        assert!(snapshot(&request).is_err());
    }

    #[test]
    fn full_bounded_page_is_valid_when_the_plan_has_more_todos() {
        use tachyon_api::todo::{Todo, TodoActor, TodoCursor};
        let mut request = request();
        let actor = TodoActor {
            source: "operator".into(),
            actor: "fixture".into(),
        };
        request.todos = TodoResponse::List {
            todos: (1..=20)
                .map(|i| Todo {
                    schema_version: 1,
                    id: format!("todo-{i:02}"),
                    scope: request.todo_scope.clone(),
                    title: format!("Task {i}"),
                    description: String::new(),
                    status: ApiStatus::Pending,
                    order_key: i,
                    revision: 1,
                    created_ms: 1,
                    updated_ms: 1,
                    created_by: actor.clone(),
                    updated_by: actor.clone(),
                })
                .collect(),
            scope_revision: 100,
            watermark: OperationalWatermark {
                instance_id: "epoch".into(),
                sequence: 100,
            },
            next_cursor: Some(TodoCursor {
                version: 1,
                instance_id: "epoch".into(),
                scope: request.todo_scope.clone(),
                filter: Default::default(),
                scope_revision: 100,
                after_order_key: 20,
                after_id: "todo-20".into(),
            }),
        };
        let prepared = prepare(
            &request,
            &[Capability::Todo, Capability::Monitor],
            &registry::builtin(),
        )
        .unwrap();
        assert!(prepared.snapshot.todos_partial);
        assert_eq!(prepared.snapshot.todos.len(), 20);
        assert_eq!(prepared.snapshot.todo_revision, 100);
        assert!(prepared.input.contains("\"todos_partial\":true"));
    }

    #[test]
    fn rejects_duplicate_invalid_records_and_mismatched_cursors() {
        use tachyon_api::todo::{Todo, TodoActor, TodoCursor};
        let mut request = request();
        let todo = Todo {
            schema_version: 1,
            id: "todo-1".into(),
            scope: request.todo_scope.clone(),
            title: "Ship".into(),
            description: String::new(),
            status: ApiStatus::Pending,
            order_key: 1,
            revision: 1,
            created_ms: 1,
            updated_ms: 1,
            created_by: TodoActor {
                source: "operator".into(),
                actor: "uid:1".into(),
            },
            updated_by: TodoActor {
                source: "operator".into(),
                actor: "uid:1".into(),
            },
        };
        for invalid in 0..5 {
            let mut records = vec![todo.clone()];
            let mut cursor = None;
            match invalid {
                0 => records.push(todo.clone()),
                1 => records[0].schema_version = 2,
                2 => records[0].revision = 0,
                3 => records[0].revision = 2,
                _ => {
                    cursor = Some(TodoCursor {
                        version: 1,
                        instance_id: "epoch".into(),
                        scope: request.todo_scope.clone(),
                        filter: Default::default(),
                        scope_revision: 2,
                        after_order_key: 1,
                        after_id: "todo-1".into(),
                    })
                }
            }
            request.todos = TodoResponse::List {
                todos: records,
                scope_revision: 1,
                watermark: OperationalWatermark {
                    instance_id: "epoch".into(),
                    sequence: 1,
                },
                next_cursor: cursor,
            };
            assert!(snapshot(&request).is_err(), "case {invalid}");
        }
    }

    #[tokio::test]
    async fn nested_unknown_observation_fields_never_reach_dispatch() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut wire = serde_json::to_value(request()).unwrap();
        wire["monitor"]["allocate"] = true.into();
        let (mut client, server) = tokio::io::duplex(8192);
        let (reader, writer) = tokio::io::split(server);
        let service = tokio::spawn(crate::requests::process_requests(
            reader,
            writer,
            |_| async { panic!("unknown nested field reached inference") },
        ));
        client
            .write_all(&serde_json::to_vec(&wire).unwrap())
            .await
            .unwrap();
        client.write_all(b"\n").await.unwrap();
        client.shutdown().await.unwrap();
        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        service.await.unwrap().unwrap();
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn one_shot_model_receives_exact_snapshot_without_tools() {
        use serde_json::json;
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model = Model::new(tachyon_model::ModelConfig {
            base_url: format!("http://{}", listener.local_addr().unwrap()),
            api_key: "offline-fixture".into(),
            model: "fixture".into(),
            temperature: 0.0,
            max_completion_tokens: Some(128),
            context_length: Some(4096),
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: None,
            debug: false,
            debug_log: None,
        });
        let request = request();
        let expected_snapshot = snapshot(&request).unwrap().render().unwrap();
        let expected_prompt = registry::builtin()
            .resolve(
                RoleId::Campaign,
                HostLane::Background,
                InvocationKind::Primary,
            )
            .unwrap()
            .render(InvocationContext::default())
            .unwrap()
            .prompt;
        let server = async {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
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
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(body.get("tools").is_none());
            assert!(body.get("tool_choice").is_none());
            assert_eq!(
                body["messages"],
                json!([
                    {"role":"system", "content":expected_prompt},
                    {"role":"user", "content":expected_snapshot}
                ])
            );
            let text = r#"{"summary":"Missing evidence","findings":[],"refs":[],"blockers":[],"attention":"insufficient_evidence"}"#;
            let body = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":"stop"}]})
            );
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            reader
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (result, ()) = tokio::join!(dispatch(request, Some(&model)), server);
            result
        })
        .await
        .unwrap();
        assert_eq!(result.request_id, "assessment-1");
        assert_eq!(result.result.unwrap().summary, "Missing evidence");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn explicit_stream_dispatch_and_empty_input_need_no_provider() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let wire = serde_json::to_string(&request()).unwrap();
        assert!(matches!(
            serde_json::from_str::<BackgroundRequest>(&wire).unwrap(),
            BackgroundRequest::CampaignAssessment(_)
        ));
        assert!(serde_json::from_str::<BackgroundRequest>(
            &wire.replace("campaign_assessment", "unknown")
        )
        .is_err());
        assert!(serde_json::from_str::<BackgroundRequest>(
            &wire.replace("\"revision\":7", "\"revision\":7,\"grants\":[\"allocate\"]")
        )
        .is_err());
        for input in [String::new(), format!("{wire}\n")] {
            let (mut client, server) = tokio::io::duplex(8192);
            let (reader, writer) = tokio::io::split(server);
            let service = tokio::spawn(crate::requests::process_requests(
                reader,
                writer,
                |request| async {
                    let BackgroundRequest::CampaignAssessment(request) = request else {
                        panic!("wrong dispatch")
                    };
                    BackgroundResponse::CampaignAssessment(dispatch(request, None).await)
                },
            ));
            client.write_all(input.as_bytes()).await.unwrap();
            client.shutdown().await.unwrap();
            let mut output = String::new();
            client.read_to_string(&mut output).await.unwrap();
            service.await.unwrap().unwrap();
            if input.is_empty() {
                assert!(output.is_empty());
            } else {
                let result: CampaignAssessmentResponse = serde_json::from_str(&output).unwrap();
                assert_eq!(result.request_id, "assessment-1");
                assert_eq!(
                    result.result.unwrap_err(),
                    CampaignAssessmentError::ModelUnavailable
                );
            }
        }
    }
}
