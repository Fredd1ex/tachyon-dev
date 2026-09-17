use super::*;
use tachyon_api::{
    agents::{
        services::{MonitorRequest, Scope, TodoRequest},
        Control, Reply, Request,
    },
    todo::{TodoError, TodoResponse, TodoScope, TodoStatus},
};

#[tokio::test]
#[ignore = "requires freshly built GHOST_TEST_BIN and existing IPython; localhost fake provider only"]
async fn actual_ghost_services_local_model_native_and_python() {
    let executable =
        std::path::PathBuf::from(std::env::var_os("GHOST_TEST_BIN").expect("fresh Ghost binary"));
    for mode in ["services", "services-python"] {
        let (_dir, store, funding, mut reservation) = tests::setup_agents();
        let store = Arc::new(store);
        let workspace = tempfile::tempdir().unwrap();
        let (url, mut wire, http) = broker_tests::fixture(store.clone(), mode).await;
        reservation.estimate.base_url = url;
        reservation.estimate.max_request_bytes = 32000;
        let model_name = reservation.estimate.model.clone();
        let broker = ModelBroker::new(store.clone(), broker_tests::model(&reservation))
            .with_controls([Control::Todo, Control::Monitor]);
        let work = tachyon_api::types::WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: reservation.identity.work_id.clone(),
            objective: funding.admission.objective.clone(),
            generation: 1,
            assignment: 1,
            deadline_ms: crate::runtime_store::monitor::now_ms() + 20000,
            lifetime_class: tachyon_api::types::LifetimeClass::Short,
        };
        broker
            .launch_private(
                &executable,
                workspace.path(),
                workspace.path(),
                funding,
                reservation.clone(),
                work,
                Instant::now() + std::time::Duration::from_secs(20),
            )
            .await
            .unwrap();
        let first = wire.recv().await.unwrap();
        assert_eq!(first["model"], model_name);
        let names: Vec<_> = first["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["function"]["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"todo") && names.contains(&"monitor") && names.contains(&"work"));
        assert!(!names.contains(&"agents") && !names.contains(&"history"));
        assert!(!first["messages"]
            .to_string()
            .contains("durable-plan-marker"));
        let second = wire.recv().await.unwrap();
        assert_eq!(second["model"], model_name);
        let output = second["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["role"] == "tool")
            .map(|m| m["content"].as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(output.contains("durable-plan-marker"), "{output}");
        assert!(output.contains("sampled_at_ms"), "{output}");
        assert!(output.contains("objective"), "{output}");
        let scope = TodoScope::Work {
            work_id: reservation.identity.work_id.clone(),
        };
        let response = store
            .todos(crate::runtime_store::todo::TodoAuthority::Ghost {
                scope: scope.clone(),
                work_id: reservation.identity.work_id,
                campaign_id: reservation.identity.campaign_id,
            })
            .unwrap()
            .execute(tachyon_api::todo::TodoRequest::List {
                scope,
                filter: Default::default(),
                limit: None,
                cursor: None,
            })
            .unwrap();
        assert!(
            matches!(response, TodoResponse::List { todos, scope_revision:1, .. } if todos.len() == 1 && todos[0].status == TodoStatus::Pending)
        );
        http.abort();
    }
}

#[tokio::test]
async fn durable_services_private_allowlist_replay_revision_scope_and_revocation() {
    for grants in [
        vec![Control::Resource],
        vec![Control::Todo, Control::Monitor],
        vec![Control::TodoCampaign, Control::MonitorCampaign],
        vec![
            Control::Todo,
            Control::Monitor,
            Control::MonitorAvailability,
        ],
    ] {
        let (_dir, store, funding, reservation) = tests::setup_agents();
        let store = Arc::new(store);
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding, None)
            .unwrap();
        let campaign = grants.contains(&Control::TodoCampaign);
        let allowed = grants.contains(&Control::Todo) || campaign;
        let availability = grants.contains(&Control::MonitorAvailability);
        let client_grants = grants.clone();
        let broker = ModelBroker::new(store.clone(), broker_tests::model(&reservation))
            .with_controls(grants);
        let (host, mut client) = tachyon_model::broker::private_pair().unwrap();
        client.controls = client_grants;
        let scope = if campaign {
            Scope::CurrentCampaign
        } else {
            Scope::CurrentWork
        };
        let request = Request::Todo {
            request: TodoRequest::Add {
                scope,
                command_id: "command".into(),
                expected_revision: 0,
                title: "plan-only".into(),
                description: String::new(),
            },
        };
        let worker = async {
            let first = client.control(&request).await.unwrap();
            if allowed {
                let Reply::Todo {
                    result:
                        Ok(TodoResponse::Mutation {
                            todo,
                            scope_revision: 1,
                            ..
                        }),
                    ..
                } = first
                else {
                    panic!("{first:?}")
                };
                assert_eq!(todo.created_by.source, "ghost");
                assert_eq!(todo.created_by.actor, reservation.identity.work_id);
                assert_eq!(
                    todo.scope,
                    if campaign {
                        TodoScope::Campaign {
                            campaign_id: reservation.identity.campaign_id.clone(),
                        }
                    } else {
                        TodoScope::Work {
                            work_id: reservation.identity.work_id.clone(),
                        }
                    }
                );
                let replay = client.control(&request).await.unwrap();
                assert!(
                    matches!(replay, Reply::Todo { result: Ok(TodoResponse::Mutation { todo: replay, .. }), .. } if replay == todo)
                );
                let mut conflict = request.clone();
                if let Request::Todo {
                    request: TodoRequest::Add { title, .. },
                } = &mut conflict
                {
                    *title = "different".into();
                }
                assert!(matches!(
                    client.control(&conflict).await.unwrap(),
                    Reply::Todo {
                        result: Err(TodoError::CommandConflict),
                        ..
                    }
                ));
                let update = Request::Todo {
                    request: TodoRequest::Update {
                        scope,
                        command_id: "done".into(),
                        expected_revision: 1,
                        id: todo.id.clone(),
                        title: None,
                        description: None,
                        status: Some(TodoStatus::Completed),
                    },
                };
                assert!(
                    matches!(client.control(&update).await.unwrap(), Reply::Todo { result: Ok(TodoResponse::Mutation { todo, .. }), .. } if todo.status == TodoStatus::Completed)
                );
                let mut stale = update;
                if let Request::Todo {
                    request: TodoRequest::Update { command_id, .. },
                } = &mut stale
                {
                    *command_id = "stale".into();
                }
                assert!(matches!(
                    client.control(&stale).await.unwrap(),
                    Reply::Todo {
                        result: Err(TodoError::RevisionConflict {
                            current_revision: 2
                        }),
                        ..
                    }
                ));
                assert!(client.completion_proposal().is_none());
                let second = Request::Todo {
                    request: TodoRequest::Add {
                        scope,
                        command_id: "second".into(),
                        expected_revision: 2,
                        title: "second record".into(),
                        description: String::new(),
                    },
                };
                assert!(matches!(
                    client.control(&second).await.unwrap(),
                    Reply::Todo { result: Ok(_), .. }
                ));
                let list = |cursor| Request::Todo {
                    request: TodoRequest::List {
                        scope,
                        filter: Default::default(),
                        limit: Some(1),
                        cursor,
                    },
                };
                let Reply::Todo {
                    scope: resolved,
                    result:
                        Ok(TodoResponse::List {
                            todos,
                            scope_revision: 3,
                            next_cursor: Some(cursor),
                            ..
                        }),
                } = client.control(&list(None)).await.unwrap()
                else {
                    panic!()
                };
                assert_eq!(resolved, todo.scope);
                assert_eq!(todos.len(), 1);
                assert_eq!(todos[0].id, todo.id);
                assert!(
                    matches!(client.control(&list(Some(cursor.clone()))).await.unwrap(), Reply::Todo { result: Ok(TodoResponse::List { todos, next_cursor: None, .. }), .. } if todos.len() == 1 && todos[0].title == "second record")
                );
                let mut forged = cursor;
                forged.scope = TodoScope::Conversation {
                    id: "foreign".into(),
                };
                assert!(matches!(
                    client.control(&list(Some(forged))).await.unwrap(),
                    Reply::Todo { result: Err(_), .. }
                ));
                let monitor = Request::Monitor {
                    request: MonitorRequest::Snapshot {
                        scope,
                        after: None,
                        limit: 8,
                    },
                };
                let Reply::Monitor {
                    query,
                    result: Ok(mut payload),
                } = client.control(&monitor).await.unwrap()
                else {
                    panic!()
                };
                assert_eq!(!payload.capacities.is_empty(), availability);
                if availability {
                    assert!(payload
                        .capacities
                        .iter()
                        .any(|c| c.resource == tachyon_api::monitor::CapacityResource::Cpu));
                    assert!(payload
                        .capacities
                        .iter()
                        .any(|c| c.resource == tachyon_api::monitor::CapacityResource::Model));
                    assert!(payload.capacities.iter().all(|c| c.sampled_at_ms > 0));
                }
                payload.capacities.clear();
                assert!(payload.durable.sampled_at_ms > 0);
                let expected = store.monitor_sample(&[query]).unwrap().remove(0).unwrap();
                assert!(payload.same_values(&expected));
                let guard = store.compute.lock().unwrap();
                let contended = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    client.control(&monitor),
                )
                .await;
                drop(guard);
                let contended = contended.unwrap().unwrap();
                if availability {
                    assert!(matches!(
                        contended,
                        Reply::Monitor {
                            result: Err(tachyon_api::monitor::MonitorError::Unavailable),
                            ..
                        }
                    ));
                } else {
                    assert!(matches!(contended, Reply::Monitor { result: Ok(_), .. }));
                }
            } else {
                assert!(matches!(first, Reply::Denied));
            }
            let foreign_scope = if campaign {
                Scope::CurrentWork
            } else {
                Scope::CurrentCampaign
            };
            for request in [
                Request::Todo {
                    request: TodoRequest::List {
                        scope: foreign_scope,
                        filter: Default::default(),
                        limit: None,
                        cursor: None,
                    },
                },
                Request::Monitor {
                    request: MonitorRequest::Snapshot {
                        scope: foreign_scope,
                        after: None,
                        limit: 8,
                    },
                },
            ] {
                assert!(matches!(
                    client.control(&request).await.unwrap(),
                    Reply::Denied
                ));
            }
            store.host_revoke_model_permit(&permit).unwrap();
            assert!(matches!(
                client.control(&request).await.unwrap(),
                Reply::Denied
            ));
            drop(client);
        };
        let (_, ()) = tokio::join!(
            broker.serve_private(
                host,
                &permit,
                reservation.clone(),
                Instant::now() + std::time::Duration::from_secs(5)
            ),
            worker
        );
    }
}
