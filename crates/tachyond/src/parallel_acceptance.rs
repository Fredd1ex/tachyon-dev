//! Real foreground process and daemon IPC, with no installed services or credentials.
use super::*;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tachyon_api::{
    InteractionCommandEnvelope, InteractionEvent, InteractionEventEnvelope, InteractionMetadata,
};
use tokio::sync::mpsc as async_mpsc;

#[path = "../../interaction/foreground/src/test_provider.rs"]
#[allow(dead_code)] // This host uses its own accounted Model; foreground tests use `model`.
pub(crate) mod provider;
#[path = "../../interaction/background/src/scheduling.rs"]
mod scheduling;

pub(crate) struct Foreground {
    child: Child,
    server: tokio::task::JoinHandle<()>,
    reader: Option<std::thread::JoinHandle<()>>,
    coordinator: Option<std::thread::JoinHandle<()>>,
    pub registry: Arc<Mutex<Registry>>,
    pub events: Vec<InteractionEventEnvelope>,
    lines: async_mpsc::UnboundedReceiver<String>,
    pub workspace: PathBuf,
    pub socket: PathBuf,
    ui_events: mpsc::Receiver<AgentEvent>,
}

impl Foreground {
    pub async fn start(root: &Path, endpoint: &str, store: Arc<RuntimeStore>) -> Self {
        let binary = PathBuf::from(
            std::env::var_os("FOREGROUND_TEST_BIN")
                .expect("explicit freshly built FOREGROUND_TEST_BIN required"),
        )
        .canonicalize()
        .unwrap();
        assert!(endpoint.starts_with("http://127.0.0.1:"));
        let workspace = root.join("conversation");
        let config = root.join("config/tachyon");
        for path in [&workspace, &config, &root.join("state"), &root.join("home")] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut cfg = tachyon_util::config::Config::default();
        cfg.provider = Some(tachyon_util::config::Provider {
            name: "openrouter".into(),
            base_url: Some(endpoint.into()),
            routing: None,
        });
        cfg.model.name = Some("scripted-test-model".into());
        cfg.save(&config.join("config.toml")).unwrap();
        let socket = root.join("state/tachyond.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (coordinator_tx, requests) = mpsc::sync_channel(16);
        let coordinator = std::thread::spawn(move || {
            while let Ok(request) = requests.recv() {
                match request {
                    CoordinatorRequest::Schedule { request, response } => {
                        response
                            .send(scheduling::review_schedule_request(request))
                            .unwrap();
                    }
                    CoordinatorRequest::WorkReview(_) => {
                        panic!("simple turns must not create work")
                    }
                }
            }
        });
        let registry = Arc::new(Mutex::new(Registry {
            foreground_id: Some(FOREGROUND_ID.into()),
            runtime_store: Some(store),
            coordinator_tx: Some(coordinator_tx),
            ..Default::default()
        }));
        let server_registry = registry.clone();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let stream = stream.into_std().unwrap();
                stream.set_nonblocking(false).unwrap();
                let registry = server_registry.clone();
                tokio::task::spawn_blocking(move || handle_connection(stream, registry).unwrap());
            }
        });
        let mut child = Command::new(binary)
            .env_clear()
            .env("HOME", root.join("home"))
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("TACHYON_DATA_DIR", root)
            // Not a credential; resolve_key never consults the keyring when this is set.
            .env("OPENROUTER_API_KEY", "local-acceptance-not-a-credential")
            .env("TZ", "UTC")
            .args(["--agent-id", FOREGROUND_ID, "--new-session", "--cwd"])
            .arg(&workspace)
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut task = crate::tests::task(FOREGROUND_ID, AgentState::Running);
        task.stdin = Some(Arc::new(Mutex::new(child.stdin.take().unwrap())));
        task.ready = true;
        registry
            .lock()
            .unwrap()
            .tasks
            .insert(FOREGROUND_ID.into(), task);
        let ui_events = registry.lock().unwrap().subscribe(FOREGROUND_ID).unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, lines) = async_mpsc::unbounded_channel();
        let output_registry = registry.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if line.contains("[foreground:error]") {
                    eprintln!("{line}");
                }
                push_event(&output_registry, FOREGROUND_ID, EventStream::Stdout, &line);
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let mut process = Self {
            child,
            server,
            reader: Some(reader),
            coordinator: Some(coordinator),
            registry,
            events: vec![],
            lines,
            workspace,
            socket,
            ui_events,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while process.lines.recv().await.expect("foreground exited") != "[foreground] ready" {}
        })
        .await
        .expect("foreground startup");
        process
    }

    pub fn send(&self, id: &str, command: InteractionCommand) {
        let mut metadata = InteractionMetadata::new(id, id, FOREGROUND_ID, 1);
        metadata.causation_id = Some(id.into());
        let wire =
            serde_json::to_string(&InteractionCommandEnvelope { metadata, command }).unwrap();
        write_task_input(
            task_input(&self.registry, FOREGROUND_ID).unwrap(),
            FOREGROUND_ID,
            &wire,
        )
        .unwrap();
    }

    pub fn user(&self, id: &str, text: &str) {
        self.send(id, InteractionCommand::AcceptUserTurn { text: text.into() });
    }

    /// Explicit completed-delegation replay fixture. Uses production idempotent
    /// dispatch and WorkSubscribe, without spawning an installed legacy worker.
    pub fn completed_delegation(&self, call: &str, objective: &str, evidence: &str) {
        let work_id = format!("{FOREGROUND_ID}:1:{call}");
        let mut info = crate::tests::task("replayed-delegate", AgentState::Completed).info;
        info.logical_task_id = Some(work_id.clone());
        info.origin_turn_id = Some(format!("conversation:{FOREGROUND_ID}:1"));
        info.tool_call_id = Some(call.into());
        let request = WorkRequest {
            context_refs: vec![],
            constraints: None,
            attempt: None,
            work_id: work_id.clone(),
            objective: objective.into(),
            generation: 0,
            assignment: 0,
            deadline_ms: unix_now_ms() + 60_000,
            lifetime_class: LifetimeClass::Short,
        };
        let result = tachyon_api::WorkResult {
            final_context: None,
            attempt_id: None,
            candidate_refs: None,
            instruction_revision: None,
            work_id: work_id.clone(),
            objective: objective.into(),
            generation: 0,
            assignment: 0,
            evidence: Default::default(),
            timing: None,
            outcome: WorkOutcome::Completed {
                result: evidence.into(),
                artifacts: vec![],
                context: String::new(),
                suggested_reuse: false,
            },
        };
        let terminal_result = correlate_event(
            &serde_json::to_string(&result_envelope(&info.id, result)).unwrap(),
            &info,
        );
        let envelope: tachyon_api::EventEnvelope = serde_json::from_str(&terminal_result).unwrap();
        assert_eq!(envelope.conversation_id.as_deref(), Some(FOREGROUND_ID));
        assert_eq!(envelope.turn_id.as_deref(), Some("1"));
        assert_eq!(envelope.task_id.as_deref(), Some(work_id.as_str()));
        assert_eq!(envelope.tool_call_id.as_deref(), Some(call));
        self.registry.lock().unwrap().works.insert(
            work_id,
            WorkRecord {
                observed_calls: Default::default(),
                partial_evidence: Default::default(),
                request,
                fingerprint: work_fingerprint(
                    objective,
                    &None,
                    &[],
                    LifetimeClass::Short,
                    "",
                    &Some(format!("conversation:{FOREGROUND_ID}:1")),
                    &None,
                    &Some(call.into()),
                    &None,
                ),
                worker_id: info.id.clone(),
                info,
                review: None,
                subs: vec![],
                terminal_result: Some(terminal_result),
            },
        );
    }

    pub async fn until(
        &mut self,
        predicate: impl Fn(&InteractionEventEnvelope) -> bool,
    ) -> InteractionEventEnvelope {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let line = self
                    .lines
                    .recv()
                    .await
                    .expect("foreground exited before publication");
                if let Ok(event) = serde_json::from_str::<InteractionEventEnvelope>(&line) {
                    self.events.push(event.clone());
                    if predicate(&event) {
                        return event;
                    }
                }
            }
        })
        .await
        .expect("foreground publication barrier")
    }

    pub async fn finished(&mut self, turn: u64, text: &str) {
        let event = self
            .until(|e| {
                e.metadata.turn_id.as_deref() == Some(&turn.to_string())
                    && matches!(e.event, InteractionEvent::ConversationFinished { .. })
            })
            .await;
        assert_eq!(
            event.event,
            InteractionEvent::ConversationFinished { text: text.into() }
        );
    }

    pub async fn checkpoint(&self, next: u64) -> Value {
        self.checkpoint_where(next, |_| true).await
    }

    pub async fn checkpoint_where(&self, next: u64, ready: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(bytes) =
                    tokio::fs::read(self.workspace.join(".tachyon/conversation.json")).await
                {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                        if value["next_commit"] == next && ready(&value) {
                            return value;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("atomic checkpoint persistence")
    }

    pub fn capture(&self, scenario: &str, campaign: Option<&str>) {
        let mut events = Vec::new();
        let mut acknowledgement = None;
        let mut identities = std::collections::BTreeMap::<String, Value>::new();
        for event in self.ui_events.try_iter() {
            if let Ok(event) = serde_json::from_str::<tachyon_api::EventEnvelope>(&event.data) {
                if matches!(&event.kind, tachyon_api::AgentEvent::Status { turn: Some(1), message, .. } if message == "I'm looking into it.")
                {
                    assert!(
                        acknowledgement.replace(event.kind).is_none(),
                        "duplicate first-turn acknowledgement"
                    );
                }
            }
            let Ok(mut envelope) = serde_json::from_str::<InteractionEventEnvelope>(&event.data)
            else {
                continue;
            };
            // Launch assessment may correctly be stale if root admission wins the race.
            if matches!(&envelope.event, InteractionEvent::UserVisibleNotificationPublished { text } if text.contains("Investigation running; no conclusion yet"))
            {
                continue;
            }
            let identity = envelope.metadata.message_id.clone();
            if let Some(previous) = identities.get(&identity) {
                events.push(previous.clone());
                continue;
            }
            let index = identities.len() + 1;
            envelope.metadata.message_id = format!("event-{index}");
            envelope.metadata.correlation_id = envelope
                .metadata
                .turn_id
                .as_ref()
                .map_or_else(|| format!("notice-{index}"), |turn| format!("turn-{turn}"));
            envelope.metadata.causation_id = Some(envelope.metadata.correlation_id.clone());
            envelope.metadata.occurred_at_ms = index as u64;
            if let Some(frame) = &mut envelope.metadata.attention {
                frame.ids = vec!["attention-fixture".into()];
            }
            if let InteractionEvent::UserVisibleNotificationPublished { text } = &mut envelope.event
            {
                if text.contains("A candidate is ready") {
                    let (advisory, _) = text.split_once("\nSources:").unwrap();
                    *text =
                        format!("{advisory}\nSources: finding:benchmark-candidate:fixture-hash");
                }
            }
            let mut wire = serde_json::to_string(&envelope).unwrap();
            if let Some(campaign) = campaign {
                wire = wire.replace(campaign, "campaign-fixture");
            }
            let normalized = serde_json::from_str::<Value>(&wire).unwrap();
            identities.insert(identity, normalized.clone());
            events.push(normalized);
        }
        let value = json!({"scenario":scenario, "events":events, "acknowledgement":acknowledgement.expect("real foreground acknowledgement")});
        if scenario == "campaign-attention" {
            let expected: Value = serde_json::from_str(include_str!(
                "../../tachyon-tui/tests/fixtures/parallel_acceptance.json"
            ))
            .unwrap();
            assert_eq!(
                value, expected,
                "production publication trace changed; review the TUI fixture"
            );
        }
        if let Some(root) = std::env::var_os("PARALLEL_ACCEPTANCE_CAPTURE_DIR") {
            let root = PathBuf::from(root);
            assert!(
                root.is_absolute() && root.is_dir(),
                "explicit existing capture directory required"
            );
            std::fs::write(
                root.join(format!("{scenario}.json")),
                serde_json::to_vec_pretty(&value).unwrap(),
            )
            .unwrap();
        }
    }
}

impl Drop for Foreground {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.server.abort();
        if let Some(reader) = self.reader.take() {
            reader.join().unwrap();
        }
        self.registry.lock().unwrap().coordinator_tx = None;
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.join().unwrap();
        }
    }
}

pub(crate) async fn request(provider: &mut provider::LocalProvider) -> provider::Request {
    tokio::time::timeout(Duration::from_secs(10), provider.requests.recv())
        .await
        .expect("HTTP request barrier")
        .expect("HTTP server exited")
}

pub(crate) async fn investigation(provider: &mut provider::LocalProvider) -> provider::Request {
    request(provider)
        .await
        .respond_tool_text("todo", json!({"operation":"list"}), "I'm looking into it.")
        .await;
    let next = request(provider).await;
    let output: Value = serde_json::from_str(
        next.body["messages"].as_array().unwrap().last().unwrap()["content"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(output["todos"], json!([]));
    next
}

pub(crate) async fn independent(
    process: &Foreground,
    provider: &mut provider::LocalProvider,
    id: &str,
    text: &str,
) -> provider::Request {
    process.user(id, text);
    let routing = request(provider).await;
    assert!(routing.body["messages"][1]["content"]
        .as_str()
        .unwrap()
        .ends_with(text));
    assert!(routing.body.get("tools").is_none());
    routing
        .respond(&[r#"{"decision":"AnswerNow","acknowledgement":"I'll check that."}"#])
        .await;
    let next = request(provider).await;
    assert_eq!(
        next.body["messages"].as_array().unwrap().last().unwrap()["content"],
        text
    );
    next
}

pub(crate) async fn simple_paths(
    process: &mut Foreground,
    provider: &mut provider::LocalProvider,
    store: &RuntimeStore,
) {
    let campaigns = || {
        serde_json::to_value(
            store
                .research_request(&ApiRequest::CampaignList {
                    research_id: None,
                    after: None,
                    limit: 100,
                })
                .unwrap(),
        )
        .unwrap()
    };
    let before = campaigns();
    independent(process, provider, "state", "What is WorkerState?")
        .await
        .respond(&["WorkerState describes the worker lifecycle."])
        .await;
    process
        .finished(2, "WorkerState describes the worker lifecycle.")
        .await;
    assert_eq!(
        process.checkpoint(1).await["messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    independent(process, provider, "reminder", "Remind me at 3 PM to review the benchmark.").await
        .respond_tool("schedule", json!({"action":"create", "text":"Review the benchmark", "local_time":"15:00", "day":"tomorrow"})).await;
    let confirmation = request(provider).await;
    let reminders = store.active_reminders().unwrap();
    assert_eq!(reminders.len(), 1);
    assert_eq!(reminders[0].text, "Review the benchmark");
    let due = Local
        .timestamp_millis_opt(reminders[0].due_at_ms as i64)
        .unwrap();
    assert_eq!(chrono::Timelike::hour(&due), 15);
    assert_eq!(chrono::Timelike::minute(&due), 0);
    assert!(!process.events.iter().any(|e| matches!(&e.event, InteractionEvent::ConversationFinished { text } if text == "Scheduled.")));
    confirmation.respond(&["Scheduled."]).await;
    process.finished(3, "Scheduled.").await;

    let mut todo_id = String::new();
    for (phase, status) in ["pending", "in_progress", "blocked", "completed"]
        .into_iter()
        .enumerate()
    {
        let next = independent(
            process,
            provider,
            &format!("plan-{phase}"),
            "Update my review plan.",
        )
        .await;
        let args = if phase == 0 {
            json!({"operation":"add", "title":"Review benchmark", "command_id":"plan-add", "expected_revision":0})
        } else {
            json!({"operation":"update", "id":todo_id, "command_id":format!("plan-{phase}"), "expected_revision":phase, "status":status})
        };
        next.respond_tool("todo", args).await;
        let confirmation = request(provider).await;
        let output: Value = serde_json::from_str(
            confirmation.body["messages"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["content"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(output["todo"]["status"], status, "{output}");
        todo_id = output["todo"]["id"].as_str().unwrap().into();
        assert!(output["todo"]["updated_by"]["actor"]
            .as_str()
            .unwrap()
            .starts_with("uid:"));
        confirmation.respond(&[status]).await;
        process.finished(4 + phase as u64, status).await;
    }
    assert!(process.registry.lock().unwrap().works.is_empty());
    assert!(!process
        .events
        .iter()
        .any(|e| matches!(e.event, InteractionEvent::ConversationIntentProduced { .. })));
    assert_eq!(
        process.checkpoint(1).await["messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        campaigns(),
        before,
        "simple paths must not create or mutate campaigns"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires freshly built FOREGROUND_TEST_BIN; loopback HTTP and temporary daemon only"]
async fn parallel_acceptance_foreground_services() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let mut provider = provider::LocalProvider::start().await;
    let mut process = Foreground::start(dir.path(), &provider.endpoint, store.clone()).await;
    process.user(
        "investigate",
        "Investigate the benchmark; the host already authorized the campaign.",
    );
    let held = investigation(&mut provider).await;
    simple_paths(&mut process, &mut provider, &store).await;
    assert!(process.registry.lock().unwrap().campaigns.is_none());
    held.respond(&["Benchmark investigation complete."]).await;
    process
        .finished(1, "Benchmark investigation complete.")
        .await;
    let checkpoint = process.checkpoint(8).await;
    let messages: Vec<tachyon_model::ChatMessage> =
        serde_json::from_value(checkpoint["messages"].clone()).unwrap();
    let users: Vec<_> = messages
        .iter()
        .filter(|m| m.role == tachyon_model::Role::User)
        .map(tachyon_model::ChatMessage::plain)
        .collect();
    assert_eq!(users.len(), 7);
    assert!(users[0].starts_with("Investigate the benchmark"));
    assert_eq!(users[1], "What is WorkerState?");
    assert!(users[2].starts_with("Remind me at 3 PM"));
    assert!(provider.requests.try_recv().is_err());
    process.capture("foreground-services", None);
    drop(process);
    provider.shutdown().await;
}
