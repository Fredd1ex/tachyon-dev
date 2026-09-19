//! Host-owned retrieval, independent of campaigns, Ghost and foreground inference.
use crate::runtime_store::RuntimeStore;
use std::sync::Arc;
use tachyon_api::web::{WebCommand, WebLimits, WebRequest, WebResult, WebStatus};
use tachyon_model::{accounting::RequestUsage, Model, ModelConfig};
use tachyon_util::config::{Config, WebPolicy};
use tokio::time::{Duration, Instant};
#[cfg(test)]
mod tests;

pub(crate) struct WebService {
    pub(crate) runtime: tokio::runtime::Runtime,
    provider: Arc<WebProvider>,
}

pub(crate) struct WebProvider {
    store: Arc<RuntimeStore>,
    model: Arc<Model>,
    worker_model: Arc<Model>,
    model_id: String,
    policy: WebPolicy,
    capacity: tokio::sync::Semaphore,
    worker_capacity: tokio::sync::Semaphore,
}

impl std::ops::Deref for WebService {
    type Target = WebProvider;
    fn deref(&self) -> &WebProvider {
        &self.provider
    }
}

struct WorkerBinding {
    registry: std::sync::Weak<std::sync::Mutex<crate::Registry>>,
    worker: String,
    actor_pid: u32,
    request: tachyon_api::WorkRequest,
}

impl WorkerBinding {
    fn live(&self) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let Ok(reg) = registry.lock() else {
            return false;
        };
        self.request.deadline_ms > crate::unix_now_ms()
            && !reg
                .service_shutdown
                .load(std::sync::atomic::Ordering::Acquire)
            && reg.tasks.get(&self.worker).is_some_and(|t| {
                t.generation == self.request.generation
                    && t.assignment == self.request.assignment
                    && t.info.pid == Some(self.actor_pid)
                    && matches!(
                        t.info.state,
                        tachyon_api::AgentState::Starting
                            | tachyon_api::AgentState::Running
                            | tachyon_api::AgentState::Completed
                    )
                    && t.terminal_result.is_none()
            })
            && reg.works.get(&self.request.work_id).is_none_or(|w| {
                w.worker_id == self.worker
                    && w.request == self.request
                    && w.terminal_result.is_none()
                    && w.review.is_none()
            })
    }
}

impl WebService {
    pub(crate) fn configured(store: Arc<RuntimeStore>) -> Result<Self, String> {
        let cfg = Config::try_load_from(&Config::default_path())
            .map_err(|_| "web configuration unavailable")?;
        cfg.web.validate()?;
        if !cfg.web.enabled {
            return Err("host web service disabled".into());
        }
        let key = cfg
            .resolve_key("openrouter")
            .ok_or("web requires the existing OpenRouter key")?;
        let settings = cfg.conversation_config().model(&cfg.model);
        let name = settings
            .name
            .or_else(|| cfg.web.model.clone())
            .ok_or("configure a Conversation model or web.model")?;
        if name.trim().is_empty()
            || name.len() > 256
            || name.chars().any(char::is_control)
            || name.contains(":online")
        {
            return Err("invalid or legacy online web model configuration".into());
        }
        // Do not forward the credential to an arbitrary public IPC-selected host.
        let base_url = cfg.provider_base_url().trim_end_matches('/').to_owned();
        if base_url != "https://openrouter.ai/api/v1" {
            return Err("web requires the OpenRouter endpoint".into());
        }
        let model = Model::new(ModelConfig {
            base_url: base_url.clone(),
            api_key: key.clone(),
            model: name.clone(),
            temperature: settings.temperature.unwrap_or(0.2),
            max_completion_tokens: Some(cfg.web.output_tokens),
            context_length: settings.context_length,
            parallel_tool_calls: false,
            reasoning: Default::default(),
            routing: cfg.provider_routing(),
            debug: false,
            debug_log: None,
        });
        let identity = serde_json::to_string(&(name, base_url, cfg.provider_routing()))
            .map_err(|_| "invalid web model configuration")?;
        let worker = cfg.worker_config().model(&cfg.model);
        let worker_model = Model::new(ModelConfig {
            base_url: cfg.provider_base_url().trim_end_matches('/').into(),
            api_key: key,
            model: worker
                .name
                .unwrap_or_else(|| Config::default_model().into()),
            temperature: worker.temperature.unwrap_or(0.2),
            max_completion_tokens: worker.max_completion_tokens,
            context_length: worker.context_length,
            parallel_tool_calls: worker.parallel_tool_calls,
            reasoning: worker.reasoning,
            routing: cfg.provider_routing(),
            debug: false,
            debug_log: None,
        });
        let mut service = Self::new(store, model, identity, cfg.web)?;
        Arc::get_mut(&mut service.provider).unwrap().worker_model = Arc::new(worker_model);
        Ok(service)
    }

    pub(crate) fn new(
        store: Arc<RuntimeStore>,
        model: Model,
        model_id: String,
        policy: WebPolicy,
    ) -> Result<Self, String> {
        policy.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| "web runtime unavailable")?;
        let model = Arc::new(model);
        Ok(Self {
            runtime,
            provider: Arc::new(WebProvider {
                store,
                worker_model: model.clone(),
                model,
                model_id,
                capacity: tokio::sync::Semaphore::new(policy.concurrency),
                worker_capacity: tokio::sync::Semaphore::new(policy.concurrency),
                policy,
            }),
        })
    }
}

impl WebProvider {
    pub(crate) async fn lookup(
        &self,
        session: &str,
        conversation: &str,
        command: WebCommand,
    ) -> Result<WebResult, String> {
        if session.is_empty() {
            return Err("web session identity missing".into());
        }
        self.lookup_bound(conversation, command, Some(session), None)
            .await
    }

    async fn lookup_bound(
        &self,
        conversation: &str,
        command: WebCommand,
        session: Option<&str>,
        binding: Option<&WorkerBinding>,
    ) -> Result<WebResult, String> {
        command.validate()?;
        if binding.is_some_and(|b| !b.live()) {
            return Err("worker web authority revoked".into());
        }
        if command.caller_id != conversation {
            return Err("web source identity mismatch".into());
        }
        if !self.policy.enabled {
            return Err("host web service disabled".into());
        }
        let deadline = Instant::now() + Duration::from_secs(self.policy.timeout_secs);
        tokio::time::timeout_at(deadline, async {
            // No unbounded wait queue; other conversations and inference lanes stay independent.
            let _capacity = self
                .capacity
                .try_acquire()
                .map_err(|_| "web service busy")?;
            let calls = match &command.request {
                WebRequest::Search { .. } => 1,
                WebRequest::Fetch { urls, .. } => {
                    u8::try_from(urls.len()).map_err(|_| "too many fetch URLs")?
                }
            };
            if calls > self.policy.max_fetch_urls {
                return Err("fetch URL allowance exceeded".into());
            }
            // Foreground turn numbers reset between host sessions. Keep the session
            // out of model input and retain the existing standalone/worker namespace.
            let scope = match session {
                Some(session) => serde_json::to_string(&(
                    "conversation",
                    conversation,
                    session,
                    &command.turn_id,
                )),
                None => serde_json::to_string(&("conversation", conversation, &command.turn_id)),
            }
            .map_err(|_| "invalid web scope")?;
            let (store, policy, model, cmd, key) = (
                self.store.clone(),
                self.policy.clone(),
                self.model_id.clone(),
                command.clone(),
                scope.clone(),
            );
            let replay = tokio::task::spawn_blocking(move || {
                store.web_reserve(&key, &model, &policy, &cmd, calls)
            })
            .await
            .map_err(|_| "web storage unavailable")??;
            if binding.is_some_and(|b| !b.live()) {
                return Err("worker web authority revoked after reservation".into());
            }
            if let Some(result) = replay {
                return result;
            }
            let limits = WebLimits {
                max_tool_calls: calls,
                max_output_tokens: self.policy.output_tokens,
                ..Default::default()
            };
            let (usage, result) = match tokio::time::timeout_at(
                deadline,
                self.model.web_lookup(&command.request, &limits, deadline),
            )
            .await
            {
                Ok(Ok(mut completion)) => {
                    if completion.usage == RequestUsage::Unknown {
                        if completion.result.status == WebStatus::Grounded {
                            completion.result.status = WebStatus::Unverified;
                        }
                        completion.result.notice.push_str(
                            " Billing is unknown; this root turn cannot make further web requests.",
                        );
                    }
                    (completion.usage, Ok(completion.result))
                }
                Ok(Err(error)) => (
                    RequestUsage::Unknown,
                    Err(format!(
                        "web retrieval failed: {}; spend may be unknown",
                        self.model.web_error_summary(&error)
                    )),
                ),
                Err(_) => (
                    RequestUsage::Unknown,
                    Err("web retrieval deadline elapsed; spend may be unknown".into()),
                ),
            };
            let (store, saved) = (self.store.clone(), result.clone());
            tokio::task::spawn_blocking(move || store.web_finish(&scope, &command, usage, saved))
                .await
                .map_err(|_| "web storage unavailable")??
        })
        .await
        .map_err(|_| "web deadline elapsed; spend may be unknown".to_string())?
    }

    /// Host binds a standalone service to one root turn before handing out the
    /// authenticated private channel. No campaign or oversight grant is created.
    #[allow(dead_code)] // Host integration point for standalone service owners.
    pub(crate) async fn serve_private(
        &self,
        channel: tachyon_model::broker::HostChannel,
        source: &str,
        root_turn: &str,
        deadline: Instant,
    ) -> tachyon_model::Result<()> {
        self.serve_channel(channel, source, root_turn, deadline, None)
            .await
    }

    async fn serve_channel(
        &self,
        channel: tachyon_model::broker::HostChannel,
        source: &str,
        root_turn: &str,
        deadline: Instant,
        binding: Option<&WorkerBinding>,
    ) -> tachyon_model::Result<()> {
        use tachyon_api::agents::{Reply, Request};
        use tachyon_model::broker::{
            protocol_error, read_frame, write_frame, FrameReply, FrameRequest,
        };
        use tokio::io::AsyncReadExt;
        tokio::time::timeout_at(deadline, async {
            let mut stream = channel.authenticate().await?;
            let mut inference_ids = std::collections::BTreeSet::new();
            for _ in 0..tachyon_model::broker::MAX_REQUESTS {
                let frame = read_frame(&mut stream).await?;
                if binding.is_some_and(|b| !b.live()) { return Err(protocol_error()); }
                if let FrameRequest::Model(request) = frame {
                    request.validate()?;
                    if binding.is_none() || !inference_ids.insert(request.id.clone()) { return Err(protocol_error()); }
                    let capacity = self.worker_capacity.try_acquire().map_err(|_| protocol_error())?;
                    let mut unexpected = [0;1];
                    let mut completion = tokio::select! {
                        biased;
                        _ = stream.read(&mut unexpected) => return Err(protocol_error()),
                        result = self.worker_model.chat_tools_bounded(&request.messages, &request.tools, tachyon_model::broker::MAX_FRAME / 2) => result.ok(),
                    };
                    if let Some(completion) = &mut completion {
                        completion.text = self.worker_model.redact_trace(&completion.text);
                        completion.finish_reason = completion.finish_reason.take()
                            .map(|reason| self.worker_model.redact_trace(&reason));
                        for call in &mut completion.tool_calls {
                            call.id = self.worker_model.redact_trace(&call.id);
                            call.name = self.worker_model.redact_trace(&call.name);
                            call.arguments = self.worker_model.redact_trace(&call.arguments);
                        }
                    }
                    drop(capacity);
                    write_frame(&mut stream, &FrameReply::Model(tachyon_model::broker::Reply { id: request.id, completion })).await?;
                    continue;
                }
                let FrameRequest::Control(request) = frame else {
                    return Err(protocol_error());
                };
                let valid = request.validate().is_ok();
                let search = matches!(request, Request::WebSearch { .. });
                let command = match request {
                    Request::WebSearch { command } | Request::WebFetch { command } => command,
                    _ => {
                        write_frame(&mut stream, &FrameReply::Control(Reply::Denied)).await?;
                        continue;
                    }
                };
                let reply = if !valid || command.caller_id != source || command.turn_id != root_turn
                {
                    Reply::Denied
                } else {
                    let mut unexpected = [0; 1];
                    let result = tokio::select! {
                        biased;
                        _ = stream.read(&mut unexpected) => return Err(protocol_error()),
                        result = self.lookup_bound(source, command.clone(), None, binding) => result,
                    };
                    match result {
                        Ok(result) if search => Reply::WebSearch { command, result },
                        Ok(result) => Reply::WebFetch { command, result },
                        Err(error) => Reply::WebError {
                            command,
                            message: self.model.web_error_summary(&tachyon_model::ModelError::Api(error)),
                        },
                    }
                };
                write_frame(&mut stream, &FrameReply::Control(reply)).await?;
            }
            Err(protocol_error())
        })
        .await
        .map_err(|_| protocol_error())?
    }
}

impl WebService {
    /// The listener is one-use, mode 0700, capability-authenticated and PID/UID
    /// checked. Only the delivery envelope contains its secret, never the ledger.
    pub(crate) fn assignment(
        self: &Arc<Self>,
        registry: std::sync::Weak<std::sync::Mutex<crate::Registry>>,
        worker: String,
        pid: u32,
        request: tachyon_api::WorkRequest,
    ) -> Result<tachyon_api::web::ServiceBootstrap, String> {
        use sha2::{Digest, Sha256};
        if !self.policy.enabled
            || request.work_id.trim().is_empty()
            || request.work_id.len() > 256
            || request.work_id.chars().any(char::is_control)
        {
            return Err("worker web service disabled or invalid scope".into());
        }
        let _entered = self.runtime.enter();
        let listener = tachyon_model::broker::PrivateListener::bind()
            .map_err(|_| "worker service listener unavailable")?;
        let bootstrap = listener.service_bootstrap(vec![
            tachyon_api::agents::Control::WebSearch,
            tachyon_api::agents::Control::WebFetch,
        ]);
        let root = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &request.work_id,
                    Option::<String>::None,
                    request.attempt.as_ref().map(|a| &a.id),
                    Some(request.generation),
                    Some(request.assignment)
                ))
                .map_err(|_| "invalid worker scope")?
            )
        );
        let service = self.provider.clone();
        let deadline = Instant::now()
            + Duration::from_millis(request.deadline_ms.saturating_sub(crate::unix_now_ms()));
        // Supervised workers authenticate as the child, while the registry tracks
        // the supervisor. Replacing either assignment's actor revokes this channel.
        let actor_pid = {
            let registry = registry.upgrade().ok_or("worker registry unavailable")?;
            let reg = registry.lock().map_err(|_| "worker registry unavailable")?;
            let task = reg.tasks.get(&worker).ok_or("worker unavailable")?;
            let actor_pid = task.info.pid.ok_or("worker PID unavailable")?;
            if task.control_socket.is_none() && actor_pid != pid {
                return Err("worker PID mismatch".into());
            }
            actor_pid
        };
        let binding = WorkerBinding {
            registry,
            worker,
            actor_pid,
            request,
        };
        if !binding.live() {
            return Err("worker service authority unavailable".into());
        }
        self.runtime.spawn(async move {
            let serve = async {
                let channel = tokio::time::timeout_at(
                    deadline.min(Instant::now() + Duration::from_secs(5)),
                    listener.accept(pid, nix::unistd::geteuid().as_raw()),
                )
                .await
                .map_err(|_| tachyon_model::broker::protocol_error())??;
                service
                    .serve_channel(
                        channel,
                        &binding.request.work_id,
                        &root,
                        deadline,
                        Some(&binding),
                    )
                    .await
            };
            tokio::select! {
                _ = serve => {},
                _ = async { loop {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    if !binding.live() { break; }
                }} => {},
            }
        });
        Ok(bootstrap)
    }
}
