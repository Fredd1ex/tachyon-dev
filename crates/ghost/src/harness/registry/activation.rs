//! Per-work instruction loading, never a permission grant or resource startup.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use super::{manifest::Manifest, RegistryError, ToolRegistry};
use crate::harness::runtime::{
    Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolPolicy, ToolResult,
};

/// Versioned instruction selection only, not authority. Restore revalidates it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationSnapshot {
    pub packages: BTreeMap<String, String>,
}

pub struct WorkTools {
    outputs: Arc<crate::harness::runtime::output_store::WorkOutputStore>,
    scope: uuid::Uuid,
    installed: ToolRegistry,
    loaded: Mutex<ActivationSnapshot>,
    ended: tokio_util::sync::CancellationToken,
    cleanup: Mutex<Option<tokio_util::sync::CancellationToken>>,
    calls: Arc<tokio::sync::RwLock<()>>,
}

impl Drop for WorkTools {
    fn drop(&mut self) {
        self.end();
    }
}

impl ToolRegistry {
    pub fn context_metadata(&self) -> tachyon_api::context::WorkerContextMetadata {
        tachyon_api::context::WorkerContextMetadata {
            activated_packages: self.activation_snapshot().packages,
            known_output_handles: self
                .work_outputs()
                .map(|outputs| outputs.list().into_iter().map(|r| r.id).collect())
                .unwrap_or_default(),
        }
    }

    pub async fn finish_work_retaining(
        &self,
        model: &impl crate::harness::agent::AgentModel,
        context: &ToolContext,
        successful: bool,
    ) -> Result<(), String> {
        let Some(work) = &self.activation else {
            return Ok(());
        };
        // Keep anonymous files alive across process teardown, then export stable pages.
        let outputs = work.outputs.clone();
        let retained = outputs.snapshot();
        work.end().cancelled().await;
        if !successful || context.cancellation.is_cancelled() {
            return Err("output retention skipped after error/cancellation; spool gap".into());
        }
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err("output retention cancelled; spool gap".into()),
            result = tokio::time::timeout_at(context.deadline.into(), model.retain_outputs(&retained)) =>
                result.map_err(|_| "output retention deadline; spool gap".to_string())?,
        }
    }
    pub(crate) fn work_outputs(
        &self,
    ) -> Result<Arc<crate::harness::runtime::output_store::WorkOutputStore>, ToolError> {
        self.activation
            .as_ref()
            .map(|work| work.outputs.clone())
            .ok_or_else(|| ToolError::invalid("output references require a per-work registry"))
    }
    pub(super) async fn work_call(&self) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        match &self.activation {
            Some(work) => Some(work.calls.clone().read_owned().await),
            None => None,
        }
    }
    /// End an objective before publishing its result. Dropping the last work handle
    /// is a fallback; explicit finish waits even if another handle is retained.
    pub async fn finish_work(&self) {
        if let Some(work) = &self.activation {
            work.end().cancelled().await;
        }
    }

    pub(super) async fn work_ended(&self) {
        match &self.activation {
            Some(work) => work.ended.cancelled().await,
            None => std::future::pending().await,
        }
    }
    pub(crate) fn work_scope(&self) -> Option<uuid::Uuid> {
        self.activation.as_ref().map(|work| work.scope)
    }
    /// Narrow Python bridge: activation and schemas use this work's current authority.
    pub(crate) fn python_require(
        &self,
        name: &str,
        policy: &ToolPolicy,
    ) -> Result<Value, ToolError> {
        let work = self
            .activation
            .as_ref()
            .ok_or_else(|| ToolError::invalid("require needs a per-work registry"))?;
        let manifest = work.package(name, policy)?;
        let operations = crate::harness::tools::python::bridge::PACKAGES
            .iter()
            .find(|(package, _)| *package == name)
            .map(|(_, operations)| *operations);
        if operations.is_none()
            || !policy.enabled_tools.contains("tools")
            || manifest.operations.iter().any(|name| {
                !operations.unwrap_or_default().contains(name) || !self.tools.contains_key(name)
            })
        {
            return Err(ToolError::new(
                ToolErrorCode::PermissionDenied,
                "Python bridge requires an authorized native package; recursive ipython calls are disabled",
                false,
            ));
        }
        let changed = work.activate(name, policy)?;
        let schemas = self
            .definitions(policy)
            .into_iter()
            .filter(|s| manifest.operations.contains(&s.name.as_str()))
            .map(|s| json!({"name": s.name, "parameters": s.parameters}))
            .collect::<Vec<_>>();
        let mut methods = serde_json::Map::new();
        for schema in &schemas {
            let tool = schema["name"].as_str().expect("native tool name");
            if name == "workspace" {
                methods.insert(
                    tool.into(),
                    json!({"tool": tool, "input": {}, "asynchronous": false}),
                );
                if tool == "grep" {
                    methods.insert("search".into(), methods[tool].clone());
                }
            } else if let Some(actions) =
                schema["parameters"]["properties"]["action"]["enum"].as_array()
            {
                for action in actions {
                    let action = action.as_str().expect("native action name");
                    methods.insert(
                        action.into(),
                        json!({"tool": tool, "input": {"action": action}, "asynchronous": true}),
                    );
                }
            } else {
                let method = match tool {
                    "agent_browser" => "run",
                    "artifact" => "register",
                    _ => tool,
                };
                methods.insert(
                    method.into(),
                    json!({"tool": tool, "input": {}, "asynchronous": true}),
                );
            }
        }
        Ok(
            json!({"package": name, "changed": changed, "guidance": manifest.interface, "schemas": schemas, "methods": methods}),
        )
    }

    /// Create once per work. Clones share this work's state; separate calls isolate it.
    pub fn for_work(
        &self,
        policy: &ToolPolicy,
        eager: &[&str],
        snapshot: &ActivationSnapshot,
    ) -> Result<Self, RegistryError> {
        // Always assemble from an installed registry, not another work's registry.
        if self.tools.contains_key("tools") {
            return Err(RegistryError::Duplicate("tools".into()));
        }
        let work = Arc::new(WorkTools {
            outputs: Default::default(),
            scope: uuid::Uuid::new_v4(),
            installed: self.clone(),
            loaded: Mutex::new(ActivationSnapshot::default()),
            ended: Default::default(),
            cleanup: Mutex::new(None),
            calls: Default::default(),
        });
        for (name, version) in &snapshot.packages {
            if !work
                .installed
                .manifests
                .iter()
                .any(|m| m.name == name && m.version == version)
            {
                return Err(RegistryError::ActivationMismatch(name.clone()));
            }
            work.activate(name, policy)
                .map_err(|_| RegistryError::ActivationMismatch(name.clone()))?;
        }
        for name in eager {
            let _ = work.activate(name, policy);
        }
        // Instructions only. Durable plans and samples are retrieved by explicit tools.
        for name in ["todo", "monitor"] {
            let _ = work.activate(name, policy);
        }
        let mut registry = self.clone();
        registry.register(Discovery {
            work: work.clone(),
            schema: ToolSpec::new("tools", "Discover authorized packages, activate concise instructions, or request detailed help. Loading never grants permissions.", json!({
                "type": "object", "properties": {
                    "action": {"type": "string", "enum": ["list", "activate", "help"]},
                    "package": {"type": "string"}
                }, "required": ["action"], "additionalProperties": false
            })),
        })?;
        registry.activation = Some(work);
        Ok(registry)
    }

    pub fn activation_snapshot(&self) -> ActivationSnapshot {
        self.activation
            .as_ref()
            .map(|work| work.loaded.lock().unwrap().clone())
            .unwrap_or_default()
    }
}

impl WorkTools {
    fn end(&self) -> tokio_util::sync::CancellationToken {
        let mut cleanup = self.cleanup.lock().unwrap();
        if let Some(done) = &*cleanup {
            return done.clone();
        }
        self.ended.cancel();
        let done = tokio_util::sync::CancellationToken::new();
        *cleanup = Some(done.clone());
        let tools = self.installed.tools.values().cloned().collect::<Vec<_>>();
        let scope = self.scope;
        let calls = self.calls.clone();
        let outputs = self.outputs.clone();
        let completed = done.clone();
        // No work handle is captured: teardown cannot keep the objective alive.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                // Cancellation drops in-flight calls before resources are detached.
                let calls = calls.write().await;
                outputs.clear();
                let futures = tools
                    .iter()
                    .map(|tool| tool.end_work(scope))
                    .collect::<Vec<_>>();
                drop(calls);
                let mut tasks = tokio::task::JoinSet::new();
                for future in futures {
                    tasks.spawn(async move {
                        if tokio::time::timeout(std::time::Duration::from_secs(5), future)
                            .await
                            .is_err()
                        {
                            eprintln!("ghost: work cleanup timed out");
                        }
                    });
                }
                while let Some(result) = tasks.join_next().await {
                    if let Err(error) = result {
                        eprintln!("ghost: work cleanup failed: {error}");
                    }
                }
                completed.cancel();
            });
        } else {
            outputs.clear();
            for tool in tools {
                drop(tool.end_work(scope));
            }
            completed.cancel();
        }
        done
    }

    fn authorized(&self, manifest: &Manifest, policy: &ToolPolicy) -> bool {
        manifest.operations.iter().all(|name| {
            self.installed
                .tools
                .get(name)
                .is_some_and(|tool| policy.permits(tool.as_ref()))
        })
    }

    fn package(&self, name: &str, policy: &ToolPolicy) -> Result<&Manifest, ToolError> {
        let manifest = self
            .installed
            .manifests
            .iter()
            .find(|m| m.name == name)
            .ok_or_else(|| ToolError::invalid(format!("unknown package: {name}")))?;
        if !self.authorized(manifest, policy) {
            return Err(ToolError::new(
                ToolErrorCode::PermissionDenied,
                format!("package is disabled by policy: {name}"),
                false,
            ));
        }
        Ok(manifest)
    }

    fn activate(&self, name: &str, policy: &ToolPolicy) -> Result<bool, ToolError> {
        let manifest = self.package(name, policy)?;
        let mut loaded = self.loaded.lock().unwrap();
        Ok(loaded
            .packages
            .insert(name.into(), manifest.version.into())
            .is_none())
    }

    fn catalog(&self, policy: &ToolPolicy) -> String {
        let loaded = self.loaded.lock().unwrap();
        self.installed
            .manifests
            .iter()
            .filter(|m| self.authorized(m, policy))
            .map(|m| {
                format!(
                    "{} [{}]: {}",
                    m.name,
                    if loaded.packages.contains_key(m.name) {
                        "loaded"
                    } else {
                        "available"
                    },
                    m.description
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(super) fn guidance(&self, policy: &ToolPolicy) -> String {
        let mut text = String::new();
        if policy.enabled_tools.contains("tools") {
            text.push_str("Tool packages (authorized installed packages only; loaded means instructions selected, not permission granted). Use `tools` action=list, action=activate package=NAME, or action=help package=NAME. Detailed help is on request. Direct authorized schemas remain exposed and callable without activation.\n");
            text.push_str(&self.catalog(policy));
        }
        let loaded = self.loaded.lock().unwrap();
        for manifest in &self.installed.manifests {
            if loaded.packages.contains_key(manifest.name) && self.authorized(manifest, policy) {
                text.push_str(&format!(
                    "\n\nLoaded {}@{}:\n{}",
                    manifest.name, manifest.version, manifest.interface
                ));
            }
        }
        text.trim().into()
    }
}

struct Discovery {
    work: Arc<WorkTools>,
    schema: ToolSpec,
}

impl Tool for Discovery {
    fn name(&self) -> &'static str {
        "tools"
    }
    fn schema(&self) -> &ToolSpec {
        &self.schema
    }
    fn capabilities(&self) -> &'static [Capability] {
        &[]
    }
    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Request {
                action: String,
                package: Option<String>,
            }
            let request: Request =
                serde_json::from_value(input).map_err(|e| ToolError::invalid(e.to_string()))?;
            if request.action == "list" && request.package.is_none() {
                return Ok(ToolResult::success(
                    self.work.catalog(&context.policy),
                    json!({}),
                ));
            }
            let name = request
                .package
                .as_deref()
                .ok_or_else(|| ToolError::invalid("package is required"))?;
            match request.action.as_str() {
                "activate" => {
                    let changed = self.work.activate(name, &context.policy)?;
                    Ok(ToolResult::success(if changed { "Package activated; concise interface is available at the next model boundary." } else { "Package already loaded." }.into(), json!({"package": name, "changed": changed})))
                }
                "help" => {
                    let manifest = self.work.package(name, &context.policy)?;
                    Ok(ToolResult::success(
                        manifest.usage.into(),
                        json!({"package": name, "version": manifest.version}),
                    ))
                }
                _ => Err(ToolError::invalid("expected list, activate, or help")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{
        backend::Local,
        profiles,
        runtime::{BrowserAvailability, NoopEventSink, NoopOutputStore, ToolIdentity},
        tools,
    };
    use std::time::{Duration, Instant};

    fn fixture() -> (ToolRegistry, ToolContext) {
        let root = std::env::current_dir().unwrap();
        let registry =
            profiles::worker(Arc::new(Local::new(&root)), BrowserAvailability::Available)
                .into_registry();
        let context = ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: Default::default(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        (registry, context)
    }

    #[tokio::test]
    async fn discovery_is_authorized_idempotent_and_resource_free() {
        let (installed, mut context) = fixture();
        let work = installed
            .for_work(&context.policy, profiles::WORKER_EAGER, &Default::default())
            .unwrap();
        let catalog = work
            .execute("tools", &context, json!({"action":"list"}))
            .await
            .unwrap()
            .content;
        assert!(
            catalog.contains("workspace [loaded]: Workspace file inspection, search, and editing.")
        );
        assert!(catalog.contains("artifact [available]:"));
        assert!(catalog.contains("browser [available]:"));
        assert_eq!(catalog.lines().count(), 6);
        assert!(catalog.contains("ctx [loaded]:"));
        for absent in ["history", "agents"] {
            assert!(!catalog.contains(absent));
        }
        assert!(!work
            .guidance(&context.policy)
            .contains(tools::browser::INTERFACE));
        assert!(work
            .definitions(&context.policy)
            .iter()
            .any(|s| s.name == "agent_browser"));
        assert_eq!(
            work.execute(
                "tools",
                &context,
                json!({"action":"activate", "package":"missing"})
            )
            .await
            .unwrap_err()
            .code,
            ToolErrorCode::InvalidInput
        );
        let call = || {
            work.execute(
                "tools",
                &context,
                json!({"action":"activate", "package":"browser"}),
            )
        };
        let (a, b) = tokio::join!(call(), call());
        let outputs = [a.unwrap().content, b.unwrap().content];
        assert_eq!(
            outputs
                .iter()
                .filter(|s| s.contains("already loaded"))
                .count(),
            1
        );
        assert_eq!(
            work.guidance(&context.policy)
                .matches(tools::browser::INTERFACE)
                .count(),
            1
        );
        assert!(!work
            .guidance(&context.policy)
            .contains(tools::browser::USAGE));
        let before = work.activation_snapshot();
        let help = work
            .execute(
                "tools",
                &context,
                json!({"action":"help", "package":"artifact"}),
            )
            .await
            .unwrap();
        assert_eq!(help.content, tools::artifact::USAGE);
        assert_eq!(before, work.activation_snapshot());
        assert!(installed.activation_snapshot().packages.is_empty());
        assert!(!installed
            .for_work(&context.policy, &[], &Default::default())
            .unwrap()
            .activation_snapshot()
            .packages
            .contains_key("browser"));

        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .remove("agent_browser");
        for action in ["activate", "help"] {
            assert_eq!(
                work.execute(
                    "tools",
                    &context,
                    json!({"action": action, "package":"browser"})
                )
                .await
                .unwrap_err()
                .code,
                ToolErrorCode::PermissionDenied
            );
        }
        assert!(!work.guidance(&context.policy).contains("browser"));
        assert_eq!(
            work.execute("agent_browser", &context, json!({}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .remove("tools");
        assert_eq!(
            work.execute("tools", &context, json!({"action":"list"}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
    }

    #[test]
    fn snapshots_revalidate_versions_permissions_and_installation() {
        let (installed, mut context) = fixture();
        let work = installed
            .for_work(
                &context.policy,
                &["workspace", "browser"],
                &Default::default(),
            )
            .unwrap();
        let encoded = serde_json::to_string(&work.activation_snapshot()).unwrap();
        let mut snapshot: ActivationSnapshot = serde_json::from_str(&encoded).unwrap();
        let restored = installed.for_work(&context.policy, &[], &snapshot).unwrap();
        assert_eq!(
            work.guidance(&context.policy),
            restored.guidance(&context.policy)
        );
        snapshot
            .packages
            .insert("browser".into(), "wrong-version".into());
        snapshot.packages.insert("missing".into(), "1".into());
        assert!(installed.for_work(&context.policy, &[], &snapshot).is_err());
        let unavailable = profiles::worker(
            Arc::new(Local::new(&context.workspace_root)),
            BrowserAvailability::Unavailable("test".into()),
        )
        .into_registry()
        .for_work(&context.policy, &["browser"], &work.activation_snapshot());
        assert!(unavailable.is_err());
        Arc::make_mut(&mut context.policy).capabilities.clear();
        assert!(installed
            .for_work(
                &context.policy,
                profiles::WORKER_EAGER,
                &work.activation_snapshot()
            )
            .is_err());
    }

    struct DiscoveryModel(std::sync::atomic::AtomicUsize);
    impl crate::harness::agent::AgentModel for DiscoveryModel {
        fn chat_with_context<'a>(
            &'a self,
            messages: &'a [tachyon_model::ChatMessage],
            schemas: &'a [ToolSpec],
            metadata: &'a tachyon_api::context::WorkerContextMetadata,
        ) -> crate::harness::agent::ModelFuture<'a> {
            assert_eq!(
                metadata
                    .activated_packages
                    .get("workspace")
                    .map(String::as_str),
                Some(env!("CARGO_PKG_VERSION"))
            );
            assert_eq!(
                metadata.activated_packages.contains_key("artifact"),
                self.0.load(std::sync::atomic::Ordering::SeqCst) > 0
            );
            assert!(metadata.activated_packages.contains_key("ctx"));
            self.chat(messages, schemas)
        }
        fn chat<'a>(
            &'a self,
            messages: &'a [tachyon_model::ChatMessage],
            schemas: &'a [ToolSpec],
        ) -> crate::harness::agent::ModelFuture<'a> {
            Box::pin(async move {
                let step = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let system = messages[0].plain();
                assert!(schemas.iter().any(|s| s.name == "tools"));
                assert!(schemas.iter().any(|s| s.name == "ipython"));
                assert_eq!(system.matches(tools::workspace::INTERFACE).count(), 1);
                assert_eq!(
                    system.matches(tools::artifact::INTERFACE).count(),
                    usize::from(step > 0)
                );
                Ok(tachyon_model::Completion {
                    text: if step == 0 { "" } else { "complete" }.into(),
                    tool_calls: if step == 0 {
                        vec![tachyon_model::ToolCall {
                            id: "activate".into(),
                            name: "tools".into(),
                            arguments: json!({"action":"activate", "package":"artifact"})
                                .to_string(),
                        }]
                    } else {
                        vec![]
                    },
                    usage: Default::default(),
                    finish_reason: None,
                })
            })
        }
    }
    impl crate::harness::agent::AgentLoopEventSink for DiscoveryModel {
        fn emit(&self, _: crate::harness::agent::AgentLoopEvent) {}
    }

    #[tokio::test]
    async fn production_loop_reconstructs_interfaces_without_history_duplication() {
        let (installed, context) = fixture();
        let work = installed
            .for_work(&context.policy, profiles::WORKER_EAGER, &Default::default())
            .unwrap();
        work.python_require("ctx", &context.policy).unwrap();
        let model = DiscoveryModel(std::sync::atomic::AtomicUsize::new(0));
        let mut messages = vec![tachyon_model::ChatMessage::new(
            tachyon_model::Role::System,
            "base",
        )];
        crate::harness::agent::run_loop(&model, &mut messages, &work, &context, 3, &model)
            .await
            .unwrap();
        assert_eq!(messages[0].plain(), "base");
        crate::harness::session::compact_completed_history(&mut messages);
        crate::harness::session::compact_context_messages(&mut messages, 0);
        let restored = installed
            .for_work(&context.policy, &[], &work.activation_snapshot())
            .unwrap();
        crate::harness::agent::run_loop(&model, &mut messages, &restored, &context, 1, &model)
            .await
            .unwrap();
        assert_eq!(messages[0].plain(), "base");
    }
}
