#![forbid(unsafe_code)]

//! Atomic validation of trusted packages before publishing their operations.

use super::manifest::{Manifest, Package};
use super::{RegistryError, ToolRegistry};
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum PackageError {
    #[error("duplicate package name: {0}")]
    DuplicatePackage(String),
    #[error("invalid package manifest: {0}")]
    InvalidManifest(String),
    #[error(transparent)]
    Registry(#[from] RegistryError),
}

#[derive(Default)]
pub struct Packages {
    registry: ToolRegistry,
}

impl Packages {
    /// Rejection leaves both manifests and registered tools unchanged.
    /// Operation names are global, not namespaced by package or version.
    pub fn register(&mut self, package: Package) -> Result<(), PackageError> {
        let manifest = &package.manifest;
        if self
            .registry
            .manifests
            .iter()
            .any(|entry| entry.name == manifest.name)
        {
            return Err(PackageError::DuplicatePackage(manifest.name.into()));
        }
        let operations = manifest.operations.iter().copied().collect::<BTreeSet<_>>();
        let names = package
            .tools
            .iter()
            .map(|tool| tool.name())
            .collect::<BTreeSet<_>>();
        if [manifest.name, manifest.version, manifest.description]
            .iter()
            .any(|text| text.trim().is_empty() || text.contains(['\n', '\r']))
            || manifest.usage.trim().is_empty()
            || manifest.interface.trim().is_empty()
            || operations.is_empty()
            || operations.iter().any(|name| name.trim().is_empty())
            || operations.len() != manifest.operations.len()
            || names.len() != package.tools.len()
            || operations != names
            || package
                .tools
                .iter()
                .any(|tool| tool.schema().name != tool.name())
        {
            return Err(PackageError::InvalidManifest(manifest.name.into()));
        }
        self.registry.register_batch(package.tools)?;
        self.registry.manifests.push(package.manifest);
        Ok(())
    }

    /// Installed metadata, not a policy-filtered list for model advertisement.
    pub fn manifests(&self) -> &[Manifest] {
        &self.registry.manifests
    }

    pub fn into_registry(self) -> ToolRegistry {
        self.registry
    }
}

#[cfg(test)]
mod tests {
    use super::super::builtins as capabilities;
    use super::{PackageError as ProfileError, Packages as Profile};
    use crate::harness::backend::Local;
    use crate::harness::profiles::worker;
    use crate::harness::runtime::BrowserAvailability;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::{json, Value};
    use tachyon_model::{ChatMessage, Completion, Content, Role, ToolCall, ToolSpec};

    use super::*;
    use crate::harness::agent::{
        run_loop, AgentLoopEvent, AgentLoopEventSink, AgentModel, ModelFuture,
    };
    use crate::harness::runtime::{
        native_registry, Capability, NoopEventSink, NoopOutputStore, Tool, ToolContext,
        ToolErrorCode, ToolFuture, ToolIdentity, ToolPolicy, ToolResult,
    };

    struct FakeTool {
        schema: ToolSpec,
        calls: Arc<AtomicUsize>,
    }

    impl Tool for FakeTool {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn schema(&self) -> &ToolSpec {
            &self.schema
        }
        fn capabilities(&self) -> &'static [Capability] {
            &[Capability::ReadFilesystem]
        }
        fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ToolResult::success(
                    "fake result".into(),
                    json!({"input": input, "call_id": context.identity.call_id}),
                ))
            })
        }
    }

    fn fake(calls: Arc<AtomicUsize>) -> Package {
        Package {
            manifest: Manifest {
                name: "fake-package",
                version: "1.0.0",
                description: "Test capability.",
                interface: "`fake` accepts a JSON object.",
                usage: "Call fake with a JSON object.",
                operations: &["fake"],
            },
            tools: vec![Arc::new(FakeTool {
                schema: ToolSpec::new("fake", "Test operation", json!({"type": "object"})),
                calls,
            })],
        }
    }

    #[test]
    fn native_set_and_worker_manifests_match_existing_schemas() {
        let root = std::env::current_dir().unwrap();
        let policy = ToolPolicy::worker_default(root.clone());
        let native = native_registry().definitions(&policy);
        assert_eq!(
            native.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["artifact", "ctx", "edit", "exec", "find", "grep", "ls", "read", "write"]
        );
        for available in [false, true] {
            let availability = if available {
                BrowserAvailability::Available
            } else {
                BrowserAvailability::Unavailable("test".into())
            };
            let profile = worker(Arc::new(Local::new(&root)), availability);
            assert_eq!(
                profile
                    .manifests()
                    .iter()
                    .map(|m| m.name)
                    .collect::<Vec<_>>(),
                if available {
                    vec!["workspace", "exec", "artifact", "ctx", "ipython", "browser"]
                } else {
                    vec!["workspace", "exec", "artifact", "ctx", "ipython"]
                }
            );
            let operations = profile
                .manifests()
                .iter()
                .flat_map(|m| m.operations.iter().copied())
                .collect::<BTreeSet<_>>();
            let schemas = profile.into_registry().definitions(&policy);
            assert_eq!(
                operations,
                schemas.iter().map(|s| s.name.as_str()).collect()
            );
            let mut expected = native.clone();
            expected.push(crate::harness::tools::ipython());
            if available {
                expected.push(crate::harness::tools::agent_browser());
            }
            expected.sort_by(|a, b| a.name.cmp(&b.name));
            for (actual, expected) in schemas.iter().zip(&expected) {
                assert_eq!(actual.name, expected.name);
                assert_eq!(actual.description, expected.description);
                assert_eq!(actual.parameters, expected.parameters);
            }
        }
    }

    #[test]
    fn native_packages_can_be_selected_independently() {
        let policy = ToolPolicy::worker_default(std::env::current_dir().unwrap());
        for (package, expected) in [
            (
                capabilities::workspace(),
                vec!["edit", "find", "grep", "ls", "read", "write"],
            ),
            (capabilities::exec(), vec!["exec"]),
            (capabilities::artifact(), vec!["artifact"]),
        ] {
            let mut profile = Profile::default();
            profile.register(package).unwrap();
            assert_eq!(profile.manifests().len(), 1);
            assert_eq!(
                profile
                    .into_registry()
                    .definitions(&policy)
                    .iter()
                    .map(|schema| schema.name.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn rejected_packages_leave_no_partial_registration() {
        let mut profile = Profile::default();
        profile.register(capabilities::workspace()).unwrap();
        let before = profile.manifests().to_vec();
        let mut duplicate = fake(Arc::default());
        duplicate.manifest.name = "workspace";
        assert!(matches!(
            profile.register(duplicate),
            Err(ProfileError::DuplicatePackage(_))
        ));

        // A new operation preceding a collision must not leak into the registry.
        let mut collision = fake(Arc::default());
        collision.manifest.operations = &["fake", "read"];
        collision
            .tools
            .push(Arc::new(crate::harness::runtime::ReadTool::new()));
        assert!(matches!(
            profile.register(collision),
            Err(ProfileError::Registry(RegistryError::Duplicate(_)))
        ));

        let mut mismatch = fake(Arc::default());
        mismatch.manifest.operations = &["not-fake"];
        assert!(matches!(
            profile.register(mismatch),
            Err(ProfileError::InvalidManifest(_))
        ));
        let mut mismatch = fake(Arc::default());
        mismatch.tools = vec![Arc::new(FakeTool {
            schema: ToolSpec::new("wrong-schema-name", "fake", json!({})),
            calls: Arc::default(),
        })];
        assert!(matches!(
            profile.register(mismatch),
            Err(ProfileError::InvalidManifest(_))
        ));
        assert_eq!(profile.manifests(), before);
        profile
            .register(fake(Arc::default()))
            .expect("failed batches did not reserve fake");
    }

    #[tokio::test]
    async fn worker_packages_do_not_grant_native_or_adapter_permissions() {
        let root = std::env::current_dir().unwrap();
        let registry =
            worker(Arc::new(Local::new(&root)), BrowserAvailability::Available).into_registry();
        let mut policy = ToolPolicy::worker_default(root.clone());
        policy.capabilities = [Capability::ReadFilesystem].into_iter().collect();
        assert_eq!(
            registry
                .definitions(&policy)
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["ctx", "find", "grep", "ls", "read"]
        );
        let context = ToolContext {
            workspace_root: root.clone(),
            cwd: root,
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: Default::default(),
            policy: Arc::new(policy),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        for name in [
            "write",
            "edit",
            "exec",
            "artifact",
            "ipython",
            "agent_browser",
        ] {
            assert_eq!(
                registry
                    .execute(name, &context, json!({}))
                    .await
                    .unwrap_err()
                    .code,
                ToolErrorCode::PermissionDenied,
                "{name}"
            );
        }
    }

    struct FakeModel;
    impl AgentModel for FakeModel {
        fn chat<'a>(
            &'a self,
            messages: &'a [ChatMessage],
            tools: &'a [ToolSpec],
        ) -> ModelFuture<'a> {
            Box::pin(async move {
                assert_eq!(
                    tools.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
                    ["fake"]
                );
                if let Some(ChatMessage {
                    role: Role::Tool,
                    content,
                }) = messages.last()
                {
                    let Content::ToolResult { id, output } = &content[0] else {
                        panic!("expected tool result")
                    };
                    assert_eq!(id, "fake-call");
                    let output: Value = serde_json::from_str(output).unwrap();
                    assert_eq!(output["content"], "fake result");
                    assert_eq!(output["is_error"], false);
                    return Ok(Completion {
                        text: "complete".into(),
                        tool_calls: vec![],
                        usage: Default::default(),
                        finish_reason: None,
                    });
                }
                Ok(Completion {
                    text: String::new(),
                    tool_calls: vec![ToolCall {
                        id: "fake-call".into(),
                        name: "fake".into(),
                        arguments: "{}".into(),
                    }],
                    usage: Default::default(),
                    finish_reason: None,
                })
            })
        }
    }

    impl AgentLoopEventSink for FakeModel {
        fn emit(&self, _: AgentLoopEvent) {}
    }

    #[tokio::test]
    async fn fake_package_assembly_loop_and_execution_authorization() {
        let root = std::env::current_dir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut profile = Profile::default();
        profile.register(fake(calls.clone())).unwrap();
        let registry = profile.into_registry();
        let mut policy = ToolPolicy::worker_default(root.clone());
        policy.enabled_tools = ["fake".into()].into_iter().collect();
        let mut context = ToolContext {
            workspace_root: root.clone(),
            cwd: root,
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(10),
            cancellation: Default::default(),
            policy: Arc::new(policy),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        };
        let (answer, _) = run_loop(&FakeModel, &mut vec![], &registry, &context, 2, &FakeModel)
            .await
            .unwrap();
        assert_eq!(answer, "complete");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Prior advertisement/execution is not authorization for the next call.
        Arc::make_mut(&mut context.policy).enabled_tools.clear();
        assert!(registry.definitions(&context.policy).is_empty());
        assert_eq!(
            registry
                .execute("fake", &context, json!({}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        Arc::make_mut(&mut context.policy)
            .enabled_tools
            .insert("fake".into());
        Arc::make_mut(&mut context.policy).capabilities.clear();
        assert!(registry.definitions(&context.policy).is_empty());
        assert_eq!(
            registry
                .execute("fake", &context, json!({}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::PermissionDenied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Removing a package means omitting it from a newly assembled profile.
        let without = Profile::default().into_registry();
        assert!(without.definitions(&context.policy).is_empty());
        assert_eq!(
            without
                .execute("fake", &context, json!({}))
                .await
                .unwrap_err()
                .code,
            ToolErrorCode::InvalidInput
        );
    }
}
