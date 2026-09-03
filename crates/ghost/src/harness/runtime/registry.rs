#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tachyon_model::ToolSpec;

use super::{bound_utf8, Tool, ToolContext, ToolError, ToolErrorCode, ToolResult, ToolTelemetry};

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("duplicate tool name: {0}")]
    Duplicate(String),
}

#[derive(Default)]
pub struct ToolRegistry {
    tools: HashMap<&'static str, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn register<T: Tool + 'static>(&mut self, tool: T) -> Result<(), RegistryError> {
        if self.tools.contains_key(tool.name()) {
            return Err(RegistryError::Duplicate(tool.name().into()));
        }
        self.tools.insert(tool.name(), Arc::new(tool));
        Ok(())
    }

    pub fn definitions(&self, policy: &super::ToolPolicy) -> Vec<ToolSpec> {
        let mut definitions = self
            .tools
            .values()
            .filter(|tool| policy.permits(tool.as_ref()))
            .map(|tool| tool.schema().clone())
            .collect::<Vec<_>>();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        definitions
    }

    pub async fn execute(
        &self,
        name: &str,
        context: &ToolContext,
        input: Value,
    ) -> Result<ToolResult, ToolError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| ToolError::invalid(format!("unknown tool: {name}")))?;
        if !context.policy.permits(tool.as_ref()) {
            return Err(ToolError::new(
                ToolErrorCode::PermissionDenied,
                format!("tool is disabled by policy: {name}"),
                false,
            ));
        }
        if context.cancellation.is_cancelled() {
            return Err(ToolError::new(
                ToolErrorCode::Cancelled,
                "tool call cancelled",
                true,
            ));
        }

        let started = Instant::now();
        let policy_deadline = started
            .checked_add(context.policy.max_duration)
            .unwrap_or(context.deadline);
        let deadline = context.deadline.min(policy_deadline);
        let result = if tool.manages_own_lifecycle() {
            tool.execute(context, input).await
        } else {
            tokio::select! {
                _ = context.cancellation.cancelled() => Err(ToolError::new(
                    ToolErrorCode::Cancelled,
                    "tool call cancelled",
                    true,
                )),
                _ = tokio::time::sleep_until(deadline.into()) => Err(ToolError::new(
                    ToolErrorCode::Timeout,
                    "tool call timed out",
                    true,
                )),
                result = tool.execute(context, input) => result,
            }
        };

        let result = match result {
            Ok(mut result) => {
                let (content, byte_truncated) =
                    bound_utf8(&result.content, context.policy.max_return_bytes);
                result.content = content;
                let line_limit = context.policy.max_return_lines;
                let line_count = result.content.lines().count();
                if line_count > line_limit {
                    let mut content = result
                        .content
                        .lines()
                        .take(line_limit)
                        .collect::<Vec<_>>()
                        .join("\n");
                    if result.content.ends_with('\n') {
                        content.push('\n');
                    }
                    result.content = content;
                    result.truncated = true;
                }
                result.truncated |= byte_truncated;
                let full_envelope = result.to_json(context.policy.max_return_bytes);
                if full_envelope.len() > context.policy.max_model_content_bytes {
                    result.output_ref = context.output_store.put(full_envelope).await;
                }
                Ok(result)
            }
            Err(error) => Err(error),
        };
        let duration = started.elapsed();
        let (success, truncated, bytes_out, error_code) = match &result {
            Ok(result) => (
                !result.is_error,
                result.truncated,
                result.content.len(),
                None,
            ),
            Err(error) => (false, false, 0, Some(error.code)),
        };
        context.event_sink.emit(ToolTelemetry {
            tool_name: name.into(),
            started,
            duration,
            success,
            truncated,
            bytes_out,
            error_code,
            identity: context.identity.clone(),
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use serde_json::json;
    use tachyon_model::ToolSpec;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{
        Capability, InMemoryOutputStore, NoopEventSink, NoopOutputStore, ToolFuture, ToolIdentity,
        ToolOutputStore, ToolPolicy,
    };

    struct FakeTool {
        name: &'static str,
        schema: ToolSpec,
        entered: Arc<AtomicBool>,
        delay: Duration,
        content: String,
    }

    impl FakeTool {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                schema: ToolSpec::new(name, "fake", json!({"type":"object"})),
                entered: Arc::new(AtomicBool::new(false)),
                delay: Duration::ZERO,
                content: "ok".into(),
            }
        }
    }

    impl Tool for FakeTool {
        fn name(&self) -> &'static str {
            self.name
        }

        fn schema(&self) -> &ToolSpec {
            &self.schema
        }

        fn capabilities(&self) -> &'static [Capability] {
            &[Capability::ReadFilesystem]
        }

        fn execute<'a>(&'a self, _context: &'a ToolContext, _input: Value) -> ToolFuture<'a> {
            Box::pin(async move {
                self.entered.store(true, Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
                Ok(ToolResult::success(self.content.clone(), json!({})))
            })
        }
    }

    fn context(cancellation: CancellationToken) -> ToolContext {
        let root = std::env::current_dir().unwrap().canonicalize().unwrap();
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(1),
            cancellation,
            policy: Arc::new(ToolPolicy {
                enabled_tools: ["fake".into()].into_iter().collect(),
                capabilities: [Capability::ReadFilesystem].into_iter().collect(),
                allowed_roots: vec![root],
                allow_absolute_paths: false,
                max_duration: Duration::from_secs(1),
                max_return_bytes: 1024,
                max_return_lines: 20,
                max_model_content_bytes: 512,
                max_read_lines: 20,
                max_ls_entries: 20,
                max_write_bytes: 1024,
                sync_writes: false,
                max_find_results: 20,
                max_grep_matches: 20,
                max_grep_context_lines: 2,
                max_search_file_bytes: 1024,
                max_search_line_bytes: 256,
                max_traversal_entries: 100,
                max_exec_duration: Duration::from_secs(1),
                exec_term_grace: Duration::from_millis(10),
                max_exec_output_bytes: 1024,
                max_exec_command_bytes: 1024,
                allow_shell_exec: false,
                exec_shell: "/bin/sh".into(),
                exec_path: "/usr/bin:/bin".into(),
                exec_env: Default::default(),
                max_artifact_bytes: 1024,
            }),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
        }
    }

    #[tokio::test]
    async fn duplicate_names_are_rejected_without_replacing_the_first_tool() {
        let mut registry = ToolRegistry::default();
        registry.register(FakeTool::new("fake")).unwrap();
        assert!(matches!(
            registry.register(FakeTool::new("fake")),
            Err(RegistryError::Duplicate(_))
        ));
        assert_eq!(
            registry
                .definitions(&context(CancellationToken::new()).policy)
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn cancellation_prevents_tool_entry() {
        let tool = FakeTool::new("fake");
        let entered = Arc::clone(&tool.entered);
        let mut registry = ToolRegistry::default();
        registry.register(tool).unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = registry
            .execute("fake", &context(cancellation), json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Cancelled);
        assert!(!entered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unknown_and_capability_denied_calls_fail_closed() {
        let tool = FakeTool::new("fake");
        let entered = Arc::clone(&tool.entered);
        let mut registry = ToolRegistry::default();
        registry.register(tool).unwrap();
        let unknown = registry
            .execute("missing", &context(CancellationToken::new()), json!({}))
            .await
            .unwrap_err();
        assert_eq!(unknown.code, ToolErrorCode::InvalidInput);

        let mut denied_context = context(CancellationToken::new());
        Arc::make_mut(&mut denied_context.policy)
            .capabilities
            .clear();
        let denied = registry
            .execute("fake", &denied_context, json!({}))
            .await
            .unwrap_err();
        assert_eq!(denied.code, ToolErrorCode::PermissionDenied);
        assert!(!entered.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancellation_interrupts_an_active_tool() {
        let mut tool = FakeTool::new("fake");
        tool.delay = Duration::from_secs(10);
        let mut registry = ToolRegistry::default();
        registry.register(tool).unwrap();
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            trigger.cancel();
        });
        let error = registry
            .execute("fake", &context(cancellation), json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn policy_deadline_interrupts_a_slow_tool() {
        let mut tool = FakeTool::new("fake");
        tool.delay = Duration::from_secs(10);
        let mut registry = ToolRegistry::default();
        registry.register(tool).unwrap();
        let mut context = context(CancellationToken::new());
        Arc::make_mut(&mut context.policy).max_duration = Duration::from_millis(10);
        let error = registry
            .execute("fake", &context, json!({}))
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::Timeout);
    }

    #[tokio::test]
    async fn oversized_model_results_receive_an_output_reference() {
        let mut tool = FakeTool::new("fake");
        tool.content = "x".repeat(2_000);
        let mut registry = ToolRegistry::default();
        registry.register(tool).unwrap();
        let mut context = context(CancellationToken::new());
        Arc::make_mut(&mut context.policy).max_return_bytes = 4_000;
        let store = Arc::new(InMemoryOutputStore::new(8_000, 4_000));
        context.output_store = store.clone();
        let result = registry.execute("fake", &context, json!({})).await.unwrap();
        let reference = result.output_ref.expect("large output is stored");
        assert!(store
            .get(&reference)
            .await
            .unwrap()
            .contains(&"x".repeat(1_000)));
    }

    #[test]
    fn definitions_are_sorted_and_policy_filtered() {
        let mut registry = ToolRegistry::default();
        registry.register(FakeTool::new("zulu")).unwrap();
        registry.register(FakeTool::new("alpha")).unwrap();
        let mut context = context(CancellationToken::new());
        Arc::make_mut(&mut context.policy).enabled_tools =
            ["zulu".into(), "alpha".into()].into_iter().collect();
        assert_eq!(
            registry
                .definitions(&context.policy)
                .into_iter()
                .map(|definition| definition.name)
                .collect::<Vec<_>>(),
            ["alpha", "zulu"]
        );
    }
}
