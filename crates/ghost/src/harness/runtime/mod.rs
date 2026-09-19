#![forbid(unsafe_code)]

pub(crate) mod binary;
pub(crate) mod cpu_jobs;
pub(crate) mod output_store;
pub(crate) mod path;
pub(crate) mod traversal;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{json, Value};
use tachyon_api::types::ArtifactRegistration;
use tachyon_model::ToolSpec;
use tokio_util::sync::CancellationToken;

pub use crate::harness::registry::{RegistryError, ToolRegistry};
pub use crate::harness::tools::artifact::ArtifactTool;
pub use crate::harness::tools::browser::{AgentBrowserTool, BrowserAvailability};
pub use crate::harness::tools::exec::ExecTool;
pub use crate::harness::tools::python::IpythonTool;
pub use crate::harness::tools::workspace::{
    EditTool, FindTool, GrepTool, LsTool, ReadTool, WriteTool,
};
pub use output_store::WorkspaceOutputStore;

pub const MAX_RETURN_BYTES: usize = 1024 * 1024;
pub const MAX_RETURN_LINES: usize = 2_000;
pub const DEFAULT_MODEL_CONTENT_BYTES: usize = 12_000;
pub const SANITIZED_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

pub fn native_registry() -> ToolRegistry {
    super::registry::builtins::native().into_registry()
}

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<ToolResult, ToolError>> + Send + 'a>>;
pub type CleanupFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub trait Tool: Send + Sync {
    /// Detach this work's resources synchronously; return bounded asynchronous teardown.
    /// Must be idempotent and must not create resources for an untouched work.
    fn end_work(&self, _scope: uuid::Uuid) -> CleanupFuture {
        Box::pin(async {})
    }
    fn name(&self) -> &'static str;
    fn schema(&self) -> &ToolSpec;
    fn capabilities(&self) -> &'static [Capability];
    fn manages_own_lifecycle(&self) -> bool {
        false
    }
    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a>;
    /// Adapters may dispatch through the active work registry; native tools need no handle.
    fn execute_with_registry<'a>(
        &'a self,
        context: &'a ToolContext,
        input: Value,
        _registry: &'a ToolRegistry,
    ) -> ToolFuture<'a> {
        self.execute(context, input)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Capability {
    ReadFilesystem,
    WriteFilesystem,
    ExecuteProcess,
    RegisterArtifact,
}

#[derive(Clone, Debug, Default)]
pub struct ToolIdentity {
    pub call_id: Option<String>,
    pub parent_call_id: Option<String>,
    pub task_id: Option<String>,
    pub work_id: Option<String>,
    pub generation: Option<u64>,
    pub assignment: Option<u64>,
    pub attempt_id: Option<String>,
}

impl ToolContext {
    pub fn for_call(&self, call_id: impl Into<String>) -> Self {
        let mut identity = self.identity.clone();
        identity.parent_call_id = identity.call_id.clone();
        identity.call_id = Some(call_id.into());
        Self {
            workspace_root: self.workspace_root.clone(),
            cwd: self.cwd.clone(),
            identity,
            deadline: self.deadline,
            cancellation: self.cancellation.clone(),
            policy: Arc::clone(&self.policy),
            event_sink: Arc::clone(&self.event_sink),
            output_store: Arc::clone(&self.output_store),
            host_service: self.host_service.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ToolPolicy {
    pub enabled_tools: BTreeSet<String>,
    pub capabilities: BTreeSet<Capability>,
    pub allowed_roots: Vec<PathBuf>,
    pub allow_absolute_paths: bool,
    pub max_duration: Duration,
    pub max_return_bytes: usize,
    pub max_return_lines: usize,
    pub max_model_content_bytes: usize,
    pub max_read_lines: usize,
    pub max_ls_entries: usize,
    pub max_write_bytes: usize,
    pub sync_writes: bool,
    pub max_find_results: usize,
    pub max_grep_matches: usize,
    pub max_grep_context_lines: usize,
    pub max_search_file_bytes: usize,
    pub max_search_line_bytes: usize,
    pub max_traversal_entries: usize,
    pub max_exec_duration: Duration,
    pub exec_term_grace: Duration,
    pub max_exec_output_bytes: usize,
    pub max_exec_command_bytes: usize,
    pub allow_shell_exec: bool,
    pub exec_shell: PathBuf,
    pub exec_path: String,
    pub exec_env: BTreeMap<String, String>,
    pub max_artifact_bytes: u64,
}

impl ToolPolicy {
    pub fn constrain(&mut self, permissions: &tachyon_api::types::WorkPermissions) {
        // Native policy only. An explicitly enabled interpreter is arbitrary code.
        if permissions.task_type == tachyon_api::types::WorkTaskType::CodingReadOnly {
            self.capabilities.remove(&Capability::WriteFilesystem);
        }
        self.enabled_tools.remove("agent_browser");
        if !permissions.allow_exec {
            self.enabled_tools.remove("exec");
            self.allow_shell_exec = false;
        }
        if !permissions.allow_python {
            self.enabled_tools.remove("ipython");
        }
        if !permissions.allow_exec && !permissions.allow_python {
            self.capabilities.remove(&Capability::ExecuteProcess);
        }
    }

    pub fn worker_default(workspace_root: PathBuf) -> Self {
        let exec_env = ["LANG", "LC_ALL", "LC_CTYPE", "TERM"]
            .into_iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| value.len() <= 256)
                    .map(|value| (name.to_string(), value))
            })
            .collect();
        Self {
            enabled_tools: [
                "read",
                "write",
                "edit",
                "ls",
                "find",
                "grep",
                "exec",
                "ctx",
                "artifact",
                "ipython",
                "agent_browser",
                "tools",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            capabilities: [
                Capability::ReadFilesystem,
                Capability::WriteFilesystem,
                Capability::ExecuteProcess,
                Capability::RegisterArtifact,
            ]
            .into_iter()
            .collect(),
            allowed_roots: vec![workspace_root],
            allow_absolute_paths: false,
            max_duration: Duration::from_secs(120),
            max_return_bytes: MAX_RETURN_BYTES,
            max_return_lines: MAX_RETURN_LINES,
            max_model_content_bytes: DEFAULT_MODEL_CONTENT_BYTES,
            max_read_lines: MAX_RETURN_LINES,
            max_ls_entries: 1_000,
            max_write_bytes: MAX_RETURN_BYTES,
            sync_writes: true,
            max_find_results: 1_000,
            max_grep_matches: 500,
            max_grep_context_lines: 10,
            max_search_file_bytes: MAX_RETURN_BYTES,
            max_search_line_bytes: 8 * 1024,
            max_traversal_entries: 100_000,
            max_exec_duration: Duration::from_secs(120),
            exec_term_grace: Duration::from_secs(2),
            max_exec_output_bytes: 64 * 1024 * 1024,
            max_exec_command_bytes: 64 * 1024,
            allow_shell_exec: true,
            exec_shell: PathBuf::from("/bin/sh"),
            exec_path: SANITIZED_PATH.into(),
            exec_env,
            max_artifact_bytes: 1024 * 1024 * 1024,
        }
    }

    pub fn permits(&self, tool: &dyn Tool) -> bool {
        self.enabled_tools.contains(tool.name())
            && tool
                .capabilities()
                .iter()
                .all(|capability| self.capabilities.contains(capability))
    }
}

#[derive(Clone)]
pub struct ToolContext {
    pub workspace_root: PathBuf,
    pub cwd: PathBuf,
    pub identity: ToolIdentity,
    pub deadline: Instant,
    pub cancellation: CancellationToken,
    pub policy: Arc<ToolPolicy>,
    pub event_sink: Arc<dyn ToolEventSink>,
    pub output_store: Arc<dyn ToolOutputStore>,
    /// Only broker-backed work is gated. Ordinary local exec is explicitly ungated.
    pub host_service: Option<Arc<tachyon_model::broker::BrokerClient>>,
}

pub trait ToolEventSink: Send + Sync {
    fn emit(&self, event: ToolTelemetry);
    fn record_result(
        &self,
        _name: &str,
        _context: &ToolContext,
        _input: &Value,
        _result: &ToolResult,
    ) {
    }
    fn register_artifact(&self, artifact: ArtifactRegistration) -> Result<(), String>;
}

pub struct WorkEvidenceCollector(Mutex<tachyon_api::types::WorkEvidence>);

impl Default for WorkEvidenceCollector {
    fn default() -> Self {
        Self(Mutex::new(tachyon_api::types::WorkEvidence {
            observed_invocations: Some(0),
            ..Default::default()
        }))
    }
}

impl WorkEvidenceCollector {
    pub fn record(&self, name: &str, context: &ToolContext, input: &Value, result: &ToolResult) {
        let mut evidence = self.0.lock().expect("evidence lock poisoned");
        evidence.observed_invocations = evidence.observed_invocations.map(|n| n.saturating_add(1));
        let arguments = if input.to_string().len() <= 2048 {
            input.clone()
        } else {
            json!({"omitted": "arguments exceeded evidence budget"})
        };
        let entry = tachyon_api::types::WorkToolEvidence {
            call_id: context.identity.call_id.clone(),
            parent_call_id: context.identity.parent_call_id.clone(),
            tool_name: name.into(),
            arguments,
            output: serde_json::from_str(&result.to_json(2048)).expect("ToolResult JSON"),
        };
        // Bound the serialized bundle too: escaping and metadata can exceed content limits.
        evidence.tools.push(entry);
        if evidence.tools.len() > 32
            || serde_json::to_vec(&*evidence).unwrap().len() > 16 * 1024 - 64
        {
            evidence.tools.pop();
            evidence.omitted = evidence.omitted.saturating_add(1);
        }
    }

    pub fn snapshot(&self) -> tachyon_api::types::WorkEvidence {
        self.0.lock().expect("evidence lock poisoned").clone()
    }
}

pub struct NoopEventSink;

impl ToolEventSink for NoopEventSink {
    fn emit(&self, _event: ToolTelemetry) {}

    fn register_artifact(&self, _artifact: ArtifactRegistration) -> Result<(), String> {
        Err("artifact event sink is unavailable".into())
    }
}

pub trait ToolOutputStore: Send + Sync {
    /// Raw bounded page for durable export; no lossy UTF-8 conversion.
    fn export_page<'a>(
        &'a self,
        _reference: &'a ToolOutputRef,
        _offset: u64,
    ) -> OutputStoreFuture<'a, Result<OutputPage, String>> {
        Box::pin(async { Err("output export unavailable".into()) })
    }
    fn put<'a>(&'a self, value: String) -> OutputStoreFuture<'a, Option<ToolOutputRef>>;
    fn get<'a>(&'a self, reference: &'a ToolOutputRef) -> OutputStoreFuture<'a, Option<String>>;
    /// Optional bounded navigation. Stores without a work-scoped catalog fail closed.
    fn page<'a>(
        &'a self,
        _reference: &'a ToolOutputRef,
        _cursor: usize,
        _limit: usize,
    ) -> OutputStoreFuture<'a, Result<ToolResult, ToolError>> {
        Box::pin(async {
            Err(ToolError::invalid(
                "this output store does not support scoped paging",
            ))
        })
    }
    fn references(&self) -> Vec<ToolOutputRef> {
        Vec::new()
    }
    fn search_page<'a>(
        &'a self,
        _reference: &'a ToolOutputRef,
        _cursor: usize,
        _limit: usize,
        _query: &'a str,
    ) -> OutputStoreFuture<'a, Result<ToolResult, ToolError>> {
        Box::pin(async {
            Err(ToolError::invalid(
                "this output store does not support scoped search",
            ))
        })
    }
}

pub type OutputStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct OutputPage {
    pub bytes: Vec<u8>,
    pub retained: u64,
    pub total: u64,
    pub storage_failed: bool,
}

pub struct NoopOutputStore;

impl ToolOutputStore for NoopOutputStore {
    fn put<'a>(&'a self, _value: String) -> OutputStoreFuture<'a, Option<ToolOutputRef>> {
        Box::pin(async { None })
    }

    fn get<'a>(&'a self, _reference: &'a ToolOutputRef) -> OutputStoreFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
}

pub struct InMemoryOutputStore {
    next_id: AtomicU64,
    max_bytes: usize,
    max_entry_bytes: usize,
    state: Mutex<OutputStoreState>,
}

#[derive(Default)]
struct OutputStoreState {
    bytes: usize,
    entries: VecDeque<(String, String)>,
}

impl InMemoryOutputStore {
    pub fn new(max_bytes: usize, max_entry_bytes: usize) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            max_bytes,
            max_entry_bytes,
            state: Mutex::new(OutputStoreState::default()),
        }
    }
}

impl ToolOutputStore for InMemoryOutputStore {
    fn put<'a>(&'a self, value: String) -> OutputStoreFuture<'a, Option<ToolOutputRef>> {
        Box::pin(async move {
            if value.len() > self.max_entry_bytes || value.len() > self.max_bytes {
                return None;
            }
            let id = format!(
                "tool-output-{}",
                self.next_id.fetch_add(1, Ordering::Relaxed)
            );
            let mut state = self.state.lock().ok()?;
            while state.bytes.saturating_add(value.len()) > self.max_bytes {
                let (_, evicted) = state.entries.pop_front()?;
                state.bytes = state.bytes.saturating_sub(evicted.len());
            }
            state.bytes += value.len();
            state.entries.push_back((id.clone(), value));
            Some(ToolOutputRef { id })
        })
    }

    fn get<'a>(&'a self, reference: &'a ToolOutputRef) -> OutputStoreFuture<'a, Option<String>> {
        Box::pin(async move {
            self.state
                .lock()
                .ok()?
                .entries
                .iter()
                .find(|(id, _)| id == &reference.id)
                .map(|(_, value)| value.clone())
        })
    }
}

#[derive(Clone, Debug)]
pub struct ToolTelemetry {
    pub tool_name: String,
    pub started: Instant,
    pub duration: Duration,
    pub success: bool,
    pub truncated: bool,
    pub bytes_out: usize,
    pub error_code: Option<ToolErrorCode>,
    pub identity: ToolIdentity,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
    pub metadata: Value,
    pub truncated: bool,
    pub continuation: Option<Continuation>,
    pub output_ref: Option<ToolOutputRef>,
}

impl ToolResult {
    pub fn success(content: String, metadata: Value) -> Self {
        Self {
            content,
            is_error: false,
            metadata,
            truncated: false,
            continuation: None,
            output_ref: None,
        }
    }

    pub fn to_json(&self, max_bytes: usize) -> String {
        let mut bounded = self.clone();
        let (content, truncated) = bound_utf8(&bounded.content, max_bytes.saturating_sub(512));
        bounded.content = content;
        bounded.truncated |= truncated;
        if truncated {
            bounded.continuation = None;
        }
        if bounded.truncated {
            // A read digest must not describe bytes omitted from the delivered snapshot.
            if let Some(digest) = bounded.metadata.get_mut("sha256") {
                *digest = Value::Null;
            }
        }
        let encoded = serde_json::to_string(&bounded).unwrap_or_default();
        if encoded.len() <= max_bytes {
            return encoded;
        }

        bounded.metadata = json!({"omitted": "metadata exceeded output budget"});
        bounded.truncated = true;
        bounded.output_ref = None;
        bounded.continuation = None;
        // Bound encoded bytes, not raw text: control characters can expand sixfold.
        loop {
            let encoded = serde_json::to_string(&bounded).expect("ToolResult JSON");
            if encoded.len() <= max_bytes || bounded.content.is_empty() {
                return encoded;
            }
            bounded.content = bound_utf8(&bounded.content, bounded.content.len() / 2).0;
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Continuation {
    pub next_offset: Option<u64>,
    pub after: Option<String>,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOutputRef {
    pub id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorCode {
    InvalidInput,
    NotFound,
    PermissionDenied,
    OutsideWorkspace,
    UnsupportedBinary,
    AmbiguousEdit,
    Conflict,
    Timeout,
    Cancelled,
    ProcessFailed,
    DependencyUnavailable,
    Io,
    Internal,
}

#[derive(Clone, Debug, Serialize)]
pub struct ToolError {
    pub code: ToolErrorCode,
    pub message: String,
    pub retryable: bool,
    pub metadata: Value,
}

impl ToolError {
    pub fn new(code: ToolErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            metadata: json!({}),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ToolErrorCode::InvalidInput, message, false)
    }

    pub fn into_result(self) -> ToolResult {
        ToolResult {
            content: self.message.clone(),
            is_error: true,
            metadata: json!({
                "code": self.code,
                "retryable": self.retryable,
                "details": self.metadata,
            }),
            truncated: false,
            continuation: None,
            output_ref: None,
        }
    }
}

pub(crate) fn decode_input<T: serde::de::DeserializeOwned>(input: Value) -> Result<T, ToolError> {
    serde_json::from_value(input).map_err(|error| ToolError::invalid(error.to_string()))
}

pub(crate) fn bound_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (value[..end].to_string(), true)
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_coding_profile_preserves_native_writes_without_granting_process_tools() {
        use super::{Capability, ToolPolicy};
        use tachyon_api::types::{WorkPermissions, WorkTaskType};
        for task_type in [WorkTaskType::CodingReadOnly, WorkTaskType::Coding] {
            let mut policy = ToolPolicy::worker_default("/tmp/work".into());
            policy.constrain(&WorkPermissions {
                task_type: task_type.clone(),
                allow_exec: false,
                allow_python: false,
            });
            assert_eq!(
                policy.capabilities.contains(&Capability::WriteFilesystem),
                task_type == WorkTaskType::Coding
            );
            assert!(!policy.capabilities.contains(&Capability::ExecuteProcess));
            for tool in ["exec", "ipython", "agent_browser"] {
                assert!(!policy.enabled_tools.contains(tool));
            }
        }
    }
    use serde_json::json;

    use super::{InMemoryOutputStore, ToolOutputStore, ToolResult};

    #[test]
    fn serialized_results_respect_the_total_byte_budget() {
        let result = ToolResult::success("x".repeat(10_000), json!({"large": "y".repeat(10_000)}));
        let encoded = result.to_json(1_000);
        assert!(encoded.len() <= 1_000, "encoded {} bytes", encoded.len());
        let decoded: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded["truncated"], true);
        for content in ["\u{0000}", "\"", "\\", "\u{00e9}"] {
            let result = super::ToolError::invalid(content.repeat(16_384)).into_result();
            let encoded = result.to_json(2048);
            assert!(encoded.len() <= 2048);
            let decoded: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded["is_error"], true);
            assert_eq!(decoded["truncated"], true);
        }
    }

    #[tokio::test]
    async fn output_store_is_bounded_and_evicts_oldest_entries() {
        let store = InMemoryOutputStore::new(8, 8);
        let first = store.put("1234".into()).await.unwrap();
        let second = store.put("5678".into()).await.unwrap();
        assert_eq!(store.get(&first).await.as_deref(), Some("1234"));
        let third = store.put("90".into()).await.unwrap();
        assert!(store.get(&first).await.is_none());
        assert_eq!(store.get(&second).await.as_deref(), Some("5678"));
        assert_eq!(store.get(&third).await.as_deref(), Some("90"));
        assert!(store.put("123456789".into()).await.is_none());
    }
}
