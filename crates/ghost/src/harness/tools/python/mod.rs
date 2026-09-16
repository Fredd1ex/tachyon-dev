#![forbid(unsafe_code)]

use std::sync::Arc;

pub(crate) mod bridge;

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use crate::harness::backend::Local;
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolFuture, ToolResult,
};

pub const USAGE: &str = include_str!("usage.md");
pub const INTERFACE: &str = "`ipython` executes persistent Python with top-level await. Use for retained intermediates, aggregation, transformation, or iteration; honor explicit user requests for Python. Combining calls is optional. Sample unknown structure first; bound reads/output, count/report skips and errors, never silently except/continue. require('workspace', asynchronous=True) exposes native methods (grep uses pattern; search aliases grep). require('exec') exposes awaitable start/status/output/wait/cancel/run; require('ctx') exposes read/list/search. Methods take keyword arguments and return native dictionaries: p['metadata']['operation'], p['metadata']['stdout']. Inspect .schemas/.guidance. require is synchronous metadata; legacy workspace methods stay synchronous. Await hostcalls sequentially; concurrent/background and recursive IPython calls are denied. `%cd` persists, `!cd` does not; native cwd is independent. Kernel failure/cancelled await loses variables; no replay. Direct tools are unchanged.";

pub fn ipython() -> ToolSpec {
    ToolSpec::new(
        "ipython",
        "Run Python or one `!` shell command in the assigned workspace.",
        json!({
            "type": "object",
            "properties": {
                "code": { "type": "string" }
            },
            "required": ["code"],
            "additionalProperties": false,
        }),
    )
}

pub struct IpythonTool {
    schema: ToolSpec,
    backend: Arc<Local>,
}

impl IpythonTool {
    pub fn new(backend: Arc<Local>) -> Self {
        Self {
            schema: ipython(),
            backend,
        }
    }
}

impl Tool for IpythonTool {
    fn end_work(&self, scope: uuid::Uuid) -> crate::harness::runtime::CleanupFuture {
        self.backend.end_work(scope)
    }
    fn manages_own_lifecycle(&self) -> bool {
        true
    }

    fn execute_with_registry<'a>(
        &'a self,
        context: &'a ToolContext,
        input: Value,
        registry: &'a crate::harness::runtime::ToolRegistry,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: IpythonInput = decode_input(input)?;
            if input.code.len() > 256 * 1024 {
                return Err(ToolError::invalid("ipython code exceeds 256 KiB"));
            }
            let execution = self
                .backend
                .python(&input.code, Some((registry, context)))
                .await;
            let mut result = ToolResult::success(
                execution.combined(),
                json!({"exit_code": execution.exit_code, "timed_out": execution.timed_out}),
            );
            result.is_error = execution.timed_out || execution.exit_code != Some(0);
            Ok(result)
        })
    }
    fn name(&self) -> &'static str {
        "ipython"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        &[Capability::ExecuteProcess]
    }

    fn execute<'a>(&'a self, context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let registry = crate::harness::runtime::ToolRegistry::default();
            self.execute_with_registry(context, input, &registry).await
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IpythonInput {
    code: String,
}
