#![forbid(unsafe_code)]

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

use crate::harness::backend::{Backend, Local};
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolFuture, ToolResult,
};

pub const USAGE: &str = include_str!("usage.md");

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
    fn name(&self) -> &'static str {
        "ipython"
    }

    fn schema(&self) -> &ToolSpec {
        &self.schema
    }

    fn capabilities(&self) -> &'static [Capability] {
        &[Capability::ExecuteProcess]
    }

    fn execute<'a>(&'a self, _context: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: IpythonInput = decode_input(input)?;
            if input.code.len() > 256 * 1024 {
                return Err(ToolError::invalid("ipython code exceeds 256 KiB"));
            }
            let execution = self.backend.run_ipython(&input.code).await;
            let is_error = execution.timed_out || execution.exit_code != Some(0);
            let mut result = ToolResult::success(
                execution.combined(),
                json!({
                    "exit_code": execution.exit_code,
                    "timed_out": execution.timed_out,
                }),
            );
            result.is_error = is_error;
            Ok(result)
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IpythonInput {
    code: String,
}
