//! Model tool schemas for Ghost's worker-only capabilities.

use serde_json::json;
use tachyon_model::ToolSpec;

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

pub fn agent_browser() -> ToolSpec {
    ToolSpec::new(
        "agent_browser",
        "Use the fixed browser. Prefer `read <URL>` for text. For interaction use `open`, `snapshot -i -c`, current refs, fresh snapshots after changes, targeted `get text`, and `close`.",
        json!({
            "type": "object",
            "properties": {
                "args": { "type": "string", "description": "One command's arguments; omit the executable and engine options." }
            },
            "required": ["args"],
            "additionalProperties": false,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_schema_budget_stays_compact() {
        let schemas = [ipython(), agent_browser()];
        let chars = schemas
            .iter()
            .map(|schema| {
                schema.name.len() + schema.description.len() + schema.parameters.to_string().len()
            })
            .sum::<usize>();
        assert!(chars < 850, "worker schemas grew to {chars} chars");
    }
}
