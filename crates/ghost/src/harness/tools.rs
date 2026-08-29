//! Model tool schemas for Ghost's worker-only capabilities.

use serde_json::json;
use tachyon_model::ToolSpec;

pub fn ipython() -> ToolSpec {
    ToolSpec::new(
        "ipython",
        "Run IPython code in the agent environment. Use normal Python for analysis and prefix shell commands with ! (for example, !rg pattern or !curl URL). The working directory is the agent workspace.",
        json!({
            "type": "object",
            "properties": {
                "code": { "type": "string", "description": "IPython code to execute." }
            },
            "required": ["code"],
            "additionalProperties": false,
        }),
    )
}

pub fn agent_browser() -> ToolSpec {
    ToolSpec::new(
        "agent_browser",
        "Use the fixed agent-browser CLI with its preconfigured Lightpanda engine. Prefer `read <URL>` for agent-readable research. For rendered interaction, use `open <URL>`, `snapshot -i -c`, current `@eN` refs, targeted `get text`, and `close`; refresh refs after navigation or page changes.",
        json!({
            "type": "object",
            "properties": {
                "args": { "type": "string", "description": "Exactly one command's arguments after the fixed executable, such as `read <URL>`, `open <URL>`, `snapshot -i -c`, `click @e2`, `get text @e1`, or `close`. Do not include `agent-browser`, `--engine`, or an executable path." }
            },
            "required": ["args"],
            "additionalProperties": false,
        }),
    )
}
