//! Provider-neutral schemas for role-owned capabilities.

use serde_json::json;

pub struct ToolSchema {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

impl ToolSchema {
    fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

pub fn spawn_agent() -> ToolSchema {
    ToolSchema::new(
        "spawn_agent",
        "Delegate one objective to a worker. Use short for disposable work, long for reusable work, or persistent for durable work. Independent objectives may be delegated together.",
        json!({
            "type": "object",
            "properties": {
                "task": { "type": "string", "description": "Objective to complete." },
                "cwd": { "type": "string", "description": "Optional workspace." },
                "lifetime_class": { "type": "string", "enum": ["short", "long", "persistent"], "default": "long" },
                "purpose": { "type": "string", "description": "Objective label." }
            },
            "required": ["task"],
            "additionalProperties": false,
        }),
    )
}

pub fn respond() -> ToolSchema {
    ToolSchema::new(
        "respond",
        "Respond directly only when the request can be answered accurately from the conversation or stable general knowledge and requires no fresh evidence, external retrieval, or execution. If information is missing, current, environment-dependent, or must be verified, use spawn_agent or spawn_agents instead.",
        json!({
            "type": "object",
            "properties": {
                "response": { "type": "string", "description": "The complete natural response to the user." }
            },
            "required": ["response"],
            "additionalProperties": false,
        }),
    )
}

pub fn spawn_agents() -> ToolSchema {
    ToolSchema::new(
        "spawn_agents",
        "Start independent worker objectives concurrently. Use short, long, or persistent lifetime as appropriate.",
        json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Independent objectives."
                },
                "lifetime_class": { "type": "string", "enum": ["short", "long", "persistent"], "default": "long" }
            },
            "required": ["tasks"],
            "additionalProperties": false,
        }),
    )
}

fn agent_control(name: &str, description: &str, needs_task: bool) -> ToolSchema {
    let mut properties = json!({
        "id": { "type": "string", "description": "The Tachyon agent id." }
    });
    if needs_task {
        properties["task"] = json!({
            "type": "string", "description": "The replacement durable objective."
        });
    }
    let mut required = vec!["id"];
    if needs_task {
        required.push("task");
    }
    ToolSchema::new(
        name,
        description,
        json!({
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        }),
    )
}

pub fn agent_inspect() -> ToolSchema {
    agent_control(
        "agent_inspect",
        "Inspect a worker's daemon-authoritative state.",
        false,
    )
}

pub fn agent_await() -> ToolSchema {
    agent_control(
        "agent_await",
        "Read a worker's current state without blocking the daemon.",
        false,
    )
}

pub fn agent_release() -> ToolSchema {
    agent_control(
        "agent_release",
        "Terminate, persist, and clean up a worker.",
        false,
    )
}

pub fn agent_retain() -> ToolSchema {
    ToolSchema::new(
        "agent_retain",
        "Retain a completed worker session and workspace for future related work. Retention is the default; optionally provide a Unix lease deadline.",
        json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "description": "The Tachyon agent id." },
                "lease_until_secs": { "type": "integer", "description": "Optional Unix timestamp at which retention may expire." },
                "lifetime_class": { "type": "string", "enum": ["short", "long", "persistent"], "description": "Optional promotion to a different lifetime policy." }
            },
            "required": ["id"],
            "additionalProperties": false,
        }),
    )
}

pub fn agent_stage() -> ToolSchema {
    ToolSchema::new(
        "agent_stage",
        "Place a worker in a visible grace period before termination so it can be retained before the deadline.",
        json!({
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "ttl_secs": { "type": "integer", "minimum": 1 }
            },
            "required": ["id", "ttl_secs"],
            "additionalProperties": false,
        }),
    )
}

pub fn agent_replan() -> ToolSchema {
    agent_control(
        "agent_replan",
        "Replace a worker with a new objective, retaining its identity and dependencies.",
        true,
    )
}
