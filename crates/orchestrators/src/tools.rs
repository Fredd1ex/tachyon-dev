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
        "Delegate one objective requiring fresh work.",
        json!({
            "type": "object",
            "properties": {
                "task": { "type": "string" },
                "cwd": { "type": "string" },
                "lifetime_class": {
                    "type": "string",
                    "enum": ["short", "long", "persistent"],
                    "default": "short",
                    "description": "short is reusable for at most 3 assignments, long lasts until this daemon exits, persistent survives daemon exit and is reattached on restart"
                },
                "purpose": { "type": "string" }
            },
            "required": ["task"],
            "additionalProperties": false,
        }),
    )
}

pub fn respond() -> ToolSchema {
    ToolSchema::new(
        "respond",
        "Return the final response when no fresh retrieval or execution is required.",
        json!({
            "type": "object",
            "properties": {
                "response": { "type": "string" }
            },
            "required": ["response"],
            "additionalProperties": false,
        }),
    )
}

pub fn spawn_agents() -> ToolSchema {
    ToolSchema::new(
        "spawn_agents",
        "Delegate independent objectives concurrently.",
        json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "lifetime_class": {
                    "type": "string",
                    "enum": ["short", "long", "persistent"],
                    "default": "short",
                    "description": "short is reusable for at most 3 assignments, long lasts until this daemon exits, persistent survives daemon exit and is reattached on restart"
                }
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
        "Retain a verified completed worker and optionally promote or demote its lifecycle as the task evolves. Short allows 3 more assignments, long lasts until daemon exit, and persistent is reattached after daemon restart.",
        json!({
            "type": "object",
            "properties": {
                "id": { "type": "string", "description": "The Tachyon agent id." },
                "lease_until_secs": { "type": "integer", "description": "Optional Unix timestamp at which retention may expire." },
                "lifetime_class": { "type": "string", "enum": ["short", "long", "persistent"], "description": "Optional promotion or demotion to a different lifetime policy." }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_schema_budget_stays_compact() {
        let schemas = [spawn_agent(), spawn_agents()];
        let chars = schemas
            .iter()
            .map(|schema| {
                schema.name.len() + schema.description.len() + schema.parameters.to_string().len()
            })
            .sum::<usize>();
        assert!(chars < 900, "conversation schemas grew to {chars} chars");
    }
}
