//! Provider-neutral schemas for role-owned capabilities.

use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
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
                "lifetime_class": {
                    "type": "string",
                    "enum": ["short", "long", "persistent"],
                    "default": "short",
                    "description": "short: up to 3 assignments; long: daemon lifetime; persistent: reattaches after restart"
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
        "Fan out independent work to materially cut latency, including same-method lists; group dependent or strongly shared-state work.",
        json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "maxItems": 8,
                    "items": { "type": "string" }
                },
                "lifetime_class": {
                    "type": "string",
                    "enum": ["short", "long", "persistent"],
                    "default": "short",
                    "description": "short: up to 3 assignments; long: daemon lifetime; persistent: reattaches after restart"
                }
            },
            "required": ["tasks"],
            "additionalProperties": false,
        }),
    )
}

pub fn memory() -> ToolSchema {
    ToolSchema::new(
        "memory",
        "Consult authoritative durable user memory when profile context is relevant, or apply one user-requested durable memory change. Never answer a durable user-profile question from conversation text alone. Recall before forget or correct so returned IDs can be used exactly.",
        json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["recall", "remember", "forget", "correct"] },
                "query": { "type": "string", "minLength": 1, "maxLength": 1000 },
                "include_history": { "type": "boolean", "description": "Whether prior conversation and task history are relevant to this recall." },
                "target_ids": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 8,
                    "uniqueItems": true,
                    "items": { "type": "string" }
                },
                "value": { "type": "string", "minLength": 1, "maxLength": 1000 },
                "kind": { "type": "string", "enum": ["fact", "preference", "constraint", "goal", "routine", "relationship"] },
                "namespace": { "type": "string", "minLength": 1, "maxLength": 100 },
                "relation": { "type": "string", "minLength": 1, "maxLength": 100 },
                "scope": { "type": "string", "minLength": 1, "maxLength": 100 },
                "cardinality": { "type": "string", "enum": ["one", "many"] },
                "topics": {
                    "type": "array",
                    "maxItems": 8,
                    "items": { "type": "string", "minLength": 1, "maxLength": 50 }
                }
            },
            "required": ["action"],
            "oneOf": [
                {
                    "properties": { "action": { "const": "recall" } },
                    "required": ["action", "query", "include_history"]
                },
                {
                    "properties": { "action": { "const": "remember" } },
                    "required": ["action", "value", "kind", "namespace", "relation", "scope", "cardinality"]
                },
                {
                    "properties": { "action": { "const": "forget" } },
                    "required": ["action", "target_ids"]
                },
                {
                    "properties": { "action": { "const": "correct" } },
                    "required": ["action", "target_ids", "value", "kind", "namespace", "relation", "scope", "cardinality"]
                }
            ],
            "additionalProperties": false,
        }),
    )
}

pub fn schedule() -> ToolSchema {
    ToolSchema::new(
        "schedule",
        "Manage durable reminders and future agent work. Preserve timing from clarification follow-ups; do not spawn future work immediately. Use start_at for 'at', finish_by for 'by', and day next when no date is given.",
        json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["create", "list", "cancel", "start_at", "finish_by"] },
                "text": { "type": "string", "minLength": 1, "maxLength": 500 },
                "objective": { "type": "string", "minLength": 1, "maxLength": 4000 },
                "delay_seconds": { "type": "integer", "minimum": 1, "maximum": 31536000 },
                "local_time": { "type": "string", "pattern": "^([01][0-9]|2[0-3]):[0-5][0-9]$" },
                "day": { "type": "string", "enum": ["next", "today", "tomorrow"] },
                "id": { "type": "string", "minLength": 1, "maxLength": 200 }
            },
            "required": ["action"],
            "oneOf": [
                {
                    "properties": { "action": { "const": "create" } },
                    "required": ["action", "text", "delay_seconds"]
                },
                {
                    "properties": { "action": { "const": "create" } },
                    "required": ["action", "text", "local_time", "day"]
                },
                {
                    "properties": { "action": { "const": "list" } },
                    "required": ["action"]
                },
                {
                    "properties": { "action": { "const": "cancel" } },
                    "required": ["action", "id"]
                },
                {
                    "properties": { "action": { "const": "start_at" } },
                    "required": ["action", "objective"]
                },
                {
                    "properties": { "action": { "const": "finish_by" } },
                    "required": ["action", "objective"]
                }
            ],
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
        let schemas = [spawn_agent(), spawn_agents(), memory(), schedule()];
        let chars = schemas
            .iter()
            .map(|schema| {
                schema.name.len() + schema.description.len() + schema.parameters.to_string().len()
            })
            .sum::<usize>();
        assert!(chars < 3_800, "conversation schemas grew to {chars} chars");
    }

    #[test]
    fn memory_schema_is_contextual_strict_and_bounded() {
        let schema = memory();
        assert!(schema
            .description
            .contains("authoritative durable user memory"));
        assert!(schema.description.contains("Never answer"));
        assert_eq!(schema.parameters["additionalProperties"], false);
        assert_eq!(schema.parameters["properties"]["target_ids"]["maxItems"], 8);
        assert_eq!(schema.parameters["properties"]["topics"]["maxItems"], 8);
        assert_eq!(schema.parameters["oneOf"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn schedule_schema_is_strict_and_bounded() {
        let schema = schedule();
        assert_eq!(schema.parameters["additionalProperties"], false);
        assert_eq!(
            schema.parameters["properties"]["delay_seconds"]["minimum"],
            1
        );
        assert_eq!(schema.parameters["oneOf"].as_array().unwrap().len(), 6);
    }

    #[test]
    fn spawn_agents_schema_bounds_material_fanout() {
        let schema = spawn_agents();
        assert_eq!(schema.parameters["properties"]["tasks"]["maxItems"], 8);
        assert!(schema.description.contains("independent work"));
        assert!(schema.description.contains("same-method lists"));
        assert!(schema.description.contains("strongly shared-state"));
        assert!(!schema.description.contains("heterogeneous"));
    }
}
