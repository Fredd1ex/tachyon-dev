//! Ordered capability selection; schemas remain shared and host grants remain external.

use crate::capabilities::Capability;
use crate::tools::ToolSchema;
use serde_json::json;

pub const REVIEW_TOOL: &str = "submit_work_review";

pub fn primary() -> Vec<ToolSchema> {
    CAPABILITIES
        .iter()
        .map(|capability| capability.schema())
        .collect()
}

pub fn review() -> Vec<ToolSchema> {
    vec![review_tool()]
}

pub fn review_tool() -> ToolSchema {
    ToolSchema {
        name: REVIEW_TOOL.into(),
        description: "Submit one advisory semantic review of the supplied worker result.".into(),
        parameters: json!({
            "type": "object",
            "properties": {
                "decision": { "type": "string", "enum": ["accept", "rework"] },
                "lifecycle": {
                    "type": "string",
                    "enum": ["keep_current", "release", "retain_short", "retain_long", "retain_persistent"]
                },
                "revised_objective": { "type": "string" },
                "rationale": { "type": "string", "minLength": 1, "maxLength": 2000 }
            },
            "required": ["decision", "rationale"],
            "additionalProperties": false
        }),
    }
}

pub const CAPABILITIES: &[Capability] = &[
    Capability::DelegateOne,
    Capability::DelegateMany,
    Capability::InspectWorker,
    Capability::AwaitWorker,
    Capability::ReleaseWorker,
    Capability::RetainWorker,
    Capability::StageWorker,
    Capability::ReplanWorker,
];
