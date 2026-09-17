use crate::{capabilities::Capability, tools::ToolSchema};
use serde_json::json;

pub const CAPABILITIES: &[Capability] = &[Capability::Todo, Capability::Monitor];

// Narrow read capabilities, not the service's mutation surface. The host must
// independently grant and bind the campaign scope before executing either.
pub fn todo() -> ToolSchema {
    ToolSchema {
        name: "todo".into(),
        description: "Read a bounded page of campaign todos in the host-bound scope.".into(),
        parameters: json!({"type":"object","properties":{"operation":{"const":"list"},"limit":{"type":"integer","minimum":1,"maximum":20}},"required":["operation","limit"],"additionalProperties":false}),
    }
}

pub fn monitor() -> ToolSchema {
    ToolSchema {
        name: "monitor".into(),
        description:
            "Read resource observations in the host-bound campaign scope; never authorizes spend."
                .into(),
        parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
    }
}

pub fn primary() -> Vec<ToolSchema> {
    CAPABILITIES.iter().map(|cap| cap.schema()).collect()
}
