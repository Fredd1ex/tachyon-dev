//! Optional durable services. Native and Python calls use this same typed boundary.
use crate::harness::{
    registry::manifest::{Manifest, Package},
    runtime::{Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tachyon_api::agents::{Control, Reply, Request};
use tachyon_model::{broker::BrokerClient, ToolSpec};

pub fn package(client: Arc<BrokerClient>, todo: bool) -> Package {
    let (name, interface, operations) = if todo {
        ("todo", "todo.list/add/update: daemon-owned plan records, not Work acceptance or evaluation. Default current_work; current_campaign requires a separate host grant. List first, follow next_cursor; add uses scope_revision, update uses record revision. Mutations require expected_revision and stable command_id; replay only identical arguments. After context reset query todo.list, never infer the plan from memory. Python: t = require('todo'); await t.list(); await t.add(title='Check evidence', expected_revision=0, command_id='check-1').", &["todo"][..])
    } else {
        ("monitor", "monitor.snapshot: read-only scoped durable observations with actual sample clocks. Default current_work; current_campaign requires a separate host grant. Unknown is not zero; native charged wall time is not CPU utilization. Funding is observation, never a budget override. No cancel or mutation. Python: m = require('monitor'); await m.snapshot().", &["monitor"][..])
    };
    let mut scopes = Vec::new();
    if client.controls.contains(&if todo {
        Control::Todo
    } else {
        Control::Monitor
    }) {
        scopes.push("current_work");
    }
    if client.controls.contains(&if todo {
        Control::TodoCampaign
    } else {
        Control::MonitorCampaign
    }) {
        scopes.push("current_campaign");
    }
    let mut properties = json!({"action":{"enum":if todo {vec!["list","add","update"]} else {vec!["snapshot"]}}, "scope":{"enum":scopes}, "limit":{"type":"integer","minimum":1,"maximum":100}});
    if todo {
        properties["limit"]["maximum"] = json!(8);
        properties["filter"] = json!({"type":"object","properties":{"status":{"enum":["pending","in_progress","blocked","completed","cancelled"]},"ids":{"type":"array","maxItems":100,"items":{"type":"string"}}},"additionalProperties":false});
        properties["cursor"] = json!({"type":"object","description":"Echo next_cursor unchanged."});
        properties["command_id"] = json!({"type":"string","minLength":1,"maxLength":256});
        properties["expected_revision"] = json!({"type":"integer","minimum":0});
        properties["id"] = json!({"type":"string","minLength":1,"maxLength":256});
        properties["title"] = json!({"type":"string","minLength":1,"maxLength":512});
        properties["description"] = json!({"type":"string","maxLength":16384});
        properties["status"] =
            json!({"enum":["pending","in_progress","blocked","completed","cancelled"]});
    } else {
        properties["after"] = json!({"type":"string","maxLength":512});
    }
    let mut parameters = json!({"type":"object","properties":properties,"required":["action"],"additionalProperties":false});
    if todo {
        parameters["allOf"] = json!([
            {"if":{"properties":{"action":{"const":"add"}}},"then":{"required":["command_id","expected_revision","title"]}},
            {"if":{"properties":{"action":{"const":"update"}}},"then":{"required":["command_id","expected_revision","id"]}}
        ]);
    }
    Package {
        manifest: Manifest {
            name,
            version: "1",
            description: if todo {
                "Durable scoped plans."
            } else {
                "Read-only scoped observations."
            },
            interface,
            usage: interface,
            operations,
        },
        tools: vec![Arc::new(Service {
            client,
            todo,
            schema: ToolSpec::new(
                name,
                if todo {
                    "List and revise durable plan records; not Work completion."
                } else {
                    "Read scoped monitor observations; no mutation or authority."
                },
                parameters,
            ),
        })],
    }
}
struct Service {
    client: Arc<BrokerClient>,
    todo: bool,
    schema: ToolSpec,
}
impl Tool for Service {
    fn name(&self) -> &'static str {
        if self.todo {
            "todo"
        } else {
            "monitor"
        }
    }
    fn schema(&self) -> &ToolSpec {
        &self.schema
    }
    fn capabilities(&self) -> &'static [Capability] {
        &[]
    }
    fn execute<'a>(&'a self, _: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let request = if self.todo {
                Request::Todo {
                    request: serde_json::from_value(input)
                        .map_err(|_| ToolError::invalid("invalid todo arguments"))?,
                }
            } else {
                Request::Monitor {
                    request: serde_json::from_value(input)
                        .map_err(|_| ToolError::invalid("invalid monitor arguments"))?,
                }
            };
            request.validate().map_err(ToolError::invalid)?;
            if !self.client.controls.contains(&request.control()) {
                return Err(ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "service scope denied",
                    false,
                ));
            }
            let reply = self.client.control(&request).await.map_err(|_| {
                ToolError::new(
                    ToolErrorCode::Io,
                    "private service failed; outcome may be unknown",
                    false,
                )
            })?;
            let value = match reply {
                Reply::Todo {
                    scope,
                    result: Ok(value),
                } => {
                    let mut value = serde_json::to_value(value).unwrap();
                    value["scope"] = serde_json::to_value(scope).unwrap();
                    value
                }
                Reply::Todo {
                    result: Err(error), ..
                } => {
                    use tachyon_api::todo::TodoError;
                    let code = match &error {
                        TodoError::RevisionConflict { .. }
                        | TodoError::CommandConflict
                        | TodoError::CursorStale => ToolErrorCode::Conflict,
                        TodoError::AuthorityDenied => ToolErrorCode::PermissionDenied,
                        TodoError::NotFound => ToolErrorCode::NotFound,
                        TodoError::Invalid { .. } => ToolErrorCode::InvalidInput,
                        TodoError::Storage { .. } => ToolErrorCode::Io,
                    };
                    let mut failure = ToolError::new(code, "todo request failed", false);
                    failure.metadata = match error {
                        TodoError::Storage { .. } => json!({"kind":"storage"}),
                        error => serde_json::to_value(error).unwrap(),
                    };
                    return Err(failure);
                }
                Reply::Monitor {
                    query,
                    result: Ok(payload),
                } => json!({"query":query,"payload":payload}),
                Reply::Monitor {
                    result: Err(error), ..
                } => {
                    let mut failure = ToolError::new(
                        ToolErrorCode::DependencyUnavailable,
                        "monitor sample unavailable",
                        true,
                    );
                    failure.metadata = json!({"kind":error});
                    return Err(failure);
                }
                _ => {
                    return Err(ToolError::new(
                        ToolErrorCode::PermissionDenied,
                        "service denied",
                        false,
                    ))
                }
            };
            Ok(ToolResult::success(value.to_string(), json!({})))
        })
    }
}
