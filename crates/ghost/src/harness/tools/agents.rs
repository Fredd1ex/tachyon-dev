//! Only installed by private broker bootstrap, never by a local profile.
use crate::harness::{
    registry::manifest::{Manifest, Package},
    runtime::{Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tachyon_api::agents::{Control, Reply, Request};
use tachyon_model::{broker::BrokerClient, ToolSpec};

pub fn package(client: Arc<BrokerClient>) -> Package {
    let actions: Vec<_> = client
        .controls
        .iter()
        .copied()
        .filter(|c| {
            !matches!(
                c,
                Control::Resource
                    | Control::Todo
                    | Control::TodoCampaign
                    | Control::Monitor
                    | Control::MonitorCampaign
                    | Control::MonitorAvailability
            )
        })
        .collect();
    Package {
        manifest: Manifest {
            name: "agents", version: "1", description: "Permit-scoped host coordination.",
            interface: "agents(action, ...): templates(limit, after?) discovers parent-approved selectors, spawn(template_id, command_id), group(template_id, command_id, max_running?), status(work_id), list(limit, after?), result(work_id, revision?), send(work_id, command_id, text), steer(work_id, command_id, expected_revision, instructions). Python: a = require('agents'); await a.templates(limit=32); await a.spawn(template_id='host-approved-key', command_id='stable-id'). Spawn/group return durable queued handles, NOT completion. Send/steer are accepted, NOT applied. Reuse command_id only with identical payload. Only host-allowed actions are available.",
            usage: "spawn(template_id, command_id) admits one exact host-approved child; group(template_id, command_id, max_running?) admits an exact approved batch. Alternatively, with a host-approved dynamic profile: spawn(profile_id, objective, context_refs, command_id), or group(specs, command_id, max_running) where each spec has profile_id, objective, context_refs. Dynamic objectives are bounded untrusted tasks; context_refs are exact campaign-scoped evidence versions, not paths. Profile IDs must come from host instructions; templates lists fixed selectors only. No path, provider, permission or budget overrides. Returns durable queued work_ids, not completion; identical command replay returns the same handles. wait(work_ids, mode, timeout_ms) waits for direct children while releasing execution capacity, retaining this Python session. mode is 'all', 'any', or {'count': N}; timeout_ms is 1..300000. Returns completed/outstanding only after reacquiring capacity; completion means worker termination, not accepted verification. Timeout may return partial results. If resume cannot be acquired within five additional seconds, the host cancels this parent rather than continuing without a lease. cancel requests cleanup, not proof of termination. group_status/group_resize require direct ownership. Shrink drains.",
            operations: &["agents"],
        },
        tools: vec![Arc::new(Agents { client, actions: actions.clone(), schema: ToolSpec::new("agents", "Scoped Work coordination and exact host-approved child admission; queued handles are not completion.", json!({
            "type":"object", "properties": {
                "action":{"type":"string", "enum":actions},
                "work_id":{"type":"string", "minLength":1, "maxLength":256},
                "work_ids":{"type":"array", "minItems":1, "maxItems":64, "uniqueItems":true, "items":{"type":"string", "minLength":1, "maxLength":256}},
                "mode":{"oneOf":[{"enum":["all","any"]},{"type":"object","properties":{"count":{"type":"integer","minimum":1,"maximum":64}},"required":["count"],"additionalProperties":false}]},
                "timeout_ms":{"type":"integer", "minimum":1, "maximum":300000},
                "template_id":{"type":"string", "minLength":1, "maxLength":256},
                "profile_id":{"type":"string", "minLength":1, "maxLength":64},
                "objective":{"type":"string", "minLength":1, "maxLength":16384},
                "context_refs":{"type":"array", "maxItems":4, "items":{"type":"object","properties":{"kind":{"enum":["attempt","finding","artifact","trace","document"]},"work_id":{"type":"string"},"id":{"type":"string"},"version":{"type":"string"}},"required":["kind","work_id","id","version"],"additionalProperties":false}},
                "specs":{"type":"array", "minItems":1, "maxItems":16, "items":{"type":"object", "properties":{"profile_id":{"type":"string"},"objective":{"type":"string"},"context_refs":{"type":"array","maxItems":4,"items":{"type":"object","properties":{"kind":{"enum":["attempt","finding","artifact","trace","document"]},"work_id":{"type":"string"},"id":{"type":"string"},"version":{"type":"string"}},"required":["kind","work_id","id","version"],"additionalProperties":false}}},"required":["profile_id","objective","context_refs"],"additionalProperties":false}},
                "generation":{"type":"integer", "minimum":1},
                "group_id":{"type":"string", "minLength":1, "maxLength":256},
                "max_running":{"type":"integer", "minimum":0, "maximum":4096},
                "after":{"type":"string", "maxLength":256}, "limit":{"type":"integer", "minimum":1, "maximum":32},
                "revision":{"type":"integer", "minimum":0}, "command_id":{"type":"string", "minLength":1, "maxLength":256},
                "text":{"type":"string", "minLength":1, "maxLength":4096},
                "instructions":{"type":"string", "minLength":1, "maxLength":4096}, "expected_revision":{"type":"integer", "minimum":0}
            }, "required":["action"], "additionalProperties":false
        })) })],
    }
}

struct Agents {
    client: Arc<BrokerClient>,
    actions: Vec<Control>,
    schema: ToolSpec,
}
impl Tool for Agents {
    fn name(&self) -> &'static str {
        "agents"
    }
    fn schema(&self) -> &ToolSpec {
        &self.schema
    }
    fn capabilities(&self) -> &'static [Capability] {
        &[]
    }
    fn execute<'a>(&'a self, _: &'a ToolContext, mut input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            match input.get("action").and_then(Value::as_str) {
                Some("spawn") if input.get("profile_id").is_some() => {
                    input["action"] = json!("propose")
                }
                Some("group") if input.get("specs").is_some() => {
                    input["action"] = json!("propose_group")
                }
                Some("propose" | "propose_group") => {
                    return Err(ToolError::invalid("use spawn or group"))
                }
                _ => {}
            }
            let request: Request = serde_json::from_value(input).map_err(|_| {
                ToolError::invalid("invalid or unsupported agents action/arguments")
            })?;
            request.validate().map_err(ToolError::invalid)?;
            if !self.actions.contains(&request.control()) {
                return Err(ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "agents action denied",
                    false,
                ));
            }
            let reply = self.client.control(&request).await.map_err(|_| {
                ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "private control failed; outcome may be unknown",
                    false,
                )
            })?;
            if matches!(reply, Reply::Denied) {
                return Err(ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "agents action denied",
                    false,
                ));
            }
            Ok(ToolResult::success(
                serde_json::to_string(&reply).unwrap(),
                json!({}),
            ))
        })
    }
}
