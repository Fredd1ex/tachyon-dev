//! Installed only when the private host advertises resource retrieval.
use crate::harness::{
    registry::manifest::{Manifest, Package},
    runtime::{Capability, Tool, ToolContext, ToolError, ToolErrorCode, ToolFuture, ToolResult},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tachyon_api::{
    agents::{Control, Reply, Request},
    context,
};
use tachyon_model::{broker::BrokerClient, ToolSpec};

pub fn package(client: Arc<BrokerClient>) -> Package {
    Package {
        manifest: Manifest {
            name: "history", version: "1", description: "Local permit-scoped research evidence, not curated memory.",
            interface: "history(action, query): search/attempts/findings/artifacts/traces/documents/snapshot with query={limit:1..16, literal?, after?, since_ms?, version?}. read(resource, offset, limit) reads an exact ResourceRef, up to 1024 bytes. Python: h = require('history'); await h.snapshot(query={'limit':8}). Snapshots are diagnostic, not kernel checkpoints or authority. Read-only, host-advertised, no campaign argument. Search matches descriptors, not raw bytes. Observations and authored findings are not proofs.",
            usage: include_str!("usage.md"), operations: &["history"],
        },
        tools: vec![Arc::new(History { client, schema: ToolSpec::new("history", "Retrieve bounded local research attempts, authored findings and Ready candidate artifacts in the host-authorized campaign.", json!({
            "type":"object", "properties": {
                "action":{"enum":["search","attempts","findings","artifacts","traces","documents","snapshot","read"]},
                "query":{"type":"object","properties":{
                    "literal":{"type":"string","maxLength":256}, "after":{"type":"string","maxLength":1024},
                    "limit":{"type":"integer","minimum":1,"maximum":16}, "since_ms":{"type":"integer","minimum":0},
                    "version":{"type":"string","maxLength":256}
                },"required":["limit"],"additionalProperties":false},
                "resource":{"type":"object","properties":{
                    "kind":{"enum":["attempt","finding","artifact","trace","document"]}, "work_id":{"type":"string","maxLength":256},
                    "id":{"type":"string","maxLength":256}, "version":{"type":"string","maxLength":256}
                },"required":["kind","work_id","id","version"],"additionalProperties":false},
                "offset":{"type":"integer","minimum":0}, "limit":{"type":"integer","minimum":1,"maximum":1024}
            },"required":["action"],"additionalProperties":false
        })) })],
    }
}

struct History {
    client: Arc<BrokerClient>,
    schema: ToolSpec,
}
impl Tool for History {
    fn name(&self) -> &'static str {
        "history"
    }
    fn schema(&self) -> &ToolSpec {
        &self.schema
    }
    fn capabilities(&self) -> &'static [Capability] {
        &[]
    }
    fn execute<'a>(&'a self, _: &'a ToolContext, input: Value) -> ToolFuture<'a> {
        Box::pin(async move {
            let denied = || {
                ToolError::new(
                    ToolErrorCode::PermissionDenied,
                    "history access denied",
                    false,
                )
            };
            if !self.client.controls.contains(&Control::Resource) {
                return Err(denied());
            }
            let request: context::Request = serde_json::from_value(input)
                .map_err(|_| ToolError::invalid("invalid history arguments"))?;
            request.validate().map_err(ToolError::invalid)?;
            let reply = self
                .client
                .control(&Request::Resource { request })
                .await
                .map_err(|_| denied())?;
            let Reply::Resource { page } = reply else {
                return Err(denied());
            };
            Ok(ToolResult::success(
                serde_json::to_string(&page)
                    .map_err(|_| ToolError::invalid("invalid history response"))?,
                json!({}),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn history_python_methods_are_generated_and_installation_is_not_permission() {
        use crate::harness::{
            backend::Local,
            profiles,
            runtime::{BrowserAvailability, ToolPolicy},
        };
        let root = tempfile::tempdir().unwrap();
        let mut policy = ToolPolicy::worker_default(root.path().into());
        let (_, mut client) = tachyon_model::broker::private_pair().unwrap();
        client.controls = vec![Control::Resource];
        let mut packages = profiles::worker(
            Arc::new(Local::new(root.path())),
            BrowserAvailability::Unavailable("test".into()),
        );
        packages.register(package(Arc::new(client))).unwrap();
        let installed = packages.into_registry();
        assert!(!installed
            .definitions(&policy)
            .iter()
            .any(|s| s.name == "history"));
        policy.enabled_tools.insert("history".into());
        let registry = installed
            .for_work(&policy, &[], &Default::default())
            .unwrap();
        let info = registry.python_require("history", &policy).unwrap();
        for action in [
            "search",
            "attempts",
            "findings",
            "artifacts",
            "traces",
            "documents",
            "snapshot",
            "read",
        ] {
            assert_eq!(
                info["methods"][action],
                json!({"tool":"history","input":{"action":action},"asynchronous":true})
            );
        }
        assert_eq!(info["methods"].as_object().unwrap().len(), 8);
        policy.enabled_tools.remove("history");
        assert!(registry.python_require("history", &policy).is_err());
    }
}
