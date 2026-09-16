//! Bounded live-output navigation and host-authorized durable descriptor retrieval.
use crate::harness::runtime::{
    decode_input, Capability, Tool, ToolContext, ToolError, ToolFuture, ToolOutputRef,
    ToolOutputStore, ToolRegistry, ToolResult,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tachyon_model::ToolSpec;

#[cfg(test)]
mod tests;

pub const INTERFACE: &str = "`ctx` lists live refs or reads/searches byte-cursor pages (8 KiB max). Follow next_cursor; an empty page is not exhaustive. Report discarded bytes/decoding/storage coverage gaps. Live refs expire at work end. With history authority, list/search(scope='current_work'|'campaign', kinds?, limit=1..16, cursor?) pages durable descriptors with string cursors. read(resource=ResourceRef, cursor=0, limit=1024) returns history's envelope; follow next_offset.";
pub const USAGE: &str = include_str!("usage.md");

pub struct CtxTool {
    schema: ToolSpec,
}
impl CtxTool {
    pub fn new() -> Self {
        Self {
            schema: ToolSpec::new(
                "ctx",
                "Navigate live output; with history authority, list/search scoped durable descriptors or read exact resources.",
                json!({
                    "type":"object", "properties": {
                        "action":{"enum":["read","list","search"]},
                        "reference":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false},
                        "resource":{"type":"object","properties":{"kind":{"enum":["attempt","finding","artifact","trace","document"]},"work_id":{"type":"string"},"id":{"type":"string"},"version":{"type":"string"}},"required":["kind","work_id","id","version"],"additionalProperties":false},
                        "scope":{"enum":["current_work","campaign"],"description":"Durable list/search only; requires host history authority. Omit scope and kinds for live navigation."},
                        "kinds":{"type":"array","minItems":1,"maxItems":5,"uniqueItems":true,"items":{"enum":["attempt","finding","artifact","trace","document"]}},
                        "cursor":{"oneOf":[{"type":"integer","minimum":0},{"type":"string","maxLength":1024}],"description":"Opaque string for scoped list/search; integer for live navigation or resource byte reads."},
                        "limit":{"type":"integer","minimum":1,"maximum":8192,"description":"Scoped list/search: 1..16 pre-filter items; resource read: 1..1024 bytes; live page: 1..8192 bytes."},
                        "query":{"type":"string","minLength":1,"maxLength":1024}
                    }, "required":["action"], "additionalProperties":false,
                    "if":{"anyOf":[{"required":["scope"]},{"required":["kinds"]}]},
                    "then":{"properties":{
                        "action":{"enum":["list","search"]},
                        "limit":{"maximum":16},
                        "cursor":{"type":"string","maxLength":1024},
                        "query":{"maxLength":256}
                    }}
                }),
            ),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    action: String,
    reference: Option<ToolOutputRef>,
    resource: Option<tachyon_api::context::ResourceRef>,
    cursor: Option<Cursor>,
    limit: Option<usize>,
    query: Option<String>,
    scope: Option<Scope>,
    kinds: Option<Vec<tachyon_api::context::ResourceKind>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Cursor {
    Live(usize),
    Durable(String),
}

#[derive(Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Scope {
    CurrentWork,
    Campaign,
}

impl Tool for CtxTool {
    fn name(&self) -> &'static str {
        "ctx"
    }
    fn schema(&self) -> &ToolSpec {
        &self.schema
    }
    fn capabilities(&self) -> &'static [Capability] {
        &[Capability::ReadFilesystem]
    }
    fn execute<'a>(&'a self, _: &'a ToolContext, _: Value) -> ToolFuture<'a> {
        Box::pin(async { Err(ToolError::invalid("ctx requires a per-work registry")) })
    }
    fn execute_with_registry<'a>(
        &'a self,
        context: &'a ToolContext,
        input: Value,
        registry: &'a ToolRegistry,
    ) -> ToolFuture<'a> {
        Box::pin(async move {
            let input: Input = decode_input(input)?;
            if input.scope.is_some() || input.kinds.is_some() {
                use tachyon_api::context::{Page, Query, Request};
                if !matches!(input.action.as_str(), "list" | "search")
                    || input.reference.is_some()
                    || input.resource.is_some()
                    || (input.action == "list" && input.query.is_some())
                    || (input.action == "search"
                        && input.query.as_ref().is_none_or(|s| s.is_empty()))
                {
                    return Err(ToolError::invalid(
                        "scoped retrieval requires list or search without a reference",
                    ));
                }
                if input.kinds.as_ref().is_some_and(|kinds| {
                    kinds.is_empty()
                        || kinds.len() > 5
                        || kinds
                            .iter()
                            .enumerate()
                            .any(|(i, kind)| kinds[..i].contains(kind))
                }) {
                    return Err(ToolError::invalid(
                        "kinds must contain 1..5 distinct resource kinds",
                    ));
                }
                let work = if input.scope == Some(Scope::Campaign) {
                    None
                } else {
                    Some(context.identity.work_id.as_ref().ok_or_else(|| {
                        ToolError::invalid("current_work retrieval needs host work identity")
                    })?)
                };
                let after = match input.cursor {
                    None => None,
                    Some(Cursor::Durable(cursor)) => Some(cursor),
                    _ => {
                        return Err(ToolError::invalid(
                            "scoped retrieval requires a history string cursor",
                        ))
                    }
                };
                let request = Request::Search {
                    query: Query {
                        literal: input.query,
                        after,
                        limit: input.limit.unwrap_or(16),
                        since_ms: None,
                        version: None,
                    },
                };
                request.validate().map_err(ToolError::invalid)?;
                // Exactly one host-bounded page: filtering never drains the campaign.
                let result = registry
                    .execute("history", context, serde_json::to_value(request).unwrap())
                    .await?;
                let mut page: Page = serde_json::from_str(&result.content)
                    .map_err(|_| ToolError::invalid("invalid history page"))?;
                page.resources.retain(|resource| {
                    work.is_none_or(|id| resource.reference.work_id == *id)
                        && input
                            .kinds
                            .as_ref()
                            .is_none_or(|kinds| kinds.contains(&resource.reference.kind))
                });
                let content = serde_json::to_string(&page)
                    .map_err(|_| ToolError::invalid("invalid history page"))?;
                if content.len() > tachyon_api::context::MAX_PAGE_BYTES {
                    return Err(ToolError::invalid("history page exceeds 8 KiB"));
                }
                return Ok(ToolResult::success(content, json!({})));
            }
            let cursor = match input.cursor {
                None => 0,
                Some(Cursor::Live(cursor)) => cursor,
                _ => return Err(ToolError::invalid("live/read cursor must be an integer")),
            };
            if let Some(resource) = input.resource {
                if input.action != "read" || input.reference.is_some() || input.query.is_some() {
                    return Err(ToolError::invalid(
                        "external resource requires read without a live reference or query",
                    ));
                }
                let request = tachyon_api::context::Request::Read {
                    resource,
                    offset: cursor as u64,
                    limit: input.limit.unwrap_or(1024),
                };
                request.validate().map_err(ToolError::invalid)?;
                // Reuse the host-advertised history adapter, including its policy and scope checks.
                return registry
                    .execute(
                        "history",
                        context,
                        serde_json::to_value(request)
                            .map_err(|_| ToolError::invalid("invalid resource"))?,
                    )
                    .await;
            }
            let outputs = registry.work_outputs()?;
            let outputs: &dyn ToolOutputStore = outputs.as_ref();
            if input.action == "list" {
                let references = outputs.references();
                if cursor > references.len() {
                    return Err(ToolError::invalid("cursor exceeds reference list"));
                }
                let end = (cursor + 32).min(references.len());
                return Ok(ToolResult::success(
                    String::new(),
                    json!({"references": &references[cursor..end], "next_cursor":end, "has_more":end < references.len(), "cursor_unit":"entries"}),
                ));
            }
            if !matches!(input.action.as_str(), "read" | "search") {
                return Err(ToolError::invalid("unknown ctx action"));
            }
            let reference = input
                .reference
                .ok_or_else(|| ToolError::invalid("reference is required"))?;
            if input.action == "search" {
                let query = input
                    .query
                    .ok_or_else(|| ToolError::invalid("query is required"))?;
                return outputs
                    .search_page(&reference, cursor, input.limit.unwrap_or(8192), &query)
                    .await;
            }
            outputs
                .page(&reference, cursor, input.limit.unwrap_or(8192))
                .await
        })
    }
}
