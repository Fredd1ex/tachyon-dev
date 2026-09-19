//! Ordered capability selection; schemas remain shared and host grants remain external.

use crate::capabilities::Capability;

pub const CAPABILITIES: &[Capability] = &[
    Capability::DelegateOne,
    Capability::DelegateMany,
    Capability::Memory,
    Capability::Schedule,
    Capability::Todo,
    Capability::ConversationCampaign,
    Capability::WebSearch,
    Capability::WebFetch,
];

pub const DELEGATION_CAPABILITIES: &[Capability] =
    &[Capability::DelegateOne, Capability::DelegateMany];

pub fn primary() -> Vec<crate::tools::ToolSchema> {
    CAPABILITIES
        .iter()
        .map(|capability| match capability {
            Capability::Todo => todo(),
            _ => capability.schema(),
        })
        .collect()
}

pub fn todo() -> crate::tools::ToolSchema {
    use serde_json::json;
    crate::tools::ToolSchema {
        name: "todo".into(),
        description: "Durable conversation todos. For plans or next steps, list relevant todos as needed; no automatic plan loading. Scope is host-bound to this conversation, not work or campaign. List returns IDs and revisions; add requires scope revision, update requires record revision. On conflict list again and use a new command_id for a revised mutation. Completing a todo does not accept Work or create a campaign.".into(),
        parameters: json!({
            "type":"object",
            "properties": {
                "operation":{"enum":["list","add","update"]},
                "scope":{"type":"string","enum":["current_conversation"]},
                "filter":{"type":"object","properties":{
                    "status":{"enum":["pending","in_progress","blocked","completed","cancelled"]},
                    "ids":{"type":"array","items":{"type":"string"},"maxItems":100}
                },"additionalProperties":false},
                "limit":{"type":"integer","minimum":1,"maximum":100},
                "cursor":{"type":"object"},
                "command_id":{"type":"string","minLength":1,"maxLength":256},
                "expected_revision":{"type":"integer","minimum":0},
                "id":{"type":"string"},
                "title":{"type":"string","minLength":1,"maxLength":512},
                "description":{"type":"string","maxLength":16384},
                "status":{"enum":["pending","in_progress","blocked","completed","cancelled"]}
            },
            "required":["operation"],"additionalProperties":false,
            "oneOf":[
                {"properties":{"operation":{"const":"list"}},"not":{"anyOf":[{"required":["command_id"]},{"required":["expected_revision"]},{"required":["id"]},{"required":["title"]},{"required":["description"]},{"required":["status"]}]}},
                {"properties":{"operation":{"const":"add"}},"required":["command_id","expected_revision","title"],"not":{"anyOf":[{"required":["filter"]},{"required":["limit"]},{"required":["cursor"]},{"required":["id"]},{"required":["status"]}]}},
                {"properties":{"operation":{"const":"update"}},"required":["command_id","expected_revision","id"],"anyOf":[{"required":["title"]},{"required":["description"]},{"required":["status"]}],"not":{"anyOf":[{"required":["filter"]},{"required":["limit"]},{"required":["cursor"]}]}}
            ]
        }),
    }
}

pub fn web(fetch: bool) -> crate::tools::ToolSchema {
    crate::tools::ToolSchema {
        name: if fetch { "webfetch" } else { "websearch" }.into(),
        description: format!("{} Returns a bounded model-mediated report with citations, not verified source text. Inspect status and notice; receipt time is not publication time. A retrieval error is not an empty search or evidence that a subject does not exist. Explain the service limitation without speculating about the subject or asking for details to repair a failed service. Respect retry and budget denials. No kind argument, browser, crawling, or link following.", if fetch {
            "Prefer webfetch over browser retrieval for known public URLs. Only urls is required (1-4); instruction and follow_links (false only) are optional."
        } else {
            "Prefer websearch for unknown or uncertain facts before requesting identifying details from the user. Only a nonempty query is required; short names are valid. domains and max_results (1-3, default 3) are optional."
        }),
        parameters: tachyon_api::web::WebRequest::tool_parameters(if fetch { "webfetch" } else { "websearch" }).unwrap(),
    }
}

#[cfg(test)]
mod web_guidance_tests {
    #[test]
    fn retrieval_failure_is_not_negative_source_evidence() {
        for fetch in [false, true] {
            let schema = super::web(fetch);
            assert!(schema
                .description
                .contains("A retrieval error is not an empty search"));
            assert!(schema
                .description
                .contains("Respect retry and budget denials"));
            assert_eq!(schema.parameters["additionalProperties"], false);
        }
    }
}

pub fn campaign() -> crate::tools::ToolSchema {
    use serde_json::json;
    crate::tools::ToolSchema {
        name: "campaign".into(),
        description: "Inspect or steer already authorized campaigns explicitly linked to this conversation. List first for exact campaign IDs, status for exact Work IDs, revisions and generations. Never infer groups from names or invent revised objectives. Steer only the user's requested instructions within existing scope; cancel targets an exact Work branch. No creation, spending grants, resizing or budget transfers. Include the attached plan only when asked. Use a separate command_id per mutation; retry identical payload with the same ID, but after a revision conflict query status and use a new ID. Accepted is NOT applied: do not say Done until status shows applied_revision >= the receipt's accepted_revision, or cancellation_done. A cancellation request is only intent until cleanup finishes.".into(),
        parameters: json!({"type":"object","properties":{
            "operation":{"enum":["list","status","steer","cancel"]},
            "campaign_id":{"type":"string"},"work_id":{"type":"string"},
            "command_id":{"type":"string","minLength":1,"maxLength":256},
            "expected_revision":{"type":"integer","minimum":0},
            "generation":{"type":"integer","minimum":1},
            "instructions":{"type":"string","minLength":1,"maxLength":4096},
            "after":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":32},
            "include_plan":{"type":"boolean"}
        },"required":["operation"],"additionalProperties":false,
        "oneOf":[
            {"properties":{"operation":{"const":"list"}},"maxProperties":1},
            {"properties":{"operation":{"const":"status"}},"required":["campaign_id"],"not":{"anyOf":[{"required":["command_id"]},{"required":["expected_revision"]},{"required":["generation"]},{"required":["instructions"]}]}},
            {"properties":{"operation":{"const":"steer"}},"required":["campaign_id","work_id","command_id","expected_revision","instructions"],"maxProperties":6},
            {"properties":{"operation":{"const":"cancel"}},"required":["campaign_id","work_id","command_id","generation"],"maxProperties":5}
        ]}),
    }
}
