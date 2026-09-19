//! Bounded, model-mediated web reports, not raw documents or a crawler API.
//! Hosts own policy, admission, per-turn counters and durable event identity.
use serde::{Deserialize, Serialize};

pub const WEBSEARCH: &str = "websearch";
pub const WEBFETCH: &str = "webfetch";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WebRequest {
    Search {
        query: String,
        #[serde(default)]
        domains: Option<Vec<String>>,
        #[serde(default = "default_results")]
        max_results: u8,
    },
    Fetch {
        urls: Vec<String>,
        #[serde(default)]
        instruction: Option<String>,
        /// Only absent/false is supported in phase 1. No crawler scope guarantee.
        #[serde(default)]
        follow_links: Option<bool>,
    },
}

fn default_results() -> u8 {
    3
}

impl WebRequest {
    /// Tool-facing arguments omit the transport discriminator and all host policy.
    pub fn tool_parameters(name: &str) -> Option<serde_json::Value> {
        use serde_json::json;
        Some(match name {
            WEBSEARCH => json!({"type":"object","properties":{
                "query":{"type":"string","minLength":1,"maxLength":4096},
                "domains":{"type":"array","minItems":1,"maxItems":16,"items":{"type":"string","minLength":1,"maxLength":253}},
                "max_results":{"type":"integer","minimum":1,"maximum":3,"default":3}
            },"required":["query"],"additionalProperties":false}),
            WEBFETCH => json!({"type":"object","properties":{
                "urls":{"type":"array","minItems":1,"maxItems":4,"items":{"type":"string","minLength":1,"maxLength":4096}},
                "instruction":{"type":"string","minLength":1,"maxLength":4096},
                "follow_links":{"type":"boolean","enum":[false]}
            },"required":["urls"],"additionalProperties":false}),
            _ => return None,
        })
    }

    pub fn from_tool_input(name: &str, mut input: serde_json::Value) -> Result<Self, &'static str> {
        let kind = match name {
            WEBSEARCH => "search",
            WEBFETCH => "fetch",
            _ => return Err("unknown web tool"),
        };
        let object = input
            .as_object_mut()
            .ok_or("web arguments must be an object")?;
        if object.contains_key("kind") {
            return Err("web kind is host-owned");
        }
        object.insert("kind".into(), kind.into());
        let request: Self = serde_json::from_value(input).map_err(|_| "invalid web arguments")?;
        request.validate()?;
        Ok(request)
    }

    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::Search { .. } => WEBSEARCH,
            Self::Fetch { .. } => WEBFETCH,
        }
    }

    /// Structural limits only. The model adapter additionally validates URLs.
    pub fn validate(&self) -> Result<(), &'static str> {
        let text = |s: &str, max| {
            !s.trim().is_empty() && s.len() <= max && !s.chars().any(char::is_control)
        };
        match self {
            Self::Search {
                query,
                domains,
                max_results,
            } => {
                if !text(query, 4096)
                    || !(1..=3).contains(max_results)
                    || domains.as_ref().is_some_and(|ds| {
                        ds.is_empty() || ds.len() > 16 || ds.iter().any(|d| !text(d, 253))
                    })
                {
                    return Err("invalid web search bounds");
                }
            }
            Self::Fetch {
                urls,
                instruction,
                follow_links,
            } => {
                if urls.is_empty()
                    || urls.len() > 4
                    || urls.iter().any(|u| !text(u, 4096))
                    || instruction.as_ref().is_some_and(|s| !text(s, 4096))
                    || *follow_links == Some(true)
                {
                    return Err("invalid web fetch bounds or unsupported link following");
                }
            }
        }
        Ok(())
    }
}

/// Correlation stays on the host; never include this envelope in provider prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebCommand {
    pub command_id: String,
    pub caller_id: String,
    pub tool_call_id: String,
    pub turn_id: String,
    pub request_id: String,
    pub request: WebRequest,
}

impl WebCommand {
    /// Hosts must additionally authenticate the caller and enforce per-turn limits.
    pub fn validate(&self) -> Result<(), &'static str> {
        for id in [
            &self.command_id,
            &self.caller_id,
            &self.tool_call_id,
            &self.turn_id,
            &self.request_id,
        ] {
            if id.trim().is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                return Err("invalid web correlation identity");
            }
        }
        self.request.validate()
    }
}

/// Host policy, deliberately not deserializable from worker tool arguments.
#[derive(Debug, Clone)]
pub struct WebLimits {
    pub max_tool_calls: u8,
    pub max_output_tokens: u32,
    /// Includes all SSE framing, reasoning, annotations and usage.
    pub max_response_bytes: usize,
    pub max_answer_bytes: usize,
    pub max_annotations: usize,
    pub allowed_domains: Vec<String>,
    pub blocked_domains: Vec<String>,
}

impl Default for WebLimits {
    fn default() -> Self {
        Self {
            max_tool_calls: 1,
            max_output_tokens: 2048,
            max_response_bytes: 262144,
            max_answer_bytes: 16384,
            max_annotations: 16,
            allowed_domains: vec![],
            blocked_domains: vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub url: String,
    pub title: Option<String>,
    pub excerpt: Option<String>,
    pub source_index: Option<u64>,
    /// Raw provider character offsets. Not UTF-8 byte indices; never slice with them.
    /// Units (code points versus UTF-16) are not guaranteed by the provider.
    pub start_index: Option<u64>,
    pub end_index: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebStatus {
    /// Positive provider use count or known citation, not per-URL fetch success.
    Grounded,
    Unverified,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebResult {
    /// Host receipt plus strict inclusive billing evidence. Missing is unknown.
    #[serde(default)]
    pub usage: WebUsage,
    pub answer: String,
    pub citations: Vec<Citation>,
    /// Only known url_citation fields, redacted and bounded. No arbitrary metadata.
    pub annotations: Vec<serde_json::Value>,
    pub status: WebStatus,
    pub notice: String,
    /// Host receipt time in Unix milliseconds, NOT source publication time.
    pub host_observed_at: u64,
    pub observed_search_uses: Option<u64>,
    /// Best-effort web_fetch_requests extension; absent means unaudited.
    pub observed_fetch_uses: Option<u64>,
    /// Original targets; phase 1 never rewrites URLs or retries a PDF as HTML.
    pub requested_urls: Vec<String>,
}

/// Observations for display/deduplication, never authority to spend or refund.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebUsage {
    pub receipt_id: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    /// Inclusive provider cost: never add server fees again.
    pub cost_micro_usd: Option<u64>,
}

impl WebUsage {
    pub fn valid(&self) -> bool {
        self.receipt_id.as_ref().is_none_or(|id| {
            !id.trim().is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
        }) && (self.input_tokens.is_some() == self.output_tokens.is_some())
            && (self.cost_micro_usd.is_none() || self.input_tokens.is_some())
            && self
                .input_tokens
                .zip(self.output_tokens)
                .is_none_or(|(input, output)| input.checked_add(output).is_some())
    }
}

/// Secret transport metadata, only delivered on a host-controlled assignment pipe.
/// Never persist this in Work/history or expose it as tool arguments.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceBootstrap {
    pub path: std::path::PathBuf,
    pub capability: [u8; 16],
    pub controls: Vec<crate::agents::Control>,
}

/// Transport envelope preserves WorkRequest's durable, non-secret identity.
#[derive(Serialize, Deserialize)]
pub struct WorkerAssignment {
    #[serde(flatten)]
    pub work: crate::WorkRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_service: Option<ServiceBootstrap>,
    /// Plain daemon agent turns retain their legacy result protocol.
    #[serde(default)]
    pub context_only: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tool_bindings_use_shared_requests_and_reject_transport_fields() {
        use serde_json::json;
        assert_eq!(
            WebRequest::from_tool_input(WEBSEARCH, json!({"query":"rust"})).unwrap(),
            WebRequest::Search {
                query: "rust".into(),
                domains: None,
                max_results: 3
            }
        );
        assert_eq!(
            WebRequest::from_tool_input(
                WEBFETCH,
                json!({"urls":["https://example.org/a.pdf?q=%2F"]})
            )
            .unwrap(),
            WebRequest::Fetch {
                urls: vec!["https://example.org/a.pdf?q=%2F".into()],
                instruction: None,
                follow_links: None
            }
        );
        for name in [WEBSEARCH, WEBFETCH] {
            assert_eq!(
                WebRequest::tool_parameters(name).unwrap()["additionalProperties"],
                false
            );
            for field in [
                "kind",
                "api_key",
                "provider",
                "engine",
                "command_id",
                "caller_id",
                "turn_id",
                "request_id",
                "limits",
            ] {
                let mut input = if name == WEBSEARCH {
                    json!({"query":"rust"})
                } else {
                    json!({"urls":["https://example.org"]})
                };
                input[field] = json!("forged");
                assert!(WebRequest::from_tool_input(name, input).is_err(), "{field}");
            }
        }
        assert!(WebRequest::tool_parameters("unknown").is_none());
        assert!(WebRequest::from_tool_input("unknown", json!({})).is_err());
    }

    #[test]
    fn search_needs_only_a_nonempty_query_including_short_names() {
        use serde_json::json;
        let schema = WebRequest::tool_parameters(WEBSEARCH).unwrap();
        assert_eq!(schema["required"], json!(["query"]));
        assert_eq!(schema["properties"]["query"]["minLength"], 1);
        assert!(schema["properties"].get("kind").is_none());
        for query in ["j", "je", "jev"] {
            assert_eq!(
                WebRequest::from_tool_input(WEBSEARCH, json!({"query":query})).unwrap(),
                WebRequest::Search {
                    query: query.into(),
                    domains: None,
                    max_results: 3,
                }
            );
        }
        for input in [json!({}), json!({"query":""}), json!({"query":"   "})] {
            assert!(WebRequest::from_tool_input(WEBSEARCH, input).is_err());
        }
    }

    #[test]
    fn defaults_and_worker_policy_boundary() {
        let request: WebRequest =
            serde_json::from_str(r#"{"kind":"search","query":"rust"}"#).unwrap();
        assert_eq!(request.tool_name(), WEBSEARCH);
        assert!(request.validate().is_ok());
        assert!(serde_json::from_str::<WebRequest>(
            r#"{"kind":"search","query":"rust","engine":"native"}"#
        )
        .is_err());
        let request = WebRequest::Fetch {
            urls: vec!["https://arxiv.org/pdf/1234.5678".into(); 5],
            instruction: None,
            follow_links: None,
        };
        assert!(request.validate().is_err());
    }
}
