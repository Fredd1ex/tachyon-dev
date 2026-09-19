//! Private, permit-bound coordination. These are not public daemon IPC requests.
use serde::{Deserialize, Serialize};
pub mod services;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    WebSearch,
    WebFetch,
    Templates,
    Resource,
    Todo,
    /// Explicit campaign-scope grants, never implied by Work membership.
    TodoCampaign,
    MonitorCampaign,
    /// Ancillary host aggregate capacity observations; no Host scope or registry IDs.
    MonitorAvailability,
    Monitor,
    Wait,
    Spawn,
    Group,
    Status,
    List,
    Result,
    Send,
    Steer,
    Cancel,
    GroupStatus,
    GroupResize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    WebSearch {
        command: crate::web::WebCommand,
    },
    WebFetch {
        command: crate::web::WebCommand,
    },
    Todo {
        request: services::TodoRequest,
    },
    Monitor {
        request: services::MonitorRequest,
    },
    Propose {
        profile_id: String,
        objective: String,
        context_refs: Vec<crate::context::ResourceRef>,
        command_id: String,
    },
    ProposeGroup {
        specs: Vec<Proposal>,
        command_id: String,
        max_running: usize,
    },
    Templates {
        after: Option<String>,
        limit: usize,
    },
    Resource {
        request: crate::context::Request,
    },
    Wait {
        work_ids: Vec<String>,
        mode: WaitMode,
        timeout_ms: u64,
    },
    /// Select one exact host-approved child; no objective or policy overrides.
    Spawn {
        template_id: String,
        command_id: String,
    },
    /// Select an exact host-approved batch, optionally lowering its running cap.
    Group {
        template_id: String,
        command_id: String,
        max_running: Option<usize>,
    },
    Cancel {
        work_id: String,
        generation: u64,
    },
    GroupStatus {
        group_id: String,
    },
    GroupResize {
        group_id: String,
        expected_revision: u64,
        max_running: usize,
    },
    Status {
        work_id: String,
    },
    List {
        after: Option<String>,
        limit: usize,
    },
    Result {
        work_id: String,
        revision: Option<u64>,
    },
    Send {
        work_id: String,
        command_id: String,
        text: String,
    },
    Steer {
        work_id: String,
        command_id: String,
        expected_revision: u64,
        instructions: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proposal {
    pub profile_id: String,
    pub objective: String,
    pub context_refs: Vec<crate::context::ResourceRef>,
}

impl Proposal {
    pub fn valid(&self) -> bool {
        !self.profile_id.is_empty()
            && self.profile_id.len() <= 64
            && !self.objective.trim().is_empty()
            && self.objective.len() <= 16384
            && !self.objective.contains('\0')
            && self.context_refs.len() <= 4
            && self
                .context_refs
                .iter()
                .all(crate::context::ResourceRef::valid)
            && self
                .context_refs
                .iter()
                .enumerate()
                .all(|(i, r)| !self.context_refs[..i].contains(r))
    }
}

impl Request {
    pub fn control(&self) -> Control {
        match self {
            Self::WebSearch { .. } => Control::WebSearch,
            Self::WebFetch { .. } => Control::WebFetch,
            Self::Todo { request } => match request.scope() {
                services::Scope::CurrentWork => Control::Todo,
                services::Scope::CurrentCampaign => Control::TodoCampaign,
            },
            Self::Monitor { request } => match request.scope() {
                services::Scope::CurrentWork => Control::Monitor,
                services::Scope::CurrentCampaign => Control::MonitorCampaign,
            },
            Self::Templates { .. } => Control::Templates,
            Self::Resource { .. } => Control::Resource,
            Self::Wait { .. } => Control::Wait,
            Self::Spawn { .. } | Self::Propose { .. } => Control::Spawn,
            Self::Group { .. } | Self::ProposeGroup { .. } => Control::Group,
            Self::Cancel { .. } => Control::Cancel,
            Self::GroupStatus { .. } => Control::GroupStatus,
            Self::GroupResize { .. } => Control::GroupResize,
            Self::Status { .. } => Control::Status,
            Self::List { .. } => Control::List,
            Self::Result { .. } => Control::Result,
            Self::Send { .. } => Control::Send,
            Self::Steer { .. } => Control::Steer,
        }
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        let id = |s: &str| !s.trim().is_empty() && s.len() <= 256;
        let text = |s: &str| !s.is_empty() && s.len() <= 4096;
        let valid = match self {
            Self::WebSearch { command } => {
                command.validate().is_ok()
                    && matches!(command.request, crate::web::WebRequest::Search { .. })
            }
            Self::WebFetch { command } => {
                command.validate().is_ok()
                    && matches!(command.request, crate::web::WebRequest::Fetch { .. })
            }
            Self::Todo {
                request: services::TodoRequest::List { limit, .. },
            } => limit.is_none_or(|n| (1..=8).contains(&n)),
            Self::Todo { .. } => true, // Durable facade validates mutation and cursor bounds.
            Self::Monitor {
                request: services::MonitorRequest::Snapshot { after, limit, .. },
            } => {
                (1..=crate::monitor::MAX_PAGE).contains(limit)
                    && after.as_deref().is_none_or(|s| {
                        !s.is_empty() && s.len() <= 512 && !s.chars().any(char::is_control)
                    })
            }
            Self::Propose {
                profile_id,
                objective,
                context_refs,
                command_id,
            } => {
                id(command_id)
                    && Proposal {
                        profile_id: profile_id.clone(),
                        objective: objective.clone(),
                        context_refs: context_refs.clone(),
                    }
                    .valid()
            }
            Self::ProposeGroup {
                specs,
                command_id,
                max_running,
            } => {
                id(command_id)
                    && (1..=16).contains(&specs.len())
                    && specs.iter().all(Proposal::valid)
                    && (1..=64).contains(max_running)
            }
            Self::Templates { after, limit } => {
                (1..=32).contains(limit) && after.as_deref().is_none_or(id)
            }
            Self::Resource { request } => request.validate().is_ok(),
            Self::Wait {
                work_ids,
                mode,
                timeout_ms,
            } => {
                (1..=64).contains(&work_ids.len())
                    && work_ids.iter().all(|s| id(s))
                    && work_ids
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == work_ids.len()
                    && (1..=300_000).contains(timeout_ms)
                    && !matches!(mode, WaitMode::Count(n) if *n == 0 || *n > work_ids.len())
            }
            Self::Spawn {
                template_id,
                command_id,
            } => id(template_id) && id(command_id),
            Self::Group {
                template_id,
                command_id,
                max_running,
            } => {
                id(template_id)
                    && id(command_id)
                    && max_running.is_none_or(|n| (1..=4096).contains(&n))
            }
            Self::Cancel {
                work_id,
                generation,
            } => id(work_id) && *generation > 0,
            Self::GroupStatus { group_id } => id(group_id),
            Self::GroupResize {
                group_id,
                max_running,
                ..
            } => id(group_id) && *max_running <= 4096,
            Self::Status { work_id } | Self::Result { work_id, .. } => id(work_id),
            Self::List { after, limit } => {
                (1..=32).contains(limit) && after.as_deref().is_none_or(id)
            }
            Self::Send {
                work_id,
                command_id,
                text: body,
            } => id(work_id) && id(command_id) && text(body),
            Self::Steer {
                work_id,
                command_id,
                instructions,
                ..
            } => id(work_id) && id(command_id) && text(instructions),
        };
        if valid {
            Ok(())
        } else {
            Err("invalid agents bounds")
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    All,
    Any,
    Count(usize),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Status {
    pub work_id: String,
    pub parent: Option<String>,
    pub admission: AdmissionState,
    pub generation: u64,
    pub execution_revision: Option<u64>,
    pub accepted_revision: u64,
    pub acknowledged_revision: u64,
    pub delivered_revision: u64,
    pub delivery_cursor: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultSnapshot {
    pub work_id: String,
    pub revision: u64,
    pub generation: u64,
    pub current: bool,
    pub phase: ResultPhase,
    pub has_candidate: bool,
    pub settled: bool,
    #[serde(default)]
    pub research: Option<ResearchResult>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchResult {
    pub availability: String,
    pub outcome: Option<String>,
    pub summary: Option<String>,
    pub truncated: bool,
    pub evidence_refs: Vec<crate::context::ResourceRef>,
    pub candidate_refs: Option<Vec<String>>,
    pub unresolved_questions: Option<Vec<String>>,
    pub accepted_revision: u64,
    pub applied_revision: u64,
    pub delivered_revision: u64,
    pub work_usage: Option<ResultUsage>,
    pub verification_usage: Option<ResultUsage>,
}

/// Exact sums of authoritative Final request records, scoped to one allocation.
/// Decimal strings preserve u128 totals through JSON and Python/JS consumers.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultUsage {
    pub input_tokens: String,
    pub output_tokens: String,
    pub cost_micro_usd: String,
    pub uncertain: bool,
}

impl ResultSnapshot {
    /// Leave room for both the agents reply and broker frame envelopes.
    pub fn bound(&mut self) {
        while serde_json::to_vec(self).map_or(true, |bytes| bytes.len() > 3900) {
            let Some(r) = self.research.as_mut() else {
                break;
            };
            r.truncated = true;
            if r.evidence_refs.pop().is_some() {
                continue;
            }
            if r.candidate_refs
                .as_mut()
                .is_some_and(|refs| refs.pop().is_some())
            {
                continue;
            }
            if r.summary.as_mut().is_some_and(|s| s.pop().is_some()) {
                continue;
            }
            self.research = None;
        }
    }

    pub fn valid(&self) -> bool {
        serde_json::to_vec(self).is_ok_and(|bytes| bytes.len() <= 3900)
            && self.research.as_ref().is_none_or(|r| {
                r.summary.as_ref().is_none_or(|s| s.len() <= 2048)
                    && r.evidence_refs.len() <= 8
                    && r.evidence_refs
                        .iter()
                        .all(|reference| reference.valid() && reference.work_id == self.work_id)
                    && r.candidate_refs.as_ref().is_none_or(|refs| {
                        refs.len() <= 8 && refs.iter().all(|id| !id.is_empty() && id.len() <= 256)
                    })
                    && r.unresolved_questions
                        .as_ref()
                        .is_none_or(|q| q.len() <= 8 && q.iter().all(|s| s.len() <= 2048))
                    && [&r.work_usage, &r.verification_usage].iter().all(|usage| {
                        usage.as_ref().is_none_or(|u| {
                            [&u.input_tokens, &u.output_tokens, &u.cost_micro_usd]
                                .iter()
                                .all(|s| s.parse::<u128>().is_ok_and(|n| n.to_string() == **s))
                        })
                    })
            })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionState {
    Admitted,
    DispatchingUnknown,
    Registered,
    ConfirmedUnspent,
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultPhase {
    AwaitingAcceptance,
    AcceptedHuman,
    ReworkPending,
    ExecutingUnknown,
    EvidenceReady,
    AwaitingVerification,
    ReviewingUnknown,
    Accepted,
    Rejected,
    Unverified,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    WebError {
        command: crate::web::WebCommand,
        message: String,
    },
    WebSearch {
        command: crate::web::WebCommand,
        result: crate::web::WebResult,
    },
    WebFetch {
        command: crate::web::WebCommand,
        result: crate::web::WebResult,
    },
    Todo {
        scope: crate::todo::TodoScope,
        result: Result<crate::todo::TodoResponse, crate::todo::TodoError>,
    },
    Monitor {
        query: crate::monitor::MonitorQuery,
        result: Result<crate::monitor::MonitorPayload, crate::monitor::MonitorError>,
    },
    Templates {
        templates: Vec<Template>,
        next_cursor: Option<String>,
    },
    Resource {
        page: crate::context::Page,
    },
    /// Returned only after the host reacquires the parent's execution lease.
    Wait {
        completed: Vec<String>,
        outstanding: Vec<String>,
        resumed: bool,
        resource_blocked: bool,
    },
    /// Durable admission receipt, not completion or a promise of immediate launch.
    Admitted {
        command_id: String,
        work_ids: Vec<String>,
        group_id: Option<String>,
    },
    CancellationRequested {
        work_id: String,
        generation: u64,
    },
    Group {
        group_id: String,
        revision: u64,
        max_running: usize,
        active: usize,
        total: usize,
        cancellation_requested: bool,
    },
    Status {
        status: Status,
    },
    List {
        work_ids: Vec<String>,
        next_cursor: Option<String>,
    },
    Result {
        snapshot: Option<ResultSnapshot>,
    },
    Accepted {
        command_id: String,
        sequence: u64,
        accepted_revision: u64,
    },
    Denied,
}

/// Discovery contains selectors and bounds, never execution paths or model policy.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub template_id: String,
    pub group_id: Option<String>,
    pub work_count: usize,
    pub max_running: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn result_legacy_defaults_and_utf8_wire_budget() {
        let mut snapshot: ResultSnapshot = serde_json::from_value(json!({
            "work_id":"child", "revision":1, "generation":1, "current":true,
            "phase":"accepted", "has_candidate":true, "settled":true
        }))
        .unwrap();
        assert!(snapshot.research.is_none());
        snapshot.research = Some(ResearchResult {
            availability: "available".into(),
            summary: Some("\u{0}\u{e9}".repeat(682)),
            candidate_refs: Some(vec!["x".repeat(256); 8]),
            work_usage: Some(ResultUsage {
                input_tokens: u128::MAX.to_string(),
                output_tokens: "0".into(),
                cost_micro_usd: u128::MAX.to_string(),
                uncertain: false,
            }),
            ..Default::default()
        });
        snapshot.bound();
        assert!(snapshot.valid());
        assert!(snapshot.research.as_ref().unwrap().truncated);
        assert!(
            serde_json::to_vec(&Reply::Result {
                snapshot: Some(snapshot)
            })
            .unwrap()
            .len()
                < 4096
        );
    }

    #[test]
    fn wait_modes_and_bounds_are_typed() {
        for mode in [json!("all"), json!("any"), json!({"count":1})] {
            let request: Request = serde_json::from_value(
                json!({"action":"wait","work_ids":["child"],"mode":mode,"timeout_ms":1}),
            )
            .unwrap();
            assert_eq!(request.control(), Control::Wait);
            request.validate().unwrap();
        }
        for (ids, mode, timeout) in [
            (json!([]), json!("all"), 1),
            (json!(["a", "a"]), json!("all"), 1),
            (json!(["a"]), json!({"count":0}), 1),
            (json!(["a"]), json!({"count":2}), 1),
            (json!(["a"]), json!("any"), 0),
            (json!(["a"]), json!("all"), 300001),
        ] {
            let request: Request = serde_json::from_value(
                json!({"action":"wait","work_ids":ids,"mode":mode,"timeout_ms":timeout}),
            )
            .unwrap();
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn typed_controls_reject_authority_lifecycle_and_byte_overflow() {
        for input in [
            r#"{"action":"status","action":"send","work_id":"w"}"#,
            r#"{"action":"spawn","template_id":"a","template_id":"b","command_id":"c"}"#,
            r#"{"action":"status","work_id":"w","work_id":"other"}"#,
            r#"{"action":"send","work_id":"w","command_id":"a","command_id":"b","text":"hello"}"#,
        ] {
            assert!(serde_json::from_str::<Request>(input).is_err());
        }
        for input in [
            json!({"action":"spawn"}),
            json!({"action":"wait"}),
            json!({"action":"cancel"}),
            json!({"action":"group"}),
            json!({"action":"spawn","template_id":"approved","command_id":"c","objective":"invented"}),
            json!({"action":"group","template_id":"approved","command_id":"c","work_ids":["invented"]}),
            json!({"action":"status","work_id":"w","campaign_id":"forged"}),
            json!({"action":"send","work_id":"w","command_id":"id","text":"hello","sender":"forged"}),
            json!({"action":"list","limit":-1}),
        ] {
            assert!(serde_json::from_value::<Request>(input).is_err());
        }
        for text in ["x".repeat(4097), "\u{e9}".repeat(2049)] {
            assert!(Request::Send {
                work_id: "w".into(),
                command_id: "id".into(),
                text
            }
            .validate()
            .is_err());
        }
        assert!(Request::Send {
            work_id: "w".into(),
            command_id: "id".into(),
            text: "x".repeat(4096)
        }
        .validate()
        .is_ok());
        for limit in [0, 33, usize::MAX] {
            assert!(Request::List { after: None, limit }.validate().is_err());
        }
        for key in [
            "objective",
            "context_refs",
            "budget",
            "provider",
            "path",
            "parent",
            "campaign_id",
        ] {
            let mut input = json!({"action":"spawn","template_id":"approved","command_id":"c"});
            input[key] = json!("forged");
            assert!(serde_json::from_value::<Request>(input).is_err());
        }
        for n in [0, 4097, usize::MAX] {
            assert!(Request::Group {
                template_id: "approved".into(),
                command_id: "c".into(),
                max_running: Some(n)
            }
            .validate()
            .is_err());
        }
    }
}
