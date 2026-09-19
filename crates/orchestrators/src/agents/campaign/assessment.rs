//! Shared preparation and validation. The caller owns provider execution,
//! accounting, freshness checks and publication; this module grants no authority.
use super::progress::*;
use crate::{
    capabilities::Capability,
    registry::{HostLane, InvocationContext, InvocationKind, OutputVisibility, Registry, RoleId},
};
use tachyon_api::{
    campaign_oversight::*,
    monitor::{MonitorScope, Observed},
    todo::{TodoResponse, TodoScope, TodoStatus as ApiStatus},
};

pub struct PreparedAssessment {
    pub snapshot: CampaignSnapshot,
    pub system: String,
    pub input: String,
}

pub fn prepare(
    request: &CampaignAssessmentRequest,
    grants: &[Capability],
    registry: &Registry<'_>,
) -> Result<PreparedAssessment, CampaignAssessmentError> {
    let snapshot = snapshot(request)?;
    let resolved = registry
        .resolve(
            RoleId::Campaign,
            HostLane::Background,
            InvocationKind::Primary,
        )
        .map_err(|_| CampaignAssessmentError::RegistryUnavailable)?;
    if ![Capability::Todo, Capability::Monitor]
        .iter()
        .all(|cap| grants.contains(cap) && resolved.descriptor().capabilities.contains(cap))
        || resolved
            .descriptor()
            .capabilities
            .iter()
            .any(|cap| !grants.contains(cap))
    {
        return Err(CampaignAssessmentError::MissingCapabilities);
    }
    let rendered = resolved
        .render(InvocationContext::default())
        .map_err(|_| CampaignAssessmentError::RegistryUnavailable)?;
    if rendered.output_visibility != OutputVisibility::Internal
        || rendered.tools != super::tools::primary()
    {
        return Err(CampaignAssessmentError::RegistryUnavailable);
    }
    Ok(PreparedAssessment {
        input: snapshot
            .render()
            .map_err(|_| CampaignAssessmentError::InvalidRequest)?,
        snapshot,
        system: rendered.prompt,
    })
}

pub fn snapshot(
    request: &CampaignAssessmentRequest,
) -> Result<CampaignSnapshot, CampaignAssessmentError> {
    let invalid = || CampaignAssessmentError::InvalidRequest;
    if request.request_id.trim().is_empty()
        || request.request_id.len() > 256
        || request.todo_scope
            != (TodoScope::Campaign {
                campaign_id: request.campaign_id.clone(),
            })
        || serde_json::to_vec(request).map_err(|_| invalid())?.len() > 64_000
        || request.monitor.query.scope
            != (MonitorScope::Campaign {
                campaign_id: request.campaign_id.clone(),
            })
        || request.monitor.query.validate().is_err()
    {
        return Err(invalid());
    }
    let TodoResponse::List {
        todos,
        scope_revision,
        next_cursor,
        watermark,
    } = &request.todos
    else {
        return Err(invalid());
    };
    if watermark.instance_id.is_empty()
        || request.monitor.version.epoch.is_empty()
        || todos.len() > 20
        || todos
            .iter()
            .map(|t| &t.id)
            .collect::<std::collections::HashSet<_>>()
            .len()
            != todos.len()
        || todos
            .windows(2)
            .any(|pair| (pair[0].order_key, &pair[0].id) >= (pair[1].order_key, &pair[1].id))
        || next_cursor.as_ref().is_some_and(|cursor| {
            cursor.version != 1
                || cursor.scope != request.todo_scope
                || cursor.instance_id != watermark.instance_id
                || cursor.scope_revision != *scope_revision
                || cursor.filter != Default::default()
                || todos.last().is_none_or(|t| {
                    cursor.after_order_key != t.order_key || cursor.after_id != t.id
                })
        })
        || todos.iter().any(|t| {
            t.schema_version != 1
                || t.revision == 0
                || t.revision > *scope_revision
                || t.title.trim().is_empty()
                || t.id.chars().any(char::is_control)
                || t.scope
                    != (TodoScope::Campaign {
                        campaign_id: request.campaign_id.clone(),
                    })
        })
    {
        return Err(invalid());
    }
    let durable = request.monitor.payload.as_ref().map(|p| &p.durable);
    let inference = durable.and_then(|d| match &d.inference {
        Observed::Known(i) => Some(i),
        Observed::Unknown => None,
    });
    let snapshot = CampaignSnapshot {
        triggers: request.triggers.clone(),
        evidence_total: request.evidence_total,
        evidence: request.evidence.clone(),
        id: request.campaign_id.clone(),
        revision: request.revision,
        objective_summary: request.objective_summary.clone(),
        todo_revision: *scope_revision,
        todos_partial: next_cursor.is_some(),
        todos: todos
            .iter()
            .map(|t| TodoSummary {
                id: t.id.clone(),
                title: t.title.clone(),
                status: match t.status {
                    ApiStatus::Pending => TodoStatus::Pending,
                    ApiStatus::InProgress => TodoStatus::InProgress,
                    ApiStatus::Blocked => TodoStatus::Blocked,
                    ApiStatus::Completed => TodoStatus::Completed,
                    ApiStatus::Cancelled => TodoStatus::Cancelled,
                },
            })
            .collect(),
        resources: ResourceSnapshot {
            unresolved_tokens: inference.map(|i| i.unresolved_reserved.tokens.0.to_string()),
            unresolved_cost_micro_usd: inference
                .map(|i| i.unresolved_reserved.cost_micro_usd.0.to_string()),
            sampled_at_ms: durable.map(|d| d.sampled_at_ms),
            stale: request.monitor.stale.is_some() || request.monitor.payload.is_none(),
            final_tokens: inference.map(|i| i.final_usage.tokens.0.to_string()),
            final_cost_micro_usd: inference.map(|i| i.final_usage.cost_micro_usd.0.to_string()),
            unresolved_native_jobs: durable.map(|d| d.native_jobs.unresolved.0.to_string()),
        },
    };
    snapshot.render().map_err(|_| invalid())?;
    Ok(snapshot)
}

pub fn validate_completion(
    text: &str,
    has_tool_calls: bool,
    finish_reason: Option<&str>,
    snapshot: &CampaignSnapshot,
) -> Result<CampaignAssessment, CampaignAssessmentError> {
    if has_tool_calls || finish_reason.is_some_and(|r| r != "stop") || text.len() > 64_000 {
        return Err(CampaignAssessmentError::MalformedOutput);
    }
    validate_assessment(text, snapshot)
}

pub fn validate_assessment(
    text: &str,
    snapshot: &CampaignSnapshot,
) -> Result<CampaignAssessment, CampaignAssessmentError> {
    if text.len() > 64_000 {
        return Err(CampaignAssessmentError::MalformedOutput);
    }
    let assessment: CampaignAssessment =
        serde_json::from_str(text).map_err(|_| CampaignAssessmentError::MalformedOutput)?;
    if assessment.summary.trim().is_empty()
        || assessment.summary.len() > 2048
        || std::iter::once(&assessment.summary)
            .chain(assessment.findings.iter())
            .chain(assessment.blockers.iter())
            .any(|s| s.chars().any(|c| c.is_control() && c != '\n' && c != '\t'))
        || [&assessment.findings, &assessment.refs, &assessment.blockers]
            .iter()
            .any(|v| v.len() > 20 || v.iter().any(|s| s.trim().is_empty() || s.len() > 2048))
        || assessment.refs.iter().any(|id| {
            !snapshot.todos.iter().any(|t| &t.id == id)
                && !snapshot.evidence.iter().any(|e| &e.reference == id)
        })
    {
        return Err(CampaignAssessmentError::MalformedOutput);
    }
    Ok(assessment)
}
