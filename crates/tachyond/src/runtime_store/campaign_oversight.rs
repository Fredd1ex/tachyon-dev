//! Campaign-owned advisory assessments. Durable claims precede provider dispatch;
//! publication rechecks the semantic inputs under the database writer.
use super::{
    campaign_launch::Launch,
    campaign_ledger::Usage,
    model_accounting::{
        services::{service_id, ServicePermit},
        ModelBroker,
    },
    RuntimeStore,
};
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tachyon_api::{
    campaign::CampaignManifest,
    campaign_oversight::*,
    monitor::*,
    todo::{TodoResponse, TodoScope},
    types::{ApiResponse, CampaignStatus},
};
use tachyon_model::{ChatMessage, Role};
use tachyon_orchestrator::{agents::campaign::assessment, capabilities::Capability, registry};
use tokio::{sync::watch, time::Instant};

const STATES: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_oversight_v1");
const OUTBOX: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_assessment_outbox_v1");
static WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(STATES).map_err(err)?;
    tx.open_table(OUTBOX).map_err(err)?;
    Ok(())
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn hash(v: &impl Serialize) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(v).map_err(err)?)
    ))
}
fn bounded(s: &str, limit: usize) -> String {
    s.chars()
        .filter(|c| !c.is_control() || *c == '\n')
        .scan(0, |n, c| {
            *n += c.len_utf8();
            (*n <= limit).then_some(c)
        })
        .collect()
}

#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Markers {
    terminal: String,
    findings: String,
    blocked: String,
    budget: u8,
}
#[derive(Default, Serialize, Deserialize)]
struct State {
    limit: u64,
    revision: u64,
    fence: String,
    markers: Markers,
    pending: BTreeSet<String>,
    running: Option<String>,
    explicit: BTreeSet<String>,
    records: Vec<AssessmentRecord>,
    stopped: bool,
}
pub(super) fn initialize_campaign_in(
    tx: &WriteTransaction,
    m: &CampaignManifest,
) -> Result<(), String> {
    let Some(policy) = &m.oversight else {
        return Ok(());
    };
    let mut state = load(tx, &m.campaign_id)?;
    if state.limit != 0 && state.limit != policy.max_assessments {
        return Err("immutable oversight limit conflict".into());
    }
    state.limit = policy.max_assessments;
    save(tx, &m.campaign_id, &state)
}

pub(super) fn enabled_in(tx: &WriteTransaction, campaign: &str) -> Result<bool, String> {
    Ok(tx
        .open_table(STATES)
        .map_err(err)?
        .get(campaign)
        .map_err(err)?
        .is_some())
}
fn load(tx: &WriteTransaction, campaign: &str) -> Result<State, String> {
    tx.open_table(STATES)
        .map_err(err)?
        .get(campaign)
        .map_err(err)?
        .map(|v| serde_json::from_slice(v.value()).map_err(err))
        .transpose()
        .map(|v| v.unwrap_or_default())
}
fn save(tx: &WriteTransaction, campaign: &str, state: &State) -> Result<(), String> {
    tx.open_table(STATES)
        .map_err(err)?
        .insert(campaign, serde_json::to_vec(state).map_err(err)?.as_slice())
        .map_err(err)?;
    Ok(())
}

/// Called in the source mutation's transaction. Rolled-back sources cannot leave
/// trigger receipts. Notifications are hints; the durable set survives a lost wake.
pub(super) fn trigger_in(tx: &WriteTransaction, campaign: &str, cause: &str) -> Result<(), String> {
    if !enabled_in(tx, campaign)? {
        return Ok(());
    }
    let mut state = load(tx, campaign)?;
    if state.stopped || state.records.len() as u64 >= state.limit {
        return Ok(());
    }
    state.pending.insert(cause.into());
    save(tx, campaign, &state)?;
    WAKE.notify_waiters();
    Ok(())
}

pub(super) fn dispatch_in(
    tx: &WriteTransaction,
    campaign: &str,
    request: &str,
) -> Result<(), String> {
    let state = load(tx, campaign)?;
    // Descriptor-only service users have no launch. A campaign launch must have
    // its own durable trigger claim and must still be within its original lease.
    let launches = tx
        .open_table(super::campaign_launch::LAUNCHES)
        .map_err(err)?;
    if let Some(row) = launches.get(campaign).map_err(err)? {
        let launch: Launch = serde_json::from_slice(row.value()).map_err(err)?;
        if state.running.as_deref() != Some(request)
            || state.stopped
            || RuntimeStore::campaign_status_in(tx, campaign)? != CampaignStatus::Running
            || super::monitor::now_ms() >= launch.manifest.deadline_ms
        {
            return Err("campaign assessment dispatch fenced".into());
        }
    }
    Ok(())
}

fn reported_band(ledger: &super::campaign_ledger::Ledger) -> u8 {
    let own = service_id(&ledger.campaign_id, "oversight");
    let (mut tokens, mut cost) = (0u128, 0u128);
    for (id, r) in &ledger.reservations {
        if ledger.allocations.contains_key(id)
            || r.allocation.as_deref() == Some(&own)
            || r.pool != super::campaign_ledger::Pool::Work
        {
            continue;
        }
        if let Usage::Final(n) | Usage::Provisional(n) = r.usage {
            tokens += u128::from(n.tokens);
            cost += u128::from(n.cost_micro_usd);
        }
    }
    let percent = (tokens * 100 / u128::from(ledger.envelope.work.tokens.max(1)))
        .max(cost * 100 / u128::from(ledger.envelope.work.cost_micro_usd.max(1)));
    [25u128, 50, 75, 90, 100]
        .into_iter()
        .filter(|n| percent >= *n)
        .count() as u8
}

pub(super) fn budget_trigger_in(
    tx: &WriteTransaction,
    ledger: &super::campaign_ledger::Ledger,
) -> Result<(), String> {
    if !enabled_in(tx, &ledger.campaign_id)? {
        return Ok(());
    }
    let band = reported_band(ledger);
    if band == 0 {
        return Ok(());
    }
    let state = load(tx, &ledger.campaign_id)?;
    if band > state.markers.budget {
        trigger_in(tx, &ledger.campaign_id, "budget_threshold")?;
        let mut state = load(tx, &ledger.campaign_id)?;
        // Disabled campaigns do not create an oversight record.
        if state.pending.contains("budget_threshold") {
            state.markers.budget = band;
            save(tx, &ledger.campaign_id, &state)?;
        }
    }
    Ok(())
}
struct Snapshot {
    fence: String,
    markers: Markers,
    request: CampaignAssessmentRequest,
}
struct Claim {
    fence: String,
    native_jobs_hash: String,
    request: CampaignAssessmentRequest,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Delivery {
    pub conversation_id: String,
    pub assessment: PublishedCampaignAssessment,
    retry_at: u64,
}

impl RuntimeStore {
    fn close_oversight(&self, campaign: &str, force: bool) -> Result<bool, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut state = load(&tx, campaign)?;
        if !force
            && !state.stopped
            && state.running.is_none()
            && !state.pending.is_empty()
            && (state.records.len() as u64) < state.limit
        {
            return Ok(false);
        }
        state.stopped = true;
        let own = service_id(campaign, "oversight");
        let ledger = Self::campaign_ledger_in(&tx, campaign)?;
        if ledger.allocations.get(&own) == Some(&false)
            && !ledger.reservations.values().any(|r| {
                r.allocation.as_deref() == Some(&own) && !matches!(r.usage, Usage::Final(_))
            })
        {
            Self::campaign_ledger_command_in(
                &tx,
                &format!("close:{own}"),
                campaign,
                super::campaign_ledger::LedgerCommand::CloseAllocation {
                    reservation_id: own,
                },
            )?;
        }
        save(&tx, campaign, &state)?;
        tx.commit().map_err(err)?;
        Ok(true)
    }

    pub(crate) fn assessment_request_known(
        &self,
        campaign: &str,
        command: &str,
    ) -> Result<bool, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let table = tx.open_table(STATES).map_err(err)?;
        let state: Option<State> = table
            .get(campaign)
            .map_err(err)?
            .map(|v| serde_json::from_slice(v.value()).map_err(err))
            .transpose()?;
        Ok(state.is_some_and(|s| s.explicit.contains(command)))
    }

    fn oversight_snapshot(
        &self,
        tx: &WriteTransaction,
        m: &CampaignManifest,
    ) -> Result<Snapshot, String> {
        // Begin the read AFTER obtaining the writer. No mutation can interleave
        // the todo/monitor/evidence projection or its publication comparison.
        let read = self.database.begin_read().map_err(err)?;
        let campaign = Self::campaign_status_in(tx, &m.campaign_id)?;
        if campaign != CampaignStatus::Running || super::monitor::now_ms() >= m.deadline_ms {
            return Err("campaign oversight is no longer dispatchable".into());
        }
        let todos = super::todo::assessment_page_in(&read, &m.campaign_id)?;
        let TodoResponse::List { scope_revision, .. } = &todos else {
            unreachable!()
        };
        let todo_revision = *scope_revision;
        let mut ledger = Self::campaign_ledger_in(tx, &m.campaign_id)?;
        let own = service_id(&m.campaign_id, "oversight");
        ledger
            .reservations
            .retain(|id, r| id != &own && r.allocation.as_deref() != Some(&own));
        ledger.allocations.remove(&own);
        ledger.transfers.remove(&own);
        // Raw ledger revision and aggregate debt include our own inference. Hash
        // every other hold/report/transfer instead, including unresolved spend.
        let budget = json!([
            ledger.envelope,
            ledger.reservations,
            ledger.allocations,
            ledger.transfers
        ]);
        let threshold = reported_band(&ledger);
        let groups: Value = read
            .open_table(super::groups::ROOTS)
            .map_err(err)?
            .get(m.campaign_id.as_str())
            .map_err(err)?
            .map(|v| serde_json::from_slice(v.value()).map_err(err))
            .transpose()?
            .unwrap_or(Value::Null);
        let blocked: Vec<_> = groups["work"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, v)| !v["wait"].is_null() || v["cancellation_requested"] == true)
            .map(|(id, v)| {
                json!([
                    id,
                    v["wait"],
                    v["wait_revision"],
                    v["cancellation_requested"]
                ])
            })
            .collect();
        let mut work_versions = Vec::new();
        let mut terminal = Vec::new();
        let mut projected_work_count = 0;
        let mut evidence = Vec::new();
        let index = read
            .open_table(super::research_context::WORK_INDEX)
            .map_err(err)?;
        let executions = read.open_table(super::execution::EXECUTIONS).map_err(err)?;
        for entry in index
            .range((m.campaign_id.as_str(), "")..=(m.campaign_id.as_str(), "\u{10ffff}"))
            .map_err(err)?
        {
            let (key, _) = entry.map_err(err)?;
            let (_, id) = key.value();
            if let Some(row) = executions.get(id).map_err(err)? {
                let record = super::execution::decode_record(row.value(), id)?;
                let version = hash(&(
                    record.policy.work.generation,
                    record.policy.model.identity.instruction_revision,
                    &record.policy.work.attempt,
                    &record.phase,
                    &record.candidate,
                    record.settled,
                ))?;
                work_versions.push((id.to_owned(), version.clone()));
                if record.settled || groups["work"][id]["terminal"] == true {
                    terminal.push((id.to_owned(), version.clone()));
                }
                if record.settled
                    || groups["work"][id]["terminal"] == true
                    || !groups["work"][id]["wait"].is_null()
                    || matches!(
                        record.phase,
                        super::execution::ExecutionPhase::AwaitingAcceptance
                            | super::execution::ExecutionPhase::AwaitingVerification
                    )
                {
                    projected_work_count += 1;
                    if evidence.len() < 10 {
                        evidence.push(AssessmentEvidence {
                            reference: format!("work:{id}:{version}"),
                            summary: bounded(
                                &format!(
                                    "Host-recorded Work phase {:?}; candidate evidence: {}",
                                    record.phase,
                                    serde_json::to_string(&record.candidate).map_err(err)?
                                ),
                                1024,
                            ),
                        });
                    }
                }
            }
        }
        let findings = read
            .open_table(super::research_context::FINDINGS)
            .map_err(err)?;
        let mut finding_versions = Vec::new();
        for entry in findings
            .range((m.campaign_id.as_str(), "")..=(m.campaign_id.as_str(), "\u{10ffff}"))
            .map_err(err)?
        {
            let (key, row) = entry.map_err(err)?;
            let (_, id) = key.value();
            let resource: tachyon_api::context::Resource =
                serde_json::from_slice(row.value()).map_err(err)?;
            let version = hash(&resource)?;
            finding_versions.push((id.to_owned(), version.clone()));
            if evidence.len() < 20 {
                evidence.push(AssessmentEvidence {
                    reference: format!("finding:{id}:{version}"),
                    summary: bounded(
                        &format!("Untrusted authored finding: {}", resource.data),
                        1024,
                    ),
                });
            }
        }
        let query = MonitorQuery {
            scope: MonitorScope::Campaign {
                campaign_id: m.campaign_id.clone(),
            },
            after: None,
            limit: 20,
        };
        let fence = hash(&(
            m.campaign_id.as_str(),
            &m.objective,
            todo_revision,
            work_versions,
            &finding_versions,
            &groups["work"],
            &groups["attention"],
            super::coordination::accepted_revisions_in(&read, &m.campaign_id)?,
            budget,
            &campaign,
        ))?;
        let markers = Markers {
            terminal: hash(&terminal)?,
            findings: hash(&finding_versions)?,
            blocked: hash(&blocked)?,
            budget: threshold,
        };
        Ok(Snapshot {
            fence: fence.clone(),
            markers,
            request: CampaignAssessmentRequest {
                triggers: vec![],
                evidence_total: (projected_work_count + finding_versions.len()) as u64,
                evidence,
                kind: CampaignRequestKind::CampaignAssessment,
                request_id: String::new(),
                campaign_id: m.campaign_id.clone(),
                revision: 0,
                objective_summary: bounded(&m.objective, 4096),
                todo_scope: TodoScope::Campaign {
                    campaign_id: m.campaign_id.clone(),
                },
                todos,
                monitor: MonitorSnapshot {
                    query,
                    version: MonitorVersion {
                        epoch: fence,
                        sequence: 0,
                    },
                    payload: None,
                    stale: None,
                },
            },
        })
    }

    fn observe_oversight(
        &self,
        m: &CampaignManifest,
        claim: bool,
    ) -> Result<Option<Claim>, String> {
        let config = m.oversight.as_ref().ok_or("oversight disabled")?;
        let tx = self.database.begin_write().map_err(err)?;
        let mut state = load(&tx, &m.campaign_id)?;
        if state.limit != config.max_assessments {
            return Err("oversight is not host-approved".into());
        }
        if state.stopped || state.records.len() as u64 >= config.max_assessments {
            return Ok(None);
        }
        if super::monitor::now_ms() >= m.deadline_ms
            || Self::campaign_status_in(&tx, &m.campaign_id)? != CampaignStatus::Running
        {
            return Err("campaign oversight is no longer dispatchable".into());
        }
        // Source transactions already coalesce causes. Idle ticks need only this
        // record lookup, not a scan of results, findings, or monitor tables.
        if state.running.is_some() || (state.revision > 0 && state.pending.is_empty()) {
            return Ok(None);
        }
        let mut snapshot = self.oversight_snapshot(&tx, m)?;
        if state.revision == 0 {
            state.pending.insert("authorized_launch".into());
        } else {
            for (changed, cause) in [
                (
                    snapshot.markers.terminal != state.markers.terminal,
                    "logical_work_terminal",
                ),
                (
                    snapshot.markers.findings != state.markers.findings,
                    "new_finding",
                ),
                (
                    snapshot.markers.blocked != state.markers.blocked,
                    "blocked_state",
                ),
                (
                    snapshot.markers.budget > state.markers.budget,
                    "budget_threshold",
                ),
            ] {
                if changed {
                    state.pending.insert(cause.into());
                }
            }
        }
        if snapshot.fence != state.fence {
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or("assessment revision exhausted")?;
            state.fence = snapshot.fence.clone();
        }
        snapshot.markers.budget = snapshot.markers.budget.max(state.markers.budget);
        state.markers = snapshot.markers;
        let result = if claim && state.running.is_none() && !state.pending.is_empty() {
            let read = self.database.begin_read().map_err(err)?;
            snapshot.request.monitor.payload = Some(
                self.monitor_sample_in(
                    &read,
                    std::slice::from_ref(&snapshot.request.monitor.query),
                )?
                .pop()
                .ok_or("missing monitor snapshot")?
                .map_err(|e| format!("monitor snapshot: {e:?}"))?,
            );
            let native_jobs_hash = hash(
                &snapshot
                    .request
                    .monitor
                    .payload
                    .as_ref()
                    .unwrap()
                    .durable
                    .native_jobs,
            )?;
            let id = format!(
                "campaign-assessment-{}-{}",
                m.campaign_id,
                state.records.len() + 1
            );
            snapshot.request.request_id = id.clone();
            snapshot.request.revision = state.revision;
            snapshot.request.triggers = state.pending.iter().cloned().collect();
            state.running = Some(id.clone());
            state.records.push(AssessmentRecord {
                input_sha256: hash(&(&snapshot.fence, &native_jobs_hash))?,
                evidence_refs: snapshot
                    .request
                    .evidence
                    .iter()
                    .map(|e| e.reference.clone())
                    .chain(match &snapshot.request.todos {
                        TodoResponse::List { todos, .. } => {
                            todos.iter().map(|t| t.id.clone()).collect::<Vec<_>>()
                        }
                        _ => vec![],
                    })
                    .collect(),
                request_id: id,
                revision: state.revision,
                triggers: std::mem::take(&mut state.pending).into_iter().collect(),
                status: "claimed_unknown".into(),
                published: None,
            });
            Some(Claim {
                fence: snapshot.fence,
                native_jobs_hash,
                request: snapshot.request,
            })
        } else {
            None
        };
        save(&tx, &m.campaign_id, &state)?;
        tx.commit().map_err(err)?;
        Ok(result)
    }

    fn finish_oversight(
        &self,
        m: &CampaignManifest,
        claim: &Claim,
        result: Result<CampaignAssessment, String>,
    ) -> Result<bool, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut state = load(&tx, &m.campaign_id)?;
        if state.running.as_deref() != Some(&claim.request.request_id) {
            return Err("assessment claim mismatch".into());
        }
        let own = service_id(&m.campaign_id, "oversight");
        let ledger = Self::campaign_ledger_in(&tx, &m.campaign_id)?;
        let final_usage = !ledger
            .reservations
            .values()
            .any(|r| r.allocation.as_deref() == Some(&own) && !matches!(r.usage, Usage::Final(_)));
        let mut fresh = self
            .oversight_snapshot(&tx, m)
            .is_ok_and(|s| s.fence == claim.fence);
        if fresh {
            let read = self.database.begin_read().map_err(err)?;
            let payload = self
                .monitor_sample_in(&read, std::slice::from_ref(&claim.request.monitor.query))?
                .pop()
                .ok_or("missing monitor snapshot")?
                .map_err(|e| format!("monitor snapshot: {e:?}"))?;
            fresh = hash(&payload.durable.native_jobs)? == claim.native_jobs_hash;
        }
        let record = state
            .records
            .last_mut()
            .ok_or("missing assessment receipt")?;
        record.status = if !final_usage {
            "unknown"
        } else if result.is_err() {
            "invalid_or_failed"
        } else if !fresh {
            "stale"
        } else {
            "published"
        }
        .into();
        if let Ok(assessment) = result {
            if fresh && final_usage {
                let mut sources = claim.request.evidence.clone();
                if let TodoResponse::List { todos, .. } = &claim.request.todos {
                    sources.extend(todos.iter().map(|t| AssessmentEvidence {
                        reference: t.id.clone(),
                        summary: t.title.clone(),
                    }));
                }
                sources.retain(|s| assessment.refs.contains(&s.reference));
                let published = PublishedCampaignAssessment {
                    id: claim.request.request_id.clone(),
                    campaign_id: m.campaign_id.clone(),
                    revision: claim.request.revision,
                    objective: claim.request.objective_summary.clone(),
                    assessment,
                    sources,
                };
                if let Some(destination) = m
                    .oversight
                    .as_ref()
                    .and_then(|o| o.conversation_id.as_ref())
                {
                    let delivery = Delivery {
                        conversation_id: destination.clone(),
                        assessment: published.clone(),
                        retry_at: 0,
                    };
                    tx.open_table(OUTBOX)
                        .map_err(err)?
                        .insert(
                            published.id.as_str(),
                            serde_json::to_vec(&delivery).map_err(err)?.as_slice(),
                        )
                        .map_err(err)?;
                }
                record.published = Some(published);
            }
        }
        state.running = None;
        state.stopped |= !final_usage;
        save(&tx, &m.campaign_id, &state)?;
        tx.commit().map_err(err)?;
        Ok(!state.stopped && (state.records.len() as u64) < state.limit)
    }

    pub(crate) fn campaign_assessments(&self, id: &str) -> Result<ApiResponse, String> {
        let tx = self.database.begin_write().map_err(err)?;
        Self::campaign_status_in(&tx, id)?;
        Ok(ApiResponse::CampaignAssessments {
            records: load(&tx, id)?.records,
        })
    }

    pub(crate) fn request_campaign_assessment(
        &self,
        id: &str,
        command: &str,
        authorized: bool,
    ) -> Result<ApiResponse, String> {
        if !authorized || command.trim().is_empty() || command.len() > 256 {
            return Err("explicit bounded host authorization required".into());
        }
        let tx = self.database.begin_write().map_err(err)?;
        let launch: Launch = serde_json::from_slice(
            tx.open_table(super::campaign_launch::LAUNCHES)
                .map_err(err)?
                .get(id)
                .map_err(err)?
                .ok_or("no launch")?
                .value(),
        )
        .map_err(err)?;
        launch.validate_digest()?;
        let policy = launch
            .manifest
            .oversight
            .as_ref()
            .ok_or("oversight disabled")?;
        let mut state = load(&tx, id)?;
        if state.explicit.contains(command) {
            return Ok(ApiResponse::CampaignAssessments {
                records: state.records,
            });
        }
        if Self::campaign_status_in(&tx, id)? != CampaignStatus::Running
            || super::monitor::now_ms() >= launch.manifest.deadline_ms
            || state.stopped
            || state.records.len() as u64 >= policy.max_assessments
            || state.explicit.len() >= 64
        {
            return Err("oversight is inactive or exhausted".into());
        }
        state.explicit.insert(command.into());
        state.pending.insert("explicit_request".into());
        save(&tx, id, &state)?;
        tx.commit().map_err(err)?;
        WAKE.notify_waiters();
        Ok(ApiResponse::CampaignAssessments {
            records: state.records,
        })
    }

    pub(crate) fn claim_assessment_delivery(
        &self,
        destination: &str,
        now: u64,
    ) -> Result<Option<Delivery>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut table = tx.open_table(OUTBOX).map_err(err)?;
        let mut found = None;
        for row in table.iter().map_err(err)? {
            let (_, v) = row.map_err(err)?;
            let delivery: Delivery = serde_json::from_slice(v.value()).map_err(err)?;
            if delivery.conversation_id == destination && delivery.retry_at <= now {
                found = Some(delivery);
                break;
            }
        }
        if let Some(delivery) = &mut found {
            delivery.retry_at = now.saturating_add(5000);
            table
                .insert(
                    delivery.assessment.id.as_str(),
                    serde_json::to_vec(delivery).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
        }
        drop(table);
        tx.commit().map_err(err)?;
        Ok(found)
    }

    pub(crate) fn acknowledge_assessment_delivery(
        &self,
        event: &tachyon_api::InteractionEventEnvelope,
    ) -> Result<bool, String> {
        let Some(id) = event
            .metadata
            .message_id
            .strip_suffix(":published")
            .filter(|id| id.starts_with("campaign-assessment-"))
        else {
            return Ok(false);
        };
        let tx = self.database.begin_write().map_err(err)?;
        let mut table = tx.open_table(OUTBOX).map_err(err)?;
        let delivery: Option<Delivery> = table
            .get(id)
            .map_err(err)?
            .map(|v| serde_json::from_slice(v.value()).map_err(err))
            .transpose()?;
        if let Some(delivery) = delivery {
            if event.metadata.conversation_id != delivery.conversation_id
                || event.metadata.turn_id.is_some()
                || event.metadata.causation_id.as_deref() != Some(id)
                || event.metadata.correlation_id != id
                || event.metadata.generation != 0
                || event.metadata.protocol_version != tachyon_api::INTERACTION_PROTOCOL_VERSION
                || event.metadata.attention.is_some()
                || event.event
                    != (tachyon_api::InteractionEvent::UserVisibleNotificationPublished {
                        text: delivery.assessment.advisory(),
                    })
            {
                return Err("assessment publication identity mismatch".into());
            }
            let projection = super::HistoryProjection {
                attention: None,
                schema_version: 1,
                event_id: event.metadata.message_id.clone(),
                kind: tachyon_api::HistoryKind::Conversation,
                conversation_id: delivery.conversation_id,
                turn_id: None,
                occurred_at_ms: event.metadata.occurred_at_ms,
                role: tachyon_api::HistoryRole::Notification,
                text: delivery.assessment.advisory(),
                task_id: None,
                task_state: None,
            };
            tx.open_table(super::HISTORY_OUTBOX)
                .map_err(err)?
                .insert(
                    projection.event_id.as_str(),
                    serde_json::to_vec(&projection).map_err(err)?.as_slice(),
                )
                .map_err(err)?;
            table.remove(id).map_err(err)?;
        } else {
            return Ok(false);
        }
        drop(table);
        tx.commit().map_err(err)?;
        Ok(true)
    }
}

/// One owner, one provider future, one durable coalesced pending set. Polling is
/// bounded to the campaign and journals semantic transitions, never progress text.
pub(super) async fn run(
    broker: Arc<ModelBroker>,
    m: CampaignManifest,
    permit: ServicePermit,
    mut cancel: watch::Receiver<bool>,
    mut done: watch::Receiver<bool>,
    deadline: Instant,
) {
    let store = broker.store.clone();
    loop {
        if *cancel.borrow() || Instant::now() >= deadline {
            permit.revoke();
            break;
        }
        let manifest = m.clone();
        let claim = match store
            .storage(move |s| s.observe_oversight(&manifest, true))
            .await
        {
            Ok(Some(c)) => c,
            Ok(None) if *done.borrow() => {
                let campaign = m.campaign_id.clone();
                // Closing and accepting a final explicit request serialize on
                // the writer. Never acknowledge new work into a dead owner.
                if store
                    .storage(move |s| s.close_oversight(&campaign, false))
                    .await
                    .unwrap_or(true)
                {
                    return;
                }
                continue;
            }
            Ok(None) => {
                tokio::select! { _ = tokio::time::sleep(Duration::from_secs(1)) => {}, _ = WAKE.notified() => {}, _ = cancel.changed() => {}, _ = done.changed() => {} }
                continue;
            }
            Err(_) => break,
        };
        let prepared = assessment::prepare(
            &claim.request,
            &[Capability::Todo, Capability::Monitor],
            &registry::builtin(),
        );
        let result = if let Ok(prepared) = prepared {
            let messages = [
                ChatMessage::new(Role::System, prepared.system),
                ChatMessage::new(Role::User, prepared.input),
            ];
            let mut sink = |_: &str| {};
            let request =
                broker.execute_service(&permit, &claim.request.request_id, &messages, &mut sink);
            tokio::pin!(request);
            tokio::select! {
                biased;
                _ = cancel.changed() => { permit.revoke(); Err("cancelled".into()) }
                _ = tokio::time::sleep_until(deadline) => { permit.revoke(); Err("deadline".into()) }
                result = &mut request => result.map_err(err).and_then(|c| assessment::validate_completion(&c.text, !c.tool_calls.is_empty(), c.finish_reason.as_deref(), &prepared.snapshot).map_err(|e| format!("{e:?}"))),
            }
        } else {
            Err("invalid assessment input".into())
        };
        let manifest = m.clone();
        if !matches!(
            store
                .storage(move |s| s.finish_oversight(&manifest, &claim, result))
                .await,
            Ok(true)
        ) {
            break;
        }
    }
    permit.revoke();
    let campaign = m.campaign_id.clone();
    let _ = store
        .storage(move |s| s.close_oversight(&campaign, true))
        .await;
}

#[cfg(test)]
mod tests;
