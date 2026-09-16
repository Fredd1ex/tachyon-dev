//! Bounded projections of existing execution history, not a second attempt journal.
#![allow(dead_code)]
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tachyon_api::{
    context::*,
    types::{ArtifactPublication, ArtifactRegistration},
};
use tachyond::{artifact_store::ArtifactStore, verification::CommandEvidence};

use super::{
    execution::{
        command::{CommandGateRecord, COMMAND_GATES},
        decode_record, ExecutionRecord, EXECUTIONS,
    },
    RuntimeStore,
};

const MAX_CONTEXT_ARTIFACT_BYTES: u64 = 4 * 1024 * 1024;
mod stopping;
pub(crate) mod traces;

const FINDINGS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("research_findings_v1");
const LINEAGE: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("research_candidate_lineage_v1");
pub(super) const WORK_INDEX: TableDefinition<(&str, &str), ()> =
    TableDefinition::new("research_context_work_v1");
fn err(e: impl std::fmt::Display) -> String {
    format!("research context: {e}")
}

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    traces::initialize(tx)?;
    tx.open_table(FINDINGS).map_err(err)?;
    tx.open_table(LINEAGE).map_err(err)?;
    let mut index = tx.open_table(WORK_INDEX).map_err(err)?;
    let mut migrations = tx.open_table(super::MIGRATIONS).map_err(err)?;
    if migrations
        .get("research_context_work_v1")
        .map_err(err)?
        .is_none()
    {
        for row in tx
            .open_table(super::admission::WORK)
            .map_err(err)?
            .iter()
            .map_err(err)?
        {
            let (_, value) = row.map_err(err)?;
            let work: super::admission::AdmittedWork =
                serde_json::from_slice(value.value()).map_err(err)?;
            index
                .insert(
                    (
                        work.admission.campaign_id.as_str(),
                        work.admission.work_id.as_str(),
                    ),
                    (),
                )
                .map_err(err)?;
        }
        migrations
            .insert("research_context_work_v1", 1)
            .map_err(err)?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    campaign: String,
    stage: u8,
    key: String,
    index: usize,
}

impl Cursor {
    // Hex keeps arbitrary UTF-8 Work IDs bounded even after the cursor is JSON-escaped.
    fn encode(&self) -> Result<String, String> {
        let value = serde_json::to_string(&Self {
            campaign: self.campaign.clone(),
            stage: self.stage,
            index: self.index,
            key: self
                .key
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        })
        .map_err(err)?;
        if serde_json::to_vec(&value).map_err(err)?.len() > 1024 {
            return Err(err("oversized cursor"));
        }
        Ok(value)
    }

    fn decode(value: &str) -> Result<Self, String> {
        let mut cursor: Self = serde_json::from_str(value).map_err(err)?;
        if cursor.key.len() > 512
            || cursor.key.len() % 2 != 0
            || !cursor.key.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(err("invalid cursor key"));
        }
        let bytes = (0..cursor.key.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&cursor.key[i..i + 2], 16).map_err(err))
            .collect::<Result<Vec<_>, _>>()?;
        cursor.key = String::from_utf8(bytes).map_err(err)?;
        Ok(cursor)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Lineage {
    candidate: ResourceRef,
    parents: Vec<ResourceRef>,
}

fn attempt(
    record: &ExecutionRecord,
    evidence: Option<&CommandEvidence>,
    at: Option<u64>,
    parent: Option<ResourceRef>,
) -> Resource {
    let identity = &record.policy.model.identity;
    Resource {
        reference: ResourceRef {
            kind: ResourceKind::Attempt,
            work_id: identity.work_id.clone(),
            id: identity.attempt_id.clone(),
            version: format!(
                "1:{:?}:{}",
                record.phase,
                evidence.map_or("", |e| e.candidate_sha256.as_str())
            ),
        },
        occurred_at_ms: at,
        data: json!({
            "phase": record.phase, "generation": identity.generation,
            "instruction_revision": identity.instruction_revision,
            "snapshot": {"schema_version":1, "reattachable":false,
                "context_refs": record.policy.work.context_refs,
                "permissions": record.policy.work.constraints.as_ref().map(|c| &c.permissions),
                "deadline_ms": record.policy.work.deadline_ms,
                "lifetime_class": record.policy.work.lifetime_class,
                "budget_upper_bound": record.policy.funding.admission.upper_bound},
            "parent": parent,
            "observation": evidence.map(|e| json!({
                "outcome": e.outcome, "exit_code": e.exit_code, "elapsed_ms": e.elapsed_ms,
                "config_hash": e.config_hash, "candidate_sha256": e.candidate_sha256,
                "artifact_id": e.artifact_id, "output_truncated": e.truncated
            }))
        }),
    }
}

fn artifact_resource(record: &ArtifactRegistration, at: Option<u64>) -> Result<Resource, String> {
    if record.sha256.len() != 64
        || !record
            .sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || record.publication
            != (ArtifactPublication::Ready {
                version: record.sha256.clone(),
            })
    {
        return Err(err("artifact not Ready"));
    }
    let reference = ResourceRef {
        kind: ResourceKind::Artifact,
        work_id: record.work_id.clone().ok_or("artifact lacks Work scope")?,
        id: record.id.clone(),
        version: record.sha256.clone(),
    };
    if !reference.valid() {
        return Err(err("invalid artifact identity"));
    }
    Ok(Resource {
        reference,
        occurred_at_ms: at,
        data: json!({"size_bytes": record.size_bytes, "attempt_id": record.attempt_id}),
    })
}

impl RuntimeStore {
    pub(super) fn context_work(
        &self,
        campaign: &str,
        work: &str,
        artifacts: Option<&ArtifactStore>,
    ) -> Result<Vec<Resource>, String> {
        let tx = self.database.begin_read().map_err(err)?;
        let executions = tx.open_table(EXECUTIONS).map_err(err)?;
        let Some(row) = executions.get(work).map_err(err)? else {
            return Ok(vec![]);
        };
        if row.value().len() > 2 * 1024 * 1024 {
            return Err(err("oversized execution record"));
        }
        let current = decode_record(row.value(), work)?;
        if current.policy.funding.admission.campaign_id != campaign {
            return Err(err("execution scope mismatch"));
        }
        let gates = tx.open_table(COMMAND_GATES).map_err(err)?;
        let gate: Option<CommandGateRecord> = gates
            .get(work)
            .map_err(err)?
            .map(|row| {
                if row.value().len() > 16 * 1024 * 1024 {
                    return Err(err("oversized attempt history"));
                }
                serde_json::from_slice(row.value()).map_err(err)
            })
            .transpose()?;
        for record in gate
            .iter()
            .flat_map(|g| g.history.iter().map(|a| &a.execution))
            .chain(std::iter::once(&current))
        {
            let identity = &record.policy.model.identity;
            if identity.campaign_id != campaign
                || identity.work_id != work
                || record.policy.work.work_id != work
                || record.policy.funding.admission.campaign_id != campaign
                || record.policy.funding.admission.work_id != work
                || record.policy.work.generation != identity.generation
                || record.candidate.as_ref().is_some_and(|c| {
                    c.work_id != work
                        || c.generation != identity.generation
                        || c.assignment != record.policy.work.assignment
                        || c.attempt_id
                            .as_ref()
                            .is_some_and(|id| id != &identity.attempt_id)
                })
            {
                return Err(err("attempt identity mismatch"));
            }
        }
        let mut resources = Vec::new();
        let mut parent = None;
        if let Some(gate) = &gate {
            if gate.policy != current.policy || gate.history.len() > 32 {
                return Err(err("invalid attempt history"));
            }
            for old in &gate.history {
                if old.execution.policy.funding.admission.campaign_id != campaign
                    || old.execution.policy.work.work_id != work
                {
                    return Err(err("attempt scope mismatch"));
                }
                let item = attempt(
                    &old.execution,
                    old.evidence.as_ref(),
                    old.evidence_at_ms,
                    parent.take(),
                );
                parent = Some(item.reference.clone());
                resources.push(item);
                if let (Some(snapshot), Some(store)) = (&old.snapshot, artifacts) {
                    if snapshot.work_id.as_deref() != Some(work)
                        || snapshot.attempt_id.as_deref()
                            != Some(old.execution.policy.model.identity.attempt_id.as_str())
                        || snapshot.generation != Some(old.execution.policy.work.generation)
                        || snapshot.assignment != Some(old.execution.policy.work.assignment)
                    {
                        return Err(err("retained artifact scope mismatch"));
                    }
                    if store.metadata(work, &snapshot.id)?.as_ref() == Some(snapshot) {
                        resources.push(artifact_resource(snapshot, old.evidence_at_ms)?);
                    }
                }
            }
        }
        let at = gate.as_ref().and_then(|g| g.evidence_at_ms);
        resources.push(attempt(
            &current,
            gate.as_ref().and_then(|g| g.evidence.as_ref()),
            at,
            parent,
        ));
        if let (Some(candidate), Some(store)) = (&current.candidate, artifacts) {
            for id in candidate.candidate_refs.iter().flatten().take(32) {
                if let Some(record) = store.metadata(work, id)? {
                    if record.work_id.as_deref() == Some(work)
                        && record.attempt_id.as_deref()
                            == Some(current.policy.model.identity.attempt_id.as_str())
                        && record.generation == Some(current.policy.work.generation)
                        && record.assignment == Some(current.policy.work.assignment)
                        && matches!(record.publication, ArtifactPublication::Ready { .. })
                    {
                        resources.push(artifact_resource(&record, at)?);
                    }
                }
            }
        }
        let lineage = tx.open_table(LINEAGE).map_err(err)?;
        for resource in &mut resources {
            if !resource.reference.valid() {
                return Err(err("invalid resource identity"));
            }
            if resource.reference.kind == ResourceKind::Artifact {
                if let Some(row) = lineage
                    .get((
                        campaign,
                        resource.reference.work_id.as_str(),
                        resource.reference.id.as_str(),
                    ))
                    .map_err(err)?
                {
                    let links: Lineage = serde_json::from_slice(row.value()).map_err(err)?;
                    if links.candidate != resource.reference {
                        return Err(err("lineage version mismatch"));
                    }
                    resource.data["parents"] = serde_json::to_value(links.parents).map_err(err)?;
                }
            }
            if serde_json::to_vec(resource).map_err(err)?.len() > 6000 {
                return Err(err("oversized resource"));
            }
        }
        Ok(resources)
    }

    fn context_resolve(
        &self,
        campaign: &str,
        reference: &ResourceRef,
        artifacts: Option<&ArtifactStore>,
    ) -> Result<Resource, String> {
        if !reference.valid() {
            return Err(err("invalid reference"));
        }
        // Even a finding ID is not authority to address another campaign or Work.
        self.admitted_work(campaign, &reference.work_id)?;
        if matches!(reference.kind, ResourceKind::Trace | ResourceKind::Document) {
            return self.trace_resolve(campaign, reference);
        }
        if reference.kind == ResourceKind::Artifact {
            let record = artifacts
                .ok_or("artifact retrieval not configured")?
                .metadata(&reference.work_id, &reference.id)?
                .ok_or("unknown artifact")?;
            let resource = artifact_resource(&record, None)?;
            if resource.reference != *reference {
                return Err(err("artifact version mismatch"));
            }
            validate_artifact(&resource, artifacts)?;
            return Ok(resource);
        }
        if reference.kind == ResourceKind::Finding {
            let tx = self.database.begin_read().map_err(err)?;
            let table = tx.open_table(FINDINGS).map_err(err)?;
            let value = table
                .get((campaign, reference.id.as_str()))
                .map_err(err)?
                .ok_or("unknown finding")?;
            if value.value().len() > 6000 {
                return Err(err("oversized finding"));
            }
            let resource: Resource = serde_json::from_slice(value.value()).map_err(err)?;
            if resource.reference != *reference {
                return Err(err("finding reference mismatch"));
            }
            return Ok(resource);
        }
        let resource = self
            .context_work(campaign, &reference.work_id, artifacts)?
            .into_iter()
            .find(|r| r.reference == *reference)
            .ok_or_else(|| err("unknown resource version"))?;
        validate_artifact(&resource, artifacts)?;
        Ok(resource)
    }

    /// Trusted host facade. The private broker supplies campaign from its live permit.
    /// All synchronous work must run on the blocking pool at an async boundary.
    pub(crate) fn host_research_context(
        &self,
        campaign: &str,
        request: &Request,
        artifacts: Option<&ArtifactStore>,
    ) -> Result<Page, String> {
        request.validate()?;
        if campaign.is_empty() || self.campaign_ledger(campaign)?.is_none() {
            return Err(err("missing scope"));
        }
        if matches!(request, Request::Artifacts { .. }) && artifacts.is_none() {
            return Err(err("artifact retrieval not configured"));
        }
        if let Request::Read {
            resource,
            offset,
            limit,
        } = request
        {
            let mut item = self.context_resolve(campaign, resource, artifacts)?;
            if resource.kind == ResourceKind::Document {
                let store = artifacts.ok_or("document retrieval not configured")?;
                let expected: ArtifactRegistration =
                    serde_json::from_value(item.data["artifact"].clone()).map_err(err)?;
                if store.metadata(&resource.work_id, &expected.id)?.as_ref() != Some(&expected) {
                    return Err(err("document descriptor mismatch"));
                }
                let bytes = store.read(&resource.work_id, &expected.id, *offset, *limit)?;
                item.data = json!({"bytes":bytes,"offset":offset,"next_offset":offset.checked_add(bytes.len() as u64).ok_or("offset overflow")?});
            } else if resource.kind == ResourceKind::Trace {
                let bytes = self.trace_read(&item, *offset, *limit)?;
                item.data["bytes"] = json!(bytes);
                item.data["offset"] = json!(offset);
                item.data["next_offset"] = json!(offset
                    .checked_add(bytes.len() as u64)
                    .ok_or("offset overflow")?);
            } else if resource.kind == ResourceKind::Artifact {
                let bytes = artifacts.ok_or("artifact retrieval not configured")?.read(
                    &resource.work_id,
                    &resource.id,
                    *offset,
                    *limit,
                )?;
                item.data = json!({"bytes": bytes, "offset": offset, "next_offset": offset.checked_add(bytes.len() as u64).ok_or("offset overflow")?});
            } else if *offset != 0 {
                return Err(err("offset only applies to artifacts"));
            }
            let page = Page {
                resources: vec![item],
                next_cursor: None,
            };
            check_page(&page)?;
            return Ok(page);
        }
        let (query, kind) = match request {
            Request::Snapshot { query } => {
                return self.trace_list_phase(
                    campaign,
                    query,
                    Some(ResourceKind::Trace),
                    Some("work_context_snapshot"),
                )
            }
            Request::Traces { query } => {
                return self.trace_list(campaign, query, Some(ResourceKind::Trace))
            }
            Request::Documents { query } => {
                return self.trace_list(campaign, query, Some(ResourceKind::Document))
            }
            Request::Search { query } => (query, None),
            Request::Attempts { query } => (query, Some(ResourceKind::Attempt)),
            Request::Findings { query } => (query, Some(ResourceKind::Finding)),
            Request::Artifacts { query } => (query, Some(ResourceKind::Artifact)),
            Request::Read { .. } => unreachable!(),
        };
        let mut cursor = match &query.after {
            Some(s) => Cursor::decode(s)?,
            None => Cursor {
                campaign: campaign.into(),
                stage: u8::from(kind == Some(ResourceKind::Finding)),
                key: String::new(),
                index: 0,
            },
        };
        if cursor.campaign != campaign
            || cursor.stage > 2
            || cursor.key.len() > 256
            || cursor.index > 128
        {
            return Err(err("cursor scope or bounds mismatch"));
        }
        if cursor.stage == 2 {
            if kind.is_some() {
                return Err(err("cursor kind mismatch"));
            }
            let mut query = query.clone();
            query.after = (!cursor.key.is_empty()).then_some(cursor.key);
            let mut page = self.trace_list(campaign, &query, None)?;
            page.next_cursor = page
                .next_cursor
                .map(|key| {
                    Cursor {
                        campaign: campaign.into(),
                        stage: 2,
                        key,
                        index: 0,
                    }
                    .encode()
                })
                .transpose()?;
            check_page(&page)?;
            return Ok(page);
        }
        let mut page = Page::default();
        let mut scanned = 0;
        let tx = self.database.begin_read().map_err(err)?;
        if cursor.stage == 0 {
            let table = tx.open_table(WORK_INDEX).map_err(err)?;
            let start = cursor.key.clone();
            for row in table.range((campaign, start.as_str())..).map_err(err)? {
                let (key, _) = row.map_err(err)?;
                if key.value().0 != campaign {
                    break;
                }
                if scanned == MAX_SCAN {
                    page.next_cursor = Some(cursor.encode()?);
                    break;
                }
                scanned += 1;
                let work = key.value().1;
                if cursor.key != work {
                    cursor.index = 0;
                    cursor.key = work.into();
                }
                let resources = self.context_work(campaign, work, artifacts)?;
                for (index, item) in resources.into_iter().enumerate().skip(cursor.index) {
                    cursor.index = index;
                    if matches_query(&item, query, kind)? {
                        page.resources.push(item);
                        page.next_cursor = Some(cursor.encode()?);
                        if check_page(&page).is_err() || page.resources.len() > query.limit {
                            page.resources.pop();
                            return Ok(page);
                        }
                        validate_artifact(page.resources.last().unwrap(), artifacts)?;
                    }
                    cursor.index = index + 1;
                }
                // The next request can safely revisit this row and skip consumed resources.
            }
            if page.next_cursor.is_some() && scanned == MAX_SCAN {
                page.next_cursor = Some(cursor.encode()?);
                return Ok(page);
            }
            cursor.stage = 1;
            cursor.key.clear();
            cursor.index = 0;
        }
        page.next_cursor = None;
        if kind.is_none() || kind == Some(ResourceKind::Finding) {
            let table = tx.open_table(FINDINGS).map_err(err)?;
            let start = cursor.key.clone();
            for row in table.range((campaign, start.as_str())..).map_err(err)? {
                let (key, value) = row.map_err(err)?;
                if key.value().0 != campaign {
                    break;
                }
                if key.value().1 == start && cursor.index == 1 {
                    continue;
                }
                if scanned == MAX_SCAN {
                    page.next_cursor = Some(cursor.encode()?);
                    return Ok(page);
                }
                scanned += 1;
                cursor.key = key.value().1.into();
                cursor.index = 0;
                if value.value().len() > 6000 {
                    return Err(err("oversized finding"));
                }
                let item: Resource = serde_json::from_slice(value.value()).map_err(err)?;
                if item.reference.kind != ResourceKind::Finding
                    || item.reference.id != key.value().1
                    || item.reference.version != "1"
                    || !item.reference.valid()
                {
                    return Err(err("invalid finding identity"));
                }
                self.admitted_work(campaign, &item.reference.work_id)?;
                if matches_query(&item, query, kind)? {
                    page.resources.push(item);
                    page.next_cursor = Some(cursor.encode()?);
                    if check_page(&page).is_err() || page.resources.len() > query.limit {
                        page.resources.pop();
                        return Ok(page);
                    }
                }
                cursor.index = 1;
            }
        }
        page.next_cursor = None;
        if kind.is_none() {
            page.next_cursor = Some(
                Cursor {
                    campaign: campaign.into(),
                    stage: 2,
                    key: String::new(),
                    index: 0,
                }
                .encode()?,
            );
        }
        check_page(&page)?;
        Ok(page)
    }

    /// Immutable, idempotent host-authored finding. No model-facing write action exists.
    pub(crate) fn host_record_finding(
        &self,
        campaign: &str,
        finding: Finding,
        artifacts: Option<&ArtifactStore>,
    ) -> Result<ResourceRef, String> {
        if finding.author.trim().is_empty()
            || finding.author.len() > 256
            || finding.claim.trim().is_empty()
            || finding.claim.len() > 1024
            || finding.conditions.trim().is_empty()
            || finding.conditions.len() > 1024
            || !(1..=8).contains(&finding.evidence.len())
            || finding.parents.len() > 8
        {
            return Err(err(
                "finding requires bounded authored claim, conditions and evidence",
            ));
        }
        let reference = ResourceRef {
            kind: ResourceKind::Finding,
            work_id: finding.work_id.clone(),
            id: finding.id.clone(),
            version: "1".into(),
        };
        if !reference.valid() {
            return Err(err("invalid finding identity"));
        }
        self.admitted_work(campaign, &finding.work_id)?;
        for r in finding.evidence.iter().chain(&finding.parents) {
            let resolved = self.context_resolve(campaign, r, artifacts)?;
            if r.kind == ResourceKind::Attempt && resolved.data["phase"].get("Reviewed").is_none() {
                return Err(err("finding requires terminal attempt evidence"));
            }
        }
        let data = serde_json::to_value(&finding).map_err(err)?;
        let item = Resource {
            reference: reference.clone(),
            occurred_at_ms: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(err)?
                    .as_millis()
                    .try_into()
                    .map_err(err)?,
            ),
            data,
        };
        // Reserve room for the cursor and transport wrapper, even with JSON escaping.
        if serde_json::to_vec(&item).map_err(err)?.len() > 6000 {
            return Err(err("finding exceeds resource bound"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        {
            let mut table = tx.open_table(FINDINGS).map_err(err)?;
            let key = (campaign, finding.id.as_str());
            if let Some(row) = table.get(key).map_err(err)? {
                let old: Resource = serde_json::from_slice(row.value()).map_err(err)?;
                if old.reference != reference || old.data != item.data {
                    return Err(err("finding ID conflict"));
                }
                return Ok(reference);
            }
            let mut count = 0;
            for row in table.range((campaign, "")..).map_err(err)?.take(1024) {
                let (key, _) = row.map_err(err)?;
                if key.value().0 != campaign {
                    break;
                }
                count += 1;
            }
            if count >= 1024 {
                return Err(err("finding quota exceeded"));
            }
            table
                .insert(key, serde_json::to_vec(&item).map_err(err)?.as_slice())
                .map_err(err)?;
        }
        tx.commit().map_err(err)?;
        Ok(reference)
    }

    /// Explicit candidate ancestry, never inferred from a failed command. Declare roots
    /// with no parents first; requiring already-declared parents makes cycles impossible.
    pub(crate) fn host_record_candidate_lineage(
        &self,
        campaign: &str,
        candidate: ResourceRef,
        parents: Vec<ResourceRef>,
        artifacts: &ArtifactStore,
    ) -> Result<(), String> {
        if candidate.kind != ResourceKind::Artifact
            || parents.len() > 8
            || parents
                .iter()
                .any(|p| p.kind != ResourceKind::Artifact || p == &candidate)
        {
            return Err(err("invalid candidate lineage"));
        }
        let mut resource = self.context_resolve(campaign, &candidate, Some(artifacts))?;
        resource.data["parents"] = serde_json::to_value(&parents).map_err(err)?;
        if serde_json::to_vec(&resource).map_err(err)?.len() > 6000 {
            return Err(err("lineage exceeds resource bound"));
        }
        for parent in &parents {
            self.context_resolve(campaign, parent, Some(artifacts))?;
        }
        let links = Lineage { candidate, parents };
        let tx = self.database.begin_write().map_err(err)?;
        {
            let mut table = tx.open_table(LINEAGE).map_err(err)?;
            let key = (
                campaign,
                links.candidate.work_id.as_str(),
                links.candidate.id.as_str(),
            );
            if let Some(row) = table.get(key).map_err(err)? {
                let existing: Lineage = serde_json::from_slice(row.value()).map_err(err)?;
                return if existing == links {
                    Ok(())
                } else {
                    Err(err("immutable lineage conflict"))
                };
            }
            for parent in &links.parents {
                let row = table
                    .get((campaign, parent.work_id.as_str(), parent.id.as_str()))
                    .map_err(err)?
                    .ok_or("declare lineage parents first")?;
                let existing: Lineage = serde_json::from_slice(row.value()).map_err(err)?;
                if existing.candidate != *parent {
                    return Err(err("lineage parent version mismatch"));
                }
            }
            table
                .insert(key, serde_json::to_vec(&links).map_err(err)?.as_slice())
                .map_err(err)?;
        }
        tx.commit().map_err(err)
    }
}

fn matches_query(
    item: &Resource,
    query: &Query,
    kind: Option<ResourceKind>,
) -> Result<bool, String> {
    Ok(kind.is_none_or(|k| item.reference.kind == k)
        && query
            .since_ms
            .is_none_or(|t| item.occurred_at_ms.is_some_and(|at| at >= t))
        && query
            .version
            .as_ref()
            .is_none_or(|v| &item.reference.version == v)
        && match &query.literal {
            Some(s) => serde_json::to_string(item).map_err(err)?.contains(s),
            None => true,
        })
}

fn validate_artifact(resource: &Resource, artifacts: Option<&ArtifactStore>) -> Result<(), String> {
    if resource.reference.kind == ResourceKind::Artifact {
        if resource.data["size_bytes"]
            .as_u64()
            .is_none_or(|n| n > MAX_CONTEXT_ARTIFACT_BYTES)
        {
            return Err(err("artifact exceeds 4 MiB research read bound"));
        }
        artifacts.ok_or("artifact retrieval not configured")?.read(
            &resource.reference.work_id,
            &resource.reference.id,
            0,
            1,
        )?;
    }
    Ok(())
}

fn check_page(page: &Page) -> Result<(), String> {
    // Reserve the maximum cursor size while filling a page: a later row can have
    // a longer key, including when the row itself does not fit this page.
    let padding = match &page.next_cursor {
        Some(cursor) => 1024usize
            .checked_sub(serde_json::to_vec(cursor).map_err(err)?.len())
            .ok_or("oversized cursor")?,
        None => 0,
    };
    if serde_json::to_vec(&tachyon_model::broker::FrameReply::Control(
        tachyon_api::agents::Reply::Resource { page: page.clone() },
    ))
    .map_err(err)?
    .len()
        + padding
        > MAX_PAGE_BYTES
    {
        Err(err("page exceeds 8 KiB"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn research_context_cursor_bounds_include_json_escaping_and_utf8() {
        for key in ["\0".repeat(256), "\u{1f680}".repeat(64), "\\\"".repeat(128)] {
            let cursor = Cursor {
                campaign: "campaign-scoped".into(),
                stage: 0,
                key: key.clone(),
                index: 32,
            };
            let encoded = cursor.encode().unwrap();
            assert!(serde_json::to_vec(&encoded).unwrap().len() <= 1024);
            assert_eq!(Cursor::decode(&encoded).unwrap().key, key);
        }
    }
}
