//! Inert metadata only. No task, worker, scheduler, or actor writes belong here.
use std::ops::Bound::{Excluded, Unbounded};
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tachyon_api::types::{
    ApiRequest, ApiResponse, Campaign, CampaignStatus, Research, RESEARCH_COMMAND_ID_MAX_BYTES,
    RESEARCH_LIST_MAX_LIMIT, RESEARCH_OBJECTIVE_MAX_BYTES, RESEARCH_TITLE_MAX_BYTES,
};

use super::RuntimeStore;

const RESEARCH: TableDefinition<&str, &[u8]> = TableDefinition::new("research");
pub(super) const CAMPAIGNS: TableDefinition<&str, &[u8]> = TableDefinition::new("campaigns");
const BY_RESEARCH: TableDefinition<(&str, &str), u32> =
    TableDefinition::new("campaigns_by_research");
const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("research_creation_receipts");
const ARCHIVED: TableDefinition<&str, bool> = TableDefinition::new("local_retention_v1");

pub(super) fn initialize(write: &WriteTransaction) -> Result<(), String> {
    write.open_table(RESEARCH).map_err(err)?;
    write.open_table(CAMPAIGNS).map_err(err)?;
    write.open_table(BY_RESEARCH).map_err(err)?;
    write.open_table(RECEIPTS).map_err(err)?;
    write.open_table(ARCHIVED).map_err(err)?;
    Ok(())
}

fn err(error: impl std::fmt::Display) -> String {
    format!("research runtime store: {error}")
}

#[derive(Serialize, Deserialize)]
struct Record<T> {
    schema_version: u32,
    data: T,
}

fn encode<T: Serialize>(data: T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&Record {
        schema_version: 1,
        data,
    })
    .map_err(err)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let record: Record<T> = serde_json::from_slice(bytes).map_err(err)?;
    if record.schema_version != 1 {
        return Err(err(format!(
            "unsupported record schema {}",
            record.schema_version
        )));
    }
    Ok(record.data)
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    request: ApiRequest,
    result: Created,
}

#[derive(Serialize, Deserialize)]
enum Created {
    Research(Research),
    Campaign(Campaign),
}

impl Created {
    fn response(self) -> ApiResponse {
        match self {
            Self::Research(research) => ApiResponse::Research { research },
            Self::Campaign(campaign) => ApiResponse::Campaign { campaign },
        }
    }
}

fn text(value: &str, name: &str, max: usize) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > max {
        return Err(format!(
            "{name} must be nonblank and at most {max} UTF-8 bytes"
        ));
    }
    Ok(())
}

fn id(value: &str, prefix: &str) -> Result<(), String> {
    let suffix = value
        .strip_prefix(prefix)
        .ok_or_else(|| "invalid record ID".to_string())?;
    if suffix.len() != 32
        || !suffix
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid record ID".into());
    }
    Ok(())
}

trait StoredRecord: DeserializeOwned {
    fn validate(&self, key: &str, parent: Option<&str>) -> Result<(), String>;
}

impl StoredRecord for Research {
    fn validate(&self, key: &str, _parent: Option<&str>) -> Result<(), String> {
        id(&self.id, "research-")?;
        if self.id != key {
            return Err(err("record ID does not match table key"));
        }
        text(&self.title, "title", RESEARCH_TITLE_MAX_BYTES)?;
        text(&self.objective, "objective", RESEARCH_OBJECTIVE_MAX_BYTES)
    }
}

impl StoredRecord for Campaign {
    fn validate(&self, key: &str, parent: Option<&str>) -> Result<(), String> {
        id(&self.id, "campaign-")?;
        id(&self.research_id, "research-")?;
        if self.id != key {
            return Err(err("record ID does not match table key"));
        }
        if parent.is_some_and(|parent| parent != self.research_id) {
            return Err(err("campaign parent does not match index"));
        }
        text(&self.title, "title", RESEARCH_TITLE_MAX_BYTES)?;
        text(&self.objective, "objective", RESEARCH_OBJECTIVE_MAX_BYTES)
    }
}

impl RuntimeStore {
    pub(super) fn require_unarchived_in(tx: &WriteTransaction, key: &str) -> Result<(), String> {
        if tx
            .open_table(ARCHIVED)
            .map_err(err)?
            .get(key)
            .map_err(err)?
            .is_some()
        {
            return Err(
                "record is archived; explicitly restore before execution or creation".into(),
            );
        }
        Ok(())
    }

    pub(crate) fn retention_get(&self, key: &str) -> Result<ApiResponse, String> {
        if key.starts_with("campaign-") {
            self.research_request(&ApiRequest::CampaignGet { id: key.into() })?;
        } else {
            self.research_request(&ApiRequest::ResearchGet { id: key.into() })?;
        }
        let tx = self.database.begin_read().map_err(err)?;
        let archived = tx
            .open_table(ARCHIVED)
            .map_err(err)?
            .get(key)
            .map_err(err)?
            .is_some();
        Ok(ApiResponse::LocalRetention {
            id: key.into(),
            archived,
        })
    }

    /// Caller holds the campaign service lifecycle lock. No filesystem mutations.
    pub(super) fn retention_set(&self, key: &str, archived: bool) -> Result<ApiResponse, String> {
        let campaign = key.starts_with("campaign-");
        id(key, if campaign { "campaign-" } else { "research-" })?;
        let tx = self.database.begin_write().map_err(err)?;
        if campaign {
            let table = tx.open_table(CAMPAIGNS).map_err(err)?;
            let record: Campaign = decode(
                table
                    .get(key)
                    .map_err(err)?
                    .ok_or("no such campaign")?
                    .value(),
            )?;
            record.validate(key, None)?;
            if archived {
                if matches!(
                    record.status,
                    CampaignStatus::Running
                        | CampaignStatus::Cancelling
                        | CampaignStatus::AwaitingAcceptance
                ) {
                    return Err(
                        "archive requires an idle campaign with no active or waiting execution"
                            .into(),
                    );
                }
                // Unknown cleanup is not proof of quiescence. Keep it recoverable,
                // but require reconciliation before changing retention state.
                let mut settled_work = std::collections::BTreeSet::new();
                for (index, row) in tx
                    .open_table(super::execution::EXECUTIONS)
                    .map_err(err)?
                    .iter()
                    .map_err(err)?
                    .enumerate()
                {
                    if index >= 20_000 {
                        return Err("retention execution scan bound exceeded".into());
                    }
                    let (work, value) = row.map_err(err)?;
                    let execution = super::execution::decode_record(value.value(), work.value())?;
                    if execution.policy.funding.admission.campaign_id == key && !execution.settled {
                        return Err("archive refused: unsettled execution or uncertain cleanup; reconcile first".into());
                    }
                    if execution.policy.funding.admission.campaign_id == key && execution.settled {
                        settled_work.insert(work.value().to_owned());
                        settled_work.insert(execution.policy.verification.admission.work_id);
                    }
                }
                for (index, row) in tx
                    .open_table(super::admission::WORK)
                    .map_err(err)?
                    .iter()
                    .map_err(err)?
                    .enumerate()
                {
                    if index >= 20_000 {
                        return Err("retention admission scan bound exceeded".into());
                    }
                    let (work, value) = row.map_err(err)?;
                    let admission: super::admission::AdmittedWork =
                        serde_json::from_slice(value.value()).map_err(err)?;
                    if admission.admission.work_id != work.value() {
                        return Err("invalid admission identity".into());
                    }
                    if admission.admission.campaign_id != key {
                        continue;
                    }
                    if settled_work.contains(work.value()) {
                        continue;
                    }
                    if matches!(
                        admission.state,
                        super::admission::DispatchState::Admitted
                            | super::admission::DispatchState::DispatchingUnknown
                    ) {
                        return Err("archive refused: queued or uncertain dispatch".into());
                    }
                    if matches!(
                        admission.state,
                        super::admission::DispatchState::Registered { .. }
                    ) {
                        return Err(
                            "archive refused: registered work without settled execution".into()
                        );
                    }
                }
                for (index, row) in tx
                    .open_table(super::research_context::traces::TRACES)
                    .map_err(err)?
                    .range((key, "")..)
                    .map_err(err)?
                    .enumerate()
                {
                    let (trace, value) = row.map_err(err)?;
                    if trace.value().0 != key {
                        break;
                    }
                    if index >= 20_000 {
                        return Err("retention trace scan bound exceeded".into());
                    }
                    let resource: tachyon_api::context::Resource =
                        serde_json::from_slice(value.value()).map_err(err)?;
                    if resource.data["retention_state"] == "staging" {
                        return Err("archive refused: pending trace upload".into());
                    }
                }
            } else {
                Self::require_unarchived_in(&tx, &record.research_id)?;
            }
        } else {
            let table = tx.open_table(RESEARCH).map_err(err)?;
            let record: Research = decode(
                table
                    .get(key)
                    .map_err(err)?
                    .ok_or("no such research")?
                    .value(),
            )?;
            record.validate(key, None)?;
            if archived {
                let markers = tx.open_table(ARCHIVED).map_err(err)?;
                for (index, row) in tx
                    .open_table(BY_RESEARCH)
                    .map_err(err)?
                    .range((key, "")..)
                    .map_err(err)?
                    .enumerate()
                {
                    let (child, _) = row.map_err(err)?;
                    if child.value().0 != key {
                        break;
                    }
                    if index >= 20_000 {
                        return Err("retention research scan bound exceeded".into());
                    }
                    if markers.get(child.value().1).map_err(err)?.is_none() {
                        return Err("archive each campaign before archiving its research".into());
                    }
                }
            }
        }
        {
            let mut table = tx.open_table(ARCHIVED).map_err(err)?;
            if archived {
                table.insert(key, true).map_err(err)?;
            } else {
                table.remove(key).map_err(err)?;
            }
        }
        tx.commit().map_err(err)?;
        Ok(ApiResponse::LocalRetention {
            id: key.into(),
            archived,
        })
    }

    pub(super) fn campaign_status_in(
        tx: &WriteTransaction,
        key: &str,
    ) -> Result<CampaignStatus, String> {
        Self::require_unarchived_in(tx, key)?;
        let table = tx.open_table(CAMPAIGNS).map_err(err)?;
        let campaign: Campaign = decode(
            table
                .get(key)
                .map_err(err)?
                .ok_or("no such campaign")?
                .value(),
        )?;
        campaign.validate(key, None)?;
        Ok(campaign.status)
    }

    pub(super) fn set_campaign_status_in(
        tx: &WriteTransaction,
        key: &str,
        status: CampaignStatus,
    ) -> Result<Campaign, String> {
        if status == CampaignStatus::Running {
            Self::require_unarchived_in(tx, key)?;
        }
        let mut table = tx.open_table(CAMPAIGNS).map_err(err)?;
        let mut campaign: Campaign = decode(
            table
                .get(key)
                .map_err(err)?
                .ok_or("no such campaign")?
                .value(),
        )?;
        campaign.validate(key, None)?;
        campaign.status = status;
        table
            .insert(key, encode(&campaign)?.as_slice())
            .map_err(err)?;
        Ok(campaign)
    }

    pub(crate) fn research_request(&self, request: &ApiRequest) -> Result<ApiResponse, String> {
        match request {
            ApiRequest::ResearchCreate {
                command_id,
                title,
                objective,
            }
            | ApiRequest::CampaignCreate {
                command_id,
                title,
                objective,
                ..
            } => {
                text(command_id, "command_id", RESEARCH_COMMAND_ID_MAX_BYTES)?;
                text(title, "title", RESEARCH_TITLE_MAX_BYTES)?;
                text(objective, "objective", RESEARCH_OBJECTIVE_MAX_BYTES)?;
                let research_id = match request {
                    ApiRequest::CampaignCreate { research_id, .. } => {
                        id(research_id, "research-")?;
                        Some(research_id)
                    }
                    _ => None,
                };
                // redb serializes writers, so receipt lookup, FK check, record,
                // index, and receipt commit form one atomic creation decision.
                let write = self.database.begin_write().map_err(err)?;
                let mut receipts = write.open_table(RECEIPTS).map_err(err)?;
                if let Some(value) = receipts.get(command_id.as_str()).map_err(err)? {
                    let receipt: Receipt = decode(value.value())?;
                    if receipt.request != *request {
                        return Err("creation command_id conflict".into());
                    }
                    match (&receipt.request, &receipt.result) {
                        (
                            ApiRequest::ResearchCreate {
                                title, objective, ..
                            },
                            Created::Research(record),
                        ) => {
                            record.validate(&record.id, None)?;
                            if record.title != *title || record.objective != *objective {
                                return Err(err("creation receipt payload mismatch"));
                            }
                        }
                        (
                            ApiRequest::CampaignCreate {
                                research_id,
                                title,
                                objective,
                                ..
                            },
                            Created::Campaign(record),
                        ) => {
                            record.validate(&record.id, Some(research_id))?;
                            if record.title != *title || record.objective != *objective {
                                return Err(err("creation receipt payload mismatch"));
                            }
                        }
                        _ => return Err(err("invalid creation receipt operation")),
                    }
                    return Ok(receipt.result.response());
                }
                let created_at_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(err)?
                    .as_millis()
                    .try_into()
                    .map_err(err)?;
                let suffix = uuid::Uuid::new_v4().simple().to_string();
                let result = if let Some(research_id) = research_id {
                    let research = write.open_table(RESEARCH).map_err(err)?;
                    let value = research
                        .get(research_id.as_str())
                        .map_err(err)?
                        .ok_or_else(|| "no such research".to_string())?;
                    let parent: Research = decode(value.value())?;
                    parent.validate(research_id, None)?;
                    Self::require_unarchived_in(&write, research_id)?;
                    let campaign = Campaign {
                        id: format!("campaign-{suffix}"),
                        research_id: research_id.clone(),
                        title: title.clone(),
                        objective: objective.clone(),
                        created_at_ms,
                        status: CampaignStatus::Draft,
                    };
                    let mut campaigns = write.open_table(CAMPAIGNS).map_err(err)?;
                    if campaigns.get(campaign.id.as_str()).map_err(err)?.is_some() {
                        return Err(err("generated campaign ID collision"));
                    }
                    campaigns
                        .insert(campaign.id.as_str(), encode(&campaign)?.as_slice())
                        .map_err(err)?;
                    write
                        .open_table(BY_RESEARCH)
                        .map_err(err)?
                        .insert((research_id.as_str(), campaign.id.as_str()), 1)
                        .map_err(err)?;
                    Created::Campaign(campaign)
                } else {
                    let research = Research {
                        id: format!("research-{suffix}"),
                        title: title.clone(),
                        objective: objective.clone(),
                        created_at_ms,
                    };
                    let mut records = write.open_table(RESEARCH).map_err(err)?;
                    if records.get(research.id.as_str()).map_err(err)?.is_some() {
                        return Err(err("generated research ID collision"));
                    }
                    records
                        .insert(research.id.as_str(), encode(&research)?.as_slice())
                        .map_err(err)?;
                    Created::Research(research)
                };
                let receipt = Receipt {
                    request: request.clone(),
                    result,
                };
                receipts
                    .insert(command_id.as_str(), encode(&receipt)?.as_slice())
                    .map_err(err)?;
                drop(receipts);
                write.commit().map_err(err)?;
                Ok(receipt.result.response())
            }
            ApiRequest::ResearchGet { id: key } => {
                id(key, "research-")?;
                Ok(ApiResponse::Research {
                    research: self.get_record(RESEARCH, key)?,
                })
            }
            ApiRequest::CampaignGet { id: key } => {
                id(key, "campaign-")?;
                Ok(ApiResponse::Campaign {
                    campaign: self.get_record(CAMPAIGNS, key)?,
                })
            }
            ApiRequest::ResearchList { after, limit } => {
                let (records, next_after) =
                    self.list_records(RESEARCH, "research-", None, after.as_deref(), *limit)?;
                Ok(ApiResponse::ResearchList {
                    records,
                    next_after,
                })
            }
            ApiRequest::CampaignList {
                research_id,
                after,
                limit,
            } => {
                if let Some(key) = research_id {
                    id(key, "research-")?;
                }
                let (campaigns, next_after) = self.list_records(
                    CAMPAIGNS,
                    "campaign-",
                    research_id.as_deref(),
                    after.as_deref(),
                    *limit,
                )?;
                Ok(ApiResponse::CampaignList {
                    campaigns,
                    next_after,
                })
            }
            _ => Err("not a research/campaign request".into()),
        }
    }

    fn get_record<T: StoredRecord>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        key: &str,
    ) -> Result<T, String> {
        let read = self.database.begin_read().map_err(err)?;
        let records = read.open_table(table).map_err(err)?;
        let value = records
            .get(key)
            .map_err(err)?
            .ok_or_else(|| "no such record".to_string())?;
        let record: T = decode(value.value())?;
        record.validate(key, None)?;
        Ok(record)
    }

    fn list_records<T: StoredRecord>(
        &self,
        table: TableDefinition<&str, &[u8]>,
        prefix: &str,
        research_id: Option<&str>,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<T>, Option<String>), String> {
        if limit == 0 || limit > RESEARCH_LIST_MAX_LIMIT {
            return Err(format!("limit must be 1..={RESEARCH_LIST_MAX_LIMIT}"));
        }
        if let Some(after) = after {
            id(after, prefix)?;
        }
        let read = self.database.begin_read().map_err(err)?;
        let records = read.open_table(table).map_err(err)?;
        let mut page = Vec::new();
        let mut last = None;
        let mut more = false;
        if let Some(research_id) = research_id {
            let index = read.open_table(BY_RESEARCH).map_err(err)?;
            let range = index
                .range((Excluded((research_id, after.unwrap_or(""))), Unbounded))
                .map_err(err)?;
            for entry in range {
                let (key, version) = entry.map_err(err)?;
                let (parent, key) = key.value();
                if parent != research_id {
                    break;
                }
                if version.value() != 1 {
                    return Err(err("unsupported campaign index schema"));
                }
                if page.len() == limit as usize {
                    more = true;
                    break;
                }
                let value = records
                    .get(key)
                    .map_err(err)?
                    .ok_or_else(|| err("dangling campaign index"))?;
                let record: T = decode(value.value())?;
                record.validate(key, Some(research_id))?;
                page.push(record);
                last = Some(key.to_string());
            }
        } else {
            for entry in records
                .range::<&str>((Excluded(after.unwrap_or("")), Unbounded))
                .map_err(err)?
            {
                let (key, value) = entry.map_err(err)?;
                if page.len() == limit as usize {
                    more = true;
                    break;
                }
                let record: T = decode(value.value())?;
                record.validate(key.value(), None)?;
                page.push(record);
                last = Some(key.value().to_string());
            }
        }
        Ok((page, if more { last } else { None }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn research(command: &str) -> ApiRequest {
        ApiRequest::ResearchCreate {
            command_id: command.into(),
            title: "Research title".into(),
            objective: "Investigate".into(),
        }
    }

    fn campaign(command: &str, parent: &str) -> ApiRequest {
        ApiRequest::CampaignCreate {
            command_id: command.into(),
            research_id: parent.into(),
            title: "Campaign title".into(),
            objective: "Describe a future investigation".into(),
        }
    }

    fn run(store: &RuntimeStore, req: &ApiRequest) -> serde_json::Value {
        serde_json::to_value(store.research_request(req).unwrap()).unwrap()
    }

    #[test]
    fn creation_reopens_replays_and_conflicts_across_operations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let r = run(&store, &research("r"));
        let rid = r["research"]["id"].as_str().unwrap();
        let req = campaign("c", rid);
        let c = run(&store, &req);
        assert_eq!(c["campaign"]["status"], "draft");
        assert!(r["research"]["created_at_ms"].as_u64().unwrap() > 0);
        assert!(c["campaign"]["created_at_ms"].as_u64().unwrap() > 0);
        assert!(store
            .research_request(&campaign("r", rid))
            .unwrap_err()
            .contains("conflict"));
        assert!(store
            .research_request(&research("c"))
            .unwrap_err()
            .contains("conflict"));
        let mut changed = research("r");
        if let ApiRequest::ResearchCreate { title, .. } = &mut changed {
            title.push('!');
        }
        assert!(store
            .research_request(&changed)
            .unwrap_err()
            .contains("conflict"));
        let mut changed = req.clone();
        if let ApiRequest::CampaignCreate { objective, .. } = &mut changed {
            objective.push('!');
        }
        assert!(store
            .research_request(&changed)
            .unwrap_err()
            .contains("conflict"));
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(run(&store, &research("r")), r);
        assert_eq!(run(&store, &req), c);
        assert_eq!(run(&store, &ApiRequest::ResearchGet { id: rid.into() }), r);
        assert_eq!(
            run(
                &store,
                &ApiRequest::CampaignGet {
                    id: c["campaign"]["id"].as_str().unwrap().into()
                }
            ),
            c
        );
        assert!(store.list_tasks().unwrap().is_empty());
        assert!(store.scheduled_tasks().unwrap().is_empty());
        assert!(store.pending_history().unwrap().is_empty());
    }

    #[test]
    fn concurrent_duplicates_commit_once_and_different_payload_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            std::sync::Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    run(&store, &research("same"))
                })
            })
            .collect();
        let values: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(values.iter().all(|v| *v == values[0]));
        let parent = values[0]["research"]["id"].as_str().unwrap().to_string();
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                let mut req = campaign("race", &parent);
                if let ApiRequest::CampaignCreate { title, .. } = &mut req {
                    *title = format!("title {i}");
                }
                std::thread::spawn(move || {
                    barrier.wait();
                    store.research_request(&req)
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .all(|e| e.contains("conflict")));
        let read = store.database.begin_read().unwrap();
        use redb::ReadableTableMetadata;
        assert_eq!(read.open_table(RESEARCH).unwrap().len().unwrap(), 1);
        assert_eq!(read.open_table(CAMPAIGNS).unwrap().len().unwrap(), 1);
        assert_eq!(read.open_table(RECEIPTS).unwrap().len().unwrap(), 2);
        assert_eq!(read.open_table(BY_RESEARCH).unwrap().len().unwrap(), 1);
    }

    #[test]
    fn missing_parent_and_invalid_input_leave_no_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let absent = format!("research-{}", "0".repeat(32));
        assert!(store
            .research_request(&campaign("retry", &absent))
            .unwrap_err()
            .contains("no such research"));
        assert!(store
            .research_request(&ApiRequest::ResearchGet { id: absent.clone() })
            .is_err());
        assert!(store
            .research_request(&ApiRequest::CampaignGet {
                id: format!("campaign-{}", "0".repeat(32))
            })
            .is_err());
        for (field, max) in [
            ("title", RESEARCH_TITLE_MAX_BYTES),
            ("objective", RESEARCH_OBJECTIVE_MAX_BYTES),
            ("command_id", RESEARCH_COMMAND_ID_MAX_BYTES),
        ] {
            for value in [" ".into(), "x".repeat(max + 1)] {
                let mut req = research("retry");
                if let ApiRequest::ResearchCreate {
                    title,
                    objective,
                    command_id,
                } = &mut req
                {
                    *match field {
                        "title" => title,
                        "objective" => objective,
                        _ => command_id,
                    } = value;
                }
                assert!(store.research_request(&req).is_err());
            }
        }
        for limit in [0, RESEARCH_LIST_MAX_LIMIT + 1, u32::MAX] {
            assert!(store
                .research_request(&ApiRequest::ResearchList { after: None, limit })
                .is_err());
            assert!(store
                .research_request(&ApiRequest::CampaignList {
                    research_id: None,
                    after: None,
                    limit
                })
                .is_err());
        }
        assert!(store
            .research_request(&ApiRequest::ResearchList {
                after: Some("x".repeat(1000)),
                limit: 1
            })
            .is_err());
        let read = store.database.begin_read().unwrap();
        use redb::ReadableTableMetadata;
        assert_eq!(read.open_table(RECEIPTS).unwrap().len().unwrap(), 0);
        drop(read);
        let valid = ApiRequest::ResearchCreate {
            command_id: "x".repeat(RESEARCH_COMMAND_ID_MAX_BYTES),
            title: "x".repeat(RESEARCH_TITLE_MAX_BYTES),
            objective: "x".repeat(RESEARCH_OBJECTIVE_MAX_BYTES),
        };
        run(&store, &valid);
        // A failed FK check did not reserve the command ID.
        run(&store, &research("retry"));
    }

    #[test]
    fn exclusive_pages_are_sorted_bounded_and_filter_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let mut parents = Vec::new();
        let mut campaigns = Vec::new();
        for i in 0..5 {
            let r = run(&store, &research(&format!("r{i}")));
            let parent = r["research"]["id"].as_str().unwrap().to_string();
            for j in 0..5 {
                let c = run(&store, &campaign(&format!("c{i}-{j}"), &parent));
                campaigns.push((
                    parent.clone(),
                    c["campaign"]["id"].as_str().unwrap().to_string(),
                ));
            }
            parents.push(parent);
        }
        parents.sort();
        campaigns.sort_by(|a, b| a.1.cmp(&b.1));
        for filter in [
            None,
            Some(parents[2].clone()),
            Some(format!("research-{}", "0".repeat(32))),
        ] {
            let mut after = None;
            let mut found = Vec::new();
            loop {
                let ApiResponse::CampaignList {
                    campaigns,
                    next_after,
                } = store
                    .research_request(&ApiRequest::CampaignList {
                        research_id: filter.clone(),
                        after: after.clone(),
                        limit: 2,
                    })
                    .unwrap()
                else {
                    panic!()
                };
                assert!(campaigns.len() <= 2);
                for c in campaigns {
                    assert!(after.as_ref().is_none_or(|a| c.id > *a));
                    found.push(c.id);
                }
                if next_after.is_none() {
                    break;
                }
                after = next_after;
            }
            let expected: Vec<_> = campaigns
                .iter()
                .filter(|(r, _)| filter.as_ref().is_none_or(|f| r == f))
                .map(|(_, c)| c.clone())
                .collect();
            assert_eq!(found, expected);
        }
        let mut after = None;
        let mut found = Vec::new();
        loop {
            let ApiResponse::ResearchList {
                records,
                next_after,
            } = store
                .research_request(&ApiRequest::ResearchList { after, limit: 2 })
                .unwrap()
            else {
                panic!()
            };
            found.extend(records.into_iter().map(|r| r.id));
            if next_after.is_none() {
                break;
            }
            after = next_after;
        }
        assert_eq!(found, parents);
        // A syntactically valid cursor need not identify an existing record.
        let page = run(
            &store,
            &ApiRequest::ResearchList {
                after: Some(format!("research-{}", "f".repeat(32))),
                limit: 1,
            },
        );
        assert_eq!(page["records"], serde_json::json!([]));
        assert!(page["next_after"].is_null());
    }

    #[test]
    fn old_v1_database_adds_tables_without_changing_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        {
            let db = redb::Database::create(&path).unwrap();
            let write = db.begin_write().unwrap();
            {
                let mut metadata = write.open_table(super::super::METADATA).unwrap();
                metadata
                    .insert(super::super::SCHEMA_VERSION_KEY, 1)
                    .unwrap();
                metadata
                    .insert(super::super::NEXT_EVENT_SEQUENCE_KEY, 42)
                    .unwrap();
            }
            write.commit().unwrap();
        }
        let store = RuntimeStore::open(&path).unwrap();
        run(&store, &research("r"));
        let read = store.database.begin_read().unwrap();
        let metadata = read.open_table(super::super::METADATA).unwrap();
        assert_eq!(
            metadata
                .get(super::super::SCHEMA_VERSION_KEY)
                .unwrap()
                .unwrap()
                .value(),
            1
        );
        assert_eq!(
            metadata
                .get(super::super::NEXT_EVENT_SEQUENCE_KEY)
                .unwrap()
                .unwrap()
                .value(),
            42
        );
    }

    #[test]
    fn unknown_record_receipt_index_and_database_versions_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let r = run(&store, &research("r"));
        let rid = r["research"]["id"].as_str().unwrap();
        let c = run(&store, &campaign("c", rid));
        let cid = c["campaign"]["id"].as_str().unwrap();
        for (table, key) in [(RESEARCH, rid), (CAMPAIGNS, cid), (RECEIPTS, "r")] {
            let write = store.database.begin_write().unwrap();
            {
                let mut records = write.open_table(table).unwrap();
                let mut value: serde_json::Value =
                    serde_json::from_slice(records.get(key).unwrap().unwrap().value()).unwrap();
                value["schema_version"] = 2.into();
                records
                    .insert(key, serde_json::to_vec(&value).unwrap().as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
        }
        for req in [
            research("r"),
            ApiRequest::ResearchGet { id: rid.into() },
            ApiRequest::CampaignGet { id: cid.into() },
            ApiRequest::ResearchList {
                after: None,
                limit: 10,
            },
            ApiRequest::CampaignList {
                research_id: None,
                after: None,
                limit: 10,
            },
            campaign("new", rid),
        ] {
            assert!(store
                .research_request(&req)
                .unwrap_err()
                .contains("unsupported record schema"));
        }
        let write = store.database.begin_write().unwrap();
        write
            .open_table(BY_RESEARCH)
            .unwrap()
            .insert((rid, cid), 2)
            .unwrap();
        write.commit().unwrap();
        assert!(store
            .research_request(&ApiRequest::CampaignList {
                research_id: Some(rid.into()),
                after: None,
                limit: 10
            })
            .unwrap_err()
            .contains("index schema"));
        let write = store.database.begin_write().unwrap();
        write
            .open_table(super::super::METADATA)
            .unwrap()
            .insert(super::super::SCHEMA_VERSION_KEY, 2)
            .unwrap();
        write.commit().unwrap();
        drop(store);
        assert!(RuntimeStore::open(&path)
            .err()
            .unwrap()
            .contains("unsupported runtime database schema"));
    }

    #[test]
    fn blocked_creation_does_not_hold_registry_or_block_other_connections() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let registry = Arc::new(Mutex::new(crate::Registry {
            runtime_store: Some(store.clone()),
            ..crate::Registry::default()
        }));
        let write = store.database.begin_write().unwrap();
        let other_registry = registry.clone();
        let handler =
            std::thread::spawn(move || crate::dispatch(&research("blocked"), &other_registry));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        // The dispatch clone proves it has entered the store branch. It cannot
        // finish until this test releases redb's exclusive writer transaction.
        while Arc::strong_count(&store) < 3 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(Arc::strong_count(&store), 3);
        while registry.try_lock().is_err() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(registry.try_lock().is_ok());
        assert!(
            matches!(crate::dispatch(&ApiRequest::ResearchList { after: None, limit: 1 }, &registry), ApiResponse::ResearchList { records, .. } if records.is_empty())
        );
        drop(write);
        assert!(matches!(
            handler.join().unwrap(),
            ApiResponse::Research { .. }
        ));
    }

    #[test]
    fn persisted_payloads_must_match_keys_indexes_and_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let r = run(&store, &research("r"));
        let rid = r["research"]["id"].as_str().unwrap();
        let c = run(&store, &campaign("c", rid));
        let cid = c["campaign"]["id"].as_str().unwrap();
        let other = format!("research-{}", "0".repeat(32));
        for (table, key, pointer, replacement, requests) in [
            (
                RESEARCH,
                rid,
                "/data/id",
                other.as_str(),
                vec![
                    ApiRequest::ResearchGet { id: rid.into() },
                    ApiRequest::ResearchList {
                        after: None,
                        limit: 10,
                    },
                    campaign("invalid-parent", rid),
                ],
            ),
            (
                CAMPAIGNS,
                cid,
                "/data/research_id",
                other.as_str(),
                vec![ApiRequest::CampaignList {
                    research_id: Some(rid.into()),
                    after: None,
                    limit: 10,
                }],
            ),
            (
                CAMPAIGNS,
                cid,
                "/data/id",
                "campaign-invalid",
                vec![
                    ApiRequest::CampaignGet { id: cid.into() },
                    ApiRequest::CampaignList {
                        research_id: None,
                        after: None,
                        limit: 10,
                    },
                ],
            ),
            (
                RESEARCH,
                rid,
                "/data/title",
                " ",
                vec![ApiRequest::ResearchGet { id: rid.into() }],
            ),
            (
                RECEIPTS,
                "r",
                "/data/result/Research/title",
                "different",
                vec![research("r")],
            ),
            (
                RECEIPTS,
                "c",
                "/data/result/Campaign/research_id",
                other.as_str(),
                vec![campaign("c", rid)],
            ),
        ] {
            let write = store.database.begin_write().unwrap();
            let original;
            {
                let mut records = write.open_table(table).unwrap();
                original = records.get(key).unwrap().unwrap().value().to_vec();
                let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
                *value.pointer_mut(pointer).unwrap() = replacement.into();
                records
                    .insert(key, serde_json::to_vec(&value).unwrap().as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
            for request in requests {
                assert!(store.research_request(&request).is_err(), "{request:?}");
            }
            let write = store.database.begin_write().unwrap();
            write
                .open_table(table)
                .unwrap()
                .insert(key, original.as_slice())
                .unwrap();
            write.commit().unwrap();
        }
        assert_eq!(run(&store, &research("r")), r);
        assert_eq!(run(&store, &campaign("c", rid)), c);
        let read = store.database.begin_read().unwrap();
        assert!(read
            .open_table(RECEIPTS)
            .unwrap()
            .get("invalid-parent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn documented_metadata_examples_have_valid_wire_shapes_and_ids() {
        let docs = include_str!("../../../../docs/ghost/API.md");
        let metadata = docs.split("## CURRENT: Ghost Tool Calls").next().unwrap();
        for block in metadata.split("```json\n").skip(1) {
            let json = block.split("```").next().unwrap();
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            if value.get("cmd").is_some() {
                serde_json::from_value::<ApiRequest>(value.clone()).unwrap();
            } else {
                serde_json::from_value::<ApiResponse>(value.clone()).unwrap();
            }
            for key in [
                value.get("id"),
                value.get("research_id"),
                value.pointer("/research/id"),
            ]
            .into_iter()
            .flatten()
            {
                let key = key.as_str().unwrap();
                id(
                    key,
                    if key.starts_with("campaign-") {
                        "campaign-"
                    } else {
                        "research-"
                    },
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn malformed_or_unversioned_records_are_not_accepted() {
        assert!(decode::<Research>(b"not json").is_err());
        assert!(decode::<Research>(
            br#"{"data":{"id":"r","title":"t","objective":"o","created_at_ms":1}}"#
        )
        .is_err());
        assert!(decode::<Receipt>(br#"{"schema_version":1,"data":{}}"#).is_err());
    }
}
