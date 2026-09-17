//! Durable conservative native job wall-time holds. No hardware probing.
use super::{
    campaign_launch::{Launch, LAUNCHES},
    RuntimeStore,
};
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};
use tachyon_model::{
    accounting::WorkIdentity,
    broker::{job_duration_ms, JobWorkload},
};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

pub(super) const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("native_jobs_v1");

#[derive(Default)]
pub(super) struct State {
    live: HashMap<Uuid, (Instant, Option<OwnedSemaphorePermit>)>,
    queue: VecDeque<(Uuid, String, JobWorkload, Instant)>,
    campaigns: [VecDeque<String>; 2],
}

#[derive(Serialize, Deserialize)]
struct Record {
    #[serde(default)]
    profile: String,
    session: Uuid,
    identity: WorkIdentity,
    workload: JobWorkload,
    reserved_ms: u64,
    device_ids: Vec<String>,
    // None retains both the entire allowance and physical occupancy after restart.
    final_ms: Option<u64>,
}

pub(super) fn monitor_in(
    tx: &redb::ReadTransaction,
    queries: &[tachyon_api::monitor::MonitorQuery],
    output: &mut [Result<
        tachyon_api::monitor::MonitorPayload,
        tachyon_api::monitor::MonitorError,
    >],
) -> Result<(), String> {
    use super::monitor::add;
    let table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
    for row in table.iter().map_err(|e| e.to_string())? {
        let (_, value) = row.map_err(|e| e.to_string())?;
        let record: Record = serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
        for (query, output) in queries.iter().zip(output.iter_mut()) {
            if !query
                .scope
                .matches(&record.identity.campaign_id, Some(&record.identity.work_id))
            {
                continue;
            }
            let Ok(output) = output else {
                continue;
            };
            let jobs = &mut output.durable.native_jobs;
            let gpu = record.workload == (JobWorkload::Gpu {});
            add(
                if gpu {
                    &mut jobs.gpu_charged_wall_ms
                } else {
                    &mut jobs.cpu_charged_wall_ms
                },
                record.final_ms.unwrap_or(record.reserved_ms).into(),
            )?;
            if record.final_ms.is_none() {
                add(&mut jobs.unresolved, 1)?;
                add(
                    if gpu {
                        &mut jobs.gpu_unresolved
                    } else {
                        &mut jobs.cpu_unresolved
                    },
                    1,
                )?;
            } else {
                add(&mut jobs.finalized, 1)?;
            }
        }
    }
    Ok(())
}

impl State {
    pub(super) fn monitor(
        &self,
        cpu_limit: usize,
        gpu_limit: usize,
        cpu_available: usize,
    ) -> Vec<tachyon_api::monitor::Capacity> {
        use tachyon_api::monitor::*;
        [
            (
                CapacityResource::Cpu,
                cpu_limit,
                cpu_limit.saturating_sub(cpu_available),
                JobWorkload::Cpu {},
            ),
            (
                CapacityResource::Gpu,
                gpu_limit,
                self.live
                    .values()
                    .filter(|(_, permit)| permit.is_none())
                    .count(),
                JobWorkload::Gpu {},
            ),
        ]
        .into_iter()
        .map(|(resource, limit, held, workload)| Capacity {
            resource,
            sampled_at_ms: super::monitor::now_ms(),
            limit: Decimal(limit as u128),
            held: Decimal(held as u128),
            queued: Decimal(
                self.queue
                    .iter()
                    .filter(|(_, _, class, _)| *class == workload)
                    .count() as u128,
            ),
            // Durable unresolved occupancy is reported separately in NativeJobs.
            unresolved: Observed::Unknown,
        })
        .collect()
    }
}

#[cfg(test)]
mod monitor_tests {
    use super::*;
    use tachyon_api::monitor::*;
    #[test]
    fn monitor_native_wall_charge_keeps_unresolved_and_zero_final_separate() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let tx = store.database.begin_write().unwrap();
        {
            let mut table = tx.open_table(JOBS).unwrap();
            for (id, workload, final_ms) in [
                ("a", JobWorkload::Cpu {}, None),
                ("b", JobWorkload::Gpu {}, Some(0)),
                ("c", JobWorkload::Cpu {}, Some(u64::MAX)),
            ] {
                let record = Record {
                    profile: "SECRET_PROFILE".into(),
                    session: Uuid::new_v4(),
                    identity: WorkIdentity {
                        campaign_id: "c".into(),
                        work_id: "w".into(),
                        attempt_id: "SECRET_ATTEMPT".into(),
                        generation: 1,
                        instruction_revision: 1,
                        class: tachyon_model::accounting::RequestClass::Work,
                    },
                    workload,
                    reserved_ms: u64::MAX,
                    device_ids: vec!["SECRET_DEVICE".into()],
                    final_ms,
                };
                table
                    .insert(id, serde_json::to_vec(&record).unwrap().as_slice())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
        let q = MonitorQuery {
            scope: MonitorScope::Host,
            after: None,
            limit: 100,
        };
        let p = store.monitor_sample(&[q]).unwrap().remove(0).unwrap();
        assert_eq!(
            p.durable.native_jobs.cpu_charged_wall_ms.0,
            u128::from(u64::MAX) * 2
        );
        assert_eq!(p.durable.native_jobs.gpu_charged_wall_ms.0, 0);
        assert_eq!(p.durable.native_jobs.unresolved.0, 1);
        assert_eq!(p.durable.native_jobs.finalized.0, 2);
        assert!(!serde_json::to_string(&p).unwrap().contains("SECRET"));
    }
}

impl RuntimeStore {
    pub(super) fn cleanup_job_in(
        tx: &redb::WriteTransaction,
        campaign: &str,
        work: &str,
        attempt: &str,
        generation: u64,
        lease: &str,
    ) -> Result<(), String> {
        let mut table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        let mut record: Record = serde_json::from_slice(
            table
                .get(lease)
                .map_err(|e| e.to_string())?
                .ok_or("unknown native lease")?
                .value(),
        )
        .map_err(|e| e.to_string())?;
        if record.identity.campaign_id != campaign
            || record.identity.work_id != work
            || record.identity.attempt_id != attempt
            || record.identity.generation != generation
        {
            return Err("native cleanup identity mismatch".into());
        }
        record.final_ms.get_or_insert(record.reserved_ms);
        table
            .insert(
                lease,
                serde_json::to_vec(&record)
                    .map_err(|e| e.to_string())?
                    .as_slice(),
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub(super) fn inspect_jobs_in(
        tx: &redb::WriteTransaction,
        campaign: &str,
    ) -> Result<Vec<String>, String> {
        let table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        let mut output = Vec::new();
        for row in table.iter().map_err(|e| e.to_string())? {
            let (id, value) = row.map_err(|e| e.to_string())?;
            let record: Record =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            if record.identity.campaign_id == campaign {
                output.push(format!("native_job_wall_time: {}", serde_json::json!({"lease_id":id.value(),"identity":record.identity,"workload":record.workload,"reserved_ms":record.reserved_ms,"final_ms":record.final_ms,"cleanup_unknown":record.final_ms.is_none()})));
            }
        }
        Ok(output)
    }

    pub(super) fn compute_bounds(
        &self,
        identity: &WorkIdentity,
    ) -> Result<(String, tachyon_api::campaign::ComputeProfile), String> {
        use tachyon_api::campaign::ComputeProfile;
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(LAUNCHES).map_err(|e| e.to_string())?;
        let Some(value) = table
            .get(identity.campaign_id.as_str())
            .map_err(|e| e.to_string())?
        else {
            return Ok((
                String::new(),
                ComputeProfile {
                    max_gpu_jobs: 0,
                    max_cpu_timeout_ms: 120_000,
                    max_gpu_timeout_ms: 0,
                },
            ));
        };
        let launch: Launch = serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
        launch.validate_digest()?;
        let m = launch.manifest;
        let Some(p) = m.compute else {
            return Ok((
                String::new(),
                ComputeProfile {
                    max_gpu_jobs: 0,
                    max_cpu_timeout_ms: 120_000,
                    max_gpu_timeout_ms: 0,
                },
            ));
        };
        if identity.work_id == format!("{}-root", identity.campaign_id) {
            return Ok((
                String::new(),
                ComputeProfile {
                    max_gpu_jobs: p.max_gpu_jobs,
                    max_cpu_timeout_ms: p.max_cpu_timeout_ms,
                    max_gpu_timeout_ms: p.max_gpu_timeout_ms,
                },
            ));
        }
        let key = m.children.as_ref().and_then(|children| {
            children
                .dynamic
                .iter()
                .flat_map(|d| &d.profiles)
                .find(|profile| {
                    profile
                        .slots(&identity.campaign_id)
                        .specs
                        .iter()
                        .any(|spec| spec.work_id.as_ref() == Some(&identity.work_id))
                })
                .map(|p| p.profile_id.clone())
                .or_else(|| {
                    children
                        .templates
                        .iter()
                        .find(|t| {
                            t.specs.iter().enumerate().any(|(index, spec)| {
                                spec.resolved_id(&identity.campaign_id, &t.template_id, index)
                                    == identity.work_id
                            })
                        })
                        .map(|t| t.template_id.clone())
                })
        });
        let bounds = key
            .as_ref()
            .and_then(|key| p.profiles.get(key))
            .cloned()
            .unwrap_or(ComputeProfile {
                max_gpu_jobs: 0,
                max_cpu_timeout_ms: p.max_cpu_timeout_ms,
                max_gpu_timeout_ms: 0,
            });
        Ok((key.unwrap_or_else(|| identity.work_id.clone()), bounds))
    }

    /// Called inside the exact operator cleanup receipt transaction. Unknown cost
    /// remains charged at the full reservation; cleanup is not a zero-cost report.
    pub(super) fn cleanup_jobs_in(
        tx: &redb::WriteTransaction,
        campaign: &str,
        work: &str,
        attempt: &str,
        generation: u64,
    ) -> Result<(), String> {
        let mut table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        let mut updates = Vec::new();
        for row in table.iter().map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let mut record: Record =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            if record.identity.campaign_id == campaign
                && record.identity.work_id == work
                && record.identity.attempt_id == attempt
                && record.identity.generation == generation
                && record.final_ms.is_none()
            {
                record.final_ms = Some(record.reserved_ms);
                updates.push((
                    key.value().to_owned(),
                    serde_json::to_vec(&record).map_err(|e| e.to_string())?,
                ));
            }
        }
        for (id, bytes) in updates {
            table
                .insert(id.as_str(), bytes.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub(super) fn release_cleaned_jobs(&self) -> Result<(), String> {
        let mut state = self
            .compute
            .lock()
            .map_err(|_| "compute authority unavailable")?;
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        let mut released = Vec::new();
        for id in state.live.keys() {
            let record: Record = serde_json::from_slice(
                table
                    .get(id.to_string().as_str())
                    .map_err(|e| e.to_string())?
                    .ok_or("missing native job")?
                    .value(),
            )
            .map_err(|e| e.to_string())?;
            if record.final_ms.is_some() {
                released.push(*id);
            }
        }
        for id in released {
            state.live.remove(&id);
        }
        Ok(())
    }

    pub(super) fn compute_profile(
        &self,
        campaign: &str,
    ) -> Result<Option<tachyon_api::campaign::ComputeEnvelope>, String> {
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx.open_table(LAUNCHES).map_err(|e| e.to_string())?;
        let Some(value) = table.get(campaign).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        let launch: Launch = serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
        launch.validate_digest()?;
        Ok(launch.manifest.compute)
    }

    pub(super) fn acquire_job(
        &self,
        session: Uuid,
        id: Uuid,
        identity: &WorkIdentity,
        workload: JobWorkload,
        duration: u64,
    ) -> Result<Option<Vec<String>>, String> {
        let mut state = match self.compute.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(_) => return Err("compute authority unavailable".into()),
        };
        let profile = self.compute_profile(&identity.campaign_id)?;
        let (profile_key, bounds) = self.compute_bounds(identity)?;
        let gpu = workload == (JobWorkload::Gpu {});
        let timeout = if gpu {
            bounds.max_gpu_timeout_ms
        } else {
            bounds.max_cpu_timeout_ms
        };
        if duration == 0
            || duration > timeout
            || (gpu
                && (bounds.max_gpu_jobs == 0
                    || self.host_capacity.limits.max_gpu_jobs == 0
                    || profile.as_ref().is_none_or(|p| p.max_gpu_jobs == 0)))
        {
            return Err("native job profile denied".into());
        }
        let now = Instant::now();
        state
            .queue
            .retain(|(_, _, _, seen)| now.duration_since(*seen).as_secs() < 2);
        if let Some(entry) = state
            .queue
            .iter_mut()
            .find(|(s, _, class, _)| *s == session && *class == workload)
        {
            entry.3 = now;
        } else {
            if state.queue.len() >= 256 {
                return Err("native queue full".into());
            }
            state
                .queue
                .push_back((session, identity.campaign_id.clone(), workload, now));
        }
        let index = usize::from(gpu);
        let State {
            queue, campaigns, ..
        } = &mut *state;
        campaigns[index].retain(|campaign| {
            queue
                .iter()
                .any(|(_, c, class, _)| c == campaign && *class == workload)
        });
        if !campaigns[index].contains(&identity.campaign_id) {
            campaigns[index].push_back(identity.campaign_id.clone());
        }
        let next = queue.iter().find(|(_, campaign, class, _)| {
            *class == workload && Some(campaign) == campaigns[index].front()
        });
        if next.is_some_and(|(s, _, _, _)| *s != session) {
            return Ok(None);
        }
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let mut table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        if table
            .get(id.to_string().as_str())
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("native lease already issued".into());
        }
        let mut occupied = 0usize;
        let mut own_active = 0usize;
        let mut profile_active = 0usize;
        let mut total = 0u64;
        let mut records = 0usize;
        let mut devices = Vec::new();
        for row in table.iter().map_err(|e| e.to_string())? {
            let (_, value) = row.map_err(|e| e.to_string())?;
            let record: Record =
                serde_json::from_slice(value.value()).map_err(|e| e.to_string())?;
            if record.workload != workload {
                continue;
            }
            if record.final_ms.is_none() {
                occupied += 1;
                devices.extend(record.device_ids);
            }
            if record.identity.campaign_id == identity.campaign_id {
                records += 1;
                own_active += usize::from(record.final_ms.is_none());
                profile_active +=
                    usize::from(record.final_ms.is_none() && record.profile == profile_key);
                total = total
                    .checked_add(record.final_ms.unwrap_or(record.reserved_ms))
                    .ok_or("compute sum overflow")?;
            }
        }
        let limit = profile
            .as_ref()
            .map(|p| if gpu { p.gpu_job_ms } else { p.cpu_job_ms });
        if records >= 65_536
            || limit.is_some_and(|limit| total.checked_add(duration).is_none_or(|sum| sum > limit))
        {
            state
                .queue
                .retain(|(s, _, class, _)| *s != session || *class != workload);
            return Err("native compute budget exhausted".into());
        }
        let cap = if gpu {
            self.host_capacity.limits.max_gpu_jobs
        } else {
            self.host_capacity.limits.max_cpu_jobs
        };
        if occupied >= cap {
            return Ok(None);
        }
        if gpu
            && (own_active >= profile.as_ref().unwrap().max_gpu_jobs
                || profile_active >= bounds.max_gpu_jobs)
        {
            // An occupied profile must not block other campaigns' free devices.
            if let Some(campaign) = state.campaigns[index].pop_front() {
                state.campaigns[index].push_back(campaign);
            }
            return Ok(None);
        }
        let selected = if gpu {
            let Some(device) = self
                .host_capacity
                .limits
                .gpu_device_ids
                .iter()
                .find(|id| !devices.contains(id))
            else {
                return Ok(None);
            };
            vec![device.clone()]
        } else {
            Vec::new()
        };
        let slot = if gpu {
            None
        } else {
            match self.host_capacity.cpu.clone().try_acquire_owned() {
                Ok(slot) => Some(slot),
                Err(_) => return Ok(None),
            }
        };
        let record = Record {
            profile: profile_key,
            session,
            identity: identity.clone(),
            workload,
            reserved_ms: duration,
            device_ids: selected.clone(),
            final_ms: None,
        };
        table
            .insert(
                id.to_string().as_str(),
                serde_json::to_vec(&record)
                    .map_err(|e| e.to_string())?
                    .as_slice(),
            )
            .map_err(|e| e.to_string())?;
        drop(table);
        tx.commit().map_err(|e| e.to_string())?;
        state.live.insert(id, (Instant::now(), slot));
        state
            .queue
            .retain(|(s, _, class, _)| *s != session || *class != workload);
        state.campaigns[index].pop_front();
        if state
            .queue
            .iter()
            .any(|(_, campaign, class, _)| campaign == &identity.campaign_id && *class == workload)
        {
            state.campaigns[index].push_back(identity.campaign_id.clone());
        }
        Ok(Some(selected))
    }

    pub(super) fn release_job(
        &self,
        session: Uuid,
        id: Uuid,
        unspawned: bool,
    ) -> Result<(), String> {
        let mut state = self
            .compute
            .lock()
            .map_err(|_| "compute authority unavailable")?;
        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
        let mut table = tx.open_table(JOBS).map_err(|e| e.to_string())?;
        let mut record: Record = serde_json::from_slice(
            table
                .get(id.to_string().as_str())
                .map_err(|e| e.to_string())?
                .ok_or("unknown job")?
                .value(),
        )
        .map_err(|e| e.to_string())?;
        if record.session != session {
            return Err("foreign job".into());
        }
        if record.final_ms.is_some() {
            return Ok(());
        }
        let elapsed = if unspawned {
            0
        } else {
            state
                .live
                .get(&id)
                .and_then(|(start, _)| job_duration_ms(start.elapsed()))
                .unwrap_or(record.reserved_ms)
        };
        // Overruns are charged, never silently clamped to the hold.
        record.final_ms = Some(elapsed);
        table
            .insert(
                id.to_string().as_str(),
                serde_json::to_vec(&record)
                    .map_err(|e| e.to_string())?
                    .as_slice(),
            )
            .map_err(|e| e.to_string())?;
        drop(table);
        tx.commit().map_err(|e| e.to_string())?;
        state.live.remove(&id);
        Ok(())
    }
}
