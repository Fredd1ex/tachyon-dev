//! Process-local campaign admission, independent of durable funding/group caps.
//! The arbiter performs only bounded queue operations, never storage or I/O.
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use tachyon_util::config::ResourceLimits;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

pub(crate) fn identity(
    campaign: &str,
    work: &str,
    attempt: &str,
    generation: u64,
    reservation: &str,
) -> String {
    serde_json::to_string(&(campaign, work, attempt, generation, reservation))
        .expect("string tuple")
}

pub(crate) struct HostCapacity {
    pub limits: ResourceLimits,
    pub resident: Arc<FairCapacity>,
    pub execution: Arc<FairCapacity>,
    pub model: Arc<FairCapacity>,
    pub cpu: Arc<Semaphore>,
}

impl HostCapacity {
    pub fn new(limits: ResourceLimits) -> Result<Self, String> {
        limits.validate()?;
        Ok(Self {
            resident: FairCapacity::new(limits.max_resident_workers),
            execution: FairCapacity::new(limits.max_execution_jobs),
            model: FairCapacity::new(limits.max_model_calls),
            cpu: Arc::new(Semaphore::new(limits.max_cpu_jobs)),
            limits,
        })
    }
}

struct Queue {
    campaigns: VecDeque<(String, VecDeque<uuid::Uuid>)>,
    count: usize,
    last: Option<String>,
}

pub(crate) struct FairCapacity {
    slots: Arc<Semaphore>,
    queue: Mutex<Queue>,
    changed: Notify,
    retained: Mutex<BTreeMap<String, Option<OwnedSemaphorePermit>>>,
}

impl FairCapacity {
    fn monitor(
        &self,
        resource: tachyon_api::monitor::CapacityResource,
        limit: usize,
    ) -> Result<tachyon_api::monitor::Capacity, String> {
        use tachyon_api::monitor::*;
        let queued = self
            .queue
            .try_lock()
            .map_err(|_| "host capacity unavailable")?
            .count;
        let unresolved = self
            .retained
            .try_lock()
            .map_err(|_| "host retained capacity unavailable")?
            .len();
        Ok(Capacity {
            resource,
            sampled_at_ms: super::monitor::now_ms(),
            limit: Decimal(limit as u128),
            held: Decimal(limit.saturating_sub(self.slots.available_permits()) as u128),
            queued: Decimal(queued as u128),
            unresolved: Observed::Known(Decimal(unresolved as u128)),
        })
    }
    #[cfg(test)]
    pub fn available(&self) -> usize {
        self.slots.available_permits()
    }

    pub fn waiting(&self, campaign: &str) -> Result<usize, String> {
        let q = self.queue.lock().map_err(|_| "host capacity unavailable")?;
        Ok(q.campaigns
            .iter()
            .find(|(c, _)| c == campaign)
            .map_or(0, |(_, entries)| entries.len()))
    }

    fn new(slots: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(slots)),
            queue: Mutex::new(Queue {
                campaigns: VecDeque::new(),
                count: 0,
                last: None,
            }),
            changed: Notify::new(),
            retained: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn reserve_unknown(&self, identity: &str) -> Result<(), String> {
        let mut retained = self
            .retained
            .lock()
            .map_err(|_| "host retained capacity unavailable")?;
        if !retained.contains_key(identity) {
            retained.insert(identity.into(), self.slots.clone().try_acquire_owned().ok());
        }
        Ok(())
    }

    /// Caller checked exact operator cleanup evidence and no active task.
    pub fn release_unknown(&self, identity: &str) -> Result<(), String> {
        let mut retained = self
            .retained
            .lock()
            .map_err(|_| "host retained capacity unavailable")?;
        if let Some(Some(permit)) = retained.remove(identity) {
            if let Some((_, debt)) = retained.iter_mut().find(|(_, permit)| permit.is_none()) {
                *debt = Some(permit);
            } else {
                drop(permit);
            }
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn acquire(self: &Arc<Self>, campaign: &str) -> Result<CapacityPermit, String> {
        let id = uuid::Uuid::new_v4();
        {
            let mut q = self.queue.lock().map_err(|_| "host capacity unavailable")?;
            if q.count >= 4096 {
                return Err("host admission queue full".into());
            }
            if let Some((_, entries)) = q.campaigns.iter_mut().find(|(c, _)| c == campaign) {
                entries.push_back(id);
            } else {
                q.campaigns
                    .push_back((campaign.to_owned(), VecDeque::from([id])));
            }
            q.count += 1;
        }
        let _waiting = Waiting {
            capacity: self.clone(),
            id,
        };
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut q = self.queue.lock().map_err(|_| "host capacity unavailable")?;
                // If another campaign arrived after the previous grant, it gets
                // the next turn rather than the previous campaign's backlog.
                if q.campaigns.len() > 1
                    && q.campaigns
                        .front()
                        .is_some_and(|(c, _)| Some(c) == q.last.as_ref())
                {
                    let front = q.campaigns.pop_front().unwrap();
                    q.campaigns.push_back(front);
                }
                if q.campaigns
                    .front()
                    .is_some_and(|(_, entries)| entries.front() == Some(&id))
                {
                    if let Ok(permit) = self.slots.clone().try_acquire_owned() {
                        let (campaign, mut entries) = q.campaigns.pop_front().unwrap();
                        entries.pop_front();
                        q.count -= 1;
                        q.last = Some(campaign.clone());
                        if !entries.is_empty() {
                            q.campaigns.push_back((campaign, entries));
                        }
                        self.changed.notify_waiters();
                        return Ok(CapacityPermit {
                            capacity: self.clone(),
                            permit: Some(permit),
                        });
                    }
                }
            }
            changed.await;
        }
    }
}

impl super::RuntimeStore {
    pub(super) fn monitor_capacities(&self) -> Result<Vec<tachyon_api::monitor::Capacity>, String> {
        use tachyon_api::monitor::CapacityResource::*;
        let c = &self.host_capacity;
        let mut output = vec![
            c.resident
                .monitor(Resident, c.limits.max_resident_workers)?,
            c.execution
                .monitor(Execution, c.limits.max_execution_jobs)?,
            c.model.monitor(Model, c.limits.max_model_calls)?,
        ];
        output.extend(
            self.compute
                .try_lock()
                .map_err(|_| "compute unavailable")?
                .monitor(
                    c.limits.max_cpu_jobs,
                    c.limits.max_gpu_jobs,
                    c.cpu.available_permits(),
                ),
        );
        Ok(output)
    }
}

struct Waiting {
    capacity: Arc<FairCapacity>,
    id: uuid::Uuid,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        let mut q = self
            .capacity
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (_, entries) in &mut q.campaigns {
            if let Some(index) = entries.iter().position(|id| id == &self.id) {
                entries.remove(index);
                q.count -= 1;
                break;
            }
        }
        q.campaigns.retain(|(_, entries)| !entries.is_empty());
        self.capacity.changed.notify_waiters();
    }
}

pub(crate) struct CapacityPermit {
    capacity: Arc<FairCapacity>,
    permit: Option<OwnedSemaphorePermit>,
}
impl CapacityPermit {
    pub fn retain_identity(mut self, identity: &str) {
        if let Some(permit) = self.permit.take() {
            let mut retained = self
                .capacity
                .retained
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(existing) = retained.get_mut(identity) {
                if existing.is_none() {
                    *existing = Some(permit);
                }
            } else {
                retained.insert(identity.into(), Some(permit));
            }
        }
    }

    /// Unknown cleanup is not free capacity. Retain this slot for this host's
    /// lifetime rather than inferring termination from a dropped execution future.
    #[cfg(test)]
    pub fn retain(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.forget();
        }
    }
}
impl Drop for CapacityPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.capacity.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn monitor_capacity_contention_and_poison_never_wait_for_source_locks() {
        use tachyon_api::monitor::*;
        for source in 0..7 {
            let dir = tempfile::tempdir().unwrap();
            let store =
                Arc::new(crate::RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
            let capacity = match source / 2 {
                0 => &store.host_capacity.resident,
                1 => &store.host_capacity.execution,
                _ => &store.host_capacity.model,
            };
            let queue = (source < 6 && source % 2 == 0).then(|| capacity.queue.lock().unwrap());
            let retained =
                (source < 6 && source % 2 == 1).then(|| capacity.retained.lock().unwrap());
            let compute = (source == 6).then(|| store.compute.lock().unwrap());
            let registry = Arc::new(Mutex::new(crate::Registry::default()));
            let shutdown = registry.lock().unwrap().service_shutdown.clone();
            let (monitor, sampler) =
                crate::monitor::Monitor::start(Arc::downgrade(&registry), store.clone(), shutdown);
            let lease = monitor
                .acquire(
                    MonitorQuery {
                        scope: MonitorScope::Host,
                        after: None,
                        limit: MAX_PAGE,
                    },
                    true,
                )
                .unwrap();
            let sample = lease.latest(None).unwrap();
            monitor.stop();
            let (done, ended) = std::sync::mpsc::channel();
            let joiner = std::thread::spawn(move || {
                sampler.join().unwrap();
                done.send(()).unwrap();
            });
            let stopped = ended.recv_timeout(Duration::from_secs(1));
            // Release (and poison) before assertions so a regression cannot hang the test.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _guards = (queue, retained, compute);
                panic!("poison monitor source fixture");
            }));
            joiner.join().unwrap();
            stopped.unwrap();
            let sample = sample.expect("source sampling must not wait for its lock");
            assert_eq!(sample.stale, Some(MonitorError::Unavailable));
            assert!(sample.payload.is_none());
            assert!(store.monitor_capacities().is_err());
        }
    }

    #[tokio::test]
    async fn monitor_capacity_distinguishes_held_queued_and_retained_unknown() {
        use tachyon_api::monitor::*;
        let capacity = FairCapacity::new(2);
        let held = capacity.acquire("a").await.unwrap();
        capacity.reserve_unknown("unresolved").unwrap();
        let queued_capacity = capacity.clone();
        let waiter = tokio::spawn(async move { queued_capacity.acquire("b").await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while capacity.waiting("b").unwrap() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let sample = capacity.monitor(CapacityResource::Resident, 2).unwrap();
        assert_eq!(sample.held.0, 2);
        assert_eq!(sample.queued.0, 1);
        assert_eq!(sample.unresolved, Observed::Known(Decimal(1)));
        // Sampling observes but never reclaims unknown occupancy or consumes a waiter.
        assert_eq!(capacity.available(), 0);
        assert_eq!(capacity.waiting("b").unwrap(), 1);
        drop(held);
        drop(waiter.await.unwrap().unwrap());
        assert_eq!(
            capacity
                .monitor(CapacityResource::Resident, 2)
                .unwrap()
                .unresolved,
            sample.unresolved
        );
    }

    #[tokio::test]
    async fn exact_cleanup_releases_once_and_restart_debt_is_conservative() {
        let capacity = FairCapacity::new(2);
        let key = identity("campaign", "work", "attempt", 1, "allocation");
        capacity
            .acquire("campaign")
            .await
            .unwrap()
            .retain_identity(&key);
        assert_eq!(capacity.available(), 1);
        capacity
            .release_unknown(&identity("campaign", "work", "other", 1, "allocation"))
            .unwrap();
        assert_eq!(capacity.available(), 1);
        capacity.release_unknown(&key).unwrap();
        capacity.release_unknown(&key).unwrap();
        assert_eq!(capacity.available(), 2);
        for key in ["a", "b", "c"] {
            capacity.reserve_unknown(key).unwrap();
        }
        capacity.reserve_unknown("a").unwrap();
        assert_eq!(capacity.available(), 0);
        capacity.release_unknown("a").unwrap();
        assert_eq!(capacity.available(), 0);
        capacity.release_unknown("b").unwrap();
        assert_eq!(capacity.available(), 1);
        capacity.release_unknown("c").unwrap();
        assert_eq!(capacity.available(), 2);
    }

    #[tokio::test]
    async fn round_robin_cancellation_timeout_and_no_leaks() {
        let capacity = FairCapacity::new(1);
        let held = capacity.acquire("a").await.unwrap();
        let a1 = capacity.acquire("a");
        let a2 = capacity.acquire("a");
        let b = capacity.acquire("b");
        tokio::pin!(a1, a2, b);
        for future in [&mut a1, &mut a2, &mut b] {
            assert!(tokio::time::timeout(Duration::from_millis(1), future)
                .await
                .is_err());
        }
        assert_eq!(capacity.queue.lock().unwrap().count, 3);
        drop(held);
        let b = tokio::time::timeout(Duration::from_secs(1), b)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(capacity.slots.available_permits(), 0);
        drop(b);
        drop(a1.await.unwrap());
        drop(a2.await.unwrap());
        assert_eq!(capacity.queue.lock().unwrap().count, 0);
        assert_eq!(capacity.slots.available_permits(), 1);
        let held = capacity.acquire("c").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), capacity.acquire("cancelled"))
                .await
                .is_err()
        );
        assert_eq!(capacity.queue.lock().unwrap().count, 0);
        drop(held);
        assert_eq!(capacity.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn independent_counts_and_unknown_cleanup_retains_capacity() {
        let host = HostCapacity::new(ResourceLimits {
            max_campaigns: 2,
            max_resident_workers: 2,
            max_execution_jobs: 1,
            max_model_calls: 1,
            max_cpu_jobs: 2,
            ..Default::default()
        })
        .unwrap();
        let a = host.resident.acquire("a").await.unwrap();
        let b = host.resident.acquire("b").await.unwrap();
        let model = host.model.acquire("b").await.unwrap();
        let execution = host.execution.acquire("a").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), host.execution.acquire("b"))
                .await
                .is_err()
        );
        assert_eq!(host.execution.waiting("b").unwrap(), 0);
        drop(execution);
        let execution = host.execution.acquire("b").await.unwrap();
        execution.retain();
        a.retain();
        drop(b);
        drop(model);
        assert_eq!(host.resident.slots.available_permits(), 1);
        assert_eq!(host.model.slots.available_permits(), 1);
        assert_eq!(host.execution.available(), 0);
        assert!(HostCapacity::new(ResourceLimits {
            max_campaigns: 0,
            ..Default::default()
        })
        .is_err());
    }
}
