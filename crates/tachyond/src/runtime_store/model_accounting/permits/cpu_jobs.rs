//! Session-scoped native job leases. Disconnect is not proof of native cleanup.
//! RuntimeStore owns live slots; redb owns unresolved restart occupancy.
use super::*;
use tachyon_model::broker::{CpuJobReply, CpuJobRequest, JobWorkload};

pub(super) struct CpuJobs {
    active: HashMap<uuid::Uuid, ()>,
    origin: u128,
    issued: u64,
}

impl Default for CpuJobs {
    fn default() -> Self {
        Self {
            active: HashMap::new(),
            origin: uuid::Uuid::new_v4().as_u128(),
            issued: 0,
        }
    }
}

impl CpuJobs {
    #[cfg(test)]
    pub(super) fn request(
        &mut self,
        broker: &ModelBroker,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: CpuJobRequest,
    ) -> CpuJobReply {
        self.request_store(&broker.store, permit, reservation, request)
    }

    pub(super) fn request_store(
        &mut self,
        store: &RuntimeStore,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: CpuJobRequest,
    ) -> CpuJobReply {
        // Historical cleanup stays valid after revocation, pause or replacement.
        if let CpuJobRequest::Release { permit } | CpuJobRequest::Unspawned { permit } = request {
            // A private session owns a random range of monotonic lease IDs.
            // Recognize exact historical IDs without retaining tombstones.
            let sequence = permit.as_u128().wrapping_sub(self.origin);
            return if sequence > 0 && sequence <= u128::from(self.issued) {
                if store
                    .release_job(
                        uuid::Uuid::from_u128(self.origin),
                        permit,
                        matches!(request, CpuJobRequest::Unspawned { .. }),
                    )
                    .is_err()
                {
                    return CpuJobReply::Denied;
                }
                self.active.remove(&permit);
                CpuJobReply::Released
            } else {
                CpuJobReply::Denied
            };
        }
        let state = match store.model_permits.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock)
                if matches!(
                    request,
                    CpuJobRequest::TryAcquire | CpuJobRequest::Acquire { .. }
                ) =>
            {
                return CpuJobReply::Busy;
            }
            Err(_) => return CpuJobReply::Denied,
        };
        let Some(grant) = state.grants.get(&permit.0) else {
            return CpuJobReply::Denied;
        };
        if !grant.active
            || grant.paused
            || grant.closed.load(std::sync::atomic::Ordering::Acquire)
            || grant.request != *reservation
            || state.current.get(&reservation.identity.work_id) != Some(&permit.0)
        {
            return CpuJobReply::Denied;
        }
        if matches!(request, CpuJobRequest::Profile) {
            return CpuJobReply::Profile {
                max_cpu_jobs: store.host_capacity.limits.max_cpu_jobs,
                max_gpu_jobs: store
                    .compute_bounds(&reservation.identity)
                    .map_or(0, |(_, p)| {
                        p.max_gpu_jobs.min(store.host_capacity.limits.max_gpu_jobs)
                    }),
            };
        }
        let Some(sequence) = self.issued.checked_add(1) else {
            return CpuJobReply::Denied;
        };
        let (workload, duration) = match request {
            CpuJobRequest::Acquire {
                workload,
                max_duration_ms,
            } => (workload, max_duration_ms),
            CpuJobRequest::TryAcquire => (JobWorkload::Cpu {}, 120_000),
            _ => return CpuJobReply::Denied,
        };
        let id = uuid::Uuid::from_u128(self.origin.wrapping_add(u128::from(sequence)));
        match store.acquire_job(
            uuid::Uuid::from_u128(self.origin),
            id,
            &reservation.identity,
            workload,
            duration,
        ) {
            Ok(Some(device_ids)) => {
                self.issued = sequence;
                if grant.closed.load(std::sync::atomic::Ordering::Acquire) {
                    let _ = store.release_job(uuid::Uuid::from_u128(self.origin), id, true);
                    return CpuJobReply::Denied;
                }
                self.active.insert(id, ());
                if matches!(request, CpuJobRequest::TryAcquire) {
                    CpuJobReply::Acquired { permit: id }
                } else {
                    CpuJobReply::Granted {
                        permit: id,
                        device_ids,
                    }
                }
            }
            Ok(None) => CpuJobReply::Busy,
            Err(_) => CpuJobReply::Denied,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_job_busy_authority_never_blocks_historical_release() {
        let (_dir, store, funding, request) = super::super::tests::setup();
        let store = Arc::new(store);
        let permit = store
            .host_issue_model_permit(request.clone(), funding, None)
            .unwrap();
        let broker = ModelBroker::new(store.clone(), super::super::broker_tests::model(&request));
        let mut jobs = CpuJobs::default();
        let mut foreign = CpuJobs::default();
        let mut first = None;
        for _ in 0..1024 {
            let CpuJobReply::Acquired { permit: cpu } =
                jobs.request(&broker, &permit, &request, CpuJobRequest::TryAcquire)
            else {
                panic!()
            };
            first.get_or_insert(cpu);
            assert_eq!(jobs.active.len(), 1);
            assert!(matches!(
                foreign.request(
                    &broker,
                    &permit,
                    &request,
                    CpuJobRequest::Release { permit: cpu }
                ),
                CpuJobReply::Denied
            ));
            assert!(matches!(
                jobs.request(
                    &broker,
                    &permit,
                    &request,
                    CpuJobRequest::Release { permit: cpu }
                ),
                CpuJobReply::Released
            ));
            assert!(jobs.active.is_empty());
        }
        assert!(matches!(
            jobs.request(
                &broker,
                &permit,
                &request,
                CpuJobRequest::Release {
                    permit: first.unwrap()
                }
            ),
            CpuJobReply::Released
        ));
        let future = uuid::Uuid::from_u128(jobs.origin.wrapping_add(u128::from(jobs.issued) + 1));
        assert!(matches!(
            jobs.request(
                &broker,
                &permit,
                &request,
                CpuJobRequest::Release { permit: future }
            ),
            CpuJobReply::Denied
        ));
        let CpuJobReply::Acquired { permit: cpu } =
            jobs.request(&broker, &permit, &request, CpuJobRequest::TryAcquire)
        else {
            panic!()
        };
        let _authority = store.model_permits.lock().unwrap();
        assert!(matches!(
            jobs.request(&broker, &permit, &request, CpuJobRequest::TryAcquire),
            CpuJobReply::Busy
        ));
        assert!(matches!(
            jobs.request(
                &broker,
                &permit,
                &request,
                CpuJobRequest::Release { permit: cpu }
            ),
            CpuJobReply::Released
        ));
        assert_eq!(store.host_capacity.cpu.available_permits(), 2);
    }
}
