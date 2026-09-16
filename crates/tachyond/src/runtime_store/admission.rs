//! Host-only admission gate. No IPC caller or production model dispatcher yet.
#![allow(dead_code)]

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use super::{
    campaign_ledger::{LedgerCommand, Pool, Units, Usage},
    RuntimeStore,
};

pub(super) const WORK: TableDefinition<&str, &[u8]> =
    TableDefinition::new("campaign_admitted_work");
pub(super) const PENDING: TableDefinition<&str, ()> =
    TableDefinition::new("campaign_dispatch_pending");
const MAX_BATCH: usize = 32;
const MAX_SCAN: usize = 64;
const CURSOR: TableDefinition<&str, &str> = TableDefinition::new("campaign_dispatch_cursor");

pub(super) fn initialize(write: &WriteTransaction) -> Result<(), String> {
    write.open_table(WORK).map_err(err)?;
    write.open_table(PENDING).map_err(err)?;
    write.open_table(CURSOR).map_err(err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::campaign_ledger::Envelope;
    use super::*;
    use redb::ReadableTableMetadata;
    use std::sync::{Arc, Barrier};
    use tachyon_api::types::{ApiRequest, ApiResponse, CampaignStatus};

    fn campaign(store: &RuntimeStore, key: &str, authorize: bool) -> String {
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: format!("research-{key}"),
                title: key.into(),
                objective: key.into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: format!("campaign-{key}"),
                research_id: research.id,
                title: key.into(),
                objective: key.into(),
            })
            .unwrap()
        else {
            panic!()
        };
        if authorize {
            store
                .host_authorize_campaign_envelope(
                    &format!("grant-{key}"),
                    &campaign.id,
                    Envelope {
                        work: Units {
                            tokens: 100,
                            cost_micro_usd: 100,
                        },
                        verification: Units::default(),
                        max_active_inferences: 100,
                    },
                )
                .unwrap();
        }
        campaign.id
    }

    fn request(campaign: &str, key: &str) -> Admission {
        Admission {
            work_id: key.into(),
            campaign_id: campaign.into(),
            objective: "bounded fake work".into(),
            instruction_revision: 1,
            generation: 1,
            pool: Pool::Work,
            upper_bound: Units {
                tokens: 1,
                cost_micro_usd: 3,
            },
        }
    }

    #[test]
    fn concurrent_replay_one_work_reservation_and_payload_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = campaign(&store, "race", true);
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, barrier, request) =
                    (store.clone(), barrier.clone(), request(&id, "same"));
                std::thread::spawn(move || {
                    barrier.wait();
                    store.admit_campaign_work(request).unwrap()
                })
            })
            .collect();
        let works: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(works.iter().all(|w| *w == works[0]));
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .reservations
                .len(),
            1
        );
        let other = campaign(&store, "other", true);
        for mutation in 0..6 {
            let mut changed = request(&id, "same");
            match mutation {
                0 => changed.campaign_id = other.clone(),
                1 => changed.objective.push('!'),
                2 => changed.generation += 1,
                3 => changed.instruction_revision += 1,
                4 => changed.upper_bound.cost_micro_usd += 1,
                _ => changed.pool = Pool::Verification,
            }
            assert!(store
                .admit_campaign_work(changed)
                .unwrap_err()
                .contains("conflict"));
        }
        assert!(store.admitted_work(&other, "same").is_err());
        let mut forged = works[0].clone();
        forged.admission.campaign_id = other;
        assert!(store.cancel_admitted_work(&forged).is_err());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || {
                    store
                        .dispatch_campaign_batch(10, |_| DispatchOutcome::Unknown)
                        .unwrap()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .sum::<usize>(),
            1
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(10, |_| panic!("replayed"))
                .unwrap(),
            0
        );
    }

    #[test]
    fn transaction_rollback_and_missing_authorization_never_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "rollback", true);
        {
            let write = store.database.begin_write().unwrap();
            RuntimeStore::admit_campaign_work_in(&write, request(&id, "aborted")).unwrap();
            // Readers see neither the work nor its hold until commit.
            assert!(store.admitted_work(&id, "aborted").is_err());
            assert!(store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .reservations
                .is_empty());
            assert!(store
                .database
                .begin_read()
                .unwrap()
                .open_table(PENDING)
                .unwrap()
                .is_empty()
                .unwrap());
            // Drop aborts after all three mutations have succeeded.
        }
        assert!(store.admitted_work(&id, "aborted").is_err());
        assert!(store
            .campaign_ledger(&id)
            .unwrap()
            .unwrap()
            .reservations
            .is_empty());
        let receipts: TableDefinition<&str, &[u8]> =
            TableDefinition::new("campaign_ledger_receipts");
        assert_eq!(
            store
                .database
                .begin_read()
                .unwrap()
                .open_table(receipts)
                .unwrap()
                .len()
                .unwrap(),
            1
        );
        assert!(store
            .database
            .begin_read()
            .unwrap()
            .open_table(PENDING)
            .unwrap()
            .is_empty()
            .unwrap());
        let unauthorized = campaign(&store, "unauthorized", false);
        assert!(store
            .admit_campaign_work(request(&unauthorized, "denied"))
            .is_err());
        let mut excessive = request(&id, "excessive");
        excessive.upper_bound.cost_micro_usd = 101;
        assert!(store.admit_campaign_work(excessive).is_err());
        assert!(store.admitted_work(&id, "excessive").is_err());
        assert_eq!(
            store
                .dispatch_campaign_batch(32, |_| panic!("uncommitted dispatch"))
                .unwrap(),
            0
        );
    }

    #[test]
    fn restart_claim_uncertainty_cancellation_and_authoritative_ack() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "restart", true);
        let cancelled = store
            .admit_campaign_work(request(&id, "cancelled"))
            .unwrap();
        store.cancel_admitted_work(&cancelled).unwrap();
        store.cancel_admitted_work(&cancelled).unwrap();
        store.admit_campaign_work(request(&id, "claim")).unwrap();
        let claim = store.claim_campaign_work().unwrap().unwrap();
        assert!(store.cancel_admitted_work(&claim).is_err());
        store.admit_campaign_work(request(&id, "pending")).unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store.admitted_work(&id, "claim").unwrap().state,
            DispatchState::DispatchingUnknown
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(10, |work| {
                    assert_eq!(work.admission.work_id, "pending");
                    // A write inside the dispatcher proves no redb writer is held.
                    store.admit_campaign_work(request(&id, "pending")).unwrap();
                    DispatchOutcome::Registered {
                        worker_id: "fake-worker".into(),
                    }
                })
                .unwrap(),
            1
        );
        let registered = store.admitted_work(&id, "pending").unwrap();
        assert!(matches!(registered.state, DispatchState::Registered { .. }));
        assert!(store
            .reconcile_campaign_dispatch(&registered, DispatchOutcome::ConfirmedUnspent)
            .is_err());
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .committed(Pool::Work)
                .unwrap()
                .cost_micro_usd,
            6
        );
        let mut stale = claim.clone();
        stale.admission.generation += 1;
        assert!(store
            .reconcile_campaign_dispatch(&stale, DispatchOutcome::ConfirmedUnspent)
            .is_err());
        store
            .reconcile_campaign_dispatch(&claim, DispatchOutcome::Unknown)
            .unwrap();
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .active_inferences(),
            2
        );
        store
            .reconcile_campaign_dispatch(&claim, DispatchOutcome::ConfirmedUnspent)
            .unwrap();
        store
            .reconcile_campaign_dispatch(&claim, DispatchOutcome::ConfirmedUnspent)
            .unwrap();
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .committed(Pool::Work)
                .unwrap()
                .cost_micro_usd,
            3
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(10, |_| panic!("replay"))
                .unwrap(),
            0
        );
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignGet { id })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(campaign.status, CampaignStatus::Draft);
        assert!(store.list_tasks().unwrap().is_empty());
    }

    #[test]
    fn callback_effect_before_panic_is_not_repeated_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "panic", true);
        store.admit_campaign_work(request(&id, "work")).unwrap();
        let mut effects = 0;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.dispatch_campaign_batch(1, |_| {
                effects += 1;
                panic!("external effect succeeded, acknowledgement lost");
            })
        }));
        assert!(result.is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store.admitted_work(&id, "work").unwrap().state,
            DispatchState::DispatchingUnknown
        );
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .active_inferences(),
            1
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(32, |_| {
                    effects += 1;
                    DispatchOutcome::Unknown
                })
                .unwrap(),
            0
        );
        assert_eq!(effects, 1);
    }

    #[test]
    fn bounded_batches_and_failed_spawn_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "batch", true);
        for i in 0..33 {
            store
                .admit_campaign_work(request(&id, &format!("work-{i:02}")))
                .unwrap();
        }
        assert_eq!(store.dispatch_campaign_batch(0, |_| panic!()).unwrap(), 0);
        assert_eq!(
            store
                .dispatch_campaign_batch(usize::MAX, |_| DispatchOutcome::ConfirmedUnspent)
                .unwrap(),
            MAX_BATCH
        );
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .active_inferences(),
            1
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(10, |_| DispatchOutcome::Unknown)
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .committed(Pool::Work)
                .unwrap()
                .cost_micro_usd,
            3
        );
        assert_eq!(store.dispatch_campaign_batch(10, |_| panic!()).unwrap(), 0);
    }

    #[test]
    fn blocked_head_does_not_starve_another_campaign() {
        for cause in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
            let id = campaign(&store, "blocked", true);
            let blocked = store
                .admit_campaign_work(request(&id, "a-blocked"))
                .unwrap();
            let command = match cause {
                0 => LedgerCommand::Cancel {
                    reservation_id: blocked.dispatch_id.clone(),
                },
                1 => LedgerCommand::Reconcile {
                    reservation_id: blocked.dispatch_id.clone(),
                    usage: Usage::Final(Units::default()),
                },
                _ => {
                    let other = store
                        .admit_campaign_work(request(&id, "b-overrun"))
                        .unwrap();
                    LedgerCommand::Reconcile {
                        reservation_id: other.dispatch_id,
                        usage: Usage::Provisional(Units {
                            tokens: 2,
                            cost_micro_usd: 4,
                        }),
                    }
                }
            };
            store
                .campaign_ledger_command("block", &id, command)
                .unwrap();
            let healthy = campaign(&store, "healthy", true);
            store
                .admit_campaign_work(request(&healthy, "z-healthy"))
                .unwrap();
            assert_eq!(
                store
                    .dispatch_campaign_batch(32, |work| {
                        assert_eq!(work.admission.campaign_id, healthy);
                        DispatchOutcome::Unknown
                    })
                    .unwrap(),
                1
            );
            assert_eq!(store.admitted_work(&id, "a-blocked").unwrap(), blocked);
            assert!(store
                .database
                .begin_read()
                .unwrap()
                .open_table(PENDING)
                .unwrap()
                .get("a-blocked")
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn bounded_cursor_reopens_and_eventually_reaches_eligible_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "scan", true);
        for i in 0..(MAX_SCAN + 2) {
            let mut admission = request(&id, &format!("a-{i:03}"));
            admission.upper_bound = Units::default();
            let work = store.admit_campaign_work(admission).unwrap();
            store
                .campaign_ledger_command(
                    &format!("cancel-{i}"),
                    &id,
                    LedgerCommand::Cancel {
                        reservation_id: work.dispatch_id,
                    },
                )
                .unwrap();
        }
        let healthy = campaign(&store, "healthy-scan", true);
        store
            .admit_campaign_work(request(&healthy, "z-healthy"))
            .unwrap();
        assert!(store.claim_campaign_work().unwrap().is_none());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store
                .claim_campaign_work()
                .unwrap()
                .unwrap()
                .admission
                .work_id,
            "z-healthy"
        );
        assert!(store.claim_campaign_work().unwrap().is_none());
    }

    #[test]
    fn cancellation_and_claim_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = campaign(&store, "cancel-race", true);
        for i in 0..16 {
            let work = store
                .admit_campaign_work(request(&id, &format!("work-{i}")))
                .unwrap();
            let barrier = Arc::new(Barrier::new(2));
            let cancel = {
                let (store, barrier, work) = (store.clone(), barrier.clone(), work.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    store.cancel_admitted_work(&work)
                })
            };
            barrier.wait();
            let claim = store.claim_campaign_work().unwrap();
            let cancelled = cancel.join().unwrap().is_ok();
            assert_eq!(cancelled, claim.is_none());
            let ledger = store.campaign_ledger(&id).unwrap().unwrap();
            assert_eq!(
                ledger.reservations[&work.dispatch_id].usage,
                if cancelled {
                    Usage::Final(Units::default())
                } else {
                    Usage::Unknown
                }
            );
            if let Some(claim) = claim {
                store
                    .reconcile_campaign_dispatch(&claim, DispatchOutcome::ConfirmedUnspent)
                    .unwrap();
            }
        }
    }

    #[test]
    fn conflicting_outcomes_cannot_release_spent_usage_or_reverse_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "outcomes", true);
        store.admit_campaign_work(request(&id, "work")).unwrap();
        let claim = store.claim_campaign_work().unwrap().unwrap();
        let spent = Units {
            tokens: 1,
            cost_micro_usd: 2,
        };
        for usage in [Usage::Provisional(spent), Usage::Final(spent)] {
            store
                .campaign_ledger_command(
                    &format!("{usage:?}"),
                    &id,
                    LedgerCommand::Reconcile {
                        reservation_id: claim.dispatch_id.clone(),
                        usage,
                    },
                )
                .unwrap();
            let ledger = store.campaign_ledger(&id).unwrap().unwrap();
            assert!(store
                .reconcile_campaign_dispatch(&claim, DispatchOutcome::ConfirmedUnspent)
                .is_err());
            assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), ledger);
            assert_eq!(store.admitted_work(&id, "work").unwrap(), claim);
        }
        store.admit_campaign_work(request(&id, "zero")).unwrap();
        let zero = store.claim_campaign_work().unwrap().unwrap();
        store
            .reconcile_campaign_dispatch(&zero, DispatchOutcome::ConfirmedUnspent)
            .unwrap();
        let ledger = store.campaign_ledger(&id).unwrap().unwrap();
        for outcome in [
            DispatchOutcome::Registered {
                worker_id: "late-worker".into(),
            },
            DispatchOutcome::Unknown,
        ] {
            assert!(store.reconcile_campaign_dispatch(&zero, outcome).is_err());
        }
        assert!(store.cancel_admitted_work(&zero).is_err());
        assert!(store
            .campaign_ledger_command(
                "late-spend",
                &id,
                LedgerCommand::Reconcile {
                    reservation_id: zero.dispatch_id.clone(),
                    usage: Usage::Final(spent)
                }
            )
            .is_err());
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), ledger);
        assert_eq!(
            store.admitted_work(&id, "zero").unwrap().state,
            DispatchState::ConfirmedUnspent
        );
        assert_eq!(
            store
                .dispatch_campaign_batch(32, |_| panic!("duplicate launch"))
                .unwrap(),
            0
        );
    }

    #[test]
    fn independently_cancelled_or_settled_reservation_cannot_launch() {
        for settle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
            let id = campaign(&store, "fence", true);
            let work = store.admit_campaign_work(request(&id, "work")).unwrap();
            let command = if settle {
                LedgerCommand::Reconcile {
                    reservation_id: work.dispatch_id,
                    usage: Usage::Final(Units::default()),
                }
            } else {
                LedgerCommand::Cancel {
                    reservation_id: work.dispatch_id,
                }
            };
            store
                .campaign_ledger_command("external", &id, command)
                .unwrap();
            assert_eq!(
                store
                    .dispatch_campaign_batch(1, |_| panic!("invalid reservation launched"))
                    .unwrap(),
                0
            );
        }
    }
}

fn err(error: impl std::fmt::Display) -> String {
    format!("campaign admission: {error}")
}

/// Supplied only by an already authorized host, never deserialized from IPC.
/// work_id is the stable Work identity and globally unique admission command id.
/// Model retries/Attempt IDs do not create new Work or replace its admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Admission {
    pub work_id: String,
    pub campaign_id: String,
    pub objective: String,
    pub instruction_revision: u64,
    pub generation: u64,
    pub pool: Pool,
    pub upper_bound: Units,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DispatchState {
    Admitted,
    /// Includes a crash before launch or before acknowledgement. Never replay.
    DispatchingUnknown,
    Registered {
        worker_id: String,
    },
    ConfirmedUnspent,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AdmittedWork {
    schema_version: u32,
    pub admission: Admission,
    /// Also the reservation id. Immutable across claim and reconciliation.
    pub dispatch_id: String,
    pub state: DispatchState,
}

/// Trusted evidence, not an arbitrary worker/IPC report. An error or timeout
/// must be Unknown; only proof that nothing launched/spent permits release.
pub(crate) enum DispatchOutcome {
    Registered { worker_id: String },
    ConfirmedUnspent,
    Unknown,
}

fn decode(bytes: &[u8], key: &str) -> Result<AdmittedWork, String> {
    let work: AdmittedWork = serde_json::from_slice(bytes).map_err(err)?;
    if work.schema_version != 1 || work.admission.work_id != key || work.dispatch_id.is_empty() {
        return Err(err("invalid work schema/identity"));
    }
    Ok(work)
}

impl RuntimeStore {
    pub(super) fn admitted_work_in(
        write: &WriteTransaction,
        work_id: &str,
    ) -> Result<AdmittedWork, String> {
        let table = write.open_table(WORK).map_err(err)?;
        let value = table
            .get(work_id)
            .map_err(err)?
            .ok_or_else(|| err("unknown admitted funding"))?;
        decode(value.value(), work_id)
    }

    pub(super) fn admitted_funding_in(
        write: &WriteTransaction,
        expected: &AdmittedWork,
    ) -> Result<(), String> {
        let work = Self::admitted_work_in(write, &expected.admission.work_id)?;
        Self::group_funding_in(write, &work)?;
        if work.admission != expected.admission
            || work.dispatch_id != expected.dispatch_id
            || !matches!(work.state, DispatchState::Registered { .. })
        {
            return Err(err("funding requires exact registered admission"));
        }
        let ledger = Self::campaign_ledger_in(write, &work.admission.campaign_id)?;
        let hold = ledger
            .reservations
            .get(&work.dispatch_id)
            .ok_or_else(|| err("missing admission hold"))?;
        if hold.pool != work.admission.pool || hold.reserved != work.admission.upper_bound {
            return Err(err("admission funding mismatch"));
        }
        Ok(())
    }

    /// Requires host authorization for this exact campaign and work payload,
    /// plus an existing host-authorized envelope. This is not an auth protocol.
    pub(crate) fn admit_campaign_work(&self, admission: Admission) -> Result<AdmittedWork, String> {
        let write = self.database.begin_write().map_err(err)?;
        let work = Self::admit_campaign_work_in(&write, admission)?;
        write.commit().map_err(err)?;
        Ok(work)
    }

    pub(super) fn admit_campaign_work_in(
        write: &WriteTransaction,
        admission: Admission,
    ) -> Result<AdmittedWork, String> {
        if admission.work_id.trim().is_empty()
            || admission.work_id.len() > 256
            || admission.objective.trim().is_empty()
            || admission.objective.len() > 32_768
            || admission.instruction_revision == 0
            || admission.generation == 0
        {
            return Err(err(
                "invalid admission identity/objective/revision/generation",
            ));
        }
        let mut table = write.open_table(WORK).map_err(err)?;
        if let Some(value) = table.get(admission.work_id.as_str()).map_err(err)? {
            let work = decode(value.value(), &admission.work_id)?;
            if work.admission != admission {
                return Err(err("admission command payload conflict"));
            }
            // Immutable identity, current lifecycle; never enqueue a replay.
            return Ok(work);
        }
        Self::group_admission_in(write, &admission)?;
        let dispatch_id = uuid::Uuid::new_v4().to_string();
        Self::campaign_ledger_command_in(
            write,
            &format!("admit:{dispatch_id}"),
            &admission.campaign_id,
            LedgerCommand::Reserve {
                reservation_id: dispatch_id.clone(),
                pool: admission.pool,
                reserved: admission.upper_bound,
            },
        )?;
        let work = AdmittedWork {
            schema_version: 1,
            admission,
            dispatch_id,
            state: DispatchState::Admitted,
        };
        #[cfg(target_os = "linux")]
        write
            .open_table(super::research_context::WORK_INDEX)
            .map_err(err)?
            .insert(
                (
                    work.admission.campaign_id.as_str(),
                    work.admission.work_id.as_str(),
                ),
                (),
            )
            .map_err(err)?;
        table
            .insert(
                work.admission.work_id.as_str(),
                serde_json::to_vec(&work).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        write
            .open_table(PENDING)
            .map_err(err)?
            .insert(work.admission.work_id.as_str(), ())
            .map_err(err)?;
        Ok(work)
    }

    pub(crate) fn admitted_work(
        &self,
        campaign: &str,
        work_id: &str,
    ) -> Result<AdmittedWork, String> {
        let read = self.database.begin_read().map_err(err)?;
        let table = read.open_table(WORK).map_err(err)?;
        let value = table
            .get(work_id)
            .map_err(err)?
            .ok_or_else(|| err("unknown work"))?;
        let work = decode(value.value(), work_id)?;
        if work.admission.campaign_id != campaign {
            return Err(err("campaign scope mismatch"));
        }
        Ok(work)
    }

    /// One serialized claim. Pending index removal and state change commit before
    /// any external effect. No timeout ever puts this claim back in the queue.
    pub(super) fn claim_campaign_work(&self) -> Result<Option<AdmittedWork>, String> {
        self.claim_campaign_work_matching(|_| true)
    }

    /// Trusted catalog filter runs synchronously before claiming any capacity.
    pub(super) fn claim_campaign_work_matching(
        &self,
        eligible: impl Fn(&AdmittedWork) -> bool,
    ) -> Result<Option<AdmittedWork>, String> {
        let write = self.database.begin_write().map_err(err)?;
        let mut pending = write.open_table(PENDING).map_err(err)?;
        let mut table = write.open_table(WORK).map_err(err)?;
        let mut selected = None;
        let mut cursor = write.open_table(CURSOR).map_err(err)?;
        let after = cursor
            .get("pending")
            .map_err(err)?
            .map(|v| v.value().to_owned())
            .unwrap_or_default();
        let mut last = None;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let entries = pending
            .range::<&str>((Excluded(after.as_str()), Unbounded))
            .map_err(err)?
            .chain(
                pending
                    .range::<&str>((Unbounded, Included(after.as_str())))
                    .map_err(err)?,
            );
        for entry in entries.take(MAX_SCAN) {
            let (key, _) = entry.map_err(err)?;
            let key = key.value();
            last = Some(key.to_owned());
            let work = decode(
                table
                    .get(key)
                    .map_err(err)?
                    .ok_or_else(|| err("missing pending work"))?
                    .value(),
                key,
            )?;
            if work.state != DispatchState::Admitted {
                return Err(err("invalid pending state"));
            }
            if !eligible(&work) {
                continue;
            }
            if Self::execution_owns_verifier_in(&write, &work)? {
                continue;
            }
            // Revalidate the current reservation, not an admission receipt snapshot.
            let ledger = Self::campaign_ledger_in(&write, &work.admission.campaign_id)?;
            let reservation = ledger
                .reservations
                .get(&work.dispatch_id)
                .ok_or_else(|| err("missing reservation"))?;
            if reservation.reserved != work.admission.upper_bound
                || reservation.pool != work.admission.pool
            {
                return Err(err("reservation identity mismatch"));
            }
            // Retain blocked work for host resolution without starving other campaigns.
            if ledger.admissions_paused
                || reservation.cancellation_requested
                || reservation.usage != Usage::Unknown
            {
                continue;
            }
            if !Self::group_claim_in(&write, &work)? {
                continue;
            }
            selected = Some((key.to_owned(), work));
            break;
        }
        if let Some(last) = last {
            cursor.insert("pending", last.as_str()).map_err(err)?;
        }
        drop(cursor);
        let Some((key, mut work)) = selected else {
            drop(table);
            drop(pending);
            write.commit().map_err(err)?;
            return Ok(None);
        };
        work.state = DispatchState::DispatchingUnknown;
        table
            .insert(
                key.as_str(),
                serde_json::to_vec(&work).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        pending.remove(key.as_str()).map_err(err)?;
        drop(table);
        drop(pending);
        write.commit().map_err(err)?;
        Ok(Some(work))
    }

    /// Cancellation wins only before claim. After claim it cannot prove zero
    /// usage; the caller must reconcile the launch instead.
    pub(crate) fn cancel_admitted_work(&self, identity: &AdmittedWork) -> Result<(), String> {
        self.finish_campaign_dispatch(identity, DispatchState::Cancelled)
    }

    pub(crate) fn reconcile_campaign_dispatch(
        &self,
        identity: &AdmittedWork,
        outcome: DispatchOutcome,
    ) -> Result<(), String> {
        let state = match outcome {
            DispatchOutcome::Registered { worker_id } if !worker_id.trim().is_empty() => {
                DispatchState::Registered { worker_id }
            }
            DispatchOutcome::Registered { .. } => return Err(err("blank worker identity")),
            DispatchOutcome::ConfirmedUnspent => DispatchState::ConfirmedUnspent,
            DispatchOutcome::Unknown => DispatchState::DispatchingUnknown,
        };
        self.finish_campaign_dispatch(identity, state)
    }

    fn finish_campaign_dispatch(
        &self,
        identity: &AdmittedWork,
        state: DispatchState,
    ) -> Result<(), String> {
        let write = self.database.begin_write().map_err(err)?;
        let mut table = write.open_table(WORK).map_err(err)?;
        let key = identity.admission.work_id.as_str();
        let mut work = decode(
            table
                .get(key)
                .map_err(err)?
                .ok_or_else(|| err("unknown work"))?
                .value(),
            key,
        )?;
        if work.admission != identity.admission || work.dispatch_id != identity.dispatch_id {
            return Err(err("campaign/dispatch/generation identity mismatch"));
        }
        if work.state == state {
            return Ok(());
        }
        let expected = if state == DispatchState::Cancelled {
            DispatchState::Admitted
        } else {
            DispatchState::DispatchingUnknown
        };
        if work.state != expected {
            return Err(err("dispatch outcome conflict"));
        }
        if matches!(
            state,
            DispatchState::Cancelled | DispatchState::ConfirmedUnspent
        ) {
            Self::group_terminal_in(&write, &work)?;
            Self::campaign_ledger_command_in(
                &write,
                &format!("unspent:{}", work.dispatch_id),
                &work.admission.campaign_id,
                LedgerCommand::Reconcile {
                    reservation_id: work.dispatch_id.clone(),
                    usage: Usage::Final(Units::default()),
                },
            )?;
        }
        work.state = state;
        table
            .insert(key, serde_json::to_vec(&work).map_err(err)?.as_slice())
            .map_err(err)?;
        write
            .open_table(PENDING)
            .map_err(err)?
            .remove(key)
            .map_err(err)?;
        drop(table);
        write.commit().map_err(err)
    }

    /// Launch-count-bounded slice; each claim scans at most 64 entries, using a
    /// durable rotating cursor. Zero claims does not imply an empty queue. No database
    /// transaction or registry lock is held across dispatch.
    /// Production must not supply a model dispatcher until
    /// every inference is fenced and budgeted. Currently exercised by fakes only.
    pub(crate) fn dispatch_campaign_batch(
        &self,
        limit: usize,
        mut dispatch: impl FnMut(&AdmittedWork) -> DispatchOutcome,
    ) -> Result<usize, String> {
        let mut count = 0;
        for _ in 0..limit.min(MAX_BATCH) {
            let Some(work) = self.claim_campaign_work()? else {
                break;
            };
            let outcome = dispatch(&work);
            self.reconcile_campaign_dispatch(&work, outcome)?;
            count += 1;
        }
        Ok(count)
    }
}
