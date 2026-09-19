//! Daemon-owned adapter only. No IPC, Ghost database access, or worker startup.
#![allow(dead_code)]

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tachyon_model::{accounting::*, ModelError};

use super::{
    admission::AdmittedWork,
    campaign_ledger::{LedgerCommand, Pool, Units, Usage},
    RuntimeStore,
};

mod permits;
pub(crate) mod services;
pub(super) use permits::PermitState;
#[allow(unused_imports)] // Internal host API; worker transport remains gated.
pub(crate) use permits::{ModelBroker, ModelBrokerRequest, ModelPermit};

pub(super) const REQUESTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("campaign_model_requests");

/// Construct only after host authorization of the complete request policy.
/// Not derived from Admission.work_id and not exposed as an IPC capability.
struct DaemonAccounting<'a> {
    store: &'a RuntimeStore,
    authorized: RequestReservation,
}

/// Explicit host choice; admission is funding, not transport authorization.
#[cfg(test)]
struct AdmittedAccounting<'a> {
    accounting: DaemonAccounting<'a>,
    funding: AdmittedWork,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Record {
    pub(super) schema_version: u32,
    pub(super) request: RequestReservation,
    pub(super) usage: RequestUsage,
    #[serde(default)]
    pub(super) allocation_id: Option<String>,
}

fn err(error: impl std::fmt::Display) -> ModelError {
    ModelError::Accounting(error.to_string())
}

#[cfg(test)]
impl RequestAccounting for DaemonAccounting<'_> {
    fn reserve<'a>(&'a self, request: &'a RequestReservation) -> AccountingFuture<'a, String> {
        self.reserve_funded(request, None)
    }

    fn reconcile<'a>(&'a self, receipt: &'a str, usage: RequestUsage) -> AccountingFuture<'a, ()> {
        self.reconcile_funded(receipt, usage, None)
    }
}

#[cfg(test)]
impl RequestAccounting for AdmittedAccounting<'_> {
    fn reserve<'a>(&'a self, request: &'a RequestReservation) -> AccountingFuture<'a, String> {
        self.accounting.reserve_funded(request, Some(&self.funding))
    }

    fn reconcile<'a>(&'a self, receipt: &'a str, usage: RequestUsage) -> AccountingFuture<'a, ()> {
        self.accounting
            .reconcile_funded(receipt, usage, Some(&self.funding.dispatch_id))
    }
}

impl DaemonAccounting<'_> {
    fn reserve_funded<'a>(
        &'a self,
        request: &'a RequestReservation,
        funding: Option<&'a AdmittedWork>,
    ) -> AccountingFuture<'a, String> {
        Box::pin(async move {
            let write = self.store.database.begin_write().map_err(err)?;
            let receipt = self.reserve_funded_in(&write, request, funding)?;
            write.commit().map_err(err)?;
            Ok(receipt)
        })
    }

    fn reserve_funded_in(
        &self,
        write: &WriteTransaction,
        request: &RequestReservation,
        funding: Option<&AdmittedWork>,
    ) -> tachyon_model::Result<String> {
        let identity = &request.identity;
        if request != &self.authorized
            || identity.generation == 0
            || identity.instruction_revision == 0
            || [
                &identity.campaign_id,
                &identity.work_id,
                &identity.attempt_id,
            ]
            .iter()
            .any(|id| id.trim().is_empty() || id.len() > 256)
        {
            return Err(err("unauthorized request identity/policy"));
        }
        let (tokens, cost_micro_usd) = request.estimate.upper_bound()?;
        let receipt = format!("model:{}", uuid::Uuid::new_v4());
        let pool = match identity.class {
            RequestClass::Work | RequestClass::Compaction => Pool::Work,
            RequestClass::Verification => Pool::Verification,
        };
        let reserved = Units {
            tokens,
            cost_micro_usd,
        };
        let command = if let Some(funding) = funding {
            let admission = &funding.admission;
            if identity.campaign_id != admission.campaign_id
                || identity.work_id != admission.work_id
                || identity.generation != admission.generation
                || identity.instruction_revision
                    != RuntimeStore::effective_instruction_revision_in(write, admission)
                        .map_err(err)?
                || pool != admission.pool
            {
                return Err(err("request does not match admitted funding scope"));
            }
            RuntimeStore::admitted_funding_in(write, funding).map_err(err)?;
            RuntimeStore::campaign_ledger_command_in(
                write,
                &format!("fund:{}", funding.dispatch_id),
                &identity.campaign_id,
                LedgerCommand::FundAllocation {
                    reservation_id: funding.dispatch_id.clone(),
                    work_id: funding.admission.work_id.clone(),
                },
            )
            .map_err(err)?;
            LedgerCommand::ReserveAllocated {
                reservation_id: receipt.clone(),
                allocation_id: funding.dispatch_id.clone(),
                pool,
                reserved,
            }
        } else {
            LedgerCommand::Reserve {
                reservation_id: receipt.clone(),
                pool,
                reserved,
            }
        };
        RuntimeStore::campaign_ledger_command_in(
            write,
            &format!("reserve:{receipt}"),
            &identity.campaign_id,
            command,
        )
        .map_err(err)?;
        let record = Record {
            schema_version: 1,
            request: request.clone(),
            usage: RequestUsage::Unknown,
            allocation_id: funding.map(|f| f.dispatch_id.clone()),
        };
        write
            .open_table(REQUESTS)
            .map_err(err)?
            .insert(
                receipt.as_str(),
                serde_json::to_vec(&record).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        Ok(receipt)
    }

    fn reconcile_funded<'a>(
        &'a self,
        receipt: &'a str,
        usage: RequestUsage,
        allocation_id: Option<&'a str>,
    ) -> AccountingFuture<'a, ()> {
        Box::pin(async move { self.reconcile_funded_sync(receipt, usage, allocation_id) })
    }

    fn reconcile_funded_sync(
        &self,
        receipt: &str,
        usage: RequestUsage,
        allocation_id: Option<&str>,
    ) -> tachyon_model::Result<()> {
        let write = self.store.database.begin_write().map_err(err)?;
        let mut table = write.open_table(REQUESTS).map_err(err)?;
        let mut record: Record = serde_json::from_slice(
            table
                .get(receipt)
                .map_err(err)?
                .ok_or_else(|| err("unknown model receipt"))?
                .value(),
        )
        .map_err(err)?;
        if record.schema_version != 1
            || record.request != self.authorized
            || record.allocation_id.as_deref() != allocation_id
        {
            return Err(err("model receipt scope mismatch"));
        }
        if record.usage == usage {
            return Ok(());
        }
        if record.usage != RequestUsage::Unknown {
            return Err(err("final usage conflict"));
        }
        let RequestUsage::Final {
            input_tokens,
            output_tokens,
            cost_micro_usd,
        } = usage
        else {
            return Ok(());
        };
        RuntimeStore::campaign_ledger_command_in(
            &write,
            &format!("final:{receipt}"),
            &record.request.identity.campaign_id,
            LedgerCommand::Reconcile {
                reservation_id: receipt.into(),
                usage: Usage::Final(Units {
                    tokens: input_tokens
                        .checked_add(output_tokens)
                        .ok_or_else(|| err("usage overflow"))?,
                    cost_micro_usd,
                }),
            },
        )
        .map_err(err)?;
        record.usage = usage;
        table
            .insert(
                receipt,
                serde_json::to_vec(&record).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        drop(table);
        write.commit().map_err(err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::campaign_ledger::Envelope;
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        task::{Context, Poll, Waker},
    };
    use tachyon_api::types::{ApiRequest, ApiResponse};

    fn ready<T>(mut future: AccountingFuture<'_, T>) -> tachyon_model::Result<T> {
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("store adapter must not await while holding a transaction"),
        }
    }

    #[test]
    fn admitted_requests_share_funding_not_root_headroom() {
        use super::super::admission::{Admission, DispatchOutcome};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = Arc::new(RuntimeStore::open(&path).unwrap());
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "r".into(),
                objective: "r".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "c".into(),
                objective: "c".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let units = |n| Units {
            tokens: n,
            cost_micro_usd: n,
        };
        store
            .host_authorize_campaign_envelope(
                "grant",
                &campaign.id,
                Envelope {
                    work: units(100),
                    verification: units(100),
                    max_active_inferences: 10,
                },
            )
            .unwrap();
        let funding = store
            .admit_campaign_work(Admission {
                work_id: "stable-work".into(),
                campaign_id: campaign.id.clone(),
                objective: "one objective".into(),
                generation: 1,
                instruction_revision: 1,
                pool: Pool::Work,
                upper_bound: units(100),
            })
            .unwrap();
        let request = RequestReservation {
            identity: WorkIdentity {
                campaign_id: campaign.id.clone(),
                work_id: funding.admission.work_id.clone(),
                attempt_id: "attempt-1".into(),
                generation: 1,
                instruction_revision: 1,
                class: RequestClass::Work,
            },
            estimate: RequestEstimate {
                base_url: "https://example.invalid".into(),
                model: "fake".into(),
                provider: "fake".into(),
                pricing_revision: "1".into(),
                max_request_bytes: 1000,
                input_tokens: 20,
                output_tokens: 10,
                input_micro_usd_per_million: 1_000_000,
                output_micro_usd_per_million: 1_000_000,
                other_micro_usd: 0,
            },
        };
        let make = |request: RequestReservation| AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: request,
            },
            funding: funding.clone(),
        };
        assert!(ready(make(request.clone()).reserve(&request)).is_err());
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "worker".into(),
            })
            .unwrap();
        let before = store.campaign_ledger(&campaign.id).unwrap().unwrap();
        let aborted_receipt;
        {
            let write = store.database.begin_write().unwrap();
            aborted_receipt = make(request.clone())
                .accounting
                .reserve_funded_in(&write, &request, Some(&funding))
                .unwrap();
            assert!(write
                .open_table(REQUESTS)
                .unwrap()
                .get(aborted_receipt.as_str())
                .unwrap()
                .is_some());
            assert_eq!(
                store.campaign_ledger(&campaign.id).unwrap().unwrap(),
                before
            );
            // Drop after conversion, child hold, both ledger receipts and request
            // evidence have been written, but before any can become visible.
        }
        assert_eq!(
            store.campaign_ledger(&campaign.id).unwrap().unwrap(),
            before
        );
        assert!(
            ready(make(request.clone()).reconcile(&aborted_receipt, RequestUsage::Unknown))
                .is_err()
        );
        let mut oversized = request.clone();
        oversized.estimate.input_tokens = 100;
        assert!(ready(make(oversized.clone()).reserve(&oversized)).is_err());
        assert_eq!(
            store.campaign_ledger(&campaign.id).unwrap().unwrap(),
            before
        );
        assert!(ready(
            DaemonAccounting {
                store: &store,
                authorized: request.clone()
            }
            .reserve(&request)
        )
        .is_err());
        for cause in 0..5 {
            let mut wrong = request.clone();
            match cause {
                0 => wrong.identity.class = RequestClass::Verification,
                1 => wrong.identity.campaign_id = "other-campaign".into(),
                2 => wrong.identity.work_id = "other-work".into(),
                3 => wrong.identity.generation += 1,
                _ => wrong.identity.instruction_revision += 1,
            }
            assert!(ready(make(wrong.clone()).reserve(&wrong)).is_err());
            assert_eq!(
                store.campaign_ledger(&campaign.id).unwrap().unwrap(),
                before
            );
        }
        let mut wrong_purpose = request.clone();
        wrong_purpose.identity.class = RequestClass::Compaction;
        assert!(ready(make(request.clone()).reserve(&wrong_purpose)).is_err());
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, funding, request, barrier) = (
                    store.clone(),
                    funding.clone(),
                    request.clone(),
                    barrier.clone(),
                );
                std::thread::spawn(move || {
                    barrier.wait();
                    ready(
                        AdmittedAccounting {
                            accounting: DaemonAccounting {
                                store: &store,
                                authorized: request.clone(),
                            },
                            funding,
                        }
                        .reserve(&request),
                    )
                })
            })
            .collect();
        let receipts: Vec<_> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap().ok())
            .collect();
        assert_eq!(receipts.len(), 3);
        let ledger = store.campaign_ledger(&campaign.id).unwrap().unwrap();
        assert_eq!(
            ledger.allocation_available(&funding.dispatch_id).unwrap(),
            units(10)
        );
        assert_eq!(ledger.committed(Pool::Work).unwrap().tokens, 100);
        assert_eq!(ledger.active_inferences(), 3);
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let adapter = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: request.clone(),
            },
            funding: funding.clone(),
        };
        let final_usage = RequestUsage::Final {
            input_tokens: 10,
            output_tokens: 0,
            cost_micro_usd: 10,
        };
        assert!(ready(adapter.accounting.reconcile(&receipts[0], final_usage)).is_err());
        let mut cross = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: request.clone(),
            },
            funding: funding.clone(),
        };
        cross.funding.dispatch_id = "other-allocation".into();
        assert!(ready(cross.reconcile(&receipts[0], final_usage)).is_err());
        for receipt in &receipts {
            ready(adapter.reconcile(receipt, final_usage)).unwrap();
            ready(adapter.reconcile(receipt, final_usage)).unwrap();
        }
        let mut retry = request.clone();
        retry.identity.attempt_id = "attempt-2".into();
        retry.estimate.input_tokens = 60;
        let retry_adapter = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: retry.clone(),
            },
            funding: funding.clone(),
        };
        let receipt = ready(retry_adapter.reserve(&retry)).unwrap();
        assert!(ready(adapter.reserve(&request)).is_err());
        ready(retry_adapter.reconcile(
            &receipt,
            RequestUsage::Final {
                input_tokens: 40,
                output_tokens: 0,
                cost_micro_usd: 40,
            },
        ))
        .unwrap();
        let closed = store
            .campaign_ledger_command(
                "close",
                &campaign.id,
                LedgerCommand::CloseAllocation {
                    reservation_id: funding.dispatch_id.clone(),
                },
            )
            .unwrap();
        assert_eq!(closed.committed(Pool::Work).unwrap().tokens, 70);
        assert_eq!(closed.committed(Pool::Work).unwrap().cost_micro_usd, 70);
        assert!(ready(adapter.reserve(&request)).is_err());
        assert!(store.list_tasks().unwrap().is_empty());
        // Closing returned exactly 30 to root, not the full admission or twice
        // the request refunds. Ordinary funding can consume that remainder.
        ready(adapter.accounting.reserve(&request)).unwrap();
        assert!(ready(adapter.accounting.reserve(&request)).is_err());
        let reused = store.campaign_ledger(&campaign.id).unwrap().unwrap();
        for command_id in ["close", "close-again"] {
            store
                .campaign_ledger_command(
                    command_id,
                    &campaign.id,
                    LedgerCommand::CloseAllocation {
                        reservation_id: funding.dispatch_id.clone(),
                    },
                )
                .unwrap();
            assert_eq!(
                store.campaign_ledger(&campaign.id).unwrap().unwrap(),
                reused
            );
            assert!(ready(adapter.reserve(&request)).is_err());
            assert!(ready(adapter.accounting.reserve(&request)).is_err());
        }
        let mut verification_admission = funding.admission.clone();
        verification_admission.work_id = "verification-work".into();
        verification_admission.pool = Pool::Verification;
        let verification_funding = store.admit_campaign_work(verification_admission).unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "verifier".into(),
            })
            .unwrap();
        let mut verification_request = request.clone();
        verification_request.identity.work_id = verification_funding.admission.work_id.clone();
        let ordinary = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: verification_request.clone(),
            },
            funding: verification_funding.clone(),
        };
        assert!(ready(ordinary.reserve(&verification_request)).is_err());
        verification_request.identity.class = RequestClass::Verification;
        let verification = AdmittedAccounting {
            accounting: DaemonAccounting {
                store: &store,
                authorized: verification_request.clone(),
            },
            funding: verification_funding,
        };
        let verification_receipt = ready(verification.reserve(&verification_request)).unwrap();
        assert_eq!(
            store
                .campaign_ledger(&campaign.id)
                .unwrap()
                .unwrap()
                .committed(Pool::Verification)
                .unwrap()
                .tokens,
            100
        );
        let actual = Units {
            tokens: 40,
            cost_micro_usd: 45,
        };
        let provisional = store
            .campaign_ledger_command(
                "verification-provisional",
                &campaign.id,
                LedgerCommand::Reconcile {
                    reservation_id: verification_receipt.clone(),
                    usage: Usage::Provisional(actual),
                },
            )
            .unwrap();
        assert_eq!(
            provisional.committed(Pool::Verification).unwrap().tokens,
            110
        );
        assert_eq!(
            provisional
                .committed(Pool::Verification)
                .unwrap()
                .cost_micro_usd,
            115
        );
        assert_eq!(provisional.debt.tokens, 10);
        assert_eq!(provisional.debt.cost_micro_usd, 15);
        let close = LedgerCommand::CloseAllocation {
            reservation_id: verification.funding.dispatch_id.clone(),
        };
        assert!(store
            .campaign_ledger_command("close-verification", &campaign.id, close.clone())
            .is_err());
        assert!(ready(verification.reserve(&verification_request)).is_err());
        let final_usage = RequestUsage::Final {
            input_tokens: 35,
            output_tokens: 5,
            cost_micro_usd: 45,
        };
        ready(verification.reconcile(&verification_receipt, final_usage)).unwrap();
        let closed = store
            .campaign_ledger_command("close-verification", &campaign.id, close)
            .unwrap();
        assert_eq!(closed.committed(Pool::Verification).unwrap().tokens, 40);
        assert_eq!(
            closed.committed(Pool::Verification).unwrap().cost_micro_usd,
            45
        );
        assert_eq!(closed.debt, provisional.debt);
        assert!(closed.admissions_paused);
        ready(verification.reconcile(&verification_receipt, final_usage)).unwrap();
        assert_eq!(
            store.campaign_ledger(&campaign.id).unwrap().unwrap(),
            closed
        );
    }

    #[test]
    fn concurrent_requests_retry_restart_scope_and_final_usage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = Arc::new(RuntimeStore::open(&path).unwrap());
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: "r".into(),
                title: "r".into(),
                objective: "r".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: "c".into(),
                research_id: research.id,
                title: "c".into(),
                objective: "c".into(),
            })
            .unwrap()
        else {
            panic!()
        };
        store
            .host_authorize_campaign_envelope(
                "grant",
                &campaign.id,
                Envelope {
                    work: Units {
                        tokens: 120,
                        cost_micro_usd: 150,
                    },
                    verification: Units {
                        tokens: 120,
                        cost_micro_usd: 150,
                    },
                    max_active_inferences: 2,
                },
            )
            .unwrap();
        let request = RequestReservation {
            identity: WorkIdentity {
                campaign_id: campaign.id.clone(),
                work_id: "stable-objective".into(),
                attempt_id: "execution-1".into(),
                generation: 1,
                instruction_revision: 1,
                class: RequestClass::Compaction,
            },
            estimate: RequestEstimate {
                base_url: "https://example.invalid".into(),
                model: "fake".into(),
                provider: "fake".into(),
                pricing_revision: "1".into(),
                max_request_bytes: 10000,
                input_tokens: 100,
                output_tokens: 20,
                input_micro_usd_per_million: 1_000_000,
                output_micro_usd_per_million: 2_000_000,
                other_micro_usd: 10,
            },
        };
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, request, barrier) = (store.clone(), request.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    ready(
                        DaemonAccounting {
                            store: &store,
                            authorized: request.clone(),
                        }
                        .reserve(&request),
                    )
                })
            })
            .collect();
        let receipts: Vec<_> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap().ok())
            .collect();
        assert_eq!(receipts.len(), 1);
        let receipt = &receipts[0];
        {
            let adapter = DaemonAccounting {
                store: &store,
                authorized: request.clone(),
            };
            ready(adapter.reconcile(receipt, RequestUsage::Unknown)).unwrap();
            assert!(ready(adapter.reserve(&request)).is_err());
            let mut wrong = request.clone();
            wrong.identity.attempt_id = "other".into();
            assert!(ready(adapter.reserve(&wrong)).is_err());
            assert!(ready(
                DaemonAccounting {
                    store: &store,
                    authorized: wrong
                }
                .reconcile(receipt, RequestUsage::Unknown)
            )
            .is_err());
        }
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let adapter = DaemonAccounting {
            store: &store,
            authorized: request.clone(),
        };
        assert_eq!(
            store
                .campaign_ledger(&campaign.id)
                .unwrap()
                .unwrap()
                .reservations[receipt]
                .usage,
            Usage::Unknown
        );
        ready(adapter.reconcile(
            receipt,
            RequestUsage::Final {
                input_tokens: 30,
                output_tokens: 7,
                cost_micro_usd: 55,
            },
        ))
        .unwrap();
        assert_eq!(
            store
                .campaign_ledger(&campaign.id)
                .unwrap()
                .unwrap()
                .committed(Pool::Work)
                .unwrap()
                .tokens,
            37
        );
        assert!(ready(adapter.reconcile(
            receipt,
            RequestUsage::Final {
                input_tokens: 0,
                output_tokens: 0,
                cost_micro_usd: 0
            }
        ))
        .is_err());
        assert!(ready(adapter.reconcile(receipt, RequestUsage::Unknown)).is_err());
        let mut retry = request.clone();
        retry.estimate.input_tokens = 40;
        let retry_adapter = DaemonAccounting {
            store: &store,
            authorized: retry.clone(),
        };
        let retry_receipt = ready(retry_adapter.reserve(&retry)).unwrap();
        assert_ne!(*receipt, retry_receipt);
        assert_eq!(retry.identity, request.identity);
        // Protected verifier allowance remains separate from work/compaction.
        let mut verifier = request.clone();
        verifier.identity.class = RequestClass::Verification;
        let verification = DaemonAccounting {
            store: &store,
            authorized: verifier.clone(),
        };
        let next = ready(verification.reserve(&verifier)).unwrap();
        assert_ne!(*receipt, next);
        assert_eq!(
            store
                .campaign_ledger(&campaign.id)
                .unwrap()
                .unwrap()
                .reservations[&next]
                .pool,
            Pool::Verification
        );
        assert!(store.list_tasks().unwrap().is_empty());
        // Malicious arithmetic must neither panic nor release the durable hold.
        assert!(ready(verification.reconcile(
            &next,
            RequestUsage::Final {
                input_tokens: u64::MAX,
                output_tokens: 1,
                cost_micro_usd: u64::MAX,
            },
        ))
        .is_err());
        assert_eq!(
            store
                .campaign_ledger(&campaign.id)
                .unwrap()
                .unwrap()
                .reservations[&next]
                .usage,
            Usage::Unknown
        );
        let mut malicious = request.clone();
        malicious.estimate.input_tokens = u64::MAX;
        malicious.estimate.input_micro_usd_per_million = u64::MAX;
        let malicious_adapter = DaemonAccounting {
            store: &store,
            authorized: malicious.clone(),
        };
        assert!(ready(malicious_adapter.reserve(&malicious)).is_err());
        // Do not clamp provider overcharges to the reservation: record debt and
        // pause ALL pools, even when the reported cost is the largest u64.
        ready(verification.reconcile(
            &next,
            RequestUsage::Final {
                input_tokens: 100,
                output_tokens: 21,
                cost_micro_usd: u64::MAX,
            },
        ))
        .unwrap();
        let ledger = store.campaign_ledger(&campaign.id).unwrap().unwrap();
        assert!(ledger.admissions_paused);
        assert!(ledger.debt.cost_micro_usd > 0);
        assert!(ready(adapter.reserve(&request)).is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store.campaign_ledger(&campaign.id).unwrap().unwrap(),
            ledger
        );
    }
}
