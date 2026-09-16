//! Internal accounting, not execution authorization. Campaign metadata stays Draft.
//! A host must explicitly authorize one immutable root envelope. Retries/children
//! reserve against that same root; neither reservations nor reconciliation mint money.
#![allow(dead_code)] // Foundation only: deliberately not wired to IPC or execution.

use std::collections::BTreeMap;

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use super::{research::CAMPAIGNS, RuntimeStore};

const ROOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_ledger_roots");
const RECEIPTS: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_ledger_receipts");

pub(super) fn initialize(write: &WriteTransaction) -> Result<(), String> {
    write.open_table(ROOTS).map_err(err)?;
    write.open_table(RECEIPTS).map_err(err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::types::{ApiRequest, ApiResponse, CampaignStatus};

    fn units(n: u64) -> Units {
        Units {
            tokens: n,
            cost_micro_usd: n,
        }
    }

    #[test]
    fn transfer_available_conserves_holds_settlements_closure_and_replay() {
        use super::super::admission::{Admission, DispatchOutcome};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(&store, "transfer");
        store
            .host_authorize_campaign_envelope("grant", &c, envelope())
            .unwrap();
        let mut ids = Vec::new();
        for (id, pool, n) in [
            ("a", Pool::Work, 60),
            ("b", Pool::Work, 40),
            ("v", Pool::Verification, 20),
        ] {
            let w = store
                .admit_campaign_work(Admission {
                    work_id: id.into(),
                    campaign_id: c.clone(),
                    objective: id.into(),
                    generation: 1,
                    instruction_revision: 1,
                    pool,
                    upper_bound: units(n),
                })
                .unwrap();
            ids.push(w.dispatch_id);
        }
        store
            .dispatch_campaign_batch(3, |_| DispatchOutcome::Registered {
                worker_id: "worker".into(),
            })
            .unwrap();
        for (id, work) in ids.iter().zip(["a", "b", "v"]) {
            store
                .campaign_ledger_command(
                    &format!("fund-{work}"),
                    &c,
                    LedgerCommand::FundAllocation {
                        reservation_id: id.clone(),
                        work_id: work.into(),
                    },
                )
                .unwrap();
        }
        store
            .campaign_ledger_command(
                "hold",
                &c,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "hold".into(),
                    allocation_id: ids[0].clone(),
                    pool: Pool::Work,
                    reserved: units(30),
                },
            )
            .unwrap();
        let before = store.campaign_ledger(&c).unwrap().unwrap();
        let transfer =
            |source: &str, target: &str, amounts, revision| LedgerCommand::TransferAvailable {
                source_allocation_id: source.into(),
                target_allocation_id: target.into(),
                amounts,
                expected_revision: revision,
            };
        for command in [
            transfer(&ids[0], &ids[1], units(31), before.revision),
            transfer(&ids[0], &ids[1], units(1), before.revision - 1),
            transfer(&ids[0], &ids[2], units(1), before.revision),
            transfer(&ids[0], &ids[0], units(1), before.revision),
            transfer(&ids[0], &ids[1], units(0), before.revision),
            transfer("missing", &ids[1], units(1), before.revision),
        ] {
            assert!(store
                .campaign_ledger_command("invalid", &c, command)
                .is_err());
            assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), before);
        }
        let command = transfer(
            &ids[0],
            &ids[1],
            Units {
                tokens: 30,
                cost_micro_usd: 0,
            },
            before.revision,
        );
        let moved = store
            .campaign_ledger_command("move", &c, command.clone())
            .unwrap();
        let receipt_snapshot = moved.clone();
        assert_eq!(
            moved.reservations, before.reservations,
            "unknown hold and original grants unchanged"
        );
        assert_eq!(
            moved.committed(Pool::Work).unwrap(),
            before.committed(Pool::Work).unwrap()
        );
        assert_eq!(
            moved.allocation_available(&ids[1]).unwrap(),
            Units {
                tokens: 70,
                cost_micro_usd: 40
            }
        );
        let moved = store
            .campaign_ledger_command(
                "move-cost",
                &c,
                transfer(
                    &ids[0],
                    &ids[1],
                    Units {
                        tokens: 0,
                        cost_micro_usd: 30,
                    },
                    moved.revision,
                ),
            )
            .unwrap();
        assert_eq!(moved.allocation_available(&ids[1]).unwrap(), units(70));
        // Incoming funds can be sent back even when total sent exceeds the original grant.
        let returned = store
            .campaign_ledger_command(
                "return-all",
                &c,
                transfer(&ids[1], &ids[0], units(70), moved.revision),
            )
            .unwrap();
        assert_eq!(returned.allocation_allowance(&ids[1]).unwrap(), units(0));
        store
            .campaign_ledger_command(
                "restore-transfer",
                &c,
                transfer(&ids[0], &ids[1], units(70), returned.revision),
            )
            .unwrap();
        store
            .campaign_ledger_command(
                "target-request",
                &c,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "target-request".into(),
                    allocation_id: ids[1].clone(),
                    pool: Pool::Work,
                    reserved: units(70),
                },
            )
            .unwrap();
        let settled = store
            .campaign_ledger_command(
                "source-final",
                &c,
                reconcile("hold", Usage::Final(units(10))),
            )
            .unwrap();
        assert_eq!(
            settled.committed(Pool::Work).unwrap(),
            before.committed(Pool::Work).unwrap()
        );
        // Historical request estimates may exceed the new allowance after their refund.
        let restored = store
            .campaign_ledger_command(
                "refund-transfer",
                &c,
                transfer(&ids[0], &ids[1], units(20), settled.revision),
            )
            .unwrap();
        assert_eq!(restored.allocation_allowance(&ids[0]).unwrap(), units(10));
        store
            .campaign_ledger_command(
                "target-final",
                &c,
                reconcile("target-request", Usage::Final(units(50))),
            )
            .unwrap();
        for id in &ids[..2] {
            store
                .campaign_ledger_command(
                    &format!("close-{id}"),
                    &c,
                    LedgerCommand::CloseAllocation {
                        reservation_id: id.clone(),
                    },
                )
                .unwrap();
        }
        let closed = store.campaign_ledger(&c).unwrap().unwrap();
        assert_eq!(
            closed.committed(Pool::Work).unwrap(),
            Totals {
                tokens: 60,
                cost_micro_usd: 60
            }
        );
        assert_eq!(closed.envelope, before.envelope);
        for (source, target) in [(&ids[0], &ids[1]), (&ids[1], &ids[0])] {
            assert!(store
                .campaign_ledger_command(
                    "post-close",
                    &c,
                    transfer(source, target, units(1), closed.revision)
                )
                .is_err());
        }
        assert!(store
            .campaign_ledger_command(
                "move",
                &c,
                transfer(&ids[0], &ids[1], units(1), before.revision)
            )
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store.campaign_ledger_command("move", &c, command).unwrap(),
            receipt_snapshot
        );
        store
            .campaign_ledger_command(
                "close-again",
                &c,
                LedgerCommand::CloseAllocation {
                    reservation_id: ids[1].clone(),
                },
            )
            .unwrap();
        assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), closed);
    }

    #[test]
    fn transfer_late_overrun_preserves_debt_and_fences_every_mutation() {
        use super::super::admission::{Admission, DispatchOutcome};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(&store, "late-overrun");
        store
            .host_authorize_campaign_envelope("grant", &c, envelope())
            .unwrap();
        let mut ids = Vec::new();
        for (id, n) in [("source", 60), ("target", 40)] {
            let work = store
                .admit_campaign_work(Admission {
                    work_id: id.into(),
                    campaign_id: c.clone(),
                    objective: id.into(),
                    generation: 1,
                    instruction_revision: 1,
                    pool: Pool::Work,
                    upper_bound: units(n),
                })
                .unwrap();
            ids.push(work.dispatch_id);
        }
        store
            .dispatch_campaign_batch(2, |_| DispatchOutcome::Registered {
                worker_id: "worker".into(),
            })
            .unwrap();
        for (id, work) in ids.iter().zip(["source", "target"]) {
            let before = store.campaign_ledger(&c).unwrap().unwrap();
            let funded = store
                .campaign_ledger_command(
                    work,
                    &c,
                    LedgerCommand::FundAllocation {
                        reservation_id: id.clone(),
                        work_id: work.into(),
                    },
                )
                .unwrap();
            assert_eq!(funded.revision, before.revision + 1);
        }
        let transfer = |revision| LedgerCommand::TransferAvailable {
            source_allocation_id: ids[0].clone(),
            target_allocation_id: ids[1].clone(),
            amounts: units(30),
            expected_revision: revision,
        };
        for (id, command) in [
            (
                "reserve",
                LedgerCommand::ReserveAllocated {
                    reservation_id: "claim".into(),
                    allocation_id: ids[0].clone(),
                    pool: Pool::Work,
                    reserved: units(30),
                },
            ),
            (
                "cancel",
                LedgerCommand::Cancel {
                    reservation_id: "claim".into(),
                },
            ),
            (
                "provisional",
                reconcile("claim", Usage::Provisional(units(20))),
            ),
            ("standalone", reserve("standalone", Pool::Verification, 1)),
        ] {
            let before = store.campaign_ledger(&c).unwrap().unwrap();
            let after = store.campaign_ledger_command(id, &c, command).unwrap();
            assert_eq!(after.revision, before.revision + 1);
            assert!(store
                .campaign_ledger_command("stale", &c, transfer(before.revision))
                .is_err());
            assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), after);
        }
        let before = store.campaign_ledger(&c).unwrap().unwrap();
        store
            .campaign_ledger_command("move", &c, transfer(before.revision))
            .unwrap();
        store
            .campaign_ledger_command(
                "target-spend",
                &c,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "target-spend".into(),
                    allocation_id: ids[1].clone(),
                    pool: Pool::Work,
                    reserved: units(70),
                },
            )
            .unwrap();
        store
            .campaign_ledger_command(
                "target-final",
                &c,
                reconcile("target-spend", Usage::Final(units(70))),
            )
            .unwrap();
        for usage in [Usage::Provisional(units(50)), Usage::Final(units(50))] {
            let ledger = store
                .campaign_ledger_command(
                    &format!("overrun-{usage:?}"),
                    &c,
                    reconcile("claim", usage),
                )
                .unwrap();
            assert_eq!(ledger.debt.tokens, 20);
            assert_eq!(ledger.debt.cost_micro_usd, 20);
            assert!(ledger.admissions_paused);
            assert_eq!(
                ledger.committed(Pool::Work).unwrap(),
                Totals {
                    tokens: 120,
                    cost_micro_usd: 120
                }
            );
            assert!(store
                .campaign_ledger_command("paused", &c, transfer(ledger.revision))
                .is_err());
            assert!(store
                .campaign_ledger_command("overspend", &c, reserve("overspend", Pool::Work, 1))
                .is_err());
        }
        for id in &ids {
            let before = store.campaign_ledger(&c).unwrap().unwrap();
            let closed = store
                .campaign_ledger_command(
                    &format!("close-{id}"),
                    &c,
                    LedgerCommand::CloseAllocation {
                        reservation_id: id.clone(),
                    },
                )
                .unwrap();
            assert_eq!(closed.revision, before.revision + 1);
            assert_eq!(
                closed.committed(Pool::Work).unwrap(),
                before.committed(Pool::Work).unwrap()
            );
        }
        let closed = store.campaign_ledger(&c).unwrap().unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), closed);
    }

    fn envelope() -> Envelope {
        Envelope {
            work: units(100),
            verification: units(20),
            max_active_inferences: 10,
        }
    }

    fn campaign(store: &RuntimeStore, key: &str) -> String {
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: format!("research-{key}"),
                title: "Research".into(),
                objective: "Objective".into(),
            })
            .unwrap()
        else {
            panic!("research response")
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: format!("campaign-{key}"),
                research_id: research.id,
                title: "Campaign".into(),
                objective: "Objective".into(),
            })
            .unwrap()
        else {
            panic!("campaign response")
        };
        campaign.id
    }

    fn reserve(id: &str, pool: Pool, n: u64) -> LedgerCommand {
        LedgerCommand::Reserve {
            reservation_id: id.into(),
            pool,
            reserved: units(n),
        }
    }

    fn reconcile(id: &str, usage: Usage) -> LedgerCommand {
        LedgerCommand::Reconcile {
            reservation_id: id.into(),
            usage,
        }
    }

    #[test]
    fn cancelled_settled_or_debt_paused_hold_cannot_convert() {
        use super::super::admission::{Admission, DispatchOutcome};
        for cause in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
            let id = campaign(&store, "conversion");
            store
                .host_authorize_campaign_envelope("grant", &id, envelope())
                .unwrap();
            let work = store
                .admit_campaign_work(Admission {
                    work_id: "work".into(),
                    campaign_id: id.clone(),
                    objective: "objective".into(),
                    generation: 1,
                    instruction_revision: 1,
                    pool: Pool::Work,
                    upper_bound: units(100),
                })
                .unwrap();
            store
                .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                    worker_id: "worker".into(),
                })
                .unwrap();
            let command = match cause {
                0 => LedgerCommand::Cancel {
                    reservation_id: work.dispatch_id.clone(),
                },
                1 => reconcile(&work.dispatch_id, Usage::Provisional(units(1))),
                2 => reconcile(&work.dispatch_id, Usage::Final(units(0))),
                _ => {
                    store
                        .campaign_ledger_command(
                            "other",
                            &id,
                            reserve("other", Pool::Verification, 1),
                        )
                        .unwrap();
                    reconcile("other", Usage::Final(units(2)))
                }
            };
            let before = store
                .campaign_ledger_command("block", &id, command)
                .unwrap();
            assert!(
                store
                    .campaign_ledger_command(
                        "fund",
                        &id,
                        LedgerCommand::FundAllocation {
                            reservation_id: work.dispatch_id,
                            work_id: work.admission.work_id,
                        }
                    )
                    .is_err(),
                "cause {cause}"
            );
            assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), before);
        }
    }

    #[test]
    fn funded_transfer_rollback_conservation_provisional_close_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "funded");
        store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap();
        use super::super::admission::{Admission, DispatchOutcome};
        let work = store
            .admit_campaign_work(Admission {
                work_id: "stable-work".into(),
                campaign_id: id.clone(),
                objective: "objective".into(),
                generation: 1,
                instruction_revision: 1,
                pool: Pool::Work,
                upper_bound: units(100),
            })
            .unwrap();
        let allocation = work.dispatch_id;
        let transfer = LedgerCommand::FundAllocation {
            reservation_id: allocation.clone(),
            work_id: "stable-work".into(),
        };
        assert!(store
            .campaign_ledger_command("fund", &id, transfer.clone())
            .is_err());
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "worker".into(),
            })
            .unwrap();
        store
            .campaign_ledger_command("ordinary", &id, reserve("ordinary", Pool::Verification, 1))
            .unwrap();
        assert!(store
            .campaign_ledger_command(
                "not-admission",
                &id,
                LedgerCommand::FundAllocation {
                    reservation_id: "ordinary".into(),
                    work_id: "stable-work".into(),
                }
            )
            .is_err());
        store
            .campaign_ledger_command(
                "ordinary-final",
                &id,
                reconcile("ordinary", Usage::Final(units(0))),
            )
            .unwrap();
        let initial = store.campaign_ledger(&id).unwrap().unwrap();
        let request = LedgerCommand::ReserveAllocated {
            reservation_id: "request".into(),
            allocation_id: allocation.clone(),
            pool: Pool::Work,
            reserved: units(60),
        };
        {
            let write = store.database.begin_write().unwrap();
            RuntimeStore::campaign_ledger_command_in(&write, "fund", &id, transfer.clone())
                .unwrap();
            RuntimeStore::campaign_ledger_command_in(&write, "request", &id, request.clone())
                .unwrap();
            assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), initial);
        }
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), initial);
        store
            .campaign_ledger_command("fund", &id, transfer)
            .unwrap();
        let held = store
            .campaign_ledger_command("request", &id, request.clone())
            .unwrap();
        assert_eq!(held.committed(Pool::Work).unwrap().tokens, 100);
        assert_eq!(held.allocation_available(&allocation).unwrap(), units(40));
        assert_eq!(held.active_inferences(), 1);
        let close = LedgerCommand::CloseAllocation {
            reservation_id: allocation.clone(),
        };
        assert!(store
            .campaign_ledger_command("close", &id, close.clone())
            .is_err());
        assert!(store
            .campaign_ledger_command(
                "parent-final",
                &id,
                reconcile(&allocation, Usage::Final(units(0)))
            )
            .is_err());
        store
            .campaign_ledger_command(
                "cancel",
                &id,
                LedgerCommand::Cancel {
                    reservation_id: "request".into(),
                },
            )
            .unwrap();
        let actual = Units {
            tokens: 80,
            cost_micro_usd: 65,
        };
        let provisional = store
            .campaign_ledger_command(
                "partial",
                &id,
                reconcile("request", Usage::Provisional(actual)),
            )
            .unwrap();
        assert_eq!(
            provisional.committed(Pool::Work).unwrap(),
            Totals {
                tokens: 120,
                cost_micro_usd: 105
            }
        );
        assert_eq!(
            provisional.debt,
            Totals {
                tokens: 20,
                cost_micro_usd: 5
            }
        );
        assert!(provisional.admissions_paused);
        assert_eq!(
            provisional.allocation_available(&allocation).unwrap(),
            units(40)
        );
        assert!(store
            .campaign_ledger_command(
                "paused-child",
                &id,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "another-request".into(),
                    allocation_id: allocation.clone(),
                    pool: Pool::Work,
                    reserved: units(1),
                }
            )
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), provisional);
        assert!(store
            .campaign_ledger_command("close", &id, close.clone())
            .is_err());
        store
            .campaign_ledger_command("final", &id, reconcile("request", Usage::Final(actual)))
            .unwrap();
        let closed = store
            .campaign_ledger_command("close", &id, close.clone())
            .unwrap();
        assert_eq!(
            closed.committed(Pool::Work).unwrap(),
            Totals {
                tokens: 80,
                cost_micro_usd: 65
            }
        );
        assert_eq!(closed.debt, provisional.debt);
        assert_eq!(closed.allocation_available(&allocation).unwrap(), units(0));
        let mut corrupt = closed.clone();
        corrupt.reservations.get_mut(&allocation).unwrap().reserved = units(101);
        assert!(corrupt.validate(&id).is_err());
        assert_eq!(
            store
                .campaign_ledger_command("close-again", &id, close)
                .unwrap(),
            closed
        );
        assert_eq!(
            store
                .campaign_ledger_command(
                    "final-again",
                    &id,
                    reconcile("request", Usage::Final(actual))
                )
                .unwrap(),
            closed
        );
        let other = campaign(&store, "cross-scope");
        assert!(store
            .campaign_ledger_command("request", &other, request)
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), closed);
    }

    #[test]
    fn provisional_overrun_and_final_release_preserve_accumulated_actuals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "release");
        store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap();
        store
            .campaign_ledger_command("a", &id, reserve("a", Pool::Work, 60))
            .unwrap();
        store
            .campaign_ledger_command("final-a", &id, reconcile("a", Usage::Final(units(20))))
            .unwrap();
        store
            .campaign_ledger_command("b", &id, reserve("b", Pool::Work, 80))
            .unwrap();
        let lower_bound = Units {
            tokens: 110,
            cost_micro_usd: 30,
        };
        let provisional = store
            .campaign_ledger_command(
                "partial-b",
                &id,
                reconcile("b", Usage::Provisional(lower_bound)),
            )
            .unwrap();
        assert_eq!(
            provisional.committed(Pool::Work).unwrap(),
            Totals {
                tokens: 130,
                cost_micro_usd: 100
            }
        );
        assert_eq!(provisional.active_inferences(), 1);
        let final_state = store
            .campaign_ledger_command("final-b", &id, reconcile("b", Usage::Final(lower_bound)))
            .unwrap();
        assert_eq!(
            final_state.committed(Pool::Work).unwrap(),
            Totals {
                tokens: 130,
                cost_micro_usd: 50
            }
        );
        assert_eq!(
            final_state.debt,
            Totals {
                tokens: 30,
                cost_micro_usd: 0
            }
        );
        assert_eq!(final_state.active_inferences(), 0);
        assert!(final_state.admissions_paused);
        assert!(store
            .campaign_ledger_command("blocked", &id, reserve("c", Pool::Verification, 0))
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), final_state);
        assert_eq!(
            store
                .campaign_ledger_command(
                    "partial-b",
                    &id,
                    reconcile("b", Usage::Provisional(lower_bound))
                )
                .unwrap(),
            provisional
        );
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), final_state);
    }

    #[test]
    fn malformed_roots_and_receipt_snapshots_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "corrupt");
        store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap();
        let action = reserve("a", Pool::Work, 60);
        let valid = store
            .campaign_ledger_command("a", &id, action.clone())
            .unwrap();
        for case in 0..9 {
            let mut invalid = valid.clone();
            match case {
                0 => invalid.schema_version = 2,
                1 => invalid.campaign_id = "another-campaign".into(),
                2 => invalid.envelope.work = units(u64::MAX),
                3 => invalid.envelope.max_active_inferences = 0,
                4 => invalid.debt.tokens = 1,
                5 => {
                    invalid.reservations.get_mut("a").unwrap().usage = Usage::Provisional(units(70))
                }
                6 => {
                    invalid.reservations.get_mut("a").unwrap().usage =
                        Usage::Provisional(units(70));
                    invalid.debt = Totals {
                        tokens: 10,
                        cost_micro_usd: 10,
                    };
                }
                7 => {
                    // Internally consistent debt/pause must not hide excessive holds.
                    invalid
                        .reservations
                        .insert("b".into(), invalid.reservations["a"].clone());
                    invalid.admissions_paused = true;
                }
                8 => invalid.reservations.get_mut("a").unwrap().reserved = units(101),
                _ => unreachable!(),
            }
            let bytes = serde_json::to_vec(&invalid).unwrap();
            let write = store.database.begin_write().unwrap();
            write
                .open_table(ROOTS)
                .unwrap()
                .insert(id.as_str(), bytes.as_slice())
                .unwrap();
            let receipt = Receipt {
                schema_version: 1,
                campaign_id: id.clone(),
                action: Action::Command(action.clone()),
                result: invalid,
            };
            write
                .open_table(RECEIPTS)
                .unwrap()
                .insert("a", serde_json::to_vec(&receipt).unwrap().as_slice())
                .unwrap();
            write.commit().unwrap();
            assert!(store.campaign_ledger(&id).is_err(), "case {case}");
            assert!(
                store
                    .campaign_ledger_command("a", &id, action.clone())
                    .is_err(),
                "receipt case {case}"
            );
            assert!(
                store
                    .campaign_ledger_command("failed", &id, reconcile("a", Usage::Final(units(0))))
                    .is_err(),
                "mutation case {case}"
            );
            let read = store.database.begin_read().unwrap();
            assert!(read
                .open_table(RECEIPTS)
                .unwrap()
                .get("failed")
                .unwrap()
                .is_none());
            assert_eq!(
                read.open_table(ROOTS)
                    .unwrap()
                    .get(id.as_str())
                    .unwrap()
                    .unwrap()
                    .value(),
                bytes.as_slice()
            );
        }
    }

    #[test]
    fn totals_json_is_exact_numeric_u128() {
        let totals = Totals {
            tokens: u128::MAX,
            cost_micro_usd: u128::from(u64::MAX) + 1,
        };
        let json = serde_json::to_string(&totals).unwrap();
        assert_eq!(
            json,
            format!(
                "{{\"tokens\":{},\"cost_micro_usd\":18446744073709551616}}",
                u128::MAX
            )
        );
        assert_eq!(serde_json::from_str::<Totals>(&json).unwrap(), totals);
        for number in [
            "-1",
            "1.5",
            "340282366920938463463374607431768211456",
            "\"1\"",
        ] {
            assert!(serde_json::from_str::<Totals>(&format!(
                "{{\"tokens\":{number},\"cost_micro_usd\":0}}"
            ))
            .is_err());
        }
    }

    #[test]
    fn concurrent_admissions_cannot_overspend_either_dimension_or_slots() {
        for (limit, request) in [
            (
                Envelope {
                    work: Units {
                        tokens: 100,
                        cost_micro_usd: 1000,
                    },
                    ..envelope()
                },
                units(30),
            ),
            (
                Envelope {
                    work: Units {
                        tokens: 1000,
                        cost_micro_usd: 100,
                    },
                    ..envelope()
                },
                units(30),
            ),
            (
                Envelope {
                    work: units(1000),
                    max_active_inferences: 3,
                    ..envelope()
                },
                units(0),
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let store =
                std::sync::Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
            let id = campaign(&store, "race");
            store
                .host_authorize_campaign_envelope("grant", &id, limit)
                .unwrap();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
            let handles: Vec<_> = (0..12)
                .map(|i| {
                    let (store, barrier, id) = (store.clone(), barrier.clone(), id.clone());
                    std::thread::spawn(move || {
                        barrier.wait();
                        store
                            .campaign_ledger_command(
                                &format!("admit-{i}"),
                                &id,
                                LedgerCommand::Reserve {
                                    reservation_id: format!("attempt-{i}"),
                                    pool: Pool::Work,
                                    reserved: request,
                                },
                            )
                            .is_ok()
                    })
                })
                .collect();
            assert_eq!(
                handles
                    .into_iter()
                    .filter_map(|h| h.join().unwrap().then_some(()))
                    .count(),
                3
            );
            assert_eq!(
                store
                    .campaign_ledger(&id)
                    .unwrap()
                    .unwrap()
                    .active_inferences(),
                3
            );
        }
    }

    #[test]
    fn protected_pool_retries_and_confirmed_unspent() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "pools");
        store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap();
        store
            .campaign_ledger_command("work", &id, reserve("work", Pool::Work, 100))
            .unwrap();
        assert!(store
            .campaign_ledger_command("retry", &id, reserve("retry", Pool::Work, 1))
            .is_err());
        store
            .campaign_ledger_command("verify", &id, reserve("verify", Pool::Verification, 20))
            .unwrap();
        assert!(store
            .campaign_ledger_command("verify-extra", &id, reserve("extra", Pool::Verification, 1))
            .is_err());
        let cancelled = store
            .campaign_ledger_command(
                "cancel",
                &id,
                LedgerCommand::Cancel {
                    reservation_id: "work".into(),
                },
            )
            .unwrap();
        assert_eq!(cancelled.committed(Pool::Work).unwrap().tokens, 100);
        assert_eq!(cancelled.active_inferences(), 2);
        store
            .campaign_ledger_command("unspent", &id, reconcile("work", Usage::Final(units(0))))
            .unwrap();
        // A rejected command has no receipt; admission can be retried after release.
        let ledger = store
            .campaign_ledger_command("retry", &id, reserve("retry", Pool::Work, 100))
            .unwrap();
        assert_eq!(ledger.reservations.len(), 3);
        assert_eq!(ledger.active_inferences(), 2);
        assert!(store
            .host_authorize_campaign_envelope("child-grant", &id, envelope())
            .is_err());
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignGet { id })
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(campaign.status, CampaignStatus::Draft);
        assert!(store.list_tasks().unwrap().is_empty());
        assert!(store.scheduled_tasks().unwrap().is_empty());
    }

    #[test]
    fn unresolved_reopen_replay_conflicts_and_overrun_debt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(&store, "reopen");
        let grant = store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap();
        let admitted = store
            .campaign_ledger_command("reserve", &id, reserve("attempt", Pool::Work, 50))
            .unwrap();
        store
            .campaign_ledger_command("unknown", &id, reconcile("attempt", Usage::Unknown))
            .unwrap();
        store
            .campaign_ledger_command(
                "provisional",
                &id,
                reconcile("attempt", Usage::Provisional(units(30))),
            )
            .unwrap();
        let unresolved = store
            .campaign_ledger_command(
                "cancel",
                &id,
                LedgerCommand::Cancel {
                    reservation_id: "attempt".into(),
                },
            )
            .unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), unresolved);
        assert_eq!(unresolved.committed(Pool::Work).unwrap().tokens, 50);
        assert_eq!(unresolved.active_inferences(), 1);
        assert_eq!(
            store
                .host_authorize_campaign_envelope("grant", &id, envelope())
                .unwrap(),
            grant
        );
        assert_eq!(
            store
                .campaign_ledger_command("reserve", &id, reserve("attempt", Pool::Work, 50))
                .unwrap(),
            admitted
        );
        assert!(store
            .campaign_ledger_command("reserve", &id, reserve("attempt", Pool::Work, 51))
            .unwrap_err()
            .contains("conflict"));
        assert!(store
            .campaign_ledger_command("reserve", &id, reconcile("attempt", Usage::Unknown))
            .unwrap_err()
            .contains("conflict"));
        assert!(store
            .campaign_ledger_command("new-reserve", &id, reserve("attempt", Pool::Work, 50))
            .is_err());
        assert!(store
            .campaign_ledger_command("discard", &id, reconcile("attempt", Usage::Unknown))
            .is_err());
        assert!(store
            .campaign_ledger_command(
                "false-unspent",
                &id,
                reconcile("attempt", Usage::Final(units(0)))
            )
            .is_err());
        let debt = store
            .campaign_ledger_command(
                "overrun",
                &id,
                reconcile("attempt", Usage::Provisional(units(130))),
            )
            .unwrap();
        assert_eq!(
            debt.debt,
            Totals {
                tokens: 80,
                cost_micro_usd: 80
            }
        );
        assert!(debt.admissions_paused);
        assert_eq!(debt.active_inferences(), 1);
        assert!(store
            .campaign_ledger_command("paused", &id, reserve("verify", Pool::Verification, 1))
            .is_err());
        let final_usage = reconcile("attempt", Usage::Final(units(140)));
        let settled = store
            .campaign_ledger_command("final", &id, final_usage.clone())
            .unwrap();
        assert_eq!(settled.debt.tokens, 90);
        assert_eq!(settled.committed(Pool::Work).unwrap().tokens, 140);
        assert_eq!(settled.active_inferences(), 0);
        assert_eq!(
            store
                .campaign_ledger_command("final", &id, final_usage.clone())
                .unwrap(),
            settled
        );
        assert_eq!(
            store
                .campaign_ledger_command("final-again", &id, final_usage)
                .unwrap(),
            settled
        );
        assert!(store
            .campaign_ledger_command("final", &id, reconcile("attempt", Usage::Final(units(141))))
            .unwrap_err()
            .contains("conflict"));
        assert!(store
            .campaign_ledger_command("amend", &id, reconcile("attempt", Usage::Final(units(141))))
            .is_err());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), settled);
    }

    #[test]
    fn unknown_campaign_authorization_conflicts_and_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert!(store
            .host_authorize_campaign_envelope("missing", "campaign-missing", envelope())
            .unwrap_err()
            .contains("unknown campaign"));
        assert!(store
            .campaign_ledger_command("missing", "campaign-missing", reserve("a", Pool::Work, 1))
            .is_err());
        let id = campaign(&store, "overflow");
        assert!(store
            .campaign_ledger_command("no-grant", &id, reserve("a", Pool::Work, 1))
            .unwrap_err()
            .contains("not authorized"));
        for limit in [
            Envelope {
                work: Units {
                    tokens: u64::MAX,
                    cost_micro_usd: 0,
                },
                ..envelope()
            },
            Envelope {
                work: Units {
                    tokens: 0,
                    cost_micro_usd: u64::MAX,
                },
                ..envelope()
            },
        ] {
            assert!(store
                .host_authorize_campaign_envelope("overflow", &id, limit)
                .unwrap_err()
                .contains("overflow"));
            assert!(store.campaign_ledger(&id).unwrap().is_none());
        }
        let limit = Envelope {
            work: units(u64::MAX),
            verification: units(0),
            max_active_inferences: 3,
        };
        store
            .host_authorize_campaign_envelope("grant", &id, limit.clone())
            .unwrap();
        assert!(store
            .host_authorize_campaign_envelope("grant", &id, envelope())
            .unwrap_err()
            .contains("conflict"));
        let other = campaign(&store, "other");
        assert!(store
            .host_authorize_campaign_envelope("grant", &other, limit)
            .unwrap_err()
            .contains("conflict"));
        store
            .campaign_ledger_command("a", &id, reserve("a", Pool::Work, u64::MAX - 1))
            .unwrap();
        assert!(store
            .campaign_ledger_command("too-much", &id, reserve("b", Pool::Work, 2))
            .is_err());
        store
            .campaign_ledger_command("b", &id, reserve("b", Pool::Work, 1))
            .unwrap();
        store
            .campaign_ledger_command("c", &id, reserve("c", Pool::Work, 0))
            .unwrap();
        store
            .campaign_ledger_command(
                "actual-a",
                &id,
                reconcile("a", Usage::Final(units(u64::MAX))),
            )
            .unwrap();
        let ledger = store
            .campaign_ledger_command(
                "actual-b",
                &id,
                reconcile("b", Usage::Final(units(u64::MAX))),
            )
            .unwrap();
        assert_eq!(
            ledger.committed(Pool::Work).unwrap().tokens,
            u128::from(u64::MAX) * 2
        );
        assert_eq!(ledger.debt.tokens, u128::from(u64::MAX));
        assert!(ledger.admissions_paused);
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), ledger);
        let ledger = store
            .campaign_ledger_command(
                "actual-c",
                &id,
                reconcile("c", Usage::Final(units(u64::MAX))),
            )
            .unwrap();
        assert_eq!(ledger.debt.tokens, u128::from(u64::MAX) * 2);
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap().unwrap(), ledger);
        assert_eq!(
            store
                .campaign_ledger_command(
                    "actual-c",
                    &id,
                    reconcile("c", Usage::Final(units(u64::MAX)))
                )
                .unwrap(),
            ledger
        );
    }
}

fn err(error: impl std::fmt::Display) -> String {
    format!("campaign ledger: {error}")
}

/// Exact aggregate inference tokens and millionths of one US dollar. No floats,
/// currency conversion, or implicit pricing; callers must supply bounded estimates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Units {
    pub tokens: u64,
    pub cost_micro_usd: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Pool {
    Work,
    Verification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Envelope {
    pub work: Units,
    /// Protected, additional allowance: work cannot borrow it (or vice versa).
    pub verification: Units,
    /// Shared across both pools; every unresolved reservation holds one slot.
    pub max_active_inferences: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Usage {
    Unknown,
    /// Cumulative lower bound, not a delta. Keeps the reservation unresolved.
    Provisional(Units),
    /// Authoritative cumulative usage; releases only the unused reservation.
    Final(Units),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Reservation {
    #[serde(default)]
    pub allocation: Option<String>,
    pub pool: Pool,
    pub reserved: Units,
    pub usage: Usage,
    pub cancellation_requested: bool,
}

impl Reservation {
    fn covered(&self) -> Units {
        match self.usage {
            Usage::Final(actual) => Units {
                tokens: actual.tokens.min(self.reserved.tokens),
                cost_micro_usd: actual.cost_micro_usd.min(self.reserved.cost_micro_usd),
            },
            _ => self.reserved,
        }
    }
}

/// Totals are wider than individual grants/reports so an overrun beyond u64::MAX
/// remains recordable. All accumulation is checked, never wrapped or saturated.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Totals {
    pub tokens: u128,
    pub cost_micro_usd: u128,
}

impl Totals {
    fn add(&mut self, units: Units) -> Result<(), String> {
        self.tokens = self
            .tokens
            .checked_add(u128::from(units.tokens))
            .ok_or_else(|| err("token overflow"))?;
        self.cost_micro_usd = self
            .cost_micro_usd
            .checked_add(u128::from(units.cost_micro_usd))
            .ok_or_else(|| err("cost overflow"))?;
        Ok(())
    }

    fn fits(self, limit: Units) -> bool {
        self.tokens <= u128::from(limit.tokens)
            && self.cost_micro_usd <= u128::from(limit.cost_micro_usd)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    pub schema_version: u32,
    #[serde(default)]
    pub revision: u64,
    pub campaign_id: String,
    pub envelope: Envelope,
    pub reservations: BTreeMap<String, Reservation>,
    /// Converted admission holds. False = open, true = closed. No new grant.
    #[serde(default)]
    pub allocations: BTreeMap<String, bool>,
    /// Original holds remain immutable; only explicit host transfers change allowance.
    #[serde(default)]
    pub transfers: BTreeMap<String, AllocationTransfers>,
    /// Sum of per-reservation reported usage above its estimate, not forgiven by
    /// other unused holds. This is accounting debt, not a new spending allowance.
    pub debt: Totals,
    /// Sticky accounting pause after any overrun. No resume/grant extension yet.
    pub admissions_paused: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AllocationTransfers {
    pub received: Units,
    pub sent: Units,
}

impl Ledger {
    pub(crate) fn allocation_allowance(&self, id: &str) -> Result<Units, String> {
        let original = self
            .reservations
            .get(id)
            .ok_or_else(|| err("unknown allocation"))?
            .reserved;
        let net = self.transfers.get(id).cloned().unwrap_or_default();
        let adjusted = |original: u64, received: u64, sent: u64| {
            u128::from(original)
                .checked_add(u128::from(received))
                .and_then(|n| n.checked_sub(u128::from(sent)))
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| err("allocation transfer overflow"))
        };
        Ok(Units {
            tokens: adjusted(original.tokens, net.received.tokens, net.sent.tokens)?,
            cost_micro_usd: adjusted(
                original.cost_micro_usd,
                net.received.cost_micro_usd,
                net.sent.cost_micro_usd,
            )?,
        })
    }

    pub(crate) fn allocation_available(&self, id: &str) -> Result<Units, String> {
        let closed = self
            .allocations
            .get(id)
            .ok_or_else(|| err("unknown allocation"))?;
        let parent = self
            .reservations
            .get(id)
            .ok_or_else(|| err("missing allocation hold"))?;
        if parent.allocation.is_some()
            || parent.usage != Usage::Unknown
            || parent.cancellation_requested
        {
            return Err(err("invalid allocation hold"));
        }
        let allowance = self.allocation_allowance(id)?;
        let mut covered = Totals::default();
        for child in self
            .reservations
            .values()
            .filter(|r| r.allocation.as_deref() == Some(id))
        {
            if child.pool != parent.pool || (*closed && !matches!(child.usage, Usage::Final(_))) {
                return Err(err("allocation pool or closure violation"));
            }
            covered.add(child.covered())?;
        }
        if !covered.fits(allowance) {
            return Err(err("allocation conservation violation"));
        }
        Ok(if *closed {
            Units::default()
        } else {
            Units {
                tokens: allowance.tokens - covered.tokens as u64,
                cost_micro_usd: allowance.cost_micro_usd - covered.cost_micro_usd as u64,
            }
        })
    }

    fn validate(&self, campaign_id: &str) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(err("unsupported ledger schema"));
        }
        if self.campaign_id != campaign_id {
            return Err(err("ledger campaign identity mismatch"));
        }
        let mut envelope = Totals::default();
        envelope.add(self.envelope.work)?;
        envelope.add(self.envelope.verification)?;
        if !envelope.fits(Units {
            tokens: u64::MAX,
            cost_micro_usd: u64::MAX,
        }) {
            return Err(err("envelope overflow"));
        }
        if self.active_inferences() as u128 > u128::from(self.envelope.max_active_inferences) {
            return Err(err("persisted active inference limit exceeded"));
        }
        let mut debt = Totals::default();
        for id in self.allocations.keys() {
            identifier(id)?;
            self.allocation_available(id)?;
        }
        for (pool, limit) in [
            (Pool::Work, self.envelope.work),
            (Pool::Verification, self.envelope.verification),
        ] {
            let mut received = Totals::default();
            let mut sent = Totals::default();
            for (id, net) in &self.transfers {
                if !self.allocations.contains_key(id) {
                    return Err(err("transfer without allocation"));
                }
                if self.reservations[id].pool == pool {
                    received.add(net.received)?;
                    sent.add(net.sent)?;
                    let allowance = self.allocation_allowance(id)?;
                    if !(Totals {
                        tokens: allowance.tokens.into(),
                        cost_micro_usd: allowance.cost_micro_usd.into(),
                    })
                    .fits(limit)
                    {
                        return Err(err("allocation exceeds pool"));
                    }
                }
            }
            if received != sent {
                return Err(err("transfer conservation violation"));
            }
            let mut covered = Totals::default();
            for (id, reservation) in self.reservations.iter().filter(|(_, r)| r.pool == pool) {
                identifier(id)?;
                if let Some(parent) = &reservation.allocation {
                    if !self.allocations.contains_key(parent) {
                        return Err(err("missing funding allocation"));
                    }
                }
                if !(Totals {
                    tokens: u128::from(reservation.reserved.tokens),
                    cost_micro_usd: u128::from(reservation.reserved.cost_micro_usd),
                })
                .fits(limit)
                {
                    return Err(err("reservation exceeds pool allowance"));
                }
                if self.allocations.contains_key(id) {
                    covered.add(self.allocation_available(id)?)?;
                    continue;
                }
                // Exclude reported overruns, not outstanding holds, from the
                // allowance check. Historical estimates can be reused after final release.
                covered.add(match reservation.usage {
                    Usage::Final(actual) => Units {
                        tokens: actual.tokens.min(reservation.reserved.tokens),
                        cost_micro_usd: actual
                            .cost_micro_usd
                            .min(reservation.reserved.cost_micro_usd),
                    },
                    _ => reservation.reserved,
                })?;
                if let Usage::Provisional(actual) | Usage::Final(actual) = reservation.usage {
                    debt.add(Units {
                        tokens: actual.tokens.saturating_sub(reservation.reserved.tokens),
                        cost_micro_usd: actual
                            .cost_micro_usd
                            .saturating_sub(reservation.reserved.cost_micro_usd),
                    })?;
                }
            }
            if !covered.fits(limit) {
                return Err(err("persisted pool conservation violation"));
            }
            self.committed(pool)?;
        }
        if self.debt != debt || (debt != Totals::default() && !self.admissions_paused) {
            return Err(err("persisted debt/pause mismatch"));
        }
        Ok(())
    }

    /// Final usage consumes actuals; unresolved usage holds max(reserved, known)
    /// componentwise. Unknown/cancellation never imply zero spend.
    pub(crate) fn committed(&self, pool: Pool) -> Result<Totals, String> {
        let mut total = Totals::default();
        for (id, reservation) in self.reservations.iter().filter(|(_, r)| r.pool == pool) {
            if self.allocations.contains_key(id) {
                total.add(self.allocation_available(id)?)?;
                continue;
            }
            let units = match reservation.usage {
                Usage::Final(actual) => actual,
                Usage::Provisional(actual) => Units {
                    tokens: actual.tokens.max(reservation.reserved.tokens),
                    cost_micro_usd: actual
                        .cost_micro_usd
                        .max(reservation.reserved.cost_micro_usd),
                },
                Usage::Unknown => reservation.reserved,
            };
            total.add(units)?;
        }
        Ok(total)
    }

    pub(crate) fn active_inferences(&self) -> usize {
        self.reservations
            .iter()
            .filter(|(id, r)| {
                !self.allocations.contains_key(*id) && !matches!(r.usage, Usage::Final(_))
            })
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LedgerCommand {
    TransferAvailable {
        source_allocation_id: String,
        target_allocation_id: String,
        amounts: Units,
        expected_revision: u64,
    },
    FundAllocation {
        reservation_id: String,
        work_id: String,
    },
    CloseAllocation {
        reservation_id: String,
    },
    ReserveAllocated {
        reservation_id: String,
        allocation_id: String,
        pool: Pool,
        reserved: Units,
    },
    /// One inference attempt. A retry uses a new reservation ID on the SAME
    /// campaign; replaying admission uses the original command and reservation ID.
    Reserve {
        reservation_id: String,
        pool: Pool,
        reserved: Units,
    },
    Reconcile {
        reservation_id: String,
        usage: Usage,
    },
    /// Intent only. Confirmed unspent is explicitly reconciled with Final(zero).
    Cancel {
        reservation_id: String,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Action {
    HostAuthorize(Envelope),
    Command(LedgerCommand),
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    schema_version: u32,
    campaign_id: String,
    action: Action,
    result: Ledger,
}

fn decode(bytes: &[u8], campaign_id: &str) -> Result<Ledger, String> {
    let ledger: Ledger = serde_json::from_slice(bytes).map_err(err)?;
    ledger.validate(campaign_id)?;
    Ok(ledger)
}

fn identifier(value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > 256 {
        return Err(err(
            "command/reservation ID must be nonblank and at most 256 bytes",
        ));
    }
    Ok(())
}

impl RuntimeStore {
    /// TRUST BOUNDARY: only a host-authorized caller may invoke this method.
    /// This is not a permission check or an API budget grant. No worker/model/IPC
    /// route calls it. An envelope cannot be replaced or topped up; allocations
    /// only constrain existing funds.
    pub(crate) fn host_authorize_campaign_envelope(
        &self,
        command_id: &str,
        campaign_id: &str,
        envelope: Envelope,
    ) -> Result<Ledger, String> {
        self.mutate_campaign_ledger(command_id, campaign_id, Action::HostAuthorize(envelope))
    }

    pub(crate) fn campaign_ledger_command(
        &self,
        command_id: &str,
        campaign_id: &str,
        command: LedgerCommand,
    ) -> Result<Ledger, String> {
        self.mutate_campaign_ledger(command_id, campaign_id, Action::Command(command))
    }

    pub(crate) fn campaign_ledger(&self, campaign_id: &str) -> Result<Option<Ledger>, String> {
        let read = self.database.begin_read().map_err(err)?;
        let roots = read.open_table(ROOTS).map_err(err)?;
        roots
            .get(campaign_id)
            .map_err(err)?
            .map(|value| decode(value.value(), campaign_id))
            .transpose()
    }

    pub(super) fn campaign_ledger_in(
        write: &WriteTransaction,
        campaign_id: &str,
    ) -> Result<Ledger, String> {
        let roots = write.open_table(ROOTS).map_err(err)?;
        let value = roots
            .get(campaign_id)
            .map_err(err)?
            .ok_or_else(|| err("campaign envelope not authorized"))?;
        decode(value.value(), campaign_id)
    }

    fn mutate_campaign_ledger(
        &self,
        command_id: &str,
        campaign_id: &str,
        action: Action,
    ) -> Result<Ledger, String> {
        // The serialized writer covers existence, replay, admission, accounting,
        // and receipt. No read/check/write gap can admit concurrent overspending.
        let write = self.database.begin_write().map_err(err)?;
        let ledger = Self::mutate_campaign_ledger_in(&write, command_id, campaign_id, action)?;
        write.commit().map_err(err)?;
        Ok(ledger)
    }

    pub(super) fn campaign_ledger_command_in(
        write: &WriteTransaction,
        command_id: &str,
        campaign_id: &str,
        command: LedgerCommand,
    ) -> Result<Ledger, String> {
        Self::mutate_campaign_ledger_in(write, command_id, campaign_id, Action::Command(command))
    }

    fn mutate_campaign_ledger_in(
        write: &WriteTransaction,
        command_id: &str,
        campaign_id: &str,
        action: Action,
    ) -> Result<Ledger, String> {
        identifier(command_id)?;
        let mut receipts = write.open_table(RECEIPTS).map_err(err)?;
        if let Some(value) = receipts.get(command_id).map_err(err)? {
            let receipt: Receipt = serde_json::from_slice(value.value()).map_err(err)?;
            if receipt.schema_version != 1 {
                return Err(err("unsupported receipt schema"));
            }
            if receipt.campaign_id != campaign_id || receipt.action != action {
                return Err(err("command_id payload conflict"));
            }
            // Return the original result, not the campaign's newer snapshot.
            receipt.result.validate(campaign_id)?;
            return Ok(receipt.result);
        }
        if write
            .open_table(CAMPAIGNS)
            .map_err(err)?
            .get(campaign_id)
            .map_err(err)?
            .is_none()
        {
            return Err(err("unknown campaign"));
        }
        let mut roots = write.open_table(ROOTS).map_err(err)?;
        let existing = roots
            .get(campaign_id)
            .map_err(err)?
            .map(|v| decode(v.value(), campaign_id))
            .transpose()?;
        let mut ledger = match &action {
            Action::HostAuthorize(envelope) => {
                if existing.is_some() {
                    return Err(err("campaign envelope already authorized"));
                }
                envelope
                    .work
                    .tokens
                    .checked_add(envelope.verification.tokens)
                    .ok_or_else(|| err("envelope token overflow"))?;
                envelope
                    .work
                    .cost_micro_usd
                    .checked_add(envelope.verification.cost_micro_usd)
                    .ok_or_else(|| err("envelope cost overflow"))?;
                Ledger {
                    schema_version: 1,
                    revision: 0,
                    campaign_id: campaign_id.into(),
                    envelope: envelope.clone(),
                    reservations: BTreeMap::new(),
                    allocations: BTreeMap::new(),
                    transfers: BTreeMap::new(),
                    debt: Totals::default(),
                    admissions_paused: false,
                }
            }
            Action::Command(_) => {
                existing.ok_or_else(|| err("campaign envelope not authorized"))?
            }
        };
        let before = ledger.clone();
        if let Action::Command(command) = &action {
            match command {
                LedgerCommand::TransferAvailable {
                    source_allocation_id: source,
                    target_allocation_id: target,
                    amounts,
                    expected_revision,
                } => {
                    if ledger.revision != *expected_revision
                        || ledger.admissions_paused
                        || ledger.debt != Totals::default()
                    {
                        return Err(err("stale or debt-paused transfer"));
                    }
                    if source == target
                        || *amounts == Units::default()
                        || ledger.allocations.get(source) != Some(&false)
                        || ledger.allocations.get(target) != Some(&false)
                        || ledger.reservations[source].pool != ledger.reservations[target].pool
                    {
                        return Err(err("invalid transfer allocations"));
                    }
                    let available = ledger.allocation_available(source)?;
                    ledger.allocation_available(target)?;
                    if amounts.tokens > available.tokens
                        || amounts.cost_micro_usd > available.cost_micro_usd
                    {
                        return Err(err("insufficient transferable allowance"));
                    }
                    for (id, incoming) in [(source, false), (target, true)] {
                        let net = ledger.transfers.entry(id.clone()).or_default();
                        let value = if incoming {
                            &mut net.received
                        } else {
                            &mut net.sent
                        };
                        value.tokens = value
                            .tokens
                            .checked_add(amounts.tokens)
                            .ok_or_else(|| err("transfer token overflow"))?;
                        value.cost_micro_usd = value
                            .cost_micro_usd
                            .checked_add(amounts.cost_micro_usd)
                            .ok_or_else(|| err("transfer cost overflow"))?;
                    }
                }
                LedgerCommand::FundAllocation {
                    reservation_id,
                    work_id,
                } => {
                    if ledger.admissions_paused {
                        return Err(err("campaign admissions paused by debt"));
                    }
                    let work = Self::admitted_work_in(write, work_id)?;
                    Self::group_funding_in(write, &work)?;
                    if work.dispatch_id != *reservation_id
                        || work.admission.campaign_id != campaign_id
                    {
                        return Err(err("allocation admission scope mismatch"));
                    }
                    let parent = ledger
                        .reservations
                        .get(reservation_id)
                        .ok_or_else(|| err("unknown admission hold"))?;
                    if !matches!(
                        work.state,
                        super::admission::DispatchState::Registered { .. }
                    ) || parent.pool != work.admission.pool
                        || parent.reserved != work.admission.upper_bound
                        || parent.allocation.is_some()
                        || parent.usage != Usage::Unknown
                        || parent.cancellation_requested
                    {
                        return Err(err("admission hold cannot be transferred"));
                    }
                    ledger
                        .allocations
                        .entry(reservation_id.clone())
                        .or_insert(false);
                }
                LedgerCommand::CloseAllocation { reservation_id } => {
                    if ledger.reservations.values().any(|r| {
                        r.allocation.as_deref() == Some(reservation_id)
                            && !matches!(r.usage, Usage::Final(_))
                    }) {
                        return Err(err("allocation has unresolved requests"));
                    }
                    *ledger
                        .allocations
                        .get_mut(reservation_id)
                        .ok_or_else(|| err("unknown allocation"))? = true;
                }
                LedgerCommand::Reserve {
                    reservation_id,
                    pool,
                    reserved,
                }
                | LedgerCommand::ReserveAllocated {
                    reservation_id,
                    pool,
                    reserved,
                    ..
                } => {
                    identifier(reservation_id)?;
                    if ledger.reservations.contains_key(reservation_id) {
                        return Err(err("reservation ID already exists"));
                    }
                    if ledger.admissions_paused {
                        return Err(err("campaign admissions paused by debt"));
                    }
                    if ledger.active_inferences() as u128
                        >= u128::from(ledger.envelope.max_active_inferences)
                    {
                        return Err(err("active inference limit"));
                    }
                    let allocation =
                        if let LedgerCommand::ReserveAllocated { allocation_id, .. } = command {
                            if ledger.allocations.get(allocation_id) != Some(&false)
                                || ledger.reservations.get(allocation_id).map(|r| r.pool)
                                    != Some(*pool)
                            {
                                return Err(err("closed or mismatched allocation"));
                            }
                            Some(allocation_id.clone())
                        } else {
                            None
                        };
                    let mut committed = if allocation.is_some() {
                        Totals::default()
                    } else {
                        ledger.committed(*pool)?
                    };
                    committed.add(*reserved)?;
                    let limit = if let Some(id) = &allocation {
                        ledger.allocation_available(id)?
                    } else {
                        match pool {
                            Pool::Work => ledger.envelope.work,
                            Pool::Verification => ledger.envelope.verification,
                        }
                    };
                    if !committed.fits(limit) {
                        return Err(err("insufficient campaign allowance"));
                    }
                    ledger.reservations.insert(
                        reservation_id.clone(),
                        Reservation {
                            allocation,
                            pool: *pool,
                            reserved: *reserved,
                            usage: Usage::Unknown,
                            cancellation_requested: false,
                        },
                    );
                }
                LedgerCommand::Cancel { reservation_id } => {
                    if ledger.allocations.contains_key(reservation_id) {
                        return Err(err(
                            "use CloseAllocation, not cancellation of transferred hold",
                        ));
                    }
                    let reservation = ledger
                        .reservations
                        .get_mut(reservation_id)
                        .ok_or_else(|| err("unknown reservation"))?;
                    reservation.cancellation_requested = true;
                }
                LedgerCommand::Reconcile {
                    reservation_id,
                    usage,
                } => {
                    if ledger.allocations.contains_key(reservation_id) {
                        return Err(err("transferred hold cannot be reconciled independently"));
                    }
                    let reservation = ledger
                        .reservations
                        .get_mut(reservation_id)
                        .ok_or_else(|| err("unknown reservation"))?;
                    if let Usage::Final(previous) = reservation.usage {
                        if *usage != Usage::Final(previous) {
                            return Err(err("final usage is immutable"));
                        }
                    }
                    if let Usage::Provisional(previous) = reservation.usage {
                        match usage {
                            Usage::Unknown => return Err(err("cannot discard known usage")),
                            Usage::Provisional(actual) | Usage::Final(actual)
                                if actual.tokens < previous.tokens
                                    || actual.cost_micro_usd < previous.cost_micro_usd =>
                            {
                                return Err(err("cumulative usage cannot decrease"));
                            }
                            _ => {}
                        }
                    }
                    reservation.usage = usage.clone();
                    let mut debt = Totals::default();
                    for reservation in ledger.reservations.values() {
                        if let Usage::Provisional(actual) | Usage::Final(actual) = reservation.usage
                        {
                            debt.add(Units {
                                tokens: actual.tokens.saturating_sub(reservation.reserved.tokens),
                                cost_micro_usd: actual
                                    .cost_micro_usd
                                    .saturating_sub(reservation.reserved.cost_micro_usd),
                            })?;
                        }
                    }
                    ledger.debt = debt;
                    ledger.admissions_paused |= debt != Totals::default();
                }
            }
        }
        if ledger != before {
            ledger.revision = ledger
                .revision
                .checked_add(1)
                .ok_or_else(|| err("ledger revision overflow"))?;
        }
        ledger.validate(campaign_id)?;
        roots
            .insert(
                campaign_id,
                serde_json::to_vec(&ledger).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        let receipt = Receipt {
            schema_version: 1,
            campaign_id: campaign_id.into(),
            action,
            result: ledger.clone(),
        };
        receipts
            .insert(
                command_id,
                serde_json::to_vec(&receipt).map_err(err)?.as_slice(),
            )
            .map_err(err)?;
        drop(roots);
        drop(receipts);
        Ok(ledger)
    }
}
