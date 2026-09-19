//! Bounded host-internal scheduling, not an agent, grant, or IPC authority.
#![allow(dead_code)]

use super::{
    admission::{Admission, AdmittedWork, DispatchState, PENDING, WORK},
    campaign_ledger::{LedgerCommand, Units, Usage},
    RuntimeStore,
};
use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const ROOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_work_limits");

fn err(e: impl std::fmt::Display) -> String {
    format!("campaign groups: {e}")
}
fn now_ms() -> Result<u64, String> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(err)?
        .as_millis()
        .try_into()
        .map_err(err)
}
pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(ROOTS).map_err(err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{
        admission::DispatchOutcome,
        campaign_ledger::{Envelope, Pool},
    };
    use super::*;
    use std::sync::{Arc, Barrier};
    use tachyon_api::types::{ApiRequest, ApiResponse};

    fn campaign(store: &RuntimeStore, key: &str, limits: WorkLimits) -> String {
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
        store
            .host_authorize_campaign_envelope(
                &format!("grant-{key}"),
                &campaign.id,
                Envelope {
                    work: Units {
                        tokens: 100,
                        cost_micro_usd: 100,
                    },
                    verification: Units {
                        tokens: 100,
                        cost_micro_usd: 100,
                    },
                    max_active_inferences: 100,
                },
            )
            .unwrap();
        store
            .host_configure_work_limits(&campaign.id, limits)
            .unwrap();
        campaign.id
    }
    fn spec(campaign: &str, id: &str, count: usize, cap: usize, parent: Option<&str>) -> GroupSpec {
        GroupSpec {
            group_id: id.into(),
            campaign_id: campaign.into(),
            parent: parent.map(str::to_owned),
            max_running: cap,
            work: (0..count)
                .map(|i| Admission {
                    work_id: format!("{id}-{i:02}"),
                    campaign_id: campaign.into(),
                    objective: "bounded work".into(),
                    instruction_revision: 1,
                    generation: 1,
                    pool: Pool::Work,
                    upper_bound: Units {
                        tokens: 1,
                        cost_micro_usd: 1,
                    },
                })
                .collect(),
        }
    }

    #[test]
    fn parent_cap_one_handoff_restart_revision_and_ancestor_resume() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let id = campaign(
            &store,
            "parent-wait",
            WorkLimits {
                max_running: 1,
                ..WorkLimits::default()
            },
        );
        store
            .create_campaign_group(spec(&id, "parent", 1, 1, None))
            .unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "host-parent".into(),
            })
            .unwrap();
        let parent = store.admitted_work(&id, "parent-00").unwrap();
        store
            .campaign_ledger_command(
                "fund-parent",
                &id,
                LedgerCommand::FundAllocation {
                    reservation_id: parent.dispatch_id.clone(),
                    work_id: parent.admission.work_id.clone(),
                },
            )
            .unwrap();
        store
            .create_campaign_group(spec(&id, "child", 2, 1, Some("parent")))
            .unwrap();
        assert_eq!(
            store
                .dispatch_campaign_batch(2, |_| panic!("parent owns slot"))
                .unwrap(),
            0
        );
        let wait = store
            .host_suspend_parent(
                &parent,
                0,
                vec!["child-00".into(), "child-01".into()],
                WaitMode::Any,
                now_ms().unwrap() + 10000,
            )
            .unwrap();
        assert!(store
            .host_suspend_parent(
                &parent,
                0,
                wait.work_ids.clone(),
                WaitMode::All,
                wait.deadline_ms
            )
            .is_err());
        {
            let tx = store.database.begin_write().unwrap();
            assert!(RuntimeStore::admitted_funding_in(&tx, &parent).is_err());
        }
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let status = store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .unwrap();
        assert!(!status.ready && !status.resumed);
        assert_eq!(status.resident_capacity, Some(16));
        assert_eq!(
            store
                .dispatch_campaign_batch(2, |_| DispatchOutcome::Unknown)
                .unwrap(),
            1
        );
        let child = store.admitted_work(&id, "child-00").unwrap();
        store.host_acknowledge_work_terminal(&child).unwrap();
        // A paused ancestor prevents resume, even with a completed dependency.
        store.resize_campaign_group(&id, "parent", 1, 0).unwrap();
        let blocked = store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .unwrap();
        assert!(blocked.ready && blocked.resource_blocked && !blocked.resumed);
        store.resize_campaign_group(&id, "parent", 2, 1).unwrap();
        let resumed = store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .unwrap();
        assert!(resumed.resumed);
        assert_eq!(resumed.completed, vec!["child-00"]);
        assert_eq!(resumed.outstanding, vec!["child-01"]);
        assert!(store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .is_err());
        assert_eq!(
            store
                .dispatch_campaign_batch(1, |_| panic!("resumed parent owns root"))
                .unwrap(),
            0
        );
        let next = store
            .host_suspend_parent(
                &parent,
                wait.revision,
                vec!["child-01".into()],
                WaitMode::Count(1),
                now_ms().unwrap() + 1000,
            )
            .unwrap();
        assert!(next.revision > wait.revision);
        assert!(store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .is_err());
        std::thread::sleep(std::time::Duration::from_millis(1010));
        let expired = store
            .host_poll_parent_wait(&parent, next.revision, true)
            .unwrap();
        assert!(expired.ready && expired.resumed);
        assert!(expired.completed.is_empty());
        assert_eq!(expired.outstanding, vec!["child-01"]);
        store.cancel_campaign_group(&id, "parent", 3).unwrap();
        assert!(store
            .host_poll_parent_wait(&parent, next.revision, true)
            .is_err());
    }

    #[test]
    fn allocation_fences_drain_steering_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(
            &store,
            "allocation",
            WorkLimits {
                total_work: 10,
                max_depth: 1,
                max_running: 3,
                max_resident: 3,
            },
        );
        store
            .create_campaign_group(spec(&c, "g", 3, 3, None))
            .unwrap();
        let first = store.claim_campaign_work().unwrap().unwrap();
        let second = store.claim_campaign_work().unwrap().unwrap();
        store.select_allocation_owner(&c, "owner").unwrap();
        let groups = vec![(
            "g".into(),
            vec!["g-00".into(), "g-01".into(), "g-02".into()],
        )];
        let ledger = store.campaign_ledger(&c).unwrap();
        store
            .allocation_tick(&c, "owner", 1, 1, &groups, &[])
            .unwrap();
        let (group, work, active) = store.campaign_group_status(&c, "g").unwrap();
        assert_eq!((group.max_running, active, work.len()), (1, 2, 3));
        assert!(
            store.claim_campaign_work().unwrap().is_none(),
            "shrink drains unknown active work"
        );
        assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
        let tx = store.database.begin_write().unwrap();
        let mut root = load(&tx, &c).unwrap().unwrap();
        let owner = group.policy_controller.as_deref().unwrap();
        assert!(resize_group(
            &mut root,
            "g",
            group.revision - 1,
            2,
            Some((owner, group.policy_epoch))
        )
        .is_err());
        assert!(resize_group(
            &mut root,
            "g",
            group.revision,
            2,
            Some((owner, group.policy_epoch + 1))
        )
        .is_err());
        assert!(resize_group(
            &mut root,
            "g",
            group.revision,
            2,
            Some(("other", group.policy_epoch))
        )
        .is_err());
        RuntimeStore::relinquish_allocation_in(&tx, &c).unwrap();
        tx.commit().unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let disabled = store.campaign_group_status(&c, "g").unwrap().0;
        assert!(disabled.policy_disabled);
        assert_eq!(disabled.policy_epoch, group.policy_epoch + 1);
        store.select_allocation_owner(&c, "new-owner").unwrap();
        assert!(store
            .allocation_tick(&c, "owner", 3, 3, &groups, &[])
            .is_err());
        store
            .allocation_tick(&c, "new-owner", 3, 3, &groups, &[])
            .unwrap();
        assert_eq!(store.campaign_group_status(&c, "g").unwrap().0, disabled);
        store.host_acknowledge_work_terminal(&first).unwrap();
        assert!(store.claim_campaign_work().unwrap().is_none());
        store.host_acknowledge_work_terminal(&second).unwrap();
        assert!(store.claim_campaign_work().unwrap().is_some());
    }

    #[test]
    fn nested_allocation_obeys_ancestor_pause_and_unknown_holds() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let c = campaign(
            &store,
            "nested-allocation",
            WorkLimits {
                total_work: 10,
                max_depth: 2,
                max_running: 3,
                max_resident: 3,
            },
        );
        store
            .create_campaign_group(spec(&c, "parent", 1, 1, None))
            .unwrap();
        store
            .create_campaign_group(spec(&c, "nested", 3, 3, Some("parent")))
            .unwrap();
        store.select_allocation_owner(&c, "owner").unwrap();
        let groups = vec![(
            "nested".into(),
            vec!["nested-00".into(), "nested-01".into(), "nested-02".into()],
        )];
        let ledger = store.campaign_ledger(&c).unwrap();
        store
            .allocation_tick(&c, "owner", 3, 3, &groups, &[])
            .unwrap();
        let group = store.campaign_group_status(&c, "nested").unwrap().0;
        assert_eq!(group.max_running, 1);
        assert_eq!(group.policy_controller.as_deref(), Some("owner:nested"));
        store.resize_campaign_group(&c, "parent", 1, 0).unwrap();
        store
            .allocation_tick(&c, "owner", 3, 3, &groups, &[])
            .unwrap();
        assert_eq!(
            store
                .campaign_group_status(&c, "nested")
                .unwrap()
                .0
                .max_running,
            1
        );
        assert!(store.claim_campaign_work().unwrap().is_none());
        assert_eq!(store.campaign_ledger(&c).unwrap(), ledger);
        assert!(store
            .allocation_tick(&c, "forged", 3, 3, &groups, &[])
            .is_err());
    }

    #[test]
    fn resident_one_denies_wait_without_releasing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(
            &store,
            "resident",
            WorkLimits {
                max_running: 1,
                max_resident: 1,
                ..WorkLimits::default()
            },
        );
        store
            .create_campaign_group(spec(&id, "parent", 1, 1, None))
            .unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "parent".into(),
            })
            .unwrap();
        let parent = store.admitted_work(&id, "parent-00").unwrap();
        store
            .campaign_ledger_command(
                "fund",
                &id,
                LedgerCommand::FundAllocation {
                    reservation_id: parent.dispatch_id.clone(),
                    work_id: parent.admission.work_id.clone(),
                },
            )
            .unwrap();
        store
            .create_campaign_group(spec(&id, "child", 1, 1, None))
            .unwrap();
        assert!(store
            .host_suspend_parent(
                &parent,
                0,
                vec!["child-00".into()],
                WaitMode::All,
                now_ms().unwrap() + 1000
            )
            .is_err());
        let status = store.campaign_work_status(&id, "parent-00").unwrap();
        assert!(status.active && status.wait.is_none() && status.wait_revision == 0);
        assert_eq!(
            store
                .dispatch_campaign_batch(1, |_| panic!("resident full"))
                .unwrap(),
            0
        );
        store.host_acknowledge_work_terminal(&parent).unwrap();
        assert_eq!(
            store
                .dispatch_campaign_batch(1, |_| DispatchOutcome::Unknown)
                .unwrap(),
            1
        );
    }

    #[test]
    fn parent_resume_races_cancellation_without_new_funding() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = campaign(
            &store,
            "wait-cancel",
            WorkLimits {
                max_running: 1,
                ..WorkLimits::default()
            },
        );
        store
            .create_campaign_group(spec(&id, "parent", 1, 1, None))
            .unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "parent".into(),
            })
            .unwrap();
        let parent = store.admitted_work(&id, "parent-00").unwrap();
        store
            .campaign_ledger_command(
                "fund",
                &id,
                LedgerCommand::FundAllocation {
                    reservation_id: parent.dispatch_id.clone(),
                    work_id: "parent-00".into(),
                },
            )
            .unwrap();
        store
            .create_campaign_group(spec(&id, "child", 1, 1, Some("parent")))
            .unwrap();
        let wait = store
            .host_suspend_parent(
                &parent,
                0,
                vec!["child-00".into()],
                WaitMode::All,
                now_ms().unwrap() + 10000,
            )
            .unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::ConfirmedUnspent)
            .unwrap();
        let before = store.campaign_ledger(&id).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let resume = {
            let (store, parent, barrier) = (store.clone(), parent.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                store.host_poll_parent_wait(&parent, wait.revision, true)
            })
        };
        barrier.wait();
        store.cancel_campaign_group(&id, "parent", 1).unwrap();
        let _ = resume.join().unwrap();
        assert!(store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .is_err());
        let tx = store.database.begin_write().unwrap();
        assert!(RuntimeStore::admitted_funding_in(&tx, &parent).is_err());
        drop(tx);
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
    }

    #[test]
    fn parent_unknown_inference_and_debt_deny_handoff_or_new_spend() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(
            &store,
            "parent-debt",
            WorkLimits {
                max_running: 1,
                ..WorkLimits::default()
            },
        );
        store
            .create_campaign_group(spec(&id, "parent", 1, 1, None))
            .unwrap();
        store
            .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
                worker_id: "parent".into(),
            })
            .unwrap();
        let parent = store.admitted_work(&id, "parent-00").unwrap();
        store
            .campaign_ledger_command(
                "fund",
                &id,
                LedgerCommand::FundAllocation {
                    reservation_id: parent.dispatch_id.clone(),
                    work_id: "parent-00".into(),
                },
            )
            .unwrap();
        store
            .create_campaign_group(spec(&id, "child", 1, 1, Some("parent")))
            .unwrap();
        store
            .campaign_ledger_command(
                "inference",
                &id,
                LedgerCommand::ReserveAllocated {
                    reservation_id: "inference".into(),
                    allocation_id: parent.dispatch_id.clone(),
                    pool: Pool::Work,
                    reserved: Units {
                        tokens: 1,
                        cost_micro_usd: 1,
                    },
                },
            )
            .unwrap();
        assert!(store
            .host_suspend_parent(
                &parent,
                0,
                vec!["child-00".into()],
                WaitMode::All,
                now_ms().unwrap() + 10000
            )
            .is_err());
        store
            .campaign_ledger_command(
                "final",
                &id,
                LedgerCommand::Reconcile {
                    reservation_id: "inference".into(),
                    usage: Usage::Final(Units {
                        tokens: 2,
                        cost_micro_usd: 2,
                    }),
                },
            )
            .unwrap();
        let wait = store
            .host_suspend_parent(
                &parent,
                0,
                vec!["child-00".into()],
                WaitMode::All,
                now_ms().unwrap() + 10000,
            )
            .unwrap();
        let before = store.campaign_ledger(&id).unwrap();
        let status = store
            .host_poll_parent_wait(&parent, wait.revision, true)
            .unwrap();
        assert!(status.ready && status.resource_blocked && !status.resumed);
        assert_eq!(
            store
                .dispatch_campaign_batch(1, |_| panic!("debt spend"))
                .unwrap(),
            0
        );
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
    }

    #[test]
    fn reallocate_respects_manual_and_cancelled_ancestor_branches() {
        use tachyon_api::campaign::{
            AllocationAction, AllocationControl, AllocationMode, AllocationSignal,
            CampaignAllocation,
        };
        for branch in ["outer", "source", "target"] {
            for control in ["manual", "pause", "cancel"] {
                let dir = tempfile::tempdir().unwrap();
                let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
                let c = campaign(
                    &store,
                    "transfer-branch",
                    WorkLimits {
                        max_depth: 2,
                        max_running: 8,
                        ..WorkLimits::default()
                    },
                );
                for (id, parent) in [
                    ("outer", None),
                    ("controlled", Some("outer")),
                    ("source", Some("controlled")),
                    ("target", Some("controlled")),
                ] {
                    store
                        .create_campaign_group(spec(&c, id, 1, 8, parent))
                        .unwrap();
                }
                store
                    .dispatch_campaign_batch(8, |_| DispatchOutcome::Registered {
                        worker_id: "worker".into(),
                    })
                    .unwrap();
                for id in ["source-00", "target-00"] {
                    let work = store.admitted_work(&c, id).unwrap();
                    store
                        .campaign_ledger_command(
                            id,
                            &c,
                            LedgerCommand::FundAllocation {
                                reservation_id: work.dispatch_id,
                                work_id: id.into(),
                            },
                        )
                        .unwrap();
                }
                if control == "cancel" {
                    store.cancel_campaign_group(&c, branch, 1).unwrap();
                } else {
                    store
                        .resize_campaign_group(
                            &c,
                            branch,
                            1,
                            if control == "pause" { 0 } else { 8 },
                        )
                        .unwrap();
                }
                store.select_allocation_owner(&c, "owner").unwrap();
                let before = store.campaign_ledger(&c).unwrap().unwrap();
                let group = store.campaign_group_status(&c, "controlled").unwrap();
                let allocation = CampaignAllocation {
                    mode: AllocationMode::Deterministic,
                    max_running: 8,
                    allowed_actions: vec![AllocationAction::Reallocate],
                    signals: vec![AllocationSignal {
                        command_id: "transfer".into(),
                        group_id: "controlled".into(),
                        expected_revision: group.0.revision,
                        action: AllocationControl::Reallocate {
                            source_work_id: "source-00".into(),
                            source_generation: 1,
                            target_work_id: "target-00".into(),
                            target_generation: 1,
                            tokens: 1,
                            cost_micro_usd: 1,
                            expected_ledger_revision: before.revision,
                        },
                    }],
                };
                assert!(
                    !store
                        .allocation_control_tick(
                            &c,
                            "owner",
                            &allocation,
                            &[("controlled".into(), vec!["controlled-00".into()])],
                        )
                        .unwrap_or(false),
                    "{branch}: {control}"
                );
                assert_eq!(store.campaign_ledger(&c).unwrap().unwrap(), before);
                assert_eq!(
                    store.campaign_group_status(&c, "controlled").unwrap(),
                    group
                );
            }
        }
    }

    #[test]
    fn host_ceiling_is_enforced_on_resize_and_persisted_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "ceiling", WorkLimits::default());
        let tx = store.database.begin_write().unwrap();
        RuntimeStore::create_campaign_group_in(&tx, spec(&id, "bounded", 1, 1, None), Some(2))
            .unwrap();
        tx.commit().unwrap();
        let before = store.campaign_group_status(&id, "bounded").unwrap().0;
        assert!(store.resize_campaign_group(&id, "bounded", 1, 3).is_err());
        assert_eq!(
            store.campaign_group_status(&id, "bounded").unwrap().0,
            before
        );
        for (revision, cap) in [(1, 0), (2, 2)] {
            store
                .resize_campaign_group(&id, "bounded", revision, cap)
                .unwrap();
        }
        let tx = store.database.begin_write().unwrap();
        let mut root = load(&tx, &id).unwrap().unwrap();
        root.groups.get_mut("bounded").unwrap().max_running = 3;
        save(&tx, &root).unwrap();
        tx.commit().unwrap();
        assert!(store.campaign_group_status(&id, "bounded").is_err());
        assert!(store.claim_campaign_work().is_err());
    }

    #[test]
    fn concurrent_group_replay_is_one_atomic_admission() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = campaign(&store, "replay", WorkLimits::default());
        let request = spec(&id, "group", 4, 2, None);
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, request, barrier) = (store.clone(), request.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    store.create_campaign_group(request).unwrap()
                })
            })
            .collect();
        let groups: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(groups.iter().all(|g| g == &groups[0]));
        assert_eq!(
            store
                .campaign_ledger(&id)
                .unwrap()
                .unwrap()
                .reservations
                .len(),
            4
        );
        assert_eq!(
            store.campaign_group_status(&id, "group").unwrap().1.len(),
            4
        );
    }

    #[test]
    fn concurrent_groups_cannot_share_an_admission_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let id = campaign(&store, "ownership-race", WorkLimits::default());
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|name| {
                let mut request = spec(&id, name, 2, 1, None);
                request.work[1].work_id = "shared".into();
                let (store, barrier) = (store.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    store.create_campaign_group(request)
                })
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        let before = store.campaign_ledger(&id).unwrap();
        assert_eq!(before.as_ref().unwrap().reservations.len(), 2);
        assert_eq!(
            store.list_campaign_groups(&id, None, 64).unwrap(),
            vec![winner.clone()]
        );
        // Direct admission replay must neither steal membership nor consume capacity.
        store
            .admit_campaign_work(winner.spec.work[1].clone())
            .unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
        assert_eq!(
            store
                .campaign_group_status(&id, &winner.spec.group_id)
                .unwrap()
                .1
                .len(),
            2
        );
        let loser = if winner.spec.group_id == "a" {
            "b"
        } else {
            "a"
        };
        assert!(store.admitted_work(&id, &format!("{loser}-00")).is_err());
        store
            .create_campaign_group(spec(&id, loser, 2, 1, None))
            .unwrap();
    }

    #[test]
    fn final_billing_does_not_release_registered_work_or_allow_direct_bypass() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(
            &store,
            "billing",
            WorkLimits {
                max_running: 1,
                ..WorkLimits::default()
            },
        );
        store
            .create_campaign_group(spec(&id, "a", 1, 1, None))
            .unwrap();
        let mut direct = spec(&id, "z", 1, 1, None).work.remove(0);
        direct.pool = Pool::Verification;
        store.admit_campaign_work(direct).unwrap();
        let work = store.claim_campaign_work().unwrap().unwrap();
        store
            .reconcile_campaign_dispatch(
                &work,
                DispatchOutcome::Registered {
                    worker_id: "worker".into(),
                },
            )
            .unwrap();
        store
            .campaign_ledger_command(
                "final",
                &id,
                LedgerCommand::Reconcile {
                    reservation_id: work.dispatch_id.clone(),
                    usage: Usage::Final(Units::default()),
                },
            )
            .unwrap();
        assert_eq!(store.campaign_group_status(&id, "a").unwrap().2, 1);
        assert!(store.claim_campaign_work().unwrap().is_none());
        store.host_acknowledge_work_terminal(&work).unwrap();
        assert_eq!(
            store
                .claim_campaign_work()
                .unwrap()
                .unwrap()
                .admission
                .work_id,
            "z-00"
        );
    }

    #[test]
    fn concurrent_claim_nested_caps_unknown_holds_drain_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = Arc::new(RuntimeStore::open(&path).unwrap());
        let id = campaign(&store, "nested", WorkLimits::default());
        store
            .create_campaign_group(spec(&id, "z-parent", 1, 2, None))
            .unwrap();
        store
            .create_campaign_group(spec(&id, "a-child", 8, 3, Some("z-parent")))
            .unwrap();
        store
            .create_campaign_group(spec(&id, "b-sibling", 4, 3, None))
            .unwrap();
        let barrier = Arc::new(Barrier::new(12));
        let handles: Vec<_> = (0..12)
            .map(|_| {
                let (store, barrier) = (store.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    store.claim_campaign_work().unwrap()
                })
            })
            .collect();
        let claims: Vec<_> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(claims.len(), 3);
        assert_eq!(store.campaign_group_status(&id, "z-parent").unwrap().2, 2);
        assert_eq!(store.campaign_group_status(&id, "b-sibling").unwrap().2, 1);
        for (i, claim) in claims.iter().enumerate() {
            if i == 0 {
                store
                    .reconcile_campaign_dispatch(
                        claim,
                        DispatchOutcome::Registered {
                            worker_id: "worker".into(),
                        },
                    )
                    .unwrap();
            } else {
                store
                    .reconcile_campaign_dispatch(claim, DispatchOutcome::Unknown)
                    .unwrap();
            }
        }
        let ledger = store.campaign_ledger(&id).unwrap();
        let resized = store.resize_campaign_group(&id, "z-parent", 1, 1).unwrap();
        assert_eq!(resized.revision, 2);
        assert!(store.resize_campaign_group(&id, "z-parent", 1, 3).is_err());
        assert_eq!(store.campaign_ledger(&id).unwrap(), ledger);
        assert!(store.claim_campaign_work().unwrap().is_none());
        let children: Vec<_> = claims
            .iter()
            .filter(|w| w.admission.work_id.starts_with("a-child"))
            .collect();
        store.host_acknowledge_work_terminal(children[0]).unwrap();
        // Root now has space, but shrinking the ancestor drains its descendants.
        let next = store.claim_campaign_work().unwrap().unwrap();
        assert!(next.admission.work_id.starts_with("b-sibling"));
        store.host_acknowledge_work_terminal(children[1]).unwrap();
        let next = store.claim_campaign_work().unwrap().unwrap();
        // The rotating cursor may choose another sibling before wrapping.
        assert!(
            next.admission.work_id.starts_with("a-child")
                || next.admission.work_id.starts_with("b-sibling")
                || next.admission.work_id.starts_with("z-parent")
        );
        assert!(store.campaign_group_status(&id, "z-parent").unwrap().2 <= 1);
        let snapshot = store.campaign_group_status(&id, "z-parent").unwrap();
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert_eq!(
            store.campaign_group_status(&id, "z-parent").unwrap(),
            snapshot
        );
        let ledger = store.campaign_ledger(&id).unwrap();
        assert_eq!(
            store
                .create_campaign_group(spec(&id, "z-parent", 1, 2, None))
                .unwrap(),
            snapshot.0
        );
        assert_eq!(store.campaign_ledger(&id).unwrap(), ledger);
        assert!(store.claim_campaign_work().unwrap().is_none());
        store.host_acknowledge_work_terminal(children[0]).unwrap();
        assert!(store.claim_campaign_work().unwrap().is_none());
    }

    #[test]
    fn atomic_creation_replay_depth_total_ownership_and_pagination() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "bounds", WorkLimits::default());
        let parent = spec(&id, "parent", 1, 3, None);
        store.create_campaign_group(parent.clone()).unwrap();
        let before = store.campaign_ledger(&id).unwrap();
        store.resize_campaign_group(&id, "parent", 1, 2).unwrap();
        assert_eq!(
            store
                .create_campaign_group(parent.clone())
                .unwrap()
                .revision,
            2
        );
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
        let mut conflict = parent.clone();
        conflict.max_running = 2;
        assert!(store.create_campaign_group(conflict).is_err());
        let mut stolen = spec(&id, "stolen", 1, 1, None);
        stolen.work = parent.work.clone();
        assert!(store.create_campaign_group(stolen).is_err());
        store
            .create_campaign_group(spec(&id, "child", 1, 3, Some("parent")))
            .unwrap();
        assert!(store
            .create_campaign_group(spec(&id, "deep", 1, 1, Some("child")))
            .is_err());
        let before = store.campaign_ledger(&id).unwrap();
        let mut expensive = spec(&id, "expensive", 2, 1, None);
        expensive.work[1].upper_bound.tokens = 100;
        assert!(store.create_campaign_group(expensive).is_err());
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
        assert!(store.admitted_work(&id, "expensive-00").is_err());
        assert!(store.campaign_group_status(&id, "expensive").is_err());
        // Reusing failed IDs succeeds, proving no receipts/partial membership escaped.
        store
            .create_campaign_group(spec(&id, "expensive", 2, 1, None))
            .unwrap();
        store
            .create_campaign_group(spec(&id, "last", 12, 3, None))
            .unwrap();
        assert!(store
            .create_campaign_group(spec(&id, "extra", 1, 1, None))
            .is_err());
        assert!(store
            .admit_campaign_work(spec(&id, "direct", 1, 1, None).work.remove(0))
            .is_err());
        let page = store.list_campaign_groups(&id, None, 2).unwrap();
        assert_eq!(
            page.iter()
                .map(|g| g.spec.group_id.as_str())
                .collect::<Vec<_>>(),
            ["child", "expensive"]
        );
        let page = store
            .list_campaign_groups(&id, Some("expensive"), 64)
            .unwrap();
        assert_eq!(
            page.iter()
                .map(|g| g.spec.group_id.as_str())
                .collect::<Vec<_>>(),
            ["last", "parent"]
        );
        assert!(store
            .host_configure_work_limits(
                &id,
                WorkLimits {
                    total_work: 32,
                    ..WorkLimits::default()
                }
            )
            .is_err());
    }

    #[test]
    fn cancellation_queued_active_and_unrelated_campaign_progress() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let id = campaign(&store, "cancel", WorkLimits::default());
        store
            .create_campaign_group(spec(&id, "a", 4, 1, None))
            .unwrap();
        store
            .create_campaign_group(spec(&id, "b", 1, 1, Some("a")))
            .unwrap();
        let active = store.claim_campaign_work().unwrap().unwrap();
        let group = store.cancel_campaign_group(&id, "a", 1).unwrap();
        assert!(group.cancellation_requested);
        let (_, status, count) = store.campaign_group_status(&id, "a").unwrap();
        assert_eq!(count, 1);
        assert_eq!(status.iter().filter(|s| s.terminal).count(), 4);
        assert!(status.iter().all(|s| s.cancellation_requested));
        assert!(store
            .create_campaign_group(spec(&id, "new-child", 1, 1, Some("b")))
            .is_err());
        assert!(store.claim_campaign_work().unwrap().is_none());
        let other = campaign(&store, "healthy", WorkLimits::default());
        store
            .create_campaign_group(spec(&other, "z", 2, 1, None))
            .unwrap();
        assert_eq!(
            store
                .dispatch_campaign_batch(10, |_| DispatchOutcome::ConfirmedUnspent)
                .unwrap(),
            2
        );
        let before = store.campaign_ledger(&id).unwrap();
        store.host_acknowledge_work_terminal(&active).unwrap();
        assert_eq!(store.campaign_ledger(&id).unwrap(), before);
        assert_eq!(store.campaign_group_status(&id, "a").unwrap().2, 0);
        let mut forged = active.clone();
        forged.dispatch_id = "stale".into();
        assert!(store.host_acknowledge_work_terminal(&forged).is_err());
        assert!(store
            .resize_campaign_group(&id, "a", group.revision, 2)
            .is_err());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkLimits {
    pub total_work: usize,
    /// Root groups have depth zero; default permits one nested level.
    pub max_depth: usize,
    pub max_running: usize,
    /// Active and suspended work retain a resident lease until host cleanup.
    #[serde(default = "legacy_resident_limit")]
    pub max_resident: usize,
}
fn legacy_resident_limit() -> usize {
    4096
}
impl Default for WorkLimits {
    fn default() -> Self {
        Self {
            total_work: 16,
            max_depth: 1,
            max_running: 3,
            max_resident: 16,
        }
    }
}
impl WorkLimits {
    fn validate(&self) -> Result<(), String> {
        if self.total_work == 0
            || self.total_work > 4096
            || self.max_depth > 8
            || self.max_running == 0
            || self.max_running > self.total_work
            || self.max_resident == 0
            || self.max_resident > 4096
        {
            return Err(err("invalid work limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GroupSpec {
    pub group_id: String,
    pub campaign_id: String,
    pub parent: Option<String>,
    pub max_running: usize,
    pub work: Vec<Admission>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Group {
    /// Exact immutable creation payload is the collision-free replay fingerprint.
    pub spec: GroupSpec,
    pub revision: u64,
    pub max_running: usize,
    /// Catalog-only worker resize ceiling. Older host-created groups have none.
    #[serde(default)]
    pub host_max_running: Option<usize>,
    pub cancellation_requested: bool,
    #[serde(default)]
    policy_disabled: bool,
    #[serde(default)]
    policy_controller: Option<String>,
    #[serde(default)]
    policy_epoch: u64,
    #[serde(default)]
    policy_seen: BTreeSet<String>,
    #[serde(default)]
    policy_commands: BTreeMap<String, tachyon_api::campaign::AllocationSignal>,
    #[serde(default)]
    policy_review: Option<allocation::PendingReview>,
}
mod allocation;
pub(crate) use allocation::PolicyActionContext;
mod attention;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Member {
    group: Option<String>,
    active: bool,
    #[serde(default)]
    resident: bool,
    terminal: bool,
    cancellation_requested: bool,
    #[serde(default)]
    wait: Option<ParentWait>,
    #[serde(default)]
    wait_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum WaitMode {
    Input(String),
    All,
    Any,
    Count(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ParentWait {
    pub revision: u64,
    pub work_ids: Vec<String>,
    pub mode: WaitMode,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParentWaitStatus {
    pub wait: ParentWait,
    pub completed: Vec<String>,
    pub outstanding: Vec<String>,
    pub ready: bool,
    pub resumed: bool,
    pub resource_blocked: bool,
    /// Configured resident ceiling, distinct from execution slots.
    pub resident_capacity: Option<usize>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GroupWorkStatus {
    pub work: AdmittedWork,
    pub active: bool,
    pub terminal: bool,
    pub cancellation_requested: bool,
    pub wait: Option<ParentWait>,
    pub wait_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Root {
    #[serde(default)]
    attention: BTreeMap<String, Vec<tachyon_api::work::Attention>>,
    schema_version: u32,
    campaign_id: String,
    limits: WorkLimits,
    groups: BTreeMap<String, Group>,
    work: BTreeMap<String, Member>,
}
fn load(tx: &WriteTransaction, campaign: &str) -> Result<Option<Root>, String> {
    tx.open_table(ROOTS)
        .map_err(err)?
        .get(campaign)
        .map_err(err)?
        .map(|v| {
            let root: Root = serde_json::from_slice(v.value()).map_err(err)?;
            if root.schema_version != 1 || root.campaign_id != campaign {
                return Err(err("invalid root schema/identity"));
            }
            root.limits.validate()?;
            if root.work.len() > root.limits.total_work || root.groups.len() > root.work.len() {
                return Err(err("invalid root work bound"));
            }
            if root.attention.len() > root.work.len() {
                return Err(err("invalid attention work bound"));
            }
            for (id, questions) in &root.attention {
                let mut seen = BTreeSet::new();
                if !root.work.contains_key(id) || questions.len() > 32 {
                    return Err(err("invalid attention bound"));
                }
                for q in questions {
                    if q.campaign_id != campaign || &q.work_id != id || q.generation == 0 || q.instruction_revision == 0 || !seen.insert(&q.request_id)
                        || (tachyon_api::work::Request::Ask { request_id: q.request_id.clone(), question: q.question.clone(), timeout_ms: q.timeout_ms }).validate().is_err()
                        || q.answer.as_ref().is_some_and(|s| s.trim().is_empty() || s.len() > 4096 || s.contains('\0')) {
                        return Err(err("invalid persisted attention"));
                    }
                }
            }
            for (id, group) in &root.groups {
                if group.spec.group_id != *id
                    || group.spec.campaign_id != campaign
                    || group.revision == 0
                    || group.max_running > root.limits.max_running
                    || group.host_max_running.is_some_and(|cap| cap < group.spec.max_running || cap < group.max_running || cap > 4096)
                    || group.spec.work.is_empty()
                    || group.policy_seen.len() > group.spec.work.len()
                    || group.policy_commands.len() > 32
                    || group.policy_seen.iter().any(|id| !group.spec.work.iter().any(|w| &w.work_id == id))
                    || group.policy_controller.as_ref().is_some_and(|id| id.is_empty() || id.len() > 512 || group.policy_epoch == 0)
                {
                    return Err(err("invalid group identity/bounds"));
                }
                ancestors(&root, Some(id))?;
                let mut ids = BTreeSet::new();
                for work in &group.spec.work {
                    if work.campaign_id != campaign
                        || !ids.insert(&work.work_id)
                        || root
                            .work
                            .get(&work.work_id)
                            .and_then(|m| m.group.as_deref())
                            != Some(id.as_str())
                    {
                        return Err(err("invalid group membership"));
                    }
                }
            }
            for (work_id, member) in &root.work {
                if member.active && member.terminal {
                    return Err(err("terminal member is active"));
                }
                if let Some(wait) = &member.wait {
                    if let WaitMode::Input(id) = &wait.mode {
                        if !wait.work_ids.is_empty() || !root.attention.get(work_id).is_some_and(|qs| qs.iter().any(|q| &q.request_id == id && q.deadline_ms == wait.deadline_ms)) {
                            return Err(err("invalid persisted attention wait"));
                        }
                    }
                    if member.active || wait.revision == 0 || wait.revision != member.wait_revision
                        || (wait.work_ids.is_empty() && !matches!(wait.mode, WaitMode::Input(_))) || wait.work_ids.len() > 64
                        || wait.work_ids.iter().collect::<BTreeSet<_>>().len() != wait.work_ids.len()
                        || wait.work_ids.iter().any(|id| !root.work.contains_key(id))
                        || matches!(wait.mode, WaitMode::Count(n) if n == 0 || n > wait.work_ids.len()) {
                        return Err(err("invalid persisted parent wait"));
                    }
                }
                ancestors(&root, member.group.as_deref())?;
            }
            Ok(root)
        })
        .transpose()
}
fn save(tx: &WriteTransaction, root: &Root) -> Result<(), String> {
    if super::campaign_oversight::enabled_in(tx, &root.campaign_id)? {
        if let Some(previous) = load(tx, &root.campaign_id)? {
            if root.work.iter().any(|(id, member)| {
                member.wait != previous.work.get(id).and_then(|m| m.wait.clone())
            }) {
                super::campaign_oversight::trigger_in(tx, &root.campaign_id, "blocked_state")?;
            }
            if root.work.iter().any(|(id, member)| {
                member.terminal && previous.work.get(id).is_some_and(|m| !m.terminal)
            }) {
                super::campaign_oversight::trigger_in(
                    tx,
                    &root.campaign_id,
                    "logical_work_terminal",
                )?;
            }
        }
    }
    tx.open_table(ROOTS)
        .map_err(err)?
        .insert(
            root.campaign_id.as_str(),
            serde_json::to_vec(root).map_err(err)?.as_slice(),
        )
        .map_err(err)?;
    Ok(())
}
fn ancestors(root: &Root, group: Option<&str>) -> Result<Vec<String>, String> {
    let mut result = Vec::new();
    let mut next = group;
    while let Some(id) = next {
        if result.len() > root.limits.max_depth || result.iter().any(|v| v == id) {
            return Err(err("group depth/cycle"));
        }
        let group = root
            .groups
            .get(id)
            .ok_or_else(|| err("unknown parent/group"))?;
        result.push(id.to_owned());
        next = group.spec.parent.as_deref();
    }
    Ok(result)
}

fn resize_group(
    root: &mut Root,
    id: &str,
    expected_revision: u64,
    max_running: usize,
    policy: Option<(&str, u64)>,
) -> Result<Group, String> {
    if max_running > root.limits.max_running {
        return Err(err("root concurrency limit"));
    }
    let group = root
        .groups
        .get_mut(id)
        .ok_or_else(|| err("unknown group"))?;
    if group.revision != expected_revision || group.cancellation_requested {
        return Err(err("group revision/cancellation conflict"));
    }
    if group.host_max_running.is_some_and(|cap| max_running > cap) {
        return Err(err("group host cap exceeded"));
    }
    if let Some((owner, epoch)) = policy {
        if group.policy_disabled
            || group.policy_controller.as_deref() != Some(owner)
            || group.policy_epoch != epoch
        {
            return Err(err("stale policy control epoch"));
        }
    } else {
        group.policy_disabled = true;
        group.policy_epoch = group
            .policy_epoch
            .checked_add(1)
            .ok_or("policy epoch overflow")?;
    }
    group.revision = group
        .revision
        .checked_add(1)
        .ok_or_else(|| err("revision overflow"))?;
    group.max_running = max_running;
    Ok(group.clone())
}

impl RuntimeStore {
    /// Cancellation is intent until the launch owner confirms cleanup.
    pub(crate) fn host_cancel_work(
        &self,
        campaign: &str,
        id: &str,
        generation: u64,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        let work = Self::admitted_work_in(&tx, id)?;
        if work.admission.campaign_id != campaign || work.admission.generation != generation {
            return Err(err("cancellation identity conflict"));
        }
        let mut ids = Self::agent_descendants_in(&tx, campaign, id)?;
        ids.push(id.to_owned());
        for id in ids {
            Self::cancel_work_in(&tx, campaign, &id)?;
        }
        tx.commit().map_err(err)
    }

    pub(super) fn cancel_work_in(
        tx: &WriteTransaction,
        campaign: &str,
        id: &str,
    ) -> Result<(), String> {
        let mut root = load(tx, campaign)?.ok_or_else(|| err("unknown root"))?;
        let member = root.work.get_mut(id).ok_or_else(|| err("unknown work"))?;
        let mut work = Self::admitted_work_in(tx, id)?;
        member.cancellation_requested = true;
        if work.state == DispatchState::Admitted {
            Self::campaign_ledger_command_in(
                &tx,
                &format!("unspent:{}", work.dispatch_id),
                campaign,
                LedgerCommand::Reconcile {
                    reservation_id: work.dispatch_id.clone(),
                    usage: Usage::Final(Units::default()),
                },
            )?;
            work.state = DispatchState::Cancelled;
            member.terminal = true;
            tx.open_table(WORK)
                .map_err(err)?
                .insert(id, serde_json::to_vec(&work).map_err(err)?.as_slice())
                .map_err(err)?;
            tx.open_table(PENDING)
                .map_err(err)?
                .remove(id)
                .map_err(err)?;
        }
        save(&tx, &root)?;
        Ok(())
    }

    pub(super) fn work_group_in(
        tx: &WriteTransaction,
        campaign: &str,
        id: &str,
    ) -> Result<Option<String>, String> {
        Ok(load(tx, campaign)?
            .ok_or("unknown root")?
            .work
            .get(id)
            .ok_or("unknown parent")?
            .group
            .clone())
    }

    pub(super) fn inherit_work_group_in(
        tx: &WriteTransaction,
        campaign: &str,
        parent: &str,
        child: &str,
    ) -> Result<(), String> {
        let mut root = load(tx, campaign)?.ok_or("unknown root")?;
        let group = root.work.get(parent).ok_or("unknown parent")?.group.clone();
        root.work.get_mut(child).ok_or("unknown child")?.group = group;
        save(tx, &root)
    }

    pub(super) fn coordination_limits_in(
        tx: &WriteTransaction,
        campaign: &str,
    ) -> Result<WorkLimits, String> {
        Ok(load(tx, campaign)?
            .ok_or_else(|| err("host work limits required"))?
            .limits)
    }

    pub(crate) fn campaign_work_status(
        &self,
        campaign: &str,
        id: &str,
    ) -> Result<GroupWorkStatus, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let Some(root) = load(&tx, campaign)? else {
            // Root-only public launches predate group enrollment. Final billing
            // alone does not prove process cleanup; require settled execution.
            let work = Self::admitted_work_in(&tx, id)?;
            if work.admission.campaign_id != campaign {
                return Err(err("campaign identity mismatch"));
            }
            let ledger = Self::campaign_ledger_in(&tx, campaign)?;
            let hold = ledger
                .reservations
                .get(&work.dispatch_id)
                .ok_or_else(|| err("missing hold"))?;
            let execution = tx.open_table(super::execution::EXECUTIONS).map_err(err)?;
            let owners = tx.open_table(super::execution::VERIFIERS).map_err(err)?;
            let owner = owners.get(work.dispatch_id.as_str()).map_err(err)?;
            let execution_id = owner.as_ref().map_or(id, |row| row.value());
            let record = execution
                .get(execution_id)
                .map_err(err)?
                .map(|row| super::execution::decode_record(row.value(), execution_id))
                .transpose()?;
            let awaiting_acceptance = record.as_ref().is_some_and(|record| {
                record.phase == super::execution::ExecutionPhase::AwaitingAcceptance
            });
            let settled = record.is_some_and(|record| {
                record.settled
                    && (record.policy.funding.dispatch_id == work.dispatch_id
                        || record.policy.verification.dispatch_id == work.dispatch_id)
            });
            let terminal = settled
                || matches!(
                    work.state,
                    DispatchState::Cancelled | DispatchState::ConfirmedUnspent
                );
            return Ok(GroupWorkStatus {
                active: !terminal
                    && !awaiting_acceptance
                    && matches!(
                        work.state,
                        DispatchState::Registered { .. } | DispatchState::DispatchingUnknown
                    ),
                terminal,
                cancellation_requested: hold.cancellation_requested,
                work,
                wait: None,
                wait_revision: 0,
            });
        };
        let member = root
            .work
            .get(id)
            .ok_or_else(|| err("unknown campaign work"))?;
        Ok(GroupWorkStatus {
            work: Self::admitted_work_in(&tx, id)?,
            active: member.active,
            terminal: member.terminal,
            cancellation_requested: member.cancellation_requested,
            wait: member.wait.clone(),
            wait_revision: member.wait_revision,
        })
    }

    /// HOST ONLY: caller fences permit dispatch under the authority lock at a
    /// tool boundary, or stops the parent and revokes its permits. The persisted
    /// inactive lease denies funding until a successful revision-bound resume.
    pub(crate) fn host_suspend_parent(
        &self,
        identity: &AdmittedWork,
        expected_revision: u64,
        work_ids: Vec<String>,
        mode: WaitMode,
        deadline_ms: u64,
    ) -> Result<ParentWait, String> {
        self.suspend_work(
            identity,
            expected_revision,
            work_ids,
            mode,
            deadline_ms,
            None,
        )
    }

    pub(crate) fn suspend_work(
        &self,
        identity: &AdmittedWork,
        expected_revision: u64,
        work_ids: Vec<String>,
        mode: WaitMode,
        deadline_ms: u64,
        attention: Option<tachyon_api::work::Attention>,
    ) -> Result<ParentWait, String> {
        let now = now_ms()?;
        if (work_ids.is_empty() && !matches!(mode, WaitMode::Input(_)))
            || work_ids.len() > 64
            || deadline_ms <= now
            || deadline_ms.saturating_sub(now) > 300_000
            || matches!(mode, WaitMode::Count(n) if n == 0 || n > work_ids.len())
        {
            return Err(err("invalid bounded parent wait"));
        }
        let tx = self.database.begin_write().map_err(err)?;
        let work = Self::admitted_work_in(&tx, &identity.admission.work_id)?;
        if work != *identity || !matches!(work.state, DispatchState::Registered { .. }) {
            return Err(err("parent requires exact registered identity"));
        }
        Self::group_funding_in(&tx, &work)?;
        let campaign = &work.admission.campaign_id;
        let ledger = Self::campaign_ledger_in(&tx, campaign)?;
        if ledger.allocations.get(&work.dispatch_id) != Some(&false)
            || ledger.reservations.values().any(|r| {
                r.allocation.as_ref() == Some(&work.dispatch_id)
                    && !matches!(r.usage, Usage::Final(_))
            })
        {
            return Err(err("parent has active or unknown inference holds"));
        }
        let mut root = load(&tx, campaign)?.ok_or_else(|| err("host work limits required"))?;
        if let Some(attention) = attention {
            if attention.campaign_id != *campaign
                || attention.work_id != work.admission.work_id
                || attention.generation != work.admission.generation
            {
                return Err(err("attention source identity mismatch"));
            }
            if Self::latest_instruction_revision_in(&tx, &work.admission)?
                != attention.instruction_revision
            {
                return Err(err("stale attention revision"));
            }
            let questions = root
                .attention
                .entry(work.admission.work_id.clone())
                .or_default();
            if questions.len() >= 32
                || questions
                    .iter()
                    .any(|q| q.request_id == attention.request_id)
            {
                return Err(err("attention limit or duplicate request"));
            }
            Self::admit_attention_in(
                &tx,
                super::attention::AttentionSource {
                    scope: tachyon_api::todo::TodoScope::Campaign {
                        campaign_id: campaign.clone(),
                    },
                    work_id: Some(attention.work_id.clone()),
                    campaign_id: Some(campaign.clone()),
                    generation: attention.generation,
                    instruction_revision: attention.instruction_revision,
                    category: tachyon_api::attention::AttentionCategory::Question,
                    cause_id: attention.request_id.clone(),
                },
                now,
            )?;
            questions.push(attention);
        }
        if root
            .work
            .values()
            .filter(|m| m.resident || m.active || m.wait.is_some())
            .count()
            >= root.limits.max_resident
            && work_ids.iter().any(|id| {
                root.work
                    .get(id)
                    .is_some_and(|m| !m.terminal && !m.resident && !m.active && m.wait.is_none())
            })
        {
            return Err(err("no resident capacity for waited work"));
        }
        let mut seen = BTreeSet::new();
        for id in &work_ids {
            if id == &work.admission.work_id || !seen.insert(id) || !root.work.contains_key(id) {
                return Err(err("wait references must be unique existing campaign work"));
            }
            // Reject dependency cycles, including indirect waits, within the root bound.
            let mut pending = vec![id.as_str()];
            let mut visited = BTreeSet::new();
            while let Some(next) = pending.pop() {
                if next == work.admission.work_id {
                    return Err(err("parent wait cycle"));
                }
                if visited.insert(next) {
                    if let Some(wait) = &root.work[next].wait {
                        pending.extend(wait.work_ids.iter().map(String::as_str));
                    }
                }
            }
        }
        let member = root.work.get_mut(&work.admission.work_id).unwrap();
        if member.wait_revision != expected_revision || member.wait.is_some() {
            return Err(err("parent wait revision conflict"));
        }
        member.wait_revision = member
            .wait_revision
            .checked_add(1)
            .ok_or_else(|| err("revision overflow"))?;
        let wait = ParentWait {
            revision: member.wait_revision,
            work_ids,
            mode,
            deadline_ms,
        };
        member.active = false;
        member.resident = true;
        member.wait = Some(wait.clone());
        save(&tx, &root)?;
        tx.commit().map_err(err)?;
        Ok(wait)
    }

    /// Bounded snapshot, never blocks an actor or holds a transaction while waiting.
    /// `resume` is an authorized host CAS, not permission to replay a process.
    pub(crate) fn host_poll_parent_wait(
        &self,
        identity: &AdmittedWork,
        revision: u64,
        resume: bool,
    ) -> Result<ParentWaitStatus, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let work = Self::admitted_work_in(&tx, &identity.admission.work_id)?;
        if work != *identity {
            return Err(err("parent identity conflict"));
        }
        let mut root =
            load(&tx, &work.admission.campaign_id)?.ok_or_else(|| err("unknown root"))?;
        let member = &root.work[&work.admission.work_id];
        let wait = member
            .wait
            .clone()
            .ok_or_else(|| err("parent is not waiting"))?;
        if wait.revision != revision || member.terminal || member.cancellation_requested {
            return Err(err("parent wait revision/cancellation conflict"));
        }
        let ledger = Self::campaign_ledger_in(&tx, &work.admission.campaign_id)?;
        let mut status = ParentWaitStatus {
            wait: wait.clone(),
            completed: vec![],
            outstanding: vec![],
            ready: false,
            resumed: false,
            resource_blocked: ledger.admissions_paused,
            resident_capacity: Some(root.limits.max_resident),
        };
        for id in &wait.work_ids {
            let child = &root.work[id];
            if child.terminal {
                status.completed.push(id.clone());
            } else {
                status.outstanding.push(id.clone());
                let admitted = Self::admitted_work_in(&tx, id)?;
                let hold = ledger
                    .reservations
                    .get(&admitted.dispatch_id)
                    .ok_or_else(|| err("missing child hold"))?;
                if !child.active
                    && (hold.cancellation_requested
                        || (!ledger.allocations.contains_key(&admitted.dispatch_id)
                            && hold.usage != Usage::Unknown))
                {
                    status.resource_blocked = true;
                }
                if child.cancellation_requested
                    || ancestors(&root, child.group.as_deref())?.iter().any(|g| {
                        root.groups[g].max_running == 0 || root.groups[g].cancellation_requested
                    })
                {
                    status.resource_blocked = true;
                }
            }
        }
        let required = match &wait.mode {
            WaitMode::All => wait.work_ids.len(),
            WaitMode::Any => 1,
            WaitMode::Count(n) => *n,
            WaitMode::Input(id) => {
                let question = root
                    .attention
                    .get(&work.admission.work_id)
                    .and_then(|qs| qs.iter().find(|q| &q.request_id == id))
                    .ok_or_else(|| err("missing attention"))?;
                if Self::latest_instruction_revision_in(&tx, &work.admission)?
                    != question.instruction_revision
                {
                    return Err(err("stale attention revision"));
                }
                if question.answer.is_some() {
                    0
                } else {
                    1
                }
            }
        };
        status.ready = status.completed.len() >= required
            || now_ms()? >= wait.deadline_ms
            || status.resource_blocked;
        if resume && status.ready {
            root.work.get_mut(&work.admission.work_id).unwrap().wait = None;
            save(&tx, &root)?;
            if ledger.admissions_paused || !Self::group_claim_in(&tx, &work)? {
                status.resource_blocked = true;
                // Abort the tentative removal; a later poll retains the exact wait.
                return Ok(status);
            }
            tx.commit().map_err(err)?;
            status.resumed = true;
        }
        Ok(status)
    }

    /// Explicit authorized host opt-in, before any Work exists. Immutable and
    /// idempotent; this creates no money and cannot retrofit away existing work.
    pub(crate) fn host_configure_work_limits(
        &self,
        campaign: &str,
        limits: WorkLimits,
    ) -> Result<(), String> {
        limits.validate()?;
        let tx = self.database.begin_write().map_err(err)?;
        Self::campaign_ledger_in(&tx, campaign)?;
        if let Some(root) = load(&tx, campaign)? {
            return if root.limits == limits {
                Ok(())
            } else {
                Err(err("work limits conflict"))
            };
        }
        for entry in tx.open_table(WORK).map_err(err)?.iter().map_err(err)? {
            let (_, value) = entry.map_err(err)?;
            let work: AdmittedWork = serde_json::from_slice(value.value()).map_err(err)?;
            if work.admission.campaign_id == campaign {
                return Err(err("configure before admission"));
            }
        }
        save(
            &tx,
            &Root {
                schema_version: 1,
                campaign_id: campaign.into(),
                limits,
                groups: BTreeMap::new(),
                work: BTreeMap::new(),
                attention: BTreeMap::new(),
            },
        )?;
        tx.commit().map_err(err)
    }

    pub(crate) fn create_campaign_group(&self, spec: GroupSpec) -> Result<Group, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let group = Self::create_campaign_group_in(&tx, spec, None)?;
        tx.commit().map_err(err)?;
        Ok(group)
    }

    pub(super) fn create_campaign_group_in(
        tx: &WriteTransaction,
        spec: GroupSpec,
        host_max_running: Option<usize>,
    ) -> Result<Group, String> {
        let mut root =
            load(&tx, &spec.campaign_id)?.ok_or_else(|| err("host work limits required"))?;
        if let Some(group) = root.groups.get(&spec.group_id) {
            return if group.spec == spec && group.host_max_running == host_max_running {
                Ok(group.clone())
            } else {
                Err(err("group payload conflict"))
            };
        }
        if spec.group_id.trim().is_empty()
            || spec.group_id.len() > 256
            || spec.work.is_empty()
            || spec.max_running == 0
            || spec.max_running > root.limits.max_running
            || host_max_running.is_some_and(|cap| cap < spec.max_running || cap > 4096)
            || spec.work.len() > root.limits.total_work - root.work.len()
        {
            return Err(err("invalid group bounds"));
        }
        let parents = ancestors(&root, spec.parent.as_deref())?;
        if parents.len() > root.limits.max_depth
            || parents
                .iter()
                .any(|id| root.groups[id].cancellation_requested)
        {
            return Err(err("parent depth/cancellation"));
        }
        let mut ids = BTreeSet::new();
        for work in &spec.work {
            if work.campaign_id != spec.campaign_id
                || root.work.contains_key(&work.work_id)
                || !ids.insert(&work.work_id)
            {
                return Err(err("work scope/ownership conflict"));
            }
        }
        // Admission updates this same root and reserves from the existing ledger.
        // Any error aborts all admissions, receipts, and membership together.
        for work in &spec.work {
            Self::admit_campaign_work_in(&tx, work.clone())?;
        }
        root = load(&tx, &spec.campaign_id)?.ok_or_else(|| err("missing root"))?;
        for work in &spec.work {
            root.work.get_mut(&work.work_id).unwrap().group = Some(spec.group_id.clone());
        }
        let group = Group {
            policy_disabled: false,
            policy_controller: None,
            policy_epoch: 0,
            policy_seen: BTreeSet::new(),
            policy_commands: BTreeMap::new(),
            policy_review: None,
            host_max_running,
            max_running: spec.max_running,
            spec,
            revision: 1,
            cancellation_requested: false,
        };
        root.groups
            .insert(group.spec.group_id.clone(), group.clone());
        save(&tx, &root)?;
        Ok(group)
    }

    pub(crate) fn resize_campaign_group(
        &self,
        campaign: &str,
        id: &str,
        expected_revision: u64,
        max_running: usize,
    ) -> Result<Group, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut root = load(&tx, campaign)?.ok_or_else(|| err("unknown root"))?;
        let result = resize_group(&mut root, id, expected_revision, max_running, None)?;
        save(&tx, &root)?;
        tx.commit().map_err(err)?;
        Ok(result)
    }

    /// Exclusive lexicographic cursor, at most 64 groups from a bounded root.
    pub(crate) fn list_campaign_groups(
        &self,
        campaign: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Group>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let root = load(&tx, campaign)?.ok_or_else(|| err("unknown root"))?;
        Ok(root
            .groups
            .into_iter()
            .filter(|(id, _)| after.is_none_or(|a| id.as_str() > a))
            .take(limit.min(64))
            .map(|(_, group)| group)
            .collect())
    }

    pub(crate) fn campaign_group_status(
        &self,
        campaign: &str,
        id: &str,
    ) -> Result<(Group, Vec<GroupWorkStatus>, usize), String> {
        let tx = self.database.begin_write().map_err(err)?;
        let root = load(&tx, campaign)?.ok_or_else(|| err("unknown root"))?;
        let group = root
            .groups
            .get(id)
            .ok_or_else(|| err("unknown group"))?
            .clone();
        let mut work = Vec::new();
        let mut active = 0;
        for (key, member) in &root.work {
            if ancestors(&root, member.group.as_deref())?
                .iter()
                .any(|v| v == id)
            {
                work.push(GroupWorkStatus {
                    work: Self::admitted_work_in(&tx, key)?,
                    active: member.active,
                    terminal: member.terminal,
                    cancellation_requested: member.cancellation_requested,
                    wait: member.wait.clone(),
                    wait_revision: member.wait_revision,
                });
                active += usize::from(member.active);
            }
        }
        Ok((group, work, active))
    }

    /// Descendant cancellation is intent for claimed work, never proof of cleanup.
    pub(crate) fn cancel_campaign_group(
        &self,
        campaign: &str,
        id: &str,
        expected_revision: u64,
    ) -> Result<Group, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut root = load(&tx, campaign)?.ok_or_else(|| err("unknown root"))?;
        let group = root
            .groups
            .get_mut(id)
            .ok_or_else(|| err("unknown group"))?;
        if group.revision != expected_revision {
            return Err(err("group revision conflict"));
        }
        group.revision = group
            .revision
            .checked_add(1)
            .ok_or_else(|| err("revision overflow"))?;
        group.cancellation_requested = true;
        let result = group.clone();
        let keys: Vec<_> = root
            .work
            .iter()
            .filter_map(
                |(key, member)| match ancestors(&root, member.group.as_deref()) {
                    Ok(path) if path.iter().any(|v| v == id) => Some(Ok(key.clone())),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                },
            )
            .collect::<Result<_, _>>()?;
        for key in keys {
            let member = root.work.get_mut(&key).unwrap();
            member.cancellation_requested = true;
            let mut work = Self::admitted_work_in(&tx, &key)?;
            if work.state == DispatchState::Admitted {
                Self::campaign_ledger_command_in(
                    &tx,
                    &format!("unspent:{}", work.dispatch_id),
                    campaign,
                    LedgerCommand::Reconcile {
                        reservation_id: work.dispatch_id.clone(),
                        usage: Usage::Final(Units::default()),
                    },
                )?;
                work.state = DispatchState::Cancelled;
                member.terminal = true;
                tx.open_table(WORK)
                    .map_err(err)?
                    .insert(
                        key.as_str(),
                        serde_json::to_vec(&work).map_err(err)?.as_slice(),
                    )
                    .map_err(err)?;
                tx.open_table(PENDING)
                    .map_err(err)?
                    .remove(key.as_str())
                    .map_err(err)?;
            }
        }
        save(&tx, &root)?;
        tx.commit().map_err(err)?;
        Ok(result)
    }

    pub(super) fn group_admission_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<(), String> {
        let Some(mut root) = load(tx, &admission.campaign_id)? else {
            return Ok(());
        };
        if root.work.len() >= root.limits.total_work {
            return Err(err("root total work limit"));
        }
        root.work.insert(
            admission.work_id.clone(),
            Member {
                group: None,
                active: false,
                resident: false,
                terminal: false,
                cancellation_requested: false,
                wait: None,
                wait_revision: 0,
            },
        );
        save(tx, &root)
    }

    pub(super) fn group_claim_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<bool, String> {
        let Some(mut root) = load(tx, &work.admission.campaign_id)? else {
            return Ok(true);
        };
        let member = root
            .work
            .get(&work.admission.work_id)
            .ok_or_else(|| err("missing membership"))?;
        if member.active
            || member.terminal
            || member.cancellation_requested
            || member.wait.is_some()
        {
            return Ok(false);
        }
        if root.work.values().filter(|m| m.active).count() >= root.limits.max_running {
            return Ok(false);
        }
        if !member.resident
            && root
                .work
                .values()
                .filter(|m| m.resident || m.active || m.wait.is_some())
                .count()
                >= root.limits.max_resident
        {
            return Ok(false);
        }
        for id in ancestors(&root, member.group.as_deref())? {
            let group = &root.groups[&id];
            if group.cancellation_requested {
                return Ok(false);
            }
            let mut count = 0;
            for other in root.work.values().filter(|m| m.active) {
                if ancestors(&root, other.group.as_deref())?.contains(&id) {
                    count += 1;
                }
            }
            if count >= group.max_running {
                return Ok(false);
            }
        }
        root.work.get_mut(&work.admission.work_id).unwrap().active = true;
        root.work.get_mut(&work.admission.work_id).unwrap().resident = true;
        save(tx, &root)?;
        Ok(true)
    }

    pub(super) fn group_funding_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<(), String> {
        if let Some(root) = load(tx, &work.admission.campaign_id)? {
            let member = root
                .work
                .get(&work.admission.work_id)
                .ok_or_else(|| err("missing membership"))?;
            if !member.active || member.terminal || member.cancellation_requested {
                return Err(err("work is not executable"));
            }
        }
        Ok(())
    }

    pub(super) fn group_cancelled_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<bool, String> {
        let Some(root) = load(tx, &work.admission.campaign_id)? else {
            return Ok(false);
        };
        let member = root
            .work
            .get(&work.admission.work_id)
            .ok_or_else(|| err("missing membership"))?;
        Ok(member.cancellation_requested
            || ancestors(&root, member.group.as_deref())?
                .iter()
                .any(|id| root.groups[id].cancellation_requested))
    }

    pub(super) fn group_terminal_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<(), String> {
        if let Some(mut root) = load(tx, &work.admission.campaign_id)? {
            let member = root
                .work
                .get_mut(&work.admission.work_id)
                .ok_or_else(|| err("missing membership"))?;
            member.active = false;
            member.resident = false;
            member.terminal = true;
            member.wait = None;
            save(tx, &root)?;
            for child in Self::agent_descendants_in(
                tx,
                &work.admission.campaign_id,
                &work.admission.work_id,
            )? {
                Self::cancel_work_in(tx, &work.admission.campaign_id, &child)?;
            }
        }
        Ok(())
    }

    /// A reaped command attempt releases running capacity, not logical Work completion.
    pub(super) fn group_review_pending_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<(), String> {
        if let Some(mut root) = load(tx, &work.admission.campaign_id)? {
            let member = root
                .work
                .get_mut(&work.admission.work_id)
                .ok_or_else(|| err("missing membership"))?;
            member.active = false;
            member.terminal = member.cancellation_requested;
            member.resident = false;
            member.wait = None;
            save(tx, &root)?;
        }
        Ok(())
    }

    /// Only the command coordinator calls this after final billing and known exit.
    /// Capacity and cancellation checks remain the ordinary group claim checks.
    pub(super) fn group_repair_claim_in(
        tx: &WriteTransaction,
        work: &AdmittedWork,
    ) -> Result<bool, String> {
        if let Some(mut root) = load(tx, &work.admission.campaign_id)? {
            let member = root
                .work
                .get_mut(&work.admission.work_id)
                .ok_or_else(|| err("missing membership"))?;
            if member.active || member.cancellation_requested || member.wait.is_some() {
                return Ok(false);
            }
            member.terminal = false;
            save(tx, &root)?;
        }
        Self::group_claim_in(tx, work)
    }

    /// Host proof that execution and all its effects have stopped. Billing stays
    /// unresolved; callers must never use timeout, registration, or final usage
    /// alone as termination evidence. Exact admission identity fences stale acks.
    pub(crate) fn host_acknowledge_work_terminal(
        &self,
        identity: &AdmittedWork,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        let work = Self::admitted_work_in(&tx, &identity.admission.work_id)?;
        if work.admission != identity.admission
            || work.dispatch_id != identity.dispatch_id
            || work.state == DispatchState::Admitted
        {
            return Err(err("terminal identity/state conflict"));
        }
        Self::group_terminal_in(&tx, &work)?;
        tx.commit().map_err(err)
    }
}
