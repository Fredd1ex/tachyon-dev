use super::*;
use sha2::{Digest, Sha256};
use tachyon_api::{
    agents::Proposal,
    campaign::{ChildInput, ChildProfile, InputFile},
    types::{WorkConstraints, WorkPermissions, WorkTaskType},
};

fn profile(template: &mut HostTemplate, path: &std::path::Path) -> ChildProfile {
    std::fs::create_dir(path.join("inputs")).unwrap();
    std::fs::create_dir(path.join("managed")).unwrap();
    std::fs::write(path.join("inputs/approved.txt"), b"hello\n").unwrap();
    std::fs::write(path.join("inputs/unapproved.txt"), b"not copied").unwrap();
    let a = &template.candidates[0].admission.upper_bound;
    let v = &template.candidates[0].verification.upper_bound;
    let profile = ChildProfile {
        profile_ids: vec![],
        evaluator: None,
        profile_id: "inspect".into(),
        max_proposals: 2,
        max_objective_bytes: 1024,
        max_context_refs: 1,
        managed_root: path.join("managed"),
        max_input_bytes: 1024,
        max_input_files: 2,
        inputs: vec![ChildInput {
            root: path.join("inputs"),
            files: vec![InputFile {
                path: "approved.txt".into(),
                sha256: format!("{:x}", Sha256::digest(b"hello\n")),
            }],
        }],
        permissions: WorkPermissions {
            task_type: WorkTaskType::CodingReadOnly,
            allow_exec: false,
            allow_python: false,
        },
        work_tokens: a.tokens,
        work_cost_micro_usd: a.cost_micro_usd,
        verification_tokens: v.tokens,
        verification_cost_micro_usd: v.cost_micro_usd,
    };
    template.template_id = profile.template_id(&template.parent.campaign_id);
    template.group_id = Some(template.template_id.clone());
    for (i, candidate) in template.candidates.iter_mut().enumerate() {
        let root = profile.managed_root.join(format!("slot-{i}"));
        candidate.workspace = root.join("work");
        candidate.home = root.join("home");
        candidate.work.constraints = Some(WorkConstraints {
            permissions: profile.permissions.clone(),
            input_context: vec![],
        });
    }
    profile
}

fn proposal(objective: &str) -> Proposal {
    Proposal {
        profile_id: "inspect".into(),
        objective: objective.into(),
        context_refs: vec![],
    }
}

fn group() -> Request {
    Request::ProposeGroup {
        specs: vec![
            proposal("Inspect the approved greeting"),
            proposal("Check the approved line ending"),
        ],
        command_id: "dynamic-once".into(),
        max_running: 1,
    }
}

#[test]
fn legacy_snapshot_census_charges_committed_receipts_once_and_restore_keeps_debt() {
    let (dir, store, root, mut template) = setup(limits());
    let profile = profile(&mut template, dir.path());
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
    let permit = register(&store, &root);
    scheduler
        .approve_profile(template.clone(), profile.clone())
        .unwrap();
    store
        .broker_control(permit.0, &root.policy.model, group())
        .unwrap();
    // Simulate an old database: keep real committed proposal metadata and files,
    // but remove the newly introduced ledger table before running startup census.
    let refs: redb::TableDefinition<(&str, &str, &str), &[u8]> =
        redb::TableDefinition::new("retained_refs_v1");
    let tx = store.database.begin_write().unwrap();
    tx.delete_table(refs).unwrap();
    tx.open_table(refs).unwrap();
    tx.commit().unwrap();
    store
        .retained
        .configure(&template.parent.campaign_id, Some(1))
        .unwrap();
    store.adopt_retained_snapshots().unwrap();
    let before = store
        .retained
        .summary(&template.parent.campaign_id)
        .unwrap();
    assert_eq!(
        before["campaign_charged_bytes"],
        2 * (profile.max_input_bytes + 524288)
    );
    store.adopt_retained_snapshots().unwrap();
    scheduler.restore_proposals().unwrap();
    assert_eq!(
        store
            .retained
            .summary(&template.parent.campaign_id)
            .unwrap(),
        before
    );
    assert!(store
        .retained
        .reserve(&template.parent.campaign_id, "trace", "new", 1, "hash")
        .is_err());
}

#[test]
fn nested_profiles_share_slots_envelope_scope_and_recursive_cancellation() {
    for (depth, inherit, total, allowed) in [
        (1, true, 16, false),
        (2, false, 16, false),
        (2, true, 4, false),
        (2, true, 16, true),
    ] {
        let (dir, store, root, mut template) = setup(WorkLimits {
            max_depth: depth,
            total_work: total,
            ..limits()
        });
        let mut p = profile(&mut template, dir.path());
        if inherit {
            p.profile_ids.push("inspect".into());
        }
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
        let permit = register(&store, &root);
        scheduler.approve_profile(template.clone(), p).unwrap();
        let request = |command: &str| Request::ProposeGroup {
            specs: vec![proposal("inspect")],
            command_id: command.into(),
            max_running: 1,
        };
        let before = store
            .campaign_ledger(&template.parent.campaign_id)
            .unwrap()
            .unwrap()
            .envelope;
        store
            .broker_control(permit.0, &root.policy.model, request("first"))
            .unwrap();
        let child = store
            .host_catalog
            .lock()
            .unwrap()
            .admitted
            .values()
            .find(|e| e.policy.work.work_id == "child-a")
            .unwrap()
            .clone();
        let child_permit = register(&store, &child);
        assert!(
            store
                .broker_control(child_permit.0, &child.policy.model, request("first"))
                .is_err(),
            "campaign command collision"
        );
        let result = store.broker_control(child_permit.0, &child.policy.model, request("second"));
        assert_eq!(
            result.is_ok(),
            allowed,
            "depth={depth}, inherit={inherit}, total={total}: {result:?}"
        );
        if allowed {
            let actor = WorkAddress {
                campaign_id: template.parent.campaign_id.clone(),
                work_id: "child-a".into(),
            };
            let grandchild = WorkAddress {
                work_id: "child-b".into(),
                ..actor.clone()
            };
            assert_eq!(
                store
                    .host_agent_control(actor)
                    .unwrap()
                    .status(&grandchild)
                    .unwrap()
                    .parent
                    .as_deref(),
                Some("child-a")
            );
            assert!(store
                .host_agent_control(template.parent.clone())
                .unwrap()
                .status(&grandchild)
                .is_err());
            assert!(store
                .broker_control(permit.0, &root.policy.model, request("exhausted"))
                .is_err());
            let groups = store
                .list_campaign_groups(&template.parent.campaign_id, None, 64)
                .unwrap();
            assert_eq!(groups.iter().filter(|g| g.spec.parent.is_some()).count(), 1);
            scheduler.restore_proposals().unwrap();
            let sibling = WorkAddress {
                campaign_id: template.parent.campaign_id.clone(),
                work_id: "sibling".into(),
            };
            store
                .host_admit_agent_work(
                    Admission {
                        work_id: sibling.work_id.clone(),
                        ..template.candidates[0].admission.clone()
                    },
                    Some(template.parent.clone()),
                )
                .unwrap();
            assert!(store
                .host_agent_control(sibling.clone())
                .unwrap()
                .status(&grandchild)
                .is_err());
            store
                .host_cancel_work(&template.parent.campaign_id, "child-a", 1)
                .unwrap();
            let sibling_status = store
                .campaign_work_status(&template.parent.campaign_id, &sibling.work_id)
                .unwrap();
            assert!(!sibling_status.terminal && !sibling_status.cancellation_requested);
            store
                .host_cancel_work(&template.parent.campaign_id, "parent", 1)
                .unwrap();
            assert!(
                store
                    .campaign_work_status(&template.parent.campaign_id, "child-a")
                    .unwrap()
                    .cancellation_requested
            );
            let status = store
                .campaign_work_status(&template.parent.campaign_id, "child-b")
                .unwrap();
            assert!(status.terminal && status.cancellation_requested);
        }
        assert_eq!(
            store
                .campaign_ledger(&template.parent.campaign_id)
                .unwrap()
                .unwrap()
                .envelope,
            before
        );
    }
}

#[test]
fn dynamic_atomic_replay_scope_lifetime_and_manual_reapproval() {
    let (dir, store, root, mut template) = setup(WorkLimits {
        max_depth: 1,
        max_running: 1,
        ..limits()
    });
    let p = profile(&mut template, dir.path());
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
    let permit = register(&store, &root);
    scheduler
        .approve_profile(template.clone(), p.clone())
        .unwrap();
    let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
    for request in [
        Request::Group {
            template_id: template.template_id.clone(),
            command_id: "hidden".into(),
            max_running: None,
        },
        Request::Propose {
            profile_id: "unknown".into(),
            objective: "x".into(),
            context_refs: vec![],
            command_id: "unknown".into(),
        },
        Request::Propose {
            profile_id: "inspect".into(),
            objective: "x".repeat(1025),
            context_refs: vec![],
            command_id: "long".into(),
        },
        Request::Propose {
            profile_id: "inspect".into(),
            objective: "x".into(),
            context_refs: vec![tachyon_api::context::ResourceRef {
                kind: tachyon_api::context::ResourceKind::Finding,
                work_id: "foreign".into(),
                id: "unknown".into(),
                version: "1".into(),
            }],
            command_id: "ref".into(),
        },
    ] {
        assert!(store
            .broker_control(permit.0, &root.policy.model, request)
            .is_err());
    }
    assert_eq!(
        before,
        store.campaign_ledger(&template.parent.campaign_id).unwrap()
    );
    let receipts = std::thread::scope(|scope| {
        (0..4)
            .map(|_| {
                scope.spawn(|| {
                    serde_json::to_value(
                        store
                            .broker_control(permit.0, &root.policy.model, group())
                            .unwrap(),
                    )
                    .unwrap()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(receipts.iter().all(|r| *r == receipts[0]));
    let after = store.campaign_ledger(&template.parent.campaign_id).unwrap();
    assert_ne!(before, after);
    for (index, c) in template.candidates.iter().enumerate() {
        assert_eq!(
            std::fs::read(c.workspace.join("inputs/0/approved.txt")).unwrap(),
            b"hello\n"
        );
        assert!(!c.workspace.join("inputs/0/unapproved.txt").exists());
        let admitted = store
            .admitted_work(&template.parent.campaign_id, &c.work.work_id)
            .unwrap();
        assert_ne!(admitted.admission.objective, c.work.objective);
        assert_eq!(
            admitted.admission.objective,
            if index == 0 {
                "Inspect the approved greeting"
            } else {
                "Check the approved line ending"
            }
        );
        assert!(
            store
                .campaign_execution(&template.parent.campaign_id, &c.work.work_id)
                .unwrap()
                .is_none(),
            "receipt precedes execution"
        );
        assert!(
            store
                .admit_catalog(
                    crate::runtime_store::coordination::WorkAddress {
                        campaign_id: template.parent.campaign_id.clone(),
                        work_id: c.work.work_id.clone(),
                    },
                    group()
                )
                .is_err(),
            "no grandchildren or sibling grants"
        );
    }
    let exhausted = Request::Propose {
        profile_id: "inspect".into(),
        objective: "Third".into(),
        context_refs: vec![],
        command_id: "third".into(),
    };
    assert!(store
        .broker_control(permit.0, &root.policy.model, exhausted)
        .is_err());
    let mut conflict = group();
    if let Request::ProposeGroup { specs, .. } = &mut conflict {
        specs[0].objective.push('!');
    }
    assert!(store
        .broker_control(permit.0, &root.policy.model, conflict)
        .is_err());
    assert_eq!(
        after,
        store.campaign_ledger(&template.parent.campaign_id).unwrap()
    );
    let launched_policy = store
        .host_catalog
        .lock()
        .unwrap()
        .admitted
        .values()
        .next()
        .unwrap()
        .policy
        .clone();
    let unknown = serde_json::json!({"schema_version":1, "policy":launched_policy, "phase":"ExecutingUnknown", "candidate":null, "settled":false});
    let tx = store.database.begin_write().unwrap();
    tx.open_table(crate::runtime_store::execution::EXECUTIONS)
        .unwrap()
        .insert(
            launched_policy.work.work_id.as_str(),
            serde_json::to_vec(&unknown).unwrap().as_slice(),
        )
        .unwrap();
    tx.commit().unwrap();
    std::fs::write(
        template.candidates[0].workspace.join("user-artifact"),
        b"retain unknown execution output",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("inputs/approved.txt"),
        "source changed after admission",
    )
    .unwrap();
    drop(scheduler);
    drop(store);
    let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
    let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
    let scheduler = HostScheduler::command_children(broker, 3).unwrap();
    scheduler.approve_profile(template.clone(), p).unwrap();
    scheduler.restore_proposals().unwrap();
    assert_eq!(
        receipts[0],
        serde_json::to_value(
            store
                .admit_catalog(template.parent.clone(), group())
                .unwrap()
        )
        .unwrap()
    );
    assert_eq!(
        after,
        store.campaign_ledger(&template.parent.campaign_id).unwrap()
    );
    assert_eq!(store.host_catalog.lock().unwrap().admitted.len(), 2);
    assert_eq!(
        serde_json::to_value(
            store
                .campaign_execution(&template.parent.campaign_id, &launched_policy.work.work_id)
                .unwrap()
                .unwrap()
        )
        .unwrap()["phase"],
        "ExecutingUnknown"
    );
    assert_eq!(
        std::fs::read(template.candidates[0].workspace.join("user-artifact")).unwrap(),
        b"retain unknown execution output"
    );
}

#[test]
fn dynamic_group_failures_rollback_funds_and_retain_bounded_staging() {
    for failure in [
        "cost",
        "depth",
        "total",
        "digest",
        "symlink",
        "bytes",
        "files",
        "preexisting",
    ] {
        let (dir, store, root, mut template) = setup(WorkLimits {
            max_depth: if failure == "depth" { 0 } else { 1 },
            total_work: if failure == "total" { 4 } else { 16 },
            ..limits()
        });
        let mut p = profile(&mut template, dir.path());
        if failure == "cost" {
            template.candidates[1].admission.upper_bound.cost_micro_usd = 1001;
        }
        if failure == "digest" {
            std::fs::write(dir.path().join("inputs/approved.txt"), "changed").unwrap();
        }
        if failure == "bytes" {
            p.max_input_bytes = 1;
        }
        if failure == "files" {
            p.max_input_files = 0;
        }
        if failure == "symlink" {
            std::fs::remove_file(dir.path().join("inputs/approved.txt")).unwrap();
            std::os::unix::fs::symlink("unapproved.txt", dir.path().join("inputs/approved.txt"))
                .unwrap();
        }
        if failure == "preexisting" {
            let target = template.candidates[1].workspace.parent().unwrap();
            std::fs::create_dir(target).unwrap();
            std::fs::write(target.join("user.txt"), "preserve").unwrap();
        }
        let broker = Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model)));
        let scheduler = HostScheduler::new(broker, vec![root.clone()], 3).unwrap();
        scheduler.approve_profile(template.clone(), p).unwrap();
        let permit = register(&store, &root);
        let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        assert!(
            store
                .broker_control(permit.0, &root.policy.model, group())
                .is_err(),
            "{failure}"
        );
        assert_eq!(
            before,
            store.campaign_ledger(&template.parent.campaign_id).unwrap(),
            "{failure}"
        );
        assert!(store.host_catalog.lock().unwrap().admitted.is_empty());
        assert!(
            std::fs::read_dir(
                &template.candidates[0]
                    .workspace
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
            )
            .unwrap()
            .count()
                <= 2,
            "{failure}"
        );
        if failure == "preexisting" {
            assert_eq!(
                std::fs::read_to_string(
                    template.candidates[1]
                        .workspace
                        .parent()
                        .unwrap()
                        .join("user.txt")
                )
                .unwrap(),
                "preserve"
            );
        }
    }
}

#[test]
fn dynamic_crash_reauthorization_reuses_original_slots_and_budget() {
    use crate::runtime_store::scheduler::snapshot::CRASH;
    for crash in [
        "mkdir",
        "owned",
        "copy",
        "prepared",
        "admission",
        "committed",
    ] {
        let (dir, store, root, mut template) = setup(WorkLimits {
            max_depth: 1,
            ..limits()
        });
        let p = profile(&mut template, dir.path());
        let scheduler = HostScheduler::new(
            Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model))),
            vec![root.clone()],
            3,
        )
        .unwrap();
        scheduler
            .approve_profile(template.clone(), p.clone())
            .unwrap();
        let permit = register(&store, &root);
        let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        CRASH.with(|point| point.set(Some(crash)));
        let error = store
            .broker_control(permit.0, &root.policy.model, group())
            .unwrap_err();
        assert!(error.contains("injected crash"), "{crash}: {error}");
        let after_crash = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        if crash != "committed" {
            assert_eq!(before, after_crash, "{crash}");
        }
        drop(scheduler);
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let scheduler = HostScheduler::command_children(
            Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model))),
            3,
        )
        .unwrap();
        scheduler.approve_profile(template.clone(), p).unwrap();
        let recovered = scheduler.restore_proposals();
        if crash == "mkdir" {
            assert!(
                recovered.is_err(),
                "markerless orphan must remain untouched"
            );
            assert_eq!(
                before,
                store.campaign_ledger(&template.parent.campaign_id).unwrap()
            );
            assert_eq!(
                std::fs::read_dir(dir.path().join("managed/.proposal-slot-0"))
                    .unwrap()
                    .count(),
                0
            );
            continue;
        }
        recovered.unwrap_or_else(|e| panic!("{crash}: {e}"));
        let after = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        if crash == "committed" {
            assert_eq!(after_crash, after);
        }
        for c in &template.candidates {
            assert!(store
                .campaign_execution(&template.parent.campaign_id, &c.work.work_id)
                .unwrap()
                .is_none());
            assert_eq!(
                std::fs::read(c.workspace.join("inputs/0/approved.txt")).unwrap(),
                b"hello\n"
            );
        }
        scheduler.restore_proposals().unwrap();
        assert_eq!(
            after,
            store.campaign_ledger(&template.parent.campaign_id).unwrap(),
            "{crash}"
        );
        assert_eq!(store.host_catalog.lock().unwrap().admitted.len(), 2);
    }
}

#[test]
fn dynamic_pending_context_is_not_reread_and_changed_inputs_fail_closed() {
    use crate::runtime_store::scheduler::snapshot::CRASH;
    use tachyon_api::context::{Resource, ResourceKind, ResourceRef};
    for crash in ["copy", "admission"] {
        let (dir, store, root, mut template) = setup(WorkLimits {
            max_depth: 1,
            ..limits()
        });
        let p = profile(&mut template, dir.path());
        let scheduler = HostScheduler::new(
            Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model))),
            vec![root.clone()],
            3,
        )
        .unwrap();
        scheduler
            .approve_profile(template.clone(), p.clone())
            .unwrap();
        let _permit = register(&store, &root);
        // Synthetic host finding, in the durable resource format.
        let reference = ResourceRef {
            kind: ResourceKind::Finding,
            work_id: template.parent.work_id.clone(),
            id: "retained-finding".into(),
            version: "1".into(),
        };
        let resource = Resource {
            reference: reference.clone(),
            occurred_at_ms: Some(1),
            data: serde_json::json!({"claim":"original evidence"}),
        };
        let findings = redb::TableDefinition::<(&str, &str), &[u8]>::new("research_findings_v1");
        let tx = store.database.begin_write().unwrap();
        tx.open_table(findings)
            .unwrap()
            .insert(
                (template.parent.campaign_id.as_str(), reference.id.as_str()),
                serde_json::to_vec(&resource).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        let request = Request::Propose {
            profile_id: "inspect".into(),
            objective: "Inspect exact evidence".into(),
            context_refs: vec![reference.clone()],
            command_id: "context-once".into(),
        };
        let before = store.campaign_ledger(&template.parent.campaign_id).unwrap();
        CRASH.with(|point| point.set(Some(crash)));
        assert!(store
            .admit_catalog(template.parent.clone(), request.clone())
            .unwrap_err()
            .contains("injected crash"));
        let tx = store.database.begin_write().unwrap();
        tx.open_table(findings)
            .unwrap()
            .remove((template.parent.campaign_id.as_str(), reference.id.as_str()))
            .unwrap();
        tx.commit().unwrap();
        std::fs::write(dir.path().join("inputs/approved.txt"), b"changed source").unwrap();
        drop(scheduler);
        drop(store);
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let scheduler = HostScheduler::command_children(
            Arc::new(ModelBroker::new(store.clone(), model(&root.policy.model))),
            3,
        )
        .unwrap();
        scheduler.approve_profile(template.clone(), p).unwrap();
        let recovered = scheduler.restore_proposals();
        if crash == "copy" {
            assert!(recovered.is_err());
            assert_eq!(
                before,
                store.campaign_ledger(&template.parent.campaign_id).unwrap()
            );
        } else {
            recovered.unwrap();
            let catalog = store.host_catalog.lock().unwrap();
            let descriptor = catalog.admitted.values().next().unwrap();
            assert_eq!(
                descriptor
                    .policy
                    .work
                    .constraints
                    .as_ref()
                    .unwrap()
                    .input_context[0],
                resource
            );
            assert_eq!(
                descriptor.policy.work.work_id,
                template.candidates[0].work.work_id
            );
        }
    }
}
