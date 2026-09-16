use super::*;
use crate::runtime_store::{
    admission::DispatchOutcome,
    coordination::{ControlCommand, WorkAddress},
};

pub(super) fn setup() -> (
    tempfile::TempDir,
    RuntimeStore,
    AdmittedWork,
    RequestReservation,
    WorkAddress,
) {
    let (dir, store, parent, mut request) = tests::setup_agents();
    let actor = WorkAddress {
        campaign_id: parent.admission.campaign_id.clone(),
        work_id: "work".into(),
    };
    // Fixture registrations have no processes; release an unrelated slot.
    store
        .host_acknowledge_work_terminal(
            &store.admitted_work(&actor.campaign_id, "stranger").unwrap(),
        )
        .unwrap();
    let mut admission = parent.admission;
    admission.work_id = "receiver".into();
    admission.upper_bound = Units {
        tokens: 60,
        cost_micro_usd: 60,
    };
    store
        .host_admit_agent_work(admission.clone(), Some(actor.clone()))
        .unwrap();
    store
        .dispatch_campaign_batch(1, |_| DispatchOutcome::Registered {
            worker_id: "receiver-worker".into(),
        })
        .unwrap();
    let funding = store
        .admitted_work(&admission.campaign_id, &admission.work_id)
        .unwrap();
    request.identity.work_id = admission.work_id;
    request.estimate.max_request_bytes = 32000;
    (dir, store, funding, request, actor)
}

#[test]
fn atomic_application_delivery_reopen_and_same_funding() {
    let (dir, store, funding, request, actor) = setup();
    let target = WorkAddress {
        campaign_id: actor.campaign_id.clone(),
        work_id: "receiver".into(),
    };
    let control = store.host_agent_control(actor.clone()).unwrap();
    control
        .command(
            &target,
            "message",
            ControlCommand::Send {
                text: "untrusted evidence".into(),
            },
        )
        .unwrap();
    for revision in 1..=2 {
        control
            .command(
                &target,
                &format!("steer-{revision}"),
                ControlCommand::Steer {
                    expected_revision: revision,
                    instructions: format!("instructions-{revision}"),
                },
            )
            .unwrap();
    }
    let old = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    let before = store.campaign_ledger(&actor.campaign_id).unwrap().unwrap();
    let (new, revised, boundary) = store
        .prepare_model_boundary(
            old.0,
            request.clone(),
            "boundary",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let boundary = boundary.unwrap();
    assert_eq!(boundary.instruction_revision, 3);
    assert_eq!(boundary.instructions.as_deref(), Some("instructions-2"));
    assert_eq!(boundary.messages.len(), 1);
    let status = control.status(&target).unwrap();
    assert_eq!(
        (
            status.accepted_revision,
            status.acknowledged_revision,
            status.delivered_revision,
            status.delivery_cursor
        ),
        (3, 3, 0, 0)
    );
    assert!(store
        .model_permit_accounting(Some(&old), "stale")
        .unwrap()
        .reserve_only(&request)
        .is_err());
    assert!(store
        .acknowledge_model_boundary(new.0, "wrong", boundary.cursor)
        .is_err());
    // The cursor counts raw commands (including steering), not rendered messages.
    assert_eq!(boundary.cursor, 3);
    assert!(store
        .acknowledge_model_boundary(new.0, "boundary", boundary.messages.len() as u64)
        .is_err());
    store
        .acknowledge_model_boundary(new.0, "boundary", boundary.cursor)
        .unwrap();
    store
        .acknowledge_model_boundary(new.0, "boundary", boundary.cursor)
        .unwrap();
    assert_eq!(control.status(&target).unwrap().delivered_revision, 3);
    assert_eq!(
        store.admitted_work(&actor.campaign_id, "receiver").unwrap(),
        funding
    );
    assert!(store
        .prepare_model_boundary(
            new.0,
            revised.clone(),
            "boundary",
            Instant::now() + std::time::Duration::from_secs(10)
        )
        .is_err());
    let after = store.campaign_ledger(&actor.campaign_id).unwrap().unwrap();
    assert_eq!(before.envelope, after.envelope);
    assert_eq!(
        after
            .allocation_available(&funding.dispatch_id)
            .unwrap()
            .tokens,
        30
    );
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    assert!(store
        .host_issue_model_permit(request, funding.clone(), None)
        .is_err());
    let resumed = store
        .host_issue_model_permit(revised.clone(), funding.clone(), None)
        .unwrap();
    let (_, _, context) = store
        .prepare_model_boundary(
            resumed.0,
            revised,
            "explicit-resume",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let context = context.unwrap();
    assert!(context.messages.is_empty());
    assert_eq!(context.context_messages, boundary.context_messages);
    assert_eq!(context.instructions, boundary.instructions);
    assert_eq!(
        store
            .host_agent_control(actor)
            .unwrap()
            .messages(0, 32)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn crash_after_apply_before_delivery_reassembles_without_replaying_request() {
    let (dir, store, funding, request, actor) = setup();
    let target = WorkAddress {
        campaign_id: actor.campaign_id.clone(),
        work_id: "receiver".into(),
    };
    let control = store.host_agent_control(actor.clone()).unwrap();
    control
        .command(
            &target,
            "data",
            ControlCommand::Send {
                text: "retain across lost boundary".into(),
            },
        )
        .unwrap();
    control
        .command(
            &target,
            "steer",
            ControlCommand::Steer {
                expected_revision: 1,
                instructions: "persist before HTTP".into(),
            },
        )
        .unwrap();
    let old = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    let (_, revised, boundary) = store
        .prepare_model_boundary(
            old.0,
            request,
            "lost",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let boundary = boundary.unwrap();
    drop(store);
    let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
    let status = store
        .host_agent_control(actor)
        .unwrap()
        .status(&target)
        .unwrap();
    assert_eq!(
        (
            status.acknowledged_revision,
            status.delivered_revision,
            status.delivery_cursor
        ),
        (2, 0, 0)
    );
    let permit = store
        .host_issue_model_permit(revised.clone(), funding.clone(), None)
        .unwrap();
    assert!(store
        .prepare_model_boundary(
            permit.0,
            revised.clone(),
            "lost",
            Instant::now() + std::time::Duration::from_secs(10)
        )
        .is_err());
    let (_, _, resumed) = store
        .prepare_model_boundary(
            permit.0,
            revised,
            "explicit-resume",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let resumed = resumed.unwrap();
    assert_eq!(resumed.messages, boundary.messages);
    assert_eq!(resumed.context_messages, boundary.context_messages);
    assert_eq!(resumed.instructions, boundary.instructions);
    assert!(store
        .acknowledge_model_boundary(permit.0, "lost", boundary.cursor)
        .is_err());
    store
        .acknowledge_model_boundary(permit.0, "explicit-resume", resumed.cursor)
        .unwrap();
    assert_eq!(
        store
            .admitted_work(&target.campaign_id, "receiver")
            .unwrap(),
        funding
    );
}

#[test]
fn bounded_pages_stale_ack_pause_revoke_and_cancel_fences() {
    let (_dir, store, funding, request, actor) = setup();
    let target = WorkAddress {
        campaign_id: actor.campaign_id.clone(),
        work_id: "receiver".into(),
    };
    let control = store.host_agent_control(actor).unwrap();
    for n in 0..33 {
        control
            .command(
                &target,
                &format!("send-{n}"),
                ControlCommand::Send {
                    text: format!("data-{n}"),
                },
            )
            .unwrap();
    }
    let permit = store
        .host_issue_model_permit(request.clone(), funding.clone(), None)
        .unwrap();
    store
        .model_permits
        .lock()
        .unwrap()
        .grants
        .get_mut(&permit.0)
        .unwrap()
        .paused = true;
    assert!(store
        .prepare_model_boundary(
            permit.0,
            request.clone(),
            "paused",
            Instant::now() + std::time::Duration::from_secs(10)
        )
        .is_err());
    store
        .model_permits
        .lock()
        .unwrap()
        .grants
        .get_mut(&permit.0)
        .unwrap()
        .paused = false;
    let (_, _, first) = store
        .prepare_model_boundary(
            permit.0,
            request.clone(),
            "first",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let first = first.unwrap();
    assert_eq!((first.messages.len(), first.cursor), (32, 32));
    store
        .acknowledge_model_boundary(permit.0, "first", 32)
        .unwrap();
    let (_, _, second) = store
        .prepare_model_boundary(
            permit.0,
            request.clone(),
            "second",
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap();
    let second = second.unwrap();
    assert_eq!(
        (
            second.messages.len(),
            second.cursor,
            second.context_messages.len()
        ),
        (1, 33, 32)
    );
    assert!(store
        .acknowledge_model_boundary(permit.0, "first", 32)
        .is_err());
    store
        .host_cancel_work(&target.campaign_id, &target.work_id, 1)
        .unwrap();
    assert!(store
        .acknowledge_model_boundary(permit.0, "second", 33)
        .is_err());
    store.host_revoke_model_permit(&permit).unwrap();
    assert!(store
        .prepare_model_boundary(
            permit.0,
            request,
            "revoked",
            Instant::now() + std::time::Duration::from_secs(10)
        )
        .is_err());
    assert_eq!(control.status(&target).unwrap().delivery_cursor, 32);
}

#[test]
fn no_capacity_leaves_steering_and_messages_pending() {
    let (_dir, store, funding, request, actor) = setup();
    let target = WorkAddress {
        campaign_id: actor.campaign_id.clone(),
        work_id: "receiver".into(),
    };
    let permit = store
        .host_issue_model_permit(request.clone(), funding, None)
        .unwrap();
    for id in ["hold-1", "hold-2"] {
        store
            .model_permit_accounting(Some(&permit), id)
            .unwrap()
            .reserve_only(&request)
            .unwrap();
    }
    let control = store.host_agent_control(actor).unwrap();
    control
        .command(
            &target,
            "pending",
            ControlCommand::Steer {
                expected_revision: 1,
                instructions: "pending instructions".into(),
            },
        )
        .unwrap();
    control
        .command(
            &target,
            "send",
            ControlCommand::Send {
                text: "pending data".into(),
            },
        )
        .unwrap();
    let before = store.campaign_ledger(&target.campaign_id).unwrap();
    assert!(store
        .prepare_model_boundary(
            permit.0,
            request,
            "denied",
            Instant::now() + std::time::Duration::from_secs(10)
        )
        .is_err());
    let status = control.status(&target).unwrap();
    assert_eq!(
        (
            status.accepted_revision,
            status.acknowledged_revision,
            status.delivery_cursor
        ),
        (2, 1, 0)
    );
    assert_eq!(before, store.campaign_ledger(&target.campaign_id).unwrap());
}

#[cfg(target_os = "linux")]
#[test]
fn pending_steering_fences_old_evidence_without_trusting_worker_revision() {
    use crate::runtime_store::execution::{
        Evaluation, ExecutionPhase, ExecutionRecord, EXECUTIONS,
    };
    use tachyon_api::types::EventEnvelope;
    let (_dir, store, funding, request, actor) = setup();
    let target = WorkAddress {
        campaign_id: actor.campaign_id.clone(),
        work_id: "receiver".into(),
    };
    let work = serde_json::json!({"work_id":"receiver", "objective":funding.admission.objective,
        "generation":1, "assignment":1, "deadline_ms":1, "lifetime_class":"short"});
    let previous: ExecutionRecord = serde_json::from_value(serde_json::json!({
        "schema_version":1,
        "policy":{"funding":funding,"model":request,"work":work,
            "verification":funding,"evaluator_id":"unused-fixture"},
        "phase":"ExecutingUnknown","candidate":null,"settled":false
    }))
    .unwrap();
    let control = store.host_agent_control(actor).unwrap();
    control
        .command(
            &target,
            "pending",
            ControlCommand::Steer {
                expected_revision: 1,
                instructions: "not yet applied".into(),
            },
        )
        .unwrap();
    for revision in [None, Some(1), Some(2), Some(999)] {
        let tx = store.database.begin_write().unwrap();
        tx.open_table(EXECUTIONS)
            .unwrap()
            .insert(
                "receiver",
                serde_json::to_vec(&previous).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        let event: EventEnvelope = serde_json::from_value(serde_json::json!({
            "event_id":1,"sequence":1,"occurred_at_ms":0,
            "session_id":"receiver-worker","task_id":"receiver-worker",
            "actor":{"kind":"worker","id":"receiver-worker"},
            "kind":"work_candidate","candidate":{
                "work_id":"receiver","objective":funding.admission.objective,
                "generation":1,"assignment":1,"instruction_revision":revision,
                "outcome":"completed","result":"old answer","artifacts":[],
                "context":"","suggested_reuse":false
            }
        }))
        .unwrap();
        let collected = store
            .host_collect_campaign_evidence(&previous, vec![event])
            .unwrap();
        assert_eq!(
            collected.phase,
            ExecutionPhase::Reviewed(Evaluation::Unverified)
        );
        assert_eq!(
            collected.candidate.is_some(),
            revision.is_none() || revision == Some(1)
        );
        let status = control.status(&target).unwrap();
        assert_eq!(
            (status.accepted_revision, status.acknowledged_revision),
            (2, 1)
        );
        assert!(!status.result.unwrap().current);
    }
}
