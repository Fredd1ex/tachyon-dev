use super::*;
use crate::runtime_store::coordination::{ControlCommand, ResultSelection, WorkAddress};
use crate::runtime_store::{
    admission::DispatchState,
    execution::{Evaluation, ExecutionPhase},
};
use tachyon_api::agents::{AdmissionState, Reply, Request, ResultPhase, ResultSnapshot, Status};
use tachyon_model::broker::protocol_error;

impl ModelBroker {
    pub(super) async fn resume_execution(
        &self,
        nonce: uuid::Uuid,
        funding: &AdmittedWork,
        deadline: Instant,
        execution: &mut Option<crate::runtime_store::host_capacity::CapacityPermit>,
    ) -> tachyon_model::Result<()> {
        *execution = Some(
            tokio::time::timeout_at(
                deadline,
                self.store
                    .host_capacity
                    .execution
                    .acquire(&funding.admission.campaign_id),
            )
            .await
            .map_err(|_| err("host execution resume deadline elapsed"))?
            .map_err(err)?,
        );
        let store = self.store.clone();
        let funding = funding.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = store.model_permits.lock().map_err(|_| protocol_error())?;
            let grant = state.grants.get(&nonce).ok_or_else(protocol_error)?;
            if !grant.active
                || !grant.paused
                || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                || state.current.get(&funding.admission.work_id) != Some(&nonce)
                || Instant::now() >= deadline
            {
                return Err(protocol_error());
            }
            let status = store
                .campaign_work_status(&funding.admission.campaign_id, &funding.admission.work_id)
                .map_err(err)?;
            if !status.active
                || status.wait.is_some()
                || status.cancellation_requested
                || status.terminal
            {
                return Err(protocol_error());
            }
            state.grants.get_mut(&nonce).unwrap().paused = false;
            Ok(())
        })
        .await
        .map_err(err)?
    }

    /// The transport is parked, not the Tokio task or a database writer. No PID
    /// supplied by a peer participates in suspension or resident accounting.
    #[cfg(test)]
    pub(super) async fn wait_private(
        &self,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: Request,
        deadline: Instant,
    ) -> tachyon_model::Result<Reply> {
        let mut execution = Some(
            self.store
                .host_capacity
                .execution
                .acquire(&reservation.identity.campaign_id)
                .await
                .map_err(err)?,
        );
        self.wait_private_admitted(permit, reservation, request, deadline, &mut execution)
            .await
    }

    pub(super) async fn wait_private_admitted(
        &self,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: Request,
        deadline: Instant,
        execution: &mut Option<crate::runtime_store::host_capacity::CapacityPermit>,
    ) -> tachyon_model::Result<Reply> {
        use crate::runtime_store::groups::WaitMode;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        if !self
            .allowed_controls
            .contains(&tachyon_api::agents::Control::Wait)
            || request.validate().is_err()
            || self
                .resident_capacity
                .load(std::sync::atomic::Ordering::Acquire)
                < 2
        {
            return Ok(Reply::Denied);
        }
        let Request::Wait {
            work_ids,
            mode,
            timeout_ms,
        } = request
        else {
            return Ok(Reply::Denied);
        };
        // Allow a bounded extra interval for reacquisition. Never return a normal
        // tool result to a worker whose execution lease is still released.
        let resume_deadline = deadline
            .min(Instant::now() + Duration::from_millis(timeout_ms) + Duration::from_secs(5));
        let store = self.store.clone();
        let nonce = permit.0;
        let binding = reservation.clone();
        let start = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let mut state = store
                .model_permits
                .lock()
                .map_err(|_| "permit authority unavailable")?;
            let grant = state.grants.get(&nonce).ok_or("unknown permit")?;
            let identity = &binding.identity;
            if !grant.active
                || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                || grant.paused
                || grant.request != binding
                || identity.class != RequestClass::Work
                || state.current.get(&identity.work_id) != Some(&nonce)
                || Instant::now() >= resume_deadline
            {
                return Err("invalid wait authority".into());
            }
            let control = store.host_agent_control(WorkAddress {
                campaign_id: identity.campaign_id.clone(),
                work_id: identity.work_id.clone(),
            })?;
            for id in &work_ids {
                let target = control.status(&WorkAddress {
                    campaign_id: identity.campaign_id.clone(),
                    work_id: id.clone(),
                })?;
                if target.parent.as_deref() != Some(identity.work_id.as_str()) {
                    return Err("wait requires direct children".into());
                }
            }
            let funding = grant.funding.clone();
            let revision = store
                .campaign_work_status(&identity.campaign_id, &identity.work_id)?
                .wait_revision;
            let mode = match mode {
                tachyon_api::agents::WaitMode::All => WaitMode::All,
                tachyon_api::agents::WaitMode::Any => WaitMode::Any,
                tachyon_api::agents::WaitMode::Count(n) => WaitMode::Count(n),
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            // This transaction checks every outstanding inference hold before
            // releasing the slot. The same authority lock fences reserve/claim.
            let wait =
                store.host_suspend_parent(&funding, revision, work_ids, mode, now + timeout_ms)?;
            state.grants.get_mut(&nonce).unwrap().paused = true;
            Ok((funding, wait.revision))
        })
        .await
        .map_err(|_| err("wait authority task lost"))?;
        let Ok((funding, revision)) = start else {
            return Ok(Reply::Denied);
        };
        // Only a committed suspension releases active capacity; residency stays held.
        drop(execution.take());
        loop {
            let store = self.store.clone();
            let polled_funding = funding.clone();
            let poll = tokio::task::spawn_blocking(move || -> Result<_, String> {
                let funding = polled_funding;
                let state = store
                    .model_permits
                    .lock()
                    .map_err(|_| "permit authority unavailable")?;
                let grant = state.grants.get(&nonce).ok_or("unknown permit")?;
                if !grant.active
                    || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                    || !grant.paused
                    || state.current.get(&funding.admission.work_id) != Some(&nonce)
                    || Instant::now() >= resume_deadline
                {
                    return Err("wait cancelled, revoked, or resume deadline elapsed".into());
                }
                let status = store.host_poll_parent_wait(&funding, revision, true)?;
                Ok(status)
            })
            .await
            .map_err(|_| err("wait polling task lost"))?;
            match poll {
                Ok(status) if status.resumed => {
                    self.resume_execution(nonce, &funding, resume_deadline, execution)
                        .await?;
                    return Ok(Reply::Wait {
                        completed: status.completed,
                        outstanding: status.outstanding,
                        resumed: true,
                        resource_blocked: status.resource_blocked,
                    });
                }
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => {
                    let store = self.store.clone();
                    tokio::task::spawn_blocking(move || {
                        store.host_cancel_work(
                            &funding.admission.campaign_id,
                            &funding.admission.work_id,
                            funding.admission.generation,
                        )
                    })
                    .await
                    .map_err(err)?
                    .map_err(err)?;
                    return Err(err(error));
                }
            }
        }
    }
}

impl RuntimeStore {
    pub(super) fn broker_control(
        &self,
        permit: uuid::Uuid,
        reservation: &RequestReservation,
        request: Request,
    ) -> Result<Reply, String> {
        self.broker_control_with_context(permit, reservation, request, None)
    }

    pub(super) fn broker_control_with_context(
        &self,
        permit: uuid::Uuid,
        reservation: &RequestReservation,
        request: Request,
        artifacts: Option<&tachyond::artifact_store::ArtifactStore>,
    ) -> Result<Reply, String> {
        request.validate()?;
        // Same lock ordering as permit issuance/revocation. Hold authority through
        // the synchronous store operation, never across await or a nested writer.
        let state = self
            .model_permits
            .lock()
            .map_err(|_| "permit authority unavailable")?;
        let grant = state.grants.get(&permit).ok_or("unknown permit")?;
        let identity = &grant.request.identity;
        if !grant.active
            || grant.closed.load(std::sync::atomic::Ordering::Acquire)
            || grant.paused
            || identity.class != RequestClass::Work
            || &grant.request != reservation
            || state.current.get(&identity.work_id) != Some(&permit)
        {
            return Err("revoked or mismatched permit".into());
        }
        if matches!(&request, Request::Todo { .. } | Request::Monitor { .. }) {
            use tachyon_api::{
                agents::services::Scope,
                monitor::{MonitorQuery, MonitorScope},
                todo::TodoScope,
            };
            let tx = self.database.begin_write().map_err(|e| e.to_string())?;
            Self::admitted_funding_in(&tx, &grant.funding)?;
            drop(tx);
            return Ok(match request {
                Request::Todo { request } => {
                    let scope = match request.scope() {
                        Scope::CurrentWork => TodoScope::Work {
                            work_id: identity.work_id.clone(),
                        },
                        Scope::CurrentCampaign => TodoScope::Campaign {
                            campaign_id: identity.campaign_id.clone(),
                        },
                    };
                    let result = self
                        .todos(crate::runtime_store::todo::TodoAuthority::Ghost {
                            scope: scope.clone(),
                            work_id: identity.work_id.clone(),
                            campaign_id: identity.campaign_id.clone(),
                        })
                        .and_then(|facade| facade.execute(request.bind(scope.clone())))
                        .map_err(|error| match error {
                            tachyon_api::todo::TodoError::Storage { .. } => {
                                tachyon_api::todo::TodoError::Storage {
                                    message: "todo storage unavailable".into(),
                                }
                            }
                            error => error,
                        });
                    Reply::Todo { scope, result }
                }
                Request::Monitor {
                    request:
                        tachyon_api::agents::services::MonitorRequest::Snapshot {
                            scope,
                            after,
                            limit,
                        },
                } => {
                    let scope = match scope {
                        Scope::CurrentWork => MonitorScope::Work {
                            campaign_id: identity.campaign_id.clone(),
                            work_id: identity.work_id.clone(),
                        },
                        Scope::CurrentCampaign => MonitorScope::Campaign {
                            campaign_id: identity.campaign_id.clone(),
                        },
                    };
                    let query = MonitorQuery {
                        scope,
                        after,
                        limit,
                    };
                    let result = self
                        .monitor_sample(std::slice::from_ref(&query))
                        .map_err(|_| tachyon_api::monitor::MonitorError::Unavailable)
                        .and_then(|mut samples| samples.remove(0));
                    Reply::Monitor { query, result }
                }
                _ => unreachable!(),
            });
        }
        if let Request::Resource { request } = &request {
            let tx = self.database.begin_write().map_err(|e| e.to_string())?;
            Self::admitted_funding_in(&tx, &grant.funding)?;
            drop(tx);
            return Ok(Reply::Resource {
                page: self.host_research_context(&identity.campaign_id, request, artifacts)?,
            });
        }
        let address = |work_id: String| WorkAddress {
            campaign_id: identity.campaign_id.clone(),
            work_id,
        };
        let control = self.host_agent_control(address(identity.work_id.clone()))?;
        let resize = match &request {
            Request::GroupResize {
                expected_revision,
                max_running,
                ..
            } => Some((*expected_revision, *max_running)),
            _ => None,
        };
        Ok(match request {
            Request::Templates { after, limit } => {
                self.catalog_templates(&address(identity.work_id.clone()), after.as_deref(), limit)?
            }
            Request::Resource { .. } | Request::Todo { .. } | Request::Monitor { .. } => {
                unreachable!()
            }
            Request::Wait { .. } => return Err("wait requires asynchronous host boundary".into()),
            request @ (Request::Spawn { .. } | Request::Group { .. }) => {
                self.admit_catalog(address(identity.work_id.clone()), request)?
            }
            request @ (Request::Propose { .. } | Request::ProposeGroup { .. }) => self
                .admit_catalog_with_context(
                    address(identity.work_id.clone()),
                    request,
                    artifacts,
                )?,
            Request::Cancel {
                work_id,
                generation,
            } => {
                let status = control.status(&address(work_id.clone()))?;
                if status.parent.as_deref() != Some(identity.work_id.as_str()) {
                    return Err("only direct parent may cancel".into());
                }
                self.host_cancel_work(&identity.campaign_id, &work_id, generation)?;
                Reply::CancellationRequested {
                    work_id,
                    generation,
                }
            }
            Request::GroupStatus { group_id } | Request::GroupResize { group_id, .. } => {
                let (mut group, members, active) =
                    self.campaign_group_status(&identity.campaign_id, &group_id)?;
                // Group ancestry is not authority. All affected members must be
                // explicitly enrolled direct children of this exact actor.
                for member in &members {
                    let s = control.status(&address(member.work.admission.work_id.clone()))?;
                    if s.parent.as_deref() != Some(identity.work_id.as_str()) {
                        return Err("group ownership denied".into());
                    }
                }
                if let Some((revision, max_running)) = resize {
                    group = self.resize_campaign_group(
                        &identity.campaign_id,
                        &group_id,
                        revision,
                        max_running,
                    )?;
                }
                Reply::Group {
                    group_id,
                    revision: group.revision,
                    max_running: group.max_running,
                    active,
                    total: members.len(),
                    cancellation_requested: group.cancellation_requested,
                }
            }
            Request::Status { work_id } => {
                let s = control.status(&address(work_id))?;
                let admission = match s.admission {
                    DispatchState::Admitted => AdmissionState::Admitted,
                    DispatchState::DispatchingUnknown => AdmissionState::DispatchingUnknown,
                    DispatchState::Registered { .. } => AdmissionState::Registered,
                    DispatchState::ConfirmedUnspent => AdmissionState::ConfirmedUnspent,
                    DispatchState::Cancelled => AdmissionState::Cancelled,
                };
                Reply::Status {
                    status: Status {
                        work_id: s.address.work_id,
                        parent: s.parent,
                        admission,
                        generation: s.generation,
                        execution_revision: s.execution_revision,
                        accepted_revision: s.accepted_revision,
                        acknowledged_revision: s.acknowledged_revision,
                        delivered_revision: s.delivered_revision,
                        delivery_cursor: s.delivery_cursor,
                    },
                }
            }
            Request::List { after, limit } => {
                let page = control.list(after.as_deref(), limit)?;
                Reply::List {
                    work_ids: page.items.into_iter().map(|a| a.work_id).collect(),
                    next_cursor: page.next_cursor,
                }
            }
            Request::Result { work_id, revision } => {
                let result = control.result(
                    &address(work_id),
                    revision
                        .map(ResultSelection::Revision)
                        .unwrap_or(ResultSelection::Current),
                )?;
                Reply::Result {
                    snapshot: result.map(|r| {
                        let mut snapshot = ResultSnapshot {
                            work_id: r.work.work_id,
                            revision: r.revision,
                            generation: r.generation,
                            current: r.current,
                            phase: if r.rework_pending {
                                ResultPhase::ReworkPending
                            } else {
                                match r.phase {
                                    ExecutionPhase::ExecutingUnknown => {
                                        ResultPhase::ExecutingUnknown
                                    }
                                    ExecutionPhase::EvidenceReady => ResultPhase::EvidenceReady,
                                    ExecutionPhase::AwaitingVerification => {
                                        ResultPhase::AwaitingVerification
                                    }
                                    ExecutionPhase::AwaitingAcceptance => {
                                        ResultPhase::AwaitingAcceptance
                                    }
                                    ExecutionPhase::Reviewed(Evaluation::AcceptedHuman) => {
                                        ResultPhase::AcceptedHuman
                                    }
                                    ExecutionPhase::ReviewingUnknown => {
                                        ResultPhase::ReviewingUnknown
                                    }
                                    ExecutionPhase::Reviewed(Evaluation::Accepted) => {
                                        ResultPhase::Accepted
                                    }
                                    ExecutionPhase::Reviewed(Evaluation::Rejected) => {
                                        ResultPhase::Rejected
                                    }
                                    ExecutionPhase::Reviewed(Evaluation::Unverified) => {
                                        ResultPhase::Unverified
                                    }
                                }
                            },
                            has_candidate: r.has_candidate,
                            settled: r.settled,
                            research: r.research,
                        };
                        snapshot.bound();
                        snapshot
                    }),
                }
            }
            Request::Send {
                work_id,
                command_id,
                text,
            } => {
                let m = control.command(
                    &address(work_id),
                    &command_id,
                    ControlCommand::Send { text },
                )?;
                Reply::Accepted {
                    command_id: m.command_id,
                    sequence: m.sequence,
                    accepted_revision: m.accepted_revision,
                }
            }
            Request::Steer {
                work_id,
                command_id,
                expected_revision,
                instructions,
            } => {
                let m = control.command(
                    &address(work_id),
                    &command_id,
                    ControlCommand::Steer {
                        expected_revision,
                        instructions,
                    },
                )?;
                Reply::Accepted {
                    command_id: m.command_id,
                    sequence: m.sequence,
                    accepted_revision: m.accepted_revision,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;
    use tachyon_api::agents::Control;

    #[tokio::test]
    async fn shared_execution_wait_releases_and_reacquires_before_reply() {
        for revoked in [false, true] {
            let (_dir, mut store, funding, reservation) = super::super::tests::setup_agents();
            store.host_capacity = crate::runtime_store::host_capacity::HostCapacity::new(
                tachyon_util::config::ResourceLimits {
                    max_execution_jobs: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            let campaign = &reservation.identity.campaign_id;
            store
                .campaign_ledger_command(
                    "fund",
                    campaign,
                    LedgerCommand::FundAllocation {
                        reservation_id: funding.dispatch_id.clone(),
                        work_id: funding.admission.work_id.clone(),
                    },
                )
                .unwrap();
            let funding = store.admitted_work(campaign, "work").unwrap();
            let store = Arc::new(store);
            let permit = store
                .host_issue_model_permit(reservation.clone(), funding, None)
                .unwrap();
            let broker = ModelBroker::new(
                store.clone(),
                super::super::broker_tests::model(&reservation),
            )
            .with_controls([Control::Wait]);
            let resident = store
                .host_capacity
                .resident
                .acquire(campaign)
                .await
                .unwrap();
            let mut execution = Some(
                store
                    .host_capacity
                    .execution
                    .acquire(campaign)
                    .await
                    .unwrap(),
            );
            let mut wait = Box::pin(broker.wait_private_admitted(
                &permit,
                &reservation,
                Request::Wait {
                    work_ids: vec!["child".into()],
                    mode: tachyon_api::agents::WaitMode::All,
                    timeout_ms: 100,
                },
                Instant::now() + std::time::Duration::from_secs(5),
                &mut execution,
            ));
            let sibling = tokio::select! {
                result = &mut wait => panic!("wait returned before suspension: {result:?}"),
                result = store.host_capacity.execution.acquire("other-campaign") => result.unwrap(),
            };
            assert_eq!(
                store.host_capacity.resident.available(),
                store.host_capacity.limits.max_resident_workers - 1
            );
            assert_eq!(
                store.host_capacity.model.available(),
                store.host_capacity.limits.max_model_calls
            );
            // The durable running lease can resume, but no normal reply may cross the
            // transport until the independent host execution queue also admits it.
            tokio::select! {
                result = &mut wait => panic!("reply escaped execution admission: {result:?}"),
                result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    while store.host_capacity.execution.waiting(campaign).unwrap() == 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                }) => result.unwrap(),
            }
            assert_eq!(store.host_capacity.execution.waiting(campaign).unwrap(), 1);
            assert!(
                store
                    .model_permits
                    .lock()
                    .unwrap()
                    .grants
                    .get(&permit.0)
                    .unwrap()
                    .paused
            );
            if revoked {
                store.host_revoke_model_permit(&permit).unwrap();
            }
            drop(sibling);
            let reply = wait.await;
            if revoked {
                assert!(reply.is_err());
            } else {
                assert!(matches!(reply.unwrap(), Reply::Wait { resumed: true, .. }));
            }
            assert!(execution.is_some());
            assert_eq!(store.host_capacity.execution.available(), 0);
            drop(execution);
            drop(resident);
            assert_eq!(store.host_capacity.execution.available(), 1);
        }
    }

    #[tokio::test]
    async fn wait_fences_permits_timeout_resume_cancel_and_unknown_billing() {
        for ending in [
            "timeout", "complete", "revoke", "cancel", "unknown", "blocked",
        ] {
            let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
            let campaign = &reservation.identity.campaign_id;
            store
                .campaign_ledger_command(
                    "fund",
                    campaign,
                    LedgerCommand::FundAllocation {
                        reservation_id: funding.dispatch_id.clone(),
                        work_id: funding.admission.work_id.clone(),
                    },
                )
                .unwrap();
            let funding = store.admitted_work(campaign, "work").unwrap();
            let store = Arc::new(store);
            let permit = store
                .host_issue_model_permit(reservation.clone(), funding.clone(), None)
                .unwrap();
            let broker = ModelBroker::new(
                store.clone(),
                super::super::broker_tests::model(&reservation),
            )
            .with_controls([Control::Wait]);
            let accountant = store
                .model_permit_accounting(Some(&permit), "before-wait")
                .unwrap();
            if ending == "timeout" {
                for id in ["stranger", "work", "missing"] {
                    assert!(matches!(
                        broker
                            .wait_private(
                                &permit,
                                &reservation,
                                Request::Wait {
                                    work_ids: vec![id.into()],
                                    mode: tachyon_api::agents::WaitMode::Any,
                                    timeout_ms: 100,
                                },
                                Instant::now() + std::time::Duration::from_secs(1)
                            )
                            .await
                            .unwrap(),
                        Reply::Denied
                    ));
                }
                assert!(store.campaign_work_status(campaign, "work").unwrap().active);
            }
            if ending == "unknown" {
                accountant.reserve_only(&reservation).unwrap();
            }
            let wait = broker.wait_private(
                &permit,
                &reservation,
                Request::Wait {
                    work_ids: vec!["child".into()],
                    mode: tachyon_api::agents::WaitMode::All,
                    timeout_ms: 100,
                },
                Instant::now() + std::time::Duration::from_secs(7),
            );
            if ending == "unknown" {
                assert!(matches!(wait.await.unwrap(), Reply::Denied));
                assert!(store.campaign_work_status(campaign, "work").unwrap().active);
                continue;
            }
            let observe = async {
                let deadline = Instant::now() + std::time::Duration::from_secs(2);
                while store
                    .campaign_work_status(campaign, "work")
                    .unwrap()
                    .wait
                    .is_none()
                {
                    assert!(Instant::now() < deadline);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                assert!(accountant.reserve_only(&reservation).is_err());
                assert!(store
                    .host_issue_model_permit(reservation.clone(), funding.clone(), Some(&permit))
                    .is_err());
                assert!(store
                    .broker_control(
                        permit.0,
                        &reservation,
                        Request::Status {
                            work_id: "work".into()
                        }
                    )
                    .is_err());
                // Neither authority nor storage is retained by the parked future.
                drop(store.model_permits.try_lock().unwrap());
                drop(store.database.begin_write().unwrap());
                match ending {
                    "complete" => store
                        .host_acknowledge_work_terminal(
                            &store.admitted_work(campaign, "child").unwrap(),
                        )
                        .unwrap(),
                    "revoke" => store.host_revoke_model_permit(&permit).unwrap(),
                    "cancel" => store.host_cancel_work(campaign, "work", 1).unwrap(),
                    "blocked" => {
                        store
                            .admit_campaign_work(crate::runtime_store::admission::Admission {
                                work_id: "occupant".into(),
                                upper_bound: Units::default(),
                                ..funding.admission.clone()
                            })
                            .unwrap();
                        assert_eq!(
                            store
                                .dispatch_campaign_batch(1, |_| {
                                    crate::runtime_store::admission::DispatchOutcome::Unknown
                                })
                                .unwrap(),
                            1
                        );
                    }
                    _ => {}
                }
            };
            let (reply, ()) = tokio::join!(wait, observe);
            let status = store.campaign_work_status(campaign, "work").unwrap();
            if matches!(ending, "cancel" | "revoke" | "blocked") {
                assert!(reply.is_err());
                assert!(!status.active && status.wait.is_some() && status.cancellation_requested);
                assert!(accountant.reserve_only(&reservation).is_err());
            } else {
                let Reply::Wait {
                    completed,
                    outstanding,
                    resumed,
                    ..
                } = reply.unwrap()
                else {
                    panic!()
                };
                assert!(resumed && status.active && status.wait.is_none());
                assert_eq!(completed.len(), usize::from(ending == "complete"));
                assert_eq!(outstanding.len(), usize::from(ending == "timeout"));
                assert!(accountant.reserve_only(&reservation).is_ok());
            }
        }
    }

    #[tokio::test]
    async fn private_cancel_signals_the_shared_registry_after_durable_intent() {
        let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
        let store = Arc::new(store);
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding, None)
            .unwrap();
        let broker = ModelBroker::new(
            store.clone(),
            super::super::broker_tests::model(&reservation),
        )
        .with_controls([Control::Cancel]);
        let (cancel, observed) = tokio::sync::watch::channel(false);
        broker.launches.lock().unwrap().insert(
            (reservation.identity.campaign_id.clone(), "child".into(), 1),
            cancel,
        );
        let (host, client) = tachyon_model::broker::private_pair().unwrap();
        let worker = async {
            assert!(matches!(
                client
                    .control(&Request::Cancel {
                        work_id: "child".into(),
                        generation: 1
                    })
                    .await
                    .unwrap(),
                Reply::CancellationRequested { .. }
            ));
            assert!(*observed.borrow());
            let status = store
                .campaign_work_status(&reservation.identity.campaign_id, "child")
                .unwrap();
            assert!(status.cancellation_requested && status.active && !status.terminal);
            drop(client);
        };
        let (_, ()) = tokio::join!(
            broker.serve_private(
                host,
                &permit,
                reservation.clone(),
                Instant::now() + std::time::Duration::from_secs(5)
            ),
            worker
        );
    }

    #[test]
    fn cancellation_and_group_controls_require_exact_direct_ownership() {
        use crate::runtime_store::groups::GroupSpec;
        let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
        let c = reservation.identity.campaign_id.clone();
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding.clone(), None)
            .unwrap();
        for (work_id, generation) in [("stranger", 1), ("work", 1), ("child", 2)] {
            assert!(store
                .broker_control(
                    permit.0,
                    &reservation,
                    Request::Cancel {
                        work_id: work_id.into(),
                        generation
                    }
                )
                .is_err());
        }
        for _ in 0..2 {
            assert!(matches!(
                store
                    .broker_control(
                        permit.0,
                        &reservation,
                        Request::Cancel {
                            work_id: "child".into(),
                            generation: 1
                        }
                    )
                    .unwrap(),
                Reply::CancellationRequested { .. }
            ));
        }
        assert!(store.campaign_work_status(&c, "child").unwrap().active);
        for (group, parent) in [("owned", "work"), ("foreign", "stranger")] {
            let admission = crate::runtime_store::admission::Admission {
                work_id: format!("{group}-member"),
                upper_bound: Units::default(),
                ..funding.admission.clone()
            };
            store
                .create_campaign_group(GroupSpec {
                    group_id: group.into(),
                    campaign_id: c.clone(),
                    parent: None,
                    max_running: 2,
                    work: vec![admission.clone()],
                })
                .unwrap();
            store
                .host_admit_agent_work(
                    admission,
                    Some(WorkAddress {
                        campaign_id: c.clone(),
                        work_id: parent.into(),
                    }),
                )
                .unwrap();
        }
        for request in [
            Request::GroupStatus {
                group_id: "foreign".into(),
            },
            Request::GroupResize {
                group_id: "foreign".into(),
                expected_revision: 1,
                max_running: 0,
            },
        ] {
            assert!(store
                .broker_control(permit.0, &reservation, request)
                .is_err());
        }
        assert!(matches!(
            store
                .broker_control(
                    permit.0,
                    &reservation,
                    Request::GroupResize {
                        group_id: "owned".into(),
                        expected_revision: 1,
                        max_running: 0
                    }
                )
                .unwrap(),
            Reply::Group {
                revision: 2,
                max_running: 0,
                ..
            }
        ));
        assert!(store
            .broker_control(
                permit.0,
                &reservation,
                Request::GroupResize {
                    group_id: "owned".into(),
                    expected_revision: 1,
                    max_running: 2
                }
            )
            .is_err());
        store.host_revoke_model_permit(&permit).unwrap();
        assert!(store
            .broker_control(
                permit.0,
                &reservation,
                Request::GroupStatus {
                    group_id: "owned".into()
                }
            )
            .is_err());
    }

    #[tokio::test]
    async fn controls_recheck_scope_allowlist_identity_revocation_and_replay() {
        let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
        let store = Arc::new(store);
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding, None)
            .unwrap();
        let broker = ModelBroker::new(
            store.clone(),
            super::super::broker_tests::model(&reservation),
        )
        .with_controls([
            Control::Status,
            Control::Result,
            Control::Send,
            Control::Steer,
        ]);
        let mut wrong = reservation.clone();
        wrong.identity.attempt_id = "forged".into();
        assert!(store
            .broker_control(
                permit.0,
                &wrong,
                Request::Status {
                    work_id: "work".into()
                }
            )
            .is_err());
        // Existing execution evidence fixture, not a worker-published result.
        let child = store
            .admitted_work(&reservation.identity.campaign_id, "child")
            .unwrap();
        let mut child_model = reservation.clone();
        child_model.identity.work_id = "child".into();
        let evidence = serde_json::json!({
            "schema_version":1,
            "policy": {"funding":child,"verification":child,"evaluator_id":"fixture",
                "work":{"work_id":"child","objective":"bounded","generation":1,"assignment":1,"deadline_ms":1,"lifetime_class":"short"},
                "model":child_model},
            "phase":"ExecutingUnknown","candidate":null,"settled":false
        });
        let write = store.database.begin_write().unwrap();
        write
            .open_table(crate::runtime_store::execution::EXECUTIONS)
            .unwrap()
            .insert("child", serde_json::to_vec(&evidence).unwrap().as_slice())
            .unwrap();
        write.commit().unwrap();
        let (host, mut client) = tachyon_model::broker::private_pair().unwrap();
        // Child-side advertisement is not host authority.
        client.controls = vec![Control::List];
        let worker = async {
            assert!(matches!(
                client
                    .control(&Request::List {
                        after: None,
                        limit: 1
                    })
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            assert!(matches!(
                client
                    .control(&Request::Status {
                        work_id: "stranger".into()
                    })
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            let Reply::Result {
                snapshot: Some(snapshot),
            } = client
                .control(&Request::Result {
                    work_id: "child".into(),
                    revision: None,
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert!(snapshot.current && !snapshot.has_candidate && !snapshot.settled);
            assert_eq!(snapshot.revision, 1);
            let command = Request::Send {
                work_id: "child".into(),
                command_id: "stable".into(),
                text: "hello".into(),
            };
            for _ in 0..2 {
                assert!(matches!(
                    client.control(&command).await.unwrap(),
                    Reply::Accepted { sequence: 1, .. }
                ));
            }
            assert!(matches!(
                client
                    .control(&Request::Send {
                        work_id: "child".into(),
                        command_id: "stable".into(),
                        text: "changed".into()
                    })
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            assert!(matches!(
                client
                    .control(&Request::Steer {
                        work_id: "child".into(),
                        command_id: "steer".into(),
                        expected_revision: 1,
                        instructions: "new".into()
                    })
                    .await
                    .unwrap(),
                Reply::Accepted {
                    accepted_revision: 2,
                    ..
                }
            ));
            let Reply::Status { status } = client
                .control(&Request::Status {
                    work_id: "child".into(),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(
                (status.accepted_revision, status.acknowledged_revision),
                (2, 1)
            );
            // Pending steering fences current results without applying instructions.
            assert!(matches!(
                client
                    .control(&Request::Result {
                        work_id: "child".into(),
                        revision: None
                    })
                    .await
                    .unwrap(),
                Reply::Result { snapshot: None }
            ));
            store
                .host_ack_agent_steering(
                    &WorkAddress {
                        campaign_id: reservation.identity.campaign_id.clone(),
                        work_id: "child".into(),
                    },
                    2,
                )
                .unwrap();
            let Reply::Result {
                snapshot: Some(snapshot),
            } = client
                .control(&Request::Result {
                    work_id: "child".into(),
                    revision: Some(1),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            assert!(!snapshot.current);
            store.host_revoke_model_permit(&permit).unwrap();
            assert!(matches!(
                client.control(&command).await.unwrap(),
                Reply::Denied
            ));
            drop(client);
        };
        let (result, ()) = tokio::join!(
            broker.serve_private(
                host,
                &permit,
                reservation.clone(),
                Instant::now() + std::time::Duration::from_secs(5)
            ),
            worker
        );
        assert!(result.is_err());
        let write = store.database.begin_write().unwrap();
        assert_eq!(
            write.open_table(DISPATCHES).unwrap().len().unwrap(),
            0,
            "controls never claim model dispatch"
        );
        drop(write);
        drop(broker);
        drop(store);
        let store = RuntimeStore::open(&_dir.path().join("runtime.redb")).unwrap();
        let control = store
            .host_agent_control(WorkAddress {
                campaign_id: reservation.identity.campaign_id,
                work_id: "work".into(),
            })
            .unwrap();
        let messages = control.messages(0, 32).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(
            control
                .command(
                    &messages[0].recipient,
                    "stable",
                    ControlCommand::Send {
                        text: "hello".into()
                    }
                )
                .unwrap(),
            messages[0]
        );
    }

    #[tokio::test]
    async fn invalid_control_permits_cannot_modify_coordination() {
        let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
        let permit = store
            .host_issue_model_permit(reservation.clone(), funding, None)
            .unwrap();
        let control = store
            .host_agent_control(WorkAddress {
                campaign_id: reservation.identity.campaign_id.clone(),
                work_id: "work".into(),
            })
            .unwrap();
        let child = WorkAddress {
            campaign_id: reservation.identity.campaign_id.clone(),
            work_id: "child".into(),
        };
        let before = control.status(&child).unwrap();
        let mut wrong = reservation.clone();
        wrong.identity.instruction_revision += 1;
        for (nonce, binding) in [(uuid::Uuid::new_v4(), &reservation), (permit.0, &wrong)] {
            for request in [
                Request::Send {
                    work_id: "child".into(),
                    command_id: "denied".into(),
                    text: "hello".into(),
                },
                Request::Steer {
                    work_id: "child".into(),
                    command_id: "denied".into(),
                    expected_revision: 1,
                    instructions: "new".into(),
                },
            ] {
                assert!(store.broker_control(nonce, binding, request).is_err());
            }
        }
        store.host_revoke_model_permit(&permit).unwrap();
        assert!(store
            .broker_control(
                permit.0,
                &reservation,
                Request::Steer {
                    work_id: "child".into(),
                    command_id: "revoked".into(),
                    expected_revision: 1,
                    instructions: "new".into(),
                }
            )
            .is_err());
        assert!(control.messages(0, 32).unwrap().is_empty());
        assert_eq!(
            control.status(&child).unwrap().accepted_revision,
            before.accepted_revision
        );
        let write = store.database.begin_write().unwrap();
        assert_eq!(write.open_table(DISPATCHES).unwrap().len().unwrap(), 0);
    }
}
