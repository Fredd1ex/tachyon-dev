use super::*;
use crate::runtime_store::groups::WaitMode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tachyon_api::work::{Attention, CompletionProposal, Reply, Request};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl ModelBroker {
    #[cfg(test)]
    pub(super) async fn work_private(
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
        self.work_private_admitted(permit, reservation, request, deadline, &mut execution)
            .await
    }

    pub(super) async fn work_private_admitted(
        &self,
        permit: &ModelPermit,
        reservation: &RequestReservation,
        request: Request,
        deadline: Instant,
        execution: &mut Option<crate::runtime_store::host_capacity::CapacityPermit>,
    ) -> tachyon_model::Result<Reply> {
        if request.validate().is_err() {
            return Ok(Reply::Denied);
        }
        let store = self.store.clone();
        let nonce = permit.0;
        let binding = reservation.clone();
        let start = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let mut state = store
                .model_permits
                .lock()
                .map_err(|_| "permit authority unavailable")?;
            let grant = state.grants.get(&nonce).ok_or("unknown permit")?;
            if !grant.active
                || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                || grant.paused
                || grant.request != binding
                || binding.identity.class != RequestClass::Work
                || state.current.get(&binding.identity.work_id) != Some(&nonce)
                || Instant::now() >= deadline
            {
                return Err("invalid work authority".into());
            }
            let funding = grant.funding.clone();
            let tx = store.database.begin_write().map_err(|e| e.to_string())?;
            RuntimeStore::admitted_funding_in(&tx, &funding)?;
            let revision = RuntimeStore::latest_instruction_revision_in(&tx, &funding.admission)?;
            let recognized =
                RuntimeStore::recognized_instruction_revision_in(&tx, &funding.admission)?;
            let remaining = RuntimeStore::campaign_ledger_in(&tx, &funding.admission.campaign_id)?
                .allocation_available(&funding.dispatch_id)?;
            drop(tx);
            let campaign = &funding.admission.campaign_id;
            let work_id = &funding.admission.work_id;
            let questions = store.work_attention(campaign, work_id)?;
            let status = store.campaign_work_status(campaign, work_id)?;
            match request {
                Request::Status {} => Ok((
                    Some(Reply::Status {
                        objective: funding.admission.objective.clone(),
                        phase: if status.terminal {
                            "terminal"
                        } else if status.cancellation_requested {
                            "cancelling"
                        } else if status.wait.is_some() {
                            "waiting"
                        } else {
                            "running"
                        }
                        .into(),
                        instruction_revision: revision,
                        remaining_tokens: remaining.tokens,
                        remaining_cost_micro_usd: remaining.cost_micro_usd,
                        pending_questions: questions
                            .into_iter()
                            .filter(|q| {
                                q.answer.is_none()
                                    && now_ms() < q.deadline_ms
                                    && q.instruction_revision == revision
                            })
                            .collect(),
                    }),
                    funding,
                    0,
                    String::new(),
                    deadline,
                )),
                Request::Complete {
                    summary,
                    candidate_refs,
                    unresolved_questions,
                } => {
                    if recognized != Some(revision) {
                        return Err("completion requires latest delivered model revision".into());
                    }
                    Ok((
                        Some(Reply::Proposed {
                            proposal: CompletionProposal {
                                summary,
                                candidate_refs,
                                unresolved_questions,
                                instruction_revision: revision,
                            },
                        }),
                        funding,
                        0,
                        String::new(),
                        deadline,
                    ))
                }
                Request::Ask {
                    request_id,
                    question,
                    timeout_ms,
                } => {
                    if recognized != Some(revision) {
                        return Err("question requires latest model revision".into());
                    }
                    if let Some(previous) = questions.iter().find(|q| q.request_id == request_id) {
                        if previous.question != question
                            || previous.timeout_ms != timeout_ms
                            || previous.instruction_revision != revision
                            || previous.generation != funding.admission.generation
                        {
                            return Err("question replay conflict".into());
                        }
                        return Ok((
                            Some(Reply::Answer {
                                request_id,
                                answer: previous.answer.clone(),
                                resumed: true,
                            }),
                            funding,
                            0,
                            String::new(),
                            deadline,
                        ));
                    }
                    let resume_deadline = deadline.min(
                        Instant::now() + Duration::from_millis(timeout_ms) + Duration::from_secs(5),
                    );
                    let wait_ms = timeout_ms.min(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .as_millis() as u64,
                    );
                    let attention = Attention {
                        campaign_id: campaign.clone(),
                        work_id: work_id.clone(),
                        generation: funding.admission.generation,
                        instruction_revision: revision,
                        request_id: request_id.clone(),
                        question,
                        deadline_ms: now_ms() + wait_ms,
                        timeout_ms,
                        answer: None,
                    };
                    let wait = store.suspend_work(
                        &funding,
                        status.wait_revision,
                        vec![],
                        WaitMode::Input(request_id.clone()),
                        attention.deadline_ms,
                        Some(attention.clone()),
                    )?;
                    state.grants.get_mut(&nonce).unwrap().paused = true;
                    if let Ok(mut queue) = store.attention_notifications.lock() {
                        if queue.len() < 64 {
                            queue.push_back(attention);
                        }
                    }
                    Ok((None, funding, wait.revision, request_id, resume_deadline))
                }
            }
        })
        .await
        .map_err(|_| err("work authority task lost"))?;
        let Ok((reply, funding, revision, request_id, resume_deadline)) = start else {
            return Ok(Reply::Denied);
        };
        if let Some(reply) = reply {
            return Ok(reply);
        }
        drop(execution.take());
        loop {
            let store = self.store.clone();
            let polled = funding.clone();
            let id = request_id.clone();
            let poll = tokio::task::spawn_blocking(move || -> Result<_, String> {
                let state = store
                    .model_permits
                    .lock()
                    .map_err(|_| "permit authority unavailable")?;
                let grant = state.grants.get(&nonce).ok_or("unknown permit")?;
                if !grant.active
                    || grant.closed.load(std::sync::atomic::Ordering::Acquire)
                    || !grant.paused
                    || state.current.get(&polled.admission.work_id) != Some(&nonce)
                    || Instant::now() >= resume_deadline
                {
                    return Err("attention cancelled, revoked, or resume deadline elapsed".into());
                }
                let status = store.host_poll_parent_wait(&polled, revision, true)?;
                if !status.resumed {
                    return Ok(None);
                }
                let question = store
                    .work_attention(&polled.admission.campaign_id, &polled.admission.work_id)?
                    .into_iter()
                    .find(|q| q.request_id == id)
                    .ok_or("missing question")?;
                Ok(Some(Reply::Answer {
                    request_id: id,
                    answer: question.answer,
                    resumed: true,
                }))
            })
            .await
            .map_err(|_| err("attention polling task lost"))?;
            match poll {
                Ok(Some(reply)) => {
                    self.resume_execution(nonce, &funding, resume_deadline, execution)
                        .await?;
                    return Ok(reply);
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(20)).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_api::types::{ApiRequest, ApiResponse};

    #[test]
    fn attention_survives_store_reopen_without_resuming_or_refunding() {
        let (dir, store, funding, reservation) = super::super::tests::setup_agents();
        let campaign = &reservation.identity.campaign_id;
        store
            .campaign_ledger_command(
                "fund",
                campaign,
                LedgerCommand::FundAllocation {
                    reservation_id: funding.dispatch_id.clone(),
                    work_id: "work".into(),
                },
            )
            .unwrap();
        let funding = store.admitted_work(campaign, "work").unwrap();
        let attention = Attention {
            campaign_id: campaign.clone(),
            work_id: "work".into(),
            generation: 1,
            instruction_revision: 1,
            request_id: "durable".into(),
            question: "Continue?".into(),
            deadline_ms: now_ms() + 60000,
            timeout_ms: 60000,
            answer: None,
        };
        let wait = store
            .suspend_work(
                &funding,
                0,
                vec![],
                WaitMode::Input("durable".into()),
                attention.deadline_ms,
                Some(attention.clone()),
            )
            .unwrap();
        let ledger = serde_json::to_value(store.campaign_ledger(campaign).unwrap()).unwrap();
        drop(store);
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        assert_eq!(
            store.work_attention(campaign, "work").unwrap(),
            vec![attention]
        );
        let status = store
            .host_poll_parent_wait(&funding, wait.revision, false)
            .unwrap();
        assert!(!status.ready && !status.resumed);
        assert_eq!(
            serde_json::to_value(store.campaign_ledger(campaign).unwrap()).unwrap(),
            ledger
        );
        store
            .attention_request(&ApiRequest::CampaignAttentionAnswer {
                id: campaign.clone(),
                work_id: "work".into(),
                request_id: "durable".into(),
                generation: 1,
                instruction_revision: 1,
                answer: "yes".into(),
            })
            .unwrap();
        let status = store
            .host_poll_parent_wait(&funding, wait.revision, false)
            .unwrap();
        assert!(
            status.ready && !status.resumed,
            "operator input is not authority to recreate or resume a process"
        );
    }

    #[tokio::test]
    async fn attention_is_durable_scoped_idempotent_and_fences_unknown_holds() {
        for ending in [
            "answer", "timeout", "cancel", "revoke", "unknown", "stale", "blocked",
        ] {
            let (_dir, store, funding, reservation) = super::super::tests::setup_agents();
            let campaign = reservation.identity.campaign_id.clone();
            store
                .campaign_ledger_command(
                    "fund",
                    &campaign,
                    LedgerCommand::FundAllocation {
                        reservation_id: funding.dispatch_id.clone(),
                        work_id: funding.admission.work_id.clone(),
                    },
                )
                .unwrap();
            let funding = store.admitted_work(&campaign, "work").unwrap();
            let store = Arc::new(store);
            let permit = store
                .host_issue_model_permit(reservation.clone(), funding.clone(), None)
                .unwrap();
            let broker = ModelBroker::new(
                store.clone(),
                super::super::broker_tests::model(&reservation),
            );
            let accountant = store
                .model_permit_accounting(Some(&permit), "unknown")
                .unwrap();
            let request = Request::Ask {
                request_id: "q1".into(),
                question: "Which option?".into(),
                timeout_ms: 150,
            };
            let deadline = Instant::now() + Duration::from_secs(2);
            let remaining = store
                .campaign_ledger(&campaign)
                .unwrap()
                .unwrap()
                .allocation_available(&funding.dispatch_id)
                .unwrap();
            assert!(matches!(
                broker
                    .work_private(&permit, &reservation, Request::Status {}, deadline)
                    .await
                    .unwrap(),
                Reply::Status {
                    instruction_revision: 1,
                    remaining_tokens,
                    remaining_cost_micro_usd,
                    ..
                } if remaining_tokens == remaining.tokens
                    && remaining_cost_micro_usd == remaining.cost_micro_usd
            ));
            let mut foreign = reservation.clone();
            foreign.identity.work_id = "foreign".into();
            assert!(matches!(
                broker
                    .work_private(&permit, &foreign, Request::Status {}, deadline)
                    .await
                    .unwrap(),
                Reply::Denied
            ));
            if ending == "unknown" {
                accountant.reserve_only(&reservation).unwrap();
                assert!(matches!(
                    broker
                        .work_private(&permit, &reservation, request, deadline)
                        .await
                        .unwrap(),
                    Reply::Denied
                ));
                assert!(store.work_attention(&campaign, "work").unwrap().is_empty());
                assert!(
                    store
                        .campaign_work_status(&campaign, "work")
                        .unwrap()
                        .active
                );
                continue;
            }
            let wait = broker.work_private(&permit, &reservation, request.clone(), deadline);
            let answer = ApiRequest::CampaignAttentionAnswer {
                id: campaign.clone(),
                work_id: "work".into(),
                request_id: "q1".into(),
                generation: 1,
                instruction_revision: 1,
                answer: "42".into(),
            };
            let observe = async {
                while store
                    .campaign_work_status(&campaign, "work")
                    .unwrap()
                    .wait
                    .is_none()
                {
                    assert!(Instant::now() < deadline);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                assert!(
                    !store
                        .campaign_work_status(&campaign, "work")
                        .unwrap()
                        .active
                );
                assert!(accountant.reserve_only(&reservation).is_err());
                drop(store.model_permits.try_lock().unwrap());
                drop(store.database.begin_write().unwrap());
                let list = ApiRequest::CampaignAttentionList {
                    id: campaign.clone(),
                    after: None,
                    limit: 32,
                };
                assert!(
                    matches!(store.attention_request(&list).unwrap(), ApiResponse::CampaignAttentionList { questions, .. } if questions.len() == 1)
                );
                for field in [
                    "id",
                    "work_id",
                    "request_id",
                    "generation",
                    "instruction_revision",
                ] {
                    let mut value = serde_json::to_value(&answer).unwrap();
                    value[field] = if matches!(field, "generation" | "instruction_revision") {
                        serde_json::json!(99)
                    } else {
                        serde_json::json!("wrong")
                    };
                    let wrong: ApiRequest = serde_json::from_value(value).unwrap();
                    assert!(store.attention_request(&wrong).is_err(), "{field}");
                }
                match ending {
                    "answer" => {
                        store.attention_request(&answer).unwrap();
                        store.attention_request(&answer).unwrap();
                        let mut conflict = answer.clone();
                        if let ApiRequest::CampaignAttentionAnswer { answer, .. } = &mut conflict {
                            *answer = "43".into();
                        }
                        assert!(store.attention_request(&conflict).is_err());
                    }
                    "cancel" => store.host_cancel_work(&campaign, "work", 1).unwrap(),
                    "revoke" => store.host_revoke_model_permit(&permit).unwrap(),
                    "stale" => {
                        // Inject a host-accepted revision into this root-only
                        // fixture; production parent steering is tested separately.
                        let tx = store.database.begin_write().unwrap();
                        {
                            let mut table = tx
                                .open_table(TableDefinition::<&str, &[u8]>::new(
                                    "campaign_coordination_v1",
                                ))
                                .unwrap();
                            let mut root: serde_json::Value = serde_json::from_slice(
                                table.get(campaign.as_str()).unwrap().unwrap().value(),
                            )
                            .unwrap();
                            root["members"]["work"]["accepted_revision"] = serde_json::json!(2);
                            table
                                .insert(
                                    campaign.as_str(),
                                    serde_json::to_vec(&root).unwrap().as_slice(),
                                )
                                .unwrap();
                        }
                        tx.commit().unwrap();
                        assert!(store.attention_request(&answer).is_err());
                    }
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
            if matches!(ending, "cancel" | "revoke" | "stale" | "blocked") {
                assert!(reply.is_err());
                assert!(
                    store
                        .campaign_work_status(&campaign, "work")
                        .unwrap()
                        .cancellation_requested
                );
                assert!(store.attention_request(&answer).is_err());
            } else {
                assert!(
                    matches!(reply.unwrap(), Reply::Answer { answer, resumed: true, .. } if answer.as_deref() == if ending == "answer" { Some("42") } else { None })
                );
                assert!(
                    store
                        .campaign_work_status(&campaign, "work")
                        .unwrap()
                        .active
                );
                assert!(matches!(
                    broker
                        .work_private(&permit, &reservation, request, deadline)
                        .await
                        .unwrap(),
                    Reply::Answer { resumed: true, .. }
                ));
                assert_eq!(store.attention_notifications.lock().unwrap().len(), 1);
                assert!(matches!(
                    broker
                        .work_private(
                            &permit,
                            &reservation,
                            Request::Ask {
                                request_id: "q1".into(),
                                question: "Different question?".into(),
                                timeout_ms: 150,
                            },
                            deadline
                        )
                        .await
                        .unwrap(),
                    Reply::Denied
                ));
                if ending == "timeout" {
                    assert!(store.attention_request(&answer).is_err());
                }
                let complete = Request::Complete {
                    summary: "candidate".into(),
                    candidate_refs: vec![],
                    unresolved_questions: vec!["review needed".into()],
                };
                assert!(
                    matches!(broker.work_private(&permit, &reservation, complete, deadline).await.unwrap(), Reply::Proposed { proposal } if proposal.summary == "candidate" && proposal.instruction_revision == 1)
                );
            }
        }
    }
}
