//! Same-user service for persisted, explicitly conversation-linked launches.
use super::{
    campaign_launch::Launch,
    coordination::{ControlCommand, WorkAddress},
    RuntimeStore,
};
use redb::ReadableTable;
use serde_json::{json, Value};
use tachyon_api::conversation_campaign::Request;

impl RuntimeStore {
    pub(crate) fn conversation_campaign(
        &self,
        conversation: &str,
        request: &Request,
    ) -> Result<Value, String> {
        request.validate()?;
        let tx = self.database.begin_read().map_err(|e| e.to_string())?;
        let table = tx
            .open_table(super::campaign_launch::LAUNCHES)
            .map_err(|e| e.to_string())?;
        let mut linked = Vec::new();
        let mut selected = None;
        let mut truncated = false;
        for row in table.iter().map_err(|e| e.to_string())? {
            let (_, bytes) = row.map_err(|e| e.to_string())?;
            let launch: Launch =
                serde_json::from_slice(bytes.value()).map_err(|e| e.to_string())?;
            if launch
                .manifest
                .oversight
                .as_ref()
                .and_then(|o| o.conversation_id.as_deref())
                != Some(conversation)
            {
                continue;
            }
            launch.validate_digest()?;
            if request.campaign_id() == Some(launch.manifest.campaign_id.as_str()) {
                selected = Some(launch.manifest.clone());
            }
            if linked.len() < 32 {
                linked.push(json!({"campaign_id":launch.manifest.campaign_id,"objective_summary":launch.manifest.objective.chars().take(512).collect::<String>()}));
            } else {
                truncated = true;
            }
        }
        drop(table);
        drop(tx);
        if matches!(request, Request::List {}) {
            return Ok(json!({"campaigns":linked,"truncated":truncated}));
        }
        let manifest = selected.ok_or("campaign is not explicitly linked to this conversation")?;
        let campaign = &manifest.campaign_id;
        // This ID is the launch protocol's root, not a model-selected actor or a
        // guess from a display name. The facade verifies its root enrollment.
        let control = self.conversation_agent_control(WorkAddress {
            campaign_id: campaign.clone(),
            work_id: format!("{campaign}-root"),
        })?;
        let address = |id: &str| WorkAddress {
            campaign_id: campaign.clone(),
            work_id: id.into(),
        };
        match request {
            Request::Status {
                work_id,
                after,
                limit,
                include_plan,
                ..
            } => {
                let (targets, next) = if let Some(id) = work_id {
                    (vec![address(id)], None)
                } else {
                    let page = control.list(after.as_deref(), *limit)?;
                    (page.items, page.next_cursor)
                };
                let mut works = Vec::new();
                for target in targets {
                    let s = control.status(&target)?;
                    let work = self.admitted_work(campaign, &target.work_id)?;
                    let lifecycle = self.campaign_work_status(campaign, &target.work_id)?;
                    let mut cancellation_done = lifecycle.cancellation_requested;
                    if cancellation_done {
                        let tx = self.database.begin_write().map_err(|e| e.to_string())?;
                        let mut branch =
                            Self::agent_descendants_in(&tx, campaign, &target.work_id)?;
                        branch.push(target.work_id.clone());
                        drop(tx);
                        for id in branch {
                            let state = self.campaign_work_status(campaign, &id)?;
                            let execution = control.status(&address(&id))?;
                            cancellation_done &= state.terminal
                                && !state.active
                                && (execution.admission
                                    == super::admission::DispatchState::Cancelled
                                    || execution.result.as_ref().is_some_and(|r| r.settled));
                        }
                    }
                    works.push(json!({"work_id":target.work_id,"parent_work_id":s.parent,
                        "objective_summary":work.admission.objective.chars().take(512).collect::<String>(),
                        "generation":s.generation,"accepted_revision":s.accepted_revision,
                        "applied_revision":s.acknowledged_revision,"delivered_revision":s.delivered_revision,
                        "admission":s.admission,
                        "active":lifecycle.active,"terminal":lifecycle.terminal,
                        "execution_phase":s.result.as_ref().map(|r| &r.phase),
                        "result_current":s.result.as_ref().map(|r| r.current),
                        "result_summary":s.result.as_ref().and_then(|r| r.research.as_ref()).and_then(|r| r.summary.as_ref()).map(|text| text.chars().take(512).collect::<String>()),
                        "cancellation_requested":lifecycle.cancellation_requested,
                        "cancellation_done":cancellation_done,
                    }));
                }
                let tx = self.database.begin_write().map_err(|e| e.to_string())?;
                let status = Self::campaign_status_in(&tx, campaign)?;
                drop(tx);
                let mut response = json!({"campaign_id":campaign,"status":status,"works":works,"next_cursor":next});
                if *include_plan {
                    use tachyon_api::todo::*;
                    let scope = TodoScope::Campaign {
                        campaign_id: campaign.clone(),
                    };
                    let plan = self
                        .todos(super::todo::TodoAuthority::Bound {
                            scope: scope.clone(),
                            actor: TodoActor {
                                source: "operator".into(),
                                actor: conversation.into(),
                            },
                        })
                        .map_err(|e| format!("{e:?}"))?
                        .execute(TodoRequest::List {
                            scope,
                            filter: Default::default(),
                            limit: Some(16),
                            cursor: None,
                        })
                        .map_err(|e| format!("{e:?}"))?;
                    response["plan"] = serde_json::to_value(plan).map_err(|e| e.to_string())?;
                }
                Ok(response)
            }
            Request::Steer {
                work_id,
                command_id,
                expected_revision,
                instructions,
                ..
            } => {
                let receipt = control.command(
                    &address(work_id),
                    command_id,
                    ControlCommand::Steer {
                        expected_revision: *expected_revision,
                        instructions: instructions.clone(),
                    },
                )?;
                Ok(
                    json!({"campaign_id":campaign,"work_id":work_id,"command_id":command_id,"status":"accepted","accepted_revision":receipt.accepted_revision,"instruction":"Accepted, not yet confirmed applied. Query status before claiming Done."}),
                )
            }
            Request::Cancel {
                work_id,
                command_id,
                generation,
                ..
            } => {
                control.cancel(&address(work_id), command_id, *generation)?;
                Ok(
                    json!({"campaign_id":campaign,"work_id":work_id,"command_id":command_id,"generation":generation,"status":"accepted","instruction":"Cancellation intent accepted, not proof of cleanup. Query status before claiming Done."}),
                )
            }
            Request::List {} => unreachable!(),
        }
    }
}
