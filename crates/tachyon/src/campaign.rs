use crate::cli::CampaignAction;
use std::{
    io::Read,
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tachyon_api::{
    campaign::{CampaignManifest, MANIFEST_MAX_BYTES},
    types::{ApiRequest, ApiResponse},
};

impl crate::cli::AcceptanceArgs {
    pub(crate) fn request(
        self,
        decision: tachyon_api::campaign::HumanDecision,
    ) -> Result<ApiRequest, String> {
        let decision = tachyon_api::campaign::AcceptanceDecision {
            command_id: self.command_id,
            campaign_id: self.id,
            candidate: self.candidate,
            candidate_sha256: self.candidate_sha256,
            expected_state_sha256: self.expected_state,
            decision,
            confirm: self.confirm,
        };
        decision.validate()?;
        Ok(ApiRequest::CampaignAcceptanceDecide(decision))
    }
}

pub fn run(action: CampaignAction) -> ExitCode {
    let result = (|| -> Result<(), String> {
        let request = match action {
            CampaignAction::IntegrationSnapshot { id, paths } => {
                ApiRequest::CampaignIntegrationSnapshot { id, paths }
            }
            CampaignAction::Integrate {
                id,
                plan,
                expected_state,
                confirm,
            } => {
                if !confirm {
                    return Err("--confirm is mandatory".into());
                }
                let mut bytes = Vec::new();
                std::fs::File::open(plan)
                    .map_err(|e| e.to_string())?
                    .take(tachyon_api::integration::MAX_PLAN_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| e.to_string())?;
                ApiRequest::CampaignIntegrate {
                    id,
                    plan: tachyon_api::integration::IntegrationPlan::parse(&bytes)?,
                    expected_state,
                    confirm,
                }
            }
            CampaignAction::Archive { id } => ApiRequest::LocalRetentionSet { id, archived: true },
            CampaignAction::Restore { id } => ApiRequest::LocalRetentionSet {
                id,
                archived: false,
            },
            CampaignAction::Retention { id } => ApiRequest::LocalRetentionGet { id },
            CampaignAction::Acceptance { id } => {
                ApiRequest::CampaignAcceptanceGet(tachyon_api::campaign::AcceptanceQuery {
                    campaign_id: id,
                })
            }
            CampaignAction::Accept(args) => {
                args.request(tachyon_api::campaign::HumanDecision::Accept)?
            }
            CampaignAction::Reject(args) => {
                args.request(tachyon_api::campaign::HumanDecision::Reject)?
            }
            CampaignAction::Attention { action } => match action {
                crate::cli::AttentionAction::List { id, after, limit } => {
                    ApiRequest::CampaignAttentionList { id, after, limit }
                }
                crate::cli::AttentionAction::Answer {
                    id,
                    work_id,
                    request_id,
                    generation,
                    instruction_revision,
                    answer,
                } => ApiRequest::CampaignAttentionAnswer {
                    id,
                    work_id,
                    request_id,
                    generation,
                    instruction_revision,
                    answer,
                },
            },
            CampaignAction::Create { title, objective } => {
                let command = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_nanos()
                    .to_string();
                let mut client = tachyon_client::Client::connect().map_err(|e| e.to_string())?;
                let response = client
                    .request(
                        &ApiRequest::ResearchCreate {
                            command_id: format!("cli-research-{command}"),
                            title: title.clone(),
                            objective: objective.clone(),
                        },
                        Duration::from_secs(10),
                    )
                    .map_err(|e| e.to_string())?;
                let research_id = match response {
                    ApiResponse::Research { research } => research.id,
                    ApiResponse::Error { message, .. } => return Err(message),
                    _ => return Err("unexpected research response".into()),
                };
                ApiRequest::CampaignCreate {
                    command_id: format!("cli-campaign-{command}"),
                    research_id,
                    title,
                    objective,
                }
            }
            CampaignAction::Run {
                manifest,
                unisolated_development,
            } => {
                if !unisolated_development {
                    return Err("--unisolated-development is mandatory".into());
                }
                eprintln!("WARNING: native same-user execution, NOT a sandbox. Workers can access host files and local sockets. Only run trusted objectives and evaluators.");
                let mut bytes = Vec::new();
                std::fs::File::open(manifest)
                    .map_err(|e| e.to_string())?
                    .take(MANIFEST_MAX_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| e.to_string())?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_millis() as u64;
                ApiRequest::CampaignRun {
                    manifest: CampaignManifest::parse(&bytes, now)?,
                    unisolated_development,
                }
            }
            CampaignAction::Status { id } => ApiRequest::CampaignProgress { id },
            CampaignAction::Continue {
                id,
                request,
                unisolated_development,
            } => {
                if !unisolated_development {
                    return Err("--unisolated-development is mandatory".into());
                }
                let mut bytes = Vec::new();
                std::fs::File::open(request)
                    .map_err(|e| e.to_string())?
                    .take(MANIFEST_MAX_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| e.to_string())?;
                let request = tachyon_api::continuation::ContinuationRequest::parse(&bytes)?;
                if request.campaign_id != id {
                    return Err("continuation campaign scope mismatch".into());
                }
                eprintln!("WARNING: fresh native same-user process, NOT a sandbox. No Python variables or prior cells are restored. Retry only the identical command request after a lost acknowledgement.");
                ApiRequest::CampaignContinue {
                    id,
                    request,
                    unisolated_development,
                }
            }
            CampaignAction::Reconcile {
                id,
                receipt,
                unisolated_development,
                confirm_authoritative,
            } => {
                if !unisolated_development || !confirm_authoritative {
                    return Err(
                        "--unisolated-development and --confirm-authoritative are mandatory".into(),
                    );
                }
                let mut bytes = Vec::new();
                std::fs::File::open(receipt)
                    .map_err(|e| e.to_string())?
                    .take(MANIFEST_MAX_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| e.to_string())?;
                if bytes.len() > MANIFEST_MAX_BYTES {
                    return Err("receipt exceeds 65536 bytes".into());
                }
                let receipt = tachyon_api::campaign::ReconciliationReceipt::parse(&bytes)?;
                eprintln!("WARNING: privileged same-user operator attestation, NOT independently fetched provider evidence. Never submit worker billing claims. No execution will be replayed.");
                ApiRequest::CampaignReconcile {
                    id,
                    receipt,
                    unisolated_development,
                    confirm_authoritative,
                }
            }
            CampaignAction::Inspect { id } => ApiRequest::CampaignInspect { id },
            CampaignAction::Assessments { id } => ApiRequest::CampaignAssessmentList { id },
            CampaignAction::Assess {
                id,
                command_id,
                unisolated_development,
            } => ApiRequest::CampaignAssessmentRequest {
                id,
                command_id,
                unisolated_development,
            },
            CampaignAction::Recover {
                id,
                unisolated_development,
            } => {
                if !unisolated_development {
                    return Err("--unisolated-development is mandatory".into());
                }
                eprintln!("WARNING: reapproving stored same-user staging policy, NOT a sandbox. No execution will be replayed.");
                ApiRequest::CampaignRecover {
                    id,
                    unisolated_development,
                }
            }
            CampaignAction::Cancel { id } => ApiRequest::CampaignCancel { id },
            CampaignAction::Resume {
                id,
                unisolated_development,
            } => {
                if !unisolated_development {
                    return Err("--unisolated-development is mandatory".into());
                }
                eprintln!("WARNING: resuming native same-user execution, NOT a sandbox.");
                ApiRequest::CampaignResume {
                    id,
                    unisolated_development,
                }
            }
        };
        let response = tachyon_client::Client::connect()
            .map_err(|e| e.to_string())?
            .request(&request, Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        if let ApiResponse::Error { message, .. } = response {
            return Err(message);
        }
        match response {
            ApiResponse::CampaignIntegration { report } => {
                println!("{report:#}");
                if report.get("error").is_some() {
                    return Err(
                        "integration stopped; snapshots retained, retry only the identical plan"
                            .into(),
                    );
                }
            }
            ApiResponse::LocalRetention { id, archived } => {
                println!("{id}\narchived: {archived}\nNo data deleted; no execution started.")
            }
            ApiResponse::CampaignAcceptance { request, receipt } => {
                if let Some(request) = request {
                    println!("awaiting_acceptance\nwork={}\ncandidate={}\ncandidate_sha256={}\nexpected_state_sha256={}\nconfig_hash={}\ngeneration={} assignment={} instruction_revision={}\ndeadline_ms={}", request.work_id, request.candidate, request.candidate_sha256, request.expected_state_sha256, request.config_hash, request.generation, request.assignment, request.instruction_revision, request.deadline_ms);
                } else if let Some(receipt) = receipt {
                    println!("{}\nsource=human host_uid={} recorded_at_ms={}\ncommand_id={}\ncandidate={}\ncandidate_sha256={}\nconfig_hash={}\nTrusted same-user attestation, not automated verification.", if receipt.decision.decision == tachyon_api::campaign::HumanDecision::Accept { "accepted_human" } else { "rejected_human" }, receipt.host_uid, receipt.recorded_at_ms, receipt.decision.command_id, receipt.request.candidate, receipt.request.candidate_sha256, receipt.request.config_hash);
                } else {
                    println!(
                        "No current human acceptance request or receipt; inspect campaign status."
                    );
                }
            }
            ApiResponse::CampaignAttentionList {
                questions,
                next_cursor,
            } => {
                for q in questions {
                    println!("work={:?} request={:?} generation={} instruction_revision={} deadline_ms={}\nquestion={:?}", q.work_id, q.request_id, q.generation, q.instruction_revision, q.deadline_ms, q.question);
                }
                if let Some(cursor) = next_cursor {
                    println!("next_cursor={cursor:?}");
                }
            }
            ApiResponse::CampaignAttentionAnswered { attention } => println!(
                "Answer accepted: work={:?} request={:?}",
                attention.work_id, attention.request_id
            ),
            ApiResponse::CampaignAssessments { records } => {
                for record in records {
                    println!(
                        "{} revision={} status={} triggers={}",
                        record.request_id,
                        record.revision,
                        record.status,
                        record.triggers.join(",")
                    );
                    if let Some(published) = record.published {
                        println!("{}", published.advisory());
                    }
                }
            }
            ApiResponse::CampaignInspection {
                campaign,
                diagnostics,
            } => {
                println!(
                    "{}\nstatus: {:?}\nobjective: {}",
                    campaign.id, campaign.status, campaign.objective
                );
                for diagnostic in diagnostics {
                    println!("{diagnostic}");
                }
            }
            ApiResponse::CampaignProgress { campaign, activity } => {
                println!("{}\nstatus: {:?}\nobjective: {}\nowned: {}\nwork (including verifiers): admitted={} queued={} active={} waiting={} terminal={}",
                    campaign.id, campaign.status, campaign.objective, activity.owned,
                    activity.admitted, activity.queued, activity.active, activity.waiting, activity.terminal);
                println!(
                    "host admission waiting: resident={} model={}",
                    activity.host_resident_waiting, activity.host_model_waiting
                );
                if !activity.owned && activity.active > 0 {
                    println!(
                        "Active counts are last-known durable leases, not live process proof."
                    );
                }
            }
            ApiResponse::Campaign { campaign } => println!(
                "{}\nstatus: {:?}\nobjective: {}",
                campaign.id, campaign.status, campaign.objective
            ),
            _ => return Err("unexpected campaign response".into()),
        }
        Ok(())
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("tachyon campaign: {e}");
            ExitCode::FAILURE
        }
    }
}
