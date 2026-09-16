//! Fenced host actions. Catalog Work commits with admission; Verify intent commits
//! its receipt only when the existing evaluator claim reserves its original funds.
use super::*;
use crate::allocation_policy as policy;
use crate::runtime_store::campaign_ledger::Pool;
use crate::runtime_store::execution::{decode_record, Evaluation, ExecutionPhase, EXECUTIONS};
use crate::runtime_store::execution::{ExecutionPolicy, ExecutionRecord};
use tachyon_api::campaign::{AllocationControl, AllocationSignal};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PolicyActionContext {
    pub owner: String,
    pub fence: policy::Fence,
    pub signal: AllocationSignal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingReview {
    context: PolicyActionContext,
    record: ExecutionRecord,
}

const OWNERS: redb::TableDefinition<&str, &str> =
    redb::TableDefinition::new("allocation_owners_v1");

impl RuntimeStore {
    pub(crate) fn allocation_action_context(
        &self,
        campaign: &str,
        owner: &str,
        signal: &AllocationSignal,
        approved: &[(String, Vec<String>)],
    ) -> Result<Option<(PolicyActionContext, bool)>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let root = load(&tx, campaign)?.ok_or("unknown allocation root")?;
        let Some((_, members)) = approved.iter().find(|(id, _)| id == &signal.group_id) else {
            return Ok(None);
        };
        let Some(group) = root.groups.get(&signal.group_id) else {
            return Ok(None);
        };
        if group
            .spec
            .work
            .iter()
            .any(|w| !members.contains(&w.work_id))
        {
            return Err("allocation signal outside approved descriptor".into());
        }
        let context = PolicyActionContext {
            owner: owner.into(),
            signal: signal.clone(),
            fence: policy::Fence {
                campaign_id: campaign.into(),
                group_id: signal.group_id.clone(),
                group_revision: signal.expected_revision,
                controller_id: group.policy_controller.clone().unwrap_or_default(),
                steering_revision: group.policy_epoch,
            },
        };
        if !group.policy_commands.contains_key(&signal.command_id)
            && (group.policy_disabled || group.cancellation_requested)
        {
            return Ok(None);
        }
        let replay = Self::check_allocation_action_in(&tx, &context)?;
        Ok(Some((context, replay)))
    }

    pub(in crate::runtime_store) fn check_allocation_action_in(
        tx: &WriteTransaction,
        context: &PolicyActionContext,
    ) -> Result<bool, String> {
        let f = &context.fence;
        if tx
            .open_table(OWNERS)
            .map_err(err)?
            .get(f.campaign_id.as_str())
            .map_err(err)?
            .is_none_or(|v| v.value() != context.owner)
        {
            return Err("stale allocation controller".into());
        }
        let root = load(tx, &f.campaign_id)?.ok_or("unknown allocation root")?;
        let group = root
            .groups
            .get(&f.group_id)
            .ok_or("unknown allocation group")?;
        if context.signal.group_id != f.group_id
            || context.signal.expected_revision != f.group_revision
        {
            return Err("allocation structural identity conflict".into());
        }
        if let Some(old) = group.policy_commands.get(&context.signal.command_id) {
            return if old == &context.signal {
                Ok(true)
            } else {
                Err("allocation command payload conflict".into())
            };
        }
        if group.policy_disabled
            || group.cancellation_requested
            || group.revision != f.group_revision
            || group.policy_epoch != f.steering_revision
            || group.policy_controller.as_deref().unwrap_or_default() != f.controller_id
        {
            return Err("stale allocation action fence".into());
        }
        if group.policy_commands.len() >= 32 {
            return Err("allocation receipt bound".into());
        }
        if group
            .policy_review
            .as_ref()
            .is_some_and(|pending| pending.context.signal != context.signal)
        {
            return Err("another allocation review is pending".into());
        }
        Ok(false)
    }

    pub(in crate::runtime_store) fn finish_allocation_action_in(
        tx: &WriteTransaction,
        context: &PolicyActionContext,
    ) -> Result<(), String> {
        if Self::check_allocation_action_in(tx, context)? {
            return Ok(());
        }
        let mut root = load(tx, &context.fence.campaign_id)?.ok_or("unknown allocation root")?;
        let group = root
            .groups
            .get_mut(&context.fence.group_id)
            .ok_or("unknown allocation group")?;
        let controller = format!("{}:{}", context.owner, context.fence.group_id);
        if group.policy_controller.as_deref() != Some(&controller) {
            group.policy_controller = Some(controller);
            group.policy_epoch = group
                .policy_epoch
                .checked_add(1)
                .ok_or("policy epoch overflow")?;
        }
        group.revision = group
            .revision
            .checked_add(1)
            .ok_or("group revision overflow")?;
        group
            .policy_commands
            .insert(context.signal.command_id.clone(), context.signal.clone());
        group.policy_review = None;
        save(tx, &root)
    }

    pub(in crate::runtime_store) fn allocation_work_scope_in(
        tx: &WriteTransaction,
        context: &PolicyActionContext,
        actor: &crate::runtime_store::coordination::WorkAddress,
    ) -> Result<(), String> {
        let AllocationControl::Work {
            parent_work_id,
            generation,
            instruction_revision,
            ..
        } = &context.signal.action
        else {
            return Err("not an allocation Work action".into());
        };
        let campaign = &context.fence.campaign_id;
        if &actor.campaign_id != campaign || &actor.work_id != parent_work_id {
            return Err("allocation parent conflict".into());
        }
        let parent = Self::admitted_work_in(tx, parent_work_id)?;
        if parent.admission.campaign_id != *campaign
            || parent.admission.generation != *generation
            || Self::latest_instruction_revision_in(tx, &parent.admission)? != *instruction_revision
        {
            return Err("allocation parent instructions/generation conflict".into());
        }
        let root = load(tx, campaign)?.ok_or("unknown allocation root")?;
        let group = root
            .groups
            .get(&context.fence.group_id)
            .ok_or("unknown allocation group")?;
        let descendants = Self::agent_descendants_in(tx, campaign, parent_work_id)?;
        let parent_inside = root.work.get(parent_work_id).is_some_and(|m| {
            ancestors(&root, m.group.as_deref()).is_ok_and(|a| a.contains(&context.fence.group_id))
        });
        if !parent_inside
            && !group
                .spec
                .work
                .iter()
                .all(|w| descendants.contains(&w.work_id))
        {
            return Err("allocation parent outside controlled branch".into());
        }
        Ok(())
    }

    pub(crate) fn stage_allocation_review(
        &self,
        context: &PolicyActionContext,
        approved: &ExecutionPolicy,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        if Self::check_allocation_action_in(&tx, context)? {
            return Ok(());
        }
        let AllocationControl::Verify {
            work_id,
            generation,
            instruction_revision,
        } = &context.signal.action
        else {
            return Err("not an allocation Verify action".into());
        };
        let record = {
            let table = tx.open_table(EXECUTIONS).map_err(err)?;
            let row = table
                .get(work_id.as_str())
                .map_err(err)?
                .ok_or("missing verification execution")?;
            decode_record(row.value(), work_id)?
        };
        let mut expected = approved.clone();
        expected.funding.state = record.policy.funding.state.clone();
        let descriptor_matches = if record.policy == expected {
            true
        } else {
            // Repair may advance the attempt, never the original host catalog
            // approval or evaluator configuration. Capture only its current record.
            use crate::runtime_store::execution::command::{CommandGateRecord, COMMAND_GATES};
            let table = tx.open_table(COMMAND_GATES).map_err(err)?;
            let gate = table
                .get(work_id.as_str())
                .map_err(err)?
                .map(|v| serde_json::from_slice::<CommandGateRecord>(v.value()).map_err(err))
                .transpose()?;
            gate.is_some_and(|g| {
                let mut original = g
                    .original_policy
                    .clone()
                    .unwrap_or_else(|| g.policy.clone());
                original.funding.state = expected.funding.state.clone();
                original == expected
                    && g.policy == record.policy
                    && !g.finalize_rework
                    && g.policy.evaluator_id == approved.evaluator_id
                    && g.config
                        .config_hash()
                        .is_ok_and(|hash| g.policy.evaluator_id == format!("command:{hash}"))
            })
        };
        if record.phase != ExecutionPhase::AwaitingVerification
            || record.settled
            || record.rework_pending
            || record.candidate.is_none()
            || !descriptor_matches
            || record.policy.work.generation != *generation
            || record.policy.funding.admission.campaign_id != context.fence.campaign_id
            || Self::latest_instruction_revision_in(&tx, &record.policy.funding.admission)?
                != *instruction_revision
            || record.policy.model.identity.instruction_revision != *instruction_revision
        {
            return Err("ineligible allocation verification descriptor".into());
        }
        let mut root = load(&tx, &context.fence.campaign_id)?.ok_or("unknown allocation root")?;
        let member = root.work.get(work_id).ok_or("unknown verification work")?;
        if member.cancellation_requested
            || !ancestors(&root, member.group.as_deref())?.contains(&context.fence.group_id)
        {
            return Err("verification outside controlled branch".into());
        }
        let verifier = Self::admitted_work_in(&tx, &record.policy.verification.admission.work_id)?;
        let ledger = Self::campaign_ledger_in(&tx, &context.fence.campaign_id)?;
        let hold = ledger
            .reservations
            .get(&verifier.dispatch_id)
            .ok_or("missing verifier hold")?;
        let funded = match &verifier.state {
            DispatchState::Admitted => !ledger.allocations.contains_key(&verifier.dispatch_id),
            DispatchState::Registered { .. } => {
                ledger.allocations.get(&verifier.dispatch_id) == Some(&false)
                    && ledger
                        .allocation_available(&verifier.dispatch_id)
                        .is_ok_and(|available| {
                            available.tokens >= verifier.admission.upper_bound.tokens
                                && available.cost_micro_usd
                                    >= verifier.admission.upper_bound.cost_micro_usd
                        })
                    && !ledger.reservations.values().any(|r| {
                        r.allocation.as_deref() == Some(&verifier.dispatch_id)
                            && !matches!(r.usage, Usage::Final(_))
                    })
            }
            _ => false,
        };
        if verifier.admission != record.policy.verification.admission
            || verifier.dispatch_id != record.policy.verification.dispatch_id
            || !funded
            || hold.cancellation_requested
            || hold.usage != Usage::Unknown
            || ledger.admissions_paused
            || ledger.debt != Default::default()
            || ledger.reservations.contains_key(
                &crate::runtime_store::execution::evaluation_receipt(&record.policy),
            )
            || Self::group_cancelled_in(&tx, &verifier)?
        {
            return Err("verification is not funded and eligible".into());
        }
        let pending = PendingReview {
            context: context.clone(),
            record,
        };
        if root.groups.iter().any(|(id, g)| {
            id != &context.fence.group_id
                && g.policy_review
                    .as_ref()
                    .is_some_and(|p| p.record.policy.work.work_id == *work_id)
        }) {
            return Err("verification already has an allocation intent".into());
        }
        let group = root.groups.get_mut(&context.fence.group_id).unwrap();
        if group.policy_review.as_ref().is_some_and(|old| {
            old.record != pending.record || old.context.signal != pending.context.signal
        }) {
            return Err("allocation review intent conflict".into());
        }
        group.policy_review = Some(pending);
        save(&tx, &root)?;
        tx.commit().map_err(err)
    }

    pub(in crate::runtime_store) fn allocation_review_claim_in(
        tx: &WriteTransaction,
        record: &ExecutionRecord,
    ) -> Result<Option<PolicyActionContext>, String> {
        let campaign = &record.policy.funding.admission.campaign_id;
        let Some(root) = load(tx, campaign)? else {
            return Ok(None);
        };
        for group in root.groups.values() {
            let Some(pending) = &group.policy_review else {
                continue;
            };
            if pending.record.policy.work.work_id != record.policy.work.work_id {
                continue;
            }
            if &pending.record != record || Self::check_allocation_action_in(tx, &pending.context)?
            {
                return Err("stale allocation review intent".into());
            }
            let AllocationControl::Verify {
                instruction_revision,
                ..
            } = pending.context.signal.action
            else {
                return Err("invalid allocation review intent".into());
            };
            if Self::latest_instruction_revision_in(tx, &record.policy.funding.admission)?
                != instruction_revision
            {
                return Err("stale allocation review instructions".into());
            }
            return Ok(Some(pending.context.clone()));
        }
        Ok(None)
    }

    pub(crate) fn allocation_review_dispatchable(
        &self,
        record: &ExecutionRecord,
    ) -> Result<bool, String> {
        let tx = self.database.begin_write().map_err(err)?;
        // A stale intent must not prevent the normal cancellation settlement path.
        if Self::group_cancelled_in(&tx, &record.policy.funding)?
            || Self::group_cancelled_in(&tx, &record.policy.verification)?
        {
            return Ok(true);
        }
        // Retain denied intent for inspection, but do not create a failing task
        // on every tick. Explicit identical reauthorization may rebind its owner.
        Ok(Self::allocation_review_claim_in(&tx, record).is_ok())
    }

    /// One explicit control per tick, selected from bounded host configuration.
    pub(crate) fn allocation_control_tick(
        &self,
        campaign: &str,
        owner: &str,
        allocation: &tachyon_api::campaign::CampaignAllocation,
        approved: &[(String, Vec<String>)],
    ) -> Result<bool, String> {
        allocation.validate()?;
        if approved.len() > 64 || approved.iter().any(|(_, work)| work.len() > 32) {
            return Err("allocation control bounds".into());
        }
        let tx = self.database.begin_write().map_err(err)?;
        let mut root = load(&tx, campaign)?.ok_or("unknown allocation root")?;
        if tx
            .open_table(OWNERS)
            .map_err(err)?
            .get(campaign)
            .map_err(err)?
            .is_none_or(|v| v.value() != owner)
        {
            return Err("stale allocation controller".into());
        }
        for signal in &allocation.signals {
            if matches!(
                signal.action,
                AllocationControl::Work { .. } | AllocationControl::Verify { .. }
            ) {
                continue;
            }
            let Some((_, members)) = approved.iter().find(|(id, _)| id == &signal.group_id) else {
                continue;
            };
            let Some(group) = root.groups.get_mut(&signal.group_id) else {
                continue;
            };
            if group
                .spec
                .work
                .iter()
                .any(|w| !members.contains(&w.work_id))
            {
                return Err("allocation signal outside approved descriptor".into());
            }
            if let Some(old) = group.policy_commands.get(&signal.command_id) {
                if old != signal {
                    return Err("allocation command payload conflict".into());
                }
                continue;
            }
            if group.policy_disabled || group.cancellation_requested {
                continue;
            }
            let controller = format!("{owner}:{}", signal.group_id);
            if group.policy_controller.as_deref() != Some(&controller) {
                group.policy_controller = Some(controller.clone());
                group.policy_epoch = group
                    .policy_epoch
                    .checked_add(1)
                    .ok_or("policy epoch overflow")?;
            }
            let fence = policy::Fence {
                campaign_id: campaign.into(),
                group_id: signal.group_id.clone(),
                group_revision: signal.expected_revision,
                controller_id: controller,
                steering_revision: group.policy_epoch,
            };
            save(&tx, &root)?;
            Self::apply_allocation_signal_in(&tx, &fence, signal)?;
            tx.commit().map_err(err)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn apply_allocation_signal_in(
        tx: &WriteTransaction,
        fence: &policy::Fence,
        signal: &tachyon_api::campaign::AllocationSignal,
    ) -> Result<(), String> {
        use tachyon_api::campaign::AllocationControl;
        let mut root = load(tx, &fence.campaign_id)?.ok_or("unknown allocation root")?;
        let group = root
            .groups
            .get(&fence.group_id)
            .ok_or("unknown allocation group")?;
        if signal.group_id != fence.group_id
            || signal.expected_revision != fence.group_revision
            || group.policy_disabled
            || group.policy_controller.as_deref() != Some(&fence.controller_id)
            || group.policy_epoch != fence.steering_revision
        {
            return Err("stale allocation signal fence".into());
        }
        if let Some(old) = group.policy_commands.get(&signal.command_id) {
            return if old == signal {
                Ok(())
            } else {
                Err("allocation command payload conflict".into())
            };
        }
        if group.policy_commands.len() == 32 {
            return Err("allocation receipt bound".into());
        }
        let mut branches = Vec::new();
        let cap = match &signal.action {
            AllocationControl::Reallocate {
                source_work_id,
                source_generation,
                target_work_id,
                target_generation,
                tokens,
                cost_micro_usd,
                expected_ledger_revision,
            } => {
                let mut allocations = Vec::new();
                for (id, generation) in [
                    (source_work_id, source_generation),
                    (target_work_id, target_generation),
                ] {
                    let work = Self::admitted_work_in(tx, id)?;
                    let member = root.work.get(id).ok_or("unknown transfer work")?;
                    if work.admission.campaign_id != fence.campaign_id
                        || work.admission.generation != *generation
                        || work.admission.pool != Pool::Work
                        || !matches!(work.state, DispatchState::Registered { .. })
                        || member.terminal
                        || member.cancellation_requested
                        || !ancestors(&root, member.group.as_deref())?.contains(&fence.group_id)
                        || Self::group_cancelled_in(tx, &work)?
                    {
                        return Err("ineligible transfer work scope/generation".into());
                    }
                    for ancestor in ancestors(&root, member.group.as_deref())? {
                        let branch = &root.groups[&ancestor];
                        if branch.max_running == 0 || branch.policy_disabled {
                            return Err("transfer branch paused or manually controlled".into());
                        }
                    }
                    allocations.push(work.dispatch_id);
                }
                // Group CAS below and this ledger receipt commit in the same writer.
                use sha2::{Digest, Sha256};
                let receipt = format!(
                    "allocation-transfer:{:x}",
                    Sha256::digest(
                        &serde_json::to_vec(&(
                            &fence.campaign_id,
                            &fence.group_id,
                            &signal.command_id
                        ))
                        .map_err(err)?
                    )
                );
                Self::campaign_ledger_command_in(
                    tx,
                    &receipt,
                    &fence.campaign_id,
                    LedgerCommand::TransferAvailable {
                        source_allocation_id: allocations[0].clone(),
                        target_allocation_id: allocations[1].clone(),
                        amounts: Units {
                            tokens: *tokens,
                            cost_micro_usd: *cost_micro_usd,
                        },
                        expected_revision: *expected_ledger_revision,
                    },
                )?;
                group.max_running
            }
            AllocationControl::Work { .. } | AllocationControl::Verify { .. } => {
                return Err("execution action requires catalog adapter".into())
            }
            AllocationControl::Pause => 0,
            AllocationControl::Stop {
                work_id,
                generation,
            } => {
                let work = Self::admitted_work_in(tx, work_id)?;
                if work.admission.campaign_id != fence.campaign_id
                    || work.admission.generation != *generation
                {
                    return Err("allocation branch generation conflict".into());
                }
                branches = Self::agent_descendants_in(tx, &fence.campaign_id, work_id)?;
                branches.push(work_id.clone());
                for id in &branches {
                    let member = root.work.get(id).ok_or("unknown allocation branch")?;
                    if !ancestors(&root, member.group.as_deref())?.contains(&fence.group_id) {
                        return Err("allocation branch outside controlled group".into());
                    }
                }
                group.max_running
            }
        };
        // CAS and receipt share the cancellation writer. Active holds/leases are
        // retained until the existing scheduler cancellation owner proves cleanup.
        resize_group(
            &mut root,
            &fence.group_id,
            fence.group_revision,
            cap,
            Some((&fence.controller_id, fence.steering_revision)),
        )?;
        let group = root.groups.get_mut(&fence.group_id).unwrap();
        group
            .policy_commands
            .insert(signal.command_id.clone(), signal.clone());
        group.policy_disabled = matches!(signal.action, AllocationControl::Pause);
        if group
            .policy_review
            .as_ref()
            .is_some_and(|pending| branches.contains(&pending.record.policy.work.work_id))
        {
            group.policy_review = None;
        }
        save(tx, &root)?;
        for id in branches {
            Self::cancel_work_in(tx, &fence.campaign_id, &id)?;
        }
        Ok(())
    }

    /// Explicit host activation fences the previous scheduler, including after restart.
    pub(crate) fn select_allocation_owner(
        &self,
        campaign: &str,
        owner: &str,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        tx.open_table(OWNERS)
            .map_err(err)?
            .insert(campaign, owner)
            .map_err(err)?;
        tx.commit().map_err(err)
    }

    pub(in crate::runtime_store) fn relinquish_allocation_in(
        tx: &WriteTransaction,
        campaign: &str,
    ) -> Result<(), String> {
        let Some(mut root) = load(tx, campaign)? else {
            return Ok(());
        };
        // Campaign-wide relinquishment also covers steering the logical parent.
        for group in root.groups.values_mut() {
            group.policy_disabled = true;
            group.policy_epoch = group
                .policy_epoch
                .checked_add(1)
                .ok_or("policy epoch overflow")?;
        }
        save(tx, &root)
    }

    pub(crate) fn allocation_tick(
        &self,
        campaign: &str,
        owner: &str,
        cap: usize,
        resident_free: usize,
        groups: &[(String, Vec<String>)],
        live: &[(String, u64)],
    ) -> Result<(), String> {
        if groups.len() > 64 || live.len() > 64 || !(1..=64).contains(&cap) {
            return Err("allocation bounds".into());
        }
        let tx = self.database.begin_write().map_err(err)?;
        if tx
            .open_table(OWNERS)
            .map_err(err)?
            .get(campaign)
            .map_err(err)?
            .is_none_or(|v| v.value() != owner)
        {
            return Err("stale allocation controller".into());
        }
        let mut root = load(&tx, campaign)?.ok_or("unknown allocation root")?;
        let ledger = Self::campaign_ledger_in(&tx, campaign)?;
        let root_active = root.work.values().filter(|m| m.active).count();
        let mut free = resident_free
            .min(root.limits.max_running.saturating_sub(root_active))
            .min(
                (ledger.envelope.max_active_inferences as usize)
                    .saturating_sub(ledger.active_inferences()),
            );
        let mut unknown = ledger.admissions_paused || ledger.debt != Default::default();
        let mut live_allocations = BTreeSet::new();
        let mut admission_holds = BTreeSet::new();
        for (id, member) in &root.work {
            let work = Self::admitted_work_in(&tx, id)?;
            admission_holds.insert(work.dispatch_id.clone());
            unknown |= !member.terminal && matches!(work.state, DispatchState::DispatchingUnknown);
            let owned = live
                .iter()
                .any(|(w, g)| w == id && *g == work.admission.generation)
                && (member.active || member.resident && member.wait.is_some())
                && !member.cancellation_requested
                && matches!(work.state, DispatchState::Registered { .. });
            if owned {
                live_allocations.insert(work.dispatch_id.clone());
            }
            if let Some(value) = tx
                .open_table(EXECUTIONS)
                .map_err(err)?
                .get(id.as_str())
                .map_err(err)?
            {
                let record = decode_record(value.value(), id)?;
                unknown |= matches!(record.phase, ExecutionPhase::ReviewingUnknown)
                    || matches!(record.phase, ExecutionPhase::ExecutingUnknown) && !owned;
            }
        }
        // Live owned requests retain their full holds and occupied slots. Unknown
        // usage after losing that cancellation/task handle blocks expansion.
        // Only known admission holds may lack an allocation; standalone requests
        // have no live execution owner in this projection.
        unknown |= ledger.reservations.iter().any(|(id, r)| {
            r.allocation.as_ref().map_or_else(
                || !admission_holds.contains(id),
                |a| !live_allocations.contains(a),
            ) && !matches!(r.usage, Usage::Final(_))
        });
        for (id, approved) in groups {
            if approved.len() > 32 {
                return Err("allocation queue bound".into());
            }
            let Some(group) = root.groups.get(id) else {
                continue;
            };
            if group.spec.work.len() > 32 {
                return Err("allocation group scan bound".into());
            }
            if group.policy_disabled
                || group.cancellation_requested
                || group.max_running == 0
                || group.policy_review.is_some()
            {
                continue;
            }
            let mut group = group.clone();
            let mut ancestor_free = free;
            let mut ancestor_cap = cap;
            for parent in ancestors(&root, group.spec.parent.as_deref())? {
                let g = &root.groups[&parent];
                let occupied = root
                    .work
                    .values()
                    .filter(|m| {
                        m.active
                            && ancestors(&root, m.group.as_deref())
                                .is_ok_and(|a| a.contains(&parent))
                    })
                    .count();
                ancestor_cap = ancestor_cap.min(g.max_running);
                ancestor_free = ancestor_free.min(g.max_running.saturating_sub(occupied));
                if g.cancellation_requested {
                    ancestor_free = 0;
                }
            }
            // An ancestor pause is not a permanent pause of this group's policy.
            if ancestor_cap == 0 {
                continue;
            }
            let controller = format!("{owner}:{id}");
            if group.policy_controller.as_deref() != Some(&controller) {
                group.policy_controller = Some(controller.clone());
                group.policy_epoch = group
                    .policy_epoch
                    .checked_add(1)
                    .ok_or("policy epoch overflow")?;
            }
            let mut queued = Vec::new();
            let mut recent = Vec::new();
            let mut funds = policy::Allowance::default();
            let active = root
                .work
                .values()
                .filter(|m| {
                    m.active && ancestors(&root, m.group.as_deref()).is_ok_and(|a| a.contains(id))
                })
                .count() as u32;
            for a in &group.spec.work {
                let member = &root.work[&a.work_id];
                let work = Self::admitted_work_in(&tx, &a.work_id)?;
                if approved.contains(&a.work_id)
                    && work.state == DispatchState::Admitted
                    && !member.cancellation_requested
                    && !member.terminal
                {
                    let hold = ledger
                        .reservations
                        .get(&work.dispatch_id)
                        .ok_or("missing queued hold")?;
                    if !hold.cancellation_requested
                        && matches!(hold.usage, Usage::Unknown)
                        && !ledger.allocations.contains_key(&work.dispatch_id)
                    {
                        let allowance = policy::Allowance {
                            money_micros: hold.reserved.cost_micro_usd,
                            inference_units: hold.reserved.tokens,
                        };
                        funds.money_micros = funds
                            .money_micros
                            .checked_add(allowance.money_micros)
                            .ok_or("fund overflow")?;
                        funds.inference_units = funds
                            .inference_units
                            .checked_add(allowance.inference_units)
                            .ok_or("fund overflow")?;
                        queued.push(policy::QueuedObjective {
                            work_id: a.work_id.clone(),
                            kind: policy::ObjectiveKind::Work,
                            allowance,
                        });
                    }
                }
                if let Some(value) = tx
                    .open_table(EXECUTIONS)
                    .map_err(err)?
                    .get(a.work_id.as_str())
                    .map_err(err)?
                {
                    let record = decode_record(value.value(), &a.work_id)?;
                    if record.settled
                        && !record.rework_pending
                        && !group.policy_seen.contains(&a.work_id)
                    {
                        let outcome = match record.phase {
                            ExecutionPhase::Reviewed(Evaluation::Accepted) => {
                                policy::RecentOutcome::Useful
                            }
                            ExecutionPhase::Reviewed(Evaluation::Rejected) => {
                                policy::RecentOutcome::Failed
                            }
                            _ => policy::RecentOutcome::Unknown,
                        };
                        recent.push(outcome);
                        if !unknown || outcome == policy::RecentOutcome::Failed {
                            group.policy_seen.insert(a.work_id.clone());
                        }
                    }
                }
            }
            let snapshot = policy::PolicySnapshot {
                fence: policy::Fence {
                    campaign_id: campaign.into(),
                    group_id: id.clone(),
                    group_revision: group.revision,
                    controller_id: controller,
                    steering_revision: group.policy_epoch,
                },
                steering: policy::Steering::Run,
                queued: &queued,
                recent: &recent,
                remaining_budget: funds,
                resources: policy::ResourceAvailability {
                    active_cap: ancestor_cap
                        .min(root.limits.max_running)
                        .min(group.host_max_running.unwrap_or(group.spec.max_running))
                        as u32,
                    free_slots: if unknown {
                        0
                    } else {
                        ancestor_free.min(64) as u32
                    },
                    remaining_work: 0,
                },
                active,
                current_limit: group.max_running as u32,
                action_limit: 1,
            };
            let proposal =
                policy::propose_resize(&snapshot).map_err(|e| format!("allocation: {e:?}"))?;
            // Snapshot, fence/resource validation and mutation share this writer;
            // neither steering nor another controller can interleave application.
            policy::validate_resize(&snapshot, &proposal)
                .map_err(|e| format!("allocation: {e:?}"))?;
            root.groups.insert(id.clone(), group);
            for action in proposal.actions {
                let policy::Action::Resize { max_running } = action else {
                    return Err("unsupported allocation action".into());
                };
                // Do not promise the same additional headroom to another group
                // in this tick. Shrink never refunds occupied physical slots.
                free = free
                    .saturating_sub(max_running.saturating_sub(snapshot.current_limit) as usize);
                resize_group(
                    &mut root,
                    id,
                    proposal.fence.group_revision,
                    max_running as usize,
                    Some((
                        &proposal.fence.controller_id,
                        proposal.fence.steering_revision,
                    )),
                )?;
            }
        }
        save(&tx, &root)?;
        tx.commit().map_err(err)
    }
}
