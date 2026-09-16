//! Host-only control boundary. IDs are durable addresses, never credentials.
#![allow(dead_code)]

use std::collections::BTreeMap;

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

use super::{
    admission::{Admission, AdmittedWork, DispatchState},
    RuntimeStore,
};

const ROOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("campaign_coordination_v1");
const MAX_WORK: usize = 256;
const MAX_COMMANDS: usize = 4096;
const MAX_PER_WORK: usize = 256;
const MAX_TEXT: usize = 4096;
const MAX_PAGE: usize = 32;

pub(super) fn initialize(tx: &WriteTransaction) -> Result<(), String> {
    tx.open_table(ROOTS).map_err(err)?;
    Ok(())
}
fn err(e: impl std::fmt::Display) -> String {
    format!("agent coordination: {e}")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkAddress {
    pub campaign_id: String,
    pub work_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ControlCommand {
    Send {
        text: String,
    },
    Steer {
        expected_revision: u64,
        instructions: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Message {
    pub sequence: u64,
    pub command_id: String,
    pub sender: WorkAddress,
    pub recipient: WorkAddress,
    pub command: ControlCommand,
    pub accepted_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Member {
    parent: Option<String>,
    accepted_revision: u64,
    acknowledged_revision: u64,
    acknowledgments: Vec<u64>,
    commands: usize,
}

#[derive(Default, Serialize, Deserialize)]
struct Root {
    members: BTreeMap<String, Member>,
    messages: Vec<Message>,
    #[serde(default)]
    deliveries: BTreeMap<String, Delivery>,
}

#[derive(Default, Serialize, Deserialize)]
struct Delivery {
    cursor: u64,
    revision: u64,
    prepared: Option<tachyon_model::broker::Boundary>,
    acknowledged_id: Option<String>,
    #[serde(default)]
    model_revision: Option<u64>,
    #[serde(default)]
    context_messages: Vec<tachyon_model::broker::ParentMessage>,
}

fn load(tx: &WriteTransaction, campaign: &str) -> Result<Root, String> {
    tx.open_table(ROOTS)
        .map_err(err)?
        .get(campaign)
        .map_err(err)?
        .map(|v| serde_json::from_slice(v.value()).map_err(err))
        .transpose()
        .map(|r| r.unwrap_or_default())
}
fn save(tx: &WriteTransaction, campaign: &str, root: &Root) -> Result<(), String> {
    tx.open_table(ROOTS)
        .map_err(err)?
        .insert(campaign, serde_json::to_vec(root).map_err(err)?.as_slice())
        .map_err(err)?;
    Ok(())
}

fn related(root: &Root, actor: &str, target: &str) -> Result<(), String> {
    let a = root
        .members
        .get(actor)
        .ok_or_else(|| err("unknown actor"))?;
    let b = root
        .members
        .get(target)
        .ok_or_else(|| err("unknown target"))?;
    if actor != target && a.parent.as_deref() != Some(target) && b.parent.as_deref() != Some(actor)
    {
        return Err(err("parent scope denied"));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct Page<T> {
    pub items: Vec<T>,
    /// Last inspected stable key, not an offset or a snapshot token.
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub(crate) struct WorkStatus {
    pub address: WorkAddress,
    pub parent: Option<String>,
    pub admission: DispatchState,
    pub generation: u64,
    pub execution_revision: Option<u64>,
    pub accepted_revision: u64,
    pub acknowledged_revision: u64,
    pub delivered_revision: u64,
    pub delivery_cursor: u64,
    #[cfg(target_os = "linux")]
    pub result: Option<ResultReference>,
}

/// Compact reference to existing evidence, not a transcript or a worker claim.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct ResultReference {
    pub attempt_id: String,
    pub work: WorkAddress,
    pub revision: u64,
    pub generation: u64,
    pub current: bool,
    pub phase: super::execution::ExecutionPhase,
    pub has_candidate: bool,
    pub settled: bool,
    pub rework_pending: bool,
    pub research: Option<tachyon_api::agents::ResearchResult>,
}

#[cfg(target_os = "linux")]
pub(crate) enum ResultSelection {
    Current,
    Revision(u64),
}

/// Non-serializable facade issued only after host authentication/authorization.
/// A bridge must retain this binding; model arguments cannot select the actor.
pub(crate) struct HostAgentControl<'a> {
    store: &'a RuntimeStore,
    actor: WorkAddress,
}

impl RuntimeStore {
    pub(super) fn context_instruction_refs_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<Vec<tachyon_api::context::InstructionContextRef>, String> {
        Ok(load(tx, &admission.campaign_id)?
            .messages
            .into_iter()
            .filter(|m| {
                m.recipient.work_id == admission.work_id
                    && matches!(m.command, ControlCommand::Steer { .. })
            })
            .map(|m| tachyon_api::context::InstructionContextRef {
                command_id: m.command_id,
                sequence: m.sequence,
                accepted_revision: m.accepted_revision,
            })
            .collect())
    }

    pub(super) fn context_work_handles_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<Vec<String>, String> {
        let root = load(tx, &admission.campaign_id)?;
        Ok(root
            .members
            .iter()
            .filter(|(_, m)| m.parent.as_deref() == Some(&admission.work_id))
            .map(|(id, _)| id.clone())
            .collect())
    }

    /// Local operator approval at a known stopped boundary, not parent authority.
    pub(super) fn continuation_instruction_in(
        tx: &WriteTransaction,
        admission: &Admission,
        command_id: &str,
        instructions: &str,
    ) -> Result<(u64, Vec<String>), String> {
        let mut root = load(tx, &admission.campaign_id)?;
        if root.messages.len() >= MAX_COMMANDS || instructions.len() > 16384 {
            return Err(err("continuation instruction capacity exhausted"));
        }
        if !root.members.contains_key(&admission.work_id) && root.members.len() >= MAX_WORK {
            return Err(err("continuation membership capacity exhausted"));
        }
        let member = root
            .members
            .entry(admission.work_id.clone())
            .or_insert(Member {
                parent: None,
                accepted_revision: admission.instruction_revision,
                acknowledged_revision: admission.instruction_revision,
                acknowledgments: Vec::new(),
                commands: 0,
            });
        if member.commands >= MAX_PER_WORK {
            return Err(err("continuation instruction capacity exhausted"));
        }
        let previous = member.accepted_revision;
        let revision = previous
            .checked_add(1)
            .ok_or("instruction revision overflow")?;
        member.accepted_revision = revision;
        member.acknowledged_revision = revision;
        member.commands += 1;
        let address = WorkAddress {
            campaign_id: admission.campaign_id.clone(),
            work_id: admission.work_id.clone(),
        };
        root.messages.push(Message {
            sequence: root.messages.last().map_or(Ok(1), |m| {
                m.sequence.checked_add(1).ok_or("message sequence overflow")
            })?,
            command_id: command_id.into(),
            sender: address.clone(),
            recipient: address,
            command: ControlCommand::Steer {
                expected_revision: previous,
                instructions: instructions.into(),
            },
            accepted_revision: revision,
        });
        // Old prepared delivery must not suppress the fresh process's full state.
        root.deliveries
            .entry(admission.work_id.clone())
            .or_default()
            .prepared = None;
        let handles = root
            .members
            .iter()
            .filter(|(id, m)| {
                id.as_str() == admission.work_id || m.parent.as_deref() == Some(&admission.work_id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        save(tx, &admission.campaign_id, &root)?;
        Ok((revision, handles))
    }

    pub(super) fn model_boundary_completed_in(
        tx: &WriteTransaction,
        admission: &Admission,
        id: &str,
        revision: u64,
    ) -> Result<(), String> {
        let mut root = load(tx, &admission.campaign_id)?;
        if let Some(delivery) = root.deliveries.get_mut(&admission.work_id) {
            if delivery.acknowledged_id.as_deref() != Some(id) || delivery.revision != revision {
                return Err(err("stale model boundary completion"));
            }
            delivery.model_revision = Some(revision);
            save(tx, &admission.campaign_id, &root)?;
        }
        Ok(())
    }

    pub(super) fn recognized_instruction_revision_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<Option<u64>, String> {
        let root = load(tx, &admission.campaign_id)?;
        Ok(match root.deliveries.get(&admission.work_id) {
            Some(delivery) => delivery.model_revision,
            None => Some(admission.instruction_revision),
        })
    }

    pub(super) fn effective_instruction_revision_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<u64, String> {
        Ok(load(tx, &admission.campaign_id)?
            .members
            .get(&admission.work_id)
            .map_or(admission.instruction_revision, |m| m.acknowledged_revision))
    }

    pub(super) fn latest_instruction_revision_in(
        tx: &WriteTransaction,
        admission: &Admission,
    ) -> Result<u64, String> {
        Ok(load(tx, &admission.campaign_id)?
            .members
            .get(&admission.work_id)
            .map_or(admission.instruction_revision, |m| m.accepted_revision))
    }

    /// Called under permit authority in the same transaction as request funding.
    /// No capacity means neither steering application nor delivery is committed.
    pub(super) fn prepare_agent_boundary_in(
        tx: &WriteTransaction,
        admission: &Admission,
        id: &str,
    ) -> Result<Option<tachyon_model::broker::Boundary>, String> {
        use tachyon_model::broker::{Boundary, ParentMessage};
        let mut root = load(tx, &admission.campaign_id)?;
        let Some(member) = root.members.get_mut(&admission.work_id) else {
            return Ok(None);
        };
        let delivery = root
            .deliveries
            .entry(admission.work_id.clone())
            .or_default();
        let unread: Vec<_> = root
            .messages
            .iter()
            .filter(|m| m.recipient.work_id == admission.work_id && m.sequence > delivery.cursor)
            .take(MAX_PAGE)
            .collect();
        let cursor = unread.last().map_or(delivery.cursor, |m| m.sequence);
        // Steering is replacement instruction state. Superseded accepted commands
        // remain history, but only the newest accepted revision becomes effective.
        let instructions = root.messages.iter().rev().find_map(|m| {
            if m.recipient.work_id == admission.work_id
                && m.accepted_revision == member.accepted_revision
            {
                if let ControlCommand::Steer { instructions, .. } = &m.command {
                    return Some(instructions.clone());
                }
            }
            None
        });
        let messages: Vec<_> = unread
            .iter()
            .filter_map(|m| match &m.command {
                ControlCommand::Send { text } => Some(ParentMessage {
                    sequence: m.sequence,
                    sender: m.sender.work_id.clone(),
                    text: text.clone(),
                }),
                _ => None,
            })
            .collect();
        for message in &messages {
            if !delivery
                .context_messages
                .iter()
                .any(|old| old.sequence == message.sequence)
            {
                delivery.context_messages.push(message.clone());
            }
        }
        delivery.context_messages.sort_by_key(|m| m.sequence);
        let excess = delivery.context_messages.len().saturating_sub(MAX_PAGE);
        delivery.context_messages.drain(..excess);
        let boundary = Boundary {
            objective: admission.objective.clone(),
            id: id.into(),
            cursor,
            instruction_revision: member.accepted_revision,
            instructions,
            messages,
            context_messages: delivery.context_messages.clone(),
        };
        if member.acknowledged_revision != member.accepted_revision {
            member.acknowledged_revision = member.accepted_revision;
            member.acknowledgments.push(member.accepted_revision);
        }
        delivery.prepared = Some(boundary.clone());
        save(tx, &admission.campaign_id, &root)?;
        Ok(Some(boundary))
    }

    pub(super) fn acknowledge_agent_boundary_in(
        tx: &WriteTransaction,
        admission: &Admission,
        id: &str,
        cursor: u64,
    ) -> Result<(), String> {
        let mut root = load(tx, &admission.campaign_id)?;
        let delivery = root
            .deliveries
            .get_mut(&admission.work_id)
            .ok_or_else(|| err("missing delivery"))?;
        if delivery.prepared.is_none()
            && delivery.acknowledged_id.as_deref() == Some(id)
            && delivery.cursor == cursor
        {
            return Ok(());
        }
        let prepared = delivery
            .prepared
            .as_ref()
            .ok_or_else(|| err("missing boundary"))?;
        if prepared.id != id || prepared.cursor != cursor {
            return Err(err("stale delivery acknowledgment"));
        }
        delivery.cursor = cursor;
        delivery.revision = prepared.instruction_revision;
        delivery.acknowledged_id = Some(id.into());
        delivery.prepared = None;
        save(tx, &admission.campaign_id, &root)
    }

    /// Host authorizes the exact admission AND parent relationship. New children
    /// are admitted atomically with mapping; existing unmapped work may only be
    /// enrolled while still queued. No group ancestry is consulted.
    pub(crate) fn host_admit_agent_work(
        &self,
        admission: Admission,
        parent: Option<WorkAddress>,
    ) -> Result<AdmittedWork, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let work = Self::host_admit_agent_work_in(&tx, admission, parent)?;
        tx.commit().map_err(err)?;
        Ok(work)
    }

    pub(super) fn host_admit_agent_work_in(
        tx: &WriteTransaction,
        admission: Admission,
        parent: Option<WorkAddress>,
    ) -> Result<AdmittedWork, String> {
        let campaign = admission.campaign_id.clone();
        let mut root = load(&tx, &campaign)?;
        let limits = Self::coordination_limits_in(&tx, &campaign)?;
        if let Some(p) = &parent {
            if p.campaign_id != campaign
                || p.work_id == admission.work_id
                || !root.members.contains_key(&p.work_id)
            {
                return Err(err("parent campaign/identity denied"));
            }
        }
        let parent = parent.map(|p| p.work_id);
        let mut next = parent.as_deref();
        let mut ancestors = std::collections::BTreeSet::new();
        while let Some(id) = next {
            if id == admission.work_id
                || !ancestors.insert(id)
                || ancestors.len() > limits.max_depth
            {
                return Err(err("work parent depth/cycle"));
            }
            next = root
                .members
                .get(id)
                .ok_or_else(|| err("unknown parent"))?
                .parent
                .as_deref();
        }
        if let Some(member) = root.members.get(&admission.work_id) {
            if member.parent != parent {
                return Err(err("immutable parent conflict"));
            }
        } else if root.members.len() >= MAX_WORK {
            return Err(err("campaign work capacity full"));
        }
        let work = Self::admit_campaign_work_in(&tx, admission)?;
        if !root.members.contains_key(&work.admission.work_id) {
            if work.state != DispatchState::Admitted {
                return Err(err("parent mapping requires queued admission"));
            }
            root.members.insert(
                work.admission.work_id.clone(),
                Member {
                    parent,
                    accepted_revision: work.admission.instruction_revision,
                    acknowledged_revision: work.admission.instruction_revision,
                    acknowledgments: Vec::new(),
                    commands: 0,
                },
            );
        }
        save(&tx, &campaign, &root)?;
        Ok(work)
    }

    pub(super) fn agent_descendants_in(
        tx: &WriteTransaction,
        campaign: &str,
        parent: &str,
    ) -> Result<Vec<String>, String> {
        let root = load(tx, campaign)?;
        let mut descendants = vec![parent.to_owned()];
        let mut index = 0;
        while index < descendants.len() {
            let children = root
                .members
                .iter()
                .filter(|(_, m)| m.parent.as_deref() == Some(descendants[index].as_str()))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            for child in children {
                if descendants.contains(&child) {
                    return Err(err("work parent cycle"));
                }
                descendants.push(child);
            }
            index += 1;
        }
        Ok(descendants.into_iter().skip(1).collect())
    }

    /// Caller has authenticated and authorized this exact actor. Never expose
    /// this factory through unauthenticated IPC, Python, or model tools.
    pub(crate) fn host_agent_control(
        &self,
        actor: WorkAddress,
    ) -> Result<HostAgentControl<'_>, String> {
        let tx = self.database.begin_write().map_err(err)?;
        let root = load(&tx, &actor.campaign_id)?;
        related(&root, &actor.work_id, &actor.work_id)?;
        Ok(HostAgentControl { store: self, actor })
    }

    /// Host has stopped delivery at a safe boundary and applied these instructions
    /// to its control state. This is NOT execution migration, permit issuance, or
    /// proof of inference under this revision. Admission remains immutable.
    #[cfg(test)]
    pub(crate) fn host_ack_agent_steering(
        &self,
        target: &WorkAddress,
        revision: u64,
    ) -> Result<(), String> {
        let tx = self.database.begin_write().map_err(err)?;
        let mut root = load(&tx, &target.campaign_id)?;
        let member = root
            .members
            .get_mut(&target.work_id)
            .ok_or_else(|| err("unknown target"))?;
        if member.acknowledgments.contains(&revision) {
            return Ok(());
        }
        if revision != member.accepted_revision || revision <= member.acknowledged_revision {
            return Err(err("stale steering acknowledgment"));
        }
        member.acknowledged_revision = revision;
        member.acknowledgments.push(revision);
        save(&tx, &target.campaign_id, &root)?;
        tx.commit().map_err(err)
    }
}

impl HostAgentControl<'_> {
    /// Current never falls back to historical evidence after steering.
    #[cfg(target_os = "linux")]
    pub(crate) fn result(
        &self,
        target: &WorkAddress,
        selection: ResultSelection,
    ) -> Result<Option<ResultReference>, String> {
        let mut result = self.status(target)?.result.filter(|r| match selection {
            ResultSelection::Current => r.current,
            ResultSelection::Revision(revision) => r.revision == revision,
        });
        if let Some(r) = result.as_mut() {
            let research = r.research.as_mut().unwrap();
            research.evidence_refs = self
                .store
                .context_work(&target.campaign_id, &target.work_id, None)?
                .into_iter()
                .filter(|resource| {
                    resource.reference.kind == tachyon_api::context::ResourceKind::Attempt
                        && resource.reference.id == r.attempt_id
                })
                .take(8)
                .map(|r| r.reference)
                .collect();
        }
        Ok(result)
    }
    pub(crate) fn command(
        &self,
        target: &WorkAddress,
        command_id: &str,
        command: ControlCommand,
    ) -> Result<Message, String> {
        if target.campaign_id != self.actor.campaign_id {
            return Err(err("campaign scope denied"));
        }
        let text = match &command {
            ControlCommand::Send { text } => text,
            ControlCommand::Steer { instructions, .. } => instructions,
        };
        if command_id.is_empty()
            || command_id.len() > 256
            || text.is_empty()
            || text.len() > MAX_TEXT
        {
            return Err(err("command size invalid"));
        }
        let tx = self.store.database.begin_write().map_err(err)?;
        let mut root = load(&tx, &self.actor.campaign_id)?;
        related(&root, &self.actor.work_id, &target.work_id)?;
        if self.actor == *target {
            return Err(err("self messaging denied"));
        }
        if let Some(old) = root.messages.iter().find(|m| m.command_id == command_id) {
            if old.sender != self.actor || old.recipient != *target || old.command != command {
                return Err(err("command ID payload conflict"));
            }
            return Ok(old.clone());
        }
        if root.messages.len() >= MAX_COMMANDS
            || [&self.actor.work_id, &target.work_id]
                .iter()
                .any(|id| root.members[*id].commands >= MAX_PER_WORK)
        {
            return Err(err("message lifetime capacity full"));
        }
        let member = root.members.get_mut(&target.work_id).unwrap();
        if let ControlCommand::Steer {
            expected_revision, ..
        } = &command
        {
            if member.parent.as_deref() != Some(&self.actor.work_id) {
                return Err(err("only direct parent may steer"));
            }
            if *expected_revision != member.accepted_revision {
                return Err(err("stale steering revision"));
            }
            member.accepted_revision = expected_revision
                .checked_add(1)
                .ok_or_else(|| err("revision exhausted"))?;
            RuntimeStore::relinquish_allocation_in(&tx, &target.campaign_id)?;
        }
        let message = Message {
            sequence: root.messages.len() as u64 + 1,
            command_id: command_id.into(),
            sender: self.actor.clone(),
            recipient: target.clone(),
            command,
            accepted_revision: member.accepted_revision,
        };
        for id in [&self.actor.work_id, &target.work_id] {
            root.members.get_mut(id).unwrap().commands += 1;
        }
        root.messages.push(message.clone());
        save(&tx, &self.actor.campaign_id, &root)?;
        tx.commit().map_err(err)?;
        Ok(message)
    }

    /// Inbox and outbox only; even a parent cannot read a child's other channels.
    pub(crate) fn messages(&self, after: u64, limit: usize) -> Result<Vec<Message>, String> {
        if !(1..=MAX_PAGE).contains(&limit) {
            return Err(err("page limit invalid"));
        }
        let tx = self.store.database.begin_write().map_err(err)?;
        let root = load(&tx, &self.actor.campaign_id)?;
        Ok(root
            .messages
            .iter()
            .filter(|m| m.sequence > after && (m.sender == self.actor || m.recipient == self.actor))
            .take(limit)
            .cloned()
            .collect())
    }

    pub(crate) fn list(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Page<WorkAddress>, String> {
        if !(1..=MAX_PAGE).contains(&limit) || after.is_some_and(|s| s.len() > 256) {
            return Err(err("page bound invalid"));
        }
        let tx = self.store.database.begin_write().map_err(err)?;
        let root = load(&tx, &self.actor.campaign_id)?;
        let mut ids = root.members.keys().filter(|id| {
            after.is_none_or(|a| id.as_str() > a) && related(&root, &self.actor.work_id, id).is_ok()
        });
        let items: Vec<_> = ids
            .by_ref()
            .take(limit)
            .map(|id| WorkAddress {
                campaign_id: self.actor.campaign_id.clone(),
                work_id: id.clone(),
            })
            .collect();
        let next_cursor = if ids.next().is_some() {
            items.last().map(|a| a.work_id.clone())
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    pub(crate) fn status(&self, target: &WorkAddress) -> Result<WorkStatus, String> {
        if target.campaign_id != self.actor.campaign_id {
            return Err(err("campaign scope denied"));
        }
        let tx = self.store.database.begin_write().map_err(err)?;
        let root = load(&tx, &target.campaign_id)?;
        related(&root, &self.actor.work_id, &target.work_id)?;
        let member = &root.members[&target.work_id];
        let work = RuntimeStore::admitted_work_in(&tx, &target.work_id)?;
        #[cfg(target_os = "linux")]
        let result = tx
            .open_table(super::execution::EXECUTIONS)
            .map_err(err)?
            .get(target.work_id.as_str())
            .map_err(err)?
            .map(|v| super::execution::decode_record(v.value(), &target.work_id))
            .transpose()?
            .map(|r| {
                use tachyon_api::{
                    agents::{ResearchResult, ResultUsage},
                    types::WorkOutcome,
                };
                use tachyon_model::accounting::RequestUsage;
                let mut research = ResearchResult {
                    availability: if r.candidate.is_some() {
                        "available"
                    } else if r.settled {
                        "unavailable"
                    } else {
                        "pending"
                    }
                    .into(),
                    accepted_revision: member.accepted_revision,
                    applied_revision: member.acknowledged_revision,
                    delivered_revision: root
                        .deliveries
                        .get(&target.work_id)
                        .map_or(0, |d| d.revision),
                    ..Default::default()
                };
                if let Some(candidate) = &r.candidate {
                    let (outcome, text) = match &candidate.outcome {
                        WorkOutcome::Completed { result, .. } => {
                            ("completed", Some(result.as_str()))
                        }
                        WorkOutcome::Failed { message } => ("failed", Some(message.as_str())),
                        WorkOutcome::Blocked { reason } => ("blocked", Some(reason.as_str())),
                        WorkOutcome::Cancelled { reason } => ("cancelled", Some(reason.as_str())),
                        WorkOutcome::TimedOut { .. } => ("timed_out", None),
                    };
                    research.outcome = Some(outcome.into());
                    research.summary = text.map(|text| {
                        let mut end = text.len().min(2048);
                        while !text.is_char_boundary(end) {
                            end -= 1;
                        }
                        research.truncated |= end < text.len();
                        text[..end].to_owned()
                    });
                    research.candidate_refs = candidate.candidate_refs.as_ref().map(|refs| {
                        research.truncated |=
                            refs.len() > 8 || refs.iter().any(|id| id.is_empty() || id.len() > 256);
                        refs.iter()
                            .filter(|id| !id.is_empty() && id.len() <= 256)
                            .take(8)
                            .cloned()
                            .collect()
                    });
                }
                for (allocation, output) in [
                    (&r.policy.funding.dispatch_id, &mut research.work_usage),
                    (
                        &r.policy.verification.dispatch_id,
                        &mut research.verification_usage,
                    ),
                ] {
                    let (mut input, mut output_tokens, mut cost) = (0u128, 0u128, 0u128);
                    let (mut seen, mut uncertain) = (false, !r.settled);
                    for row in tx
                        .open_table(super::model_accounting::REQUESTS)
                        .map_err(err)?
                        .iter()
                        .map_err(err)?
                    {
                        let (_, value) = row.map_err(err)?;
                        let request: super::model_accounting::Record =
                            serde_json::from_slice(value.value()).map_err(err)?;
                        if request.allocation_id.as_ref() != Some(allocation)
                            || request.request.identity.campaign_id != target.campaign_id
                        {
                            continue;
                        }
                        seen = true;
                        match request.usage {
                            RequestUsage::Unknown => uncertain = true,
                            RequestUsage::Final {
                                input_tokens,
                                output_tokens: tokens,
                                cost_micro_usd,
                            } => {
                                input += u128::from(input_tokens);
                                output_tokens += u128::from(tokens);
                                cost += u128::from(cost_micro_usd);
                            }
                        }
                    }
                    if seen {
                        *output = Some(ResultUsage {
                            input_tokens: input.to_string(),
                            output_tokens: output_tokens.to_string(),
                            cost_micro_usd: cost.to_string(),
                            uncertain,
                        });
                    }
                }
                Ok::<_, String>(ResultReference {
                    attempt_id: r.policy.model.identity.attempt_id.clone(),
                    research: Some(research),
                    rework_pending: !r.settled
                        && super::execution::command::retain_rejected_in(&tx, &r)?,
                    work: target.clone(),
                    revision: r
                        .candidate
                        .as_ref()
                        .and_then(|c| c.instruction_revision)
                        .unwrap_or(r.policy.model.identity.instruction_revision),
                    generation: r.policy.funding.admission.generation,
                    current: r
                        .candidate
                        .as_ref()
                        .and_then(|c| c.instruction_revision)
                        .unwrap_or(r.policy.model.identity.instruction_revision)
                        == member.accepted_revision,
                    phase: r.phase,
                    has_candidate: r.candidate.is_some(),
                    settled: r.settled,
                })
            })
            .transpose()?;
        Ok(WorkStatus {
            address: target.clone(),
            parent: member.parent.clone(),
            admission: work.state,
            generation: work.admission.generation,
            #[cfg(target_os = "linux")]
            execution_revision: result.as_ref().map(|r| r.revision),
            #[cfg(not(target_os = "linux"))]
            execution_revision: None,
            accepted_revision: member.accepted_revision,
            acknowledged_revision: member.acknowledged_revision,
            delivered_revision: root
                .deliveries
                .get(&target.work_id)
                .map_or(0, |d| d.revision),
            delivery_cursor: root.deliveries.get(&target.work_id).map_or(0, |d| d.cursor),
            #[cfg(target_os = "linux")]
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::campaign_ledger::{Envelope, Pool, Units};
    use super::*;
    use std::sync::{Arc, Barrier};
    use tachyon_api::types::{ApiRequest, ApiResponse};

    fn campaign(store: &RuntimeStore, id: &str) -> String {
        campaign_with_depth(store, id, 1)
    }

    fn campaign_with_depth(store: &RuntimeStore, id: &str, max_depth: usize) -> String {
        let ApiResponse::Research { research } = store
            .research_request(&ApiRequest::ResearchCreate {
                command_id: format!("r-{id}"),
                title: id.into(),
                objective: id.into(),
            })
            .unwrap()
        else {
            panic!()
        };
        let ApiResponse::Campaign { campaign } = store
            .research_request(&ApiRequest::CampaignCreate {
                command_id: format!("c-{id}"),
                research_id: research.id,
                title: id.into(),
                objective: id.into(),
            })
            .unwrap()
        else {
            panic!()
        };
        store
            .host_authorize_campaign_envelope(
                &format!("grant-{id}"),
                &campaign.id,
                Envelope {
                    work: Units {
                        tokens: 1000,
                        cost_micro_usd: 1000,
                    },
                    verification: Units::default(),
                    max_active_inferences: 10,
                },
            )
            .unwrap();
        store
            .host_configure_work_limits(
                &campaign.id,
                super::super::groups::WorkLimits {
                    total_work: MAX_WORK,
                    max_depth,
                    max_running: 10,
                    max_resident: MAX_WORK,
                },
            )
            .unwrap();
        campaign.id
    }
    fn admission(c: &str, w: &str) -> Admission {
        Admission {
            campaign_id: c.into(),
            work_id: w.into(),
            objective: "bounded".into(),
            instruction_revision: 1,
            generation: 1,
            pool: Pool::Work,
            upper_bound: Units {
                tokens: 1,
                cost_micro_usd: 1,
            },
        }
    }
    fn address(c: &str, w: &str) -> WorkAddress {
        WorkAddress {
            campaign_id: c.into(),
            work_id: w.into(),
        }
    }
    fn send() -> ControlCommand {
        ControlCommand::Send {
            text: "hello".into(),
        }
    }

    #[test]
    fn operator_continuation_replaces_pending_instructions_without_changing_goal_or_funding() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let c = campaign(&store, "continuation");
        store
            .host_admit_agent_work(admission(&c, "p"), None)
            .unwrap();
        let child = store
            .host_admit_agent_work(admission(&c, "child"), Some(address(&c, "p")))
            .unwrap();
        store
            .host_agent_control(address(&c, "p"))
            .unwrap()
            .command(
                &address(&c, "child"),
                "pending-steer",
                ControlCommand::Steer {
                    expected_revision: 1,
                    instructions: "previous pending instruction".into(),
                },
            )
            .unwrap();
        let before = store.campaign_ledger(&c).unwrap();
        let tx = store.database.begin_write().unwrap();
        assert_eq!(
            RuntimeStore::effective_instruction_revision_in(&tx, &child.admission).unwrap(),
            1
        );
        let (revision, _) = RuntimeStore::continuation_instruction_in(
            &tx,
            &child.admission,
            "operator",
            "complete replacement instruction",
        )
        .unwrap();
        assert_eq!(revision, 3);
        let boundary =
            RuntimeStore::prepare_agent_boundary_in(&tx, &child.admission, "fresh-boundary")
                .unwrap()
                .unwrap();
        assert_eq!(
            boundary.instructions.as_deref(),
            Some("complete replacement instruction")
        );
        assert_eq!(boundary.instruction_revision, 3);
        assert_eq!(boundary.objective, child.admission.objective);
        tx.commit().unwrap();
        assert_eq!(store.admitted_work(&c, "child").unwrap(), child);
        assert_eq!(store.campaign_ledger(&c).unwrap(), before);
    }

    #[test]
    fn parent_depth_and_missing_limits_reject_without_admission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(&store, "depth");
        store
            .host_admit_agent_work(admission(&c, "p"), None)
            .unwrap();
        store
            .host_admit_agent_work(admission(&c, "child"), Some(address(&c, "p")))
            .unwrap();
        let before = store.campaign_ledger(&c).unwrap();
        assert!(store
            .host_admit_agent_work(admission(&c, "deep"), Some(address(&c, "child")))
            .unwrap_err()
            .contains("depth/cycle"));
        assert!(store
            .host_admit_agent_work(admission(&c, "p"), Some(address(&c, "child")))
            .is_err());
        assert!(store.admitted_work(&c, "deep").is_err());
        assert_eq!(store.campaign_ledger(&c).unwrap(), before);
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        assert!(store
            .host_admit_agent_work(admission(&c, "deep"), Some(address(&c, "child")))
            .is_err());

        let c = campaign(&store, "no-limits");
        // Fixture: authorized envelope with no optional scheduler root configured.
        let tx = store.database.begin_write().unwrap();
        tx.open_table(TableDefinition::<&str, &[u8]>::new("campaign_work_limits"))
            .unwrap()
            .remove(c.as_str())
            .unwrap();
        tx.commit().unwrap();
        let before = store.campaign_ledger(&c).unwrap();
        assert!(store
            .host_admit_agent_work(admission(&c, "unbounded"), None)
            .unwrap_err()
            .contains("host work limits required"));
        assert!(store.admitted_work(&c, "unbounded").is_err());
        assert_eq!(store.campaign_ledger(&c).unwrap(), before);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn result_reference_stays_historical_after_late_evidence_and_reopen() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(&store, "results");
        let p = address(&c, "parent");
        let child = address(&c, "child");
        store
            .host_admit_agent_work(admission(&c, "parent"), None)
            .unwrap();
        let funding = store
            .host_admit_agent_work(admission(&c, "child"), Some(p.clone()))
            .unwrap();
        // Storage fixture only: no launch, model, evaluator or funding transfer.
        let mut record = json!({
            "schema_version": 1,
            "policy": {
                "funding": funding, "verification": funding, "evaluator_id": "fixture",
                "work": {"work_id": "child", "objective": "bounded", "generation": 1, "assignment": 1, "deadline_ms": 1, "lifetime_class": "short"},
                "model": {
                    "identity": {"campaign_id": c, "work_id": "child", "attempt_id": "attempt", "generation": 1, "instruction_revision": 1, "class": "Work"},
                    "estimate": {"base_url": "https://example.invalid", "model": "fake", "provider": "fake", "pricing_revision": "1", "max_request_bytes": 1, "input_tokens": 1, "output_tokens": 1, "input_micro_usd_per_million": 0, "output_micro_usd_per_million": 0, "other_micro_usd": 0}
                }
            },
            "phase": "ExecutingUnknown", "candidate": null, "settled": false
        });
        record["policy"]["verification"]["dispatch_id"] = json!("verification-allocation");
        let persist = |value: &serde_json::Value| {
            let tx = store.database.begin_write().unwrap();
            tx.open_table(super::super::execution::EXECUTIONS)
                .unwrap()
                .insert("child", serde_json::to_vec(value).unwrap().as_slice())
                .unwrap();
            tx.commit().unwrap();
        };
        persist(&record);
        let control = store.host_agent_control(p.clone()).unwrap();
        assert!(control.status(&child).unwrap().result.unwrap().current);
        let pending = control
            .result(&child, ResultSelection::Current)
            .unwrap()
            .unwrap()
            .research
            .unwrap();
        assert_eq!(pending.availability, "pending");
        assert!(pending.summary.is_none() && pending.work_usage.is_none());
        record["candidate"] = json!({
            "work_id":"child", "objective":"bounded", "generation":1, "assignment":1,
            "outcome":"failed", "message":"actual error: permission denied"
        });
        persist(&record);
        let failed = control
            .result(&child, ResultSelection::Current)
            .unwrap()
            .unwrap()
            .research
            .unwrap();
        assert_eq!(failed.outcome.as_deref(), Some("failed"));
        assert_eq!(
            failed.summary.as_deref(),
            Some("actual error: permission denied")
        );
        assert!(failed.unresolved_questions.is_none());
        record["candidate"] = serde_json::Value::Null;
        persist(&record);
        control
            .command(
                &child,
                "steer",
                ControlCommand::Steer {
                    expected_revision: 1,
                    instructions: "revised".into(),
                },
            )
            .unwrap();
        assert!(!control.status(&child).unwrap().result.unwrap().current);
        assert_eq!(control.status(&child).unwrap().acknowledged_revision, 1);
        store.host_ack_agent_steering(&child, 2).unwrap();
        record["phase"] = json!({"Reviewed": "Unverified"});
        record["candidate"] = json!({
            "work_id":"child", "objective":"bounded", "generation":1, "assignment":1,
            "outcome":"completed", "result":"\u{e9}".repeat(3000),
            "candidate_refs":["immutable-candidate"],
            "evidence":{"tools":[],"omitted":9000}
        });
        persist(&record);
        let result = control.status(&child).unwrap().result.unwrap();
        assert!(!result.current);
        assert_eq!(result.revision, 1);
        assert!(control
            .result(&child, ResultSelection::Current)
            .unwrap()
            .is_none());
        assert!(control
            .result(&child, ResultSelection::Revision(1))
            .unwrap()
            .is_some());
        assert!(control
            .result(&child, ResultSelection::Revision(2))
            .unwrap()
            .is_none());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let control = store.host_agent_control(p).unwrap();
        let result = control
            .result(&child, ResultSelection::Revision(1))
            .unwrap()
            .unwrap();
        let research = result.research.unwrap();
        assert_eq!(research.summary.as_ref().unwrap().len(), 2048);
        assert!(research.truncated);
        assert_eq!(research.candidate_refs.unwrap(), ["immutable-candidate"]);
        assert!(!research.evidence_refs.is_empty());
        assert!(research.unresolved_questions.is_none());
        assert!(research.work_usage.is_none());
        let tx = store.database.begin_read().unwrap();
        let table = tx.open_table(super::super::execution::EXECUTIONS).unwrap();
        let stored: serde_json::Value =
            serde_json::from_slice(table.get("child").unwrap().unwrap().value()).unwrap();
        assert_eq!(stored["candidate"]["result"].as_str().unwrap().len(), 6000);
        let status = control.status(&child).unwrap();
        assert_eq!(
            (
                status.accepted_revision,
                status.acknowledged_revision,
                status.generation
            ),
            (2, 2, 1)
        );
        assert!(!status.result.unwrap().current);
        store.host_ack_agent_steering(&child, 2).unwrap();
        drop(table);
        drop(tx);
        // Authoritative accounting storage fixture: no provider or worker telemetry.
        let tx = store.database.begin_write().unwrap();
        let mut requests = tx
            .open_table(super::super::model_accounting::REQUESTS)
            .unwrap();
        for (id, usage) in [
            ("unknown", json!("Unknown")),
            (
                "final-1",
                json!({"Final":{"input_tokens":u64::MAX,"output_tokens":0,"cost_micro_usd":u64::MAX}}),
            ),
            (
                "final-2",
                json!({"Final":{"input_tokens":u64::MAX,"output_tokens":0,"cost_micro_usd":u64::MAX}}),
            ),
        ] {
            requests
                .insert(
                    id,
                    serde_json::to_vec(&json!({
                        "schema_version":1, "request":record["policy"]["model"],
                        "allocation_id":record["policy"]["funding"]["dispatch_id"], "usage":usage
                    }))
                    .unwrap()
                    .as_slice(),
                )
                .unwrap();
        }
        drop(requests);
        tx.commit().unwrap();
        let research = control
            .result(&child, ResultSelection::Revision(1))
            .unwrap()
            .unwrap()
            .research
            .unwrap();
        let usage = research.work_usage.unwrap();
        assert_eq!(usage.cost_micro_usd, (u128::from(u64::MAX) * 2).to_string());
        assert_eq!(usage.input_tokens, (u128::from(u64::MAX) * 2).to_string());
        assert_eq!(usage.output_tokens, "0");
        assert!(usage.uncertain);
        assert!(research.verification_usage.is_none());

        let tx = store.database.begin_write().unwrap();
        let mut requests = tx
            .open_table(super::super::model_accounting::REQUESTS)
            .unwrap();
        for id in ["unknown", "final-1", "final-2"] {
            requests.remove(id).unwrap();
        }
        for (id, allocation, cost) in [
            (
                "zero",
                record["policy"]["funding"]["dispatch_id"].clone(),
                0,
            ),
            ("verification", json!("verification-allocation"), 21),
        ] {
            requests
                .insert(
                    id,
                    serde_json::to_vec(&json!({
                        "schema_version":1, "request":record["policy"]["model"],
                        "allocation_id":allocation,
                        "usage":{"Final":{"input_tokens":0,"output_tokens":0,"cost_micro_usd":cost}}
                    }))
                    .unwrap()
                    .as_slice(),
                )
                .unwrap();
        }
        drop(requests);
        record["settled"] = json!(true);
        tx.open_table(super::super::execution::EXECUTIONS)
            .unwrap()
            .insert("child", serde_json::to_vec(&record).unwrap().as_slice())
            .unwrap();
        tx.commit().unwrap();
        let research = control
            .result(&child, ResultSelection::Revision(1))
            .unwrap()
            .unwrap()
            .research
            .unwrap();
        let usage = research.work_usage.unwrap();
        assert_eq!(usage.cost_micro_usd, "0");
        assert!(!usage.uncertain);
        assert_eq!(research.verification_usage.unwrap().cost_micro_usd, "21");
    }

    #[test]
    fn campaign_lifetime_cap_rejects_without_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let c = campaign(&store, "lifetime");
        let p = address(&c, "p");
        let child = address(&c, "child");
        store
            .host_admit_agent_work(admission(&c, "p"), None)
            .unwrap();
        store
            .host_admit_agent_work(admission(&c, "child"), Some(p.clone()))
            .unwrap();
        // Fill independent channels in one storage fixture to isolate the root cap
        // from the separately tested per-Work limit.
        let tx = store.database.begin_write().unwrap();
        let mut root = load(&tx, &c).unwrap();
        for pair in 0..16 {
            let parent = format!("parent-{pair}");
            let target = format!("target-{pair}");
            for (id, parent) in [
                (parent.clone(), None),
                (target.clone(), Some(parent.clone())),
            ] {
                root.members.insert(
                    id,
                    Member {
                        parent,
                        accepted_revision: 1,
                        acknowledged_revision: 1,
                        acknowledgments: Vec::new(),
                        commands: MAX_PER_WORK,
                    },
                );
            }
            for n in 0..MAX_PER_WORK {
                root.messages.push(Message {
                    sequence: root.messages.len() as u64 + 1,
                    command_id: format!("{pair}-{n}"),
                    sender: address(&c, &parent),
                    recipient: address(&c, &target),
                    command: send(),
                    accepted_revision: 1,
                });
            }
        }
        save(&tx, &c, &root).unwrap();
        tx.commit().unwrap();
        assert!(store
            .host_agent_control(p)
            .unwrap()
            .command(&child, "overflow", send())
            .unwrap_err()
            .contains("full"));
        let tx = store.database.begin_write().unwrap();
        assert_eq!(load(&tx, &c).unwrap().messages.len(), MAX_COMMANDS);
    }

    #[test]
    fn list_and_result_use_direct_relations_not_campaign_membership() {
        let dir = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap();
        let c = campaign_with_depth(&store, "scope", 3);
        for (id, parent) in [
            ("root", None),
            ("actor", Some("root")),
            ("sibling", Some("root")),
            ("child", Some("actor")),
            ("grandchild", Some("child")),
            ("stranger", None),
        ] {
            store
                .host_admit_agent_work(admission(&c, id), parent.map(|p| address(&c, p)))
                .unwrap();
        }
        let control = store.host_agent_control(address(&c, "actor")).unwrap();
        let page = control.list(None, 32).unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|a| a.work_id.as_str())
                .collect::<Vec<_>>(),
            ["actor", "child", "root"]
        );
        assert!(page.next_cursor.is_none());
        for id in [
            "actor",
            "child",
            "root",
            "sibling",
            "grandchild",
            "stranger",
        ] {
            let allowed = matches!(id, "actor" | "child" | "root");
            assert_eq!(control.status(&address(&c, id)).is_ok(), allowed);
            #[cfg(target_os = "linux")]
            assert_eq!(
                control
                    .result(&address(&c, id), ResultSelection::Current)
                    .is_ok(),
                allowed
            );
        }
        control
            .command(&address(&c, "child"), "shared", send())
            .unwrap();
        // Command IDs are campaign-wide: neither another target nor sender can reuse one.
        assert!(control
            .command(&address(&c, "root"), "shared", send())
            .is_err());
        assert!(store
            .host_agent_control(address(&c, "child"))
            .unwrap()
            .command(&address(&c, "actor"), "shared", send())
            .is_err());
    }

    #[test]
    fn scope_replay_reopen_capacity_and_immutable_parent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.redb");
        let store = RuntimeStore::open(&path).unwrap();
        let c = campaign(&store, "one");
        let other = campaign(&store, "two");
        let p = address(&c, "p");
        let child = address(&c, "child");
        for id in ["p", "stranger"] {
            store
                .host_admit_agent_work(admission(&c, id), None)
                .unwrap();
        }
        store
            .host_admit_agent_work(admission(&c, "child"), Some(p.clone()))
            .unwrap();
        assert!(store
            .host_admit_agent_work(admission(&c, "child"), None)
            .is_err());
        assert!(store
            .host_admit_agent_work(admission(&other, "foreign"), Some(p.clone()))
            .is_err());
        assert!(store.admitted_work(&other, "foreign").is_err());
        let control = store.host_agent_control(p.clone()).unwrap();
        let first = control.command(&child, "m", send()).unwrap();
        assert_eq!(first, control.command(&child, "m", send()).unwrap());
        assert!(control
            .command(
                &child,
                "m",
                ControlCommand::Send {
                    text: "changed".into()
                }
            )
            .is_err());
        assert!(control
            .command(&address(&other, "child"), "x", send())
            .is_err());
        assert!(store
            .host_agent_control(address(&c, "stranger"))
            .unwrap()
            .status(&child)
            .is_err());
        assert!(store
            .host_agent_control(address(&c, "stranger"))
            .unwrap()
            .command(&child, "x", send())
            .is_err());
        let page = control.list(None, 1).unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            control
                .list(page.next_cursor.as_deref(), 1)
                .unwrap()
                .items
                .len(),
            1
        );
        for n in 1..MAX_PER_WORK {
            control.command(&child, &format!("m{n}"), send()).unwrap();
        }
        assert!(control
            .command(&child, "full", send())
            .unwrap_err()
            .contains("full"));
        assert_eq!(first, control.command(&child, "m", send()).unwrap());
        drop(store);
        let store = RuntimeStore::open(&path).unwrap();
        let control = store.host_agent_control(p).unwrap();
        assert_eq!(first, control.command(&child, "m", send()).unwrap());
        assert_eq!(control.messages(0, 1).unwrap(), vec![first]);
        assert_eq!(control.messages(255, 32).unwrap().len(), 1);
        assert!(control.command(&child, "still-full", send()).is_err());
    }

    #[test]
    fn concurrent_steering_one_wins_ack_separate_no_admission_migration() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(RuntimeStore::open(&dir.path().join("runtime.redb")).unwrap());
        let c = campaign(&store, "race");
        let p = address(&c, "p");
        let child = address(&c, "child");
        store
            .host_admit_agent_work(admission(&c, "p"), None)
            .unwrap();
        let original = store
            .host_admit_agent_work(admission(&c, "child"), Some(p.clone()))
            .unwrap();
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let (store, barrier, p, child) =
                    (store.clone(), barrier.clone(), p.clone(), child.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    store.host_agent_control(p).unwrap().command(
                        &child,
                        &format!("s{i}"),
                        ControlCommand::Steer {
                            expected_revision: 1,
                            instructions: "new instructions".into(),
                        },
                    )
                })
            })
            .collect();
        let wins: Vec<_> = threads
            .into_iter()
            .filter_map(|t| t.join().unwrap().ok())
            .collect();
        assert_eq!(wins.len(), 1);
        let control = store.host_agent_control(p).unwrap();
        let status = control.status(&child).unwrap();
        assert_eq!(
            (
                status.accepted_revision,
                status.acknowledged_revision,
                status.execution_revision
            ),
            (2, 1, None)
        );
        assert_eq!(
            control
                .command(&child, &wins[0].command_id, wins[0].command.clone())
                .unwrap(),
            wins[0]
        );
        assert!(store.host_ack_agent_steering(&child, 3).is_err());
        store.host_ack_agent_steering(&child, 2).unwrap();
        store.host_ack_agent_steering(&child, 2).unwrap();
        assert!(store.host_ack_agent_steering(&child, 1).is_err());
        control
            .command(
                &child,
                "next",
                ControlCommand::Steer {
                    expected_revision: 2,
                    instructions: "third revision".into(),
                },
            )
            .unwrap();
        store.host_ack_agent_steering(&child, 3).unwrap();
        store.host_ack_agent_steering(&child, 2).unwrap();
        assert_eq!(control.status(&child).unwrap().acknowledged_revision, 3);
        assert_eq!(store.admitted_work(&c, "child").unwrap(), original);
        let mut illegal = admission(&c, "child");
        illegal.instruction_revision = 2;
        assert!(store
            .host_admit_agent_work(illegal, Some(address(&c, "p")))
            .is_err());
    }
}
