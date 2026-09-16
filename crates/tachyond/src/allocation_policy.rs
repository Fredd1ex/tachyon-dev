//! Pure proposals; the host supports resize and separately configured finite,
//! exact Work/Verify/Pause/Stop signals. Proposals alone grant no authority.
//! Reduced host projection: API Campaign is draft metadata; durable group/ledger
//! state remains owned by runtime_store. IDs must refer to that host-owned state.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

pub const MAX_RECORDS: usize = 4096;
pub const MAX_ACTIONS: usize = 32;
pub const MAX_ACTIVE: u32 = 128;
pub const MAX_OUTCOMES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Fence {
    pub campaign_id: String,
    pub group_id: String,
    pub group_revision: u64,
    pub controller_id: String,
    /// Host must advance this on every user steering change, even pause/resume.
    pub steering_revision: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Allowance {
    pub money_micros: u64,
    pub inference_units: u64,
}

impl Allowance {
    fn subtract(self, cost: Self) -> Option<Self> {
        Some(Self {
            money_micros: self.money_micros.checked_sub(cost.money_micros)?,
            inference_units: self.inference_units.checked_sub(cost.inference_units)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectiveKind {
    Work,
    Verify,
}

/// Complete admission specs stay with the host. This is only an explicit queued
/// identity and its host-approved allowance, never a generated objective.
#[derive(Clone, Debug)]
pub struct QueuedObjective {
    pub work_id: String,
    pub kind: ObjectiveKind,
    pub allowance: Allowance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecentOutcome {
    Useful,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Steering {
    Run,
    Pause,
    Stop,
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceAvailability {
    /// Effective root/ancestor/host ceiling, independent of logical record count.
    pub active_cap: u32,
    /// Additional physical execution slots; unknown capacity is represented as 0.
    pub free_slots: u32,
    /// Remaining lifetime admission slots; terminal records do not refund these.
    pub remaining_work: u32,
}

pub struct PolicySnapshot<'a> {
    pub fence: Fence,
    pub steering: Steering,
    pub queued: &'a [QueuedObjective],
    /// Host supplies a bounded recent window, not the entire event history.
    pub recent: &'a [RecentOutcome],
    pub remaining_budget: Allowance,
    pub resources: ResourceAvailability,
    pub active: u32,
    pub current_limit: u32,
    pub action_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Work {
        work_id: String,
        allowance: Allowance,
    },
    Verify {
        work_id: String,
        allowance: Allowance,
    },
    /// Changes concurrency within existing allowance, never increases funding.
    Resize {
        max_running: u32,
    },
    Pause,
    Stop,
}

/// A suggestion only. The host must recheck the fence and perform authoritative
/// admission/CAS/resource checks atomically; this object grants no budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal {
    pub fence: Fence,
    pub actions: Vec<Action>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    InvalidSnapshot,
    StaleFence,
    InvalidProposal,
    InsufficientResources,
    UnsupportedAction,
}

pub trait AllocationPolicy {
    fn propose(&self, snapshot: &PolicySnapshot<'_>) -> Result<Proposal, PolicyError>;
}

pub struct FixedAllocation {
    pub max_running: u32,
}

/// The caller obtains explicit model output elsewhere. No model calls here.
pub struct ModelProposed {
    pub proposal: Proposal,
}

pub struct DeterministicAdaptive;

/// Already-admitted queue: allowances are existing holds, not fresh root budget.
/// No Work/Verify actions are emitted or silently discarded by this adapter.
pub fn propose_resize(s: &PolicySnapshot<'_>) -> Result<Proposal, PolicyError> {
    check_snapshot(s)?;
    if s.steering != Steering::Run {
        return Err(PolicyError::UnsupportedAction);
    }
    let mut target = s.current_limit;
    if s.recent.contains(&RecentOutcome::Failed) {
        target = target.saturating_sub(1).max(1);
    } else if s.recent.contains(&RecentOutcome::Useful)
        && s.resources.free_slots > 0
        && s.queued
            .iter()
            .any(|q| s.remaining_budget.subtract(q.allowance).is_some())
        && s.active
            .saturating_add((s.queued.len() as u32).min(s.resources.free_slots))
            > target
    {
        target = target.saturating_add(1);
    }
    target = target.min(s.resources.active_cap);
    let p = Proposal {
        fence: s.fence.clone(),
        actions: if target == s.current_limit {
            vec![]
        } else {
            vec![Action::Resize {
                max_running: target,
            }]
        },
    };
    validate_resize(s, &p)
}

pub fn validate_resize(s: &PolicySnapshot<'_>, p: &Proposal) -> Result<Proposal, PolicyError> {
    check_snapshot(s)?;
    if p.fence != s.fence {
        return Err(PolicyError::StaleFence);
    }
    if p.actions
        .iter()
        .any(|a| !matches!(a, Action::Resize { .. }))
    {
        return Err(PolicyError::UnsupportedAction);
    }
    if s.steering != Steering::Run || p.actions.len() > 1 {
        return Err(PolicyError::InvalidProposal);
    }
    for action in &p.actions {
        let Action::Resize { max_running } = action else {
            unreachable!()
        };
        if *max_running > s.resources.active_cap {
            return Err(PolicyError::InsufficientResources);
        }
        if *max_running > s.current_limit
            && (s.resources.free_slots == 0
                || *max_running > s.active.saturating_add(s.queued.len() as u32)
                || *max_running > s.active.saturating_add(s.resources.free_slots)
                || !s
                    .queued
                    .iter()
                    .any(|q| s.remaining_budget.subtract(q.allowance).is_some()))
        {
            return Err(PolicyError::InsufficientResources);
        }
    }
    Ok(p.clone())
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256 && !id.trim().is_empty()
}

fn check_snapshot(s: &PolicySnapshot<'_>) -> Result<(), PolicyError> {
    if !valid_id(&s.fence.campaign_id)
        || !valid_id(&s.fence.group_id)
        || !valid_id(&s.fence.controller_id)
        || s.queued.len() > MAX_RECORDS
        || s.recent.len() > MAX_OUTCOMES
        || s.action_limit == 0
        || s.action_limit > MAX_ACTIONS
        || s.resources.active_cap > MAX_ACTIVE
        || s.resources.free_slots > MAX_ACTIVE
        || s.resources.remaining_work > MAX_RECORDS as u32
        || s.active > MAX_ACTIVE
        || s.current_limit > MAX_ACTIVE
    {
        return Err(PolicyError::InvalidSnapshot);
    }
    let mut ids = HashSet::with_capacity(s.queued.len());
    for objective in s.queued {
        if !valid_id(&objective.work_id) || !ids.insert(&objective.work_id) {
            return Err(PolicyError::InvalidSnapshot);
        }
    }
    Ok(())
}

fn steering(s: &PolicySnapshot<'_>) -> Option<Proposal> {
    let action = match s.steering {
        Steering::Run => return None,
        Steering::Pause => Action::Pause,
        Steering::Stop => Action::Stop,
    };
    Some(Proposal {
        fence: s.fence.clone(),
        actions: vec![action],
    })
}

/// Shared validator for every implementation, also usable at an adapter boundary.
/// User steering supersedes policy content, but cannot revive a stale proposal.
pub fn validate(s: &PolicySnapshot<'_>, p: &Proposal) -> Result<Proposal, PolicyError> {
    check_snapshot(s)?;
    if p.fence != s.fence {
        return Err(PolicyError::StaleFence);
    }
    if let Some(override_proposal) = steering(s) {
        return Ok(override_proposal);
    }
    if p.actions.len() > s.action_limit {
        return Err(PolicyError::InvalidProposal);
    }
    let queued: HashMap<_, _> = s.queued.iter().map(|q| (q.work_id.as_str(), q)).collect();
    let mut used = HashSet::new();
    let mut remaining = s.remaining_budget;
    let mut limit = s.current_limit.min(s.resources.active_cap);
    let mut resized = false;
    let mut count = 0u32;
    for action in &p.actions {
        match action {
            Action::Pause | Action::Stop => {
                if p.actions.len() != 1 {
                    return Err(PolicyError::InvalidProposal);
                }
            }
            Action::Resize { max_running } => {
                if resized || *max_running > s.resources.active_cap {
                    return Err(PolicyError::InvalidProposal);
                }
                resized = true;
                limit = *max_running;
            }
            Action::Work { work_id, allowance } | Action::Verify { work_id, allowance } => {
                if !valid_id(work_id) {
                    return Err(PolicyError::InvalidProposal);
                }
                let q = queued
                    .get(work_id.as_str())
                    .ok_or(PolicyError::InvalidProposal)?;
                let kind = if matches!(action, Action::Work { .. }) {
                    ObjectiveKind::Work
                } else {
                    ObjectiveKind::Verify
                };
                if q.kind != kind || q.allowance != *allowance || !used.insert(work_id) {
                    return Err(PolicyError::InvalidProposal);
                }
                remaining = remaining
                    .subtract(*allowance)
                    .ok_or(PolicyError::InsufficientResources)?;
                count += 1; // At most MAX_ACTIONS, checked before this loop.
            }
        }
    }
    if (resized && limit > s.current_limit && count == 0)
        || count > limit.saturating_sub(s.active)
        || count > s.resources.free_slots
        || count > s.resources.remaining_work
    {
        return Err(PolicyError::InsufficientResources);
    }
    Ok(p.clone())
}

fn allocate(s: &PolicySnapshot<'_>, target: u32) -> Result<Proposal, PolicyError> {
    check_snapshot(s)?;
    if let Some(p) = steering(s) {
        return Ok(p);
    }
    if target > MAX_ACTIVE {
        return Err(PolicyError::InvalidProposal);
    }
    let target = target.min(s.resources.active_cap);
    let mut p = Proposal {
        fence: s.fence.clone(),
        actions: Vec::new(),
    };
    if target != s.current_limit {
        p.actions.push(Action::Resize {
            max_running: target,
        });
    }
    let slots = target
        .saturating_sub(s.active)
        .min(s.resources.free_slots)
        .min(s.resources.remaining_work) as usize;
    let mut remaining = s.remaining_budget;
    let mut count = 0;
    for q in s.queued {
        if count == slots || p.actions.len() == s.action_limit {
            break;
        }
        let Some(next) = remaining.subtract(q.allowance) else {
            continue;
        };
        remaining = next;
        let action = match q.kind {
            ObjectiveKind::Work => Action::Work {
                work_id: q.work_id.clone(),
                allowance: q.allowance,
            },
            ObjectiveKind::Verify => Action::Verify {
                work_id: q.work_id.clone(),
                allowance: q.allowance,
            },
        };
        p.actions.push(action);
        count += 1;
    }
    // Do not grow concurrency without a funded explicit objective to use it.
    if target > s.current_limit && count == 0 {
        p.actions.clear();
    }
    validate(s, &p)
}

impl AllocationPolicy for FixedAllocation {
    fn propose(&self, s: &PolicySnapshot<'_>) -> Result<Proposal, PolicyError> {
        allocate(s, self.max_running)
    }
}

impl AllocationPolicy for ModelProposed {
    fn propose(&self, s: &PolicySnapshot<'_>) -> Result<Proposal, PolicyError> {
        validate(s, &self.proposal)
    }
}

impl AllocationPolicy for DeterministicAdaptive {
    fn propose(&self, s: &PolicySnapshot<'_>) -> Result<Proposal, PolicyError> {
        check_snapshot(s)?;
        // Conservative deterministic additive growth; any failure wins. Unknown
        // outcomes are not evidence of usefulness and never trigger expansion.
        let target = if s.recent.contains(&RecentOutcome::Failed) {
            s.current_limit.saturating_sub(1)
        } else if s.recent.contains(&RecentOutcome::Useful) {
            s.current_limit.saturating_add(1).min(MAX_ACTIVE)
        } else {
            s.current_limit
        };
        allocate(s, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(n: usize) -> Vec<QueuedObjective> {
        (0..n)
            .map(|i| QueuedObjective {
                work_id: format!("work-{i}"),
                kind: if i % 2 == 0 {
                    ObjectiveKind::Work
                } else {
                    ObjectiveKind::Verify
                },
                allowance: Allowance {
                    money_micros: 3,
                    inference_units: 2,
                },
            })
            .collect()
    }

    #[test]
    fn resize_only_admitted_queue_comparison_and_unsupported_actions() {
        for n in [1, 8, 32, 128, 1000] {
            let q = queue(n);
            let mut s = snapshot(&q);
            s.current_limit = 1;
            s.resources.active_cap = 8;
            s.resources.remaining_work = 0;
            let start = std::time::Instant::now();
            let p = propose_resize(&s).unwrap();
            assert_eq!(
                p.actions,
                if n == 1 {
                    vec![]
                } else {
                    vec![Action::Resize { max_running: 2 }]
                }
            );
            assert_eq!(q.len(), n);
            eprintln!(
                "resize-only records={n} fixed=1 adaptive={:?} elapsed={:?}",
                p.actions,
                start.elapsed()
            );
            assert!(p.actions.len() <= 1);
            for action in [
                Action::Pause,
                Action::Stop,
                Action::Work {
                    work_id: q[0].work_id.clone(),
                    allowance: q[0].allowance,
                },
                Action::Verify {
                    work_id: q[0].work_id.clone(),
                    allowance: q[0].allowance,
                },
            ] {
                assert_eq!(
                    validate_resize(
                        &s,
                        &Proposal {
                            fence: s.fence.clone(),
                            actions: vec![action]
                        }
                    ),
                    Err(PolicyError::UnsupportedAction)
                );
            }
            s.resources.free_slots = 0;
            assert!(propose_resize(&s).unwrap().actions.is_empty());
            s.resources.free_slots = 8;
            s.remaining_budget = Allowance::default();
            assert!(propose_resize(&s).unwrap().actions.is_empty());
            s.current_limit = 3;
            s.active = 3;
            s.recent = &[RecentOutcome::Failed];
            assert_eq!(
                propose_resize(&s).unwrap().actions,
                vec![Action::Resize { max_running: 2 }]
            );
            s.recent = &[RecentOutcome::Unknown];
            assert!(propose_resize(&s).unwrap().actions.is_empty());
        }
    }

    fn snapshot(queued: &[QueuedObjective]) -> PolicySnapshot<'_> {
        PolicySnapshot {
            fence: Fence {
                campaign_id: "campaign-a".into(),
                group_id: "group-a".into(),
                group_revision: 7,
                controller_id: "user-a".into(),
                steering_revision: 9,
            },
            steering: Steering::Run,
            queued,
            recent: &[RecentOutcome::Useful],
            remaining_budget: Allowance {
                money_micros: 100,
                inference_units: 100,
            },
            resources: ResourceAvailability {
                active_cap: 8,
                free_slots: 8,
                remaining_work: 4096,
            },
            active: 0,
            current_limit: 3,
            action_limit: MAX_ACTIONS,
        }
    }

    #[test]
    fn synthetic_logical_records_are_not_active_executions() {
        for n in [1, 8, 32, 128, 1000] {
            let q = queue(n);
            let s = snapshot(&q);
            let start = std::time::Instant::now();
            let p = FixedAllocation { max_running: 8 }.propose(&s).unwrap();
            let actions = p
                .actions
                .iter()
                .filter(|a| matches!(a, Action::Work { .. } | Action::Verify { .. }))
                .count();
            assert_eq!(actions, n.min(8));
            assert!(p.actions.len() <= MAX_ACTIONS);
            assert_eq!(validate(&s, &p), Ok(p.clone()));
            eprintln!(
                "synthetic records={n} active_cap=8 proposals={} elapsed={:?}; not a real swarm",
                p.actions.len(),
                start.elapsed()
            );
        }
    }

    #[test]
    fn adaptive_is_additive_and_only_uses_funded_explicit_queue() {
        let q = queue(32);
        let mut s = snapshot(&q);
        let p = DeterministicAdaptive.propose(&s).unwrap();
        assert_eq!(p.actions[0], Action::Resize { max_running: 4 });
        assert_eq!(p.actions.len(), 5);
        assert_eq!(DeterministicAdaptive.propose(&s).unwrap(), p);
        s.remaining_budget = Allowance::default();
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.remaining_budget = Allowance {
            money_micros: 100,
            inference_units: 1,
        };
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.queued = &[];
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.recent = &[RecentOutcome::Useful, RecentOutcome::Failed];
        assert_eq!(
            DeterministicAdaptive.propose(&s).unwrap().actions,
            vec![Action::Resize { max_running: 2 }]
        );
    }

    #[test]
    fn model_output_is_untrusted_and_fenced() {
        let q = queue(8);
        let mut s = snapshot(&q);
        let p = FixedAllocation { max_running: 3 }.propose(&s).unwrap();
        assert_eq!(
            ModelProposed {
                proposal: p.clone()
            }
            .propose(&s),
            Ok(p.clone())
        );
        for field in 0..5 {
            let mut stale = p.clone();
            match field {
                0 => stale.fence.campaign_id.push('x'),
                1 => stale.fence.group_id.push('x'),
                2 => stale.fence.group_revision += 1,
                3 => stale.fence.controller_id.push('x'),
                _ => stale.fence.steering_revision += 1,
            }
            assert_eq!(validate(&s, &stale), Err(PolicyError::StaleFence));
        }
        let mut invalid = p.clone();
        invalid.actions.push(p.actions[0].clone());
        assert_eq!(validate(&s, &invalid), Err(PolicyError::InvalidProposal));
        invalid.actions = vec![Action::Work {
            work_id: "invented".into(),
            allowance: Allowance::default(),
        }];
        assert_eq!(validate(&s, &invalid), Err(PolicyError::InvalidProposal));
        invalid.actions = vec![Action::Verify {
            work_id: q[0].work_id.clone(),
            allowance: q[0].allowance,
        }];
        assert_eq!(validate(&s, &invalid), Err(PolicyError::InvalidProposal));
        invalid.actions = vec![Action::Work {
            work_id: q[0].work_id.clone(),
            allowance: Allowance::default(),
        }];
        assert_eq!(validate(&s, &invalid), Err(PolicyError::InvalidProposal));
        for control in [Steering::Pause, Steering::Stop] {
            s.steering = control;
            assert_eq!(
                ModelProposed {
                    proposal: invalid.clone()
                }
                .propose(&s)
                .unwrap(),
                steering(&s).unwrap()
            );
        }
    }

    #[test]
    fn integer_action_and_resource_bounds() {
        let mut q = queue(64);
        q[0].allowance = Allowance {
            money_micros: u64::MAX,
            inference_units: u64::MAX,
        };
        q[1].allowance = q[0].allowance;
        let mut s = snapshot(&q);
        s.remaining_budget = q[0].allowance;
        let p = Proposal {
            fence: s.fence.clone(),
            actions: vec![
                Action::Work {
                    work_id: q[0].work_id.clone(),
                    allowance: q[0].allowance,
                },
                Action::Verify {
                    work_id: q[1].work_id.clone(),
                    allowance: q[1].allowance,
                },
            ],
        };
        assert_eq!(validate(&s, &p), Err(PolicyError::InsufficientResources));
        s.resources.active_cap = MAX_ACTIVE;
        s.resources.free_slots = MAX_ACTIVE;
        s.current_limit = MAX_ACTIVE;
        s.remaining_budget = Allowance {
            money_micros: 1000,
            inference_units: 1000,
        };
        assert_eq!(
            FixedAllocation {
                max_running: MAX_ACTIVE
            }
            .propose(&s)
            .unwrap()
            .actions
            .len(),
            MAX_ACTIONS
        );
        s.active = MAX_ACTIVE;
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.active = 0;
        s.resources.free_slots = 0;
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.resources.free_slots = 128;
        s.resources.remaining_work = 0;
        assert!(DeterministicAdaptive
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.action_limit = usize::MAX;
        assert_eq!(
            DeterministicAdaptive.propose(&s),
            Err(PolicyError::InvalidSnapshot)
        );
        s.action_limit = MAX_ACTIONS;
        assert_eq!(
            FixedAllocation {
                max_running: u32::MAX
            }
            .propose(&s),
            Err(PolicyError::InvalidProposal)
        );
    }

    #[test]
    fn scan_slices_and_unknown_outcomes_are_bounded() {
        let q = queue(MAX_RECORDS);
        let mut s = snapshot(&q);
        s.recent = &[RecentOutcome::Unknown; MAX_OUTCOMES];
        let p = DeterministicAdaptive.propose(&s).unwrap();
        assert_eq!(p.actions.len(), s.current_limit as usize);
        assert!(!p.actions.iter().any(|a| matches!(a, Action::Resize { .. })));
        s.recent = &[RecentOutcome::Useful; MAX_OUTCOMES + 1];
        assert_eq!(
            DeterministicAdaptive.propose(&s),
            Err(PolicyError::InvalidSnapshot)
        );
        // Both input scan slices have hard limits, irrespective of active cap.
        let oversized = queue(MAX_RECORDS + 1);
        assert_eq!(
            FixedAllocation { max_running: 1 }.propose(&snapshot(&oversized)),
            Err(PolicyError::InvalidSnapshot)
        );
    }

    #[test]
    fn expansion_requires_work_and_shrinking_drains() {
        let q = queue(8);
        let mut s = snapshot(&q);
        let p = Proposal {
            fence: s.fence.clone(),
            actions: vec![Action::Resize { max_running: 4 }],
        };
        assert_eq!(validate(&s, &p), Err(PolicyError::InsufficientResources));
        s.active = 5;
        s.recent = &[RecentOutcome::Failed];
        assert_eq!(
            DeterministicAdaptive.propose(&s).unwrap().actions,
            vec![Action::Resize { max_running: 2 }]
        );
        s.active = 0;
        s.action_limit = 1;
        assert!(FixedAllocation { max_running: 8 }
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        s.current_limit = 8;
        assert_eq!(
            FixedAllocation { max_running: 8 }
                .propose(&s)
                .unwrap()
                .actions
                .len(),
            1
        );
    }

    #[test]
    fn effective_cap_clamps_growth_and_money_alone_can_block_allocation() {
        let q = queue(8);
        let mut s = snapshot(&q);
        s.current_limit = 8;
        s.resources.active_cap = 2;
        s.active = 3;
        for p in [
            FixedAllocation {
                max_running: MAX_ACTIVE,
            }
            .propose(&s)
            .unwrap(),
            DeterministicAdaptive.propose(&s).unwrap(),
        ] {
            assert_eq!(p.actions, vec![Action::Resize { max_running: 2 }]);
            assert_eq!(
                ModelProposed {
                    proposal: p.clone()
                }
                .propose(&s),
                Ok(p)
            );
        }
        s.active = 0;
        s.current_limit = 1;
        s.remaining_budget.money_micros = 2;
        assert!(FixedAllocation { max_running: 2 }
            .propose(&s)
            .unwrap()
            .actions
            .is_empty());
        let p = Proposal {
            fence: s.fence.clone(),
            actions: vec![Action::Work {
                work_id: q[0].work_id.clone(),
                allowance: q[0].allowance,
            }],
        };
        assert_eq!(validate(&s, &p), Err(PolicyError::InsufficientResources));
        s.remaining_budget.money_micros = 6;
        let p = FixedAllocation {
            max_running: MAX_ACTIVE,
        }
        .propose(&s)
        .unwrap();
        assert_eq!(p.actions.len(), 3);
        assert_eq!(p.actions[0], Action::Resize { max_running: 2 });
    }

    #[test]
    fn malformed_projection_and_mixed_controls_rejected() {
        let mut q = queue(2);
        q[1].work_id = q[0].work_id.clone();
        assert_eq!(
            DeterministicAdaptive.propose(&snapshot(&q)),
            Err(PolicyError::InvalidSnapshot)
        );
        let q = queue(MAX_RECORDS + 1);
        assert_eq!(
            DeterministicAdaptive.propose(&snapshot(&q)),
            Err(PolicyError::InvalidSnapshot)
        );
        let q = queue(1);
        let s = snapshot(&q);
        for actions in [
            vec![Action::Pause, Action::Stop],
            vec![Action::Stop; MAX_ACTIONS + 1],
            vec![Action::Resize {
                max_running: u32::MAX,
            }],
            vec![Action::Resize { max_running: 1 }; 2],
        ] {
            assert_eq!(
                validate(
                    &s,
                    &Proposal {
                        fence: s.fence.clone(),
                        actions
                    }
                ),
                Err(PolicyError::InvalidProposal)
            );
        }
    }
}
