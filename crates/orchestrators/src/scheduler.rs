use crate::tasks::{Task, TaskState};

/// Scheduling decisions belong here; process creation and termination belong
/// to Tachyond.
pub fn dependencies_satisfied(task: &Task, known: &[Task]) -> bool {
    task.depends_on.iter().all(|dependency| {
        known
            .iter()
            .any(|candidate| candidate.id == *dependency && candidate.state == TaskState::Completed)
    })
}
