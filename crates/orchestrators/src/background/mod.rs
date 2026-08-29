//! Background Coordinator policy.

pub mod prompt;

use crate::capabilities::Capability;

pub const CAPABILITIES: &[Capability] = &[
    Capability::DelegateOne,
    Capability::DelegateMany,
    Capability::InspectWorker,
    Capability::AwaitWorker,
    Capability::ReleaseWorker,
    Capability::RetainWorker,
    Capability::StageWorker,
    Capability::ReplanWorker,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_has_lifecycle_but_no_execution_capabilities() {
        let names: Vec<_> = CAPABILITIES
            .iter()
            .map(|capability| capability.tool_name())
            .collect();
        assert!(names.contains(&"spawn_agent"));
        assert!(names.contains(&"agent_replan"));
        assert!(!names.contains(&"respond"));
        assert!(!names.contains(&"ipython"));
        assert!(!names.contains(&"agent_browser"));
    }
}
