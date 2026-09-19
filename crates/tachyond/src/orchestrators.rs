//! Read-only registry projection. Host observations never imply sibling activity.
use super::Registry;
use tachyon_api::*;
use tachyon_orchestrator::registry::{HostLane, InvocationKind, OutputVisibility, RoleId};

pub(super) fn catalog(
    roles: &tachyon_orchestrator::registry::Registry<'_>,
    runtime: &Registry,
    conversation_name: &str,
) -> Vec<OrchestratorInfo> {
    let mut rows = vec![OrchestratorInfo {
        id: "service:daemon".into(),
        display_name: "tachyond".into(),
        purpose: "Daemon service".into(),
        kind: OrchestratorKind::Service,
        host: OrchestratorHost::Daemon,
        visibility: OrchestratorVisibility::Internal,
        invocations: vec![],
        runtime_id: Some(std::process::id().to_string()),
        host_target: Some(OrchestratorHost::Daemon),
        status: "online".into(),
        active: None,
        started_secs: None,
    }];
    rows.push(OrchestratorInfo {
        id: "service:memory".into(),
        display_name: "Memory".into(),
        purpose: "Storage and context service".into(),
        runtime_id: None,
        host_target: None,
        status: if runtime.memory_store.is_some() {
            "online"
        } else {
            "unavailable"
        }
        .into(),
        started_secs: runtime
            .memory_store
            .as_ref()
            .map(|_| runtime.memory_started_secs),
        ..rows[0].clone()
    });
    for role in roles.enabled_roles().take(ORCHESTRATOR_ROLE_LIMIT) {
        let mut row = OrchestratorInfo {
            id: format!("role:{}", role.stable_id),
            display_name: role.display_name.into(),
            purpose: role.purpose.into(),
            kind: OrchestratorKind::Role,
            host: match role.host_lane {
                HostLane::Foreground => OrchestratorHost::Foreground,
                HostLane::Background => OrchestratorHost::Background,
            },
            visibility: match role.output_visibility {
                OutputVisibility::UserFacing => OrchestratorVisibility::UserFacing,
                OutputVisibility::Internal => OrchestratorVisibility::Internal,
            },
            invocations: role
                .invocations
                .iter()
                .map(|binding| match binding.kind {
                    InvocationKind::Primary => OrchestratorInvocation::Primary,
                    InvocationKind::Review => OrchestratorInvocation::Review,
                })
                .collect(),
            runtime_id: None,
            host_target: None,
            status: "available".into(),
            active: None,
            started_secs: None,
        };
        if role.id == RoleId::Conversation {
            if !conversation_name.is_empty() {
                row.display_name = conversation_name.into();
            }
            if let Some(task) = runtime
                .foreground_id
                .as_ref()
                .and_then(|id| runtime.tasks.get(id))
            {
                row.runtime_id = Some(task.info.id.clone());
                row.status = task.info.state.to_string();
                row.active = Some(task.info.state == AgentState::Running);
                row.started_secs = Some(task.info.created_secs);
            }
        } else if role.id == RoleId::Coordinator {
            let reviewing = runtime.works.values().any(|work| work.review.is_some());
            row.status = if !runtime.background_online {
                "restarting"
            } else if reviewing {
                "reviewing"
            } else {
                "idle"
            }
            .into();
            row.active = runtime.background_online.then_some(reviewing);
        }
        rows.push(row);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_orchestrator::{
        agents,
        registry::{Registry as Roles, RoleDescriptor},
    };

    #[test]
    fn catalog_add_remove_disable_reorder_and_host_boundaries() {
        let custom = RoleDescriptor {
            id: RoleId::Custom("custom-observer"),
            stable_id: "custom-observer",
            display_name: "Custom Observer",
            purpose: "Observe without owning a process.",
            ..agents::campaign::definition()
        };
        let runtime = Registry {
            background_online: true,
            ..Registry::default()
        };
        let definitions = [
            custom,
            agents::conversation::definition(),
            agents::coordinator::definition(),
            agents::campaign::definition(),
        ];
        let rows = catalog(&Roles::new(&definitions).unwrap(), &runtime, "Jarvis");
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[2].display_name, "Custom Observer");
        assert_eq!(rows[2].purpose, custom.purpose);
        assert_eq!(rows[2].invocations, [OrchestratorInvocation::Primary]);
        assert_eq!(rows[2].visibility, OrchestratorVisibility::Internal);
        assert_eq!(rows[3].display_name, "Jarvis");
        assert_eq!(rows[4].display_name, "Coordinator");
        assert_eq!(rows[4].status, "idle");
        for row in [&rows[2], &rows[5]] {
            assert_eq!(row.status, "available");
            assert_eq!(row.host, OrchestratorHost::Background);
            assert_eq!(row.runtime_id, None);
            assert_eq!(row.host_target, None);
            assert_eq!(row.started_secs, None);
            assert_eq!(row.active, None);
        }
        assert_eq!(rows[0].kind, OrchestratorKind::Service);
        assert_eq!(rows[1].kind, OrchestratorKind::Service);
        assert_eq!(rows[1].host_target, None);
        let reordered = [definitions[3], custom];
        let rows = catalog(&Roles::new(&reordered).unwrap(), &runtime, "");
        assert_eq!(rows[2].id, "role:campaign");
        assert_eq!(rows[3].id, "role:custom-observer");
        let disabled = [RoleDescriptor {
            enabled: false,
            ..custom
        }];
        assert_eq!(
            catalog(&Roles::new(&disabled).unwrap(), &runtime, "").len(),
            2
        );
        assert_eq!(catalog(&Roles::new(&[]).unwrap(), &runtime, "").len(), 2);
    }

    #[test]
    fn catalog_is_bounded_without_rendering_prompts() {
        let definitions: Vec<_> = (0..100)
            .map(|index| {
                let id: &'static str = Box::leak(format!("test-{index}").into_boxed_str());
                RoleDescriptor {
                    id: RoleId::Custom(id),
                    stable_id: id,
                    invocations: &[],
                    ..agents::campaign::definition()
                }
            })
            .collect();
        let rows = catalog(&Roles::new(&definitions).unwrap(), &Registry::default(), "");
        assert_eq!(rows.len(), ORCHESTRATOR_ROLE_LIMIT + 2);
    }
}
