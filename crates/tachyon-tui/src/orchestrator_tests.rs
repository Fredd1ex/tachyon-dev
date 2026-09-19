use super::*;
use ratatui::{layout::Constraint, widgets::Table};
use tachyon_api::{OrchestratorHost, OrchestratorInfo, OrchestratorKind};

fn snapshot() -> DaemonInfo {
    // A shipped status payload without the additive catalog remains readable.
    serde_json::from_value(serde_json::json!({
        "pid": 1, "version": "test", "proto_version": "test",
        "provider_ready": false, "socket": "unused"
    }))
    .unwrap()
}

fn custom() -> OrchestratorInfo {
    serde_json::from_value(serde_json::json!({
        "id": "role:custom-observer", "display_name": "Custom Observer",
        "purpose": "Observe without owning a process.", "kind": "role",
        "host": "background", "visibility": "internal", "invocations": ["primary"],
        "runtime_id": null, "host_target": null, "status": "available",
        "active": null, "started_secs": null
    }))
    .unwrap()
}

#[test]
fn orchestrator_selection_survives_reorder_but_never_retargets_removed_role() {
    let mut daemon = snapshot();
    let role = custom();
    let service = OrchestratorInfo {
        id: "service:test-host".into(),
        kind: OrchestratorKind::Service,
        host_target: Some(OrchestratorHost::Daemon),
        ..role.clone()
    };
    daemon.orchestrators = vec![role.clone(), service.clone()];
    let mut selected = OrchestratorSelection::default();
    assert_eq!(selected.target(Some(&daemon)), None);
    selected.select(Some(&daemon), 0);
    assert_eq!(selected.target(Some(&daemon)), None);
    daemon.orchestrators.swap(0, 1);
    assert_eq!(selected.index(Some(&daemon)), Some(1));
    assert_eq!(selected.target(Some(&daemon)), None);
    daemon.orchestrators = vec![service];
    assert_eq!(selected.index(Some(&daemon)), None);
    assert_eq!(selected.target(Some(&daemon)), None);
    selected.select(Some(&daemon), 0);
    assert_eq!(
        selected.target(Some(&daemon)),
        Some(OrchestratorHost::Daemon)
    );
    assert_eq!(selected.target(None), None);
    daemon.orchestrators.insert(0, role);
    assert_eq!(selected.index(Some(&daemon)), Some(1));
    selected.step(Some(&daemon), false);
    assert_eq!(selected.target(Some(&daemon)), None);
    selected.select(Some(&daemon), 99);
    assert_eq!(selected.target(Some(&daemon)), None);
}

fn rendered(daemon: Option<&DaemonInfo>) -> String {
    let backend = ratatui::backend::TestBackend::new(140, 8);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| {
            frame.render_widget(
                Table::new(
                    orchestrator_rows(daemon, usize::MAX),
                    [
                        Constraint::Length(22),
                        Constraint::Length(22),
                        Constraint::Length(18),
                        Constraint::Length(16),
                        Constraint::Length(9),
                        Constraint::Min(30),
                    ],
                ),
                frame.area(),
            );
        })
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

#[test]
fn orchestrator_catalog_renders_custom_add_remove_and_offline_without_static_roles() {
    let mut daemon = snapshot();
    assert!(daemon.orchestrators.is_empty());
    assert!(rendered(Some(&daemon)).contains("Catalog unavailable"));
    daemon.orchestrators.push(custom());
    let screen = rendered(Some(&daemon));
    for expected in [
        "Custom Observer",
        "available",
        "inference role",
        "read-only",
        "Observe without",
    ] {
        assert!(screen.contains(expected), "missing {expected}: {screen}");
    }
    assert!(!screen.contains("running"));
    daemon.orchestrators.clear();
    assert!(!rendered(Some(&daemon)).contains("Custom Observer"));
    let offline = rendered(None);
    assert!(offline.contains("Daemon offline"));
    assert!(!offline.contains("running"));
}

#[test]
fn orchestrator_paging_clicks_resolve_the_visible_stable_id() {
    let mut daemon = snapshot();
    daemon.orchestrators = (0..66)
        .map(|index| OrchestratorInfo {
            id: format!("role:test-{index}"),
            ..custom()
        })
        .collect();
    let mut selected = OrchestratorSelection::default();
    selected.select(Some(&daemon), 65);
    let offset = orchestrator_offset(selected.index(Some(&daemon)).unwrap(), 18);
    assert_eq!(offset, 65);
    selected.select(Some(&daemon), offset);
    assert_eq!(selected.0.as_deref(), Some("role:test-65"));
    assert_eq!(selected.target(Some(&daemon)), None);
    assert_eq!(orchestrator_offset(usize::MAX, 18), 0);
}
