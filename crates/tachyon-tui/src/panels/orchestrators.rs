//! Stable orchestrator selection and catalog rows.
use crate::app::ui::format::format_age;
use ratatui::style::{Color, Style};
use ratatui::widgets::Row;
use tachyon_api::types::DaemonInfo;
#[cfg(test)]
#[path = "../orchestrator_tests.rs"]
mod tests;

/// Never keep a row number across catalog updates: removal must not retarget controls.
#[derive(Default)]
pub(in crate::app) struct OrchestratorSelection(Option<String>);

impl OrchestratorSelection {
    pub(in crate::app) fn index(&self, daemon: Option<&DaemonInfo>) -> Option<usize> {
        let id = self.0.as_ref()?;
        daemon?.orchestrators.iter().position(|row| &row.id == id)
    }

    pub(in crate::app) fn select(&mut self, daemon: Option<&DaemonInfo>, index: usize) {
        self.0 = daemon
            .and_then(|d| d.orchestrators.get(index))
            .map(|row| row.id.clone());
    }

    pub(in crate::app) fn step(&mut self, daemon: Option<&DaemonInfo>, down: bool) {
        let len = daemon.map_or(0, |d| d.orchestrators.len());
        let index = self.index(daemon).map_or(0, |index| {
            if down {
                (index + 1).min(len.saturating_sub(1))
            } else {
                index.saturating_sub(1)
            }
        });
        self.select(daemon, index);
    }

    pub(in crate::app) fn target(
        &self,
        daemon: Option<&DaemonInfo>,
    ) -> Option<tachyon_api::OrchestratorHost> {
        daemon?.orchestrators.get(self.index(daemon)?)?.host_target
    }
}

pub(in crate::app) fn orchestrator_rows(
    daemon: Option<&DaemonInfo>,
    focus: usize,
) -> Vec<Row<'static>> {
    let Some(daemon) = daemon else {
        return vec![Row::new(["Daemon offline", "catalog unavailable"])];
    };
    if daemon.orchestrators.is_empty() {
        return vec![Row::new(["Catalog unavailable", "update daemon"])];
    }
    daemon
        .orchestrators
        .iter()
        .enumerate()
        .map(|(index, info)| {
            Row::new([
                info.display_name.clone(),
                info.status.clone(),
                match info.kind {
                    tachyon_api::OrchestratorKind::Role => "inference role",
                    tachyon_api::OrchestratorKind::Service => "service",
                }
                .into(),
                if info.host_target.is_some() {
                    "host controls"
                } else {
                    "read-only"
                }
                .into(),
                info.started_secs
                    .map(format_age)
                    .unwrap_or_else(|| "-".into()),
                info.purpose.clone(),
            ])
            .style(if focus == index {
                Style::default().bg(Color::Rgb(42, 42, 52))
            } else {
                Style::default()
            })
        })
        .collect()
}

pub(in crate::app) fn orchestrator_offset(focus: usize, pane_height: u16) -> usize {
    let visible = usize::from(pane_height.saturating_sub(5)).max(1);
    if focus == usize::MAX {
        0
    } else {
        focus / visible * visible
    }
}
