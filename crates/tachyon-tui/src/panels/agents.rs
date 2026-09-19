//! Agent and operational tab content.
use crate::app::model::thread::Thread;
use crate::app::panels::orchestrators::{orchestrator_offset, orchestrator_rows};
use crate::app::panels::tabs::{PaneTab, TabStrip};
use crate::app::transcript::text::truncate_text;
use crate::app::ui::activity::agent_pane_status;
use crate::app::ui::format::{
    agent_duration, agent_lifetime, format_age, format_duration, now_seconds,
};
use crate::app::{daemon_state_cache, ui, WINDOW_LOGO_BUTTON};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Clear, Paragraph, Row, Table};
use ratatui::Frame;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tachyon_api::types::{
    AgentInfo, AgentState, DaemonInfo, ScheduledTaskInfo, ScheduledTaskMode, ScheduledTaskStatus,
};
use tachyon_api::{FOREGROUND_ID, MEMORY_ID};

pub(in crate::app) fn pane_agent_ids(agent_infos: &HashMap<String, AgentInfo>) -> Vec<String> {
    let mut worker_ids: Vec<String> = agent_infos
        .keys()
        .filter(|id| id.as_str() != FOREGROUND_ID && id.as_str() != MEMORY_ID)
        .cloned()
        .collect();
    worker_ids.sort_by(|left, right| {
        let left = &agent_infos[left];
        let right = &agent_infos[right];
        agent_sort_rank(left)
            .cmp(&agent_sort_rank(right))
            .then_with(|| right.created_secs.cmp(&left.created_secs))
            .then_with(|| left.id.cmp(&right.id))
    });
    worker_ids
}

pub(in crate::app) fn agent_sort_rank(info: &AgentInfo) -> u8 {
    match info.state {
        AgentState::Created | AgentState::Starting | AgentState::Running | AgentState::Staged => 0,
        AgentState::Waiting => 1,
        AgentState::Completed if info.retained => 2,
        AgentState::Failed | AgentState::Interrupted | AgentState::Terminated => 3,
        AgentState::Completed | AgentState::Released => 4,
    }
}

/// Copy the selected conversation cell, or the latest cell when none is selected.

pub(in crate::app) fn draw_agent_pane(
    f: &mut Frame,
    area: Rect,
    _threads: &[Thread],
    focus: usize,
    daemon: Option<&DaemonInfo>,
    _daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
    scheduled_tasks: &[ScheduledTaskInfo],
    tab: PaneTab,
    operational: &daemon_state_cache::View,
    operational_scroll: u16,
) {
    f.render_widget(Clear, area);
    let block = ui::panel_shell().style(Style::default().bg(Color::Rgb(18, 18, 22)));
    let mut inner = block.inner(area);
    let tabs = TabStrip::new(
        area,
        pane_agent_ids(agent_infos).len(),
        scheduled_tasks.len(),
    );
    let extra = tabs.height(area).saturating_sub(1).min(inner.height);
    inner.y += extra;
    inner.height -= extra;
    f.render_widget(block, area);

    let checklist = if tab == PaneTab::Foreground {
        operational.global_checklist()
    } else {
        String::new()
    };
    let checklist_height = (checklist.lines().count() as u16).min(inner.height.saturating_sub(5));
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Length(2),
            Constraint::Length(checklist_height),
        ])
        .split(inner);
    if !checklist.is_empty() {
        f.render_widget(Paragraph::new(checklist), sections[2]);
    }
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            WINDOW_LOGO_BUTTON,
            Style::default().fg(Color::White),
        ))),
        Rect {
            x: area.x.saturating_add(1),
            y: area.y,
            width: area.width.saturating_sub(2),
            height: 1,
        },
    );

    tabs.draw(f, tab);
    if matches!(tab, PaneTab::Todos | PaneTab::Resources) {
        let text = if operational.rows.is_empty() {
            if tab == PaneTab::Todos {
                "Waiting for live conversation metadata or todo snapshot (unknown)."
            } else {
                "Waiting for Host monitor data (unknown)."
            }
        } else {
            &operational.rows
        };
        f.render_widget(
            Paragraph::new(text).scroll((operational_scroll, 0)),
            sections[0],
        );
        f.render_widget(Paragraph::new(if operational.stale {
            "STALE / disconnected; last known data retained\nRead-only | arrows scroll | PgDn next page | PgUp first page"
        } else if tab == PaneTab::Todos {
            "Read-only; ask an agent to edit todos\nArrows scroll | PgDn next page | PgUp first page"
        } else {
            "Read-only Host observations; unknown is not zero\nArrows scroll | PgDn next page | PgUp first page"
        }), sections[1]);
        return;
    }

    let header = Row::new(["NAME", "STATUS", "POLICY", "CAPACITY", "DURATION", "TASK"]).style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::BOLD),
    );
    let widths = [
        Constraint::Length(17),
        Constraint::Length(16),
        Constraint::Length(19),
        Constraint::Length(15),
        Constraint::Length(9),
        Constraint::Min(16),
    ];
    let rows = match tab {
        PaneTab::Todos | PaneTab::Resources => unreachable!(),
        PaneTab::Foreground => orchestrator_rows(daemon, focus)
            .into_iter()
            .skip(orchestrator_offset(
                focus,
                area.height.saturating_sub(extra + checklist_height),
            ))
            .collect(),
        PaneTab::Agents => {
            let ids = pane_agent_ids(agent_infos);
            if ids.is_empty() {
                vec![Row::new(vec![
                    Cell::from("No managed agents"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("Start a task to create a worker agent"),
                ])
                .style(Style::default().fg(Color::DarkGray))]
            } else {
                ids.iter()
                    .enumerate()
                    .map(|(index, id)| {
                        let info = &agent_infos[id];
                        let selected = focus == index + 2;
                        let reviewing = daemon.is_some_and(|daemon| {
                            daemon
                                .background
                                .pending_reviews
                                .iter()
                                .any(|review| review.worker_id == info.id)
                        });
                        let (lifetime, remaining) = agent_lifetime(info);
                        let (status, status_color) = agent_pane_status(info, reviewing);
                        Row::new(vec![
                            Cell::from(truncate_text(&format!("ghost {id}"), 17)),
                            Cell::from(status).style(Style::default().fg(status_color)),
                            Cell::from(lifetime),
                            Cell::from(remaining),
                            Cell::from(agent_duration(info)),
                            Cell::from(truncate_text(
                                &format!("{} · {}", info.task_type, info.description),
                                48,
                            )),
                        ])
                        .style(if selected {
                            Style::default().bg(Color::Rgb(42, 42, 52))
                        } else {
                            Style::default()
                        })
                    })
                    .collect()
            }
        }
        PaneTab::Scheduled => {
            if scheduled_tasks.is_empty() {
                vec![Row::new(vec![
                    Cell::from("No scheduled tasks"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("-"),
                    Cell::from("Schedule future work from the conversation"),
                ])
                .style(Style::default().fg(Color::DarkGray))]
            } else {
                scheduled_tasks
                    .iter()
                    .map(|schedule| Row::new(scheduled_task_columns(schedule)))
                    .collect()
            }
        }
        PaneTab::Memory => vec![
            Row::new(vec![
                Cell::from("User profile"),
                Cell::from("planned"),
                Cell::from("memories.redb"),
                Cell::from("typed read/write"),
                Cell::from("-"),
                Cell::from("Durable facts, preferences, and corrections"),
            ]),
            Row::new(vec![
                Cell::from("History"),
                Cell::from("planned"),
                Cell::from("history.redb"),
                Cell::from("typed read"),
                Cell::from("-"),
                Cell::from("Conversation and task activity"),
            ]),
            Row::new(vec![
                Cell::from("Runtime"),
                Cell::from("planned"),
                Cell::from("runtime.redb"),
                Cell::from("typed read"),
                Cell::from("-"),
                Cell::from("Schedules, workers, and runtime state"),
            ]),
        ],
    };
    f.render_widget(
        Table::new(rows, widths)
            .header(header)
            .column_spacing(1)
            .style(Style::default().fg(Color::Gray).bg(Color::Rgb(18, 18, 22))),
        sections[0],
    );
    f.render_widget(
        Paragraph::new(match tab {
            PaneTab::Todos | PaneTab::Resources => unreachable!(),
            PaneTab::Scheduled => {
                "left/right tabs · durable scheduled work · worker appears when execution starts"
            }
            PaneTab::Memory => "left/right tabs · placeholder · right: TODO, RESOURCES (read-only)",
            PaneTab::Foreground => {
                "left/right tabs · up/down or click to select · host controls only · s stop · r restart"
            }
            PaneTab::Agents => {
                "left/right tabs · click a row to focus · s stop · r restart · u resume · k kill"
            }
        })
        .alignment(Alignment::Center)
        .style(
            Style::default()
                .fg(Color::DarkGray)
                .bg(Color::Rgb(18, 18, 22)),
        ),
        sections[1],
    );
}

pub(in crate::app) fn scheduled_task_columns(schedule: &ScheduledTaskInfo) -> [String; 6] {
    let status = match schedule.status {
        ScheduledTaskStatus::Pending => "pending",
        ScheduledTaskStatus::Running => "running",
        ScheduledTaskStatus::Completed => "completed",
        ScheduledTaskStatus::Failed => "failed",
        ScheduledTaskStatus::Cancelled => "cancelled",
    };
    let mode = match schedule.mode {
        ScheduledTaskMode::StartAt => "start at",
        ScheduledTaskMode::FinishBy => "finish by",
    };
    let remaining_ms = schedule.due_at_ms.saturating_sub(now_seconds());
    let deadline = if remaining_ms == 0 {
        "due now".into()
    } else {
        format!(
            "in {}",
            format_duration(Duration::from_millis(remaining_ms))
        )
    };
    [
        truncate_text(&format!("schedule t{}", schedule.turn), 17),
        status.into(),
        mode.into(),
        deadline,
        format_age(schedule.created_at_ms / 1_000),
        truncate_text(&schedule.objective, 48),
    ]
}
