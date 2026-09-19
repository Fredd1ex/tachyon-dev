//! Help and information overlays and panel titles.
use crate::app::model::thread::Thread;
use crate::app::transcript::text::truncate_text;
use crate::app::ui::format::format_duration;
use crate::app::ui::popup_rect;
use crate::app::{ui, MouseCapture, WINDOW_LOGO_BUTTON};
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::block::Title;
use ratatui::Frame;
use std::collections::HashMap;
use std::time::Instant;
use tachyon_api::types::{AgentInfo, AgentState, DaemonInfo};

pub(in crate::app) fn help_section(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!(" {} ", title),
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))
}

pub(in crate::app) fn help_key(key: &str, description: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!(" {:<14} ", key),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {description}")),
    ])
}

pub(in crate::app) fn help_divider(width: usize) -> Line<'static> {
    Line::from(Span::styled(
        "─".repeat(width),
        Style::default().fg(Color::DarkGray),
    ))
}

pub(in crate::app) fn popup_title(label: &'static str) -> Title<'static> {
    Title::from(Line::from(vec![
        Span::styled(WINDOW_LOGO_BUTTON, Style::default().fg(Color::White)),
        Span::raw(" "),
        Span::styled(
            label,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ]))
    .alignment(Alignment::Left)
}

pub(in crate::app) fn draw_command_palette(
    f: &mut Frame,
    area: Rect,
    mouse_capture: MouseCapture,
    scroll: &mut u16,
) {
    let popup = popup_rect(area, 76, 40);
    let commands = vec![
        Line::from(mouse_capture.label()),
        help_section("KEYBINDS"),
        Line::from(""),
        help_key("? / Ctrl+P", "toggle help"),
        help_key("Tab", "agents pane"),
        help_key("Ctrl+I", "toggle info (if distinct from Tab)"),
        help_key("Ctrl+O", "toggle inline details for selected/current turn"),
        help_key(
            "Ctrl+D",
            "toggle secondary diagnostics while details are open",
        ),
        help_key("Diagnostics: Up/Down", "select individual activity row"),
        help_key(
            "Enter / Space / click",
            "expand/collapse selected activity row",
        ),
        help_key(
            "Diagnostics: r / d",
            "raw output / diagnostic metrics (opt-in)",
        ),
        help_key(
            "y (empty input)",
            "copy selected/latest cell (prompt + reply)",
        ),
        help_key("Drag", "native selection; capture: Shift+drag may bypass"),
        help_key(
            "Ctrl+Shift+C",
            "terminal Copy; forwarded cell copy ONLY in capture",
        ),
        help_key("Shift+Enter", "insert newline"),
        help_key("Up / Down", "scroll active view (empty input)"),
        help_key(
            "PageUp / PageDown",
            "page active view without opening traces",
        ),
        help_key("Wheel", "terminal-owned by default; /mouse for TUI scroll"),
        help_key("End", "chat: follow latest; panels: scroll to bottom"),
        help_key("Ctrl+L", "hide/show previous visits"),
        help_key("Alt+Up / Down", "inspect turns across session history"),
        help_key("Ctrl+C / Esc", "quit / close topmost panel first"),
        help_divider(popup.width.saturating_sub(2) as usize),
        help_section("AGENT ACTIONS"),
        Line::from(""),
        help_key("Up / Down", "focus daemon or agent"),
        help_key("a", "await"),
        help_key("u", "resume"),
        help_key("s / k", "stop / kill"),
        help_key("r / x", "restart / release"),
        help_key("S", "start daemon"),
        help_divider(popup.width.saturating_sub(2) as usize),
        help_section("COMMANDS"),
        help_key("/clear", "hide/show previous visits"),
        help_key("/mouse", "toggle native selection / clickable capture"),
        help_key("/managed TEXT", "use managed worker workspaces"),
        help_key(
            "/attention, /ack ID",
            "list attention / explicitly acknowledge",
        ),
        help_key("/exit", "quit Tachyon"),
    ];
    ui::text_panel(f, popup, " HELP - Esc closes ", commands, scroll);
}

pub(in crate::app) fn draw_info_panel(
    f: &mut Frame,
    area: Rect,
    daemon: Option<&DaemonInfo>,
    daemon_since: Option<Instant>,
    agent_infos: &HashMap<String, AgentInfo>,
    threads: &[Thread],
    config: &tachyon_util::config::Config,
    session_label: &str,
    scroll: &mut u16,
) {
    let popup = popup_rect(area, 58, 25);
    let section = |title: &str| {
        Line::from(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
    };
    let value = |label: &str, text: String| {
        Line::from(vec![
            Span::styled(format!("  {label:<14}"), Style::default().fg(Color::Gray)),
            Span::styled(text, Style::default().fg(Color::White)),
        ])
    };
    let running = agent_infos
        .values()
        .filter(|info| matches!(info.state, AgentState::Starting | AgentState::Running))
        .count();
    let completed = agent_infos
        .values()
        .filter(|info| {
            matches!(
                info.state,
                AgentState::Completed | AgentState::Released | AgentState::Terminated
            )
        })
        .count();
    let failed = agent_infos
        .values()
        .filter(|info| info.state == AgentState::Failed)
        .count();
    let daemon_status = match daemon {
        Some(info) if info.provider_ready => format!("online · v{}", info.version),
        Some(info) => format!("online · provider unavailable · v{}", info.version),
        None => "offline".into(),
    };
    let daemon_pid = daemon
        .map(|info| info.pid.to_string())
        .unwrap_or_else(|| "-".into());
    let uptime = daemon_since
        .map(|since| format_duration(since.elapsed()))
        .unwrap_or_else(|| "-".into());
    let provider = config
        .provider
        .as_ref()
        .map(|provider| provider.name.clone())
        .unwrap_or_else(|| "not configured".into());
    let provider = if config.provider.is_some() {
        format!("{} · config.toml", provider)
    } else {
        provider
    };
    let model = if config.model.name.is_some() {
        format!("{} · config.toml", config.active_model())
    } else {
        format!("{} · default", config.active_model())
    };
    let (routing_profile, routing_policy, routing_fallbacks) = config
        .provider_routing()
        .map(|routing| {
            let profile = match routing.profile {
                tachyon_util::config::RoutingProfile::Cost => "cost",
                tachyon_util::config::RoutingProfile::Performance => "performance",
                tachyon_util::config::RoutingProfile::Manual => "manual",
            };
            let preferences = routing.active_preferences();
            let mut settings = Vec::new();
            if let Some(order) = &preferences.order {
                if !order.is_empty() {
                    settings.push(format!("order: {}", order.join(" > ")));
                }
            }
            if let Some(sort) = &preferences.sort {
                settings.push(format!("sort: {sort}"));
            }
            if let Some(latency) = preferences.preferred_max_latency {
                settings.push(format!("TTFT: <= {latency:.2}s"));
            }
            if let Some(throughput) = preferences.preferred_min_throughput {
                settings.push(format!("throughput: >= {throughput:.0} tok/s"));
            }
            let policy = if settings.is_empty() {
                "OpenRouter defaults".into()
            } else {
                settings.join(" · ")
            };
            let fallbacks = if routing.allow_fallbacks.unwrap_or(true) {
                "enabled"
            } else {
                "disabled"
            };
            (profile.to_string(), policy, fallbacks.to_string())
        })
        .unwrap_or_else(|| {
            (
                "default".into(),
                "OpenRouter defaults".into(),
                "enabled".into(),
            )
        });
    let cwd = std::env::current_dir()
        .ok()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "not available".into());
    let cwd = truncate_text(&cwd, popup.width.saturating_sub(18) as usize);
    let lines = vec![
        Line::from(""),
        section("SESSION"),
        Line::from(""),
        value("state", session_label.to_string()),
        value("threads", threads.len().to_string()),
        value("cwd", cwd),
        value(
            "agents",
            format!(
                "{} running · {} done · {} failed",
                running, completed, failed
            ),
        ),
        help_divider(popup.width.saturating_sub(2) as usize),
        section("FOREGROUND"),
        Line::from(""),
        value("provider", provider),
        value("model", model),
        value("routing", routing_profile),
        value("policy", routing_policy),
        value("fallbacks", routing_fallbacks),
        value("context", "not reported".into()),
        help_divider(popup.width.saturating_sub(2) as usize),
        section("DAEMON"),
        Line::from(""),
        value("status", daemon_status),
        value("pid", daemon_pid),
        value("uptime", uptime),
    ];
    ui::text_panel(f, popup, " INFO - Esc closes ", lines, scroll);
}
