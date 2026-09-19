//! Shared timestamp, duration, and agent lifetime labels.
use std::time::Duration;
use tachyon_api::types::{AgentInfo, AgentState, LifetimeClass};

pub(in crate::app) fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(in crate::app) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub(in crate::app) fn format_age(created_secs: u64) -> String {
    format_elapsed(created_secs, unix_now_secs())
}

pub(in crate::app) fn format_elapsed(start_secs: u64, end_secs: u64) -> String {
    format_duration(Duration::from_secs(end_secs.saturating_sub(start_secs)))
}

pub(in crate::app) fn agent_duration(info: &AgentInfo) -> String {
    let end_secs = if info.state.is_terminal() {
        info.last_activity_secs.max(info.created_secs)
    } else {
        unix_now_secs()
    };
    format_elapsed(info.created_secs, end_secs)
}

pub(in crate::app) fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{}h {:02}m", seconds / 3600, (seconds / 60) % 60)
    }
}

pub(in crate::app) fn remaining_duration(deadline_secs: u64) -> String {
    let remaining = deadline_secs.saturating_sub(unix_now_secs());
    if remaining == 0 {
        "due".into()
    } else {
        format_duration(Duration::from_secs(remaining))
    }
}

pub(in crate::app) fn agent_lifetime(info: &AgentInfo) -> (String, String) {
    let retention = if info.retained {
        "retained"
    } else {
        "unretained"
    };
    let lifetime = format!("{} · {retention}", info.lifetime_class);
    if info.state.is_terminal() && info.state != AgentState::Completed {
        return (lifetime, "ended".into());
    }
    if let Some(deadline) = info.stage_until_secs {
        return (
            lifetime,
            format!("kill in {}", remaining_duration(deadline)),
        );
    }
    if let Some(deadline) = info.lease_until_secs {
        return (lifetime, format!("lease {}", remaining_duration(deadline)));
    }
    let remaining = match info.lifetime_class {
        LifetimeClass::Short => info
            .turn_budget
            .map(|budget| {
                let remaining = budget.saturating_sub(info.turns_used);
                format!(
                    "{remaining} assignment{}",
                    if remaining == 1 { "" } else { "s" }
                )
            })
            .unwrap_or_else(|| "idle cleanup".into()),
        LifetimeClass::Long => "daemon stop".into(),
        LifetimeClass::Persistent => "manual release".into(),
    };
    (lifetime, remaining)
}

pub(in crate::app) fn timestamp_label(timestamp: u64) -> String {
    let seconds = if timestamp < 10_000_000_000 {
        timestamp
    } else {
        timestamp / 1_000
    };
    let day = seconds % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        day / 3_600,
        (day % 3_600) / 60,
        day % 60
    )
}
