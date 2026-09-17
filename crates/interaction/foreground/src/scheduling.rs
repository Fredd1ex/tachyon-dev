//! Future-time detection and enforced schedule-call construction.

use chrono::Local;
use tachyon_model::{ChatMessage, Content, Role, ToolCall};

pub(super) fn future_schedule_required(history: &[ChatMessage], incoming: &str) -> bool {
    if contains_explicit_future_time(incoming) {
        return true;
    }
    let mut clarification = false;
    for message in history.iter().rev() {
        match message.role {
            Role::Assistant if !clarification => {
                clarification = chat_message_text(message).trim_end().ends_with('?');
                if !clarification {
                    return false;
                }
            }
            Role::User if clarification => {
                return contains_explicit_future_time(&chat_message_text(message));
            }
            _ => {}
        }
    }
    false
}

pub(super) fn enforced_schedule_call(
    history: &[ChatMessage],
    incoming: &str,
    turn: u64,
) -> Option<ToolCall> {
    let source = if contains_explicit_future_time(incoming) {
        incoming.to_string()
    } else {
        let prior = history.iter().rev().find_map(|message| {
            (message.role == Role::User)
                .then(|| chat_message_text(message))
                .filter(|text| contains_explicit_future_time(text))
        })?;
        format!("{prior}\nClarification: {incoming}")
    };
    let lowercase = source.to_ascii_lowercase();
    let reminder = ["remind", "alert", "notify"].iter().any(|word| {
        lowercase
            .split_whitespace()
            .any(|token| token.starts_with(word))
    });
    let words = source
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != ':'
            })
        })
        .collect::<Vec<_>>();
    let mut action = "start_at";
    let mut timing = None;
    for pair in words.windows(2) {
        if matches!(pair[0].to_ascii_lowercase().as_str(), "at" | "by") && is_clock_token(pair[1]) {
            action = if pair[0].eq_ignore_ascii_case("by") {
                "finish_by"
            } else {
                "start_at"
            };
            timing = Some(serde_json::json!({
                "local_time": next_local_clock(pair[1], Local::now())?,
                "day": "next"
            }));
            break;
        }
    }
    if timing.is_none() {
        for parts in words.windows(3) {
            if parts[0].eq_ignore_ascii_case("in") {
                let value = parts[1].parse::<u64>().ok()?;
                let multiplier = match parts[2].to_ascii_lowercase().as_str() {
                    "second" | "seconds" | "sec" | "secs" => 1,
                    "minute" | "minutes" | "min" | "mins" => 60,
                    "hour" | "hours" => 3_600,
                    "day" | "days" => 86_400,
                    _ => continue,
                };
                timing = Some(serde_json::json!({
                    "delay_seconds": value.checked_mul(multiplier)?
                }));
                break;
            }
        }
    }
    let mut arguments = timing?;
    arguments["action"] =
        serde_json::Value::String(if reminder { "create" } else { action }.into());
    arguments[if reminder { "text" } else { "objective" }] = serde_json::Value::String(source);
    Some(ToolCall {
        id: format!("enforced-schedule-{turn}"),
        name: "schedule".into(),
        arguments: arguments.to_string(),
    })
}

fn next_local_clock(token: &str, now: chrono::DateTime<Local>) -> Option<String> {
    use chrono::Timelike;

    let lowercase = token.to_ascii_lowercase();
    let suffix = lowercase
        .ends_with("am")
        .then_some("am")
        .or_else(|| lowercase.ends_with("pm").then_some("pm"));
    let clock = suffix
        .and_then(|suffix| lowercase.strip_suffix(suffix))
        .unwrap_or(&lowercase);
    let (hour, minute) = clock.split_once(':').map_or((clock, "0"), |parts| parts);
    let hour = hour.parse::<u32>().ok()?;
    let minute = minute.parse::<u32>().ok()?;
    if minute > 59 || hour > 23 || suffix.is_some() && !(1..=12).contains(&hour) {
        return None;
    }
    let hour = match suffix {
        Some("am") => hour % 12,
        Some("pm") => hour % 12 + 12,
        Some(_) => return None,
        None if hour <= 12 => {
            let morning = hour % 12;
            let evening = morning + 12;
            let now_minutes = now.hour() * 60 + now.minute();
            [morning, evening].into_iter().min_by_key(|candidate| {
                let candidate = candidate * 60 + minute;
                candidate
                    .checked_sub(now_minutes)
                    .filter(|delta| *delta > 0)
                    .unwrap_or_else(|| 1_440 - now_minutes + candidate)
            })?
        }
        None => hour,
    };
    Some(format!("{hour:02}:{minute:02}"))
}

fn chat_message_text(message: &ChatMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn contains_explicit_future_time(text: &str) -> bool {
    let words = text
        .split_whitespace()
        .map(|word| {
            word.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && character != ':'
            })
        })
        .collect::<Vec<_>>();
    words.windows(2).any(|pair| {
        matches!(pair[0].to_ascii_lowercase().as_str(), "at" | "by") && is_clock_token(pair[1])
    }) || words.windows(3).any(|parts| {
        parts[0].eq_ignore_ascii_case("in")
            && parts[1].parse::<u64>().is_ok_and(|value| value > 0)
            && matches!(
                parts[2].to_ascii_lowercase().as_str(),
                "second"
                    | "seconds"
                    | "sec"
                    | "secs"
                    | "minute"
                    | "minutes"
                    | "min"
                    | "mins"
                    | "hour"
                    | "hours"
                    | "day"
                    | "days"
            )
    })
}

fn is_clock_token(token: &str) -> bool {
    let lowercase = token.to_ascii_lowercase();
    let clock = lowercase
        .strip_suffix("am")
        .or_else(|| lowercase.strip_suffix("pm"))
        .unwrap_or(&lowercase);
    let Some((hour, minute)) = clock.split_once(':') else {
        return (lowercase.ends_with("am") || lowercase.ends_with("pm"))
            && clock
                .parse::<u32>()
                .is_ok_and(|hour| (1..=12).contains(&hour));
    };
    hour.parse::<u32>().is_ok_and(|hour| hour <= 23)
        && minute.parse::<u32>().is_ok_and(|minute| minute <= 59)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_future_work_requires_scheduler_routing() {
        assert!(contains_explicit_future_time(
            "get me the weather in London at 8:31"
        ));
        assert!(contains_explicit_future_time(
            "finish the forecast by 20:31"
        ));
        assert!(contains_explicit_future_time("remind me in 30 secs"));
        assert!(!contains_explicit_future_time(
            "get the current weather in London"
        ));
        let now = chrono::TimeZone::with_ymd_and_hms(&Local, 2026, 9, 5, 19, 36, 0)
            .single()
            .unwrap();
        assert_eq!(next_local_clock("8:37", now).as_deref(), Some("20:37"));
        let call = enforced_schedule_call(&[], "inspect the release artifacts at 9:15pm", 7)
            .expect("generic scheduled task");
        let arguments: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
        assert_eq!(arguments["action"], "start_at");
        assert_eq!(
            arguments["objective"],
            "inspect the release artifacts at 9:15pm"
        );
    }

    #[test]
    fn scheduler_routing_survives_a_clarification_follow_up() {
        let history = vec![
            ChatMessage::new(Role::User, "get the weather at 8:31"),
            ChatMessage::new(Role::Assistant, "Which location?"),
        ];
        assert!(future_schedule_required(&history, "London please"));
        let call = enforced_schedule_call(&history, "London please", 4).unwrap();
        let arguments: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
        assert_eq!(arguments["action"], "start_at");
        assert_eq!(arguments["day"], "next");
        assert!(arguments["objective"]
            .as_str()
            .unwrap()
            .contains("Clarification: London please"));

        let completed = vec![
            ChatMessage::new(Role::User, "get the weather at 8:31"),
            ChatMessage::new(Role::Assistant, "It is currently overcast."),
        ];
        assert!(!future_schedule_required(&completed, "Thanks"));
    }
}
