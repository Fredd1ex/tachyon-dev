use tachyon_api::{
    BackgroundScheduleAction, BackgroundScheduleDecision, BackgroundScheduleRequest,
};

pub(super) fn review_schedule_request(
    request: BackgroundScheduleRequest,
) -> BackgroundScheduleDecision {
    let valid = !request.request_id.trim().is_empty()
        && match &request.action {
            BackgroundScheduleAction::Create {
                source_event_id,
                conversation_id,
                text,
                delay_seconds,
                local_time,
                day,
                ..
            } => {
                !source_event_id.trim().is_empty()
                    && !conversation_id.trim().is_empty()
                    && !text.trim().is_empty()
                    && text.chars().count() <= 500
                    && match (delay_seconds, local_time, day) {
                        (Some(delay), None, None) => (1..=31_536_000).contains(delay),
                        (None, Some(time), Some(_)) => !time.trim().is_empty(),
                        _ => false,
                    }
            }
            BackgroundScheduleAction::List => true,
            BackgroundScheduleAction::Cancel { id } => !id.trim().is_empty(),
            BackgroundScheduleAction::CreateTask {
                source_event_id,
                conversation_id,
                objective,
                delay_seconds,
                local_time,
                day,
                ..
            } => {
                !source_event_id.trim().is_empty()
                    && !conversation_id.trim().is_empty()
                    && !objective.trim().is_empty()
                    && objective.chars().count() <= 4_000
                    && match (delay_seconds, local_time, day) {
                        (Some(delay), None, None) => (1..=31_536_000).contains(delay),
                        (None, Some(time), Some(_)) => !time.trim().is_empty(),
                        _ => false,
                    }
            }
        };
    BackgroundScheduleDecision {
        request_id: request.request_id,
        action: request.action,
        approved: valid,
        reason: if valid {
            "validated".into()
        } else {
            "invalid schedule request".into()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_requests_are_validated_without_a_model() {
        let valid = review_schedule_request(BackgroundScheduleRequest {
            request_id: "schedule-1".into(),
            action: BackgroundScheduleAction::Create {
                source_event_id: "turn-1".into(),
                conversation_id: "foreground".into(),
                turn: 1,
                text: "Your coffee is ready.".into(),
                delay_seconds: Some(60),
                local_time: None,
                day: None,
                created_at_ms: 123,
            },
        });
        assert!(valid.approved);

        let invalid = review_schedule_request(BackgroundScheduleRequest {
            request_id: "schedule-2".into(),
            action: BackgroundScheduleAction::Create {
                source_event_id: "turn-2".into(),
                conversation_id: "foreground".into(),
                turn: 2,
                text: String::new(),
                delay_seconds: Some(0),
                local_time: None,
                day: None,
                created_at_ms: 124,
            },
        });
        assert!(!invalid.approved);
    }
}
