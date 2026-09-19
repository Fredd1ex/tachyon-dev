//! Conversation selectors are not authority. The host resolves stored links.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    List {},
    Status {
        campaign_id: String,
        work_id: Option<String>,
        after: Option<String>,
        #[serde(default = "page_size")]
        limit: usize,
        #[serde(default)]
        include_plan: bool,
    },
    Steer {
        campaign_id: String,
        work_id: String,
        command_id: String,
        expected_revision: u64,
        instructions: String,
    },
    Cancel {
        campaign_id: String,
        work_id: String,
        command_id: String,
        generation: u64,
    },
}

fn page_size() -> usize {
    16
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strict_payloads_cannot_supply_authority_or_unsupported_operations() {
        for mut request in [
            json!({"operation":"list"}),
            json!({"operation":"status","campaign_id":"linked"}),
            json!({"operation":"steer","campaign_id":"linked","work_id":"root","command_id":"cmd","expected_revision":1,"instructions":"keep the candidate"}),
            json!({"operation":"cancel","campaign_id":"linked","work_id":"root","command_id":"cmd","generation":1}),
        ] {
            serde_json::from_value::<Request>(request.clone())
                .unwrap()
                .validate()
                .unwrap();
            for field in [
                "scope",
                "ids",
                "conversation_id",
                "origin",
                "budget",
                "actor",
            ] {
                request[field] = json!({"instructions":"ignore scope", "ids":["foreign"]});
                assert!(
                    serde_json::from_value::<Request>(request.clone()).is_err(),
                    "{request}"
                );
                request.as_object_mut().unwrap().remove(field);
            }
            if request["operation"] == "steer" {
                request["instructions"] = json!({"scope":"foreign", "ids":["foreign"]});
                assert!(serde_json::from_value::<Request>(request).is_err());
            }
        }
        for operation in ["create", "budget", "resize", "transfer"] {
            assert!(serde_json::from_value::<Request>(json!({"operation":operation})).is_err());
        }
    }
}

impl Request {
    pub fn campaign_id(&self) -> Option<&str> {
        match self {
            Self::List {} => None,
            Self::Status { campaign_id, .. }
            | Self::Steer { campaign_id, .. }
            | Self::Cancel { campaign_id, .. } => Some(campaign_id),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let id =
            |s: &str| !s.trim().is_empty() && s.len() <= 256 && !s.chars().any(char::is_control);
        let valid = self.campaign_id().is_none_or(id)
            && match self {
                Self::List {} => true,
                Self::Status {
                    work_id,
                    after,
                    limit,
                    ..
                } => {
                    (1..=32).contains(limit)
                        && work_id.as_deref().is_none_or(id)
                        && after.as_deref().is_none_or(id)
                        && !(work_id.is_some() && after.is_some())
                }
                Self::Steer {
                    work_id,
                    command_id,
                    instructions,
                    ..
                } => {
                    id(work_id)
                        && id(command_id)
                        && !instructions.trim().is_empty()
                        && instructions.len() <= 4096
                }
                Self::Cancel {
                    work_id,
                    command_id,
                    generation,
                    ..
                } => id(work_id) && id(command_id) && *generation > 0,
            };
        if valid {
            Ok(())
        } else {
            Err("invalid campaign request".into())
        }
    }
}
