//! Private selectors contain no caller-supplied authority or scope identities.
use crate::todo::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    #[default]
    CurrentWork,
    CurrentCampaign,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum TodoRequest {
    List {
        #[serde(default)]
        scope: Scope,
        #[serde(default)]
        filter: TodoFilter,
        limit: Option<usize>,
        cursor: Option<TodoCursor>,
    },
    Add {
        #[serde(default)]
        scope: Scope,
        command_id: String,
        expected_revision: u64,
        title: String,
        #[serde(default)]
        description: String,
    },
    Update {
        #[serde(default)]
        scope: Scope,
        command_id: String,
        expected_revision: u64,
        id: String,
        title: Option<String>,
        description: Option<String>,
        status: Option<TodoStatus>,
    },
}
impl TodoRequest {
    pub fn scope(&self) -> Scope {
        match self {
            Self::List { scope, .. } | Self::Add { scope, .. } | Self::Update { scope, .. } => {
                *scope
            }
        }
    }
    /// Called only by the host after validating the permit and scope grant.
    pub fn bind(self, scope: TodoScope) -> crate::todo::TodoRequest {
        match self {
            Self::List {
                filter,
                limit,
                cursor,
                ..
            } => crate::todo::TodoRequest::List {
                scope,
                filter,
                limit: Some(limit.unwrap_or(8)),
                cursor,
            },
            Self::Add {
                command_id,
                expected_revision,
                title,
                description,
                ..
            } => crate::todo::TodoRequest::Add {
                scope,
                command_id,
                expected_revision,
                title,
                description,
            },
            Self::Update {
                command_id,
                expected_revision,
                id,
                title,
                description,
                status,
                ..
            } => crate::todo::TodoRequest::Update {
                scope,
                command_id,
                expected_revision,
                id,
                title,
                description,
                status,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum MonitorRequest {
    Snapshot {
        #[serde(default)]
        scope: Scope,
        after: Option<String>,
        #[serde(default = "page_limit")]
        limit: usize,
    },
}
fn page_limit() -> usize {
    16
}
impl MonitorRequest {
    pub fn scope(&self) -> Scope {
        let Self::Snapshot { scope, .. } = self;
        *scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn selectors_and_mutations_are_strict_and_independently_granted() {
        for value in [
            json!({"action":"list","scope":"conversation"}),
            json!({"action":"list","work_id":"foreign"}),
            json!({"action":"add","title":"x","command_id":"c"}),
            json!({"action":"add","title":"x","expected_revision":0}),
            json!({"action":"update","id":"x","expected_revision":1,"command_id":"c","accepted":true}),
            json!({"action":"list","actor":"forged"}),
        ] {
            assert!(serde_json::from_value::<TodoRequest>(value).is_err());
        }
        for value in [
            json!({"action":"cancel"}),
            json!({"action":"snapshot","scope":"host"}),
            json!({"action":"snapshot","campaign_id":"foreign"}),
        ] {
            assert!(serde_json::from_value::<MonitorRequest>(value).is_err());
        }
        let work: TodoRequest = serde_json::from_value(json!({"action":"list"})).unwrap();
        assert_eq!(work.scope(), Scope::CurrentWork);
        let campaign: TodoRequest =
            serde_json::from_value(json!({"action":"list","scope":"current_campaign"})).unwrap();
        assert_eq!(
            super::super::Request::Todo { request: work }.control(),
            super::super::Control::Todo
        );
        assert_eq!(
            super::super::Request::Todo { request: campaign }.control(),
            super::super::Control::TodoCampaign
        );
    }
}
