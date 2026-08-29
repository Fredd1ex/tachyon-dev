use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const CATEGORIES: &[&str] = &[
    "profile",
    "preferences",
    "projects",
    "tasks",
    "events",
    "working",
    "archive",
];

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("memory io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("memory metadata error: {0}")]
    Metadata(#[from] toml::de::Error),
    #[error("memory serialization error: {0}")]
    Serialization(#[from] toml::ser::Error),
    #[error("invalid memory identifier: {0}")]
    InvalidIdentifier(String),
    #[error("task not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Ready,
    Running,
    Waiting,
    Paused,
    Completed,
    Failed,
    Terminated,
    Released,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TaskMetadata {
    pub id: String,
    pub kind: String,
    pub objective: String,
    pub state: TaskState,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default = "default_sensitivity")]
    pub sensitivity: String,
}

fn default_sensitivity() -> String {
    "normal".into()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TaskDocument {
    pub metadata: TaskMetadata,
    pub body: String,
}

pub struct MemoryStore {
    root: PathBuf,
}

impl MemoryStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, MemoryError> {
        let store = Self { root: root.into() };
        store.ensure_layout()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn ensure_layout(&self) -> Result<(), MemoryError> {
        for category in CATEGORIES {
            fs::create_dir_all(self.root.join(category))?;
        }
        Ok(())
    }

    pub fn write_task(&self, document: &TaskDocument) -> Result<(), MemoryError> {
        validate_identifier(&document.metadata.id)?;
        let path = self
            .root
            .join("tasks")
            .join(format!("{}.md", document.metadata.id));
        let temp = path.with_extension(format!("md.tmp.{}", std::process::id()));
        let metadata = toml::to_string_pretty(&document.metadata)?;
        let content = format!("+++\n{metadata}+++\n\n{}\n", document.body.trim_end());
        fs::write(&temp, content)?;
        fs::rename(temp, path)?;
        Ok(())
    }

    pub fn read_task(&self, id: &str) -> Result<TaskDocument, MemoryError> {
        validate_identifier(id)?;
        let path = self.root.join("tasks").join(format!("{id}.md"));
        let content = fs::read_to_string(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                MemoryError::NotFound(id.into())
            } else {
                MemoryError::Io(error)
            }
        })?;
        parse_task(&content)
    }

    pub fn list_tasks(&self) -> Result<Vec<TaskDocument>, MemoryError> {
        let mut tasks = Vec::new();
        for entry in fs::read_dir(self.root.join("tasks"))? {
            let entry = entry?;
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("md") {
                continue;
            }
            tasks.push(parse_task(&fs::read_to_string(entry.path())?)?);
        }
        tasks.sort_by(|left, right| left.metadata.id.cmp(&right.metadata.id));
        Ok(tasks)
    }
}

fn validate_identifier(id: &str) -> Result<(), MemoryError> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
    {
        return Err(MemoryError::InvalidIdentifier(id.into()));
    }
    Ok(())
}

fn parse_task(content: &str) -> Result<TaskDocument, MemoryError> {
    let Some(rest) = content.strip_prefix("+++\n") else {
        return Err(MemoryError::InvalidIdentifier(
            "missing TOML front matter".into(),
        ));
    };
    let Some((metadata, body)) = rest.split_once("\n+++\n") else {
        return Err(MemoryError::InvalidIdentifier(
            "unterminated TOML front matter".into(),
        ));
    };
    Ok(TaskDocument {
        metadata: toml::from_str(metadata)?,
        body: body.trim_start_matches('\n').trim_end().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_round_trip_uses_markdown_front_matter() {
        let root = std::env::temp_dir().join(format!("tachyon-memory-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = MemoryStore::open(&root).unwrap();
        let document = TaskDocument {
            metadata: TaskMetadata {
                id: "task-weather".into(),
                kind: "task".into(),
                objective: "Find the forecast".into(),
                state: TaskState::Waiting,
                depends_on: vec!["task-location".into()],
                created_at: "2026-08-23T00:00:00Z".into(),
                updated_at: "2026-08-23T00:01:00Z".into(),
                sensitivity: "normal".into(),
            },
            body: "Waiting for the location task.".into(),
        };
        store.write_task(&document).unwrap();
        assert_eq!(store.read_task("task-weather").unwrap(), document);
        assert_eq!(store.list_tasks().unwrap().len(), 1);
        let _ = fs::remove_dir_all(root);
    }
}
