#![forbid(unsafe_code)]

mod edit;
mod find;
pub mod integration;
mod list;
mod read;
mod search;
mod version;
mod write;
mod write_lock;

pub use edit::EditTool;
pub use find::FindTool;
pub use list::LsTool;
pub use read::ReadTool;
pub use search::GrepTool;
pub use write::WriteTool;

/// Integrate one exact patch against an inspected version using native policy and
/// atomic replacement. Separate calls are separate transactions, never a batch commit.
pub async fn apply_exact_patch(
    context: &crate::harness::runtime::ToolContext,
    path: &str,
    expected_version: &str,
    old: &str,
    new: &str,
) -> Result<crate::harness::runtime::ToolResult, crate::harness::runtime::ToolError> {
    use crate::harness::runtime::Tool;
    EditTool::new()
        .execute(
            context,
            serde_json::json!({
                "path": path, "expected_version": expected_version, "old": old, "new": new,
            }),
        )
        .await
}

pub const USAGE: &str = include_str!("usage.md");
pub const INTERFACE: &str = "`ls`/`find`: paths; `grep`: search; `read`: text; `edit`: exact replacement; `write`: whole file. Pass read metadata.version as expected_version for edits/overwrites; re-read on conflict. sha256 requires a complete read. Prefer native read/grep. Sample unknown structure first; follow continuations and report skips/errors/truncation for coverage. Paths are workspace-relative; use native schemas.";
