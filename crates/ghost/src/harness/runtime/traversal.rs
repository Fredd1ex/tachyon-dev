#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Instant;

use ignore::{DirEntry, WalkBuilder};
use tokio_util::sync::CancellationToken;

use super::{ToolError, ToolErrorCode};

pub(crate) struct TraversalOptions {
    pub root: PathBuf,
    pub hidden: bool,
    pub max_entries: usize,
    pub deadline: Instant,
    pub cancellation: CancellationToken,
}

pub(crate) struct TraversalStats {
    pub visited: usize,
    pub errors: usize,
    pub entry_limit_reached: bool,
}

pub(crate) enum WalkControl {
    Continue,
    Stop,
}

pub(crate) fn walk(
    options: TraversalOptions,
    mut visitor: impl FnMut(&DirEntry) -> Result<WalkControl, ToolError>,
) -> Result<TraversalStats, ToolError> {
    let mut builder = WalkBuilder::new(&options.root);
    builder
        .standard_filters(true)
        .follow_links(false)
        .hidden(!options.hidden)
        .require_git(false)
        .sort_by_file_name(|left, right| left.cmp(right));

    let mut stats = TraversalStats {
        visited: 0,
        errors: 0,
        entry_limit_reached: false,
    };
    for entry in builder.build() {
        check_interruption(&options)?;
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                stats.errors += 1;
                continue;
            }
        };
        if entry.depth() == 0 {
            continue;
        }
        if stats.visited == options.max_entries {
            stats.entry_limit_reached = true;
            break;
        }
        stats.visited += 1;
        if entry.file_type().is_some_and(|kind| kind.is_symlink()) {
            continue;
        }
        if matches!(visitor(&entry)?, WalkControl::Stop) {
            break;
        }
    }
    Ok(stats)
}

pub(crate) fn relative_utf8(root: &Path, entry: &DirEntry) -> Option<String> {
    let relative = entry.path().strip_prefix(root).ok()?;
    relative
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect::<Option<Vec<_>>>()
        .map(|components| components.join("/"))
}

fn check_interruption(options: &TraversalOptions) -> Result<(), ToolError> {
    if options.cancellation.is_cancelled() {
        return Err(ToolError::new(
            ToolErrorCode::Cancelled,
            "filesystem traversal cancelled",
            true,
        ));
    }
    if Instant::now() >= options.deadline {
        return Err(ToolError::new(
            ToolErrorCode::Timeout,
            "filesystem traversal timed out",
            true,
        ));
    }
    Ok(())
}
