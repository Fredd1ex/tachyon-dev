#![forbid(unsafe_code)]

use std::path::{Component, Path, PathBuf};

use super::{ToolContext, ToolError, ToolErrorCode};

pub(crate) const NATIVE_WRITE_LOCK_DIRECTORY: &str = ".tachyon-write-locks";

pub(crate) struct WriteTarget {
    pub path: PathBuf,
    pub parent: PathBuf,
    pub existed: bool,
    pub locked_parent: Option<std::fs::File>,
}

pub(crate) async fn resolve_existing(
    context: &ToolContext,
    requested: &str,
) -> Result<PathBuf, ToolError> {
    let candidate = candidate(context, requested)?;

    let canonical = tokio::fs::canonicalize(&candidate).await.map_err(|error| {
        let code = match error.kind() {
            std::io::ErrorKind::NotFound => ToolErrorCode::NotFound,
            std::io::ErrorKind::PermissionDenied => ToolErrorCode::PermissionDenied,
            _ => ToolErrorCode::Io,
        };
        ToolError::new(
            code,
            format!("cannot resolve {requested:?}: {error}"),
            false,
        )
    })?;
    if !is_allowed(context, &canonical) {
        return Err(outside_workspace(requested));
    }
    Ok(canonical)
}

pub(crate) async fn resolve_write_target(
    context: &ToolContext,
    requested: &str,
    create_parents: bool,
) -> Result<WriteTarget, ToolError> {
    let candidate = candidate(context, requested)?;
    match tokio::fs::symlink_metadata(&candidate).await {
        Ok(metadata) => {
            return existing_write_target(context, requested, candidate, metadata).await
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(requested, error)),
    }

    let requested_parent = candidate
        .parent()
        .ok_or_else(|| ToolError::invalid("write target has no parent directory"))?;
    let mut nearest = requested_parent.to_path_buf();
    loop {
        match tokio::fs::symlink_metadata(&nearest).await {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !nearest.pop() {
                    return Err(outside_workspace(requested));
                }
            }
            Err(error) => return Err(io_error(requested, error)),
        }
    }
    let canonical_nearest = tokio::fs::canonicalize(&nearest)
        .await
        .map_err(|error| io_error(requested, error))?;
    if !is_allowed(context, &canonical_nearest) {
        return Err(outside_workspace(requested));
    }
    if nearest != requested_parent && !create_parents {
        return Err(ToolError::new(
            ToolErrorCode::NotFound,
            "parent directory does not exist and create_parents is false",
            false,
        ));
    }
    if create_parents {
        tokio::fs::create_dir_all(requested_parent)
            .await
            .map_err(|error| io_error(requested, error))?;
    }
    let parent = tokio::fs::canonicalize(requested_parent)
        .await
        .map_err(|error| io_error(requested, error))?;
    if !is_allowed(context, &parent) {
        return Err(outside_workspace(requested));
    }

    match tokio::fs::symlink_metadata(
        parent.join(
            candidate
                .file_name()
                .ok_or_else(|| ToolError::invalid("write target must include a file name"))?,
        ),
    )
    .await
    {
        Ok(metadata) => existing_write_target(context, requested, candidate, metadata).await,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(WriteTarget {
            path: parent.join(candidate.file_name().expect("file name validated")),
            parent,
            existed: false,
            locked_parent: None,
        }),
        Err(error) => Err(io_error(requested, error)),
    }
}

async fn existing_write_target(
    context: &ToolContext,
    requested: &str,
    candidate: PathBuf,
    metadata: std::fs::Metadata,
) -> Result<WriteTarget, ToolError> {
    if metadata.file_type().is_symlink() {
        return Err(ToolError::new(
            ToolErrorCode::PermissionDenied,
            "write targets may not be symlinks",
            false,
        ));
    }
    if !metadata.is_file() {
        return Err(ToolError::invalid("write target is not a regular file"));
    }
    if metadata.permissions().readonly() {
        return Err(ToolError::new(
            ToolErrorCode::PermissionDenied,
            "read-only files cannot be replaced",
            false,
        ));
    }
    let path = tokio::fs::canonicalize(candidate)
        .await
        .map_err(|error| io_error(requested, error))?;
    if !is_allowed(context, &path) {
        return Err(outside_workspace(requested));
    }
    let parent = path
        .parent()
        .ok_or_else(|| ToolError::invalid("write target has no parent directory"))?
        .to_path_buf();
    Ok(WriteTarget {
        path,
        parent,
        existed: true,
        locked_parent: None,
    })
}

fn candidate(context: &ToolContext, requested: &str) -> Result<PathBuf, ToolError> {
    if requested.is_empty() || requested.contains('\0') {
        return Err(ToolError::invalid(
            "path must be a non-empty string without NUL bytes",
        ));
    }
    if requested.len() > 4096 {
        return Err(ToolError::invalid("path exceeds 4096 bytes"));
    }
    let path = Path::new(requested);
    if path.is_absolute() && !context.policy.allow_absolute_paths {
        return Err(ToolError::new(
            ToolErrorCode::PermissionDenied,
            "absolute paths are disabled by policy",
            false,
        ));
    }
    let candidate = if path.is_absolute() {
        normalize(path)
    } else {
        normalize(&context.cwd.join(path))
    };
    if !is_allowed(context, &candidate) {
        return Err(outside_workspace(requested));
    }
    Ok(candidate)
}

pub(crate) fn is_allowed(context: &ToolContext, path: &Path) -> bool {
    !path
        .components()
        .any(|part| part.as_os_str() == NATIVE_WRITE_LOCK_DIRECTORY)
        && context
            .policy
            .allowed_roots
            .iter()
            .any(|root| path.starts_with(root))
}

fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn outside_workspace(requested: &str) -> ToolError {
    ToolError::new(
        ToolErrorCode::OutsideWorkspace,
        format!("path escapes the allowed workspace: {requested:?}"),
        false,
    )
}

fn io_error(requested: &str, error: std::io::Error) -> ToolError {
    let code = match error.kind() {
        std::io::ErrorKind::NotFound => ToolErrorCode::NotFound,
        std::io::ErrorKind::PermissionDenied => ToolErrorCode::PermissionDenied,
        _ => ToolErrorCode::Io,
    };
    ToolError::new(
        code,
        format!("cannot resolve {requested:?}: {error}"),
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::harness::runtime::{NoopEventSink, NoopOutputStore, ToolIdentity, ToolPolicy};

    fn context(root: &Path) -> ToolContext {
        let root = root.canonicalize().unwrap();
        ToolContext {
            workspace_root: root.clone(),
            cwd: root.clone(),
            identity: ToolIdentity::default(),
            deadline: Instant::now() + Duration::from_secs(2),
            cancellation: CancellationToken::new(),
            policy: Arc::new(ToolPolicy::worker_default(root)),
            event_sink: Arc::new(NoopEventSink),
            output_store: Arc::new(NoopOutputStore),
            host_service: None,
        }
    }

    #[tokio::test]
    async fn rejects_parent_traversal() {
        let root = tempdir().unwrap();
        let error = resolve_existing(&context(root.path()), "../outside")
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::OutsideWorkspace);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinks_that_escape_the_workspace() {
        let root = tempdir().unwrap();
        std::os::unix::fs::symlink("/etc/passwd", root.path().join("escape")).unwrap();
        let error = resolve_existing(&context(root.path()), "escape")
            .await
            .unwrap_err();
        assert_eq!(error.code, ToolErrorCode::OutsideWorkspace);
    }

    #[tokio::test]
    async fn resolves_new_targets_only_when_parent_policy_allows_it() {
        let root = tempdir().unwrap();
        let context = context(root.path());
        let error = resolve_write_target(&context, "new/child.txt", false)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, ToolErrorCode::NotFound);

        let target = resolve_write_target(&context, "new/child.txt", true)
            .await
            .unwrap();
        assert!(!target.existed);
        assert!(target.parent.ends_with("new"));
        assert!(target.path.ends_with("new/child.txt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_new_targets_below_an_escaping_symlink() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
        let error = resolve_write_target(&context(root.path()), "escape/file", true)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, ToolErrorCode::OutsideWorkspace);
        assert!(!outside.path().join("file").exists());
    }
}
