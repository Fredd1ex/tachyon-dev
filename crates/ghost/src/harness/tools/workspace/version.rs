#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};

use crate::harness::runtime::{ToolError, ToolErrorCode};

pub(super) fn sha256(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

pub(super) fn version(metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let identity = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        );
        format!("stat-v1:{}", sha256(identity.as_bytes()))
    }
    #[cfg(not(unix))]
    {
        let identity = format!(
            "{}:{:?}:{:?}",
            metadata.len(),
            metadata.modified(),
            metadata.created()
        );
        format!("stat-v1:{}", sha256(identity.as_bytes()))
    }
}

pub(super) fn conflict() -> ToolError {
    ToolError::new(
        ToolErrorCode::Conflict,
        "workspace version conflict: re-read the file and recompute the change",
        true,
    )
}

pub(super) fn check(
    expected_version: Option<&str>,
    expected_sha256: Option<&str>,
    actual_version: &str,
    content: Option<&[u8]>,
) -> Result<(), ToolError> {
    if expected_version.is_some_and(|expected| expected != actual_version)
        || expected_sha256
            .is_some_and(|expected| content.is_none_or(|bytes| expected != sha256(bytes)))
    {
        return Err(conflict());
    }
    Ok(())
}
