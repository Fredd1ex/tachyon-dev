#![forbid(unsafe_code)]

use std::path::PathBuf;

pub(crate) fn discover(name: &str, search_path: &str) -> Option<PathBuf> {
    search_path.split(':').find_map(|directory| {
        if directory.is_empty() {
            return None;
        }
        let candidate = PathBuf::from(directory).join(name);
        let metadata = std::fs::metadata(&candidate).ok()?;
        if !metadata.is_file() || !is_executable(&metadata) {
            return None;
        }
        Some(candidate)
    })
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[cfg(unix)]
    #[test]
    fn discovers_only_executable_files() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let binary = directory.path().join("tool");
        std::fs::write(&binary, "").unwrap();
        assert!(discover("tool", directory.path().to_str().unwrap()).is_none());
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            discover("tool", directory.path().to_str().unwrap()).as_deref(),
            Some(binary.as_path())
        );
    }
}
