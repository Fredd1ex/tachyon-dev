#![forbid(unsafe_code)]

use std::path::PathBuf;

pub fn memory_path() -> PathBuf {
    tachyon_util::daemon::memories_database_path()
}

pub fn erase(paths: &[PathBuf]) -> Result<Vec<(PathBuf, bool)>, String> {
    paths
        .iter()
        .map(|path| match std::fs::remove_file(path) {
            Ok(()) => Ok((path.clone(), true)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok((path.clone(), false)),
            Err(error) => Err(format!("delete {}: {error}", path.display())),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn erase_removes_present_files_and_accepts_absent_files() {
        let directory = std::env::temp_dir().join(format!(
            "tachyon-data-erase-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let present = directory.join("present.redb");
        let absent = directory.join("absent.redb");
        std::fs::write(&present, b"database").unwrap();

        let erased = erase(&[present.clone(), absent.clone()]).unwrap();

        assert_eq!(erased, vec![(present.clone(), true), (absent, false)]);
        assert!(!present.exists());
        std::fs::remove_dir(directory).unwrap();
    }
}
