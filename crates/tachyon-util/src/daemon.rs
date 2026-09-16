#![forbid(unsafe_code)]

//! Shared daemon layout + process management helpers used by both the
//! `tachyond` binary and the `tachyon` CLI.

use std::fs;
use std::path::PathBuf;

pub const DATA_DIR_ENV: &str = "TACHYON_DATA_DIR";

/// The base Tachyon data directory, e.g. `~/.local/share/tachyon`.
pub fn data_dir() -> PathBuf {
    if let Ok(d) = std::env::var(DATA_DIR_ENV) {
        return PathBuf::from(d);
    }
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tachyon")
}

pub fn runtime_dir() -> PathBuf {
    data_dir().join("state")
}

pub fn logs_dir() -> PathBuf {
    data_dir().join("agents")
}

pub fn workspaces_dir() -> PathBuf {
    data_dir().join("workspaces")
}

/// Validate host selection without creating directories or changing process cwd.
pub fn selected_workspace(path: &str) -> Result<PathBuf, String> {
    let path = std::path::Path::new(path);
    if !path.is_absolute() {
        return Err("workspace selection must be an absolute path".into());
    }
    let path = path
        .canonicalize()
        .map_err(|error| format!("workspace unavailable: {error}"))?;
    if !path.is_dir() {
        return Err("workspace selection is not a directory".into());
    }
    if path.parent().is_none() {
        return Err("filesystem root cannot be a worker workspace".into());
    }
    Ok(path)
}

/// Resolve configuration only, without creating the managed root.
pub fn managed_agent_root(config: &crate::config::Config) -> Result<PathBuf, String> {
    let root = config
        .managed_agent_root
        .clone()
        .or_else(|| dirs::home_dir().map(|home| home.join("Agents")))
        .ok_or("managed workspace requires a home directory or managed_agent_root")?;
    if !root.is_absolute() || root.parent().is_none() {
        return Err("managed_agent_root must be an absolute non-root directory".into());
    }
    Ok(root)
}

/// Ensure the managed root exists without modifying any existing contents.
pub fn ensure_managed_agent_root(root: &std::path::Path) -> Result<PathBuf, String> {
    if !root.is_absolute() || root.parent().is_none() {
        return Err("managed_agent_root must be an absolute non-root directory".into());
    }
    fs::create_dir_all(root).map_err(|error| {
        format!(
            "managed root provisioning failed at {}: {error}",
            root.display()
        )
    })?;
    selected_workspace(&root.to_string_lossy())
}

pub fn provision_managed_workspace(root: &std::path::Path, id: &str) -> Result<PathBuf, String> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err("invalid managed workspace id".into());
    }
    // Refuse collisions rather than adopting another assignment's data or symlinks.
    let root = ensure_managed_agent_root(root)?;
    let workspace = root.join(id);
    fs::create_dir(&workspace)
        .map_err(|error| format!("managed workspace provisioning failed: {error}"))?;
    let workspace = selected_workspace(&workspace.to_string_lossy())?;
    if workspace.parent() != Some(root.as_path()) {
        return Err("managed workspace escaped configured root".into());
    }
    for directory in ["research", "artifacts"] {
        fs::create_dir(workspace.join(directory))
            .map_err(|error| format!("managed {directory} provisioning failed: {error}"))?;
    }
    Ok(workspace)
}

/// Authoritative databases. Portable imports and exports live outside this
/// directory and must go through their versioned APIs.
pub fn databases_dir() -> PathBuf {
    data_dir().join("databases")
}

pub fn runtime_database_path() -> PathBuf {
    databases_dir().join("runtime.redb")
}

pub fn history_database_path() -> PathBuf {
    databases_dir().join("history.redb")
}

pub fn memories_database_path() -> PathBuf {
    databases_dir().join("memories.redb")
}

pub fn pidfile_path() -> PathBuf {
    runtime_dir().join("tachyond.pid")
}

pub fn socket_path() -> PathBuf {
    runtime_dir().join("tachyond.sock")
}

/// Create the standard data directory layout.
pub fn ensure_layout() -> std::io::Result<()> {
    fs::create_dir_all(data_dir())?;
    fs::create_dir_all(runtime_dir())?;
    fs::create_dir_all(logs_dir())?;
    fs::create_dir_all(workspaces_dir())?;
    fs::create_dir_all(databases_dir())?;
    Ok(())
}

/// Read the daemon pid, if one was written.
pub fn read_pid() -> Option<u32> {
    let contents = fs::read_to_string(pidfile_path()).ok()?;
    contents.trim().parse::<u32>().ok()
}

/// Whether a pid currently exists (probe with signal 0).
fn pid_alive(pid: u32) -> bool {
    #[cfg(target_family = "unix")]
    {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        let result = kill(Pid::from_raw(pid as i32), None);
        // Ok means the process exists and we may signal it; EPERM means it
        // exists but belongs to another user. Anything else (ESRCH) is dead.
        matches!(result, Ok(())) || matches!(result, Err(Errno::EPERM))
    }
    #[cfg(not(target_family = "unix"))]
    {
        false
    }
}

/// Returns true if the daemon appears to be running.
pub fn is_running() -> bool {
    running_pid().is_some()
}

/// Returns the running daemon pid, if any.
pub fn running_pid() -> Option<u32> {
    read_pid().filter(|pid| pid_alive(*pid))
}

/// Write the daemon pid file.
pub fn write_pid(pid: u32) -> std::io::Result<()> {
    ensure_layout()?;
    fs::write(pidfile_path(), pid.to_string())
}

/// Remove the daemon pid file.
pub fn clear_pid() {
    let _ = fs::remove_file(pidfile_path());
}

/// The path to the daemon's own log file.
pub fn daemon_log_path() -> PathBuf {
    logs_dir().join("tachyond.log")
}

/// Path to the single-instance lock file.
pub fn lock_path() -> PathBuf {
    runtime_dir().join("tachyond.lock")
}

/// A held single-instance lock. Dropping it releases the lock.
///
/// Based on `flock(2)` semantics: the lock is tied to the open file
/// description, so if the process dies (even by SIGKILL) the kernel releases
/// it automatically. This makes it robust against stale pidfiles.
pub struct Lock {
    #[allow(dead_code)] // held open for the lifetime to keep the flock.
    file: std::fs::File,
}

impl Lock {
    /// Try to acquire the daemon's single-instance lock.
    ///
    /// Returns `Some(lock)` on success (the caller should keep it alive for
    /// the daemon's lifetime), or `None` if another daemon instance already
    /// holds the lock.
    pub fn try_acquire() -> std::io::Result<Option<Self>> {
        use fs2::FileExt;
        ensure_layout()?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(lock_path())?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Lock { file })),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}
