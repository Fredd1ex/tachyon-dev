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
