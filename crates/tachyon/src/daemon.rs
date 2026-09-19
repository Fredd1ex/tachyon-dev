#![forbid(unsafe_code)]

//! CLI-side daemon management: status, start, stop, restart, and auto-start.

use std::path::Path;
use std::process::{Child, Command, Stdio};

#[cfg(target_family = "unix")]
use std::os::unix::process::CommandExt;

use tachyon_util::daemon::{self, is_running, running_pid};

pub fn status() -> bool {
    is_running()
}

pub fn pid() -> Option<u32> {
    running_pid()
}

/// Start the daemon in the background if it is not already running.
///
/// Returns true if the daemon is running afterwards (either was already
/// running, or we successfully started it).
pub fn ensure_running() -> bool {
    if is_running() {
        return true;
    }
    start().is_some()
}

/// Launch the daemon detached from this process. Returns the new pid on success.
pub fn start() -> Option<u32> {
    let current_exe = std::env::current_exe().ok()?;
    let tachyond_bin = current_exe.parent()?.join("tachyond");

    let mut child = match launch(&tachyond_bin) {
        Ok(child) => child,
        Err(error) => {
            eprintln!("{}", launch_error(&tachyond_bin, &error));
            return None;
        }
    };

    // Give it a moment to write its pid file.
    for _ in 0..30 {
        if let Some(pid) = running_pid() {
            return Some(pid);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Didn't detect it; drain child to avoid a zombie and report none.
    let _ = child.wait();
    None
}

fn launch(path: &Path) -> std::io::Result<Child> {
    Command::new(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        // Detach from the calling session so closing the CLI doesn't kill us.
        .process_group(0)
        .spawn()
}

fn launch_error(path: &Path, error: &std::io::Error) -> String {
    let mut message = format!("failed to launch {}: {error}", path.display());
    if error.kind() == std::io::ErrorKind::NotFound {
        message.push_str("\nBuild all companion binaries from the repository root: cargo build --workspace --bins\nBuilding or running only --bin tachyon does not build tachyond or the interaction hosts.");
    }
    message
}

/// Stop the daemon gracefully (SIGTERM, escalating to SIGKILL). Returns Ok if
/// it was running or stopped.
pub fn stop() -> Result<(), String> {
    match running_pid() {
        Some(pid) => {
            #[cfg(target_family = "unix")]
            {
                use nix::errno::Errno;
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                let p = Pid::from_raw(pid as i32);

                // Graceful first.
                match kill(p, Some(Signal::SIGTERM)) {
                    Ok(()) => {}
                    Err(Errno::ESRCH) => {
                        daemon::clear_pid();
                        return Ok(());
                    }
                    Err(e) => return Err(e.to_string()),
                }
                // Wait up to 5s for a clean exit.
                for _ in 0..50 {
                    if !is_running() {
                        return Ok(());
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                // Escalate to SIGKILL.
                let _ = kill(p, Some(Signal::SIGKILL));
                for _ in 0..50 {
                    if !is_running() {
                        return Ok(());
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                daemon::clear_pid();
                Ok(())
            }
            #[cfg(not(target_family = "unix"))]
            {
                let _ = (pid, daemon::clear_pid());
                Ok(())
            }
        }
        None => Ok(()),
    }
}

/// Force-kill the daemon (SIGKILL) if running.
pub fn kill() -> Result<(), String> {
    match running_pid() {
        Some(pid) => {
            #[cfg(target_family = "unix")]
            {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                kill(Pid::from_raw(pid as i32), Some(Signal::SIGKILL))
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        None => Ok(()),
    }
}

/// Restart the daemon: stop if running, then start fresh.
pub fn restart() -> Option<u32> {
    // Do not stop a working daemon when its replacement has been cleaned away.
    let executable = std::env::current_exe().ok()?.parent()?.join("tachyond");
    if let Err(error) = std::fs::metadata(&executable) {
        eprintln!("{}", launch_error(&executable, &error));
        return None;
    }
    if let Err(error) = stop() {
        eprintln!("failed to stop daemon before restart: {error}");
        return None;
    }
    // Ensure the old pid is cleared so start() doesn't see a live pid.
    daemon::clear_pid();
    start()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_companion_reports_path_and_workspace_build_command() {
        let missing = std::env::temp_dir()
            .join(format!(
                "tachyon-missing-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .join("tachyond");
        let error = launch(&missing).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        let message = launch_error(&missing, &error);
        assert!(message.contains(missing.to_str().unwrap()));
        assert!(message.contains("cargo build --workspace --bins"));
    }

    #[test]
    fn launch_failure_preserves_non_missing_errors() {
        let error = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let message = launch_error(Path::new("/example/tachyond"), &error);
        assert!(message.contains("/example/tachyond"));
        assert!(message.contains(&error.to_string()));
        assert!(!message.contains("cargo build"));
    }
}
