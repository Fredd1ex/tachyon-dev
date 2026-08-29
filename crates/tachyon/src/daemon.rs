#![forbid(unsafe_code)]

//! CLI-side daemon management: status, start, stop, restart, and auto-start.

use std::process::{Command, Stdio};

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

    let mut child = Command::new(&tachyond_bin)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        // Detach from the calling session so closing the CLI doesn't kill us.
        .process_group(0)
        .spawn()
        .ok()?;

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
    let _ = stop();
    // Ensure the old pid is cleared so start() doesn't see a live pid.
    daemon::clear_pid();
    start()
}
