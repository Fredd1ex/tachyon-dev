//! Native process-group cleanup shared by Ghost and host command verification.
//! This is lifecycle supervision, not isolation: descendants can escape a group.
use std::{io, process::ExitStatus, time::Duration};
use tokio::process::Child;

pub struct ProcessGroupGuard(pub Option<u32>);

/// After kill/reap, distinguish terminated zombies from processes still using
/// native resources. Failure to inspect a group is not cleanup confirmation.
pub async fn group_has_live_processes(pid: u32) -> io::Result<bool> {
    match nix::sys::signal::kill(nix::unistd::Pid::from_raw(-(pid as i32)), None) {
        Err(nix::errno::Errno::ESRCH) => return Ok(false),
        Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
        Ok(()) => {}
    }
    #[cfg(target_os = "linux")]
    {
        tokio::task::spawn_blocking(move || {
            for entry in std::fs::read_dir("/proc")? {
                let entry = entry?;
                if entry.file_name().to_string_lossy().parse::<u32>().is_err() {
                    continue;
                }
                let stat = match std::fs::read_to_string(entry.path().join("stat")) {
                    Ok(stat) => stat,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                // comm can contain spaces and parentheses; fields after its
                // final ')' start with state, ppid, pgrp.
                let (_, fields) = stat
                    .rsplit_once(')')
                    .ok_or_else(|| io::Error::other("invalid process stat"))?;
                let mut fields = fields.split_whitespace();
                let state = fields
                    .next()
                    .ok_or_else(|| io::Error::other("missing process state"))?;
                let group = fields
                    .nth(1)
                    .and_then(|s| s.parse::<u32>().ok())
                    .ok_or_else(|| io::Error::other("missing process group"))?;
                if group == pid && !matches!(state, "Z" | "X") {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .map_err(io::Error::other)?
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(true)
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = signal_group(pid, nix::sys::signal::Signal::SIGKILL);
        }
    }
}

pub fn signal_group(pid: u32, signal: nix::sys::signal::Signal) -> io::Result<()> {
    match nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid as i32), signal) {
        Ok(()) | Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(e) => Err(io::Error::from_raw_os_error(e as i32)),
    }
}

pub async fn cleanup_remaining_group(pid: u32, grace: Duration) -> io::Result<()> {
    use nix::{
        errno::Errno,
        sys::signal::{kill, Signal},
        unistd::Pid,
    };
    signal_group(pid, Signal::SIGTERM)?;
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match kill(Pid::from_raw(-(pid as i32)), None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(()) => return signal_group(pid, Signal::SIGKILL),
            Err(e) => return Err(io::Error::from_raw_os_error(e as i32)),
        }
    }
}

pub async fn terminate_process_group(
    child: &mut Child,
    pid: u32,
    grace: Duration,
) -> io::Result<ExitStatus> {
    signal_group(pid, nix::sys::signal::Signal::SIGTERM)?;
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(status) => {
            let status = status?;
            cleanup_remaining_group(pid, grace).await?;
            Ok(status)
        }
        Err(_) => {
            signal_group(pid, nix::sys::signal::Signal::SIGKILL)?;
            tokio::time::timeout(grace.max(Duration::from_millis(100)), child.wait())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "process did not exit after SIGKILL",
                    )
                })?
        }
    }
}
