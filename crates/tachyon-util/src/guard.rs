#![forbid(unsafe_code)]

//! Startup guards. Tachyon must not run as root unless the user explicitly
//! overrides the restriction.

use std::io::Write;

const ALLOW_ROOT_ENV: &str = "TACHYON_ALLOW_ROOT";

/// Returns true if the current process is running with effective UID 0 (root).
///
/// Uses `nix::unistd::geteuid` (safe wrapper over the C call, no raw FFI).
#[cfg(target_family = "unix")]
pub fn running_as_root() -> bool {
    use nix::unistd::Uid;
    Uid::effective() == Uid::from_raw(0)
}

#[cfg(not(target_family = "unix"))]
pub fn running_as_root() -> bool {
    false
}

/// Whether the user has explicitly allowed running as root.
pub fn root_allowed() -> bool {
    std::env::var(ALLOW_ROOT_ENV)
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

/// Guard against running as root.
///
/// If the process runs as root and the user has not set `TACHYON_ALLOW_ROOT`,
/// returns `Err`. The caller decides how to exit.
///
/// Returns `Ok(())` when not root, or when the user explicitly overrides.
pub fn check() -> Result<(), RootBlocked> {
    if running_as_root() && !root_allowed() {
        return Err(RootBlocked);
    }
    Ok(())
}

#[derive(Debug)]
pub struct RootBlocked;

impl std::fmt::Display for RootBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Tachyon refuses to run as root.\n\
             Run as an unprivileged user, or set {ALLOW_ROOT_ENV}=1 to override."
        )
    }
}

/// Exit code used when the root guard blocks startup.
pub const ROOT_BLOCKED_EXIT: i32 = 2;

/// Print a red warning to stderr. Uses raw ANSI to stay dependency-free.
pub fn warn_root() {
    let msg = format!(
        "\x1b[1;31mWarning:\x1b[0m running as root. Tachyon is designed to run \
         as an unprivileged user.\nSet {ALLOW_ROOT_ENV}=1 to override this check.\n"
    );
    let mut err = std::io::stderr();
    let _ = err.write_all(msg.as_bytes());
    let _ = err.flush();
}

/// Convenience: run the guard and, if blocked, warn and return the process
/// exit code. Call this at the top of `main`.
pub fn guard_or_exit_code() -> Option<i32> {
    match check() {
        Ok(()) => None,
        Err(_) => {
            warn_root();
            Some(ROOT_BLOCKED_EXIT)
        }
    }
}
