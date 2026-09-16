use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const AGENT_BROWSER_VERSION: &str = "0.35.0";
const BROWSER_SETUP_STAMP_VERSION: u32 = 3;
const BROWSER_SETUP_STAMP: &str = "browser-setup.verified.json";
const AGENT_BROWSER_ENGINE: &str = "lightpanda";
const DEFAULT_MAX_OUTPUT: &str = "30000";

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct BrowserSetupStamp {
    format_version: u32,
    agent_browser_contract: String,
    engine: String,
    max_output: Vec<u8>,
    agent_browser: ExecutableStamp,
    lightpanda: ExecutableStamp,
    managed_lightpanda_digest: Option<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExecutableStamp {
    path: Vec<u8>,
    device: u64,
    inode: u64,
    len: u64,
    mode: u32,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

pub fn ensure() -> Result<(PathBuf, PathBuf), String> {
    let tools_root = std::env::var_os("TACHYON_HARNESS_TOOLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| tachyon_util::daemon::data_dir().join("tools"));
    ensure_at(
        &tools_root,
        std::env::var_os("TACHYON_AGENT_BROWSER_BIN").as_deref(),
        std::env::var_os("TACHYON_LIGHTPANDA_BIN").as_deref(),
        lightpanda_download(),
    )
}

fn ensure_at(
    tools_root: &Path,
    agent: Option<&std::ffi::OsStr>,
    light: Option<&std::ffi::OsStr>,
    download: (Result<&str, String>, &str),
) -> Result<(PathBuf, PathBuf), String> {
    std::fs::create_dir_all(tools_root).map_err(|error| {
        format!(
            "failed to create harness tool directory {}: {error}",
            tools_root.display()
        )
    })?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(tools_root.join("browser-setup.lock"))
        .map_err(|error| format!("failed to open browser setup lock: {error}"))?;
    lock_setup(&lock, Instant::now() + Duration::from_secs(20))?;

    let result = ensure_locked(tools_root, agent, light, download);
    let _ = FileExt::unlock(&lock);
    result
}

fn lock_setup(lock: &File, deadline: Instant) -> Result<(), String> {
    loop {
        match lock.try_lock_exclusive() {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(format!("failed to lock browser setup: {error}")),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("browser setup lock timed out".into());
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

fn ensure_locked(
    tools_root: &Path,
    configured_agent_browser: Option<&std::ffi::OsStr>,
    configured_lightpanda: Option<&std::ffi::OsStr>,
    download: (Result<&str, String>, &str),
) -> Result<(PathBuf, PathBuf), String> {
    let stamp_path = tools_root.join(BROWSER_SETUP_STAMP);
    let agent_browser_root = tools_root.join("agent-browser");
    let managed_agent_browser = agent_browser_root.join("bin/agent-browser");
    let mut agent_browser =
        resolve_browser_binary(configured_agent_browser, &managed_agent_browser)?;
    let lightpanda_root = tools_root.join("lightpanda");
    let managed_lightpanda = lightpanda_root.join("lightpanda");
    let mut lightpanda = resolve_browser_binary(configured_lightpanda, &managed_lightpanda)?;
    let digest = configured_lightpanda.is_none().then_some(download.1);
    if let (Some(agent), Some(light)) = (&agent_browser, &lightpanda) {
        let mut expected = verification_stamp(agent, light)?;
        expected.managed_lightpanda_digest = digest.map(str::to_owned);
        if verified_stamp_matches(&stamp_path, &expected) {
            return Ok((
                canonicalize_or_original(agent.clone()),
                canonicalize_or_original(light.clone()),
            ));
        }
    }
    if configured_lightpanda.is_none()
        && lightpanda
            .as_deref()
            .is_some_and(|path| verify_digest(path, download.1).is_err())
    {
        lightpanda = None;
    }
    let lightpanda_valid = lightpanda.as_deref().is_some_and(valid_lightpanda);
    if configured_lightpanda.is_some() && !lightpanda_valid {
        return Err("requested Lightpanda override failed its version check".into());
    }

    if !agent_browser
        .as_deref()
        .is_some_and(supports_required_agent_browser)
    {
        if configured_agent_browser.is_some() {
            return Err(format!(
                "requested agent-browser override must report version {AGENT_BROWSER_VERSION}"
            ));
        }
        eprintln!("ghost: agent-browser is unavailable; installing it");
        agent_browser = Some(install_agent_browser(&agent_browser_root)?);
    }
    let agent_browser = agent_browser.expect("agent-browser exists after successful installation");

    if !lightpanda_valid {
        if configured_lightpanda.is_some() {
            return Err("requested Lightpanda override failed its version check".into());
        }
        eprintln!("ghost: Lightpanda is unavailable; installing it");
        lightpanda = Some(install_lightpanda_from(
            &lightpanda_root,
            download.0?,
            download.1,
        )?);
    }
    let lightpanda = lightpanda.expect("Lightpanda exists after successful installation");

    let agent_browser = canonicalize_or_original(agent_browser);
    let lightpanda = canonicalize_or_original(lightpanda);
    let mut stamp = verification_stamp(&agent_browser, &lightpanda)?;
    stamp.managed_lightpanda_digest = digest.map(str::to_owned);
    write_verified_stamp(&stamp_path, &stamp)?;
    eprintln!(
        "ghost: agent-browser ready with Lightpanda at {}",
        lightpanda.display()
    );
    Ok((agent_browser, lightpanda))
}

fn canonicalize_or_original(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn verification_stamp(
    agent_browser: &Path,
    lightpanda: &Path,
) -> Result<BrowserSetupStamp, String> {
    Ok(BrowserSetupStamp {
        format_version: BROWSER_SETUP_STAMP_VERSION,
        agent_browser_contract: AGENT_BROWSER_VERSION.to_owned(),
        engine: AGENT_BROWSER_ENGINE.to_owned(),
        max_output: std::env::var_os("AGENT_BROWSER_MAX_OUTPUT")
            .unwrap_or_else(|| DEFAULT_MAX_OUTPUT.into())
            .as_bytes()
            .to_vec(),
        agent_browser: executable_stamp(agent_browser)?,
        lightpanda: executable_stamp(lightpanda)?,
        managed_lightpanda_digest: None,
    })
}

fn executable_stamp(path: &Path) -> Result<ExecutableStamp, String> {
    let path = path
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", path.display()))?;
    let metadata = path
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
        return Err(format!("{} is not an executable file", path.display()));
    }
    Ok(ExecutableStamp {
        path: path.as_os_str().as_bytes().to_vec(),
        device: metadata.dev(),
        inode: metadata.ino(),
        len: metadata.len(),
        mode: metadata.mode(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn verified_stamp_matches(path: &Path, expected: &BrowserSetupStamp) -> bool {
    std::fs::read(path)
        .ok()
        .and_then(|contents| serde_json::from_slice::<BrowserSetupStamp>(&contents).ok())
        .is_some_and(|stamp| stamp == *expected)
}

fn write_verified_stamp(path: &Path, stamp: &BrowserSetupStamp) -> Result<(), String> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(&temporary);
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| format!("failed to create {}: {error}", temporary.display()))?;
        serde_json::to_writer(&mut file, stamp)
            .map_err(|error| format!("failed to serialize browser setup stamp: {error}"))?;
        file.write_all(b"\n")
            .map_err(|error| format!("failed to write browser setup stamp: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("failed to sync browser setup stamp: {error}"))?;
        std::fs::rename(&temporary, path)
            .map_err(|error| format!("failed to activate browser setup stamp: {error}"))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn resolve_browser_binary(
    configured: Option<&std::ffi::OsStr>,
    managed: &Path,
) -> Result<Option<PathBuf>, String> {
    if let Some(configured) = configured {
        let path = Path::new(configured);
        if !path.is_absolute() {
            return Err("browser binary overrides must be absolute executable paths; PATH fallback is disabled".into());
        }
        executable_stamp(path)?;
        return path
            .canonicalize()
            .map(Some)
            .map_err(|error| error.to_string());
    }
    Ok(executable_stamp(managed)
        .ok()
        .map(|_| managed.to_path_buf()))
}

fn version_output(program: &Path, args: &[&str], timeout: Duration) -> Option<String> {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};
    use std::os::fd::{AsRawFd, OwnedFd};
    use std::os::unix::process::CommandExt;
    let deadline = Instant::now() + timeout;
    let mut command = Command::new(program);
    command
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = loop {
        match command.spawn() {
            Ok(child) => break child,
            // A concurrent fork can retain the staging writer until exec, even
            // after our descriptor closes. No program ran on ETXTBSY.
            Err(error)
                if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32)
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => return None,
        }
    };
    let capture = |mut pipe: File| {
        std::thread::spawn(move || {
            let flags = fcntl(pipe.as_raw_fd(), FcntlArg::F_GETFL).ok()?;
            fcntl(
                pipe.as_raw_fd(),
                FcntlArg::F_SETFL(OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK),
            )
            .ok()?;
            let mut output = Vec::new();
            let mut buffer = [0; 1024];
            // Escaped descendants may retain a writer after group cleanup.
            // Check the deadline even while output arrives continuously.
            while Instant::now() < deadline {
                match pipe.read(&mut buffer) {
                    Ok(0) => return Some(output),
                    Ok(count) => {
                        let keep = count.min(4096usize.saturating_sub(output.len()));
                        output.extend_from_slice(&buffer[..keep]);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => return None,
                }
            }
            None
        })
    };
    let stdout = capture(File::from(OwnedFd::from(child.stdout.take()?)));
    let stderr = capture(File::from(OwnedFd::from(child.stderr.take()?)));
    let success = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Err(_) => break false,
            _ if Instant::now() >= deadline => break false,
            _ => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGKILL,
    );
    let _ = child.wait();
    // Join both before inspecting either result, including timeout/error paths.
    let stdout = stdout.join();
    let stderr = stderr.join();
    let mut output = stdout.ok()??;
    output.extend(stderr.ok()??);
    success.then(|| String::from_utf8_lossy(&output).into_owned())
}

fn supports_required_agent_browser(program: &Path) -> bool {
    version_output(program, &["--version"], Duration::from_secs(5))
        .is_some_and(|version| version.trim() == format!("agent-browser {AGENT_BROWSER_VERSION}"))
}

fn valid_lightpanda(program: &Path) -> bool {
    version_output(program, &["version"], Duration::from_secs(5)).is_some_and(|version| {
        // Lightpanda prints build_config.version as one line, without a product prefix.
        let version = version.trim();
        !version.is_empty()
            && version.len() <= 128
            && version.chars().any(|c| c.is_ascii_digit())
            && version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'))
    })
}

fn install_agent_browser(install_root: &Path) -> Result<PathBuf, String> {
    let browser = install_root.join("bin/agent-browser");
    download_executable(
        &agent_browser_download_url()?,
        &browser,
        "agent-browser",
        None,
        supports_required_agent_browser,
    )?;
    Ok(browser)
}

fn install_lightpanda_from(
    install_root: &Path,
    url: &str,
    digest: &str,
) -> Result<PathBuf, String> {
    let browser = install_root.join("lightpanda");
    download_executable(url, &browser, "Lightpanda", Some(digest), valid_lightpanda)?;
    Ok(browser)
}

fn download_executable(
    url: &str,
    destination: &Path,
    name: &str,
    digest: Option<&str>,
    validate: fn(&Path) -> bool,
) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| format!("failed to create {name} downloader: {error}"))?;
    let response = client
        .get(url)
        .header("Accept", "application/octet-stream")
        .header("User-Agent", "tachyon-browser-setup")
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| format!("failed to download {name}: {error}"))?;
    stage_executable(response, destination, name, digest, validate)
}

fn stage_executable(
    response: impl Read,
    destination: &Path,
    name: &str,
    digest: Option<&str>,
    validate: fn(&Path) -> bool,
) -> Result<(), String> {
    let parent = destination
        .parent()
        .ok_or_else(|| format!("invalid {name} destination {}", destination.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("failed to create {name} directory: {error}"))?;
    let download = destination.with_extension("download");
    // The setup lock owns this staging name; a previous interrupted download is disposable.
    if download.symlink_metadata().is_ok() {
        std::fs::remove_file(&download)
            .map_err(|error| format!("failed to remove stale download: {error}"))?;
    }
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&download)
            .map_err(|error| format!("failed to create {name} download: {error}"))?;
        let count = std::io::copy(&mut response.take(256 * 1024 * 1024 + 1), &mut file)
            .map_err(|error| format!("failed to save {name} download: {error}"))?;
        if count == 0 || count > 256 * 1024 * 1024 {
            return Err(format!("{name} download has invalid size"));
        }
        file.sync_all().map_err(|error| error.to_string())?;
        if let Some(expected) = digest {
            verify_digest(&download, expected)?;
        }
        let mut permissions = file
            .metadata()
            .map_err(|error| format!("failed to inspect {name} download: {error}"))?
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&download, permissions)
            .map_err(|error| format!("failed to make {name} executable: {error}"))?;
        drop(file);
        if !validate(&download) {
            return Err(format!("staged {name} failed its version check"));
        }
        std::fs::rename(&download, destination)
            .map_err(|error| format!("failed to activate {name}: {error}"))?;
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&download);
    }
    result
}

fn verify_digest(path: &Path, expected: &str) -> Result<(), String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut hash = Sha256::new();
    std::io::copy(&mut file, &mut hash).map_err(|error| error.to_string())?;
    if format!("{:x}", hash.finalize()) != expected {
        return Err("Lightpanda SHA-256 mismatch; refusing to execute or activate download".into());
    }
    Ok(())
}

fn agent_browser_download_url() -> Result<String, String> {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") if cfg!(target_env = "musl") => "linux-musl-x64",
        ("linux", "aarch64") if cfg!(target_env = "musl") => "linux-musl-arm64",
        ("linux", "x86_64") => "linux-x64",
        ("linux", "aarch64") => "linux-arm64",
        ("macos", "x86_64") => "darwin-x64",
        ("macos", "aarch64") => "darwin-arm64",
        (os, arch) => {
            return Err(format!(
                "automatic agent-browser installation is unsupported on {os}/{arch}"
            ))
        }
    };
    Ok(format!(
        "https://github.com/vercel-labs/agent-browser/releases/download/v{AGENT_BROWSER_VERSION}/agent-browser-{platform}"
    ))
}

fn lightpanda_download() -> (Result<&'static str, String>, &'static str) {
    // GitHub asset IDs and published SHA-256 digests, inspected 2026-09-09.
    // Never follow the mutable nightly tag to a replacement executable.
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => (
            Ok("https://api.github.com/repos/lightpanda-io/browser/releases/assets/551831859"),
            "50533da8fb42505479cec086291695949c67169ff8840deb259a6bd253b6169b",
        ),
        ("macos", "aarch64") => (
            Ok("https://api.github.com/repos/lightpanda-io/browser/releases/assets/551818190"),
            "f0ae8b4b8ed671a17a14d4f4e13addb1434873f49648f3385dfb81410aa4d32a",
        ),
        (os, arch) => (
            Err(format!(
                "automatic Lightpanda installation is unsupported on {os}/{arch}"
            )),
            "",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_lightpanda_download_checks_hash_before_publish_or_execution() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("lightpanda/lightpanda");
        let agent = dir.path().join("agent");
        executable(&agent, b"#!/bin/sh\nprintf 'agent-browser 0.35.0\\n'\n");
        let good = b"#!/bin/sh\nprintf '0.2.0-fixture\\n'\n";
        let stale = format!(
            "#!/bin/sh\ntouch '{}'\nprintf '0.2.0-stale\\n'\n",
            dir.path().join("stale-executed").display()
        );
        let digest = format!("{:x}", Sha256::digest(good));
        for (body, succeeds) in [
            (&good[..], true),
            (&b"#!/bin/sh\nexit 99\n"[..], false),
            (&good[..], true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/fixture", listener.local_addr().unwrap());
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        socket.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    socket.write_all(body).unwrap();
                });
                // The fixture serves exactly one download, even for concurrent cold use/repair.
                for _ in 0..if succeeds { 8 } else { 1 } {
                    let url = &url;
                    let digest = &digest;
                    let dir = &dir;
                    let agent = &agent;
                    scope.spawn(move || {
                        let result =
                            ensure_at(dir.path(), Some(agent.as_os_str()), None, (Ok(url), digest));
                        assert_eq!(result.is_ok(), succeeds, "{result:?}");
                        if !succeeds {
                            assert!(result.unwrap_err().contains("SHA-256 mismatch"));
                        }
                    });
                }
            });
            assert_eq!(
                std::fs::read(&destination).unwrap(),
                if succeeds {
                    &good[..]
                } else {
                    stale.as_bytes()
                }
            );
            assert!(!destination.with_extension("download").exists());
            assert!(!dir.path().join("stale-executed").exists());
            if succeeds {
                // No server remains: an unchanged managed installation must hit the cache.
                ensure_at(
                    dir.path(),
                    Some(agent.as_os_str()),
                    None,
                    (Err("must not download on cache hit".into()), &digest),
                )
                .unwrap();
                executable(&destination, stale.as_bytes());
            }
        }
    }

    #[test]
    fn browser_concurrent_first_use_caches_probes_and_rejects_stale_install() {
        let dir = tempfile::tempdir().unwrap();
        let agent = dir.path().join("agent");
        let light = dir.path().join("light");
        let probes = dir.path().join("probes");
        executable(
            &agent,
            format!(
                "#!/bin/sh\necho agent >> '{}'\nprintf 'agent-browser 0.35.0\\n'\n",
                probes.display()
            )
            .as_bytes(),
        );
        executable(
            &light,
            format!(
                "#!/bin/sh\necho light >> '{}'\nprintf '0.2.0-fixture\\n'\n",
                probes.display()
            )
            .as_bytes(),
        );
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    ensure_at(
                        dir.path(),
                        Some(agent.as_os_str()),
                        Some(light.as_os_str()),
                        lightpanda_download(),
                    )
                    .unwrap();
                });
            }
        });
        let calls = std::fs::read_to_string(&probes).unwrap();
        assert_eq!(calls.lines().count(), 2, "{calls}");
        executable(&agent, b"#!/bin/sh\nprintf 'agent-browser 0.36.0\\n'\n");
        assert!(ensure_at(
            dir.path(),
            Some(agent.as_os_str()),
            Some(light.as_os_str()),
            lightpanda_download()
        )
        .unwrap_err()
        .contains("override"));
        let expected = verification_stamp(&agent, &light).unwrap();
        assert!(!verified_stamp_matches(
            &dir.path().join(BROWSER_SETUP_STAMP),
            &expected
        ));
        // A stamp for trusted overrides must never authorize a managed binary.
        write_verified_stamp(&dir.path().join(BROWSER_SETUP_STAMP), &expected).unwrap();
        let mut managed = expected;
        managed.managed_lightpanda_digest = Some(lightpanda_download().1.into());
        assert!(!verified_stamp_matches(
            &dir.path().join(BROWSER_SETUP_STAMP),
            &managed
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn browser_version_deadline_covers_escaped_descendant_pipes() {
        struct EscapedChild(PathBuf);
        impl Drop for EscapedChild {
            fn drop(&mut self) {
                if let Some(pid) = std::fs::read_to_string(&self.0)
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok())
                {
                    let _ = nix::sys::signal::killpg(
                        nix::unistd::Pid::from_raw(pid),
                        nix::sys::signal::Signal::SIGKILL,
                    );
                }
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("browser");
        let pid_path = dir.path().join("escaped.pid");
        let _cleanup = EscapedChild(pid_path.clone());
        executable(
            &binary,
            br#"#!/bin/sh
setsid sh -c 'echo $$ > "$1"; exec sleep 5' sh "$1" &
while [ ! -s "$1" ]; do sleep 0.01; done
printf 'agent-browser 0.35.0\n'
"#,
        );
        let start = Instant::now();
        let result = version_output(
            &binary,
            &[pid_path.to_str().unwrap()],
            Duration::from_millis(200),
        );
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(result.is_none(), "unterminated capture must fail closed");
        let pid = std::fs::read_to_string(&pid_path)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_eq!(
            nix::unistd::getsid(Some(nix::unistd::Pid::from_raw(pid)))
                .unwrap()
                .as_raw(),
            pid,
            "fixture must escape into a new session and remain alive through the probe"
        );
    }

    #[test]
    fn browser_version_retries_busy_executable_within_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("browser");
        executable(&binary, b"#!/bin/sh\nprintf 'agent-browser 0.35.0\\n'\n");
        let writer = OpenOptions::new().write(true).open(&binary).unwrap();
        assert!(version_output(&binary, &[], Duration::from_millis(30)).is_none());
        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                drop(writer);
            });
            assert!(supports_required_agent_browser(&binary));
        });
    }

    #[test]
    fn staged_browser_recovers_partial_download_and_preserves_previous_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("agent-browser");
        let good = b"#!/bin/sh\nprintf 'agent-browser 0.35.0\\n'\n";
        std::fs::write(destination.with_extension("download"), b"partial").unwrap();
        stage_executable(
            &good[..],
            &destination,
            "agent-browser",
            None,
            supports_required_agent_browser,
        )
        .unwrap();
        assert!(supports_required_agent_browser(&destination));
        assert!(!destination.with_extension("download").exists());
        assert!(stage_executable(
            &b"#!/bin/sh\nexit 1\n"[..],
            &destination,
            "agent-browser",
            None,
            supports_required_agent_browser
        )
        .is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), good);
        let digest = format!("{:x}", Sha256::digest(good));
        stage_executable(
            &good[..],
            &destination,
            "fixture",
            Some(&digest),
            supports_required_agent_browser,
        )
        .unwrap();
        assert!(supports_required_agent_browser(&destination));
        assert!(!destination.with_extension("download").exists());
        assert!(stage_executable(
            &good[..],
            &destination,
            "Lightpanda",
            Some("wrong"),
            |_| panic!("must hash before execution")
        )
        .is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), good);
    }

    #[test]
    fn browser_overrides_fail_closed_and_never_search_path() {
        let dir = tempfile::tempdir().unwrap();
        let managed = dir.path().join("managed");
        executable(&managed, b"#!/bin/sh\nexit 0\n");
        assert!(
            resolve_browser_binary(Some(std::ffi::OsStr::new("agent-browser")), &managed).is_err()
        );
        assert!(
            resolve_browser_binary(Some(dir.path().join("missing").as_os_str()), &managed).is_err()
        );
        assert_eq!(
            resolve_browser_binary(None, &dir.path().join("absent")).unwrap(),
            None
        );
        std::fs::set_permissions(&managed, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(resolve_browser_binary(Some(managed.as_os_str()), &managed).is_err());
    }

    #[test]
    fn browser_version_checks_are_exact_bounded_and_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("fake-browser");
        executable(&binary, b"#!/bin/sh\nprintf 'agent-browser 0.36.0\\n'\n");
        assert!(!supports_required_agent_browser(&binary));
        executable(&binary, b"#!/bin/sh\nprintf '0.2.0-dev+123abc\\n'\n");
        assert!(valid_lightpanda(&binary));
        executable(&binary, b"#!/bin/sh\nprintf 'Chrome 123\\n'\n");
        assert!(!valid_lightpanda(&binary));
        executable(
            &binary,
            b"#!/bin/sh\ni=0; while [ $i -lt 10000 ]; do printf x; i=$((i+1)); done\n",
        );
        assert_eq!(
            version_output(&binary, &[], Duration::from_secs(2))
                .unwrap()
                .len(),
            4096
        );
        executable(&binary, b"#!/bin/sh\nsleep 30\n");
        let start = Instant::now();
        assert!(version_output(&binary, &[], Duration::from_millis(50)).is_none());
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn browser_setup_lock_is_exclusive_and_recovers_after_owner_exits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let first = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .unwrap();
        let second = OpenOptions::new().write(true).open(&path).unwrap();
        first.lock_exclusive().unwrap();
        assert!(second.try_lock_exclusive().is_err());
        let started = Instant::now();
        assert!(lock_setup(&second, started + Duration::from_millis(30))
            .unwrap_err()
            .contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(first);
        lock_setup(&second, Instant::now() + Duration::from_secs(1)).unwrap();
    }

    fn executable(path: &Path, contents: &[u8]) {
        std::fs::write(path, contents).unwrap();
        let mut permissions = path.metadata().unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    fn stamp(agent_browser: &Path, lightpanda: &Path) -> BrowserSetupStamp {
        BrowserSetupStamp {
            format_version: BROWSER_SETUP_STAMP_VERSION,
            agent_browser_contract: AGENT_BROWSER_VERSION.to_owned(),
            engine: AGENT_BROWSER_ENGINE.to_owned(),
            max_output: DEFAULT_MAX_OUTPUT.as_bytes().to_vec(),
            agent_browser: executable_stamp(agent_browser).unwrap(),
            lightpanda: executable_stamp(lightpanda).unwrap(),
            managed_lightpanda_digest: None,
        }
    }

    #[test]
    fn explicit_existing_executable_is_resolved() {
        let path =
            std::env::temp_dir().join(format!("tachyon-browser-setup-test-{}", std::process::id()));
        executable(&path, b"test");
        assert_eq!(
            resolve_browser_binary(Some(path.as_os_str()), Path::new("unused")).unwrap(),
            Some(path.clone())
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn supported_target_has_an_official_lightpanda_download() {
        if matches!(
            (std::env::consts::OS, std::env::consts::ARCH),
            ("linux", "x86_64") | ("macos", "aarch64")
        ) {
            assert!(lightpanda_download().0.unwrap().starts_with(
                "https://api.github.com/repos/lightpanda-io/browser/releases/assets/"
            ));
            assert_eq!(lightpanda_download().1.len(), 64);
        }
    }

    #[test]
    fn required_agent_browser_version_matches_prompt_contract() {
        assert_eq!(AGENT_BROWSER_VERSION, "0.35.0");
        assert!(agent_browser_download_url()
            .unwrap()
            .contains("/releases/download/v0.35.0/agent-browser-"));
    }

    #[test]
    fn verified_stamp_round_trips_and_malformed_stamp_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let agent_browser = directory.path().join("agent-browser");
        let lightpanda = directory.path().join("lightpanda");
        let stamp_path = directory.path().join(BROWSER_SETUP_STAMP);
        executable(&agent_browser, b"agent");
        executable(&lightpanda, b"lightpanda");
        let expected = stamp(&agent_browser, &lightpanda);

        write_verified_stamp(&stamp_path, &expected).unwrap();
        assert!(verified_stamp_matches(&stamp_path, &expected));

        std::fs::write(&stamp_path, b"not json").unwrap();
        assert!(!verified_stamp_matches(&stamp_path, &expected));
    }

    #[test]
    fn executable_metadata_change_invalidates_verified_stamp() {
        let directory = tempfile::tempdir().unwrap();
        let agent_browser = directory.path().join("agent-browser");
        let lightpanda = directory.path().join("lightpanda");
        let stamp_path = directory.path().join(BROWSER_SETUP_STAMP);
        executable(&agent_browser, b"agent");
        executable(&lightpanda, b"lightpanda");
        let original = stamp(&agent_browser, &lightpanda);
        write_verified_stamp(&stamp_path, &original).unwrap();

        executable(&lightpanda, b"changed-lightpanda");
        let changed = stamp(&agent_browser, &lightpanda);
        assert_ne!(original, changed);
        assert!(!verified_stamp_matches(&stamp_path, &changed));
    }

    #[test]
    fn version_and_configuration_changes_invalidate_verified_stamp() {
        let directory = tempfile::tempdir().unwrap();
        let agent_browser = directory.path().join("agent-browser");
        let other_agent_browser = directory.path().join("other-agent-browser");
        let lightpanda = directory.path().join("lightpanda");
        let stamp_path = directory.path().join(BROWSER_SETUP_STAMP);
        executable(&agent_browser, b"agent");
        executable(&other_agent_browser, b"agent");
        executable(&lightpanda, b"lightpanda");
        let original = stamp(&agent_browser, &lightpanda);
        write_verified_stamp(&stamp_path, &original).unwrap();

        let mut changed_version = stamp(&agent_browser, &lightpanda);
        changed_version.agent_browser_contract = "new-contract".to_owned();
        assert!(!verified_stamp_matches(&stamp_path, &changed_version));

        let mut changed_config = stamp(&agent_browser, &lightpanda);
        changed_config.max_output = b"1234".to_vec();
        assert!(!verified_stamp_matches(&stamp_path, &changed_config));

        let changed_path = stamp(&other_agent_browser, &lightpanda);
        assert!(!verified_stamp_matches(&stamp_path, &changed_path));
    }

    #[test]
    fn failed_preflight_does_not_create_verified_stamp() {
        let directory = tempfile::tempdir().unwrap();
        let agent_browser = directory.path().join("agent-browser");
        let lightpanda = directory.path().join("lightpanda");
        let stamp_path = directory.path().join(BROWSER_SETUP_STAMP);
        executable(&agent_browser, b"#!/bin/sh\nexit 1\n");
        executable(&lightpanda, b"lightpanda");

        assert!(ensure_at(
            directory.path(),
            Some(agent_browser.as_os_str()),
            Some(lightpanda.as_os_str()),
            lightpanda_download()
        )
        .is_err());
        assert!(!stamp_path.exists());
    }
}
