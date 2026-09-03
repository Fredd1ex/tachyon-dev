use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use fs2::FileExt;
use serde::{Deserialize, Serialize};

const AGENT_BROWSER_VERSION: &str = "0.35.0";
const BROWSER_SETUP_STAMP_VERSION: u32 = 1;
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

pub fn ensure() -> Result<(), String> {
    let tools_root = std::env::var_os("TACHYON_HARNESS_TOOLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| tachyon_util::daemon::data_dir().join("tools"));
    std::fs::create_dir_all(&tools_root).map_err(|error| {
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
    lock.lock_exclusive()
        .map_err(|error| format!("failed to lock browser setup: {error}"))?;

    let result = ensure_locked(&tools_root);
    let _ = FileExt::unlock(&lock);
    result
}

fn ensure_locked(tools_root: &Path) -> Result<(), String> {
    let stamp_path = tools_root.join(BROWSER_SETUP_STAMP);
    let agent_browser_root = tools_root.join("agent-browser");
    let managed_agent_browser = agent_browser_root.join("bin/agent-browser");
    let configured_agent_browser = std::env::var("TACHYON_AGENT_BROWSER_BIN").ok();
    let mut agent_browser = configured_agent_browser
        .as_deref()
        .and_then(executable_on_path)
        .or_else(|| executable_on_path("agent-browser"))
        .or_else(|| {
            managed_agent_browser
                .is_file()
                .then(|| managed_agent_browser.clone())
        });
    let lightpanda_root = tools_root.join("lightpanda");
    let managed_lightpanda = lightpanda_root.join("lightpanda");
    let configured_lightpanda = std::env::var("TACHYON_LIGHTPANDA_BIN").ok();
    let mut lightpanda = configured_lightpanda
        .as_deref()
        .and_then(executable_on_path)
        .or_else(|| executable_on_path("lightpanda"))
        .or_else(|| {
            managed_lightpanda
                .is_file()
                .then(|| managed_lightpanda.clone())
        });

    if let (Some(agent_browser), Some(lightpanda)) = (&agent_browser, &lightpanda) {
        let agent_browser = canonicalize_or_original(agent_browser.clone());
        let lightpanda = canonicalize_or_original(lightpanda.clone());
        configure_browser_environment(&agent_browser, &lightpanda);
        if verification_stamp(&agent_browser, &lightpanda)
            .is_ok_and(|expected| verified_stamp_matches(&stamp_path, &expected))
        {
            eprintln!(
                "ghost: agent-browser ready with Lightpanda at {}",
                lightpanda.display()
            );
            return Ok(());
        }
    }

    if !agent_browser
        .as_deref()
        .is_some_and(supports_required_agent_browser)
    {
        eprintln!("ghost: agent-browser is unavailable; installing it");
        agent_browser = Some(install_agent_browser(&agent_browser_root)?);
    }
    let agent_browser = agent_browser.expect("agent-browser exists after successful installation");
    if !supports_required_agent_browser(&agent_browser) {
        return Err(format!(
            "agent-browser at {} does not provide the required {AGENT_BROWSER_VERSION} command contract",
            agent_browser.display(),
        ));
    }

    if !lightpanda
        .as_deref()
        .is_some_and(|program| command_succeeds(program, &["version"]))
    {
        eprintln!("ghost: Lightpanda is unavailable; installing it");
        lightpanda = Some(install_lightpanda(&lightpanda_root)?);
    }
    let lightpanda = lightpanda.expect("Lightpanda exists after successful installation");
    if !command_succeeds(&lightpanda, &["version"]) {
        return Err(format!(
            "Lightpanda at {} failed its version check",
            lightpanda.display()
        ));
    }

    let agent_browser = canonicalize_or_original(agent_browser);
    let lightpanda = canonicalize_or_original(lightpanda);
    configure_browser_environment(&agent_browser, &lightpanda);
    verify_and_cache_browser(&stamp_path, &agent_browser, &lightpanda)?;
    eprintln!(
        "ghost: agent-browser ready with Lightpanda at {}",
        lightpanda.display()
    );
    Ok(())
}

fn verify_and_cache_browser(
    stamp_path: &Path,
    agent_browser: &Path,
    lightpanda: &Path,
) -> Result<(), String> {
    if !verify_lightpanda_launch(agent_browser) {
        return Err(format!(
            "agent-browser at {} could not launch Lightpanda at {}",
            agent_browser.display(),
            lightpanda.display()
        ));
    }
    if let Err(error) = verification_stamp(agent_browser, lightpanda)
        .and_then(|stamp| write_verified_stamp(stamp_path, &stamp))
    {
        eprintln!("ghost: could not cache browser preflight: {error}");
    }
    Ok(())
}

fn canonicalize_or_original(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn configure_browser_environment(agent_browser: &Path, lightpanda: &Path) {
    std::env::set_var("TACHYON_AGENT_BROWSER_BIN", agent_browser);
    std::env::set_var("TACHYON_LIGHTPANDA_BIN", lightpanda);
    std::env::set_var("AGENT_BROWSER_ENGINE", AGENT_BROWSER_ENGINE);
    std::env::set_var("AGENT_BROWSER_EXECUTABLE_PATH", lightpanda);
    if std::env::var_os("AGENT_BROWSER_MAX_OUTPUT").is_none() {
        std::env::set_var("AGENT_BROWSER_MAX_OUTPUT", DEFAULT_MAX_OUTPUT);
    }
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
    })
}

fn executable_stamp(path: &Path) -> Result<ExecutableStamp, String> {
    let path = path
        .canonicalize()
        .map_err(|error| format!("failed to resolve {}: {error}", path.display()))?;
    let metadata = path
        .metadata()
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{} is not a file", path.display()));
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

fn executable_on_path(program: &str) -> Option<PathBuf> {
    let path = Path::new(program);
    if path.components().count() > 1 {
        return path.is_file().then(|| path.to_path_buf());
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
}

fn command_succeeds(program: &Path, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn supports_required_agent_browser(program: &Path) -> bool {
    let Ok(output) = Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let version = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    version.split_whitespace().any(|part| {
        let numeric =
            part.trim_matches(|character: char| !character.is_ascii_digit() && character != '.');
        let mut components = numeric
            .split('.')
            .filter_map(|value| value.parse::<u64>().ok());
        match (components.next(), components.next()) {
            (Some(major), Some(minor)) => major > 0 || minor >= 35,
            _ => false,
        }
    })
}

fn install_agent_browser(install_root: &Path) -> Result<PathBuf, String> {
    let browser = install_root.join("bin/agent-browser");
    download_executable(&agent_browser_download_url()?, &browser, "agent-browser")?;
    Ok(browser)
}

fn install_lightpanda(install_root: &Path) -> Result<PathBuf, String> {
    let browser = install_root.join("lightpanda");
    download_executable(lightpanda_download_url()?, &browser, "Lightpanda")?;
    Ok(browser)
}

fn download_executable(url: &str, destination: &Path, name: &str) -> Result<(), String> {
    let parent = destination
        .parent()
        .ok_or_else(|| format!("invalid {name} destination {}", destination.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("failed to create {name} directory: {error}"))?;
    let download = destination.with_extension("download");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|error| format!("failed to create {name} downloader: {error}"))?;
    let mut response = client
        .get(url)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| format!("failed to download {name}: {error}"))?;
    let mut file = File::create(&download)
        .map_err(|error| format!("failed to create {name} download: {error}"))?;
    response
        .copy_to(&mut file)
        .map_err(|error| format!("failed to save {name} download: {error}"))?;
    let mut permissions = file
        .metadata()
        .map_err(|error| format!("failed to inspect {name} download: {error}"))?
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&download, permissions)
        .map_err(|error| format!("failed to make {name} executable: {error}"))?;
    std::fs::rename(&download, destination)
        .map_err(|error| format!("failed to activate {name}: {error}"))?;
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

fn lightpanda_download_url() -> Result<&'static str, String> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok(
            "https://github.com/lightpanda-io/browser/releases/download/nightly/lightpanda-x86_64-linux",
        ),
        ("macos", "aarch64") => Ok(
            "https://github.com/lightpanda-io/browser/releases/download/nightly/lightpanda-aarch64-macos",
        ),
        (os, arch) => Err(format!(
            "automatic Lightpanda installation is unsupported on {os}/{arch}"
        )),
    }
}

fn verify_lightpanda_launch(agent_browser: &Path) -> bool {
    for attempt in 0..3 {
        let session = format!(
            "tachyon-lightpanda-preflight-{}-{attempt}",
            std::process::id()
        );
        let _ = command_succeeds(agent_browser, &["--session", &session, "close"]);
        let opened = command_succeeds(
            agent_browser,
            &["--session", &session, "open", "about:blank"],
        );
        let closed = command_succeeds(agent_browser, &["--session", &session, "close"]);
        if opened && closed {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

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
        }
    }

    #[test]
    fn explicit_existing_executable_is_resolved() {
        let path =
            std::env::temp_dir().join(format!("tachyon-browser-setup-test-{}", std::process::id()));
        std::fs::write(&path, b"test").unwrap();
        assert_eq!(
            executable_on_path(path.to_str().unwrap()),
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
            assert!(lightpanda_download_url()
                .unwrap()
                .starts_with("https://github.com/lightpanda-io/browser/releases/"));
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

        assert!(verify_and_cache_browser(&stamp_path, &agent_browser, &lightpanda).is_err());
        assert!(!stamp_path.exists());
    }
}
