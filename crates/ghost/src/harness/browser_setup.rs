use std::fs::{File, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use fs2::FileExt;

const AGENT_BROWSER_VERSION: &str = "0.35.0";

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

    let agent_browser = agent_browser.canonicalize().unwrap_or(agent_browser);
    let lightpanda = lightpanda.canonicalize().unwrap_or(lightpanda);
    std::env::set_var("TACHYON_AGENT_BROWSER_BIN", &agent_browser);
    std::env::set_var("TACHYON_LIGHTPANDA_BIN", &lightpanda);
    std::env::set_var("AGENT_BROWSER_ENGINE", "lightpanda");
    std::env::set_var("AGENT_BROWSER_EXECUTABLE_PATH", &lightpanda);
    if std::env::var_os("AGENT_BROWSER_MAX_OUTPUT").is_none() {
        std::env::set_var("AGENT_BROWSER_MAX_OUTPUT", "30000");
    }
    if !verify_lightpanda_launch(&agent_browser) {
        return Err(format!(
            "agent-browser at {} could not launch Lightpanda at {}",
            agent_browser.display(),
            lightpanda.display()
        ));
    }
    eprintln!(
        "ghost: agent-browser ready with Lightpanda at {}",
        lightpanda.display()
    );
    Ok(())
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
    let session = "tachyon-lightpanda-preflight";
    let _ = command_succeeds(agent_browser, &["--session", session, "close"]);
    let opened = command_succeeds(
        agent_browser,
        &["--session", session, "open", "about:blank"],
    );
    let closed = command_succeeds(agent_browser, &["--session", session, "close"]);
    opened && closed
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
