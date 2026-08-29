//! Provider credentials kept outside Tachyon's configuration files.

#[cfg(not(target_os = "linux"))]
use keyring::v1::Entry;
#[cfg(target_os = "linux")]
use keyring_core::Entry;

const SERVICE: &str = "tachyon";
const OPENROUTER_ACCOUNT: &str = "openrouter-api-key";

#[cfg(target_os = "linux")]
fn use_linux_kernel_keyring() -> Result<(), String> {
    use std::sync::OnceLock;

    static INITIALIZED: OnceLock<Result<(), String>> = OnceLock::new();
    INITIALIZED
        .get_or_init(|| {
            let store =
                linux_keyutils_keyring_store::Store::new().map_err(|error| error.to_string())?;
            keyring_core::set_default_store(store);
            Ok(())
        })
        .clone()
}

#[cfg(not(target_os = "linux"))]
fn use_linux_kernel_keyring() -> Result<(), String> {
    Ok(())
}

fn openrouter_entry() -> Result<Entry, String> {
    // Headless Linux sessions commonly lack a Secret Service. The kernel
    // keyring is available without a desktop session or systemd.
    use_linux_kernel_keyring()?;
    Entry::new(SERVICE, OPENROUTER_ACCOUNT).map_err(|error| error.to_string())
}

/// Read the OpenRouter key from the operating system credential store.
///
/// A missing credential and an unavailable store are intentionally both
/// treated as absent by callers; neither should make a daemon unusable when a
/// shell-provided key is available.
pub fn openrouter_key() -> Option<String> {
    openrouter_entry().ok()?.get_password().ok()
}

/// Store the OpenRouter key in the operating system credential store.
pub fn store_openrouter_key(key: &str) -> Result<(), String> {
    openrouter_entry()?
        .set_password(key)
        .map_err(|error| error.to_string())
}

/// Remove the OpenRouter key from the operating system credential store.
pub fn delete_openrouter_key() -> Result<(), String> {
    openrouter_entry()?
        .delete_credential()
        .map_err(|error| error.to_string())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn kernel_keyring_stores_and_removes_a_credential() {
        use_linux_kernel_keyring().unwrap();
        let account = format!("test-{}", std::process::id());
        let entry = Entry::new("tachyon-test", &account).unwrap();
        entry.set_password("test-credential").unwrap();
        assert_eq!(entry.get_password().unwrap(), "test-credential");
        entry.delete_credential().unwrap();
    }
}
