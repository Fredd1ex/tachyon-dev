//! Provider credentials kept outside Tachyon's configuration files.
//! Linux prefers persistent Secret Service storage, with an explicit volatile
//! kernel-keyring fallback. A volatile login overrides an older persistent key.

const SERVICE: &str = "tachyon";
const OPENROUTER_ACCOUNT: &str = "openrouter-api-key";

enum Operation<'a> {
    Read,
    Write(&'a str),
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreStatus {
    Persistent,
    Volatile,
}

#[cfg(target_os = "linux")]
fn backend_error(_: impl std::fmt::Debug) -> String {
    "Persistent credential store unavailable or locked: configure/unlock a Secret Service provider on the user session D-Bus with a persistent default collection, then retry `tachyon providers login`, or set OPENROUTER_API_KEY in the daemon environment.".into()
}

#[cfg(target_os = "linux")]
fn operate(
    service: &str,
    account: &str,
    operation: Operation<'_>,
) -> Result<Option<String>, String> {
    use secret_service::{blocking::SecretService, EncryptionType};
    use std::collections::HashMap;

    let ss = SecretService::connect(EncryptionType::Dh).map_err(backend_error)?;
    let collection = ss.get_default_collection().map_err(backend_error)?;
    // A misconfigured default alias must not turn login into session-only storage.
    match ss.get_collection_by_alias("session") {
        Ok(session) if session.collection_path == collection.collection_path => {
            return Err(backend_error(()));
        }
        Ok(_) | Err(secret_service::Error::NoResult) => {}
        Err(error) => return Err(backend_error(error)),
    }
    collection.ensure_unlocked().map_err(backend_error)?;
    let attributes = HashMap::from([("service", service), ("username", account)]);
    let items = collection
        .search_items(attributes.clone())
        .map_err(backend_error)?;
    if items.len() > 1 {
        return Err("Persistent credential store contains ambiguous OpenRouter entries; resolve duplicates in your credential manager.".into());
    }
    let item = items.first();
    if let Some(item) = item {
        item.ensure_unlocked().map_err(backend_error)?;
    }
    match operation {
        Operation::Read => item
            .map(|item| {
                let bytes = item.get_secret().map_err(backend_error)?;
                String::from_utf8(bytes).map_err(|_| "Stored credential is not valid UTF-8.".into())
            })
            .transpose(),
        Operation::Write(key) => {
            if let Some(item) = item {
                item.set_secret(key.as_bytes(), "text/plain")
                    .map_err(backend_error)?;
            } else {
                collection
                    .create_item(
                        "Tachyon OpenRouter API key",
                        attributes,
                        key.as_bytes(),
                        true,
                        "text/plain",
                    )
                    .map_err(backend_error)?;
            }
            Ok(None)
        }
        Operation::Delete => {
            if let Some(item) = item {
                item.delete().map_err(backend_error)?;
            }
            Ok(None)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn operate(
    service: &str,
    account: &str,
    operation: Operation<'_>,
) -> Result<Option<String>, String> {
    use keyring::v1::{Entry, Error};
    let error =
        |_| "Operating system credential store unavailable, locked, or inaccessible.".to_string();
    let entry = Entry::new(service, account).map_err(error)?;
    match operation {
        Operation::Read => match entry.get_password() {
            Ok(key) => Ok(Some(key)),
            Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(error(e)),
        },
        Operation::Write(key) => entry.set_password(key).map(|()| None).map_err(error),
        Operation::Delete => match entry.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(error(e)),
        },
    }
}

#[cfg(target_os = "linux")]
fn volatile(operation: Operation<'_>) -> Result<Option<String>, String> {
    use keyring_core::{api::CredentialStoreApi, Error};
    let error = |_| {
        "Volatile kernel credential store inaccessible; check kernel keyring permissions/support or set OPENROUTER_API_KEY in the daemon environment.".to_string()
    };
    // Build directly: do not change the process-wide default credential store.
    let store = linux_keyutils_keyring_store::Store::new().map_err(error)?;
    let entry = store
        .build(SERVICE, OPENROUTER_ACCOUNT, None)
        .map_err(error)?;
    match operation {
        Operation::Read => match entry.get_password() {
            Ok(key) => Ok(Some(key)),
            Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(error(e)),
        },
        Operation::Write(key) => entry.set_password(key).map(|()| None).map_err(error),
        Operation::Delete => match entry.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(error(e)),
        },
    }
}

#[cfg(target_os = "linux")]
fn read_with(
    persistent: impl FnOnce() -> Result<Option<String>, String>,
    volatile: impl FnOnce() -> Result<Option<String>, String>,
) -> Result<Option<String>, String> {
    // Fail closed if the override cannot be read, rather than use an older key.
    match volatile()? {
        Some(key) => Ok(Some(key)),
        None => persistent().map_err(|error| {
            format!(
                "{error} No volatile credential found; run `tachyon providers login` to store one."
            )
        }),
    }
}

#[cfg(target_os = "linux")]
fn store_with<'a>(
    key: &'a str,
    persistent: impl FnOnce(Operation<'a>) -> Result<Option<String>, String>,
    volatile: impl FnOnce(Operation<'a>) -> Result<Option<String>, String>,
) -> Result<StoreStatus, String> {
    match persistent(Operation::Write(key)) {
        Ok(_) => {
            volatile(Operation::Delete).map_err(|error| format!("Credential saved persistently, but the previous volatile override could not be cleared and may still take precedence. {error} Retry login when both stores are accessible."))?;
            Ok(StoreStatus::Persistent)
        }
        Err(persistent_error) => {
            volatile(Operation::Write(key))
                .map_err(|error| format!("Login failed. {persistent_error} {error}"))?;
            Ok(StoreStatus::Volatile)
        }
    }
}

#[cfg(target_os = "linux")]
fn delete_with(
    persistent: impl FnOnce() -> Result<Option<String>, String>,
    volatile: impl FnOnce() -> Result<Option<String>, String>,
) -> Result<(), String> {
    let persistent = persistent();
    let volatile = volatile();
    let mut errors = Vec::new();
    if let Err(error) = persistent {
        errors.push(format!("Persistent removal failed: {error}"));
    }
    if let Err(error) = volatile {
        errors.push(format!("Volatile removal failed: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!("Logout incomplete; credentials may remain. {} Retry logout when both stores are accessible. OPENROUTER_API_KEY is unchanged; unset it separately in the daemon environment. Restart the daemon to clear its cached key.", errors.join(" ")))
    }
}

/// Distinguish a missing credential from an inaccessible backend.
pub fn openrouter_key() -> Result<Option<String>, String> {
    let persistent = || operate(SERVICE, OPENROUTER_ACCOUNT, Operation::Read);
    #[cfg(target_os = "linux")]
    {
        read_with(persistent, || volatile(Operation::Read))
    }
    #[cfg(not(target_os = "linux"))]
    {
        persistent()
    }
}

/// Return the actual storage lifetime; callers must warn for volatile storage.
pub fn store_openrouter_key(key: &str) -> Result<StoreStatus, String> {
    #[cfg(target_os = "linux")]
    {
        store_with(key, |op| operate(SERVICE, OPENROUTER_ACCOUNT, op), volatile)
    }
    #[cfg(not(target_os = "linux"))]
    {
        operate(SERVICE, OPENROUTER_ACCOUNT, Operation::Write(key)).map(|_| StoreStatus::Persistent)
    }
}

pub fn delete_openrouter_key() -> Result<(), String> {
    let persistent = || operate(SERVICE, OPENROUTER_ACCOUNT, Operation::Delete);
    #[cfg(target_os = "linux")]
    {
        delete_with(persistent, || volatile(Operation::Delete))
    }
    #[cfg(not(target_os = "linux"))]
    {
        persistent().map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[derive(Default)]
    struct FakeStore {
        key: Option<String>,
        fail: bool,
        calls: Vec<&'static str>,
    }

    #[cfg(target_os = "linux")]
    impl FakeStore {
        fn operate(&mut self, op: Operation<'_>) -> Result<Option<String>, String> {
            self.calls.push(match op {
                Operation::Read => "read",
                Operation::Write(_) => "write",
                Operation::Delete => "delete",
            });
            if self.fail {
                return Err(backend_error("fake-secret-sentinel"));
            }
            match op {
                Operation::Read => return Ok(self.key.clone()),
                Operation::Write(key) => self.key = Some(key.into()),
                Operation::Delete => self.key = None,
            }
            Ok(None)
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn fallback_overrides_old_persistent_until_next_persistent_login() {
        let mut persistent = FakeStore {
            key: Some("old-fake-key".into()),
            fail: true,
            ..Default::default()
        };
        let mut volatile = FakeStore::default();
        assert_eq!(
            store_with(
                "new-fake-key",
                |op| persistent.operate(op),
                |op| volatile.operate(op)
            )
            .unwrap(),
            StoreStatus::Volatile
        );
        assert_eq!(volatile.calls, ["write"]);
        persistent.fail = false;
        assert_eq!(
            read_with(
                || persistent.operate(Operation::Read),
                || volatile.operate(Operation::Read)
            )
            .unwrap()
            .as_deref(),
            Some("new-fake-key")
        );
        assert_eq!(persistent.calls, ["write"]);
        volatile.calls.clear();
        assert_eq!(
            store_with(
                "persistent-fake-key",
                |op| persistent.operate(op),
                |op| volatile.operate(op)
            )
            .unwrap(),
            StoreStatus::Persistent
        );
        assert_eq!(volatile.calls, ["delete"]);
        assert_eq!(volatile.key, None);
        assert_eq!(
            read_with(
                || persistent.operate(Operation::Read),
                || volatile.operate(Operation::Read)
            )
            .unwrap()
            .as_deref(),
            Some("persistent-fake-key")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_distinguish_missing_unavailable_and_volatile_override() {
        for fail in [false, true] {
            let mut persistent = FakeStore {
                fail,
                ..Default::default()
            };
            let mut volatile = FakeStore {
                key: Some("fake-key".into()),
                ..Default::default()
            };
            assert_eq!(
                read_with(
                    || persistent.operate(Operation::Read),
                    || volatile.operate(Operation::Read)
                )
                .unwrap()
                .as_deref(),
                Some("fake-key")
            );
            volatile.key = None;
            let result = read_with(
                || persistent.operate(Operation::Read),
                || volatile.operate(Operation::Read),
            );
            if fail {
                let message = result.unwrap_err();
                assert!(message.contains("No volatile credential"));
                assert!(message.contains("tachyon providers login"));
                assert!(!message.contains("fake-secret-sentinel"));
            } else {
                assert_eq!(result.unwrap(), None);
            }
        }
        let result = read_with(
            || panic!("must not use potentially stale persistent key"),
            || Err("kernel inaccessible".into()),
        );
        assert!(result.is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn failed_fallback_and_stale_override_cleanup_are_reported() {
        let mut persistent = FakeStore {
            fail: true,
            ..Default::default()
        };
        let mut volatile = FakeStore {
            fail: true,
            ..Default::default()
        };
        let message = store_with(
            "fake-secret-sentinel",
            |op| persistent.operate(op),
            |op| volatile.operate(op),
        )
        .unwrap_err();
        assert!(message.contains("Login failed"));
        assert!(!message.contains("fake-secret-sentinel"));
        persistent.fail = false;
        let message = store_with(
            "fake-secret-sentinel",
            |op| persistent.operate(op),
            |op| volatile.operate(op),
        )
        .unwrap_err();
        assert!(message.contains("saved persistently"));
        assert!(message.contains("may still take precedence"));
        assert!(!message.contains("fake-secret-sentinel"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn logout_attempts_both_stores_and_reports_each_failure() {
        for (persistent_fail, volatile_fail) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let mut persistent = FakeStore {
                key: Some("fake-key".into()),
                fail: persistent_fail,
                ..Default::default()
            };
            let mut volatile = FakeStore {
                key: Some("fake-key".into()),
                fail: volatile_fail,
                ..Default::default()
            };
            let result = delete_with(
                || persistent.operate(Operation::Delete),
                || volatile.operate(Operation::Delete),
            );
            assert_eq!(persistent.calls, ["delete"]);
            assert_eq!(volatile.calls, ["delete"]);
            assert_eq!(persistent.key.is_some(), persistent_fail);
            assert_eq!(volatile.key.is_some(), volatile_fail);
            if persistent_fail || volatile_fail {
                let message = result.unwrap_err();
                assert_eq!(
                    message.contains("Persistent removal failed"),
                    persistent_fail
                );
                assert_eq!(message.contains("Volatile removal failed"), volatile_fail);
                assert!(message.contains("OPENROUTER_API_KEY is unchanged"));
                assert!(!message.contains("fake-key"));
            } else {
                result.unwrap();
                delete_with(
                    || persistent.operate(Operation::Delete),
                    || volatile.operate(Operation::Delete),
                )
                .unwrap();
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn diagnostics_do_not_include_backend_payloads() {
        let message = backend_error("fake-secret-sentinel");
        assert!(message.contains("Secret Service"));
        assert!(message.contains("locked"));
        assert!(!message.contains("fake-secret-sentinel"));
    }

    #[test]
    #[ignore = "requires an explicitly configured disposable OS credential store"]
    fn persistent_store_round_trip() {
        let service = format!(
            "tachyon-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        struct Cleanup(String);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = operate(&self.0, "fake-account", Operation::Delete);
            }
        }
        let _cleanup = Cleanup(service.clone());
        operate(
            &service,
            "fake-account",
            Operation::Write("fake-credential"),
        )
        .unwrap();
        // Each operation reconnects, so no process-local entry/cache can satisfy this.
        assert_eq!(
            operate(&service, "fake-account", Operation::Read)
                .unwrap()
                .as_deref(),
            Some("fake-credential")
        );
        operate(&service, "fake-account", Operation::Delete).unwrap();
        assert_eq!(
            operate(&service, "fake-account", Operation::Read).unwrap(),
            None
        );
    }
}
