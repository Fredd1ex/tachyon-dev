#![forbid(unsafe_code)]

//! Provider configuration. API keys are never stored in the config file; they
//! come from the environment or operating system credential store.

use crate::config::Config;
use crate::style::{palette, render};
use tachyon_util::credentials::StoreStatus;

pub fn list() {
    let cfg = Config::load();
    let p = palette();
    let has_key = cfg.resolve_key("openrouter").is_some();
    let src = if has_key {
        "environment or operating system credential store"
    } else {
        "not set — run `tachyon providers login` or export OPENROUTER_API_KEY"
    };
    println!(
        "{} provider: {}",
        render(&p.accent, "OpenRouter"),
        render(&p.dim, "(default)")
    );
    println!(
        "  {}   {}",
        render(&p.dim, "key:"),
        render(
            if has_key { &p.good } else { &p.bad },
            if has_key { "set" } else { "unset" }
        )
    );
    println!("         {}", render(&p.dim, src));
    println!("  {}   {}", render(&p.dim, "model:"), cfg.active_model());
    println!(
        "  {}   {}",
        render(&p.dim, "base:"),
        cfg.provider_base_url()
    );
    println!();
    println!(
        "{}",
        render(
            &p.dim,
            "secrets are never stored in config — use `tachyon providers login` or OPENROUTER_API_KEY"
        )
    );
}

pub fn set_model(model: &str) -> Result<(), String> {
    let mut cfg = Config::load();
    cfg.model.name = Some(model.trim().to_string());
    cfg.provider.get_or_insert(crate::config::Provider {
        name: "openrouter".into(),
        base_url: Some("https://openrouter.ai/api/v1".into()),
        routing: None,
    });
    cfg.save(&Config::default_path())
        .map_err(|e| e.to_string())?;
    println!(
        "{}",
        crate::style::render(
            &crate::style::palette().good,
            format!("Model set to {model}.")
        )
    );
    Ok(())
}

pub fn login() -> Result<(), String> {
    let key =
        rpassword::prompt_password("OpenRouter API key: ").map_err(|error| error.to_string())?;
    let key = key.trim();
    let confirmation = persist_login(key, tachyon_util::credentials::store_openrouter_key)?;
    println!("{confirmation}");
    println!("Restart the daemon to use it: tachyon daemon restart");
    Ok(())
}

fn persist_login(
    key: &str,
    store: impl FnOnce(&str) -> Result<StoreStatus, String>,
) -> Result<&'static str, String> {
    if key.is_empty() {
        return Err("key cannot be empty".into());
    }
    if key.starts_with("sk-test") || key.starts_with("placeholder") || key.contains("your-api-key")
    {
        return Err("key looks like a placeholder".into());
    }
    match store(key)? {
        StoreStatus::Persistent => Ok("Credential saved to the persistent operating system credential store (survives reboot). Environment overrides still take precedence."),
        StoreStatus::Volatile => Ok("WARNING: Persistent credential storage failed (it may be unavailable or locked). Credential saved to the volatile Linux kernel keyring and will be lost on reboot (or earlier if cleared). It overrides any older persistent key until then; after reboot that older key may become active again. Configure/unlock Secret Service and log in again for persistent storage. Environment overrides still take precedence."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_reports_actual_storage_lifetime() {
        let fake_store = std::rc::Rc::new(std::cell::RefCell::new(None));
        let connection = fake_store.clone();
        let confirmation = persist_login("fake-login-key", move |key| {
            *connection.borrow_mut() = Some(key.to_string());
            Ok(StoreStatus::Persistent)
        })
        .unwrap();
        assert!(confirmation.contains("persistent"));
        assert!(confirmation.contains("survives reboot"));
        assert!(!confirmation.contains("fake-login-key"));
        let warning = persist_login("fake-login-key", |_| Ok(StoreStatus::Volatile)).unwrap();
        assert!(warning.contains("WARNING"));
        assert!(warning.contains("lost on reboot"));
        assert!(warning.contains("older key may become active"));
        assert!(!warning.contains("survives reboot"));
        assert!(!warning.contains("fake-login-key"));
        // The login connection is gone; a new client still sees the saved value.
        let new_connection = fake_store.clone();
        assert_eq!(new_connection.borrow().as_deref(), Some("fake-login-key"));
        assert_eq!(
            persist_login("fake-login-key", |_| Err("persistent store locked".into())).unwrap_err(),
            "persistent store locked"
        );
        for key in ["", "sk-test-fake", "placeholder", "your-api-key"] {
            assert!(persist_login(key, |_| panic!("invalid key must not be stored")).is_err());
        }
    }
}

pub fn logout() -> Result<(), String> {
    tachyon_util::credentials::delete_openrouter_key()?;
    println!("Stored credentials removed (or already absent), including both persistent and volatile stores on Linux. OPENROUTER_API_KEY is unchanged; unset it separately in the daemon environment.");
    println!("Restart the daemon to clear it: tachyon daemon restart");
    Ok(())
}

pub fn get(key: &str) -> Option<String> {
    let cfg = Config::load();
    match key {
        "model" => Some(cfg.active_model()),
        "base_url" => Some(cfg.provider_base_url()),
        _ => None,
    }
}
