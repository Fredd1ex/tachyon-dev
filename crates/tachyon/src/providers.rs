#![forbid(unsafe_code)]

//! Provider configuration. API keys are never stored in the config file; they
//! come from the environment or operating system credential store.

use crate::config::Config;
use crate::style::{palette, render};

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
    if key.is_empty() {
        return Err("key cannot be empty".into());
    }
    if key.starts_with("sk-test") || key.starts_with("placeholder") || key.contains("your-api-key")
    {
        return Err("key looks like a placeholder".into());
    }
    tachyon_util::credentials::store_openrouter_key(key)?;
    println!("Credential stored in the operating system credential store.");
    println!("Restart the daemon to use it: tachyon daemon restart");
    Ok(())
}

pub fn logout() -> Result<(), String> {
    tachyon_util::credentials::delete_openrouter_key()?;
    println!("Credential removed from the operating system credential store.");
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
