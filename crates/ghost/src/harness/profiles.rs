#![forbid(unsafe_code)]

//! Startup selection only; registry owns package validation and dispatch.

use super::backend::Local;
use super::registry::{builtins, packages::Packages};
use super::runtime::BrowserAvailability;
use std::sync::Arc;

pub fn worker(backend: Arc<Local>, browser: BrowserAvailability) -> Packages {
    let mut packages = builtins::native();
    packages
        .register(builtins::ipython(Arc::clone(&backend)))
        .expect("valid ipython package");
    if let Some(package) = builtins::browser(backend, browser) {
        packages.register(package).expect("valid browser package");
    }
    packages
}
