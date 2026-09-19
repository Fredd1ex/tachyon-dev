use crate::harness::registry::manifest::Package;
use std::sync::Arc;
use tachyon_api::{agents::Control, web::WEBFETCH};
use tachyon_model::broker::BrokerClient;

pub const INTERFACE: &str = "webfetch: prefer over browser for known public URLs. Only urls is required (1-4 exact URLs); instruction is optional, follow_links may only be absent or false. Python: await require('webfetch').fetch(urls=['https://example.org/page']). No kind argument. Bounded report, not raw documents; inspect status, notice and citations. Partial/unverified does not confirm every URL was read.";
pub const USAGE: &str = include_str!("usage.md");

pub fn package(client: Arc<BrokerClient>) -> Option<Package> {
    super::websearch::package_for(
        client,
        WEBFETCH,
        Control::WebFetch,
        INTERFACE,
        USAGE,
        &[WEBFETCH],
    )
}
