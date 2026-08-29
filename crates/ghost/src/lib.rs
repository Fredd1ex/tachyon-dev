#![forbid(unsafe_code)]

//! Ghost — the Tachyon agent harness.

pub mod harness;
pub mod model;
pub mod role;

pub mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum GhostError {
        #[error("api key not configured (set the OPENROUTER_API_KEY environment variable and restart the daemon)")]
        NoApiKey,
        #[error("http error: {0}")]
        Http(#[from] reqwest::Error),
        #[error("io error: {0}")]
        Io(#[from] std::io::Error),
        #[error("api error: {0}")]
        Api(String),
        #[error(transparent)]
        Model(#[from] tachyon_model::ModelError),
    }
}
