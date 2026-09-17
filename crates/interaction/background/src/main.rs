#![forbid(unsafe_code)]

mod campaign;
mod requests;
mod review;
mod scheduling;

use std::{process::ExitCode, sync::Arc};

use tachyon_model::{Model, ModelConfig};

use requests::process_requests;
use review::review;
use tachyon_api::campaign_oversight::{BackgroundRequest, BackgroundResponse};

fn main() -> ExitCode {
    if let Some(code) = tachyon_util::guard::guard_or_exit_code() {
        return ExitCode::from(code as u8);
    }
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tachyon-background: tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run());
    // Tokio stdin/stdout use blocking tasks that cannot be aborted on stream failure.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn run() -> ExitCode {
    let config = tachyon_util::config::Config::load();
    let background = config.background_config();
    let persona = background.persona.clone().map(Arc::<str>::from);
    let model = model_from_config(&config, &background).ok().map(Arc::new);
    eprintln!("tachyon-background: ready");
    let result = process_requests(
        tokio::io::stdin(),
        tokio::io::BufWriter::new(tokio::io::stdout()),
        move |request| {
            let model = model.clone();
            let persona = persona.clone();
            async move {
                match request {
                    BackgroundRequest::Review(request) => BackgroundResponse::Review(
                        review(&request.request, model.as_deref(), persona.as_deref()).await,
                    ),
                    BackgroundRequest::CampaignAssessment(request) => {
                        BackgroundResponse::CampaignAssessment(
                            campaign::dispatch(request, model.as_deref()).await,
                        )
                    }
                }
            }
        },
    )
    .await;
    if let Err(error) = result {
        eprintln!("tachyon-background: review stream failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn model_from_config(
    config: &tachyon_util::config::Config,
    agent: &tachyon_util::config::AgentConfig,
) -> Result<Model, String> {
    let api_key = config.resolve_key("openrouter").ok_or_else(|| {
        "api key not configured (set OPENROUTER_API_KEY and restart the daemon)".to_string()
    })?;
    let configured = agent.model(&config.model);
    Ok(Model::new(ModelConfig {
        base_url: config.provider_base_url(),
        api_key,
        model: configured
            .name
            .clone()
            .unwrap_or_else(|| tachyon_util::config::Config::default_model().into()),
        temperature: configured.temperature.unwrap_or(0.0),
        max_completion_tokens: configured.max_completion_tokens,
        context_length: configured.context_length,
        parallel_tool_calls: false,
        reasoning: configured.reasoning,
        routing: config.provider_routing(),
        debug: std::env::var("TACHYON_DEBUG").is_ok_and(|value| value == "1" || value == "true"),
        debug_log: Some(tachyon_util::daemon::logs_dir().join("debug-http.log")),
    }))
}
