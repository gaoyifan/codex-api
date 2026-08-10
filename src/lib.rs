use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use axum::{Router, routing::post};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing_subscriber::EnvFilter;

mod auth;
mod chat;
mod config;
mod credentials;
mod error;
mod http_api;
mod sse;
mod state;
mod store;
mod upstream_http;
mod upstream_ws;
mod ws;

/// Time boundary used by quota windows, token refresh, and request timing.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> time::OffsetDateTime;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> time::OffsetDateTime {
        time::OffsetDateTime::now_utc()
    }
}

/// Starts the relay from a TOML configuration file.
pub async fn run(config_path: &Path) -> anyhow::Result<()> {
    run_with_clock(config_path, Arc::new(SystemClock)).await
}

/// Starts the relay with an injected system-time boundary while preserving the
/// real network and persistence seams used by integration tests.
pub async fn run_with_clock(config_path: &Path, clock: Arc<dyn Clock>) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("codex_api=info")),
        )
        .try_init();

    let config = Arc::new(config::Config::load(config_path)?);
    let store = Arc::new(
        store::Store::open(&config.state.path, Arc::clone(&clock))
            .await
            .context("failed to initialize SQLite state")?,
    );
    let http_client = reqwest::Client::builder()
        .build()
        .context("failed to initialize upstream HTTP client")?;
    let credentials = Arc::new(
        credentials::CredentialManager::load(
            Arc::clone(&store),
            &config.upstream.auth_file,
            config.upstream.oauth_token_url.clone(),
            http_client.clone(),
            clock,
        )
        .await
        .context("failed to initialize ChatGPT authentication")?,
    );
    let upstream_http = upstream_http::UpstreamHttpClient::new(
        http_client,
        &config.upstream.base_url,
        Arc::clone(&credentials),
    );
    let shutdown = CancellationToken::new();
    let pending_requests = TaskTracker::new();
    let websocket_tasks = TaskTracker::new();
    let state = Arc::new(state::AppState {
        config: Arc::clone(&config),
        store,
        credentials: Arc::clone(&credentials),
        upstream_http,
        shutdown: shutdown.clone(),
        pending_requests: pending_requests.clone(),
        websocket_tasks: websocket_tasks.clone(),
    });

    let responses = if config.server.enable_websockets {
        post(http_api::responses).get(ws::responses_websocket)
    } else {
        post(http_api::responses)
    };
    let router = Router::new()
        .route("/v1/responses", responses)
        .route("/v1/chat/completions", post(http_api::chat_completions))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(config.server.listen)
        .await
        .context("failed to bind configured listen address")?;
    tracing::info!(listen = %config.server.listen, "codex-api listening");
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(shutdown.clone()))
        .await;
    shutdown.cancel();
    pending_requests.close();
    websocket_tasks.close();
    tokio::join!(pending_requests.wait(), websocket_tasks.wait());
    credentials.finish_refreshes().await;
    result.context("HTTP server failed")
}

async fn shutdown_signal(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler installation failed");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    shutdown.cancel();
}
