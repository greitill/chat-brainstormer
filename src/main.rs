mod config;
mod hub;
mod llm;
mod models;
mod rest;
mod ws;

use crate::config::Config;
use crate::hub::AppState;
use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env();
    let state = Arc::new(AppState::new(config.clone()).await?);

    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");
    let app = Router::new()
        .route("/ws", get(ws::ws_handler))
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;
    tracing::info!("Probability Amplifier v2 (Rust) listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
