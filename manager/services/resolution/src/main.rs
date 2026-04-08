use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::{routing::get, Router};
use chrono::Utc;
use polybet_events::NatsClient;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

mod api;
mod resolution;
mod settings;
mod types;

use settings::Settings;
use types::AppState;

#[derive(Debug, serde::Serialize)]
struct RoleStartedEvent {
    service_role: &'static str,
    started_at: chrono::DateTime<Utc>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    let settings = Settings::from_env()?;
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&settings.database_url)
        .await
        .context("failed to connect to postgres")?;
    let nats = NatsClient::connect(&settings.nats_url).await?;

    let state = Arc::new(AppState {
        db: db.clone(),
        nats: nats.clone(),
        gamma_base: settings.gamma_api_base.clone(),
        resolution_sweep_interval_seconds: settings.resolution_sweep_interval_seconds,
        resolution_retry_base_minutes: settings.resolution_retry_base_minutes,
        resolution_retry_max_minutes: settings.resolution_retry_max_minutes,
        resolution_retry_batch_size: settings.resolution_retry_batch_size,
    });

    resolution::start_resolution_loop(state.clone());

    let app = Router::new()
        .route("/health", get(api::health))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], settings.manager_port));
    info!("manager-resolution service starting");
    nats.publish_json(
        "poly.service.role.started.v1",
        &RoleStartedEvent {
            service_role: "resolution",
            started_at: Utc::now(),
        },
    )
    .await
    .context("failed to publish startup role event")?;
    info!("manager-resolution listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
