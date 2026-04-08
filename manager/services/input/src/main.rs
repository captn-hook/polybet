use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{routing::get, Router};
use chrono::Utc;
use polybet_db::run_migrations;
use polybet_events::NatsClient;
use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

mod api;
mod settings;
mod sync;
mod types;

use settings::Settings;
use types::{AppState, ObserveEventsState};

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
    run_migrations(&db).await?;
    let nats = NatsClient::connect(&settings.nats_url).await?;

    let state = Arc::new(AppState {
        db: db.clone(),
        client: Client::builder()
            .user_agent("polybet-manager-input/0.1")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(20))
            .build()?,
        nats: nats.clone(),
        gamma_base: settings.gamma_api_base.clone(),
        data_base: settings.data_api_base.clone(),
        observe_events: Arc::new(tokio::sync::RwLock::new(ObserveEventsState {
            total_events: 0,
            topics: HashMap::new(),
        })),
    });

    sync::start_sync_loop(
        state.clone(),
        settings.sync_interval_seconds,
        settings.top_markets_limit,
        settings.top_events_limit,
        settings.market_max_minutes_to_end,
        settings.zero_eligible_fail_streak,
    );

    let app = Router::new()
        .route("/health", get(api::health))
        .route("/status", get(api::status))
        .route("/api/sync/status", get(api::get_sync_status))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], settings.manager_port));
    info!("manager-input service starting");
    nats.publish_json(
        "poly.service.role.started.v1",
        &RoleStartedEvent {
            service_role: "input",
            started_at: Utc::now(),
        },
    )
    .await
    .context("failed to publish startup role event")?;
    info!("manager-input listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
