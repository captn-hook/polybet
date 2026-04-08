use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{routing::{get, post}, Router};
use chrono::Utc;
use polybet_events::NatsClient;
use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

mod api;
mod launcher;
mod observe;
mod settings;
mod types;
mod util;

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
    let nats = NatsClient::connect(&settings.nats_url).await?;

    let state = Arc::new(AppState {
        db: db.clone(),
        client: Client::builder()
            .user_agent("polybet-manager-observe/0.1")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(20))
            .build()?,
        nats: nats.clone(),
        gamma_base: settings.gamma_api_base.clone(),
        data_base: String::new(),
        launcher_manifest_path: settings.launcher_manifest_path.clone(),
        launcher_docker_base: settings.launcher_docker_base.clone(),
        launcher_experiment_image: settings.launcher_experiment_image.clone(),
        launcher_experiment_config_bind: settings.launcher_experiment_config_bind.clone(),
        zero_eligible_fail_streak: settings.zero_eligible_fail_streak,
        launcher_enabled: true,
        observe_events: Arc::new(tokio::sync::RwLock::new(ObserveEventsState {
            total_events: 0,
            topics: HashMap::new(),
        })),
    });

    observe::start_observe_loop(state.clone());
    launcher::start_launcher_loop(state.clone(), 15);

    let app = Router::new()
        .route("/health", get(api::health))
        .route("/status", get(api::status))
        .route("/observability", get(api::observability))
        .route("/api/sync/status", get(api::get_sync_status))
        .route("/api/launcher/status", get(api::get_launcher_status))
        .route("/api/launcher/reconcile", post(api::trigger_launcher_reconcile))
        .route("/dashboard", get(api::dashboard))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], settings.manager_port));
    info!("manager-observe service starting");
    nats.publish_json(
        "poly.service.role.started.v1",
        &RoleStartedEvent {
            service_role: "observe",
            started_at: Utc::now(),
        },
    )
    .await
    .context("failed to publish startup role event")?;
    info!("manager-observe listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
