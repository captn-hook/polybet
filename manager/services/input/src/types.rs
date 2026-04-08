use std::sync::Arc;

use chrono::{DateTime, Utc};
use polybet_events::NatsClient;
use reqwest::Client;
use sqlx::PgPool;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub client: Client,
    pub nats: NatsClient,
    pub gamma_base: String,
    pub data_base: String,
    pub launcher_manifest_path: String,
    pub launcher_docker_base: String,
    pub launcher_experiment_image: String,
    pub launcher_experiment_config_bind: String,
    pub observe_events: Arc<RwLock<ObserveEventsState>>,
}

#[derive(Debug, serde::Serialize)]
pub struct LaunchResult {
    pub launched: usize,
    pub stopped: usize,
    pub running: usize,
    pub desired: usize,
}

#[derive(Debug, Default, Clone)]
pub struct ObserveEventsState {
    pub total_events: u64,
    pub topics: std::collections::HashMap<String, ObserveTopicState>,
}

#[derive(Debug, Default, Clone)]
pub struct ObserveTopicState {
    pub count: u64,
    pub last_received_at: Option<DateTime<Utc>>,
    pub last_payload_json: Option<String>,
}
