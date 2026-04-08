use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use polybet_events::NatsClient;
use reqwest::Client;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub client: Client,
    pub nats: NatsClient,
    pub gamma_base: String,
    pub data_base: String,
    pub zero_eligible_fail_streak: i64,
    pub observe_events: Arc<RwLock<ObserveEventsState>>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ObserveEventsState {
    pub total_events: u64,
    pub topics: HashMap<String, ObserveTopicState>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ObserveTopicState {
    pub count: u64,
    pub last_received_at: Option<DateTime<Utc>>,
    pub last_payload_json: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SyncStatusResponse {
    pub last_success: Option<DateTime<Utc>>,
    pub markets_synced: i64,
    pub eligible_markets: i64,
    pub zero_eligible_streak: i64,
    pub events_synced: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct LauncherStatusResponse {
    pub last_success: Option<DateTime<Utc>>,
    pub desired_instances: i64,
    pub running_instances: i64,
    pub launched_last_run: i64,
    pub stopped_last_run: i64,
    pub last_error: Option<String>,
}
