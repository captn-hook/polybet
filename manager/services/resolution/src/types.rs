use polybet_events::NatsClient;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub consumer_db: PgPool,
    pub nats: NatsClient,
    pub gamma_base: String,
    pub resolution_sweep_interval_seconds: u64,
    pub resolution_retry_base_minutes: i64,
    pub resolution_retry_max_minutes: i64,
    pub resolution_retry_batch_size: i64,
}
