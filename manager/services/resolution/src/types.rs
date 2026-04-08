use polybet_events::NatsClient;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub nats: NatsClient,
}
