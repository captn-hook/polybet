use chrono::Utc;
use serde_json::Value;

use crate::types::AppState;

#[derive(Debug, Clone, serde::Serialize)]
struct MarketErrorEvent<'a> {
    event_type: &'static str,
    emitted_at: chrono::DateTime<Utc>,
    service: &'a str,
    error_code: &'a str,
    message: String,
    context: Value,
}

pub(super) async fn publish_market_error(
    state: &AppState,
    error_code: &str,
    message: String,
    context: Value,
) -> anyhow::Result<()> {
    state
        .nats
        .publish_json(
            "market.error.v1",
            &MarketErrorEvent {
                event_type: "market.error.v1",
                emitted_at: Utc::now(),
                service: "input",
                error_code,
                message,
                context,
            },
        )
        .await?;
    Ok(())
}

pub(super) async fn record_sync_error(pool: &sqlx::PgPool, message: &str) -> anyhow::Result<()> {
    let trimmed = if message.len() > 2000 { &message[..2000] } else { message };
    sqlx::query(
        r#"
        INSERT INTO manager_sync_status (
            id, last_success, markets_synced, events_synced, data_api_health, last_error, updated_at
        ) VALUES (1, NULL, 0, 0, NULL, $1, now())
        ON CONFLICT (id) DO UPDATE SET
            last_error = excluded.last_error,
            updated_at = now()
        "#,
    )
    .bind(trimmed)
    .execute(pool)
    .await?;
    Ok(())
}
