use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;

use crate::types::AppState;

#[derive(Debug, Serialize)]
pub struct SyncStatusResponse {
    pub last_success: Option<DateTime<Utc>>,
    pub markets_synced: i64,
    pub eligible_markets: i64,
    pub zero_eligible_streak: i64,
    pub events_synced: i64,
    pub last_error: Option<String>,
}

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

pub async fn status(State(state): State<Arc<AppState>>) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = match row {
        Some((last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error)) => {
            SyncStatusResponse {
                last_success,
                markets_synced,
                eligible_markets,
                zero_eligible_streak,
                events_synced,
                last_error,
            }
        }
        None => SyncStatusResponse {
            last_success: None,
            markets_synced: 0,
            eligible_markets: 0,
            zero_eligible_streak: 0,
            events_synced: 0,
            last_error: None,
        },
    };

    Ok(Json(serde_json::json!({ "ok": true, "sync": response })))
}

pub async fn get_sync_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SyncStatusResponse>, axum::http::StatusCode> {
    let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = match row {
        Some((last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error)) => {
            SyncStatusResponse {
                last_success,
                markets_synced,
                eligible_markets,
                zero_eligible_streak,
                events_synced,
                last_error,
            }
        }
        None => SyncStatusResponse {
            last_success: None,
            markets_synced: 0,
            eligible_markets: 0,
            zero_eligible_streak: 0,
            events_synced: 0,
            last_error: None,
        },
    };

    Ok(Json(response))
}
