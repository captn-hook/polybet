mod dashboard;
mod render;
mod status;

use std::{sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    Json,
};
use futures_util::stream;
use serde_json::json;

use crate::types::{AppState, LauncherStatusResponse, SyncStatusResponse};

pub use dashboard::dashboard;
pub use status::status;

// ---------------------------------------------------------------------------
// Simple handlers
// ---------------------------------------------------------------------------

pub async fn health() -> impl IntoResponse {
    Json(json!({ "ok": true }))
}

pub async fn observability(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let snapshot = state.observe_events.read().await.clone();
    Ok(Json(json!({
        "ok": true,
        "observability": snapshot,
    })))
}

pub async fn dashboard_live(
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>>, axum::http::StatusCode> {
    let stream = stream::unfold(state, |state| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let payload = status(State(state.clone())).await.ok().map(|j| j.0).unwrap_or_else(|| json!({}));
        let event = Event::default()
            .event("dashboard")
            .data(serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string()));
        Some((Ok::<Event, std::convert::Infallible>(event), state))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub async fn get_sync_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SyncStatusResponse>, axum::http::StatusCode> {
    use chrono::{DateTime, Utc};

    let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = match row {
        Some((last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error)) => {
            SyncStatusResponse { last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error }
        }
        None => SyncStatusResponse {
            last_success: None, markets_synced: 0, eligible_markets: 0,
            zero_eligible_streak: 0, events_synced: 0, last_error: None,
        },
    };
    Ok(Json(response))
}

pub async fn get_launcher_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<LauncherStatusResponse>, axum::http::StatusCode> {
    use chrono::{DateTime, Utc};

    let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        r#"
        SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error
        FROM launcher_status WHERE id = 1
        "#,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = match row {
        Some((last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error)) => {
            LauncherStatusResponse { last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error }
        }
        None => LauncherStatusResponse {
            last_success: None, desired_instances: 0, running_instances: 0,
            launched_last_run: 0, stopped_last_run: 0, last_error: None,
        },
    };
    Ok(Json(response))
}
