use axum::{response::IntoResponse, Json};

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}
