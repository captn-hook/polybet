use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::types::AppState;

pub async fn status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let now = Utc::now();
    let db = &state.db;

    let sync_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let launcher_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error FROM launcher_status WHERE id = 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let counts = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM gamma_markets) AS markets_total,
            (SELECT COUNT(*) FROM gamma_markets WHERE COALESCE(active, false) = true AND COALESCE(closed, false) = false) AS markets_open,
            (SELECT COUNT(*) FROM gamma_markets WHERE COALESCE(active, false) = true AND COALESCE(closed, false) = false AND COALESCE(accepting_orders, true) = true) AS markets_open_accepting,
            (SELECT COUNT(*) FROM gamma_markets WHERE COALESCE(is_eligible, false) = true) AS markets_eligible,
            (SELECT COUNT(*) FROM gamma_events) AS events_total,
            (SELECT COUNT(*) FROM signal_outputs) AS signals_total,
            (SELECT COUNT(*) FROM experiment_predictions) AS predictions_total,
            (SELECT COUNT(DISTINCT experiment_id) FROM experiment_predictions) AS experiments_distinct,
            (SELECT COUNT(*) FROM market_outcomes) AS outcomes_total,
            (SELECT COUNT(*) FROM market_outcomes WHERE winning_side IN ('YES', 'NO')) AS outcomes_with_winner,
            (
                WITH ranked AS (
                    SELECT experiment_id, market_id, ROW_NUMBER() OVER (PARTITION BY experiment_id, market_id ORDER BY created_at DESC) AS rn
                    FROM experiment_predictions
                )
                SELECT COUNT(*) FROM ranked r
                LEFT JOIN market_outcomes mo ON mo.market_id = r.market_id
                WHERE r.rn = 1 AND mo.winning_side IS NULL
            ) AS outstanding_latest_bets,
            (
                WITH ranked AS (
                    SELECT experiment_id, market_id, ROW_NUMBER() OVER (PARTITION BY experiment_id, market_id ORDER BY created_at DESC) AS rn
                    FROM experiment_predictions
                )
                SELECT COUNT(*) FROM ranked r
                JOIN market_outcomes mo ON mo.market_id = r.market_id
                WHERE r.rn = 1 AND mo.winning_side IN ('YES', 'NO')
            ) AS resolved_latest_bets
        "#,
    )
    .fetch_one(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let latest_signal = sqlx::query_as::<_, (String, String, Option<DateTime<Utc>>)>(
        "SELECT signal_kind, market_id, created_at FROM signal_outputs ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let last_prediction_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "SELECT MAX(created_at) FROM experiment_predictions",
    )
    .fetch_one(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let db_connections = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()",
    )
    .fetch_one(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let unresolved_resolved_rows = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM market_outcomes WHERE resolution_status = 'resolved' AND winning_side IS NULL",
    )
    .fetch_one(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let top_experiments = sqlx::query_as::<_, (String, i64, f64, f64, Option<DateTime<Utc>>)>(
        r#"
        SELECT experiment_id, COUNT(*) AS prediction_count,
               AVG(confidence)::double precision AS avg_confidence,
               AVG(CASE WHEN side = 'YES' THEN 1.0 ELSE 0.0 END)::double precision AS yes_rate,
               MAX(created_at) AS last_prediction
        FROM experiment_predictions
        GROUP BY experiment_id
        ORDER BY last_prediction DESC
        LIMIT 10
        "#,
    )
    .fetch_all(db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    // ---- Destructure raw rows ----
    let to_age = |ts: Option<DateTime<Utc>>| ts.map(|v| (now - v).num_seconds().max(0));

    let (sync_last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, sync_error) =
        sync_row.unwrap_or((None, 0, 0, 0, 0, None));

    let (launcher_last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, launcher_error) =
        launcher_row.unwrap_or((None, 0, 0, 0, 0, None));

    let (
        markets_total, markets_open, markets_open_accepting, markets_eligible_count,
        events_total, signals_total, predictions_total, experiments_distinct,
        outcomes_total, outcomes_with_winner, outstanding_latest_bets, resolved_latest_bets,
    ) = counts;

    let latest_signal_json = latest_signal
        .map(|(signal_kind, market_id, created_at)| {
            json!({
                "signal_kind": signal_kind,
                "market_id": market_id,
                "created_at": created_at,
                "age_seconds": to_age(created_at),
            })
        })
        .unwrap_or(json!(null));

    let top_experiments_json: Vec<serde_json::Value> = top_experiments
        .into_iter()
        .map(|(experiment_id, prediction_count, avg_confidence, yes_rate, last_prediction)| {
            json!({
                "experiment_id": experiment_id,
                "prediction_count": prediction_count,
                "avg_confidence": avg_confidence,
                "yes_rate": yes_rate,
                "last_prediction": last_prediction,
                "last_prediction_age_seconds": to_age(last_prediction),
            })
        })
        .collect();

    // ---- Alerts ----
    let mut alerts = Vec::<String>::new();
    if sync_error.is_some() {
        alerts.push("manager sync has an error".to_string());
    }
    if launcher_error.is_some() {
        alerts.push("launcher has an error".to_string());
    }
    if state.zero_eligible_fail_streak > 0 && zero_eligible_streak > 0 {
        alerts.push(format!("eligible market streak at {zero_eligible_streak}"));
    }
    if unresolved_resolved_rows > 0 {
        alerts.push(format!("{unresolved_resolved_rows} resolved outcomes missing winner_side"));
    }
    if running_instances < desired_instances {
        alerts.push(format!(
            "launcher running_instances ({running_instances}) below desired_instances ({desired_instances})"
        ));
    }

    Ok(Json(json!({
        "ok": sync_error.is_none() && launcher_error.is_none(),
        "timestamp": now,
        "alerts": alerts,
        "sync": {
            "last_success": sync_last_success,
            "last_success_age_seconds": to_age(sync_last_success),
            "markets_synced_last_run": markets_synced,
            "eligible_markets_last_run": eligible_markets,
            "zero_eligible_streak": zero_eligible_streak,
            "events_synced_last_run": events_synced,
            "last_error": sync_error,
        },
        "launcher": {
            "last_success": launcher_last_success,
            "last_success_age_seconds": to_age(launcher_last_success),
            "desired_instances": desired_instances,
            "running_instances": running_instances,
            "launched_last_run": launched_last_run,
            "stopped_last_run": stopped_last_run,
            "last_error": launcher_error,
        },
        "counts": {
            "markets_total": markets_total,
            "markets_open": markets_open,
            "markets_open_accepting_orders": markets_open_accepting,
            "markets_eligible": markets_eligible_count,
            "events_total": events_total,
            "signals_total": signals_total,
            "predictions_total": predictions_total,
            "experiments_distinct": experiments_distinct,
            "outcomes_total": outcomes_total,
            "outcomes_with_winner": outcomes_with_winner,
            "outstanding_latest_bets": outstanding_latest_bets,
            "resolved_latest_bets": resolved_latest_bets,
        },
        "freshness": {
            "latest_signal": latest_signal_json,
            "last_prediction_at": last_prediction_at,
            "last_prediction_age_seconds": to_age(last_prediction_at),
        },
        "database": {
            "connections_current_db": db_connections,
        },
        "top_experiments": top_experiments_json,
    })))
}
