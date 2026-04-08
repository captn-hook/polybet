use std::{collections::{HashMap, HashSet}, sync::Arc};

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    response::{Html, IntoResponse},
    Json,
};
use chrono::{DateTime, Utc};
use futures_util::stream;
use std::time::Duration;
use serde_json::json;

use crate::{
    types::{AppState, LauncherStatusResponse, SyncStatusResponse},
    util::html_escape,
};

fn status_class(value: &str) -> &'static str {
    let v = value.trim().to_ascii_lowercase();
    match v.as_str() {
        "yes" | "true" | "correct" | "resolved" | "closed" => "ok",
        "no" | "false" | "incorrect" => "bad",
        "pending" | "undecided" | "unknown" | "open" | "end time passed" => "warn",
        _ => "warn",
    }
}

#[cfg(test)]
mod tests {
    use super::status_class;

    #[test]
    fn status_class_maps_boolean_variants() {
        assert_eq!(status_class("true"), "ok");
        assert_eq!(status_class("false"), "bad");
        assert_eq!(status_class("YES"), "ok");
        assert_eq!(status_class("NO"), "bad");
    }

    #[test]
    fn status_class_maps_pending_variants() {
        assert_eq!(status_class("pending"), "warn");
        assert_eq!(status_class("undecided"), "warn");
        assert_eq!(status_class("unknown"), "warn");
    }
}

pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
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

pub async fn status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let now = Utc::now();

    let sync_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let launcher_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        r#"
        SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error
        FROM launcher_status
        WHERE id = 1
        "#,
    )
    .fetch_optional(&state.db)
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
                SELECT COUNT(*)
                FROM ranked r
                LEFT JOIN market_outcomes mo ON mo.market_id = r.market_id
                WHERE r.rn = 1 AND mo.winning_side IS NULL
            ) AS outstanding_latest_bets,
            (
                WITH ranked AS (
                    SELECT experiment_id, market_id, ROW_NUMBER() OVER (PARTITION BY experiment_id, market_id ORDER BY created_at DESC) AS rn
                    FROM experiment_predictions
                )
                SELECT COUNT(*)
                FROM ranked r
                JOIN market_outcomes mo ON mo.market_id = r.market_id
                WHERE r.rn = 1 AND mo.winning_side IN ('YES', 'NO')
            ) AS resolved_latest_bets
        "#,
    )
    .fetch_one(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let latest_signal = sqlx::query_as::<_, (String, String, f64, Option<DateTime<Utc>>)>(
        r#"
        SELECT sentiment_side, market_id, sentiment_confidence, created_at
        FROM signal_outputs
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let last_prediction_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "SELECT MAX(created_at) FROM experiment_predictions",
    )
    .fetch_one(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let db_connections = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()",
    )
    .fetch_one(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let unresolved_resolved_rows = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM market_outcomes WHERE resolution_status = 'resolved' AND winning_side IS NULL",
    )
    .fetch_one(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let top_experiments = sqlx::query_as::<_, (String, i64, f64, f64, Option<DateTime<Utc>>)>(
        r#"
        SELECT
            experiment_id,
            COUNT(*) AS prediction_count,
            AVG(confidence)::double precision AS avg_confidence,
            AVG(CASE WHEN side = 'YES' THEN 1.0 ELSE 0.0 END)::double precision AS yes_rate,
            MAX(created_at) AS last_prediction
        FROM experiment_predictions
        GROUP BY experiment_id
        ORDER BY last_prediction DESC
        LIMIT 10
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let to_age = |ts: Option<DateTime<Utc>>| ts.map(|v| (now - v).num_seconds().max(0));
    let (sync_last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, sync_error) =
        sync_row.unwrap_or((None, 0, 0, 0, 0, None));
    let (
        launcher_last_success,
        desired_instances,
        running_instances,
        launched_last_run,
        stopped_last_run,
        launcher_error,
    ) = launcher_row.unwrap_or((None, 0, 0, 0, 0, None));

    let (
        markets_total,
        markets_open,
        markets_open_accepting,
        markets_eligible_count,
        events_total,
        signals_total,
        predictions_total,
        experiments_distinct,
        outcomes_total,
        outcomes_with_winner,
        outstanding_latest_bets,
        resolved_latest_bets,
    ) = counts;

    let latest_signal_json = latest_signal
        .map(|(side, market_id, confidence, created_at)| {
            json!({
                "side": side,
                "market_id": market_id,
                "confidence": confidence,
                "created_at": created_at,
                "age_seconds": to_age(created_at),
            })
        })
        .unwrap_or_else(|| json!(null));

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

    let mut alerts = Vec::<String>::new();
    if sync_error.is_some() {
        alerts.push("manager sync has an error".to_string());
    }
    if launcher_error.is_some() {
        alerts.push("launcher has an error".to_string());
    }
    if state.zero_eligible_fail_streak > 0 && zero_eligible_streak > 0 {
        alerts.push(format!("eligible market streak at {}", zero_eligible_streak));
    }
    if unresolved_resolved_rows > 0 {
        alerts.push(format!(
            "{} resolved outcomes missing winner_side",
            unresolved_resolved_rows
        ));
    }
    if running_instances < desired_instances {
        alerts.push(format!(
            "launcher running_instances ({}) below desired_instances ({})",
            running_instances, desired_instances
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

pub async fn get_launcher_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<LauncherStatusResponse>, axum::http::StatusCode> {
    let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        r#"
        SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error
        FROM launcher_status
        WHERE id = 1
        "#,
    )
    .fetch_optional(&state.db)
    .await
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let response = match row {
        Some((last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error)) => {
            LauncherStatusResponse {
                last_success,
                desired_instances,
                running_instances,
                launched_last_run,
                stopped_last_run,
                last_error,
            }
        }
        None => LauncherStatusResponse {
            last_success: None,
            desired_instances: 0,
            running_instances: 0,
            launched_last_run: 0,
            stopped_last_run: 0,
            last_error: None,
        },
    };
    Ok(Json(response))
}

pub async fn dashboard(State(state): State<Arc<AppState>>) -> Result<Html<String>, axum::http::StatusCode> {
    let sync_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let launcher_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        r#"
        SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error
        FROM launcher_status
        WHERE id = 1
        "#,
    )
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None);

    let market_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM gamma_markets")
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None)
        .unwrap_or((0,));
    let event_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM gamma_events")
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None)
        .unwrap_or((0,));

    let prediction_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM experiment_predictions")
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None)
        .unwrap_or((0,));

    let distinct_experiments =
        sqlx::query_as::<_, (i64,)>("SELECT COUNT(DISTINCT experiment_id) FROM experiment_predictions")
            .fetch_optional(&state.db)
            .await
            .unwrap_or(None)
            .unwrap_or((0,));

    let signal_summary = sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
        "SELECT COUNT(*), MAX(created_at) FROM signal_outputs",
    )
    .fetch_optional(&state.db)
    .await
    .unwrap_or(None)
    .unwrap_or((0, None));

    let latest_signal =
        sqlx::query_as::<_, (String, String, f64, Option<DateTime<Utc>>)>(
            r#"
        SELECT sentiment_side, market_id, sentiment_confidence, created_at
        FROM signal_outputs
        ORDER BY created_at DESC
        LIMIT 1
        "#,
        )
        .fetch_optional(&state.db)
        .await
        .unwrap_or(None);

    let recent_signals = sqlx::query_as::<_, (String, String, String, f64, Option<DateTime<Utc>>)>(
            r#"
        SELECT signal_id, market_id, sentiment_side, sentiment_confidence, created_at
        FROM signal_outputs
        ORDER BY created_at DESC
        LIMIT 300
        "#,
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let experiment_rollups = sqlx::query_as::<_, (String, i64, f64, f64, Option<DateTime<Utc>>)>(
            r#"
        SELECT
            experiment_id,
            COUNT(*) AS prediction_count,
            AVG(confidence)::double precision AS avg_confidence,
            AVG(CASE WHEN side = 'YES' THEN 1.0 ELSE 0.0 END)::double precision AS yes_rate,
            MAX(created_at) AS last_prediction
        FROM experiment_predictions
        GROUP BY experiment_id
        ORDER BY last_prediction DESC
        LIMIT 50
        "#,
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let leaderboard_rows_raw = sqlx::query_as::<_, (String, i64, i64, f64, f64, Option<DateTime<Utc>>)>(
            r#"
        SELECT
            p.experiment_id,
            COUNT(*)::bigint AS resolved_count,
            SUM(CASE WHEN p.side = mo.winning_side THEN 1 ELSE 0 END)::bigint AS correct_count,
            AVG(CASE WHEN p.side = mo.winning_side THEN 1.0 ELSE 0.0 END)::double precision AS accuracy,
            AVG(p.confidence)::double precision AS avg_confidence,
            MAX(p.created_at) AS last_prediction
        FROM experiment_predictions p
        JOIN market_outcomes mo ON mo.market_id = p.market_id
        WHERE mo.winning_side IN ('YES','NO')
        GROUP BY p.experiment_id
        ORDER BY accuracy DESC, resolved_count DESC, last_prediction DESC
        LIMIT 50
        "#,
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let observe_snapshot = state.observe_events.read().await.clone();

    let latest_bets = sqlx::query_as::<_, (
        String,
        String,
        Option<String>,
        String,
        f64,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<String>,
        Option<bool>,
        Option<String>,
        Option<String>,
        Option<DateTime<Utc>>,
    )>(
        r#"
        WITH ranked AS (
            SELECT
                experiment_id,
                market_id,
                market_question,
                side,
                confidence,
                created_at,
                market_end_date_raw,
                ROW_NUMBER() OVER (PARTITION BY experiment_id, market_id ORDER BY created_at DESC) AS rn
            FROM experiment_predictions
        )
        SELECT
            r.experiment_id,
            r.market_id,
            r.market_question,
            r.side,
            r.confidence,
            r.created_at,
            r.market_end_date_raw,
            g.end_date_raw,
            g.closed,
            mo.winning_side,
            mo.resolution_status,
            mo.resolved_at
        FROM ranked r
        LEFT JOIN gamma_markets g ON g.market_id = r.market_id
        LEFT JOIN market_outcomes mo ON mo.market_id = r.market_id
        WHERE r.rn = 1
        ORDER BY r.created_at DESC
        LIMIT 500
        "#,
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let recent_markets = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            Option<f64>,
            Option<f64>,
            Option<bool>,
            Option<bool>,
            Option<String>,
            Option<f64>,
            bool,
        ),
    >(
        r#"
        SELECT
            market_id,
            question,
            volume,
            liquidity,
            active,
            accepting_orders,
            end_date_raw,
            (EXTRACT(EPOCH FROM (end_date_raw::timestamptz - now())) / 60.0)::double precision AS minutes_to_end,
            is_eligible
        FROM markets
        WHERE
            COALESCE(active, false) = true
            AND COALESCE(closed, false) = false
            AND end_date_raw IS NOT NULL
            AND btrim(end_date_raw) <> ''
            AND end_date_raw::timestamptz > now()
        ORDER BY end_date_raw::timestamptz ASC
        LIMIT 200
        "#,
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let recent_predictions = sqlx::query_as::<_, (String, String, String, Option<String>, f64, Option<DateTime<Utc>>)>(
            r#"
        SELECT experiment_id, market_id, side, market_question, confidence, created_at
        FROM experiment_predictions
        ORDER BY created_at DESC
        LIMIT 30
        "#,
        )
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let resolved_market_rows = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<bool>, Option<f64>)>(
        r#"
        SELECT market_id, question, end_date_raw, closed, volume
        FROM markets
        WHERE
            COALESCE(closed, false) = true
            OR (
                end_date_raw IS NOT NULL
                AND btrim(end_date_raw) <> ''
                AND end_date_raw::timestamptz <= now()
            )
        ORDER BY updated_at DESC
        LIMIT 40
        "#,
    )
    .fetch_all(&state.db)
    .await
    .unwrap_or_default();

    let launcher_instances = sqlx::query_as::<_, (String, String, Option<i64>, String, Option<DateTime<Utc>>)>(
        r#"
        SELECT container_name, experiment_name, seed, status, updated_at
        FROM launcher_instances
        ORDER BY experiment_name, seed NULLS FIRST
        LIMIT 100
        "#,
    )
    .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let mut outstanding_rows = String::new();
    let mut resolved_rows = String::new();
    let mut exp_outstanding: HashMap<String, i64> = HashMap::new();
    let mut exp_resolved: HashMap<String, i64> = HashMap::new();
    let now = Utc::now();

    for (
        experiment_id,
        market_id,
        market_question,
        side,
        confidence,
        created_at,
        pred_end_raw,
        gm_end_raw,
        gm_closed,
        winning_side,
        resolution_status,
        resolved_at,
    ) in latest_bets
    {
        let end_raw = gm_end_raw.or(pred_end_raw);
        let parsed_end = end_raw.as_deref().and_then(|v| DateTime::parse_from_rfc3339(v).ok());
        let end_utc = parsed_end.map(|v| v.with_timezone(&Utc));
        let closed_flag = gm_closed.unwrap_or(false);
        let outcome_resolved = winning_side
            .as_deref()
            .map(|s| matches!(s, "YES" | "NO"))
            .unwrap_or(false);
        let is_resolved = outcome_resolved;

        let end_display = end_utc
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        let created_display = created_at
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        let question = html_escape(market_question.as_deref().unwrap_or("-"));
        let resolved_at_display = resolved_at
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        let resolve_reason = if outcome_resolved {
            "resolved"
        } else if closed_flag {
            "closed-awaiting-outcome"
        } else if end_utc.map(|ts| ts <= now).unwrap_or(false) {
            "end time passed"
        } else {
            "open"
        };

        let correctness = match winning_side.as_deref() {
            Some(win) if win == side => "correct",
            Some("YES") | Some("NO") => "incorrect",
            _ => "pending",
        };
        let correctness_class = status_class(correctness);
        let outcome_display = winning_side.unwrap_or_else(|| "-".to_string());
        let resolution_display = resolution_status.unwrap_or_else(|| resolve_reason.to_string());

        let row_html = format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{:.4}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td><td>{}</td><td class=\"{}\">{}</td></tr>",
            html_escape(&experiment_id),
            html_escape(&market_id),
            question,
            status_class(&side),
            html_escape(&side),
            confidence,
            end_display,
            created_display,
            status_class(&resolution_display),
            html_escape(&resolution_display),
            status_class(&outcome_display),
            html_escape(&outcome_display),
            resolved_at_display,
            correctness_class,
            html_escape(correctness),
        );

        if is_resolved {
            *exp_resolved.entry(experiment_id).or_insert(0) += 1;
            resolved_rows.push_str(&row_html);
        } else {
            *exp_outstanding.entry(experiment_id).or_insert(0) += 1;
            outstanding_rows.push_str(&row_html);
        }
    }

    let mut market_rows = String::new();
    for (id, question, volume, liquidity, active, accepting_orders, end_date_raw, minutes_to_end, is_eligible) in recent_markets {
        let active_text = if active.unwrap_or(false) { "true" } else { "false" };
        let accepting_text = if accepting_orders.unwrap_or(false) { "true" } else { "false" };
        let eligible_text = if is_eligible { "true" } else { "false" };
        market_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.2}</td><td>{:.2}</td><td>{:.2}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td><td class=\"{}\">{}</td></tr>",
            html_escape(&id),
            html_escape(question.as_deref().unwrap_or("-")),
            html_escape(end_date_raw.as_deref().unwrap_or("-")),
            volume.unwrap_or(0.0),
            liquidity.unwrap_or(0.0),
            minutes_to_end.unwrap_or(-1.0),
            status_class(active_text),
            active_text,
            status_class(accepting_text),
            accepting_text,
            status_class(eligible_text),
            eligible_text,
        ));
    }

    let mut resolved_market_table_rows = String::new();
    for (id, question, end_date_raw, closed, volume) in resolved_market_rows {
        let closed_text = if closed.unwrap_or(false) { "true" } else { "false" };
        resolved_market_table_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{:.2}</td></tr>",
            html_escape(&id),
            html_escape(question.as_deref().unwrap_or("-")),
            html_escape(end_date_raw.as_deref().unwrap_or("-")),
            status_class(closed_text),
            closed_text,
            volume.unwrap_or(0.0),
        ));
    }

    let mut prediction_rows = String::new();
    for (experiment_id, market_id, side, market_question, confidence, created_at) in recent_predictions {
        prediction_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{:.4}</td><td>{}</td></tr>",
            html_escape(&experiment_id),
            html_escape(&market_id),
            status_class(&side),
            html_escape(&side),
            html_escape(market_question.as_deref().unwrap_or("-")),
            confidence,
            created_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        ));
    }

    let mut signal_rows = String::new();
    let mut seen_signal_keys: HashSet<(String, String, String, String)> = HashSet::new();
    let mut rendered_signal_rows = 0usize;
    for (signal_id, market_id, side, confidence, created_at) in recent_signals {
        let key = (
            signal_id.clone(),
            market_id.clone(),
            side.clone(),
            format!("{confidence:.4}"),
        );
        if seen_signal_keys.contains(&key) {
            continue;
        }
        seen_signal_keys.insert(key);
        signal_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{:.4}</td><td>{}</td></tr>",
            html_escape(&signal_id),
            html_escape(&market_id),
            status_class(&side),
            html_escape(&side),
            confidence,
            created_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        ));
        rendered_signal_rows += 1;
        if rendered_signal_rows >= 30 {
            break;
        }
    }

    let mut rollup_rows = String::new();
    for (experiment_id, pred_count, avg_conf, yes_rate, last_prediction) in experiment_rollups {
        let outstanding = exp_outstanding.get(&experiment_id).copied().unwrap_or(0);
        let resolved = exp_resolved.get(&experiment_id).copied().unwrap_or(0);
        rollup_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:.4}</td><td class=\"{}\">{:.2}%</td><td>{}</td></tr>",
            html_escape(&experiment_id),
            pred_count,
            outstanding,
            resolved,
            avg_conf,
            if yes_rate >= 0.5 { "ok" } else { "bad" },
            yes_rate * 100.0,
            last_prediction
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        ));
    }

    let mut leaderboard_rows = String::new();
    for (experiment_id, resolved_count, correct_count, accuracy, avg_confidence, last_prediction) in leaderboard_rows_raw {
        leaderboard_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{:.2}%</td><td>{:.4}</td><td>{}</td></tr>",
            html_escape(&experiment_id),
            resolved_count,
            correct_count,
            if accuracy >= 0.5 { "ok" } else { "bad" },
            accuracy * 100.0,
            avg_confidence,
            last_prediction
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        ));
    }

    let mut queue_rows: Vec<(String, u64, Option<DateTime<Utc>>, Option<String>)> = observe_snapshot
        .topics
        .iter()
        .map(|(topic, state)| {
            (
                topic.clone(),
                state.count,
                state.last_received_at,
                state.last_payload_json.clone(),
            )
        })
        .collect();
    queue_rows.sort_by(|a, b| b.1.cmp(&a.1));
    let mut queue_rows_html = String::new();
    for (topic, count, last_received_at, last_payload_json) in queue_rows.into_iter().take(50) {
        let preview = last_payload_json
            .unwrap_or_else(|| "-".to_string())
            .chars()
            .take(160)
            .collect::<String>();
        queue_rows_html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td><code>{}</code></td></tr>",
            html_escape(&topic),
            count,
            last_received_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            html_escape(&preview),
        ));
    }

    let mut launcher_rows = String::new();
    for (container_name, experiment_name, seed, status, updated_at) in launcher_instances {
        launcher_rows.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&container_name),
            html_escape(&experiment_name),
            seed.map(|s| s.to_string()).unwrap_or_else(|| "-".to_string()),
            html_escape(&status),
            updated_at
                .map(|ts| ts.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
        ));
    }

    let (latest_signal_side, latest_signal_market, latest_signal_conf, latest_signal_at) = latest_signal
        .map(|(side, market, conf, ts)| (side, market, conf, ts))
        .unwrap_or_else(|| ("-".to_string(), "-".to_string(), 0.0, None));

    let (last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error) =
        sync_row.unwrap_or((None, 0, 0, 0, 0, None));
    let (
        launcher_last_success,
        desired_instances,
        running_instances,
        launched_last_run,
        stopped_last_run,
        launcher_error,
    ) = launcher_row.unwrap_or((None, 0, 0, 0, 0, None));

    let outstanding_count = exp_outstanding.values().sum::<i64>();
    let resolved_count = exp_resolved.values().sum::<i64>();

    let html = format!(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Polybet Manager Dashboard</title>
    <style>
      body {{ font-family: system-ui, sans-serif; margin: 20px; }}
      h1, h2, h3 {{ margin: 8px 0; }}
      .cards {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); gap: 10px; }}
      .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 10px; }}
      .ok {{ background: #dff7e5; color: #1e5a2d; font-weight: 600; }}
      .bad {{ background: #fde3e3; color: #7f1d1d; font-weight: 600; }}
      .warn {{ background: #fff4d6; color: #6b4e00; font-weight: 600; }}
      td.ok, td.bad, td.warn {{ border-radius: 6px; }}
      .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; }}
      .muted {{ color: #666; font-size: 0.9rem; }}
      table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
      th, td {{ text-align: left; border-bottom: 1px solid #eee; padding: 7px; font-size: 0.92rem; }}
      .grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; }}
      details.section {{ border: 1px solid #efefef; border-radius: 8px; padding: 6px 10px; }}
      details.section > summary {{ cursor: pointer; font-weight: 600; margin: 4px 0; }}
      .subtle {{ color: #444; font-size: 0.9rem; }}
      .pager {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; }}
      .pager button {{ padding: 4px 8px; border: 1px solid #ccc; background: #fafafa; border-radius: 6px; cursor: pointer; }}
      .pager button:disabled {{ opacity: 0.5; cursor: default; }}
    </style>
    <script>
      document.addEventListener('DOMContentLoaded', () => {{
        const key = 'polybet_dashboard_sections_v1';
        let saved = {{}};
        try {{
          saved = JSON.parse(localStorage.getItem(key) || '{{}}');
        }} catch (_) {{}}
        document.querySelectorAll('details[data-section-id]').forEach((el) => {{
          const id = el.getAttribute('data-section-id');
          if (saved[id] !== undefined) {{
            el.open = !!saved[id];
          }}
          el.addEventListener('toggle', () => {{
            saved[id] = el.open;
            localStorage.setItem(key, JSON.stringify(saved));
            document.cookie = `polybet_${{id}}=${{el.open ? 1 : 0}}; path=/; max-age=2592000`;
          }});
        }});

        const paginate = (tableId, pageSize = 15) => {{
          const table = document.getElementById(tableId);
          if (!table) return;
          const body = table.querySelector('tbody');
          if (!body) return;
          const rows = Array.from(body.querySelectorAll('tr'));
          if (rows.length <= pageSize) return;

          let page = 0;
          const pages = Math.ceil(rows.length / pageSize);
          const pager = document.createElement('div');
          pager.className = 'pager';
          pager.innerHTML = `<button type="button">Prev</button><span></span><button type="button">Next</button>`;
          const [prev, info, next] = pager.children;
          table.parentElement.appendChild(pager);

          const render = () => {{
            const start = page * pageSize;
            const end = start + pageSize;
            rows.forEach((r, idx) => {{
              r.style.display = idx >= start && idx < end ? '' : 'none';
            }});
            info.textContent = `Page ${{page + 1}} / ${{pages}}`;
            prev.disabled = page === 0;
            next.disabled = page >= pages - 1;
          }};

          prev.addEventListener('click', () => {{
            if (page > 0) {{
              page -= 1;
              render();
            }}
          }});
          next.addEventListener('click', () => {{
            if (page < pages - 1) {{
              page += 1;
              render();
            }}
          }});
          render();
        }};

        paginate('tbl-launcher', 20);
        paginate('tbl-leaderboard', 20);
        paginate('tbl-event-queue', 20);
        paginate('tbl-signals', 20);
        paginate('tbl-exp-metrics', 20);
        paginate('tbl-bets-outstanding', 20);
        paginate('tbl-bets-resolved', 20);
        paginate('tbl-predictions', 20);
        paginate('tbl-markets', 20);
        paginate('tbl-resolved-markets', 20);

        const live = new EventSource('/api/dashboard/live');
        live.addEventListener('dashboard', () => {{
          window.location.reload();
        }});
        live.onerror = () => {{
          live.close();
        }};
      }});
    </script>
  </head>
  <body>
    <h1>Polybet Manager Dashboard</h1>
    <p class="muted">Live updates via server-sent events</p>

    <details class="section" data-section-id="system-overview" open>
      <summary>System Overview</summary>
      <div class="cards">
        <div class="card"><strong>Markets in DB</strong><div>{}</div></div>
        <div class="card"><strong>Events in DB</strong><div>{}</div></div>
        <div class="card"><strong>Total Predictions</strong><div>{}</div></div>
        <div class="card"><strong>Active Experiments</strong><div>{}</div></div>
        <div class="card"><strong>Total Signals</strong><div>{}</div></div>
      </div>
    </details>

    <details class="section" data-section-id="sync-status" open>
      <summary>Gamma/Data Sync</summary>
      <div class="cards">
        <div class="card"><strong>Last Sync Markets</strong><div>{}</div></div>
        <div class="card"><strong>Eligible Markets</strong><div>{}</div></div>
        <div class="card"><strong>Zero Eligible Streak</strong><div>{}</div></div>
        <div class="card"><strong>Last Sync Events</strong><div>{}</div></div>
        <div class="card"><strong>Last Success</strong><div>{}</div></div>
        <div class="card"><strong>Latest Signal Side</strong><div>{}</div></div>
        <div class="card"><strong>Latest Signal Market</strong><div>{}</div></div>
      </div>
      <p><strong>Sync Error:</strong> {}</p>
    </details>

    <details class="section" data-section-id="launcher" open>
      <summary>Launcher</summary>
      <div class="cards">
        <div class="card"><strong>Desired</strong><div>{}</div></div>
        <div class="card"><strong>Running</strong><div>{}</div></div>
        <div class="card"><strong>Launched (last)</strong><div>{}</div></div>
        <div class="card"><strong>Stopped (last)</strong><div>{}</div></div>
        <div class="card"><strong>Last Reconcile</strong><div>{}</div></div>
      </div>
      <p><strong>Launcher Error:</strong> {}</p>
      <table id="tbl-launcher">
        <thead>
          <tr><th>Container</th><th>Experiment</th><th>Seed</th><th>Status</th><th>Updated</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="signal-feed">
      <summary>Signal Feed (polymarket_sentiment)</summary>
      <div class="cards">
        <div class="card"><strong>Latest Side</strong><div><span class="pill {}">{}</span></div></div>
        <div class="card"><strong>Latest Confidence</strong><div>{:.4}</div></div>
        <div class="card"><strong>Latest Market ID</strong><div>{}</div></div>
        <div class="card"><strong>Latest Signal Time</strong><div>{}</div></div>
        <div class="card"><strong>Signals Written</strong><div>{}</div></div>
      </div>
      <table id="tbl-signals">
        <thead>
          <tr><th>Signal ID</th><th>Market ID</th><th>Side</th><th>Confidence</th><th>Created</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="experiment-summary" open>
      <summary>Experiment Metrics</summary>
      <table id="tbl-exp-metrics">
        <thead>
          <tr><th>Experiment</th><th>Predictions</th><th>Outstanding Bets</th><th>Resolved Bets</th><th>Avg Confidence</th><th>YES Rate</th><th>Last Prediction</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="experiment-leaderboard" open>
      <summary>Experiment Leaderboard (Resolved Markets)</summary>
      <table id="tbl-leaderboard">
        <thead>
          <tr><th>Experiment</th><th>Resolved</th><th>Correct</th><th>Accuracy</th><th>Avg Confidence</th><th>Last Prediction</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="event-queue" open>
      <summary>Live Event Queue View</summary>
      <div class="cards">
        <div class="card"><strong>Total Observed Events</strong><div>{}</div></div>
        <div class="card"><strong>Observed Topics</strong><div>{}</div></div>
      </div>
      <table id="tbl-event-queue">
        <thead>
          <tr><th>Topic</th><th>Count</th><th>Last Received</th><th>Last Payload Preview</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="bets-outstanding" open>
      <summary>Outstanding Bets</summary>
      <p class="subtle">Latest prediction per experiment+market where market appears open.</p>
      <div class="cards">
        <div class="card"><strong>Outstanding Count</strong><div>{}</div></div>
        <div class="card"><strong>Resolved Count</strong><div>{}</div></div>
      </div>
      <table id="tbl-bets-outstanding">
        <thead>
          <tr><th>Experiment</th><th>Market</th><th>Question</th><th>Side</th><th>Confidence</th><th>End Time</th><th>Predicted At</th><th>Resolution</th><th>Winning Side</th><th>Resolved At</th><th>Result</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <details class="section" data-section-id="bets-resolved">
      <summary>Resolved Bets</summary>
      <p class="subtle">Latest prediction per experiment+market where market appears closed or expired.</p>
      <table id="tbl-bets-resolved">
        <thead>
          <tr><th>Experiment</th><th>Market</th><th>Question</th><th>Side</th><th>Confidence</th><th>End Time</th><th>Predicted At</th><th>Resolution</th><th>Winning Side</th><th>Resolved At</th><th>Result</th></tr>
        </thead>
        <tbody>{}</tbody>
      </table>
    </details>

    <div class="grid">
      <details class="section" data-section-id="recent-predictions">
        <summary>Recent Predictions</summary>
        <table id="tbl-predictions">
          <thead>
            <tr><th>Experiment</th><th>Market ID</th><th>Side</th><th>Question</th><th>Confidence</th><th>Created</th></tr>
          </thead>
          <tbody>{}</tbody>
        </table>
      </details>
      <details class="section" data-section-id="recent-markets">
        <summary>Soonest Closing Open Markets</summary>
        <table id="tbl-markets">
          <thead>
            <tr><th>Market ID</th><th>Question</th><th>End Date</th><th>Volume</th><th>Liquidity</th><th>Minutes to End</th><th>Active</th><th>Accepting</th><th>Eligible</th></tr>
          </thead>
          <tbody>{}</tbody>
        </table>
      </details>
      <details class="section" data-section-id="recent-resolved-markets">
        <summary>Recent Resolved/Expired Markets</summary>
        <table id="tbl-resolved-markets">
          <thead>
            <tr><th>Market ID</th><th>Question</th><th>End Date</th><th>Closed</th><th>Volume</th></tr>
          </thead>
          <tbody>{}</tbody>
        </table>
      </details>
    </div>
  </body>
</html>"#,
        market_count.0,
        event_count.0,
        prediction_count.0,
        distinct_experiments.0,
        signal_summary.0,
        markets_synced,
        eligible_markets,
        zero_eligible_streak,
        events_synced,
        last_success.map(|ts| ts.to_rfc3339()).unwrap_or_else(|| "never".to_string()),
        status_class(&latest_signal_side),
        html_escape(&latest_signal_side),
        html_escape(&latest_signal_market),
        html_escape(last_error.as_deref().unwrap_or("none")),
        desired_instances,
        running_instances,
        launched_last_run,
        stopped_last_run,
        launcher_last_success
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "never".to_string()),
        html_escape(launcher_error.as_deref().unwrap_or("none")),
        launcher_rows,
        html_escape(&latest_signal_side),
        latest_signal_conf,
        html_escape(&latest_signal_market),
        latest_signal_at
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "never".to_string()),
        signal_summary.0,
        signal_rows,
        rollup_rows,
        leaderboard_rows,
        observe_snapshot.total_events,
        observe_snapshot.topics.len(),
        queue_rows_html,
        outstanding_count,
        resolved_count,
        outstanding_rows,
        resolved_rows,
        prediction_rows,
        market_rows,
        resolved_market_table_rows
    );

    Ok(Html(html))
}
