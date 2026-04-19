use std::sync::Arc;

use axum::{extract::State, response::Html};
use chrono::{DateTime, Utc};

use crate::types::AppState;

use super::render::{
    render_bet_rows, render_launcher_rows, render_leaderboard_rows, render_market_rows,
    render_page, render_prediction_rows, render_queue_rows, render_resolved_market_rows,
    render_rollup_rows, render_signal_rows, PageData,
};

pub async fn dashboard(
    State(state): State<Arc<AppState>>,
) -> Result<Html<String>, axum::http::StatusCode> {
    let db = &state.db;

    // ---- DB queries ----

    let sync_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, last_error FROM manager_sync_status WHERE id = 1",
    )
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    let launcher_row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64, i64, i64, i64, Option<String>)>(
        "SELECT last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error FROM launcher_status WHERE id = 1",
    )
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    let market_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM gamma_markets")
        .fetch_optional(db).await.unwrap_or(None).unwrap_or((0,));
    let event_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM gamma_events")
        .fetch_optional(db).await.unwrap_or(None).unwrap_or((0,));
    let prediction_count = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM experiment_predictions")
        .fetch_optional(db).await.unwrap_or(None).unwrap_or((0,));
    let distinct_experiments = sqlx::query_as::<_, (i64,)>("SELECT COUNT(DISTINCT experiment_id) FROM experiment_predictions")
        .fetch_optional(db).await.unwrap_or(None).unwrap_or((0,));
    let signal_summary = sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
        "SELECT COUNT(*), MAX(created_at) FROM signal_outputs",
    )
    .fetch_optional(db).await.unwrap_or(None).unwrap_or((0, None));

    let latest_signal = sqlx::query_as::<_, (String, String, Option<DateTime<Utc>>)>(
        "SELECT signal_kind, market_id, created_at FROM signal_outputs ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(db).await.unwrap_or(None);

    let recent_signals = sqlx::query_as::<_, (String, String, String, Option<DateTime<Utc>>)>(
        "SELECT signal_id, market_id, signal_kind, created_at FROM signal_outputs ORDER BY created_at DESC LIMIT 300",
    )
    .fetch_all(db).await.unwrap_or_default();

    let experiment_rollups = sqlx::query_as::<_, (String, i64, f64, f64, Option<DateTime<Utc>>)>(
        r#"
        SELECT experiment_id, COUNT(*) AS prediction_count,
               AVG(confidence)::double precision AS avg_confidence,
               AVG(CASE WHEN side = 'YES' THEN 1.0 ELSE 0.0 END)::double precision AS yes_rate,
               MAX(created_at) AS last_prediction
        FROM experiment_predictions
        GROUP BY experiment_id ORDER BY last_prediction DESC LIMIT 50
        "#,
    )
    .fetch_all(db).await.unwrap_or_default();

    let leaderboard_rows = sqlx::query_as::<_, (String, i64, i64, f64, f64, Option<DateTime<Utc>>)>(
        r#"
        SELECT p.experiment_id, COUNT(*)::bigint AS resolved_count,
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
    .fetch_all(db).await.unwrap_or_default();

    let latest_bets = sqlx::query_as::<_, (
        String, String, Option<String>, String, f64, Option<DateTime<Utc>>,
        Option<String>, Option<String>, Option<bool>, Option<String>, Option<String>, Option<DateTime<Utc>>,
    )>(
        r#"
        WITH ranked AS (
            SELECT experiment_id, market_id, market_question, side, confidence, created_at,
                   market_end_date_raw,
                   ROW_NUMBER() OVER (PARTITION BY experiment_id, market_id ORDER BY created_at DESC) AS rn
            FROM experiment_predictions
        )
        SELECT r.experiment_id, r.market_id, r.market_question, r.side, r.confidence, r.created_at,
               r.market_end_date_raw, g.end_date_raw, g.closed,
               mo.winning_side, mo.resolution_status, mo.resolved_at
        FROM ranked r
        LEFT JOIN gamma_markets g ON g.market_id = r.market_id
        LEFT JOIN market_outcomes mo ON mo.market_id = r.market_id
        WHERE r.rn = 1
        ORDER BY r.created_at DESC
        "#,
    )
    .fetch_all(db).await.unwrap_or_default();

    let recent_markets = sqlx::query_as::<_, (
        String, Option<String>, Option<f64>, Option<f64>, Option<bool>, Option<bool>, Option<String>, Option<f64>, bool,
    )>(
        r#"
        SELECT market_id, question, volume, liquidity, active, accepting_orders, end_date_raw,
               (EXTRACT(EPOCH FROM (end_date_raw::timestamptz - now())) / 60.0)::double precision AS minutes_to_end,
               is_eligible
        FROM markets
        WHERE COALESCE(active, false) = true AND COALESCE(closed, false) = false
          AND end_date_raw IS NOT NULL AND btrim(end_date_raw) <> ''
          AND end_date_raw::timestamptz > now()
        ORDER BY end_date_raw::timestamptz ASC
        LIMIT 200
        "#,
    )
    .fetch_all(db).await.unwrap_or_default();

    let resolved_market_rows = sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<bool>, Option<f64>)>(
        r#"
        SELECT market_id, question, end_date_raw, closed, volume
        FROM markets
        WHERE COALESCE(closed, false) = true
          OR (end_date_raw IS NOT NULL AND btrim(end_date_raw) <> '' AND end_date_raw::timestamptz <= now())
        ORDER BY updated_at DESC LIMIT 40
        "#,
    )
    .fetch_all(db).await.unwrap_or_default();

    let recent_predictions = sqlx::query_as::<_, (String, String, String, Option<String>, f64, Option<DateTime<Utc>>)>(
        "SELECT experiment_id, market_id, side, market_question, confidence, created_at FROM experiment_predictions ORDER BY created_at DESC LIMIT 30",
    )
    .fetch_all(db).await.unwrap_or_default();

    let launcher_instances = sqlx::query_as::<_, (String, String, Option<i64>, String, Option<DateTime<Utc>>)>(
        "SELECT container_name, experiment_name, seed, status, updated_at FROM launcher_instances ORDER BY experiment_name, seed NULLS FIRST LIMIT 100",
    )
    .fetch_all(db).await.unwrap_or_default();

    // ---- Destructure raw rows ----
    let (sync_last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, sync_last_error) =
        sync_row.unwrap_or((None, 0, 0, 0, 0, None));
    let (launcher_last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, launcher_last_error) =
        launcher_row.unwrap_or((None, 0, 0, 0, 0, None));
    let (latest_signal_kind, latest_signal_market, latest_signal_at) =
        latest_signal.map(|(k, m, t)| (k, m, t)).unwrap_or_else(|| ("-".into(), "-".into(), None));

    let observe_snapshot = state.observe_events.read().await.clone();

    // ---- Build HTML rows ----
    let bet_rows = render_bet_rows(latest_bets);
    let outstanding_count = bet_rows.exp_outstanding.values().sum::<i64>();
    let resolved_count = bet_rows.exp_resolved.values().sum::<i64>();

    let page = PageData {
        market_count: market_count.0,
        event_count: event_count.0,
        prediction_count: prediction_count.0,
        distinct_experiments: distinct_experiments.0,
        signal_count: signal_summary.0,
        latest_signal_kind,
        latest_signal_market,
        latest_signal_at,
        markets_synced,
        eligible_markets,
        zero_eligible_streak,
        events_synced,
        sync_last_success,
        sync_last_error,
        desired_instances,
        running_instances,
        launched_last_run,
        stopped_last_run,
        launcher_last_success,
        launcher_last_error,
        launcher_rows_html: render_launcher_rows(launcher_instances),
        signal_rows_html: render_signal_rows(recent_signals),
        rollup_rows_html: render_rollup_rows(experiment_rollups, &bet_rows.exp_outstanding, &bet_rows.exp_resolved),
        leaderboard_rows_html: render_leaderboard_rows(leaderboard_rows),
        observe_total_events: observe_snapshot.total_events,
        observe_topic_count: observe_snapshot.topics.len(),
        queue_rows_html: render_queue_rows(observe_snapshot.topics),
        outstanding_count,
        resolved_count,
        outstanding_rows_html: bet_rows.outstanding_html,
        resolved_rows_html: bet_rows.resolved_html,
        prediction_rows_html: render_prediction_rows(recent_predictions),
        market_rows_html: render_market_rows(recent_markets),
        resolved_market_rows_html: render_resolved_market_rows(resolved_market_rows),
    };

    Ok(Html(render_page(&page)))
}
