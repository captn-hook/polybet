use std::sync::Arc;

use reqwest::Client;
use serde_json::{json, Value};
use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::types::AppState;

use super::consumers::publish_resolution_error;
use super::outcome::{derive_market_outcome, emit_last_snapshot_signal, normalized_resolution_status};
use super::parse::{extract_first_market_item, value_as_bool_opt, value_as_f64_opt, value_as_string_opt};

struct DueMarket {
    market_id: String,
    retry_count: i32,
    payload_json: Option<Value>,
    closed: Option<bool>,
}

pub(super) async fn run_resolution_scheduler(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut ticker = time::interval(Duration::from_secs(state.resolution_sweep_interval_seconds.max(10)));
    let client = Client::new();
    loop {
        ticker.tick().await;
        if let Err(err) = run_resolution_sweep(&state, &client).await {
            if let Err(report_err) = publish_resolution_error(
                &state,
                "resolution_sweep",
                err.to_string(),
                json!({"service_role": "resolution"}),
            )
            .await
            {
                error!(error = %report_err, original_error = %err, "failed to persist resolution sweep error");
            }
        }
    }
}

async fn run_resolution_sweep(state: &Arc<AppState>, client: &Client) -> anyhow::Result<()> {
    let bootstrap_count = bootstrap_tracking_candidates(state).await?;
    let due_markets = fetch_due_markets(state).await?;
    let mut resolved_count = 0_i64;
    let mut retry_count = 0_i64;

    for market in due_markets {
        let market_id = market.market_id.clone();
        let payload = if market.retry_count == 0 {
            if let Some(payload) = market.payload_json {
                payload
            } else {
                fetch_and_refresh_market(state, client, &market_id).await?;
                refresh_payload_from_db(state, &market_id).await?.unwrap_or_else(|| json!({}))
            }
        } else {
            fetch_and_refresh_market(state, client, &market_id).await?;
            refresh_payload_from_db(state, &market_id).await?
                .or(market.payload_json)
                .unwrap_or_else(|| json!({}))
        };

        let closed = market.closed.unwrap_or_else(|| {
            payload.get("closed").and_then(Value::as_bool).unwrap_or(false)
        });

        if let Some((resolution_status, winning_side, resolved_at, payload_json)) =
            derive_market_outcome(&payload, closed)
        {
            if let Err(err) = emit_last_snapshot_signal(state, &market_id, &payload).await {
                tracing::warn!(error = %err, market_id = %market_id, "failed to emit last-snapshot signal");
            }

            sqlx::query(
                r#"
                INSERT INTO market_outcomes (
                    market_id, resolution_status, winning_side, resolved_at, source, payload_json, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (market_id) DO UPDATE SET
                    resolution_status = excluded.resolution_status,
                    winning_side = excluded.winning_side,
                    resolved_at = COALESCE(excluded.resolved_at, market_outcomes.resolved_at),
                    source = excluded.source,
                    payload_json = excluded.payload_json,
                    updated_at = now()
                "#,
            )
            .bind(&market_id)
            .bind(&resolution_status)
            .bind(winning_side.as_deref())
            .bind(resolved_at)
            .bind("resolution_scheduler")
            .bind(payload_json)
            .execute(&state.db)
            .await?;

            sqlx::query("DELETE FROM market_tracking WHERE market_id = $1")
                .bind(&market_id)
                .execute(&state.db)
                .await?;
            resolved_count += 1;
            continue;
        }

        let new_retry_count = market.retry_count.saturating_add(1);
        let delay_minutes = compute_retry_delay_minutes(
            state.resolution_retry_base_minutes,
            state.resolution_retry_max_minutes,
            new_retry_count,
        );
        let status = normalized_resolution_status(&payload, closed);
        sqlx::query(
            r#"
            UPDATE market_tracking
            SET retry_count = $2,
                next_check_at = now() + ($3::text || ' minutes')::interval,
                last_checked_at = now(),
                last_resolution_status = $4,
                last_error = NULL,
                updated_at = now()
            WHERE market_id = $1
            "#,
        )
        .bind(&market_id)
        .bind(new_retry_count)
        .bind(delay_minutes)
        .bind(status)
        .execute(&state.db)
        .await?;
        retry_count += 1;
    }

    info!(bootstrap_count, resolved_count, retry_count, "resolution sweep completed");
    Ok(())
}

async fn bootstrap_tracking_candidates(state: &Arc<AppState>) -> anyhow::Result<i64> {
    let result = sqlx::query(
        r#"
        INSERT INTO market_tracking (
            market_id, next_check_at, retry_count, first_seen_at, last_checked_at,
            last_resolution_status, last_error, updated_at
        )
        SELECT
            m.market_id, now(), 0, now(), NULL, 'pending', NULL, now()
        FROM markets m
        LEFT JOIN market_outcomes mo ON mo.market_id = m.market_id
        LEFT JOIN market_tracking mt ON mt.market_id = m.market_id
        WHERE mt.market_id IS NULL
          AND m.end_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
          AND m.end_date_raw::timestamptz <= now()
          AND (
              mo.market_id IS NULL OR
              mo.resolution_status NOT IN ('resolved', 'finalized', 'settled')
          )
        LIMIT $1
        "#,
    )
    .bind(state.resolution_retry_batch_size)
    .execute(&state.db)
    .await?;
    Ok(result.rows_affected() as i64)
}

async fn fetch_due_markets(state: &Arc<AppState>) -> anyhow::Result<Vec<DueMarket>> {
    let rows = sqlx::query_as::<_, (String, i32, Option<Value>, Option<bool>)>(
        r#"
        SELECT mt.market_id, mt.retry_count, gm.payload_json, gm.closed
        FROM market_tracking mt
        LEFT JOIN gamma_markets gm ON gm.market_id = mt.market_id
        WHERE mt.next_check_at IS NULL OR mt.next_check_at <= now()
        ORDER BY mt.retry_count ASC, COALESCE(mt.next_check_at, mt.first_seen_at) ASC
        LIMIT $1
        "#,
    )
    .bind(state.resolution_retry_batch_size)
    .fetch_all(&state.db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(market_id, retry_count, payload_json, closed)| DueMarket {
            market_id,
            retry_count,
            payload_json,
            closed,
        })
        .collect())
}

async fn fetch_and_refresh_market(
    state: &Arc<AppState>,
    client: &Client,
    market_id: &str,
) -> anyhow::Result<()> {
    let base = state.gamma_base.trim_end_matches('/');
    let url = format!("{base}/markets?id={market_id}");
    let res = client.get(url).send().await?.error_for_status()?;
    let body = res.json::<Value>().await?;
    let Some(market) = extract_first_market_item(&body) else {
        sqlx::query(
            r#"
            UPDATE market_tracking
            SET last_checked_at = now(),
                last_error = 'gamma market missing during resolution sweep',
                last_resolution_status = 'pending',
                updated_at = now()
            WHERE market_id = $1
            "#,
        )
        .bind(market_id)
        .execute(&state.db)
        .await?;
        return Ok(());
    };

    sqlx::query(
        r#"
        INSERT INTO gamma_markets (
            market_id, slug, question, start_date_raw, end_date_raw,
            volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now())
        ON CONFLICT (market_id) DO UPDATE SET
            slug = excluded.slug,
            question = excluded.question,
            start_date_raw = excluded.start_date_raw,
            end_date_raw = excluded.end_date_raw,
            volume = excluded.volume,
            liquidity = excluded.liquidity,
            active = excluded.active,
            closed = excluded.closed,
            accepting_orders = excluded.accepting_orders,
            is_eligible = excluded.is_eligible,
            payload_json = excluded.payload_json,
            updated_at = now()
        "#,
    )
    .bind(value_as_string_opt(&market, &["id", "marketId", "conditionId", "slug"]).unwrap_or_else(|| market_id.to_string()))
    .bind(value_as_string_opt(&market, &["slug"]))
    .bind(value_as_string_opt(&market, &["question", "title"]))
    .bind(value_as_string_opt(&market, &["startDate", "start_date", "startTime"]))
    .bind(value_as_string_opt(&market, &["endDate", "end_date", "endTime"]))
    .bind(value_as_f64_opt(&market, &["volume", "volumeNum", "volumeUsd"]))
    .bind(value_as_f64_opt(&market, &["liquidity", "liquidityNum", "liquidityUsd"]))
    .bind(value_as_bool_opt(&market, &["active", "isActive"]))
    .bind(value_as_bool_opt(&market, &["closed", "isClosed"]))
    .bind(value_as_bool_opt(&market, &["acceptingOrders", "accepting_orders"]))
    .bind(false)
    .bind(&market)
    .execute(&state.db)
    .await?;
    Ok(())
}

async fn refresh_payload_from_db(state: &Arc<AppState>, market_id: &str) -> anyhow::Result<Option<Value>> {
    let row = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT payload_json FROM gamma_markets WHERE market_id = $1",
    )
    .bind(market_id)
    .fetch_optional(&state.db)
    .await?;
    Ok(row.flatten())
}

fn compute_retry_delay_minutes(base_minutes: i64, max_minutes: i64, retry_count: i32) -> i64 {
    let safe_base = base_minutes.max(1);
    let safe_max = max_minutes.max(safe_base);
    let exponent = retry_count.max(0).min(20) as u32;
    let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
    safe_base.saturating_mul(multiplier).min(safe_max)
}

#[cfg(test)]
mod tests {
    use super::compute_retry_delay_minutes;

    #[test]
    fn computes_exponential_retry_delay_with_cap() {
        assert_eq!(compute_retry_delay_minutes(60, 1440, 0), 60);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 1), 120);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 2), 240);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 3), 480);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 4), 960);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 5), 1440);
        assert_eq!(compute_retry_delay_minutes(60, 1440, 6), 1440);
    }
}
