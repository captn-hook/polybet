use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::types::AppState;

const ERROR_SUBJECTS: &[&str] = &[
    "market.error.v1",
    "signal.error.v1",
    "prediction.error.v1",
    "resolution.error.v1",
];

#[derive(Debug, Deserialize)]
struct PredictionProposedEvent {
    experiment_id: String,
    strategy: Option<String>,
    market_id: String,
    market_question: Option<String>,
    market_end_date_raw: Option<String>,
    side: String,
    confidence: f64,
    horizon_minutes: Option<i32>,
    rationale: Option<String>,
    meta: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SignalComputedEvent {
    signal_id: Option<String>,
    signal_kind: Option<String>,
    market_id: Option<String>,
    market_question: Option<String>,
    status: Option<String>,
    sentiment_side: Option<String>,
    sentiment_confidence: Option<f64>,
    #[serde(flatten)]
    full_payload: Value,
}

#[derive(Debug, Serialize)]
struct ResolutionErrorEvent<'a> {
    event_type: &'a str,
    emitted_at: DateTime<Utc>,
    service: &'a str,
    error_code: &'a str,
    stage: &'a str,
    message: String,
    context: Value,
}

#[derive(Debug, Deserialize)]
struct GenericErrorEvent {
    event_type: Option<String>,
    service: Option<String>,
    error_code: Option<String>,
    message: Option<String>,
    context: Option<Value>,
    occurred_at: Option<DateTime<Utc>>,
    emitted_at: Option<DateTime<Utc>>,
}

pub fn start_resolution_loop(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = wait_for_required_tables(&state).await {
            error!(error = %err, "resolution startup dependency check failed");
            return;
        }

        let prediction_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = consume_predictions(prediction_state).await {
                error!(error = %err, "prediction consumer failed");
            }
        });

        let signal_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = consume_signals(signal_state).await {
                error!(error = %err, "signal consumer failed");
            }
        });

        let scheduler_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_resolution_scheduler(scheduler_state).await {
                error!(error = %err, "resolution scheduler failed");
            }
        });

        for subject in ERROR_SUBJECTS {
            let state_clone = state.clone();
            let subject_name = (*subject).to_string();
            tokio::spawn(async move {
                if let Err(err) = consume_error_events(state_clone, subject_name.as_str()).await {
                    error!(subject = %subject_name, error = %err, "resolution error-event consumer failed");
                }
            });
        }
    });
}

async fn wait_for_required_tables(state: &AppState) -> anyhow::Result<()> {
    let required_tables = [
        "markets",
        "gamma_markets",
        "market_outcomes",
        "market_tracking",
        "experiment_predictions",
        "error_events",
        "signal_outputs",
    ];
    let mut attempts = 0_u32;
    loop {
        let mut missing: Vec<&str> = Vec::new();
        for table in required_tables {
            let regclass_name = format!("public.{table}");
            let exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(regclass_name)
                .fetch_one(&state.db)
                .await?;
            if exists.is_none() {
                missing.push(table);
            }
        }
        if missing.is_empty() {
            info!("resolution startup dependencies are ready");
            return Ok(());
        }
        attempts = attempts.saturating_add(1);
        if attempts % 6 == 1 {
            info!(?missing, "waiting for required tables");
        }
        if attempts >= 120 {
            anyhow::bail!(
                "required tables not ready after {} attempts: {}",
                attempts,
                missing.join(", ")
            );
        }
        time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_resolution_scheduler(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut ticker = time::interval(Duration::from_secs(state.resolution_sweep_interval_seconds.max(10)));
    let client = Client::new();
    loop {
        ticker.tick().await;
        if let Err(err) = run_resolution_sweep(&state, &client).await {
            if let Err(report_err) = publish_resolution_error(
                &state,
                "resolution_sweep",
                err.to_string(),
                json!({"service_role":"resolution"}),
            )
            .await
            {
                error!(error = %report_err, original_error = %err, "failed to persist resolution sweep error");
            }
        }
    }
}

#[derive(Debug)]
struct DueMarket {
    market_id: String,
    retry_count: i32,
    payload_json: Option<Value>,
    closed: Option<bool>,
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
            payload
                .get("closed")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        });

        if let Some((resolution_status, winning_side, resolved_at, payload_json)) =
            derive_market_outcome(&payload, closed)
        {
            // Emit last-snapshot market_implied signal before recording resolution
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

    info!(
        bootstrap_count,
        resolved_count,
        retry_count,
        "resolution sweep completed"
    );
    Ok(())
}

async fn bootstrap_tracking_candidates(state: &Arc<AppState>) -> anyhow::Result<i64> {
    let result = sqlx::query(
        r#"
        INSERT INTO market_tracking (
            market_id, next_check_at, retry_count, first_seen_at, last_checked_at, last_resolution_status, last_error, updated_at
        )
        SELECT
            m.market_id,
            now(),
            0,
            now(),
            NULL,
            'pending',
            NULL,
            now()
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
        SELECT
            mt.market_id,
            mt.retry_count,
            gm.payload_json,
            gm.closed
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

async fn fetch_and_refresh_market(state: &Arc<AppState>, client: &Client, market_id: &str) -> anyhow::Result<()> {
    let base = state.gamma_base.trim_end_matches('/');
    let url = format!("{base}/markets?id={market_id}");
    let res = client.get(url).send().await?.error_for_status()?;
    let body = res.json::<Value>().await?;
    let Some(market) = extract_first_market_item(&body) else {
        sqlx::query(
            r#"
            UPDATE market_tracking
            SET
                last_checked_at = now(),
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
            market_id, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json, updated_at
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

fn normalized_resolution_status(payload: &Value, closed: bool) -> String {
    if let Some(status) = payload
        .get("umaResolutionStatus")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return status.to_ascii_lowercase();
    }
    if closed {
        "closed_unresolved".to_string()
    } else {
        "pending".to_string()
    }
}

fn derive_market_outcome(payload: &Value, closed: bool) -> Option<(String, Option<String>, Option<DateTime<Utc>>, Value)> {
    let uma_status = payload
        .get("umaResolutionStatus")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase());
    let is_resolved = uma_status
        .as_deref()
        .map(|s| matches!(s, "resolved" | "finalized" | "settled"))
        .unwrap_or(false);
    if !is_resolved && !closed {
        return None;
    }

    let resolved_at = extract_resolved_at(payload);
    let winning_side = infer_winning_side(payload);
    let resolution_status = if is_resolved {
        uma_status.unwrap_or_else(|| "resolved".to_string())
    } else {
        "closed".to_string()
    };
    let outcome_payload = json!({
        "source": "resolution_scheduler",
        "closed": closed,
        "uma_resolution_status": payload.get("umaResolutionStatus"),
        "winning_side_inferred": winning_side,
        "raw_market": payload
    });
    Some((resolution_status, winning_side, resolved_at, outcome_payload))
}

fn extract_resolved_at(raw: &Value) -> Option<DateTime<Utc>> {
    let parse = |s: &str| {
        let normalized = if s.len() >= 3 {
            let tz = &s[s.len() - 3..];
            if (tz.starts_with('+') || tz.starts_with('-')) && tz[1..].chars().all(|c| c.is_ascii_digit()) {
                format!("{s}00")
            } else {
                s.to_string()
            }
        } else {
            s.to_string()
        };

        DateTime::parse_from_rfc3339(s)
            .map(|v| v.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%z")
                    .ok()
                    .map(|v| v.with_timezone(&Utc))
            })
            .or_else(|| {
                DateTime::parse_from_str(&format!("{s}+0000"), "%Y-%m-%d %H:%M:%S%z")
                    .ok()
                    .map(|v| v.with_timezone(&Utc))
            })
    };

    for key in ["closedTime", "umaEndDate", "updatedAt"] {
        if let Some(v) = raw.get(key).and_then(Value::as_str).and_then(parse) {
            return Some(v);
        }
    }
    None
}

async fn emit_last_snapshot_signal(state: &AppState, market_id: &str, payload: &Value) -> anyhow::Result<()> {
    let prices = payload
        .get("outcomePrices")
        .and_then(Value::as_array)
        .or_else(|| payload.get("outcome_prices").and_then(Value::as_array));
    let prices = match prices {
        Some(p) if p.len() >= 2 => p,
        _ => return Ok(()),
    };
    let parse = |v: &Value| -> Option<f64> {
        v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    };
    let yes_p = match parse(&prices[0]) {
        Some(p) if p.is_finite() => p,
        _ => return Ok(()),
    };
    let no_p = match parse(&prices[1]) {
        Some(p) if p.is_finite() => p,
        _ => return Ok(()),
    };
    let total = yes_p + no_p;
    if total <= 0.0 {
        return Ok(());
    }
    let yes_share = yes_p / total;
    let no_share = no_p / total;
    let side = if yes_p >= no_p { "YES" } else { "NO" };
    let confidence = yes_share.max(no_share);
    let question = payload.get("question").and_then(Value::as_str).unwrap_or("");

    let signal = json!({
        "event_type": "signal.computed.v1",
        "emitted_at": Utc::now().to_rfc3339(),
        "signal_id": "resolution-market-implied",
        "signal_kind": "market_implied",
        "market_id": market_id,
        "market_question": question,
        "status": "ok",
        "reason": "last_snapshot",
        "sentiment_side": side,
        "sentiment_confidence": confidence,
        "yes_probability": yes_p,
        "no_probability": no_p,
        "yes_share": yes_share,
        "no_share": no_share,
        "margin": (yes_share - no_share).abs(),
    });
    state.nats.publish_json("signal.computed.v1.market_implied", &signal).await?;
    Ok(())
}

fn infer_winning_side(raw: &Value) -> Option<String> {
    let outcomes = raw.get("outcomes").and_then(Value::as_array)?;
    let prices = raw
        .get("outcomePrices")
        .and_then(Value::as_array)
        .or_else(|| raw.get("outcome_prices").and_then(Value::as_array))?;
    if outcomes.len() != 2 || prices.len() != 2 {
        return None;
    }
    let parse_price = |v: &Value| -> Option<f64> {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
    };
    let p0 = parse_price(&prices[0])?;
    let p1 = parse_price(&prices[1])?;
    if !(p0.is_finite() && p1.is_finite()) {
        return None;
    }
    let max_p = p0.max(p1);
    if max_p < 0.95 {
        return None;
    }
    let win_idx = if p0 >= p1 { 0 } else { 1 };
    let outcome_label = outcomes[win_idx].as_str().unwrap_or_default().trim().to_ascii_uppercase();
    if outcome_label == "YES" || outcome_label == "NO" {
        Some(outcome_label)
    } else if win_idx == 0 {
        Some("YES".to_string())
    } else {
        Some("NO".to_string())
    }
}

fn extract_first_market_item(body: &Value) -> Option<Value> {
    if let Some(arr) = body.as_array() {
        return arr.first().cloned();
    }
    let obj = body.as_object()?;
    for key in ["markets", "data", "items", "results"] {
        if let Some(value) = obj.get(key) {
            if let Some(arr) = value.as_array() {
                return arr.first().cloned();
            }
        }
    }
    for value in obj.values() {
        if let Some(arr) = value.as_array() {
            return arr.first().cloned();
        }
    }
    None
}

fn value_as_string_opt(v: &Value, keys: &[&str]) -> Option<String> {
    let obj = v.as_object()?;
    for key in keys {
        if let Some(raw) = obj.get(*key) {
            match raw {
                Value::String(s) if !s.is_empty() => return Some(s.clone()),
                Value::Number(n) => return Some(n.to_string()),
                Value::Bool(b) => return Some(b.to_string()),
                _ => {}
            }
        }
    }
    None
}

fn value_as_f64_opt(v: &Value, keys: &[&str]) -> Option<f64> {
    let obj = v.as_object()?;
    for key in keys {
        if let Some(raw) = obj.get(*key) {
            match raw {
                Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        return Some(f);
                    }
                }
                Value::String(s) => {
                    if let Ok(f) = s.parse::<f64>() {
                        return Some(f);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn value_as_bool_opt(v: &Value, keys: &[&str]) -> Option<bool> {
    let obj = v.as_object()?;
    for key in keys {
        if let Some(raw) = obj.get(*key) {
            match raw {
                Value::Bool(b) => return Some(*b),
                Value::Number(n) => return Some(n.as_i64().unwrap_or(0) != 0),
                Value::String(s) => {
                    let x = s.trim().to_ascii_lowercase();
                    if x == "true" || x == "1" {
                        return Some(true);
                    }
                    if x == "false" || x == "0" {
                        return Some(false);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

async fn consume_predictions(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe("prediction.proposed.v1").await?;
    while let Some(msg) = sub.next().await {
        let payload: PredictionProposedEvent = match state.nats.decode_json(&msg) {
            Ok(v) => v,
            Err(err) => {
                publish_resolution_error(
                    &state,
                    "prediction_decode",
                    err.to_string(),
                    json!({"subject":"prediction.proposed.v1"}),
                )
                .await?;
                continue;
            }
        };

        if let Err(err) = record_prediction(&state, &payload).await {
            publish_resolution_error(
                &state,
                "prediction_record",
                err.to_string(),
                json!({"market_id": payload.market_id, "experiment_id": payload.experiment_id}),
            )
            .await?;
        }
    }
    Ok(())
}

async fn record_prediction(state: &AppState, e: &PredictionProposedEvent) -> anyhow::Result<()> {
    let side = e.side.trim().to_ascii_uppercase();
    if side != "YES" && side != "NO" {
        anyhow::bail!("invalid side in prediction proposal: {}", e.side);
    }
    if !(0.0..=1.0).contains(&e.confidence) {
        anyhow::bail!("invalid confidence in prediction proposal: {}", e.confidence);
    }

    let strategy = e.strategy.clone().unwrap_or_else(|| "nats".to_string());
    let meta = e.meta.clone().unwrap_or_else(|| json!({}));

    sqlx::query(
        r#"
        INSERT INTO experiment_predictions (
            experiment_id, strategy, market_id, market_question, market_end_date_raw,
            side, confidence, horizon_minutes, rationale, meta_json
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        "#,
    )
    .bind(&e.experiment_id)
    .bind(&strategy)
    .bind(&e.market_id)
    .bind(&e.market_question)
    .bind(&e.market_end_date_raw)
    .bind(&side)
    .bind(e.confidence)
    .bind(e.horizon_minutes)
    .bind(&e.rationale)
    .bind(&meta)
    .execute(&state.consumer_db)
    .await?;

    Ok(())
}

async fn consume_signals(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe("signal.computed.v1.*").await?;
    while let Some(msg) = sub.next().await {
        let payload: SignalComputedEvent = match state.nats.decode_json(&msg) {
            Ok(v) => v,
            Err(err) => {
                publish_resolution_error(
                    &state,
                    "signal_decode",
                    err.to_string(),
                    json!({"subject":"signal.computed.v1"}),
                )
                .await?;
                continue;
            }
        };

        let status = payload.status.as_deref().unwrap_or("");
        if status != "ok" {
            continue;
        }

        if let Err(err) = record_signal(&state, &payload).await {
            publish_resolution_error(
                &state,
                "signal_record",
                err.to_string(),
                json!({"market_id": payload.market_id, "signal_kind": payload.signal_kind}),
            )
            .await?;
        }
    }
    Ok(())
}

async fn record_signal(state: &AppState, e: &SignalComputedEvent) -> anyhow::Result<()> {
    let signal_id = e.signal_id.as_deref().unwrap_or("unknown");
    let signal_kind = e.signal_kind.as_deref().unwrap_or("unknown");
    let market_id = e.market_id.as_deref().unwrap_or("");
    if market_id.is_empty() {
        anyhow::bail!("missing market_id in signal.computed.v1");
    }

    sqlx::query(
        r#"
        INSERT INTO signal_outputs (
            signal_id, signal_kind, market_id, market_question,
            sentiment_side, sentiment_confidence, payload_json
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(signal_id)
    .bind(signal_kind)
    .bind(market_id)
    .bind(&e.market_question)
    .bind(&e.sentiment_side)
    .bind(e.sentiment_confidence)
    .bind(&e.full_payload)
    .execute(&state.consumer_db)
    .await?;

    Ok(())
}

async fn publish_resolution_error(
    state: &AppState,
    stage: &str,
    message: String,
    context: Value,
) -> anyhow::Result<()> {
    let occurred_at = Utc::now();
    store_error_event(
        state,
        "resolution.error.v1",
        "resolution",
        stage,
        &message,
        &context,
        None,
        None,
        occurred_at,
    )
    .await?;
    state
        .nats
        .publish_json(
            "resolution.error.v1",
            &ResolutionErrorEvent {
                event_type: "resolution.error.v1",
                emitted_at: occurred_at,
                service: "resolution",
                error_code: stage,
                stage,
                message,
                context,
            },
        )
        .await?;
    Ok(())
}

async fn consume_error_events(state: Arc<AppState>, subject: &str) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe(subject).await?;
    while let Some(msg) = sub.next().await {
        let raw_payload = serde_json::from_slice::<Value>(&msg.payload).ok();
        let parsed = state
            .nats
            .decode_json::<GenericErrorEvent>(&msg)
            .unwrap_or(GenericErrorEvent {
                event_type: None,
                service: None,
                error_code: None,
                message: None,
                context: None,
                occurred_at: None,
                emitted_at: None,
            });
        let event_type = parsed
            .event_type
            .as_deref()
            .unwrap_or(subject);
        let service = parsed
            .service
            .as_deref()
            .unwrap_or("unknown");
        let error_code = parsed
            .error_code
            .as_deref()
            .unwrap_or("unknown");
        let message = parsed
            .message
            .as_deref()
            .unwrap_or("missing error message");
        let context = parsed.context.unwrap_or_else(|| json!({}));
        let occurred_at = parsed
            .occurred_at
            .or(parsed.emitted_at)
            .unwrap_or_else(Utc::now);

        store_error_event(
            &state,
            event_type,
            service,
            error_code,
            message,
            &context,
            Some(subject),
            raw_payload.as_ref(),
            occurred_at,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn store_error_event(
    state: &AppState,
    event_type: &str,
    service: &str,
    error_code: &str,
    message: &str,
    context: &Value,
    source_subject: Option<&str>,
    raw_payload_json: Option<&Value>,
    occurred_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO error_events (
            event_type, service, error_code, message, context, source_subject, raw_payload_json, occurred_at, recorded_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())
        "#,
    )
    .bind(event_type)
    .bind(service)
    .bind(error_code)
    .bind(message)
    .bind(context)
    .bind(source_subject)
    .bind(raw_payload_json)
    .bind(occurred_at)
    .execute(&state.consumer_db)
    .await?;
    Ok(())
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
