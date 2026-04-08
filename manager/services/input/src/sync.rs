use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::time;
use tracing::{error, info};

use polybet_manager_input::{parse_events_response, parse_markets_response, ParsedEvent, ParsedMarket};

use crate::types::AppState;

#[derive(Debug, Clone, serde::Serialize)]
struct MarketNewQuestionEvent {
    event_type: &'static str,
    market_id: String,
    slug: Option<String>,
    question: String,
    event_id: Option<String>,
    start_date_raw: Option<String>,
    end_date_raw: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    accepting_orders: Option<bool>,
    volume: Option<f64>,
    liquidity: Option<f64>,
    payload: Value,
    emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MarketResolutionChangedEvent {
    event_type: &'static str,
    market_id: String,
    resolution_status: String,
    winning_side: Option<String>,
    resolved_at: Option<DateTime<Utc>>,
    payload: Value,
    emitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MarketErrorEvent<'a> {
    event_type: &'static str,
    emitted_at: DateTime<Utc>,
    service: &'a str,
    error_code: &'a str,
    message: String,
    context: Value,
}

pub fn start_sync_loop(
    state: Arc<AppState>,
    interval_seconds: u64,
    markets_limit: i64,
    events_limit: i64,
    market_max_minutes_to_end: i64,
    zero_eligible_fail_streak: i64,
) {
    tokio::spawn(async move {
        if let Err(err) = sync_once(
            &state,
            markets_limit,
            events_limit,
            market_max_minutes_to_end,
            zero_eligible_fail_streak,
        )
        .await
        {
            error!(error = %err, "initial sync failed");
            let market_error_message = err.to_string();
            if let Err(report_err) = publish_market_error(
                &state,
                "sync.initial_failed",
                market_error_message.clone(),
                json!({
                    "stage": "initial",
                    "service_role": "input",
                }),
            )
            .await
            {
                error!(error = %report_err, "failed to publish market.error.v1");
                return;
            }
            if let Err(record_err) = record_sync_error(&state.db, &market_error_message).await {
                error!(error = %record_err, "failed to record sync error");
                return;
            }
        }

        let mut ticker = time::interval(Duration::from_secs(interval_seconds.max(10)));
        loop {
            ticker.tick().await;
            if let Err(err) = sync_once(
                &state,
                markets_limit,
                events_limit,
                market_max_minutes_to_end,
                zero_eligible_fail_streak,
            )
            .await
            {
                error!(error = %err, "periodic sync failed");
                let market_error_message = err.to_string();
                if let Err(report_err) = publish_market_error(
                    &state,
                    "sync.periodic_failed",
                    market_error_message.clone(),
                    json!({
                        "stage": "periodic",
                        "service_role": "input",
                    }),
                )
                .await
                {
                    error!(error = %report_err, "failed to publish market.error.v1");
                    return;
                }
                if let Err(record_err) = record_sync_error(&state.db, &market_error_message).await {
                    error!(error = %record_err, "failed to record sync error");
                    return;
                }
            }
        }
    });
}

async fn sync_once(
    state: &AppState,
    markets_limit: i64,
    events_limit: i64,
    market_max_minutes_to_end: i64,
    zero_eligible_fail_streak: i64,
) -> anyhow::Result<()> {
    let mut markets = fetch_markets(&state.client, &state.gamma_base, markets_limit).await?;
    let events = fetch_events(&state.client, &state.gamma_base, events_limit).await?;
    markets.extend(extract_markets_from_events(&events));
    let markets = dedupe_markets(markets);
    let now = Utc::now();
    let cutoff = now + ChronoDuration::minutes(market_max_minutes_to_end.max(1));

    let mut tx = state.db.begin().await?;

    let mut markets_synced = 0_i64;
    let mut outcomes_synced = 0_i64;
    let mut eligible_markets = 0_i64;
    let mut new_question_candidates: Vec<MarketNewQuestionEvent> = Vec::new();
    for market in markets {
        let market_id = market.market_id.clone();
        let slug = market.slug.clone();
        let question = market.question.clone();
        let start_date_raw = market.start_date_raw.clone();
        let end_date_raw = market.end_date_raw.clone();
        let volume = market.volume;
        let liquidity = market.liquidity;
        let active = market.active;
        let closed = market.closed;
        let accepting_orders = market.accepting_orders;
        let raw = market.raw.clone();

        let start_utc = start_date_raw
            .as_deref()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| v.with_timezone(&Utc));
        let end_utc = end_date_raw
            .as_deref()
            .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
            .map(|v| v.with_timezone(&Utc));
        let is_eligible = active.unwrap_or(false)
            && !closed.unwrap_or(false)
            && accepting_orders.unwrap_or(false)
            && start_utc.map(|ts| ts <= now).unwrap_or(true)
            && end_utc.map(|ts| ts > now && ts <= cutoff).unwrap_or(false);

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
        .bind(&market_id)
        .bind(&slug)
        .bind(&question)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(volume)
        .bind(liquidity)
        .bind(active)
        .bind(closed)
        .bind(accepting_orders)
        .bind(is_eligible)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO markets (
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
        .bind(&market_id)
        .bind(&slug)
        .bind(&question)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(volume)
        .bind(liquidity)
        .bind(active)
        .bind(closed)
        .bind(accepting_orders)
        .bind(is_eligible)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;

        if let Some((resolution_status, winning_side, resolved_at, payload_json)) =
            derive_market_outcome(&market)
        {
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
            .bind("gamma_markets")
            .bind(payload_json)
            .execute(&mut *tx)
            .await?;
            outcomes_synced += 1;
        }

        sqlx::query(
            r#"
            INSERT INTO market_snapshots (
                market_id, observed_at, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json
            ) VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(&market_id)
        .bind(&slug)
        .bind(&question)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(volume)
        .bind(liquidity)
        .bind(active)
        .bind(closed)
        .bind(accepting_orders)
        .bind(is_eligible)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;
        markets_synced += 1;
        if is_eligible {
            eligible_markets += 1;
            let already_emitted: Option<i32> = sqlx::query_scalar(
                r#"
                SELECT 1
                FROM market_new_question_emits
                WHERE market_id = $1
                "#,
            )
            .bind(&market_id)
            .fetch_optional(&mut *tx)
            .await?;
            if already_emitted.is_none() {
                new_question_candidates.push(MarketNewQuestionEvent {
                    event_type: "market.new_question.v1",
                    market_id: market_id.clone(),
                    slug: slug.clone(),
                    question: question.clone().unwrap_or_default(),
                    event_id: read_string_key(&raw, &["eventId", "event_id"]),
                    start_date_raw: start_date_raw.clone(),
                    end_date_raw: end_date_raw.clone(),
                    active,
                    closed,
                    accepting_orders,
                    volume,
                    liquidity,
                    payload: raw.clone(),
                    emitted_at: Utc::now(),
                });
            }
        }
    }

    let mut events_synced = 0_i64;
    for event in events {
        let event_id = event.event_id.clone();
        let slug = event.slug.clone();
        let title = event.title.clone();
        let start_date_raw = event.start_date_raw.clone();
        let end_date_raw = event.end_date_raw.clone();
        let active = event.active;
        let raw = event.raw.clone();

        sqlx::query(
            r#"
            INSERT INTO gamma_events (
                event_id, slug, title, start_date_raw, end_date_raw, active, payload_json, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,now())
            ON CONFLICT (event_id) DO UPDATE SET
                slug = excluded.slug,
                title = excluded.title,
                start_date_raw = excluded.start_date_raw,
                end_date_raw = excluded.end_date_raw,
                active = excluded.active,
                payload_json = excluded.payload_json,
                updated_at = now()
            "#,
        )
        .bind(&event_id)
        .bind(&slug)
        .bind(&title)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(active)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO events (
                event_id, slug, title, start_date_raw, end_date_raw, active, payload_json, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,now())
            ON CONFLICT (event_id) DO UPDATE SET
                slug = excluded.slug,
                title = excluded.title,
                start_date_raw = excluded.start_date_raw,
                end_date_raw = excluded.end_date_raw,
                active = excluded.active,
                payload_json = excluded.payload_json,
                updated_at = now()
            "#,
        )
        .bind(&event_id)
        .bind(&slug)
        .bind(&title)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(active)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO event_snapshots (
                event_id, observed_at, slug, title, start_date_raw, end_date_raw, active, payload_json
            ) VALUES ($1, now(), $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&event_id)
        .bind(&slug)
        .bind(&title)
        .bind(&start_date_raw)
        .bind(&end_date_raw)
        .bind(active)
        .bind(&raw)
        .execute(&mut *tx)
        .await?;
        events_synced += 1;
    }

    sqlx::query(
        r#"
        UPDATE market_outcomes mo
        SET
            winning_side = CASE
                WHEN gm.payload_json->>'outcomePrices' IS NULL OR gm.payload_json->>'outcomePrices' = '' THEN mo.winning_side
                WHEN (
                    (gm.payload_json->>'outcomePrices')::jsonb->>0
                )::double precision >= (
                    (gm.payload_json->>'outcomePrices')::jsonb->>1
                )::double precision THEN 'YES'
                ELSE 'NO'
            END,
            updated_at = now()
        FROM gamma_markets gm
        WHERE mo.market_id = gm.market_id
          AND mo.resolution_status = 'resolved'
          AND mo.winning_side IS NULL
          AND gm.payload_json->>'outcomePrices' IS NOT NULL
          AND gm.payload_json->>'outcomes' IS NOT NULL
          AND jsonb_array_length((gm.payload_json->>'outcomePrices')::jsonb) = 2
          AND jsonb_array_length((gm.payload_json->>'outcomes')::jsonb) = 2
        "#,
    )
    .execute(&mut *tx)
    .await?;

    let data_health =
        fetch_data_health(&state.client, &state.data_base).await.unwrap_or_else(|e| format!("unreachable: {e}"));

    sqlx::query(
        r#"
        INSERT INTO manager_sync_status (
            id, last_success, markets_synced, eligible_markets, zero_eligible_streak, events_synced, data_api_health, last_error, updated_at
        ) VALUES (1, now(), $1, $2, CASE WHEN $2 = 0 THEN 1 ELSE 0 END, $3, $4, NULL, now())
        ON CONFLICT (id) DO UPDATE SET
            last_success = now(),
            markets_synced = excluded.markets_synced,
            eligible_markets = excluded.eligible_markets,
            zero_eligible_streak = CASE
                WHEN excluded.eligible_markets = 0 THEN manager_sync_status.zero_eligible_streak + 1
                ELSE 0
            END,
            events_synced = excluded.events_synced,
            data_api_health = excluded.data_api_health,
            last_error = NULL,
            updated_at = now()
        "#,
    )
    .bind(markets_synced)
    .bind(eligible_markets)
    .bind(events_synced)
    .bind(data_health)
    .execute(&mut *tx)
    .await?;

    if eligible_markets == 0 && zero_eligible_fail_streak > 0 {
        let streak: (i32,) = sqlx::query_as("SELECT zero_eligible_streak FROM manager_sync_status WHERE id = 1")
            .fetch_one(&mut *tx)
            .await?;
        if i64::from(streak.0) >= zero_eligible_fail_streak {
            anyhow::bail!(
                "eligible market pool empty for {} consecutive syncs (threshold={})",
                streak.0,
                zero_eligible_fail_streak
            );
        }
    }

    tx.commit().await?;

    let mut new_questions_emitted = 0_i64;
    for event in new_question_candidates {
        state
            .nats
            .publish_json("market.new_question.v1", &event)
            .await?;
        sqlx::query(
            r#"
            INSERT INTO market_new_question_emits (market_id, first_emitted_at)
            VALUES ($1, now())
            ON CONFLICT (market_id) DO NOTHING
            "#,
        )
        .bind(&event.market_id)
        .execute(&state.db)
        .await?;
        info!(market_id = %event.market_id, "published market.new_question.v1");
        new_questions_emitted += 1;
    }

    let resolution_events = sqlx::query_as::<_, (String, String, Option<String>, Option<DateTime<Utc>>, Value)>(
        r#"
        SELECT market_id, resolution_status, winning_side, resolved_at, payload_json
        FROM market_outcomes
        WHERE updated_at > now() - interval '10 minutes'
        "#,
    )
    .fetch_all(&state.db)
    .await?;
    for (market_id, resolution_status, winning_side, resolved_at, payload_json) in resolution_events {
        let emitted = sqlx::query_scalar::<_, Option<i32>>(
            "SELECT 1 FROM market_new_question_emits WHERE market_id = $1",
        )
        .bind(&market_id)
        .fetch_optional(&state.db)
        .await?;
        if emitted.is_none() {
            continue;
        }
        state
            .nats
            .publish_json(
                "market.resolution.changed.v1",
                &MarketResolutionChangedEvent {
                    event_type: "market.resolution.changed.v1",
                    market_id,
                    resolution_status,
                    winning_side,
                    resolved_at,
                    payload: payload_json,
                    emitted_at: Utc::now(),
                },
            )
            .await?;
    }

    info!(
        markets_synced,
        eligible_markets,
        outcomes_synced,
        events_synced,
        new_questions_emitted,
        market_max_minutes_to_end,
        "sync completed"
    );
    Ok(())
}

fn derive_market_outcome(market: &ParsedMarket) -> Option<(String, Option<String>, Option<DateTime<Utc>>, Value)> {
    let is_closed = market.closed.unwrap_or(false);
    let uma_status = market
        .raw
        .get("umaResolutionStatus")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase());

    let is_resolved = uma_status
        .as_deref()
        .map(|s| matches!(s, "resolved" | "finalized" | "settled"))
        .unwrap_or(false);

    if !is_closed && !is_resolved {
        return None;
    }

    let winning_side = infer_winning_side(&market.raw);
    let resolution_status = match uma_status {
        Some(status) => status,
        None if winning_side.is_some() => "resolved".to_string(),
        None if is_closed => "closed".to_string(),
        None => "unknown".to_string(),
    };
    let resolved_at = extract_resolved_at(&market.raw);
    let payload = json!({
        "closed": market.closed,
        "active": market.active,
        "accepting_orders": market.accepting_orders,
        "end_date_raw": market.end_date_raw,
        "uma_resolution_status": market.raw.get("umaResolutionStatus"),
        "closed_time": market.raw.get("closedTime"),
        "uma_end_date": market.raw.get("umaEndDate"),
        "updated_at_raw": market.raw.get("updatedAt"),
        "outcomes": market.raw.get("outcomes"),
        "outcome_prices": market.raw.get("outcomePrices"),
    });

    Some((resolution_status, winning_side, resolved_at, payload))
}

fn infer_winning_side(raw: &Value) -> Option<String> {
    let labels = parse_outcome_labels(raw)?;
    let prices = parse_outcome_prices(raw)?;
    if labels.len() < 2 || prices.len() < 2 || labels.len() != prices.len() {
        return None;
    }

    let (winner_idx, winner_price) = prices
        .iter()
        .copied()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))?;
    if winner_price < 0.95 {
        return None;
    }

    let winner_label = labels.get(winner_idx)?.trim().to_ascii_uppercase();
    if winner_label == "YES" {
        return Some("YES".to_string());
    }
    if winner_label == "NO" {
        return Some("NO".to_string());
    }
    if labels.len() == 2 {
        return Some(if winner_idx == 0 { "YES" } else { "NO" }.to_string());
    }
    None
}

fn parse_outcome_labels(raw: &Value) -> Option<Vec<String>> {
    let value = raw.get("outcomes")?;
    parse_string_array(value)
}

fn parse_outcome_prices(raw: &Value) -> Option<Vec<f64>> {
    let value = raw.get("outcomePrices")?;
    let items = parse_string_array(value)?;
    let mut out = Vec::with_capacity(items.len());
    for s in items {
        let parsed = s.parse::<f64>().ok()?;
        out.push(parsed);
    }
    Some(out)
}

fn parse_string_array(value: &Value) -> Option<Vec<String>> {
    if let Some(arr) = value.as_array() {
        let parsed = arr
            .iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .collect::<Vec<_>>();
        return if parsed.is_empty() { None } else { Some(parsed) };
    }

    if let Some(raw) = value.as_str() {
        let parsed_json: Value = serde_json::from_str(raw).ok()?;
        if let Some(arr) = parsed_json.as_array() {
            let parsed = arr
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    Value::Number(n) => Some(n.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            return if parsed.is_empty() { None } else { Some(parsed) };
        }
    }

    None
}

fn read_string_key(raw: &Value, keys: &[&str]) -> Option<String> {
    let obj = raw.as_object()?;
    for key in keys {
        if let Some(value) = obj.get(*key) {
            match value {
                Value::String(s) if !s.trim().is_empty() => return Some(s.clone()),
                Value::Number(n) => return Some(n.to_string()),
                _ => {}
            }
        }
    }
    None
}

fn extract_resolved_at(raw: &Value) -> Option<DateTime<Utc>> {
    let parse = |s: &str| {
        let normalized = if s.len() >= 3 {
            let tz = &s[s.len() - 3..];
            if (tz.starts_with('+') || tz.starts_with('-')) && tz[1..].chars().all(|c| c.is_ascii_digit()) {
                format!("{}00", s)
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

async fn fetch_markets(client: &Client, gamma_base: &str, limit: i64) -> anyhow::Result<Vec<ParsedMarket>> {
    let base = gamma_base.trim_end_matches('/');
    let page_size = limit.clamp(10, 100);
    let max_pages = 10_i64;
    let query_variants = [
        ("active=true&closed=false&acceptingOrders=true", "id", "false"),
        ("active=true&closed=false&acceptingOrders=true", "endDate", "true"),
        ("active=true&closed=true", "id", "false"),
        ("active=true&closed=true", "endDate", "true"),
        ("active=false&closed=true", "id", "false"),
        ("active=false&closed=true", "endDate", "true"),
    ];

    let mut all_markets = Vec::new();
    for (filters, order, ascending) in query_variants {
        for page in 0..max_pages {
            let offset = page * page_size;
            let url = format!(
                "{}/markets?limit={}&offset={}&{}&order={}&ascending={}",
                base, page_size, offset, filters, order, ascending
            );
            let res = client.get(url).send().await?.error_for_status()?;
            let body = res.json::<Value>().await?;
            let page_markets = parse_markets_response(body)?;
            if page_markets.is_empty() {
                break;
            }
            all_markets.extend(page_markets);
        }
    }

    Ok(dedupe_markets(all_markets))
}

async fn fetch_events(client: &Client, gamma_base: &str, limit: i64) -> anyhow::Result<Vec<ParsedEvent>> {
    let base = gamma_base.trim_end_matches('/');
    let page_size = limit.clamp(10, 100);
    let max_pages = 10_i64;
    let query_variants = [
        // Prefer latest IDs to catch rolling short-window markets (e.g. btc-updown-5m)
        "active=true&closed=false&order=id&ascending=false",
        // Keep the old earliest-endDate scan so long-horizon active events are still represented
        "active=true&closed=false&order=endDate&ascending=true",
    ];

    let mut all_events = Vec::new();
    for filters in query_variants {
        for page in 0..max_pages {
            let offset = page * page_size;
            let url = format!(
                "{}/events?{}&limit={}&offset={}",
                base, filters, page_size, offset
            );
            let res = client.get(url).send().await?.error_for_status()?;
            let body = res.json::<Value>().await?;
            let page_events = parse_events_response(body)?;
            if page_events.is_empty() {
                break;
            }
            all_events.extend(page_events);
        }
    }

    let mut deduped: HashMap<String, ParsedEvent> = HashMap::new();
    for event in all_events {
        if event.event_id.is_empty() {
            continue;
        }
        deduped.entry(event.event_id.clone()).or_insert(event);
    }
    Ok(deduped.into_values().collect())
}

fn extract_markets_from_events(events: &[ParsedEvent]) -> Vec<ParsedMarket> {
    let mut out = Vec::new();
    for event in events {
        let Some(markets) = event.raw.get("markets").and_then(|m| m.as_array()) else {
            continue;
        };
        if let Ok(parsed) = parse_markets_response(Value::Array(markets.clone())) {
            out.extend(parsed);
        }
    }
    out
}

fn dedupe_markets(markets: Vec<ParsedMarket>) -> Vec<ParsedMarket> {
    let mut map: HashMap<String, ParsedMarket> = HashMap::new();
    for market in markets {
        if market.market_id.is_empty() {
            continue;
        }
        match map.get(&market.market_id) {
            Some(existing) => {
                let existing_score = (existing.accepting_orders.unwrap_or(false) as i32)
                    + (existing.active.unwrap_or(false) as i32)
                    + ((!existing.closed.unwrap_or(true)) as i32);
                let new_score = (market.accepting_orders.unwrap_or(false) as i32)
                    + (market.active.unwrap_or(false) as i32)
                    + ((!market.closed.unwrap_or(true)) as i32);
                if new_score >= existing_score {
                    map.insert(market.market_id.clone(), market);
                }
            }
            None => {
                map.insert(market.market_id.clone(), market);
            }
        }
    }
    map.into_values().collect()
}

async fn fetch_data_health(client: &Client, data_base: &str) -> anyhow::Result<String> {
    let url = format!("{}/", data_base.trim_end_matches('/'));
    let res = client.get(url).send().await?;
    Ok(format!(
        "{} {}",
        res.status().as_u16(),
        res.status().canonical_reason().unwrap_or("unknown")
    ))
}

async fn record_sync_error(pool: &sqlx::PgPool, message: &str) -> anyhow::Result<()> {
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

async fn publish_market_error(
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::infer_winning_side;

    #[test]
    fn infers_yes_no_from_explicit_labels() {
        let raw = json!({
            "outcomes": ["YES", "NO"],
            "outcomePrices": ["0.99", "0.01"]
        });
        assert_eq!(infer_winning_side(&raw).as_deref(), Some("YES"));
    }

    #[test]
    fn infers_yes_no_from_binary_non_standard_labels() {
        let raw = json!({
            "outcomes": ["Over 2.5", "Under 2.5"],
            "outcomePrices": ["0.02", "0.98"]
        });
        assert_eq!(infer_winning_side(&raw).as_deref(), Some("NO"));
    }

    #[test]
    fn does_not_infer_when_confidence_threshold_not_met() {
        let raw = json!({
            "outcomes": ["YES", "NO"],
            "outcomePrices": ["0.60", "0.40"]
        });
        assert_eq!(infer_winning_side(&raw), None);
    }

    #[test]
    fn does_not_infer_when_outcome_vectors_mismatch() {
        let raw = json!({
            "outcomes": ["YES", "NO"],
            "outcomePrices": ["0.99"]
        });
        assert_eq!(infer_winning_side(&raw), None);
    }
}
