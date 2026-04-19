use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value;
use tracing::info;

use polybet_manager_input::value_as_string_opt;

use crate::types::AppState;

use super::fetch::{dedupe_markets, extract_markets_from_events, fetch_data_health, fetch_events, fetch_markets};
use super::outcome::derive_market_outcome;

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

pub(super) async fn sync_once(
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
                "SELECT 1 FROM market_new_question_emits WHERE market_id = $1",
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
                    event_id: value_as_string_opt(&raw, &["eventId", "event_id"]),
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
