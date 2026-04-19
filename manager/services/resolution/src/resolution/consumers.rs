use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::types::AppState;

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

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
    /// "last_snapshot" signals are emitted by the resolution service at detection time
    /// and carry outcome-correlated prices — they must not be stored in signal_outputs.
    reason: Option<String>,
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

// ---------------------------------------------------------------------------
// NATS consumers
// ---------------------------------------------------------------------------

pub(super) async fn consume_predictions(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe("prediction.proposed.v1").await?;
    while let Some(msg) = sub.next().await {
        let payload: PredictionProposedEvent = match state.nats.decode_json(&msg) {
            Ok(v) => v,
            Err(err) => {
                publish_resolution_error(
                    &state,
                    "prediction_decode",
                    err.to_string(),
                    json!({"subject": "prediction.proposed.v1"}),
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

pub(super) async fn consume_signals(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe("signal.computed.v1.*").await?;
    while let Some(msg) = sub.next().await {
        let payload: SignalComputedEvent = match state.nats.decode_json(&msg) {
            Ok(v) => v,
            Err(err) => {
                publish_resolution_error(
                    &state,
                    "signal_decode",
                    err.to_string(),
                    json!({"subject": "signal.computed.v1"}),
                )
                .await?;
                continue;
            }
        };

        if payload.status.as_deref() != Some("ok") {
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

pub(super) async fn consume_error_events(state: Arc<AppState>, subject: &str) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe(subject).await?;
    while let Some(msg) = sub.next().await {
        let raw_payload = serde_json::from_slice::<Value>(&msg.payload).ok();
        let parsed = state.nats.decode_json::<GenericErrorEvent>(&msg).unwrap_or(GenericErrorEvent {
            event_type: None,
            service: None,
            error_code: None,
            message: None,
            context: None,
            occurred_at: None,
            emitted_at: None,
        });

        let event_type = parsed.event_type.as_deref().unwrap_or(subject);
        let service = parsed.service.as_deref().unwrap_or("unknown");
        let error_code = parsed.error_code.as_deref().unwrap_or("unknown");
        let message = parsed.message.as_deref().unwrap_or("missing error message");
        let context = parsed.context.unwrap_or_else(|| json!({}));
        let occurred_at = parsed.occurred_at.or(parsed.emitted_at).unwrap_or_else(Utc::now);

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

// ---------------------------------------------------------------------------
// Signal cleanup
// ---------------------------------------------------------------------------

/// Delete signal rows captured after a market's resolved_at. These arise from
/// signal workers polling bootstrap markets (already expired before being tracked)
/// and contaminate the training dataset with outcome-correlated feature values.
/// Runs hourly; the real fix is prevention in record_signal — this is a safety net.
pub(super) async fn run_signal_cleanup_loop(state: Arc<AppState>) {
    let mut ticker = time::interval(Duration::from_secs(3600));
    loop {
        ticker.tick().await;
        match prune_post_resolution_signals(&state).await {
            Ok(n) if n > 0 => info!(pruned = n, "pruned post-resolution signals from signal_outputs"),
            Ok(_) => {}
            Err(err) => error!(error = %err, "signal cleanup failed"),
        }
    }
}

async fn prune_post_resolution_signals(state: &AppState) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        DELETE FROM signal_outputs so
        USING market_outcomes mo
        WHERE so.market_id = mo.market_id
          AND mo.winning_side IN ('YES', 'NO')
          AND so.created_at >= mo.resolved_at
        "#,
    )
    .execute(&state.consumer_db)
    .await?;
    Ok(result.rows_affected())
}

// ---------------------------------------------------------------------------
// DB writers
// ---------------------------------------------------------------------------

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

async fn record_signal(state: &AppState, e: &SignalComputedEvent) -> anyhow::Result<()> {
    let signal_id = e.signal_id.as_deref().unwrap_or("unknown");
    let signal_kind = e.signal_kind.as_deref().unwrap_or("unknown");
    let market_id = e.market_id.as_deref().unwrap_or("");
    if market_id.is_empty() {
        anyhow::bail!("missing market_id in signal.computed.v1");
    }

    // Drop last_snapshot signals — emitted at resolution time with near-final prices,
    // they carry outcome-correlated data and must not enter the training dataset.
    if matches!(e.reason.as_deref(), Some("last_snapshot")) {
        return Ok(());
    }

    // Drop signals for markets that are already resolved. Signal workers continue
    // polling bootstrap markets (added to market_tracking after they'd already expired),
    // so their signals arrive after resolved_at. Storing them contaminates training data.
    let already_resolved: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM market_outcomes WHERE market_id = $1 AND winning_side IN ('YES', 'NO'))",
    )
    .bind(market_id)
    .fetch_one(&state.consumer_db)
    .await?;

    if already_resolved {
        return Ok(());
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

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

pub(super) async fn publish_resolution_error(
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
            event_type, service, error_code, message, context,
            source_subject, raw_payload_json, occurred_at, recorded_at
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
