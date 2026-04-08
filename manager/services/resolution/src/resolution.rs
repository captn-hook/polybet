use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::error;

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
struct MarketResolutionChangedEvent {
    market_id: String,
    resolution_status: String,
    winning_side: Option<String>,
    resolved_at: Option<DateTime<Utc>>,
    payload: Option<Value>,
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
    let prediction_state = state.clone();
    let resolution_state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = consume_predictions(prediction_state).await {
            error!(error = %err, "prediction consumer failed");
        }
    });

    tokio::spawn(async move {
        if let Err(err) = consume_resolution_changes(resolution_state).await {
            error!(error = %err, "resolution consumer failed");
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

async fn consume_resolution_changes(state: Arc<AppState>) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe("market.resolution.changed.v1").await?;
    while let Some(msg) = sub.next().await {
        let payload: MarketResolutionChangedEvent = match state.nats.decode_json(&msg) {
            Ok(v) => v,
            Err(err) => {
                publish_resolution_error(
                    &state,
                    "resolution_decode",
                    err.to_string(),
                    json!({"subject":"market.resolution.changed.v1"}),
                )
                .await?;
                continue;
            }
        };
        if let Err(err) = apply_resolution_update(&state, &payload).await {
            publish_resolution_error(
                &state,
                "resolution_apply",
                err.to_string(),
                json!({"market_id": payload.market_id}),
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
    .execute(&state.db)
    .await?;

    Ok(())
}

async fn apply_resolution_update(state: &AppState, e: &MarketResolutionChangedEvent) -> anyhow::Result<()> {
    let updated_payload = e.payload.clone().unwrap_or_else(|| json!({}));
    sqlx::query(
        r#"
        INSERT INTO market_outcomes (market_id, resolution_status, winning_side, resolved_at, source, payload_json, updated_at)
        VALUES ($1,$2,$3,$4,'nats_resolution',$5,now())
        ON CONFLICT (market_id) DO UPDATE SET
            resolution_status = excluded.resolution_status,
            winning_side = excluded.winning_side,
            resolved_at = COALESCE(excluded.resolved_at, market_outcomes.resolved_at),
            source = excluded.source,
            payload_json = excluded.payload_json,
            updated_at = now()
        "#,
    )
    .bind(&e.market_id)
    .bind(&e.resolution_status)
    .bind(&e.winning_side)
    .bind(e.resolved_at)
    .bind(&updated_payload)
    .execute(&state.db)
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
    .execute(&state.db)
    .await?;
    Ok(())
}
