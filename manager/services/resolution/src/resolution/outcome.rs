use chrono::{DateTime, Utc};
use polybet_manager_input::extract_resolved_at;
use serde_json::{json, Value};

use crate::types::AppState;

pub(super) fn derive_market_outcome(
    payload: &Value,
    closed: bool,
) -> Option<(String, Option<String>, Option<DateTime<Utc>>, Value)> {
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

pub(super) fn normalized_resolution_status(payload: &Value, closed: bool) -> String {
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

/// Emit a last-snapshot `market_implied` signal just before a market is resolved.
///
/// This publishes to `signal.computed.v1.market_implied` so downstream subscribers
/// can react to the final price snapshot. The signal carries `reason: "last_snapshot"`
/// and is intentionally filtered out of `signal_outputs` by the signal consumer to
/// prevent training-data contamination (prices at resolution are outcome-correlated).
pub(super) async fn emit_last_snapshot_signal(
    state: &AppState,
    market_id: &str,
    payload: &Value,
) -> anyhow::Result<()> {
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
    if p0.max(p1) < 0.95 {
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
