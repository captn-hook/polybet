use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use polybet_manager_input::{extract_resolved_at, ParsedMarket};

pub(super) fn derive_market_outcome(
    market: &ParsedMarket,
) -> Option<(String, Option<String>, Option<DateTime<Utc>>, Value)> {
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
