use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ParsedMarket {
    pub market_id: String,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub start_date_raw: Option<String>,
    pub end_date_raw: Option<String>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct ParsedEvent {
    pub event_id: String,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub start_date_raw: Option<String>,
    pub end_date_raw: Option<String>,
    pub active: Option<bool>,
    pub raw: Value,
}

pub fn parse_markets_response(body: Value) -> anyhow::Result<Vec<ParsedMarket>> {
    let items = extract_items(body, &["markets", "data", "items", "results"])?;
    let mut markets = Vec::with_capacity(items.len());
    for item in items {
        let id = value_as_string_opt(&item, &["id", "marketId", "conditionId"])
            .or_else(|| value_as_string_opt(&item, &["slug"]))
            .unwrap_or_default();
        if id.is_empty() {
            continue;
        }
        markets.push(ParsedMarket {
            market_id: id,
            slug: value_as_string_opt(&item, &["slug"]),
            question: value_as_string_opt(&item, &["question", "title"]),
            start_date_raw: value_as_string_opt(&item, &["startDate", "start_date", "startTime"]),
            end_date_raw: value_as_string_opt(&item, &["endDate", "end_date", "endTime"]),
            volume: value_as_f64_opt(&item, &["volume", "volumeNum", "volumeUsd"]),
            liquidity: value_as_f64_opt(&item, &["liquidity", "liquidityNum", "liquidityUsd"]),
            active: value_as_bool_opt(&item, &["active", "isActive"]),
            closed: value_as_bool_opt(&item, &["closed", "isClosed"]),
            accepting_orders: value_as_bool_opt(&item, &["acceptingOrders", "accepting_orders"]),
            raw: item,
        });
    }
    Ok(markets)
}

pub fn parse_events_response(body: Value) -> anyhow::Result<Vec<ParsedEvent>> {
    let items = extract_items(body, &["events", "data", "items", "results"])?;
    let mut events = Vec::with_capacity(items.len());
    for item in items {
        let id = value_as_string_opt(&item, &["id", "eventId"])
            .or_else(|| value_as_string_opt(&item, &["slug"]))
            .unwrap_or_default();
        if id.is_empty() {
            continue;
        }
        events.push(ParsedEvent {
            event_id: id,
            slug: value_as_string_opt(&item, &["slug"]),
            title: value_as_string_opt(&item, &["title", "name"]),
            start_date_raw: value_as_string_opt(&item, &["startDate", "start_date", "startTime"]),
            end_date_raw: value_as_string_opt(&item, &["endDate", "end_date", "endTime"]),
            active: value_as_bool_opt(&item, &["active", "isActive"]),
            raw: item,
        });
    }
    Ok(events)
}

fn extract_items(body: Value, preferred_keys: &[&str]) -> anyhow::Result<Vec<Value>> {
    if let Value::Array(arr) = body {
        return Ok(arr);
    }
    let Some(obj) = body.as_object() else {
        anyhow::bail!("unexpected response type; expected array/object");
    };

    for key in preferred_keys {
        if let Some(value) = obj.get(*key) {
            if let Some(arr) = value.as_array() {
                return Ok(arr.clone());
            }
            if let Some(nested_obj) = value.as_object() {
                for nested in nested_obj.values() {
                    if let Some(arr) = nested.as_array() {
                        return Ok(arr.clone());
                    }
                }
            }
        }
    }

    for value in obj.values() {
        if let Some(arr) = value.as_array() {
            return Ok(arr.clone());
        }
    }

    anyhow::bail!("no array payload found in response object")
}

pub fn value_as_string_opt(v: &Value, keys: &[&str]) -> Option<String> {
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

pub fn value_as_f64_opt(v: &Value, keys: &[&str]) -> Option<f64> {
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

pub fn value_as_bool_opt(v: &Value, keys: &[&str]) -> Option<bool> {
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

pub fn extract_resolved_at(raw: &Value) -> Option<DateTime<Utc>> {
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
