use std::collections::HashMap;

use reqwest::Client;
use serde_json::Value;

use polybet_manager_input::{parse_events_response, parse_markets_response, ParsedEvent, ParsedMarket};

pub(super) async fn fetch_markets(
    client: &Client,
    gamma_base: &str,
    limit: i64,
) -> anyhow::Result<Vec<ParsedMarket>> {
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

pub(super) async fn fetch_events(
    client: &Client,
    gamma_base: &str,
    limit: i64,
) -> anyhow::Result<Vec<ParsedEvent>> {
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

pub(super) fn extract_markets_from_events(events: &[ParsedEvent]) -> Vec<ParsedMarket> {
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

pub(super) fn dedupe_markets(markets: Vec<ParsedMarket>) -> Vec<ParsedMarket> {
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

pub(super) async fn fetch_data_health(client: &Client, data_base: &str) -> anyhow::Result<String> {
    let url = format!("{}/", data_base.trim_end_matches('/'));
    let res = client.get(url).send().await?;
    Ok(format!(
        "{} {}",
        res.status().as_u16(),
        res.status().canonical_reason().unwrap_or("unknown")
    ))
}
