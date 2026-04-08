use polybet_manager_input::{parse_events_response, parse_markets_response};
use serde_json::json;

#[test]
fn parses_markets_from_top_level_array() {
    let body = json!([
        {
            "id": "m1",
            "slug": "market-1",
            "question": "Will X happen?",
            "volume": "1234.56",
            "liquidity": 88.1,
            "active": "true",
            "closed": false
        }
    ]);

    let parsed = parse_markets_response(body).expect("parse should succeed");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].market_id, "m1");
    assert_eq!(parsed[0].slug.as_deref(), Some("market-1"));
    assert_eq!(parsed[0].volume, Some(1234.56));
    assert_eq!(parsed[0].liquidity, Some(88.1));
    assert_eq!(parsed[0].active, Some(true));
    assert_eq!(parsed[0].closed, Some(false));
}

#[test]
fn parses_markets_from_wrapped_payload() {
    let body = json!({
        "data": {
            "items": [
                {
                    "conditionId": "cond-1",
                    "title": "Fallback title",
                    "isActive": 1,
                    "isClosed": 0
                }
            ]
        }
    });

    let parsed = parse_markets_response(body).expect("parse should succeed");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].market_id, "cond-1");
    assert_eq!(parsed[0].question.as_deref(), Some("Fallback title"));
    assert_eq!(parsed[0].active, Some(true));
    assert_eq!(parsed[0].closed, Some(false));
}

#[test]
fn parses_events_from_wrapped_payload() {
    let body = json!({
        "events": [
            {
                "id": "e1",
                "slug": "event-1",
                "name": "Election Event",
                "startTime": "2026-11-03T00:00:00Z",
                "endTime": "2026-11-04T00:00:00Z",
                "isActive": true
            }
        ]
    });

    let parsed = parse_events_response(body).expect("parse should succeed");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].event_id, "e1");
    assert_eq!(parsed[0].title.as_deref(), Some("Election Event"));
    assert_eq!(parsed[0].active, Some(true));
}

#[test]
fn fails_when_no_array_payload_exists() {
    let body = json!({"status": "ok"});
    let err = parse_markets_response(body).expect_err("parse should fail");
    assert!(err.to_string().contains("no array payload found"));
}

