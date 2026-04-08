use polybet_manager_input::{parse_events_response, parse_markets_response};
use serde_json::json;

#[test]
fn status_shape_contract_examples_are_parseable() {
    let fake_status = json!({
        "ok": true,
        "timestamp": "2026-04-07T16:06:19.463893+00:00",
        "alerts": [],
        "sync": {
            "last_success": "2026-04-07T16:06:10+00:00",
            "last_success_age_seconds": 9,
            "markets_synced_last_run": 120,
            "eligible_markets_last_run": 60,
            "zero_eligible_streak": 0,
            "events_synced_last_run": 20,
            "last_error": null
        },
        "launcher": {
            "last_success": "2026-04-07T16:06:15+00:00",
            "last_success_age_seconds": 4,
            "desired_instances": 7,
            "running_instances": 7,
            "launched_last_run": 0,
            "stopped_last_run": 0,
            "last_error": null
        },
        "counts": {
            "markets_total": 1000,
            "markets_open": 300,
            "markets_open_accepting_orders": 280,
            "markets_eligible": 60,
            "events_total": 200,
            "signals_total": 9500,
            "predictions_total": 18000,
            "experiments_distinct": 7,
            "outcomes_total": 120,
            "outcomes_with_winner": 115,
            "outstanding_latest_bets": 220,
            "resolved_latest_bets": 480
        },
        "freshness": {
            "latest_signal": {
                "side": "YES",
                "market_id": "123",
                "confidence": 0.73,
                "created_at": "2026-04-07T16:06:18+00:00",
                "age_seconds": 1
            },
            "last_prediction_at": "2026-04-07T16:06:18+00:00",
            "last_prediction_age_seconds": 1
        },
        "database": {
            "connections_current_db": 12
        },
        "top_experiments": [
            {
                "experiment_id": "exp-follow-signal-sentiment",
                "prediction_count": 3000,
                "avg_confidence": 0.81,
                "yes_rate": 0.54,
                "last_prediction": "2026-04-07T16:06:18+00:00",
                "last_prediction_age_seconds": 1
            }
        ]
    });

    assert!(fake_status.get("ok").is_some());
    assert!(fake_status.get("sync").is_some());
    assert!(fake_status.get("launcher").is_some());
    assert!(fake_status.get("counts").is_some());
    assert!(fake_status.get("freshness").is_some());
    assert!(fake_status.get("database").is_some());
    assert!(fake_status.get("top_experiments").is_some());
}

#[test]
fn existing_gamma_parsers_still_accept_arrays_and_wrappers() {
    let markets = parse_markets_response(json!([{
        "id": "m1",
        "question": "Will this resolve yes?",
        "active": true,
        "closed": false
    }]))
    .expect("markets parse");
    assert_eq!(markets.len(), 1);

    let events = parse_events_response(json!({
        "events": [{
            "id": "e1",
            "name": "Event One",
            "isActive": true
        }]
    }))
    .expect("events parse");
    assert_eq!(events.len(), 1);
}
