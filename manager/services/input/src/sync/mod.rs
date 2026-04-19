mod error;
mod fetch;
mod outcome;
mod run;

use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::time;
use tracing::error;

use crate::types::AppState;

use self::error::{publish_market_error, record_sync_error};
use self::run::sync_once;

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
                json!({ "stage": "initial", "service_role": "input" }),
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
                    json!({ "stage": "periodic", "service_role": "input" }),
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
