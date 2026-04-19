mod consumers;
mod outcome;
mod parse;
mod sweep;

use std::sync::Arc;

use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::types::AppState;

const ERROR_SUBJECTS: &[&str] = &[
    "market.error.v1",
    "signal.error.v1",
    "prediction.error.v1",
    "resolution.error.v1",
];

pub fn start_resolution_loop(state: Arc<AppState>) {
    tokio::spawn(async move {
        if let Err(err) = wait_for_required_tables(&state).await {
            error!(error = %err, "resolution startup dependency check failed");
            return;
        }

        let prediction_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = consumers::consume_predictions(prediction_state).await {
                error!(error = %err, "prediction consumer failed");
            }
        });

        let signal_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = consumers::consume_signals(signal_state).await {
                error!(error = %err, "signal consumer failed");
            }
        });

        let cleanup_state = state.clone();
        tokio::spawn(async move {
            consumers::run_signal_cleanup_loop(cleanup_state).await;
        });

        let scheduler_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = sweep::run_resolution_scheduler(scheduler_state).await {
                error!(error = %err, "resolution scheduler failed");
            }
        });

        for subject in ERROR_SUBJECTS {
            let state_clone = state.clone();
            let subject_name = (*subject).to_string();
            tokio::spawn(async move {
                if let Err(err) = consumers::consume_error_events(state_clone, &subject_name).await {
                    error!(subject = %subject_name, error = %err, "resolution error-event consumer failed");
                }
            });
        }
    });
}

async fn wait_for_required_tables(state: &AppState) -> anyhow::Result<()> {
    let required_tables = [
        "markets",
        "gamma_markets",
        "market_outcomes",
        "market_tracking",
        "experiment_predictions",
        "error_events",
        "signal_outputs",
    ];
    let mut attempts = 0_u32;
    loop {
        let mut missing: Vec<&str> = Vec::new();
        for table in required_tables {
            let regclass_name = format!("public.{table}");
            let exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(regclass_name)
                .fetch_one(&state.db)
                .await?;
            if exists.is_none() {
                missing.push(table);
            }
        }
        if missing.is_empty() {
            info!("resolution startup dependencies are ready");
            return Ok(());
        }
        attempts = attempts.saturating_add(1);
        if attempts % 6 == 1 {
            info!(?missing, "waiting for required tables");
        }
        if attempts >= 120 {
            anyhow::bail!(
                "required tables not ready after {} attempts: {}",
                attempts,
                missing.join(", ")
            );
        }
        time::sleep(Duration::from_secs(5)).await;
    }
}
