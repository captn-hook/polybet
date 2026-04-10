use std::sync::Arc;

use chrono::Utc;
use futures_util::StreamExt;
use tracing::error;

use crate::types::AppState;

const OBSERVE_SUBJECTS: &[&str] = &[
    "market.new_question.v1",
    "market.resolution.changed.v1",
    "signal.computed.v1.*",
    "prediction.proposed.v1",
    "market.error.v1",
    "signal.error.v1",
    "prediction.error.v1",
    "resolution.error.v1",
];

pub fn start_observe_loop(state: Arc<AppState>) {
    for subject in OBSERVE_SUBJECTS {
        let state_clone = state.clone();
        let subject_name = (*subject).to_string();
        tokio::spawn(async move {
            if let Err(err) = consume_subject(state_clone, subject_name.as_str()).await {
                error!(subject = %subject_name, error = %err, "observe subject consumer failed");
            }
        });
    }
}

async fn consume_subject(state: Arc<AppState>, subject: &str) -> anyhow::Result<()> {
    let mut sub = state.nats.subscribe(subject).await?;
    while let Some(msg) = sub.next().await {
        let payload = std::str::from_utf8(&msg.payload)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<non-utf8-payload>".to_string());
        let now = Utc::now();
        let mut guard = state.observe_events.write().await;
        guard.total_events = guard.total_events.saturating_add(1);
        let entry = guard.topics.entry(subject.to_string()).or_default();
        entry.count = entry.count.saturating_add(1);
        entry.last_received_at = Some(now);
        entry.last_payload_json = Some(payload);
    }
    Ok(())
}
