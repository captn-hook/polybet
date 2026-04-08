use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Settings {
    pub manager_port: u16,
    pub gamma_api_base: String,
    pub data_api_base: String,
    pub sync_interval_seconds: u64,
    pub top_markets_limit: i64,
    pub top_events_limit: i64,
    pub market_max_minutes_to_end: i64,
    pub zero_eligible_fail_streak: i64,
    pub database_url: String,
    pub nats_url: String,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            manager_port: read_env("MANAGER_PORT", "8080")?.parse().context("invalid MANAGER_PORT")?,
            gamma_api_base: read_env("GAMMA_API_BASE", "https://gamma-api.polymarket.com")?,
            data_api_base: read_env("DATA_API_BASE", "https://data-api.polymarket.com")?,
            sync_interval_seconds: read_env("SYNC_INTERVAL_SECONDS", "120")?
                .parse()
                .context("invalid SYNC_INTERVAL_SECONDS")?,
            top_markets_limit: read_env("TOP_MARKETS_LIMIT", "100")?
                .parse()
                .context("invalid TOP_MARKETS_LIMIT")?,
            top_events_limit: read_env("TOP_EVENTS_LIMIT", "100")?
                .parse()
                .context("invalid TOP_EVENTS_LIMIT")?,
            market_max_minutes_to_end: read_env("MARKET_MAX_MINUTES_TO_END", "10")?
                .parse()
                .context("invalid MARKET_MAX_MINUTES_TO_END")?,
            zero_eligible_fail_streak: read_env("ZERO_ELIGIBLE_FAIL_STREAK", "5")?
                .parse()
                .context("invalid ZERO_ELIGIBLE_FAIL_STREAK")?,
            database_url: read_env(
                "DATABASE_URL",
                "postgresql://polybet:polybet_dev_password@127.0.0.1:5432/polybet",
            )?,
            nats_url: read_env("NATS_URL", "nats://127.0.0.1:4222")?,
        })
    }
}

fn read_env(key: &str, default: &str) -> anyhow::Result<String> {
    Ok(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}
