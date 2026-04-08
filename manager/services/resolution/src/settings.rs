use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Settings {
    pub manager_port: u16,
    pub database_url: String,
    pub nats_url: String,
    pub gamma_api_base: String,
    pub resolution_sweep_interval_seconds: u64,
    pub resolution_retry_base_minutes: i64,
    pub resolution_retry_max_minutes: i64,
    pub resolution_retry_batch_size: i64,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            manager_port: read_env("MANAGER_PORT", "8080")?.parse().context("invalid MANAGER_PORT")?,
            database_url: read_env(
                "DATABASE_URL",
                "postgresql://polybet:polybet_dev_password@127.0.0.1:5432/polybet",
            )?,
            nats_url: read_env("NATS_URL", "nats://127.0.0.1:4222")?,
            gamma_api_base: read_env("GAMMA_API_BASE", "https://gamma-api.polymarket.com")?,
            resolution_sweep_interval_seconds: read_env("RESOLUTION_SWEEP_INTERVAL_SECONDS", "60")?
                .parse()
                .context("invalid RESOLUTION_SWEEP_INTERVAL_SECONDS")?,
            resolution_retry_base_minutes: read_env("RESOLUTION_RETRY_BASE_MINUTES", "60")?
                .parse()
                .context("invalid RESOLUTION_RETRY_BASE_MINUTES")?,
            resolution_retry_max_minutes: read_env("RESOLUTION_RETRY_MAX_MINUTES", "1440")?
                .parse()
                .context("invalid RESOLUTION_RETRY_MAX_MINUTES")?,
            resolution_retry_batch_size: read_env("RESOLUTION_RETRY_BATCH_SIZE", "200")?
                .parse()
                .context("invalid RESOLUTION_RETRY_BATCH_SIZE")?,
        })
    }
}

fn read_env(key: &str, default: &str) -> anyhow::Result<String> {
    Ok(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}
