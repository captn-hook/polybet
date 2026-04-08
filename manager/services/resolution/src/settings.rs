use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Settings {
    pub manager_port: u16,
    pub database_url: String,
    pub nats_url: String,
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
        })
    }
}

fn read_env(key: &str, default: &str) -> anyhow::Result<String> {
    Ok(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}
