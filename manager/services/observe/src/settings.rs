use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Settings {
    pub manager_port: u16,
    pub gamma_api_base: String,
    pub zero_eligible_fail_streak: i64,
    pub database_url: String,
    pub nats_url: String,
    pub launcher_manifest_path: String,
    pub launcher_docker_base: String,
    pub launcher_experiment_image: String,
    pub launcher_experiment_config_bind: String,
}

impl Settings {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            manager_port: read_env("MANAGER_PORT", "8080")?.parse().context("invalid MANAGER_PORT")?,
            gamma_api_base: read_env("GAMMA_API_BASE", "https://gamma-api.polymarket.com")?,
            zero_eligible_fail_streak: read_env("ZERO_ELIGIBLE_FAIL_STREAK", "5")?
                .parse()
                .context("invalid ZERO_ELIGIBLE_FAIL_STREAK")?,
            database_url: read_env(
                "DATABASE_URL",
                "postgresql://polybet:polybet_dev_password@127.0.0.1:5432/polybet",
            )?,
            nats_url: read_env("NATS_URL", "nats://127.0.0.1:4222")?,
            launcher_manifest_path: read_env(
                "LAUNCHER_MANIFEST_PATH",
                "/app/experiment-launcher/manifest.yaml",
            )?,
            launcher_docker_base: read_env("LAUNCHER_DOCKER_BASE", "http://docker-proxy:2375")?,
            launcher_experiment_image: read_env("LAUNCHER_EXPERIMENT_IMAGE", "polybet-experiment:local")?,
            launcher_experiment_config_bind: read_env(
                "LAUNCHER_EXPERIMENT_CONFIG_BIND",
                "G:\\polybet\\experiment\\configs:/app/configs:ro",
            )?,
        })
    }
}

fn read_env(key: &str, default: &str) -> anyhow::Result<String> {
    Ok(std::env::var(key).unwrap_or_else(|_| default.to_string()))
}
