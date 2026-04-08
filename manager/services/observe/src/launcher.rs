use std::{collections::HashSet, sync::Arc};

use anyhow::Context;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::time;
use tracing::error;

use crate::types::{AppState, LaunchResult};

#[derive(Debug, Deserialize)]
struct LauncherManifest {
    experiments: Vec<ExperimentSpec>,
}

#[derive(Debug, Deserialize)]
struct ExperimentSpec {
    name: String,
    config: String,
    #[serde(default)]
    module: Option<String>,
    #[serde(default)]
    seeds: Option<Vec<i64>>,
}

#[derive(Clone)]
struct DesiredInstance {
    container_name: String,
    experiment_name: String,
    seed: Option<i64>,
    config_path: String,
    module: Option<String>,
}

struct ExistingContainer {
    id: String,
    name: String,
    state: String,
}

#[derive(Deserialize)]
struct DockerContainerListItem {
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Names")]
    names: Vec<String>,
    #[serde(rename = "State")]
    state: Option<String>,
}

#[derive(Serialize)]
struct DockerCreateReq {
    #[serde(rename = "Image")]
    image: String,
    #[serde(rename = "Env")]
    env: Vec<String>,
    #[serde(rename = "Labels")]
    labels: serde_json::Value,
    #[serde(rename = "HostConfig")]
    host_config: serde_json::Value,
}

#[derive(Deserialize)]
struct DockerCreateResp {
    #[serde(rename = "Id")]
    id: String,
}

pub fn start_launcher_loop(state: Arc<AppState>, interval_seconds: u64) {
    tokio::spawn(async move {
        let mut ticker = time::interval(std::time::Duration::from_secs(interval_seconds.max(5)));
        loop {
            ticker.tick().await;
            if let Err(err) = reconcile_launcher(&state).await {
                error!(error = %err, "launcher reconcile failed");
                if let Err(record_err) = record_launcher_error(&state.db, &err.to_string()).await {
                    error!(error = %record_err, "failed to persist launcher error");
                    return;
                }
            }
        }
    });
}

pub async fn reconcile_launcher(state: &AppState) -> anyhow::Result<LaunchResult> {
    let manifest_text = tokio::fs::read_to_string(&state.launcher_manifest_path)
        .await
        .with_context(|| format!("failed reading launcher manifest: {}", state.launcher_manifest_path))?;
    let manifest: LauncherManifest = serde_yaml::from_str(&manifest_text).context("invalid launcher manifest yaml")?;

    let mut desired = Vec::<DesiredInstance>::new();
    for exp in manifest.experiments {
        if exp.name.trim().is_empty() {
            continue;
        }
        let seeds = exp.seeds.unwrap_or_default();
        if seeds.is_empty() {
            desired.push(DesiredInstance {
                container_name: exp.name.clone(),
                experiment_name: exp.name.clone(),
                seed: None,
                config_path: exp.config.clone(),
                module: exp.module.clone(),
            });
        } else {
            for seed in seeds {
                desired.push(DesiredInstance {
                    container_name: format!("{}_{}", exp.name, seed),
                    experiment_name: exp.name.clone(),
                    seed: Some(seed),
                    config_path: exp.config.clone(),
                    module: exp.module.clone(),
                });
            }
        }
    }

    let desired_names: HashSet<String> = desired.iter().map(|x| x.container_name.clone()).collect();
    let docker_base = state.launcher_docker_base.as_str();

    let existing = list_existing_launcher_containers(&state.client, docker_base).await?;
    let existing_names: HashSet<String> = existing.iter().map(|x| x.name.clone()).collect();

    let mut launched = 0usize;
    let mut stopped = 0usize;

    for inst in &desired {
        if !existing_names.contains(&inst.container_name) {
            create_and_start_container(
                &state.client,
                docker_base,
                inst,
                &state.launcher_experiment_image,
                &state.launcher_experiment_config_bind,
            )
            .await?;
            launched += 1;
            continue;
        }

        if let Some(ex) = existing.iter().find(|x| x.name == inst.container_name) {
            if ex.state != "running" && start_container(&state.client, docker_base, &ex.id).await.is_err() {
                stop_and_remove_container(&state.client, docker_base, &ex.id).await?;
                create_and_start_container(
                    &state.client,
                    docker_base,
                    inst,
                    &state.launcher_experiment_image,
                    &state.launcher_experiment_config_bind,
                )
                .await?;
                launched += 1;
            }
        }
    }

    for ex in existing {
        if !desired_names.contains(&ex.name) {
            stop_and_remove_container(&state.client, docker_base, &ex.id).await?;
            stopped += 1;
        }
    }

    let refreshed = list_existing_launcher_containers(&state.client, docker_base).await?;
    let state_by_name: std::collections::HashMap<String, String> =
        refreshed.into_iter().map(|c| (c.name, c.state)).collect();
    let running = desired
        .iter()
        .filter(|d| state_by_name.get(&d.container_name).map(|s| s == "running").unwrap_or(false))
        .count();

    write_launcher_instances(&state.db, &desired, &state_by_name).await?;
    write_launcher_status(
        &state.db,
        desired.len() as i64,
        running as i64,
        launched as i64,
        stopped as i64,
    )
    .await?;

    Ok(LaunchResult {
        launched,
        stopped,
        running,
        desired: desired.len(),
    })
}

async fn list_existing_launcher_containers(client: &Client, docker_base: &str) -> anyhow::Result<Vec<ExistingContainer>> {
    let url = format!("{}/containers/json?all=true", docker_base);
    let items = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<DockerContainerListItem>>()
        .await?;

    let mut out = Vec::new();
    for item in items {
        let Some(first_name) = item.names.first() else {
            continue;
        };
        let normalized = first_name.trim_start_matches('/').to_string();
        if normalized.starts_with("exp-") || normalized.starts_with("control-") || normalized.starts_with("random-") {
            out.push(ExistingContainer {
                id: item.id,
                name: normalized,
                state: item.state.unwrap_or_else(|| "unknown".to_string()),
            });
        }
    }
    Ok(out)
}

async fn start_container(client: &Client, docker_base: &str, id: &str) -> anyhow::Result<()> {
    let start_url = format!("{}/containers/{}/start", docker_base, id);
    client.post(start_url).send().await?.error_for_status()?;
    Ok(())
}

async fn create_and_start_container(
    client: &Client,
    docker_base: &str,
    inst: &DesiredInstance,
    experiment_image: &str,
    experiment_config_bind: &str,
) -> anyhow::Result<()> {
    let mut env = vec![
        format!("EXPERIMENT_CONFIG_PATH={}", inst.config_path),
        "APP_ENV=dev".to_string(),
        "MANAGER_BASE_URL=http://manager:8080".to_string(),
        "NATS_URL=nats://nats:4222".to_string(),
        "MARKET_NEW_QUESTION_TOPIC=market.new_question.v1".to_string(),
        "EXPERIMENT_SIGNAL_TOPIC=signal.computed.v1".to_string(),
        "PREDICTION_OUTPUT_TOPIC=prediction.proposed.v1".to_string(),
        "OLLAMA_BASE_URL=http://host.docker.internal:11434".to_string(),
        "VECTOR_BACKEND=pgvector".to_string(),
    ];
    if let Some(seed) = inst.seed {
        env.push(format!("EXPERIMENT_SEED={seed}"));
    }
    if let Some(module) = &inst.module {
        env.push(format!("EXPERIMENT_MODULE={module}"));
    }

    let req = DockerCreateReq {
        image: experiment_image.to_string(),
        env,
        labels: serde_json::json!({
            "polybet.launcher": "true",
            "polybet.experiment_name": inst.experiment_name,
        }),
        host_config: serde_json::json!({
            "Binds": [experiment_config_bind],
            "RestartPolicy": {"Name": "unless-stopped"},
            "ExtraHosts": ["host.docker.internal:host-gateway"],
            "NetworkMode": "polybet_net"
        }),
    };

    let create_url = format!("{}/containers/create?name={}", docker_base, inst.container_name);
    let create_res = client
        .post(create_url)
        .json(&req)
        .send()
        .await?
        .error_for_status()?
        .json::<DockerCreateResp>()
        .await?;

    let start_url = format!("{}/containers/{}/start", docker_base, create_res.id);
    client.post(start_url).send().await?.error_for_status()?;
    Ok(())
}

async fn stop_and_remove_container(client: &Client, docker_base: &str, id: &str) -> anyhow::Result<()> {
    let stop_url = format!("{}/containers/{}/stop?t=5", docker_base, id);
    let stop_res = client.post(stop_url).send().await?;
    if stop_res.status().as_u16() != 304 {
        stop_res.error_for_status()?;
    }
    let rm_url = format!("{}/containers/{}?force=true", docker_base, id);
    client.delete(rm_url).send().await?.error_for_status()?;
    Ok(())
}

async fn write_launcher_instances(
    db: &PgPool,
    desired: &[DesiredInstance],
    state_by_name: &std::collections::HashMap<String, String>,
) -> anyhow::Result<()> {
    let mut tx = db.begin().await?;

    sqlx::query("DELETE FROM launcher_instances").execute(&mut *tx).await?;
    for d in desired {
        let status = state_by_name
            .get(&d.container_name)
            .map(|s| s.as_str())
            .unwrap_or("missing");
        sqlx::query(
            r#"
            INSERT INTO launcher_instances (container_name, experiment_name, seed, status, updated_at)
            VALUES ($1, $2, $3, $4, now())
            "#,
        )
        .bind(&d.container_name)
        .bind(&d.experiment_name)
        .bind(d.seed)
        .bind(status)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn write_launcher_status(
    db: &PgPool,
    desired: i64,
    running: i64,
    launched: i64,
    stopped: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO launcher_status (id, last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error, updated_at)
        VALUES (1, now(), $1, $2, $3, $4, NULL, now())
        ON CONFLICT (id) DO UPDATE SET
            last_success = now(),
            desired_instances = excluded.desired_instances,
            running_instances = excluded.running_instances,
            launched_last_run = excluded.launched_last_run,
            stopped_last_run = excluded.stopped_last_run,
            last_error = NULL,
            updated_at = now()
        "#,
    )
    .bind(desired)
    .bind(running)
    .bind(launched)
    .bind(stopped)
    .execute(db)
    .await?;
    Ok(())
}

async fn record_launcher_error(db: &PgPool, message: &str) -> anyhow::Result<()> {
    let trimmed = if message.len() > 2000 { &message[..2000] } else { message };
    sqlx::query(
        r#"
        INSERT INTO launcher_status (id, last_success, desired_instances, running_instances, launched_last_run, stopped_last_run, last_error, updated_at)
        VALUES (1, NULL, 0, 0, 0, 0, $1, now())
        ON CONFLICT (id) DO UPDATE SET
            last_error = excluded.last_error,
            updated_at = now()
        "#,
    )
    .bind(trimmed)
    .execute(db)
    .await?;
    Ok(())
}
