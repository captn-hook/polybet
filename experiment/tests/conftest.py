import os
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

EXPERIMENT_ROOT = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "docker-compose.yml"
E2E_PROJECT = "polybetint"

COMMON_PYTHON = ROOT / "common" / "python"

if str(EXPERIMENT_ROOT) not in sys.path:
    sys.path.insert(0, str(EXPERIMENT_ROOT))
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))


@pytest.fixture(scope="session")
def integration_compose_env() -> dict[str, str]:
    return {
        "DOCKER_NETWORK_NAME": f"{E2E_PROJECT}_net",
        "NATS_VOLUME_NAME": f"{E2E_PROJECT}_nats_data",
        "POSTGRES_DATA_PATH": f".\\data\\postgres-{E2E_PROJECT}",
        "POSTGRES_BACKUP_PATH": f".\\data\\backups\\postgres-{E2E_PROJECT}",
        "POSTGRES_USER": "polybet",
        "POSTGRES_PASSWORD": "polybet_dev_password",
        "INPUT_DB_USER": "polybet_input",
        "INPUT_DB_PASSWORD": "polybet_input_dev_password",
        "POSTGRES_PORT": "55433",
        "INPUT_PORT": "58011",
        "RESOLUTION_PORT": "58012",
        "MANAGER_PORT": "58000",
        "NATS_PORT": "54223",
        "NATS_MONITOR_PORT": "58223",
        "SYNC_INTERVAL_SECONDS": "20",
        "ZERO_ELIGIBLE_FAIL_STREAK": "0",
    }


def _compose(*args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-p", E2E_PROJECT, "-f", str(COMPOSE_FILE), *args]
    merged_env = os.environ.copy()
    merged_env.update(env)
    return subprocess.run(
        cmd,
        cwd=ROOT,
        env=merged_env,
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.fixture(scope="session")
def compose_runner(integration_compose_env: dict[str, str]):
    def _run(*args: str) -> subprocess.CompletedProcess[str]:
        return _compose(*args, env=integration_compose_env)

    return _run


def _wait_for_postgres(dsn: str, timeout_s: int = 120) -> None:
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            with psycopg.connect(dsn, autocommit=True):
                return
        except psycopg.Error:
            time.sleep(2)
    raise AssertionError(f"Timed out waiting for integration postgres at {dsn}")


def _wait_for_schema(dsn: str, timeout_s: int = 120) -> None:
    required_tables = {"markets", "gamma_markets", "signal_outputs", "experiment_predictions"}
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        """
                    )
                    present = {str(row[0]) for row in cur.fetchall()}
            if required_tables.issubset(present):
                return
        except psycopg.Error:
            pass
        time.sleep(2)
    raise AssertionError(
        f"Timed out waiting for integration schema tables: {sorted(required_tables)}"
    )


@pytest.fixture(scope="session")
def dsn(integration_compose_env: dict[str, str]) -> str:
    postgres_data_path = ROOT / "data" / f"postgres-{E2E_PROJECT}"
    if postgres_data_path.exists():
        shutil.rmtree(postgres_data_path, ignore_errors=True)
    _compose("down", "--remove-orphans", "-v", env=integration_compose_env)
    up = _compose("up", "-d", "--build", "postgres", "nats", "input", env=integration_compose_env)
    if up.returncode != 0:
        raise AssertionError(f"integration compose up failed:\n{up.stdout}\n{up.stderr}")

    dsn_value = "postgresql://polybet:polybet_dev_password@127.0.0.1:55433/polybet"
    _wait_for_postgres(dsn_value, timeout_s=120)
    _wait_for_schema(dsn_value, timeout_s=120)
    yield dsn_value

    _compose("down", "--remove-orphans", "-v", env=integration_compose_env)
    if postgres_data_path.exists():
        shutil.rmtree(postgres_data_path, ignore_errors=True)


@pytest.fixture()
def conn(dsn: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(
        dsn,
        autocommit=True,
        options="-c statement_timeout=15000 -c idle_in_transaction_session_timeout=15000",
    ) as connection:
        yield connection
