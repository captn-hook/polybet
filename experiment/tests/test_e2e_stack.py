from __future__ import annotations

import os
import subprocess
import time
import json
import socket
import shutil
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import psycopg
import pytest


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "docker-compose.yml"
ENV_FILE = ROOT / ".env"
E2E_PROJECT = "polybete2e"


def _compose(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    cmd = [
        "docker",
        "compose",
        "-p",
        E2E_PROJECT,
        "-f",
        str(COMPOSE_FILE),
    ]
    if ENV_FILE.exists():
        cmd.extend(["--env-file", str(ENV_FILE)])
    cmd.extend(args)
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        cwd=ROOT,
        env=merged_env,
        text=True,
        capture_output=True,
        check=False,
    )


def _wait_http_ok(url: str, timeout_s: int = 120) -> None:
    started = time.time()
    last_error: str | None = None
    while time.time() - started < timeout_s:
        try:
            with urlopen(url, timeout=3) as res:
                if res.status == 200:
                    return
                last_error = f"unexpected status {res.status}"
        except (HTTPError, URLError, ConnectionResetError, TimeoutError, socket.timeout, OSError) as exc:
            # During compose startup, the service can briefly reset/close sockets.
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(2)
    raise AssertionError(f"Timed out waiting for {url} to return 200; last_error={last_error}")


def _wait_for_data(conn: psycopg.Connection, timeout_s: int = 120) -> None:
    started = time.time()
    while time.time() - started < timeout_s:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM signal_outputs")
            signal_count = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM experiment_predictions")
            pred_count = int(cur.fetchone()[0])
            if signal_count > 0 and pred_count > 0:
                return
        time.sleep(2)
    raise AssertionError("Timed out waiting for signal_outputs and experiment_predictions rows")


def _seed_minimum_data(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM gamma_markets WHERE market_id = 'e2e-market-1';
            DELETE FROM markets WHERE market_id = 'e2e-market-1';
            INSERT INTO gamma_markets (
                market_id, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json
            )
            VALUES (
                'e2e-market-1',
                'e2e-seeded-market',
                'E2E seeded market',
                now()::text,
                (now() + interval '5 minutes')::text,
                0,
                0,
                true,
                false,
                true,
                true,
                '{"source":"e2e_seed"}'::jsonb
            );
            DELETE FROM signal_outputs WHERE signal_id = 'e2e-seed-signal';
            INSERT INTO signal_outputs (
                signal_id, signal_kind, market_id, market_question, sentiment_side, sentiment_confidence, meta_json
            )
            VALUES (
                'e2e-seed-signal',
                'polymarket_sentiment',
                'e2e-market-1',
                'E2E seeded market',
                'YES',
                0.75,
                '{"source":"e2e_seed"}'::jsonb
            )
            """
        )
        cur.execute(
            """
            INSERT INTO markets (
                market_id, slug, question, start_date_raw, end_date_raw, volume, liquidity, active, closed, accepting_orders, is_eligible, payload_json
            )
            VALUES (
                'e2e-market-1',
                'e2e-seeded-market',
                'E2E seeded market',
                now()::text,
                (now() + interval '5 minutes')::text,
                0,
                0,
                true,
                false,
                true,
                true,
                '{"source":"e2e_seed"}'::jsonb
            )
            ON CONFLICT (market_id) DO UPDATE SET
                question = excluded.question,
                active = excluded.active,
                closed = excluded.closed,
                is_eligible = excluded.is_eligible,
                updated_at = now()
            """
        )
        cur.execute(
            """
            DELETE FROM experiment_predictions WHERE experiment_id = 'exp-e2e-seed';
            INSERT INTO experiment_predictions (
                experiment_id,
                strategy,
                market_id,
                market_question,
                market_end_date_raw,
                side,
                confidence,
                horizon_minutes,
                rationale,
                meta_json
            )
            VALUES (
                'exp-e2e-seed',
                'seeded',
                'e2e-market-1',
                'E2E seeded market',
                now()::text,
                'YES',
                0.75,
                5,
                'seeded by e2e test',
                '{"source":"e2e_seed"}'::jsonb
            )
            """
        )
        cur.execute(
            """
            INSERT INTO market_outcomes (
                market_id, resolution_status, winning_side, resolved_at, source, payload_json
            )
            VALUES (
                'e2e-market-1',
                'resolved',
                'YES',
                now(),
                'e2e_seed',
                '{"source":"e2e_seed"}'::jsonb
            )
            ON CONFLICT (market_id) DO UPDATE SET
                resolution_status = excluded.resolution_status,
                winning_side = excluded.winning_side,
                resolved_at = excluded.resolved_at,
                source = excluded.source,
                payload_json = excluded.payload_json,
                updated_at = now()
            """
        )


@pytest.mark.e2e
def test_e2e_compose_status_and_data_contract() -> None:
    if os.getenv("POLYBET_RUN_E2E", "0") != "1":
        pytest.skip("Set POLYBET_RUN_E2E=1 to run docker-compose e2e tests")
    if not COMPOSE_FILE.exists():
        pytest.skip("docker-compose.yml not found")

    compose_env = {
        "DOCKER_NETWORK_NAME": f"{E2E_PROJECT}_net",
        "NATS_VOLUME_NAME": f"{E2E_PROJECT}_nats_data",
        "POSTGRES_DATA_PATH": f".\\data\\postgres-{E2E_PROJECT}",
        "POSTGRES_BACKUP_PATH": f".\\data\\backups\\postgres-{E2E_PROJECT}",
        "POSTGRES_USER": "polybet",
        "POSTGRES_PASSWORD": "polybet_dev_password",
        # E2E uses a deterministic bootstrap credential instead of role provisioning.
        "INPUT_DB_USER": "polybet",
        "INPUT_DB_PASSWORD": "polybet_dev_password",
        "RESOLUTION_DB_USER": "polybet",
        "RESOLUTION_DB_PASSWORD": "polybet_dev_password",
        "OBSERVE_DB_USER": "polybet",
        "OBSERVE_DB_PASSWORD": "polybet_dev_password",
        "POSTGRES_PORT": "55432",
        "MANAGER_PORT": "58000",
        "INPUT_PORT": "58001",
        "RESOLUTION_PORT": "58002",
        "NATS_PORT": "54222",
        "NATS_MONITOR_PORT": "58222",
        "SYNC_INTERVAL_SECONDS": "20",
        "SIGNAL_POLL_INTERVAL_SEC": "5",
        "ZERO_ELIGIBLE_FAIL_STREAK": "0",
    }
    postgres_data_path = ROOT / "data" / f"postgres-{E2E_PROJECT}"
    if postgres_data_path.exists():
        shutil.rmtree(postgres_data_path, ignore_errors=True)
    _compose("down", "--remove-orphans", "-v", env=compose_env)

    up = _compose(
        "up",
        "-d",
        "--build",
        "postgres",
        "nats",
        "input",
        "resolution",
        "observe",
        "signal_market_implied",
        env=compose_env,
    )
    if up.returncode != 0:
        raise AssertionError(f"docker compose up failed:\n{up.stdout}\n{up.stderr}")

    try:
        _wait_http_ok("http://127.0.0.1:58000/health", timeout_s=180)
        _wait_http_ok("http://127.0.0.1:58000/status", timeout_s=180)

        with urlopen("http://127.0.0.1:58000/status", timeout=10) as res:
            assert res.status == 200
            payload: dict[str, Any] = json.loads(res.read().decode("utf-8"))
        assert isinstance(payload.get("ok"), bool)
        assert isinstance(payload.get("alerts"), list)
        assert isinstance(payload.get("sync"), dict)
        assert isinstance(payload.get("launcher"), dict)
        assert isinstance(payload.get("counts"), dict)
        assert isinstance(payload.get("freshness"), dict)

        compose_dsn = "postgresql://polybet:polybet_dev_password@127.0.0.1:55432/polybet"
        with psycopg.connect(compose_dsn, autocommit=True) as conn:
            _seed_minimum_data(conn)
            _wait_for_data(conn, timeout_s=30)

            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM signal_outputs")
                (signal_count,) = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM experiment_predictions")
                (prediction_count,) = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM gamma_markets")
                (market_count,) = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM markets")
                (market_questions_count,) = cur.fetchone()
                cur.execute("SELECT COUNT(*) FROM market_outcomes")
                (outcomes_count,) = cur.fetchone()

            assert int(signal_count) > 0
            assert int(prediction_count) > 0
            assert int(market_count) > 0
            assert int(market_questions_count) > 0
            assert int(outcomes_count) > 0

    finally:
        _compose("down", "--remove-orphans", env=compose_env)
        if postgres_data_path.exists():
            shutil.rmtree(postgres_data_path, ignore_errors=True)
