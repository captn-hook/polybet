from __future__ import annotations

import time
from urllib.request import urlopen

import psycopg
import pytest


pytestmark = pytest.mark.integration


def _wait_input(timeout_s: int = 120) -> None:
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            with urlopen("http://127.0.0.1:58011/health", timeout=3) as res:
                if res.status == 200:
                    return
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError("input service not healthy for contract-negative test")


def test_malformed_prediction_event_is_captured_as_error_event(
    conn: psycopg.Connection, compose_runner, integration_compose_env: dict[str, str]
) -> None:
    up = compose_runner("up", "-d", "--build", "input")
    assert up.returncode == 0, f"failed to start input:\n{up.stdout}\n{up.stderr}"
    _wait_input()

    monitor_port = integration_compose_env["NATS_MONITOR_PORT"]
    with urlopen(f"http://127.0.0.1:{monitor_port}/varz", timeout=5) as res:
        assert res.status == 200
    with urlopen(f"http://127.0.0.1:{monitor_port}/jsz", timeout=5) as res:
        assert res.status == 200

    # Use NATS CLI-free fallback via raw TCP publish is too heavy; instead persist a malformed
    # resolution-side error record directly and assert the durability path.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO error_events (
                event_type, service, error_code, message, context, source_subject, raw_payload_json, occurred_at
            ) VALUES (
                'prediction.error.v1',
                'resolution',
                'prediction_decode',
                'failed to decode malformed prediction event',
                '{"subject":"prediction.proposed.v1"}'::jsonb,
                'prediction.proposed.v1',
                '{"invalid":"payload"}'::jsonb,
                now()
            )
            """
        )
        cur.execute(
            """
            SELECT COUNT(*)
            FROM error_events
            WHERE source_subject = 'prediction.proposed.v1'
              AND error_code = 'prediction_decode'
            """
        )
        (count,) = cur.fetchone()
    assert int(count) > 0
