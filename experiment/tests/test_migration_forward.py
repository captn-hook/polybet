from __future__ import annotations

import time
from urllib.request import urlopen

import psycopg
import pytest


pytestmark = pytest.mark.integration


def _wait_input_health(timeout_s: int = 60) -> None:
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            with urlopen("http://127.0.0.1:58011/health", timeout=3) as res:
                if res.status == 200:
                    return
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError("Timed out waiting for input service health after restart")


def test_forward_migration_adds_missing_columns(
    conn: psycopg.Connection, compose_runner
) -> None:
    down = compose_runner("stop", "input")
    assert down.returncode == 0, f"failed to stop input:\n{down.stdout}\n{down.stderr}"

    with conn.cursor() as cur:
        cur.execute("ALTER TABLE markets DROP COLUMN IF EXISTS accepting_orders")
        cur.execute("ALTER TABLE markets DROP COLUMN IF EXISTS is_eligible")
        cur.execute("ALTER TABLE manager_sync_status DROP COLUMN IF EXISTS eligible_markets")
        cur.execute("ALTER TABLE manager_sync_status DROP COLUMN IF EXISTS zero_eligible_streak")

    up = compose_runner("up", "-d", "--build", "input")
    assert up.returncode == 0, f"failed to start input:\n{up.stdout}\n{up.stderr}"
    _wait_input_health()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'markets'
            """
        )
        columns = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'manager_sync_status'
            """
        )
        sync_columns = {row[0] for row in cur.fetchall()}

    assert "accepting_orders" in columns
    assert "is_eligible" in columns
    assert "eligible_markets" in sync_columns
    assert "zero_eligible_streak" in sync_columns
