from __future__ import annotations

import time
from urllib.request import urlopen

import pytest


pytestmark = pytest.mark.integration


def _wait_ok(url: str, timeout_s: int = 90) -> None:
    started = time.time()
    while time.time() - started < timeout_s:
        try:
            with urlopen(url, timeout=3) as res:
                if res.status == 200:
                    return
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError(f"Timed out waiting for health endpoint: {url}")


def test_input_service_restart_recovery(compose_runner) -> None:
    stop = compose_runner("stop", "input")
    assert stop.returncode == 0, f"failed to stop input:\n{stop.stdout}\n{stop.stderr}"

    up = compose_runner("up", "-d", "--build", "input")
    assert up.returncode == 0, f"failed to restart input:\n{up.stdout}\n{up.stderr}"

    _wait_ok("http://127.0.0.1:58011/health")
