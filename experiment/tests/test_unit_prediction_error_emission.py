from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

sys.modules.setdefault("polybet_nats", SimpleNamespace(PolyNats=object))
import exp_runner


class _FakeNats:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    async def publish_json(self, subject: str, payload: dict[str, object]) -> None:
        self.calls.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


def test_emit_prediction_error_uses_expected_event_contract(monkeypatch) -> None:
    fake = _FakeNats()

    async def _fake_connect(url: str) -> _FakeNats:
        assert url == "nats://unit-test:4222"
        return fake

    monkeypatch.setattr(exp_runner, "PolyNats", SimpleNamespace(connect=_fake_connect))

    asyncio.run(
        exp_runner._emit_prediction_error(
            "nats://unit-test:4222",
            "prediction.runtime.crash",
            "boom",
            {"k": "v"},
        )
    )

    assert fake.closed is True
    assert len(fake.calls) == 1
    subject, payload = fake.calls[0]
    assert subject == "prediction.error.v1"
    assert payload["event_type"] == "prediction.error.v1"
    assert payload["service"] == "experiment"
    assert payload["error_code"] == "prediction.runtime.crash"
    assert payload["message"] == "boom"
    assert payload["context"] == {"k": "v"}
    assert isinstance(payload["emitted_at"], str)
