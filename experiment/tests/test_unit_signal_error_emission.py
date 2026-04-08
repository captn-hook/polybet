from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
SIGNAL_DIR = ROOT / "signal"
if str(SIGNAL_DIR) not in sys.path:
    sys.path.insert(0, str(SIGNAL_DIR))

sys.modules.setdefault("polybet_nats", SimpleNamespace(PolyNats=object))
import market_implied as signal_main
import pass_through as signal_pass_through
import signal_common


class _FakeNats:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    async def publish_json(self, subject: str, payload: dict[str, object]) -> None:
        self.calls.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


def test_signal_market_implied_emits_signal_error_contract(monkeypatch) -> None:
    fake = _FakeNats()

    async def _fake_connect(url: str) -> _FakeNats:
        assert url == "nats://unit-test:4222"
        return fake

    monkeypatch.setattr(signal_common, "PolyNats", SimpleNamespace(connect=_fake_connect))

    asyncio.run(
        signal_main._emit_signal_error(
            "nats://unit-test:4222",
            "signal.runtime.crash",
            "boom",
            {"k": "v"},
        )
    )

    assert fake.closed is True
    assert len(fake.calls) == 1
    subject, payload = fake.calls[0]
    assert subject == "signal.error.v1"
    assert payload["event_type"] == "signal.error.v1"
    assert payload["service"] == "signal_market_implied"
    assert payload["error_code"] == "signal.runtime.crash"
    assert payload["message"] == "boom"
    assert payload["context"] == {"k": "v"}
    assert isinstance(payload["emitted_at"], str)


def test_signal_pass_through_emits_signal_error_contract(monkeypatch) -> None:
    fake = _FakeNats()

    async def _fake_connect(url: str) -> _FakeNats:
        assert url == "nats://unit-test:4222"
        return fake

    monkeypatch.setattr(signal_common, "PolyNats", SimpleNamespace(connect=_fake_connect))

    asyncio.run(
        signal_pass_through._emit_signal_error(
            "nats://unit-test:4222",
            "signal.runtime.crash",
            "boom",
            {"k": "v"},
        )
    )

    assert fake.closed is True
    assert len(fake.calls) == 1
    subject, payload = fake.calls[0]
    assert subject == "signal.error.v1"
    assert payload["event_type"] == "signal.error.v1"
    assert payload["service"] == "signal_pass_through"
    assert payload["error_code"] == "signal.runtime.crash"
    assert payload["message"] == "boom"
    assert payload["context"] == {"k": "v"}
    assert isinstance(payload["emitted_at"], str)
