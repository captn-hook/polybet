from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import nats.errors

ROOT = Path(__file__).resolve().parents[2]
SIGNAL_DIR = ROOT / "signal"
if str(SIGNAL_DIR) not in sys.path:
    sys.path.insert(0, str(SIGNAL_DIR))

sys.modules.setdefault("polybet_nats", SimpleNamespace(PolyNats=object))
import signal_common


class _FakeSubscription:
    def __init__(self) -> None:
        self.calls = 0

    async def next_msg(self, timeout: int = 0):
        self.calls += 1
        if self.calls == 1:
            raise nats.errors.TimeoutError()
        if self.calls == 2:
            return {"market_id": "m1", "question": "q1"}
        raise KeyboardInterrupt()


class _FakeClient:
    def __init__(self) -> None:
        self.sub = _FakeSubscription()
        self.published: list[tuple[str, dict[str, object]]] = []

    async def subscribe(self, _: str) -> _FakeSubscription:
        return self.sub

    async def publish_json(self, subject: str, payload: dict[str, object]) -> None:
        self.published.append((subject, payload))


def test_run_signal_loop_ignores_idle_timeout_and_keeps_processing(monkeypatch) -> None:
    fake_client = _FakeClient()
    monkeypatch.setenv("NATS_URL", "nats://unit-test:4222")
    monkeypatch.setenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
    monkeypatch.setenv("SIGNAL_OUTPUT_TOPIC", "signal.computed.v1")

    async def _fake_connect(_: str) -> _FakeClient:
        return fake_client

    monkeypatch.setattr(signal_common, "PolyNats", SimpleNamespace(connect=_fake_connect, decode_json=lambda msg: msg))

    def _builder(_: dict[str, object]) -> tuple[dict[str, object], str]:
        return {"sentiment_side": "YES", "sentiment_confidence": 0.9, "meta_json": {}}, "ok"

    with asyncio.Runner() as runner:
        try:
            runner.run(
                signal_common.run_signal_loop(
                    service_name="signal_test",
                    default_signal_id="signal-test",
                    default_signal_kind="test_kind",
                    builder=_builder,
                )
            )
        except KeyboardInterrupt:
            pass

    assert fake_client.sub.calls >= 2
    assert len(fake_client.published) == 1
    subject, payload = fake_client.published[0]
    assert subject == "signal.computed.v1"
    assert payload["market_id"] == "m1"
    assert payload["status"] == "ok"
