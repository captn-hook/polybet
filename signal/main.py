import asyncio
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))
from polybet_nats import PolyNats


def _extract_prices(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    prices = raw.get("outcome_prices") or raw.get("outcomePrices")
    if not isinstance(prices, list) or len(prices) < 2:
        return None, None
    try:
        return float(prices[0]), float(prices[1])
    except (TypeError, ValueError):
        return None, None


def _build_sentiment(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    raw = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
    yes_p, no_p = _extract_prices(raw)
    if yes_p is None or no_p is None:
        return None, "missing_or_invalid_outcome_prices"
    total = yes_p + no_p
    if total <= 0:
        return None, "non_positive_price_total"
    yes_share = yes_p / total
    no_share = no_p / total
    side = "YES" if yes_p >= no_p else "NO"
    confidence = max(yes_share, no_share)
    return {
        "sentiment_side": side,
        "sentiment_confidence": confidence,
        "yes_probability": yes_p,
        "no_probability": no_p,
        "yes_share": yes_share,
        "no_share": no_share,
        "margin": abs(yes_share - no_share),
    }, "ok"


async def run() -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    source_topic = os.getenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
    output_topic = os.getenv("SIGNAL_OUTPUT_TOPIC", "signal.computed.v1")
    signal_id = os.getenv("SIGNAL_ID", "signal-poly-sentiment")
    signal_kind = os.getenv("SIGNAL_KIND", "polymarket_sentiment")

    client = await PolyNats.connect(nats_url)
    sub = await client.subscribe(source_topic)
    print(f"[signal] listening source={source_topic} output={output_topic} signal_id={signal_id}")

    while True:
        msg = await sub.next_msg(timeout=60)
        event = PolyNats.decode_json(msg)
        market_id = str(event.get("market_id") or "").strip()
        if not market_id:
            continue
        signal, reason = _build_sentiment(event)
        payload: dict[str, Any] = {
            "event_type": "signal.computed.v1",
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "signal_id": signal_id,
            "signal_kind": signal_kind,
            "market_id": market_id,
            "market_question": event.get("question"),
            "status": "ok" if signal is not None else "unavailable",
            "reason": reason,
        }
        if signal is not None:
            payload.update(signal)
        await client.publish_json(output_topic, payload)
        print(
            f"[signal] emitted signal.computed.v1 market_id={market_id} status={payload['status']} reason={reason}"
        )


async def _emit_signal_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    client = await PolyNats.connect(nats_url)
    try:
        await client.publish_json(
            "signal.error.v1",
            {
                "event_type": "signal.error.v1",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "service": "signal_polymarket_sentiment",
                "error_code": error_code,
                "message": message,
                "context": context,
            },
        )
    finally:
        await client.close()


def main() -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        asyncio.run(run())
    except Exception as exc:
        print(f"[signal] fatal error: {exc}", file=sys.stderr)
        traceback.print_exc()
        asyncio.run(
            _emit_signal_error(
                nats_url,
                "signal.runtime.crash",
                str(exc),
                {"traceback": traceback.format_exc()},
            )
        )
        raise


if __name__ == "__main__":
    main()
