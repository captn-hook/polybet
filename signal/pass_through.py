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


async def run() -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    source_topic = os.getenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
    output_topic = os.getenv("SIGNAL_OUTPUT_TOPIC", "signal.computed.v1")
    signal_id = os.getenv("SIGNAL_ID", "signal-pass-through")
    signal_kind = os.getenv("SIGNAL_KIND", "pass_through")

    client = await PolyNats.connect(nats_url)
    sub = await client.subscribe(source_topic)
    print(
        f"[signal-pass-through] listening source={source_topic} output={output_topic} signal_id={signal_id}"
    )

    while True:
        msg = await sub.next_msg(timeout=60)
        event = PolyNats.decode_json(msg)
        market_id = str(event.get("market_id") or "").strip()
        if not market_id:
            continue

        payload: dict[str, Any] = {
            "event_type": "signal.computed.v1",
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "signal_id": signal_id,
            "signal_kind": signal_kind,
            "market_id": market_id,
            "market_question": event.get("question"),
            "status": "ok",
            "reason": "pass_through",
            "sentiment_side": "YES",
            "sentiment_confidence": 0.5,
            "meta": {
                "pass_through": True,
                "source_event": "market.new_question.v1",
            },
        }
        await client.publish_json(output_topic, payload)
        print(f"[signal-pass-through] emitted signal.computed.v1 market_id={market_id}")


async def _emit_signal_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    client = await PolyNats.connect(nats_url)
    try:
        await client.publish_json(
            "signal.error.v1",
            {
                "event_type": "signal.error.v1",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "service": "signal_pass_through",
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
        print(f"[signal-pass-through] fatal error: {exc}", file=sys.stderr)
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
