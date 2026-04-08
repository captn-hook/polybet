import random
import time
import asyncio
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from exp_config import read_runtime_config
from exp_strategy import decide_side

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))
from polybet_nats import PolyNats


async def run_async() -> None:
    runtime = read_runtime_config()
    rng = random.Random(runtime.seed if runtime.seed is not None else 0)
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    input_signal_topic = os.getenv("EXPERIMENT_SIGNAL_TOPIC", "signal.computed.v1")
    prediction_topic = os.getenv("PREDICTION_OUTPUT_TOPIC", "prediction.proposed.v1")
    pass_through_signal_kind = os.getenv("PASS_THROUGH_SIGNAL_KIND", "pass_through")
    market_new_question_topic = os.getenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
    nats_client = await PolyNats.connect(nats_url)
    subscription = await nats_client.subscribe(input_signal_topic)
    market_subscription = await nats_client.subscribe(market_new_question_topic)

    print(
        f"[experiment] starting strategy: id={runtime.experiment_id}, mode={runtime.strategy_mode}, "
        f"p_yes={runtime.yes_probability}, seed={runtime.seed}, "
        f"signal_topic={input_signal_topic}, market_topic={market_new_question_topic}, prediction_topic={prediction_topic}"
    )
    latest_market_by_id: dict[str, dict[str, object | None]] = {}
    latest_signal_by_market: dict[str, dict[str, object | None]] = {}
    run_id = f"{runtime.experiment_id}:{runtime.seed}" if runtime.seed else runtime.experiment_id

    while True:
        try:
            market_msg = await market_subscription.next_msg(timeout=1)
            market_event = PolyNats.decode_json(market_msg)
            market_id = str(market_event.get("market_id") or "").strip()
            if market_id:
                latest_market_by_id[market_id] = {
                    "market_id": market_id,
                    "question": market_event.get("question"),
                    "end_date_raw": market_event.get("end_date_raw"),
                }
        except TimeoutError:
            pass

        try:
            msg = await subscription.next_msg(timeout=1)
        except TimeoutError:
            await asyncio.sleep(0.1)
            continue

        event = PolyNats.decode_json(msg)
        if str(event.get("event_type")) != "signal.computed.v1":
            continue
        if str(event.get("status")) not in {"ok", "unavailable"}:
            continue
        market_id = str(event.get("market_id") or "").strip()
        if not market_id:
            continue
        if (
            runtime.strategy_mode_normalized == "pass_through"
            and str(event.get("signal_kind") or "") != pass_through_signal_kind
        ):
            continue

        latest_signal_by_market[market_id] = {
            "market_id": market_id,
            "question": event.get("market_question"),
            "signal_side": event.get("sentiment_side"),
            "signal_confidence": float(event.get("sentiment_confidence", 0.5)),
            "signal_created_at": event.get("emitted_at"),
        }

        market = latest_market_by_id.get(market_id) or {
            "market_id": market_id,
            "question": event.get("market_question"),
            "end_date_raw": None,
        }
        signal_market = latest_signal_by_market.get(market_id)

        side, confidence = decide_side(
            runtime.strategy_mode,
            runtime.yes_probability,
            rng,
            signal_market,
        )
        rationale = f"Control strategy '{runtime.strategy_mode}' generated this prediction."
        await nats_client.publish_json(
            prediction_topic,
            {
                "event_type": "prediction.proposed.v1",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                "experiment_id": runtime.experiment_id,
                "strategy": runtime.strategy,
                "market_id": market["market_id"],
                "market_question": market.get("question"),
                "market_end_date_raw": market.get("end_date_raw"),
                "side": side,
                "confidence": confidence,
                "horizon_minutes": runtime.horizon_minutes,
                "rationale": rationale,
                "meta": {
                    "source": "experiment.nats",
                    "strategy_mode": runtime.strategy_mode,
                    "seed": runtime.seed,
                },
            },
        )

        print(
            f"[experiment] proposed prediction market_id={market['market_id']} side={side} confidence={confidence}"
        )
        if runtime.loop_interval_seconds > 0:
            time.sleep(runtime.loop_interval_seconds)


async def _emit_prediction_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    client = await PolyNats.connect(nats_url)
    try:
        await client.publish_json(
            "prediction.error.v1",
            {
                "event_type": "prediction.error.v1",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "service": "experiment",
                "error_code": error_code,
                "message": message,
                "context": context,
            },
        )
    finally:
        await client.close()


async def run_with_top_level_error_emission() -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        await run_async()
    except Exception as exc:
        print(f"[experiment] fatal error: {exc}", file=sys.stderr)
        traceback.print_exc()
        await _emit_prediction_error(
            nats_url,
            "prediction.runtime.crash",
            str(exc),
            {"traceback": traceback.format_exc()},
        )
        raise


def run() -> None:
    asyncio.run(run_with_top_level_error_emission())
