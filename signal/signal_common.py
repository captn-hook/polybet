import asyncio
import nats.errors
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))
from polybet_nats import PolyNats


def _extract_market_id(event: dict[str, Any]) -> str:
    return str(event.get("market_id") or "").strip()


def _extract_market_question(event: dict[str, Any]) -> Any:
    return event.get("question") or event.get("market_question")


def _extract_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick(payload: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _parse_json_array(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        import json

        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, list) else None
    return None


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_json(url: str, timeout_s: float = 5.0) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "polybet-signal/0.1",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        body = response.read().decode("utf-8")
    import json

    return json.loads(body)


async def emit_signal_error(service: str, nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    client = await PolyNats.connect(nats_url)
    try:
        await client.publish_json(
            "signal.error.v1",
            {
                "event_type": "signal.error.v1",
                "emitted_at": _iso_now(),
                "service": service,
                "error_code": error_code,
                "message": message,
                "context": context,
            },
        )
    finally:
        await client.close()


async def run_signal_loop(
    *,
    service_name: str,
    default_signal_id: str,
    default_signal_kind: str,
    builder: Any,
) -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    source_topic = os.getenv("MARKET_NEW_QUESTION_TOPIC", "market.new_question.v1")
    output_topic = os.getenv("SIGNAL_OUTPUT_TOPIC", "signal.computed.v1")
    signal_id = os.getenv("SIGNAL_ID", default_signal_id)
    signal_kind = os.getenv("SIGNAL_KIND", default_signal_kind)

    client = await PolyNats.connect(nats_url)
    sub = await client.subscribe(source_topic)
    print(f"[{service_name}] listening source={source_topic} output={output_topic} signal_id={signal_id}")

    while True:
        try:
            msg = await sub.next_msg(timeout=60)
        except nats.errors.TimeoutError:
            # Idle poll window elapsed without new messages.
            continue
        event = PolyNats.decode_json(msg)
        market_id = _extract_market_id(event)
        if not market_id:
            continue

        details, reason = builder(event)
        status = "ok" if details is not None else "unavailable"
        payload: dict[str, Any] = {
            "event_type": "signal.computed.v1",
            "emitted_at": _iso_now(),
            "signal_id": signal_id,
            "signal_kind": signal_kind,
            "market_id": market_id,
            "market_question": _extract_market_question(event),
            "status": status,
            "reason": reason,
        }
        if details is not None:
            payload.update(details)

        await client.publish_json(output_topic, payload)
        print(f"[{service_name}] emitted signal.computed.v1 market_id={market_id} status={status} reason={reason}")


def run_signal_entrypoint(
    *,
    service_name: str,
    default_signal_id: str,
    default_signal_kind: str,
    builder: Any,
) -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        asyncio.run(
            run_signal_loop(
                service_name=service_name,
                default_signal_id=default_signal_id,
                default_signal_kind=default_signal_kind,
                builder=builder,
            )
        )
    except Exception as exc:
        import traceback

        print(f"[{service_name}] fatal error: {exc}", file=sys.stderr)
        traceback.print_exc()
        asyncio.run(
            emit_signal_error(
                service_name,
                nats_url,
                "signal.runtime.crash",
                str(exc),
                {"traceback": traceback.format_exc()},
            )
        )
        raise
