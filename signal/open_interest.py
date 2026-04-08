import os
import urllib.parse
from typing import Any

from signal_common import _as_float, _extract_payload, _pick, _fetch_json, run_signal_entrypoint


def _extract_condition_id(event: dict[str, Any]) -> str | None:
    payload = _extract_payload(event)
    condition_id = _pick(payload, ["conditionId", "condition_id", "market"])
    if condition_id is None:
        return None
    value = str(condition_id).strip()
    return value if value else None


def _build_open_interest(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    condition_id = _extract_condition_id(event)
    if not condition_id:
        return None, "missing_condition_id"

    data_base = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").rstrip("/")
    condition_q = urllib.parse.quote(condition_id, safe="")
    url = f"{data_base}/oi?market={condition_q}"

    try:
        payload = _fetch_json(url)
    except Exception:
        return None, "open_interest_fetch_failed"

    if not isinstance(payload, list) or not payload:
        return None, "open_interest_unavailable"

    row = payload[0] if isinstance(payload[0], dict) else None
    if row is None:
        return None, "open_interest_unavailable"
    oi_value = _as_float(row.get("value"))
    if oi_value is None:
        return None, "open_interest_unavailable"

    return {
        "sentiment_side": "YES",
        "sentiment_confidence": 0.5,
        "condition_id": condition_id,
        "open_interest": oi_value,
        "meta": {
            "source_event": "market.new_question.v1",
            "data_api_base": data_base,
            "endpoint": "/oi",
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_open_interest",
        default_signal_id="signal-open-interest",
        default_signal_kind="open_interest",
        builder=_build_open_interest,
    )


if __name__ == "__main__":
    main()
