import os
import urllib.parse
from typing import Any

from signal_common import _as_float, _extract_payload, _pick, _fetch_json, run_signal_entrypoint

_PII_FIELDS = {"proxyWallet", "name", "pseudonym", "bio", "profileImage", "profileImageOptimized", "transactionHash"}


def _extract_condition_id(event: dict[str, Any]) -> str | None:
    payload = _extract_payload(event)
    condition_id = _pick(payload, ["conditionId", "condition_id", "market"])
    if condition_id is None:
        return None
    value = str(condition_id).strip()
    return value if value else None


def _build_trade_flow(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    condition_id = _extract_condition_id(event)
    if not condition_id:
        return None, "missing_condition_id"

    data_base = os.getenv("DATA_API_BASE", "https://data-api.polymarket.com").rstrip("/")
    limit = os.getenv("TRADE_FLOW_LIMIT", "50")
    condition_q = urllib.parse.quote(condition_id, safe="")
    url = f"{data_base}/trades?market={condition_q}&limit={limit}"

    try:
        data = _fetch_json(url)
    except Exception:
        return None, "trade_flow_fetch_failed"

    if not isinstance(data, list):
        return None, "trade_flow_unavailable"

    trades = [
        {k: (_as_float(v) if k in ("size", "price") else v) for k, v in row.items() if k not in _PII_FIELDS}
        for row in data
        if isinstance(row, dict)
    ]

    return {
        "condition_id": condition_id,
        "trades": trades,
        "meta": {
            "source_event": "market.new_question.v1",
            "data_api_base": data_base,
            "endpoint": "/trades",
            "limit": limit,
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_trade_flow",
        default_signal_id="signal-trade-flow",
        default_signal_kind="trade_flow",
        builder=_build_trade_flow,
    )


if __name__ == "__main__":
    main()
