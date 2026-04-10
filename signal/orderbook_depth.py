import os
import urllib.parse
from typing import Any

from signal_common import _as_float, _extract_token_id, _fetch_json, run_signal_entrypoint


def _build_orderbook_depth(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    token_id = _extract_token_id(event)
    if not token_id:
        return None, "missing_token_id"

    clob_base = os.getenv("CLOB_API_BASE", "https://clob.polymarket.com").rstrip("/")
    token_q = urllib.parse.quote(token_id, safe="")
    url = f"{clob_base}/book?token_id={token_q}"

    try:
        data = _fetch_json(url)
    except Exception:
        return None, "orderbook_fetch_failed"

    if not isinstance(data, dict):
        return None, "orderbook_unavailable"

    bids = data.get("bids")
    asks = data.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        return None, "orderbook_unavailable"

    return {
        "token_id": token_id,
        "bids": [{"price": _as_float(b.get("price")), "size": _as_float(b.get("size"))} for b in bids if isinstance(b, dict)],
        "asks": [{"price": _as_float(a.get("price")), "size": _as_float(a.get("size"))} for a in asks if isinstance(a, dict)],
        "book_timestamp": data.get("timestamp"),
        "meta": {
            "source_event": "market.new_question.v1",
            "clob_api_base": clob_base,
            "endpoint": "/book",
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_orderbook_depth",
        default_signal_id="signal-orderbook-depth",
        default_signal_kind="orderbook_depth",
        builder=_build_orderbook_depth,
    )


if __name__ == "__main__":
    main()
