import os
import urllib.parse
from typing import Any

from signal_common import _as_float, _extract_token_id, _fetch_json, run_signal_entrypoint


def _build_price_history(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    token_id = _extract_token_id(event)
    if not token_id:
        return None, "missing_token_id"

    clob_base = os.getenv("CLOB_API_BASE", "https://clob.polymarket.com").rstrip("/")
    interval = os.getenv("PRICE_HISTORY_INTERVAL", "1d")
    fidelity = os.getenv("PRICE_HISTORY_FIDELITY", "60")
    token_q = urllib.parse.quote(token_id, safe="")
    url = f"{clob_base}/prices-history?market={token_q}&interval={interval}&fidelity={fidelity}"

    try:
        data = _fetch_json(url)
    except Exception:
        return None, "price_history_fetch_failed"

    if not isinstance(data, dict):
        return None, "price_history_unavailable"

    history = data.get("history")
    if not isinstance(history, list):
        return None, "price_history_unavailable"

    parsed = [
        {"t": row.get("t"), "p": _as_float(row.get("p"))}
        for row in history
        if isinstance(row, dict)
    ]

    return {
        "token_id": token_id,
        "history": parsed,
        "meta": {
            "source_event": "market.new_question.v1",
            "clob_api_base": clob_base,
            "endpoint": "/prices-history",
            "interval": interval,
            "fidelity": fidelity,
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_price_history",
        default_signal_id="signal-price-history",
        default_signal_kind="price_history",
        builder=_build_price_history,
    )


if __name__ == "__main__":
    main()
