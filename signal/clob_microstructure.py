import os
import urllib.parse
from typing import Any

from signal_common import _as_float, _extract_payload, _pick, _fetch_json, run_signal_entrypoint


def _extract_token_id(event: dict[str, Any]) -> str | None:
    payload = _extract_payload(event)
    token_id = _pick(payload, ["tokenId", "token_id", "clobTokenId", "clob_token_id"])
    if token_id is None:
        for key in ("outcomeTokenIds", "clobTokenIds"):
            value = payload.get(key)
            if isinstance(value, str):
                import json

                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    value = None
            if isinstance(value, list) and value:
                token_id = value[0]
                break
    if token_id is None:
        return None
    token = str(token_id).strip()
    return token if token else None


def _build_clob_microstructure(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    token_id = _extract_token_id(event)
    if not token_id:
        return None, "missing_token_id"

    clob_base = os.getenv("CLOB_API_BASE", "https://clob.polymarket.com").rstrip("/")
    token_q = urllib.parse.quote(token_id, safe="")
    spread_url = f"{clob_base}/spread?token_id={token_q}"
    last_trade_url = f"{clob_base}/last-trade-price?token_id={token_q}"

    spread = None
    last_trade_price = None
    last_trade_side = None

    try:
        spread_payload = _fetch_json(spread_url)
        if isinstance(spread_payload, dict):
            spread = _as_float(spread_payload.get("spread"))
    except Exception:
        pass

    try:
        last_trade_payload = _fetch_json(last_trade_url)
        if isinstance(last_trade_payload, dict):
            last_trade_price = _as_float(last_trade_payload.get("price"))
            side = last_trade_payload.get("side")
            if isinstance(side, str):
                side = side.strip().upper()
            last_trade_side = side if side in {"BUY", "SELL"} else None
    except Exception:
        pass

    if spread is None and last_trade_price is None:
        return None, "clob_data_unavailable"

    return {
        "sentiment_side": "YES",
        "sentiment_confidence": 0.5,
        "token_id": token_id,
        "spread": spread,
        "last_trade_price": last_trade_price,
        "last_trade_side": last_trade_side,
        "meta": {
            "source_event": "market.new_question.v1",
            "clob_api_base": clob_base,
            "spread_endpoint": "/spread",
            "last_trade_endpoint": "/last-trade-price",
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_clob_microstructure",
        default_signal_id="signal-clob-microstructure",
        default_signal_kind="clob_microstructure",
        builder=_build_clob_microstructure,
    )


if __name__ == "__main__":
    main()
