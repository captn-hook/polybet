from typing import Any

from signal_common import _extract_payload, _pick, emit_signal_error, run_signal_entrypoint


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_prices(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    prices = _pick(raw, ["outcomePrices", "outcome_prices"])
    if isinstance(prices, str):
        import json

        try:
            prices = json.loads(prices)
        except json.JSONDecodeError:
            return None, None
    if not isinstance(prices, list) or len(prices) < 2:
        return None, None
    yes_price = _as_float(prices[0])
    no_price = _as_float(prices[1])
    return yes_price, no_price


def _build_market_implied_signal(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    raw = _extract_payload(event)
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
        "meta": {
            "source_event": "market.new_question.v1",
            "source_field": "payload.outcomePrices",
        },
    }, "ok"


async def _emit_signal_error(nats_url: str, error_code: str, message: str, context: dict[str, Any]) -> None:
    await emit_signal_error("signal_market_implied", nats_url, error_code, message, context)


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_market_implied",
        default_signal_id="signal-market-implied",
        default_signal_kind="market_implied",
        builder=_build_market_implied_signal,
    )


if __name__ == "__main__":
    main()
