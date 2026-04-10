import statistics
from typing import Any

from signal_common import run_signal_entrypoint

_SOURCE_KIND = "price_history"


def _build_price_history_derived(event: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    if event.get("signal_kind") != _SOURCE_KIND:
        return None, "skip"
    if event.get("status") != "ok":
        return None, "source_signal_unavailable"

    raw = event.get("history") or []
    history = sorted(
        [h for h in raw if h.get("p") is not None and h.get("t") is not None],
        key=lambda h: h["t"],
    )

    if len(history) < 2:
        return None, "insufficient_history"

    prices = [h["p"] for h in history]
    price_latest = prices[-1]
    price_earliest = prices[0]
    price_mean = sum(prices) / len(prices)
    price_std = statistics.stdev(prices) if len(prices) >= 2 else 0.0

    returns = [
        (prices[i] - prices[i - 1]) / prices[i - 1]
        for i in range(1, len(prices))
        if prices[i - 1] != 0
    ]

    momentum = (price_latest - price_earliest) / price_earliest if price_earliest != 0 else 0.0
    volatility = statistics.stdev(returns) if len(returns) >= 2 else 0.0
    mean_reversion_z = (price_latest - price_mean) / price_std if price_std > 0 else 0.0

    if momentum > 0.01:
        trend = "UP"
    elif momentum < -0.01:
        trend = "DOWN"
    else:
        trend = "FLAT"

    sentiment_side = "YES" if momentum >= 0 else "NO"
    sentiment_confidence = min(1.0, 0.5 + abs(momentum) * 0.5)

    source_meta = event.get("meta") or {}
    return {
        "sentiment_side": sentiment_side,
        "sentiment_confidence": sentiment_confidence,
        "token_id": event.get("token_id"),
        "price_latest": price_latest,
        "price_earliest": price_earliest,
        "price_mean": price_mean,
        "price_std": price_std,
        "momentum": momentum,
        "volatility": volatility,
        "trend": trend,
        "mean_reversion_z": mean_reversion_z,
        "n_points": len(prices),
        "meta": {
            "source_signal_kind": _SOURCE_KIND,
            "source_signal_id": event.get("signal_id"),
            "interval": source_meta.get("interval"),
            "fidelity": source_meta.get("fidelity"),
        },
    }, "ok"


def main() -> None:
    run_signal_entrypoint(
        service_name="signal_price_history_derived",
        default_signal_id="signal-price-history-derived",
        default_signal_kind="price_history_derived",
        builder=_build_price_history_derived,
    )


if __name__ == "__main__":
    main()
