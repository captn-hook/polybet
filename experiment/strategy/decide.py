import random
from typing import Any


def normalized_strategy_mode(strategy_mode: str) -> str:
    return strategy_mode.strip().lower()


def decide_side(
    strategy_mode: str,
    yes_probability: float,
    rng: random.Random,
    signal_market: dict[str, Any] | None = None,
) -> tuple[str, float] | None:
    mode = normalized_strategy_mode(strategy_mode)
    if mode == "always_yes":
        return "YES", 1.0
    if mode == "always_no":
        return "NO", 1.0
    if mode == "follow_signal_sentiment":
        if signal_market is None:
            return None
        side = str(signal_market["signal_side"]).upper()
        if side not in {"YES", "NO"}:
            return None
        confidence = float(signal_market.get("signal_confidence", 0.5))
        return side, round(max(0.0, min(1.0, confidence)), 4)
    if mode == "against_signal_sentiment":
        if signal_market is None:
            return None
        signal_side = str(signal_market["signal_side"]).upper()
        if signal_side not in {"YES", "NO"}:
            return None
        side = "NO" if signal_side == "YES" else "YES"
        confidence = float(signal_market.get("signal_confidence", 0.5))
        return side, round(max(0.0, min(1.0, confidence)), 4)
    if mode == "pass_through":
        return "YES", 0.5
    if mode in {"probabilistic", "random"}:
        p = max(0.0, min(1.0, yes_probability))
        side = "YES" if rng.random() < p else "NO"
        confidence = max(p, 1.0 - p)
        return side, round(confidence, 4)
    raise RuntimeError(f"Unsupported strategy_mode: {strategy_mode}")
