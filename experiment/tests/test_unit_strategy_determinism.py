from __future__ import annotations

import random

from exp_strategy import decide_side


def test_probabilistic_strategy_is_deterministic_for_fixed_seed() -> None:
    rng_a = random.Random(1337)
    rng_b = random.Random(1337)

    seq_a = [decide_side("probabilistic", 0.62, rng_a)[0] for _ in range(25)]
    seq_b = [decide_side("probabilistic", 0.62, rng_b)[0] for _ in range(25)]

    assert seq_a == seq_b


def test_follow_signal_sentiment_rejects_invalid_side() -> None:
    rng = random.Random(1)
    try:
        decide_side(
            "follow_signal_sentiment",
            0.5,
            rng,
            signal_market={"signal_side": "MAYBE", "signal_confidence": 0.8},
        )
    except RuntimeError as exc:
        assert "Invalid signal side" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError for invalid signal side")
