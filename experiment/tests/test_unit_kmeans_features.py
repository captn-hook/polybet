from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_DIR = Path(__file__).resolve().parents[1]
if str(EXPERIMENT_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENT_DIR))

import numpy as np

from kmeans_features import (
    FEATURE_DIM,
    FEATURE_SPEC,
    REQUIRED_SIGNAL_KINDS,
    build_training_matrix,
    extract_feature_vector,
)


def _make_signal(kind: str, **overrides: object) -> dict[str, object]:
    """Build a minimal signal payload for a given kind with defaults."""
    defaults: dict[str, dict[str, object]] = {
        "market_implied": {
            "yes_probability": 0.6, "no_probability": 0.4, "margin": 0.2,
            "sentiment_side": "YES", "sentiment_confidence": 0.6,
        },
        "clob_microstructure": {"spread": 0.02, "last_trade_price": 0.59},
        "open_interest": {"open_interest": 50000.0},
        "orderbook_depth_derived": {
            "imbalance": 0.15, "weighted_mid": 0.58, "best_bid": 0.57, "best_ask": 0.59,
            "total_bid_size": 10000.0, "total_ask_size": 8500.0, "slippage_at_100": 0.005,
        },
        "price_history_derived": {
            "momentum": 0.03, "volatility": 0.01, "trend": "UP", "mean_reversion_z": 0.5,
            "price_latest": 0.6, "price_earliest": 0.55, "price_mean": 0.57, "price_std": 0.02,
        },
        "trade_flow_derived": {
            "net_flow": 200.0, "buy_sell_ratio": 1.5, "vwap_buy": 0.59, "vwap_sell": 0.56,
            "price_impact": 0.03, "trade_velocity": 12.0,
            "buy_count": 30, "sell_count": 20, "buy_size": 500.0, "sell_size": 300.0,
        },
    }
    payload = dict(defaults.get(kind, {}))
    payload.update(overrides)
    return payload


def _make_full_signals() -> dict[str, dict[str, object]]:
    return {kind: _make_signal(kind) for kind in REQUIRED_SIGNAL_KINDS}


def test_feature_dim_matches_spec() -> None:
    assert FEATURE_DIM == len(FEATURE_SPEC)
    assert FEATURE_DIM == 33


def test_required_signal_kinds() -> None:
    expected = {
        "clob_microstructure", "market_implied", "open_interest",
        "orderbook_depth_derived", "price_history_derived", "trade_flow_derived",
    }
    assert set(REQUIRED_SIGNAL_KINDS) == expected


def test_extract_complete_vector() -> None:
    signals = _make_full_signals()
    vec = extract_feature_vector(signals)
    assert vec is not None
    assert vec.shape == (FEATURE_DIM,)
    assert not np.any(np.isnan(vec))


def test_extract_empty_returns_none() -> None:
    assert extract_feature_vector({}) is None


def test_extract_missing_kind_gives_nan() -> None:
    signals = _make_full_signals()
    del signals["open_interest"]
    vec = extract_feature_vector(signals)
    assert vec is not None
    # open_interest field should be NaN
    oi_idx = next(i for i, (k, f) in enumerate(FEATURE_SPEC) if k == "open_interest")
    assert np.isnan(vec[oi_idx])
    # other fields should be filled
    mi_idx = next(i for i, (k, f) in enumerate(FEATURE_SPEC) if k == "market_implied" and f == "yes_probability")
    assert not np.isnan(vec[mi_idx])


def test_categorical_encoding_sentiment_side() -> None:
    signals = _make_full_signals()
    signals["market_implied"] = _make_signal("market_implied", sentiment_side="NO")
    vec = extract_feature_vector(signals)
    idx = next(i for i, (k, f) in enumerate(FEATURE_SPEC) if k == "market_implied" and f == "sentiment_side")
    assert vec[idx] == 0.0


def test_categorical_encoding_trend() -> None:
    signals = _make_full_signals()
    signals["price_history_derived"] = _make_signal("price_history_derived", trend="DOWN")
    vec = extract_feature_vector(signals)
    idx = next(i for i, (k, f) in enumerate(FEATURE_SPEC) if k == "price_history_derived" and f == "trend")
    assert vec[idx] == -1.0


def test_build_training_matrix_imputes_nan() -> None:
    # One row with complete data, one with missing open_interest
    full = _make_full_signals()
    vec1 = extract_feature_vector(full)

    partial = _make_full_signals()
    del partial["open_interest"]
    vec2 = extract_feature_vector(partial)

    result = build_training_matrix([(vec1, "YES"), (vec2, "NO")])
    assert result is not None
    X, y = result
    assert X.shape == (2, FEATURE_DIM)
    assert not np.any(np.isnan(X)), "NaN should be imputed"
    assert y[0] == 1.0
    assert y[1] == 0.0


def test_build_training_matrix_empty() -> None:
    assert build_training_matrix([]) is None
