"""Feature vector specification and extraction for K-Means clustering experiment."""

from typing import Any

import numpy as np

# Ordered feature spec: (signal_kind, field_name) -> vector index.
# ~30 features from 6 signal kinds.
FEATURE_SPEC: list[tuple[str, str]] = [
    # market_implied (5)
    ("market_implied", "yes_probability"),
    ("market_implied", "no_probability"),
    ("market_implied", "margin"),
    ("market_implied", "sentiment_side"),
    ("market_implied", "sentiment_confidence"),
    # clob_microstructure (2)
    ("clob_microstructure", "spread"),
    ("clob_microstructure", "last_trade_price"),
    # open_interest (1)
    ("open_interest", "open_interest"),
    # orderbook_depth_derived (5) — raw bid/ask sizes dropped, captured by imbalance ratio
    ("orderbook_depth_derived", "imbalance"),
    ("orderbook_depth_derived", "weighted_mid"),
    ("orderbook_depth_derived", "best_bid"),
    ("orderbook_depth_derived", "best_ask"),
    ("orderbook_depth_derived", "slippage_at_100"),
    # price_history_derived (8)
    ("price_history_derived", "momentum"),
    ("price_history_derived", "volatility"),
    ("price_history_derived", "trend"),
    ("price_history_derived", "mean_reversion_z"),
    ("price_history_derived", "price_latest"),
    ("price_history_derived", "price_earliest"),
    ("price_history_derived", "price_mean"),
    ("price_history_derived", "price_std"),
    # trade_flow_derived (5) — raw counts/sizes/net_flow dropped, ratios/derived kept
    ("trade_flow_derived", "buy_sell_ratio"),
    ("trade_flow_derived", "vwap_buy"),
    ("trade_flow_derived", "vwap_sell"),
    ("trade_flow_derived", "price_impact"),
    ("trade_flow_derived", "trade_velocity"),
]

FEATURE_DIM = len(FEATURE_SPEC)

# Number of PCA components extracted from the 768-dim question embedding and
# appended to the feature vector.  Captures ~56% of embedding variance while
# keeping the feature space manageable for KMeans.
EMBEDDING_PCA_DIM = 10

REQUIRED_SIGNAL_KINDS = sorted({kind for kind, _ in FEATURE_SPEC})

# Features that are naturally log-distributed — log1p applied before scaling
LOG1P_FEATURES: set[tuple[str, str]] = {
    ("open_interest", "open_interest"),
}

_CATEGORICAL_ENCODERS: dict[tuple[str, str], dict[str, float]] = {
    ("market_implied", "sentiment_side"): {"YES": 1.0, "NO": 0.0},
    ("price_history_derived", "trend"): {"UP": 1.0, "FLAT": 0.0, "DOWN": -1.0},
}


def _encode_value(signal_kind: str, field: str, value: Any) -> float:
    encoder = _CATEGORICAL_ENCODERS.get((signal_kind, field))
    if encoder is not None:
        if isinstance(value, str):
            return encoder.get(value.upper(), float("nan"))
        return float("nan")
    if value is None:
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def extract_feature_vector(signals: dict[str, dict[str, Any]]) -> np.ndarray | None:
    """Build a 1-D feature vector from accumulated signals per market.

    Args:
        signals: dict keyed by signal_kind, each value is the signal payload dict.

    Returns:
        1-D numpy array of length FEATURE_DIM, or None if no signal kinds present.
    """
    if not signals:
        return None
    vec = np.full(FEATURE_DIM, float("nan"))
    for i, (kind, field) in enumerate(FEATURE_SPEC):
        payload = signals.get(kind)
        if payload is None:
            continue
        raw = payload.get(field)
        val = _encode_value(kind, field, raw)
        if (kind, field) in LOG1P_FEATURES and not np.isnan(val):
            val = np.log1p(max(0.0, val))
        vec[i] = val
    return vec


def build_training_matrix(
    rows: list[tuple[np.ndarray, str]],
) -> tuple[np.ndarray, np.ndarray] | None:
    """Assemble training set from (feature_vector, winning_side) pairs.

    Returns (X, y) where y is 1 for YES, 0 for NO, or None if empty.
    NaN values are imputed with column medians.
    """
    if not rows:
        return None
    X = np.stack([r[0] for r in rows])
    y = np.array([1.0 if r[1].upper() == "YES" else 0.0 for r in rows])

    # Median imputation per column (KMeans cannot handle NaN)
    for col in range(X.shape[1]):
        mask = np.isnan(X[:, col])
        if mask.all():
            X[:, col] = 0.0
        elif mask.any():
            X[mask, col] = np.nanmedian(X[:, col])
    return X, y


def impute_single(x: np.ndarray, col_medians: np.ndarray) -> np.ndarray:
    """Impute NaN values in a single feature vector using pre-computed medians."""
    out = x.copy()
    mask = np.isnan(out)
    out[mask] = col_medians[mask]
    return out
