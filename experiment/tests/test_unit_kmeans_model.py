from __future__ import annotations

import sys
from pathlib import Path

EXPERIMENT_DIR = Path(__file__).resolve().parents[1]
if str(EXPERIMENT_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENT_DIR))

import numpy as np

from model.features import FEATURE_DIM
from model.predictor import KMeansPredictor


def _make_training_data(n: int = 20) -> tuple[np.ndarray, np.ndarray]:
    """Create synthetic training data with two separable clusters."""
    rng = np.random.RandomState(42)
    half = n // 2
    # Cluster A: centered around 0.7 → YES
    cluster_a = rng.normal(loc=0.7, scale=0.1, size=(half, FEATURE_DIM))
    # Cluster B: centered around 0.3 → NO
    cluster_b = rng.normal(loc=0.3, scale=0.1, size=(n - half, FEATURE_DIM))
    X = np.vstack([cluster_a, cluster_b])
    y = np.array([1.0] * half + [0.0] * (n - half))
    return X, y


def test_cold_start_refuses_to_predict() -> None:
    model = KMeansPredictor(n_clusters=2, cold_start_min=3)
    X = np.random.rand(2, FEATURE_DIM)
    y = np.array([1.0, 0.0])
    assert model.fit(X, y) is False
    assert model.is_fitted is False


def test_fit_and_predict() -> None:
    model = KMeansPredictor(n_clusters=2, cold_start_min=3)
    X, y = _make_training_data(20)
    assert model.fit(X, y) is True
    assert model.is_fitted is True
    assert model.training_count == 20

    # Predict a YES-like sample
    yes_sample = np.full(FEATURE_DIM, 0.7)
    side, confidence = model.predict(yes_sample)
    assert side in ("YES", "NO")
    assert 0.5 <= confidence <= 1.0

    # Predict a NO-like sample
    no_sample = np.full(FEATURE_DIM, 0.3)
    side_no, conf_no = model.predict(no_sample)
    assert side_no in ("YES", "NO")
    assert 0.5 <= conf_no <= 1.0

    # The two predictions should differ in direction for well-separated data
    assert side != side_no


def test_refit_updates_model() -> None:
    model = KMeansPredictor(n_clusters=2, cold_start_min=3)
    X, y = _make_training_data(10)
    model.fit(X, y)
    assert model.training_count == 10

    X2, y2 = _make_training_data(30)
    model.refit(X2, y2)
    assert model.training_count == 30


def test_predict_handles_nan_in_input() -> None:
    model = KMeansPredictor(n_clusters=2, cold_start_min=3)
    X, y = _make_training_data(20)
    model.fit(X, y)

    # Input with some NaN values
    sample = np.full(FEATURE_DIM, 0.6)
    sample[0] = float("nan")
    sample[5] = float("nan")
    side, confidence = model.predict(sample)
    assert side in ("YES", "NO")
    assert 0.5 <= confidence <= 1.0
