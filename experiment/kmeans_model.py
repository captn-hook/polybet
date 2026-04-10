"""K-Means model wrapper for binary market outcome prediction."""

import numpy as np
from sklearn.cluster import KMeans

from kmeans_features import FEATURE_DIM, impute_single


class KMeansPredictor:
    def __init__(self, n_clusters: int = 2, cold_start_min: int = 3, random_state: int = 42):
        self.n_clusters = n_clusters
        self.cold_start_min = cold_start_min
        self.random_state = random_state
        self.model: KMeans | None = None
        self.col_medians: np.ndarray = np.zeros(FEATURE_DIM)
        self.cluster_yes_rate: dict[int, float] = {}
        self.is_fitted: bool = False
        self.training_count: int = 0

    def fit(self, X: np.ndarray, y: np.ndarray) -> bool:
        """Fit on training data. Returns False if insufficient samples."""
        if len(X) < self.cold_start_min:
            return False

        # Compute column medians for imputation (ignoring NaN)
        self.col_medians = np.nanmedian(X, axis=0)
        self.col_medians = np.nan_to_num(self.col_medians, nan=0.0)

        # Impute NaN in training data
        X_clean = X.copy()
        for col in range(X_clean.shape[1]):
            mask = np.isnan(X_clean[:, col])
            if mask.any():
                X_clean[mask, col] = self.col_medians[col]

        # Fit KMeans
        k = min(self.n_clusters, len(X_clean))
        self.model = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
        self.model.fit(X_clean)

        # Determine cluster -> YES rate mapping
        labels = self.model.labels_
        self.cluster_yes_rate = {}
        for cluster_id in range(k):
            mask = labels == cluster_id
            if mask.sum() > 0:
                self.cluster_yes_rate[cluster_id] = float(y[mask].mean())
            else:
                self.cluster_yes_rate[cluster_id] = 0.5

        self.is_fitted = True
        self.training_count = len(X)
        return True

    def predict(self, x: np.ndarray) -> tuple[str, float]:
        """Predict side and confidence for a single feature vector.

        Returns ("YES"/"NO", confidence 0.5-1.0).
        """
        if not self.is_fitted or self.model is None:
            raise RuntimeError("Model not fitted")

        x_clean = impute_single(x, self.col_medians)
        cluster = self.model.predict(x_clean.reshape(1, -1))[0]
        yes_rate = self.cluster_yes_rate.get(cluster, 0.5)

        if yes_rate >= 0.5:
            side = "YES"
            confidence = yes_rate
        else:
            side = "NO"
            confidence = 1.0 - yes_rate

        # Clamp to [0.5, 1.0]
        confidence = max(0.5, min(1.0, confidence))
        return side, round(confidence, 4)

    def refit(self, X: np.ndarray, y: np.ndarray) -> bool:
        """Re-fit from scratch with updated training data."""
        return self.fit(X, y)
