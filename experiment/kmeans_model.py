#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "scikit-learn>=1.5.0",
#   "psycopg[binary]>=3.2.0",
#   "numpy>=1.26.0",
#   "matplotlib>=3.8.0",
# ]
# ///
"""K-Means model wrapper for binary market outcome prediction.

Confidence reflects geometric proximity to the YES cluster centroid rather than
a flat cluster yes-rate: a market deep inside the yes cluster scores near 1.0,
one sitting on the boundary scores near 0.5.

Standalone usage (fits on DB data, saves a PCA plot):
    uv run experiment/kmeans_model.py --plot kmeans_clusters.png
    uv run experiment/kmeans_model.py --plot clusters.png --n-clusters 3
    DATABASE_URL=postgresql://... uv run experiment/kmeans_model.py --plot out.png
"""

import os
import sys
from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).parent))
from kmeans_features import EMBEDDING_PCA_DIM, FEATURE_DIM, impute_single

_DEFAULT_DSN = (
    "postgresql://polybet_observe:polybet_observe_dev_password@localhost:5432/polybet"
)


class EmbeddingPCA:
    """Reduce 768-dim question embeddings to EMBEDDING_PCA_DIM components.

    Embeddings are StandardScaled per-dimension before PCA so no single
    embedding dimension dominates the principal components.  The resulting
    components are then appended to the microstructure feature vector; the
    KMeansPredictor's own StandardScaler handles the final joint normalisation.
    """

    def __init__(self, n_components: int = EMBEDDING_PCA_DIM):
        self.n_components = n_components
        self._scaler: StandardScaler = StandardScaler()
        self._pca: PCA | None = None

    @property
    def is_fitted(self) -> bool:
        return self._pca is not None

    def fit_transform(self, embeddings: np.ndarray) -> np.ndarray:
        X_scaled = self._scaler.fit_transform(embeddings)
        self._pca = PCA(n_components=self.n_components, random_state=42)
        result = self._pca.fit_transform(X_scaled)
        var = self._pca.explained_variance_ratio_.sum()
        print(f"  EmbeddingPCA: {self.n_components} components explain {var:.1%} of embedding variance")
        return result

    def transform(self, embeddings: np.ndarray) -> np.ndarray:
        if not self.is_fitted:
            raise RuntimeError("EmbeddingPCA not fitted")
        return self._pca.transform(self._scaler.transform(embeddings))


class KMeansPredictor:
    def __init__(self, n_clusters: int = 2, cold_start_min: int = 3, random_state: int = 42):
        self.n_clusters = n_clusters
        self.cold_start_min = cold_start_min
        self.random_state = random_state
        self.model: KMeans | None = None
        self.scaler: StandardScaler = StandardScaler()
        self.col_medians: np.ndarray = np.zeros(FEATURE_DIM)  # resized in fit()
        self.cluster_yes_rate: dict[int, float] = {}
        self.yes_cluster_id: int = 0  # cluster with the highest YES rate
        self.is_fitted: bool = False
        self.training_count: int = 0
        self.embedding_pca: EmbeddingPCA | None = None

    def fit(self, X: np.ndarray, y: np.ndarray, embedding_pca: "EmbeddingPCA | None" = None) -> bool:
        """Fit on training data. Returns False if insufficient samples.

        X may include appended embedding PCA components (produced by EmbeddingPCA).
        Pass the fitted EmbeddingPCA instance so predict() can project new embeddings
        at inference time.
        """
        if len(X) < self.cold_start_min:
            return False

        self.col_medians = np.nanmedian(X, axis=0)
        self.col_medians = np.nan_to_num(self.col_medians, nan=0.0)

        X_clean = X.copy()
        for col in range(X_clean.shape[1]):
            mask = np.isnan(X_clean[:, col])
            if mask.any():
                X_clean[mask, col] = self.col_medians[col]

        k = min(self.n_clusters, len(X_clean))
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X_clean)
        self.model = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
        self.model.fit(X_scaled)

        labels = self.model.labels_
        self.cluster_yes_rate = {}
        for cid in range(k):
            mask = labels == cid
            self.cluster_yes_rate[cid] = float(y[mask].mean()) if mask.sum() > 0 else 0.5

        # Track which cluster is the YES cluster so confidence is stable across refits
        self.yes_cluster_id = max(self.cluster_yes_rate, key=self.cluster_yes_rate.get)

        self.is_fitted = True
        self.training_count = len(X)
        self.embedding_pca = embedding_pca
        return True

    def predict(self, x: np.ndarray, embedding: "np.ndarray | None" = None) -> tuple[str, float]:
        """Predict side and confidence for a single feature vector.

        Confidence is distance-based: how much closer this point is to the YES
        cluster centroid vs the nearest other centroid.  A point at the YES
        centroid → 1.0; equidistant between clusters → 0.5.

        Returns ("YES"/"NO", confidence 0.5–1.0).
        """
        if not self.is_fitted or self.model is None:
            raise RuntimeError("Model not fitted")

        # Append embedding PCA components if the model was trained with them.
        # Fall back to zeros (approx. mean) when no embedding is available.
        if self.embedding_pca is not None and self.embedding_pca.is_fitted:
            if embedding is not None:
                emb_components = self.embedding_pca.transform(embedding.reshape(1, -1))[0]
            else:
                emb_components = np.zeros(self.embedding_pca.n_components)
            x = np.concatenate([x, emb_components])

        x_clean = impute_single(x, self.col_medians)
        x_scaled = self.scaler.transform(x_clean.reshape(1, -1))

        # Euclidean distances to every centroid in scaled space
        distances = self.model.transform(x_scaled)[0]  # shape (k,)
        d_yes = distances[self.yes_cluster_id]
        d_others = np.delete(distances, self.yes_cluster_id)
        d_nearest_other = d_others.min() if len(d_others) > 0 else d_yes + 1e-9

        # yes_prob = 1 when at yes centroid, 0 when at farthest other centroid
        yes_prob = float(d_nearest_other / (d_yes + d_nearest_other))
        side = "YES" if yes_prob >= 0.5 else "NO"
        confidence = round(max(0.5, min(1.0, max(yes_prob, 1 - yes_prob))), 4)
        return side, confidence

    def refit(self, X: np.ndarray, y: np.ndarray, embedding_pca: "EmbeddingPCA | None" = None) -> bool:
        """Re-fit from scratch with updated training data."""
        return self.fit(X, y, embedding_pca)

    def plot(self, X: np.ndarray, y: np.ndarray, output_path: str) -> None:
        """Save a PCA scatter plot of the fitted clusters to output_path.

        The yes cluster centroid is marked with ★.  Axes are clipped to the
        1st–99th percentile of each PC to suppress outlier distortion.
        """
        if not self.is_fitted or self.model is None:
            raise RuntimeError("Model not fitted — call fit() first")

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches

        # Impute + scale using the already-fitted scaler
        X_clean = X.copy()
        for col in range(X_clean.shape[1]):
            mask = np.isnan(X_clean[:, col])
            if mask.any():
                X_clean[mask, col] = self.col_medians[col]
        X_scaled = self.scaler.transform(X_clean)

        pca = PCA(n_components=2, random_state=self.random_state)
        X2 = pca.fit_transform(X_scaled)
        var = pca.explained_variance_ratio_
        centroids_2d = pca.transform(self.model.cluster_centers_)

        cluster_labels = self.model.labels_
        k = self.model.n_clusters

        # Axis limits: clip at 1st–99th percentile with 10% padding
        pc1_lo, pc1_hi = np.percentile(X2[:, 0], [1, 99])
        pc2_lo, pc2_hi = np.percentile(X2[:, 1], [1, 99])
        pad_x = max((pc1_hi - pc1_lo) * 0.1, 0.1)
        pad_y = max((pc2_hi - pc2_lo) * 0.1, 0.1)

        cluster_colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
        outcome_markers = {1.0: "o", 0.0: "X"}
        outcome_colors  = {1.0: "#2ca02c", 0.0: "#d62728"}

        fig, ax = plt.subplots(figsize=(9, 7))

        for cid in range(k):
            mask = cluster_labels == cid
            for outcome in [1.0, 0.0]:
                sel = mask & (y == outcome)
                if sel.sum() == 0:
                    continue
                ax.scatter(
                    X2[sel, 0], X2[sel, 1],
                    marker=outcome_markers[outcome],
                    color=outcome_colors[outcome],
                    alpha=0.5,
                    edgecolors=cluster_colors[cid % len(cluster_colors)],
                    linewidths=1.2,
                    s=40,
                    zorder=2,
                )

        for cid, (cx, cy) in enumerate(centroids_2d):
            yes_rate = self.cluster_yes_rate[cid]
            predicted_side = "YES" if yes_rate >= 0.5 else "NO"
            conf = yes_rate if yes_rate >= 0.5 else 1 - yes_rate
            star = "★ " if cid == self.yes_cluster_id else ""
            ax.scatter(cx, cy, marker="*", s=300,
                       color=cluster_colors[cid % len(cluster_colors)],
                       edgecolors="black", linewidths=0.8, zorder=5)
            ax.annotate(
                f" {star}C{cid}: {predicted_side}\n {conf:.0%} yes rate",
                (cx, cy), fontsize=9, fontweight="bold",
                color=cluster_colors[cid % len(cluster_colors)],
                va="bottom",
            )

        legend_handles = []
        for cid in range(k):
            yes_rate = self.cluster_yes_rate[cid]
            predicted_side = "YES" if yes_rate >= 0.5 else "NO"
            conf = yes_rate if yes_rate >= 0.5 else 1 - yes_rate
            star = " ★" if cid == self.yes_cluster_id else ""
            legend_handles.append(mpatches.Patch(
                color=cluster_colors[cid % len(cluster_colors)],
                label=(
                    f"Cluster {cid}{star} → {predicted_side} "
                    f"({conf:.0%} yes rate, n={int((cluster_labels == cid).sum())})"
                ),
            ))
        legend_handles += [
            plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#2ca02c",
                       markersize=8, label="Actual YES"),
            plt.Line2D([0], [0], marker="X", color="w", markerfacecolor="#d62728",
                       markersize=8, label="Actual NO"),
            plt.Line2D([0], [0], marker="*", color="w", markerfacecolor="grey",
                       markersize=12, label="Centroid  (★ = yes cluster)"),
        ]
        ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

        ax.set_xlim(pc1_lo - pad_x, pc1_hi + pad_x)
        ax.set_ylim(pc2_lo - pad_y, pc2_hi + pad_y)
        ax.set_xlabel(f"PC1 ({var[0]:.1%} variance)", fontsize=10)
        ax.set_ylabel(f"PC2 ({var[1]:.1%} variance)", fontsize=10)
        ax.set_title(
            f"K-Means Clusters (k={k}, n={len(X)} markets)\n"
            f"PCA of standardised features (axes clipped to 1st–99th pct)",
            fontsize=12,
        )
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(output_path, dpi=150)
        print(f"Saved: {output_path}")
        plt.close(fig)


# ---------------------------------------------------------------------------
# Standalone: fit on resolved DB markets and optionally save a cluster plot
# ---------------------------------------------------------------------------

def _load_training_data(dsn: str) -> tuple[np.ndarray, np.ndarray, list[str], "EmbeddingPCA | None"]:
    import psycopg
    from kmeans_features import REQUIRED_SIGNAL_KINDS, build_training_matrix, extract_feature_vector

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (so.market_id, so.signal_kind)
                  so.market_id, so.signal_kind, so.payload_json, mo.winning_side
                FROM signal_outputs so
                JOIN market_outcomes mo ON mo.market_id = so.market_id
                WHERE mo.winning_side IN ('YES', 'NO')
                  AND so.signal_kind = ANY(%s)
                  AND so.created_at < mo.resolved_at
                ORDER BY so.market_id, so.signal_kind, so.created_at DESC
                """,
                (REQUIRED_SIGNAL_KINDS,),
            )
            rows = cur.fetchall()

            cur.execute("SELECT market_id, embedding::float4[] FROM market_embeddings")
            raw_embeddings: dict[str, np.ndarray] = {
                r[0]: np.array(r[1], dtype=np.float32) for r in cur.fetchall()
            }

    markets: dict[str, tuple[dict, str]] = {}
    for market_id, signal_kind, payload_json, winning_side in rows:
        if market_id not in markets:
            markets[market_id] = ({}, winning_side)
        payload = payload_json if isinstance(payload_json, dict) else {}
        markets[market_id][0][signal_kind] = payload

    training_rows: list[tuple[np.ndarray, str]] = []
    market_ids: list[str] = []
    for market_id, (signals, winning_side) in markets.items():
        vec = extract_feature_vector(signals)
        if vec is not None:
            training_rows.append((vec, winning_side))
            market_ids.append(market_id)

    if not training_rows:
        raise RuntimeError("No training data found — are markets resolved with signals?")

    result = build_training_matrix(training_rows)
    if result is None:
        raise RuntimeError("build_training_matrix returned None")
    X, y = result

    # Append embedding PCA components when enough markets have embeddings.
    embedding_pca: EmbeddingPCA | None = None
    emb_vecs = [raw_embeddings.get(mid) for mid in market_ids]
    n_with_emb = sum(e is not None for e in emb_vecs)
    print(f"  Embeddings available: {n_with_emb}/{len(market_ids)} markets")

    if n_with_emb >= 50:
        emb_matrix = np.stack([e for e in emb_vecs if e is not None])
        embedding_pca = EmbeddingPCA()
        emb_projected = embedding_pca.fit_transform(emb_matrix)

        # Build full component matrix; zero-impute markets without embeddings.
        emb_components = np.zeros((len(market_ids), EMBEDDING_PCA_DIM))
        emb_idx = 0
        for i, e in enumerate(emb_vecs):
            if e is not None:
                emb_components[i] = emb_projected[emb_idx]
                emb_idx += 1

        X = np.concatenate([X, emb_components], axis=1)
        print(f"  Feature dim: {X.shape[1]} ({X.shape[1] - EMBEDDING_PCA_DIM} microstructure + {EMBEDDING_PCA_DIM} embedding PCA)")
    else:
        print(f"  Embedding PCA skipped: need >= 50 embeddings, have {n_with_emb}")

    return X, y, market_ids, embedding_pca


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="K-Means predictor — fit on DB data and plot")
    parser.add_argument("--plot", metavar="OUTPUT_PNG", default="kmeans_clusters.png",
                        help="Output path for PCA cluster plot (default: kmeans_clusters.png)")
    parser.add_argument("--n-clusters", type=int, default=2)
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    print("Connecting to DB…")
    X, y, market_ids, embedding_pca = _load_training_data(dsn)
    print(f"Loaded {len(X)} resolved markets with signals, feature_dim={X.shape[1]}")

    predictor = KMeansPredictor(n_clusters=args.n_clusters)
    predictor.fit(X, y, embedding_pca)

    yes_rate = predictor.cluster_yes_rate[predictor.yes_cluster_id]
    print(
        f"YES cluster: C{predictor.yes_cluster_id} "
        f"({yes_rate:.0%} yes rate, "
        f"n={int((predictor.model.labels_ == predictor.yes_cluster_id).sum())})"
    )

    predictor.plot(X, y, args.plot)
