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
"""Plot a labeled 2-D PCA projection of the fitted K-Means clusters.

Usage:
    uv run experiment/kmeans_plot.py
    uv run experiment/kmeans_plot.py --output clusters.png
    DATABASE_URL=postgresql://... uv run experiment/kmeans_plot.py
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import psycopg
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Allow importing feature helpers from same directory
sys.path.insert(0, str(Path(__file__).parent))
from kmeans_features import REQUIRED_SIGNAL_KINDS, build_training_matrix, extract_feature_vector

_DEFAULT_DSN = (
    "postgresql://polybet_observe:polybet_observe_dev_password@localhost:5432/polybet"
)


def load_training_data(dsn: str) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Query resolved markets with signals; return X, y, market_ids."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT so.market_id, so.signal_kind, so.payload_json, mo.winning_side
                FROM signal_outputs so
                JOIN market_outcomes mo ON mo.market_id = so.market_id
                WHERE mo.winning_side IN ('YES', 'NO')
                  AND so.signal_kind = ANY(%s)
                ORDER BY so.market_id, so.signal_kind
                """,
                (REQUIRED_SIGNAL_KINDS,),
            )
            rows = cur.fetchall()

    markets: dict[str, tuple[dict, str]] = {}
    for market_id, signal_kind, payload_json, winning_side in rows:
        if market_id not in markets:
            markets[market_id] = ({}, winning_side)
        payload = payload_json if isinstance(payload_json, dict) else {}
        markets[market_id][0][signal_kind] = payload

    training_rows = []
    market_ids = []
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
    return X, y, market_ids


def make_plot(output_path: str, n_clusters: int = 2, random_state: int = 42) -> None:
    dsn = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    print(f"Connecting to DB…")
    X, y, market_ids = load_training_data(dsn)
    print(f"Loaded {len(X)} resolved markets with signals")

    # Scale features before clustering and projection (prevents high-magnitude
    # features like open_interest from dominating PCA / KMeans distance)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Fit KMeans
    k = min(n_clusters, len(X))
    model = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    model.fit(X_scaled)
    cluster_labels = model.labels_

    # Cluster YES rates
    cluster_yes_rate = {}
    for cid in range(k):
        mask = cluster_labels == cid
        cluster_yes_rate[cid] = float(y[mask].mean()) if mask.sum() > 0 else 0.5

    # PCA to 2D
    pca = PCA(n_components=2, random_state=random_state)
    X2 = pca.fit_transform(X_scaled)
    var = pca.explained_variance_ratio_
    centroids_2d = pca.transform(model.cluster_centers_)

    # Clip axes to 1st–99th percentile so extreme outliers don't compress the view
    pc1_lo, pc1_hi = np.percentile(X2[:, 0], [1, 99])
    pc2_lo, pc2_hi = np.percentile(X2[:, 1], [1, 99])
    pad_x = max((pc1_hi - pc1_lo) * 0.1, 0.1)
    pad_y = max((pc2_hi - pc2_lo) * 0.1, 0.1)
    x_lim = (pc1_lo - pad_x, pc1_hi + pad_x)
    y_lim = (pc2_lo - pad_y, pc2_hi + pad_y)

    # Plot
    fig, ax = plt.subplots(figsize=(9, 7))

    cluster_colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
    outcome_markers = {1.0: "o", 0.0: "X"}  # YES=circle, NO=cross
    outcome_colors  = {1.0: "#2ca02c",       0.0: "#d62728"}

    for cid in range(k):
        mask = cluster_labels == cid
        yes_rate = cluster_yes_rate[cid]
        predicted_side = "YES" if yes_rate >= 0.5 else "NO"
        conf = yes_rate if yes_rate >= 0.5 else 1 - yes_rate
        label = f"Cluster {cid} → {predicted_side} ({conf:.0%} yes rate)"
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

    # Cluster centroids
    for cid, (cx, cy) in enumerate(centroids_2d):
        yes_rate = cluster_yes_rate[cid]
        predicted_side = "YES" if yes_rate >= 0.5 else "NO"
        conf = yes_rate if yes_rate >= 0.5 else 1 - yes_rate
        ax.scatter(cx, cy, marker="*", s=300, color=cluster_colors[cid % len(cluster_colors)],
                   edgecolors="black", linewidths=0.8, zorder=5)
        ax.annotate(
            f" C{cid}: {predicted_side}\n {conf:.0%} yes rate",
            (cx, cy), fontsize=9, fontweight="bold",
            color=cluster_colors[cid % len(cluster_colors)],
            va="bottom",
        )

    # Legend
    legend_handles = []
    for cid in range(k):
        yes_rate = cluster_yes_rate[cid]
        predicted_side = "YES" if yes_rate >= 0.5 else "NO"
        conf = yes_rate if yes_rate >= 0.5 else 1 - yes_rate
        legend_handles.append(
            mpatches.Patch(
                color=cluster_colors[cid % len(cluster_colors)],
                label=f"Cluster {cid} → {predicted_side} ({conf:.0%} yes rate, n={int((cluster_labels == cid).sum())})",
            )
        )
    legend_handles += [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor="#2ca02c", markersize=8, label="Actual YES"),
        plt.Line2D([0], [0], marker="X", color="w", markerfacecolor="#d62728", markersize=8, label="Actual NO"),
        plt.Line2D([0], [0], marker="*", color="w", markerfacecolor="grey", markersize=12, label="Centroid"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

    ax.set_xlim(*x_lim)
    ax.set_ylim(*y_lim)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot K-Means cluster projection")
    parser.add_argument("--output", default="kmeans_clusters.png", help="Output PNG path")
    parser.add_argument("--n-clusters", type=int, default=2)
    args = parser.parse_args()
    make_plot(args.output, n_clusters=args.n_clusters)
