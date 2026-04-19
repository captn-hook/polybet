#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg[binary]>=3.2.0",
#   "numpy>=1.26.0",
#   "scikit-learn>=1.5.0",
#   "matplotlib>=3.8.0",
# ]
# ///
"""PCA plot of question embeddings labelled by market outcome.

Usage:
    uv run experiment/analysis/plot_embeddings.py
    uv run experiment/analysis/plot_embeddings.py --output embeddings.png --sample 5000
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import psycopg
from psycopg.rows import dict_row
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler

_DEFAULT_DSN = (
    "postgresql://polybet_observe:polybet_observe_dev_password@localhost:5432/polybet"
)


def load(dsn: str, sample: int) -> tuple[np.ndarray, np.ndarray, int]:
    with psycopg.connect(dsn, row_factory=dict_row) as db:
        total = db.execute("""
            SELECT count(*) AS n FROM market_embeddings e
            JOIN market_outcomes o ON o.market_id = e.market_id
            WHERE o.winning_side IN ('YES', 'NO')
        """).fetchone()["n"]

        rows = db.execute("""
            SELECT e.embedding::float4[] AS embedding, o.winning_side
            FROM market_embeddings e
            JOIN market_outcomes o ON o.market_id = e.market_id
            WHERE o.winning_side IN ('YES', 'NO')
            ORDER BY random()
            LIMIT %s
        """, (sample,)).fetchall()

    X = np.array([r["embedding"] for r in rows], dtype=np.float32)
    y = np.array([1.0 if r["winning_side"] == "YES" else 0.0 for r in rows])
    return X, y, total


def analyse(X: np.ndarray, y: np.ndarray, X_scaled: np.ndarray, X2: np.ndarray, var: np.ndarray) -> None:
    """Print diagnostic stats about the embedding space and label separability."""
    n, dim = X.shape
    n_yes = int(y.sum())
    n_no = n - n_yes

    print()
    print("=" * 60)
    print("EMBEDDING ANALYSIS")
    print("=" * 60)
    print(f"  Embedding dim     : {dim}")
    print(f"  Samples           : {n:,}  (YES={n_yes:,}  NO={n_no:,}  balance={n_yes/n:.1%})")

    # Raw norms
    norms = np.linalg.norm(X, axis=1)
    print(f"  L2 norm           : mean={norms.mean():.3f}  std={norms.std():.3f}  "
          f"min={norms.min():.3f}  max={norms.max():.3f}")

    # YES vs NO centroid distance in original space
    yes_mask = y == 1.0
    centroid_yes = X_scaled[yes_mask].mean(axis=0)
    centroid_no  = X_scaled[~yes_mask].mean(axis=0)
    centroid_dist = np.linalg.norm(centroid_yes - centroid_no)
    print(f"  Centroid distance : {centroid_dist:.4f}  (scaled space, higher = more separable)")

    # Within-class variance vs total variance
    var_yes = X_scaled[yes_mask].var(axis=0).mean()
    var_no  = X_scaled[~yes_mask].var(axis=0).mean()
    var_total = X_scaled.var(axis=0).mean()
    print(f"  Within-class var  : YES={var_yes:.4f}  NO={var_no:.4f}  total={var_total:.4f}")
    print(f"  Between/total ratio: {(var_total - (n_yes*var_yes + n_no*var_no)/n) / var_total:.4f}  "
          f"(0=no separation, 1=perfect)")

    print()
    print("PCA VARIANCE BREAKDOWN")
    print("-" * 40)
    # Run a fuller PCA to see cumulative variance
    pca_full = PCA(n_components=min(50, dim, n), random_state=42)
    pca_full.fit(X_scaled)
    cum_var = np.cumsum(pca_full.explained_variance_ratio_)
    for k in [2, 5, 10, 20, 50]:
        if k <= len(cum_var):
            print(f"  Top {k:>2} PCs explain: {cum_var[k-1]:.1%} of variance")
    print(f"  PC1={var[0]:.2%}  PC2={var[1]:.2%}  (used for plot)")

    print()
    print("LINEAR SEPARABILITY (2D PCA)")
    print("-" * 40)
    lr_2d = LogisticRegression(max_iter=1000, random_state=42)
    cv_scores_2d = cross_val_score(lr_2d, X2, y, cv=5, scoring="roc_auc")
    print(f"  Logistic reg AUC on 2 PCs : {cv_scores_2d.mean():.4f} ± {cv_scores_2d.std():.4f}")

    print()
    print("LINEAR SEPARABILITY (full scaled embeddings)")
    print("-" * 40)
    lr_full = LogisticRegression(max_iter=1000, random_state=42, C=0.1)
    cv_scores_full = cross_val_score(lr_full, X_scaled, y, cv=5, scoring="roc_auc")
    print(f"  Logistic reg AUC (full dim): {cv_scores_full.mean():.4f} ± {cv_scores_full.std():.4f}")

    print()
    print("CLUSTER PURITY (YES vs NO in PCA 2D quadrants)")
    print("-" * 40)
    # Split into quadrants around PCA centroid and check YES rate per quadrant
    cx, cy = X2[:, 0].mean(), X2[:, 1].mean()
    for q_label, q_mask in [
        ("Q1 (+,+)", (X2[:, 0] >= cx) & (X2[:, 1] >= cy)),
        ("Q2 (-,+)", (X2[:, 0] <  cx) & (X2[:, 1] >= cy)),
        ("Q3 (-,-)", (X2[:, 0] <  cx) & (X2[:, 1] <  cy)),
        ("Q4 (+,-)", (X2[:, 0] >= cx) & (X2[:, 1] <  cy)),
    ]:
        n_q = q_mask.sum()
        if n_q == 0:
            continue
        yes_rate = y[q_mask].mean()
        print(f"  {q_label}: n={n_q:,}  YES rate={yes_rate:.1%}")

    print("=" * 60)
    print()


def main(output: str, sample: int) -> None:
    dsn = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    print(f"Loading up to {sample} embedded markets with outcomes…")
    X, y, total = load(dsn, sample)
    print(f"Loaded {len(X)} (of {total} total). YES={int(y.sum())} NO={int((1-y).sum())}")

    print("Scaling + PCA…")
    X_scaled = StandardScaler().fit_transform(X)
    pca = PCA(n_components=2, random_state=42)
    X2 = pca.fit_transform(X_scaled)
    var = pca.explained_variance_ratio_

    # Detailed analysis before clipping axes
    analyse(X, y, X_scaled, X2, var)

    # Clip axes for plotting
    X2_plot = X2.copy()
    for axis in range(2):
        lo, hi = np.percentile(X2_plot[:, axis], [1, 99])
        pad = max((hi - lo) * 0.05, 0.1)
        X2_plot[:, axis] = np.clip(X2_plot[:, axis], lo - pad, hi + pad)

    fig, ax = plt.subplots(figsize=(10, 8))

    for outcome, label, color, marker, alpha in [
        (1.0, "YES", "#2ca02c", "o", 0.35),
        (0.0, "NO",  "#d62728", "x", 0.35),
    ]:
        sel = y == outcome
        ax.scatter(
            X2_plot[sel, 0], X2_plot[sel, 1],
            c=color, marker=marker, alpha=alpha,
            s=12, linewidths=0.5,
            label=f"{label} (n={sel.sum():,})",
        )

    pc1_lo, pc1_hi = np.percentile(X2_plot[:, 0], [1, 99])
    pc2_lo, pc2_hi = np.percentile(X2_plot[:, 1], [1, 99])
    pad_x = max((pc1_hi - pc1_lo) * 0.1, 0.1)
    pad_y = max((pc2_hi - pc2_lo) * 0.1, 0.1)
    ax.set_xlim(pc1_lo - pad_x, pc1_hi + pad_x)
    ax.set_ylim(pc2_lo - pad_y, pc2_hi + pad_y)

    ax.set_xlabel(f"PC1 ({var[0]:.1%} variance)", fontsize=11)
    ax.set_ylabel(f"PC2 ({var[1]:.1%} variance)", fontsize=11)
    ax.set_title(
        f"Question Embeddings — PCA (nomic-embed-text, n={len(X):,} of {total:,})\n"
        f"Labelled by market outcome",
        fontsize=12,
    )
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(output, dpi=150)
    print(f"Saved: {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="embedding_clusters.png")
    parser.add_argument("--sample", type=int, default=8000,
                        help="Max points to plot (random sample, default 8000)")
    args = parser.parse_args()
    main(args.output, args.sample)
