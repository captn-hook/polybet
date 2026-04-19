#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg[binary]>=3.2.0",
#   "numpy>=1.26.0",
#   "scikit-learn>=1.5.0",
# ]
# ///
"""K-fold cross-validation for the KMeans predictor.

Compares two feature sets on held-out data:
  A) microstructure only  (26 features)
  B) microstructure + 10 embedding PCA components (36 features)

Metrics per fold and aggregated:
  - AUC  (distance-to-YES-centroid as score)
  - Accuracy
  - YES precision  (= cluster purity on held-out test set)
  - YES recall     (fraction of YES markets captured by the YES cluster)

Usage:
    uv run experiment/analysis/cv.py
    uv run experiment/analysis/cv.py --folds 10 --n-clusters 2
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import psycopg
from sklearn.cluster import KMeans
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from model.features import (
    EMBEDDING_PCA_DIM,
    REQUIRED_SIGNAL_KINDS,
    build_training_matrix,
    extract_feature_vector,
)
from model.predictor import EmbeddingPCA
from model.loader import load_resolved_signals

_DEFAULT_DSN = (
    "postgresql://polybet_observe:polybet_observe_dev_password@localhost:5432/polybet"
)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_all(dsn: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (X_micro, X_emb, y) for all resolved markets with full signals.

    X_micro : (n, 26)  raw microstructure features (median-imputed)
    X_emb   : (n, 768) raw unit-normalised embeddings  (zeros if missing)
    y       : (n,)     1=YES, 0=NO
    """
    print("Loading signal data from DB…")
    markets, raw_embeddings = load_resolved_signals(dsn, list(REQUIRED_SIGNAL_KINDS))

    training_rows = []
    market_ids = []
    for mid, (signals, winning_side) in markets.items():
        vec = extract_feature_vector(signals)
        if vec is not None:
            training_rows.append((vec, winning_side))
            market_ids.append(mid)

    result = build_training_matrix(training_rows)
    if result is None:
        raise RuntimeError("No training data")
    X_micro, y = result

    emb_dim = next(iter(raw_embeddings.values())).shape[0] if raw_embeddings else 768
    X_emb = np.zeros((len(market_ids), emb_dim), dtype=np.float32)
    n_with_emb = 0
    for i, mid in enumerate(market_ids):
        e = raw_embeddings.get(mid)
        if e is not None:
            X_emb[i] = e
            n_with_emb += 1

    print(
        f"Loaded {len(market_ids)} markets | "
        f"YES={int(y.sum())} NO={int((1-y).sum())} "
        f"({y.mean():.1%} YES) | "
        f"embeddings={n_with_emb}/{len(market_ids)}"
    )
    return X_micro, X_emb, y


# ---------------------------------------------------------------------------
# Per-fold evaluation helpers
# ---------------------------------------------------------------------------

def _predict_scores(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    n_clusters: int,
) -> tuple[np.ndarray, int]:
    """Fit KMeans on X_train, return (distance-to-YES-centroid scores for X_test, yes_cluster_id).

    Scores are inverted distances so higher = more YES-like (for AUC).
    """
    scaler = StandardScaler()
    X_tr_scaled = scaler.fit_transform(X_train)
    X_te_scaled = scaler.transform(X_test)

    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    km.fit(X_tr_scaled)

    # Identify YES cluster by YES rate on training labels
    cluster_yes_rate = {}
    for cid in range(n_clusters):
        mask = km.labels_ == cid
        cluster_yes_rate[cid] = float(y_train[mask].mean()) if mask.sum() > 0 else 0.5
    yes_cid = max(cluster_yes_rate, key=cluster_yes_rate.get)

    # Score = proximity to YES centroid (higher → more YES-like)
    distances = km.transform(X_te_scaled)          # (n_test, k)
    d_yes = distances[:, yes_cid]
    # Use negative distance: closer to YES centroid → higher score
    scores = -d_yes
    return scores, yes_cid, cluster_yes_rate[yes_cid]


def eval_fold(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    n_clusters: int,
) -> dict:
    scores, yes_cid, train_yes_rate = _predict_scores(X_train, y_train, X_test, n_clusters)

    # Hard prediction: top-k% of scores called YES (same fraction as training YES rate)
    # Use distance threshold: assign to YES if score > median score of training YES cluster scores
    # Simpler: refit KMeans on test with same centroids — just assign by nearest centroid
    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_train)
    X_te_s = scaler.transform(X_test)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    km.fit(X_tr_s)
    cluster_yes_rate = {}
    for cid in range(n_clusters):
        mask = km.labels_ == cid
        cluster_yes_rate[cid] = float(y_train[mask].mean()) if mask.sum() > 0 else 0.5
    yes_cid = max(cluster_yes_rate, key=cluster_yes_rate.get)

    test_labels = km.predict(X_te_s)
    y_pred = (test_labels == yes_cid).astype(float)

    auc = roc_auc_score(y_test, scores)
    acc = float((y_pred == y_test).mean())

    # YES precision and recall on test set
    tp = float(((y_pred == 1) & (y_test == 1)).sum())
    fp = float(((y_pred == 1) & (y_test == 0)).sum())
    fn = float(((y_pred == 0) & (y_test == 1)).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
    recall    = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
    yes_pred_count = int(y_pred.sum())
    train_yes_rate_in_yes_cluster = cluster_yes_rate[yes_cid]

    return {
        "auc": auc,
        "acc": acc,
        "yes_precision": precision,
        "yes_recall": recall,
        "yes_pred_n": yes_pred_count,
        "train_yes_cluster_purity": train_yes_rate_in_yes_cluster,
    }


# ---------------------------------------------------------------------------
# Main CV loop
# ---------------------------------------------------------------------------

def run_cv(X_micro: np.ndarray, X_emb: np.ndarray, y: np.ndarray, n_folds: int, n_clusters: int) -> None:
    skf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=42)

    results_micro: list[dict] = []
    results_combined: list[dict] = []

    print()
    print(f"{'='*70}")
    print(f"K-FOLD CV  (k={n_folds} folds, n_clusters={n_clusters})")
    print(f"{'='*70}")

    header = f"{'Fold':>4}  {'AUC':>6}  {'Acc':>6}  {'YES prec':>9}  {'YES rec':>8}  {'YES n':>6}  {'Train purity':>13}"
    sep    = "-" * len(header)

    for label, results_list, use_emb in [
        ("A) Microstructure only (26 features)", results_micro, False),
        ("B) Micro + Embedding PCA (36 features)", results_combined, True),
    ]:
        print()
        print(f"  {label}")
        print(f"  {header}")
        print(f"  {sep}")

        for fold_idx, (train_idx, test_idx) in enumerate(skf.split(X_micro, y)):
            y_train, y_test = y[train_idx], y[test_idx]
            X_tr_m, X_te_m = X_micro[train_idx], X_micro[test_idx]

            if use_emb:
                # Fit EmbeddingPCA on training embeddings, project both splits
                emb_train = X_emb[train_idx]
                emb_test  = X_emb[test_idx]
                epca = EmbeddingPCA(n_components=EMBEDDING_PCA_DIM)
                emb_tr_proj = epca.fit_transform(emb_train)
                emb_te_proj = epca.transform(emb_test)
                X_tr = np.concatenate([X_tr_m, emb_tr_proj], axis=1)
                X_te = np.concatenate([X_te_m, emb_te_proj], axis=1)
            else:
                X_tr, X_te = X_tr_m, X_te_m

            metrics = eval_fold(X_tr, y_train, X_te, y_test, n_clusters)
            results_list.append(metrics)

            print(
                f"  {fold_idx+1:>4}  "
                f"{metrics['auc']:>6.4f}  "
                f"{metrics['acc']:>6.1%}  "
                f"{metrics['yes_precision']:>9.1%}  "
                f"{metrics['yes_recall']:>8.1%}  "
                f"{metrics['yes_pred_n']:>6}  "
                f"{metrics['train_yes_cluster_purity']:>13.1%}"
            )

        # Aggregate
        print(f"  {sep}")
        for key, label_s in [
            ("auc",         "mean AUC"),
            ("acc",         "mean Acc"),
            ("yes_precision","mean YES prec"),
            ("yes_recall",   "mean YES rec"),
        ]:
            vals = [r[key] for r in results_list if not (isinstance(r[key], float) and r[key] != r[key])]
            print(f"  {label_s:>15}: {np.mean(vals):.4f}  (std {np.std(vals):.4f})")

    # Delta summary
    print()
    print(f"{'='*70}")
    print("DELTA: Combined - Micro-only")
    print(f"{'='*70}")
    for key, label_s in [
        ("auc",          "AUC"),
        ("acc",          "Accuracy"),
        ("yes_precision","YES precision"),
        ("yes_recall",   "YES recall"),
    ]:
        vals_m = np.array([r[key] for r in results_micro])
        vals_c = np.array([r[key] for r in results_combined])
        delta = vals_c - vals_m
        print(f"  {label_s:>14}: {delta.mean():+.4f}  (per-fold range [{delta.min():+.4f}, {delta.max():+.4f}])")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--folds",      type=int, default=5)
    parser.add_argument("--n-clusters", type=int, default=2)
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL", _DEFAULT_DSN)
    X_micro, X_emb, y = load_all(dsn)
    run_cv(X_micro, X_emb, y, n_folds=args.folds, n_clusters=args.n_clusters)
