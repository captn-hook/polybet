#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "scikit-learn>=1.5.0",
#   "psycopg[binary]>=3.2.0",
#   "numpy>=1.26.0",
# ]
# ///
"""Clear and backfill experiment_predictions for exp-kmeans-clustering.

Fits the model from resolved markets, then generates a prediction for every
market that has a complete set of signals in signal_outputs (latest per kind).

Usage:
    uv run experiment/backfill/predictions.py
    DATABASE_URL=postgresql://... uv run experiment/backfill/predictions.py
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import psycopg
from psycopg.rows import dict_row

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from model.features import REQUIRED_SIGNAL_KINDS, build_training_matrix, extract_feature_vector
from model.loader import load_resolved_signals
from model.predictor import KMeansPredictor

EXPERIMENT_ID = "exp-kmeans-clustering"
_DEFAULT_DSN = "postgresql://polybet:polybet_dev_password@localhost:5432/polybet"


def fit_model(cur: psycopg.Cursor) -> KMeansPredictor:
    # Use the all-signals variant (no temporal filter) for backfill
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
    for row in rows:
        mid, kind, payload, side = row["market_id"], row["signal_kind"], row["payload_json"], row["winning_side"]
        if mid not in markets:
            markets[mid] = ({}, side)
        markets[mid][0][kind] = payload if isinstance(payload, dict) else {}

    training_rows = []
    for signals, winning_side in markets.values():
        vec = extract_feature_vector(signals)
        if vec is not None:
            training_rows.append((vec, winning_side))

    if not training_rows:
        raise RuntimeError("No resolved markets with signals found — cannot fit model")

    result = build_training_matrix(training_rows)
    if result is None:
        raise RuntimeError("build_training_matrix returned None")
    X, y = result

    model = KMeansPredictor(n_clusters=2, cold_start_min=3, random_state=42)
    if not model.fit(X, y):
        raise RuntimeError(f"Insufficient training data ({len(X)} markets)")

    print(f"Model fitted on {len(X)} resolved markets, {model.n_clusters} clusters")
    for cid, yr in model.cluster_yes_rate.items():
        side = "YES" if yr >= 0.5 else "NO"
        conf = yr if yr >= 0.5 else 1 - yr
        n = int((model.model.labels_ == cid).sum())
        print(f"  Cluster {cid}: {side} ({conf:.1%} yes rate, n={n})")
    return model


def load_latest_signals(cur: psycopg.Cursor) -> dict[str, dict[str, dict]]:
    """Return {market_id: {signal_kind: payload}} using latest signal per market+kind."""
    cur.execute(
        """
        SELECT DISTINCT ON (market_id, signal_kind)
            market_id, signal_kind, payload_json, market_question
        FROM signal_outputs
        WHERE signal_kind = ANY(%s)
        ORDER BY market_id, signal_kind, created_at DESC
        """,
        (REQUIRED_SIGNAL_KINDS,),
    )
    markets: dict[str, dict] = {}
    for row in cur.fetchall():
        mid = row["market_id"]
        if mid not in markets:
            markets[mid] = {"signals": {}, "question": row["market_question"]}
        markets[mid]["signals"][row["signal_kind"]] = row["payload_json"] if isinstance(row["payload_json"], dict) else {}
    return markets


def main() -> None:
    dsn = os.getenv("DATABASE_URL", _DEFAULT_DSN)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Clear old predictions
            cur.execute(
                "DELETE FROM experiment_predictions WHERE experiment_id = %s",
                (EXPERIMENT_ID,),
            )
            deleted = cur.rowcount
            print(f"Deleted {deleted} existing predictions for {EXPERIMENT_ID}")

            # Fit model
            model = fit_model(cur)

            # Load latest signals per market
            all_markets = load_latest_signals(cur)
            print(f"Found {len(all_markets)} markets with at least one signal")

            # Predict for markets with complete signal sets
            predictions = []
            skipped = 0
            for market_id, data in all_markets.items():
                signals = data["signals"]
                if not set(REQUIRED_SIGNAL_KINDS).issubset(signals.keys()):
                    skipped += 1
                    continue
                vec = extract_feature_vector(signals)
                if vec is None:
                    skipped += 1
                    continue
                side, confidence = model.predict(vec)
                predictions.append({
                    "market_id": market_id,
                    "question": data["question"],
                    "side": side,
                    "confidence": confidence,
                })

            print(f"Generating {len(predictions)} predictions ({skipped} skipped — incomplete signals)")

            if predictions:
                cur.executemany(
                    """
                    INSERT INTO experiment_predictions
                        (experiment_id, strategy, market_id, market_question, side,
                         confidence, horizon_minutes, rationale, meta_json, created_at)
                    VALUES
                        (%(experiment_id)s, %(strategy)s, %(market_id)s, %(question)s, %(side)s,
                         %(confidence)s, 5, %(rationale)s, '{}', %(created_at)s)
                    """,
                    [
                        {
                            "experiment_id": EXPERIMENT_ID,
                            "strategy": "kmeans_clustering",
                            "market_id": p["market_id"],
                            "question": p["question"],
                            "side": p["side"],
                            "confidence": p["confidence"],
                            "rationale": f"backfill: K-Means cluster (training_count={model.training_count})",
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                        for p in predictions
                    ],
                )

        conn.commit()

    yes = sum(1 for p in predictions if p["side"] == "YES")
    no = len(predictions) - yes
    print(f"Done. YES={yes} ({yes/len(predictions):.1%}), NO={no} ({no/len(predictions):.1%})")


if __name__ == "__main__":
    main()
