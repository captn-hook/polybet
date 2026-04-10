"""K-Means clustering experiment runner.

Three concurrent async tasks:
1. Signal accumulator — buffers signal.computed.v1 per market, predicts when complete
2. Resolution watcher — refits model on market.resolution.changed.v1
3. Timeout sweeper — cleans up stale incomplete signal sets
"""

import asyncio
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psycopg

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))
from polybet_nats import PolyNats

from kmeans_features import (
    REQUIRED_SIGNAL_KINDS,
    build_training_matrix,
    extract_feature_vector,
)
from kmeans_model import KMeansPredictor


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_config() -> dict[str, Any]:
    from exp_config import load_config
    return load_config()


class KMeansExperimentRunner:
    def __init__(
        self,
        config: dict[str, Any],
        nats_client: Any,
        db_dsn: str,
    ):
        self.config = config
        self.nats = nats_client
        self.db_dsn = db_dsn

        model_params = config.get("model", {}).get("params", {})
        self.experiment_id = config.get("experiment", {}).get("id", "exp-kmeans-clustering")
        self.prediction_topic = os.getenv("PREDICTION_OUTPUT_TOPIC", "prediction.proposed.v1")
        self.signal_timeout_s = config.get("data", {}).get("signal_timeout_seconds", 120)

        self.model = KMeansPredictor(
            n_clusters=model_params.get("n_clusters", 2),
            cold_start_min=model_params.get("cold_start_min_resolved", 3),
            random_state=model_params.get("random_state", 42),
        )

        # Per-market signal accumulation buffer
        self.pending_signals: dict[str, dict[str, dict[str, Any]]] = {}
        self.signal_first_seen: dict[str, float] = {}

    async def run(self) -> None:
        """Start all tasks and run until cancelled."""
        # Initial fit from existing DB data
        self._fit_from_db()

        signal_base = os.getenv("EXPERIMENT_SIGNAL_TOPIC", "signal.computed.v1")
        signal_topic = f"{signal_base}.*"
        resolution_topic = "market.resolution.changed.v1"

        signal_sub = await self.nats.subscribe(signal_topic)
        resolution_sub = await self.nats.subscribe(resolution_topic)

        print(
            f"[kmeans] started experiment_id={self.experiment_id} "
            f"signal_topic={signal_topic} resolution_topic={resolution_topic} "
            f"model_fitted={self.model.is_fitted} training_count={self.model.training_count}"
        )

        await asyncio.gather(
            self._signal_accumulator(signal_sub),
            self._resolution_watcher(resolution_sub),
            self._timeout_sweeper(),
        )

    async def _signal_accumulator(self, sub: Any) -> None:
        """Consume signal.computed.v1, buffer per market, predict when complete."""
        msg_count = 0
        while True:
            try:
                msg = await sub.next_msg(timeout=5)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                print(f"[kmeans] accumulator error: {type(exc).__name__}: {exc}")
                await asyncio.sleep(0.1)
                continue

            msg_count += 1
            event = PolyNats.decode_json(msg)
            if msg_count <= 5 or msg_count % 1000 == 0:
                print(f"[kmeans] accumulator msg#{msg_count} subject={msg.subject} kind={event.get('signal_kind')} market={event.get('market_id')} data={str(msg.data[:100])}")
            if str(event.get("status")) != "ok":
                continue
            signal_kind = str(event.get("signal_kind") or "")
            market_id = str(event.get("market_id") or "").strip()
            if not market_id or signal_kind not in REQUIRED_SIGNAL_KINDS:
                continue

            if market_id not in self.pending_signals:
                self.pending_signals[market_id] = {}
                self.signal_first_seen[market_id] = time.monotonic()

            self.pending_signals[market_id][signal_kind] = event

            # Check completeness
            received = set(self.pending_signals[market_id].keys())
            if received.issuperset(REQUIRED_SIGNAL_KINDS):
                signals = self.pending_signals.pop(market_id)
                self.signal_first_seen.pop(market_id, None)
                await self._predict_and_emit(market_id, signals)

    async def _resolution_watcher(self, sub: Any) -> None:
        """Consume market.resolution.changed.v1, batch refits to avoid blocking."""
        refit_interval = 300  # refit at most every 5 minutes
        last_refit = 0.0
        pending_resolutions = 0
        while True:
            try:
                msg = await sub.next_msg(timeout=10)
                event = PolyNats.decode_json(msg)
                if event.get("winning_side") in ("YES", "NO"):
                    pending_resolutions += 1
            except Exception:
                pass

            now = time.monotonic()
            if pending_resolutions > 0 and now - last_refit >= refit_interval:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._fit_from_db)
                print(f"[kmeans] refit ({pending_resolutions} resolutions) training_count={self.model.training_count}")
                pending_resolutions = 0
                last_refit = now

    async def _timeout_sweeper(self) -> None:
        """On timeout, predict with whatever signals arrived (model imputes missing features)."""
        while True:
            await asyncio.sleep(10)
            now = time.monotonic()
            expired = [
                mid for mid, first in self.signal_first_seen.items()
                if now - first > self.signal_timeout_s
            ]
            for market_id in expired:
                signals = self.pending_signals.pop(market_id, {})
                self.signal_first_seen.pop(market_id, None)
                if signals:
                    received = sorted(signals.keys())
                    missing = sorted(set(REQUIRED_SIGNAL_KINDS) - set(received))
                    print(f"[kmeans] partial predict market_id={market_id} received={received} missing={missing}")
                    await self._predict_and_emit(market_id, signals)

    async def _predict_and_emit(self, market_id: str, signals: dict[str, dict[str, Any]]) -> None:
        """Build feature vector, predict, and emit prediction."""
        if not self.model.is_fitted:
            print(f"[kmeans] skip prediction market_id={market_id} — cold start ({self.model.training_count} resolved)")
            return

        # Extract payload dicts from signal events
        signal_payloads = {}
        for kind, event in signals.items():
            signal_payloads[kind] = event

        vec = extract_feature_vector(signal_payloads)
        if vec is None:
            return

        side, confidence = self.model.predict(vec)
        market_question = None
        for event in signals.values():
            q = event.get("market_question")
            if q:
                market_question = q
                break

        await self.nats.publish_json(
            self.prediction_topic,
            {
                "event_type": "prediction.proposed.v1",
                "emitted_at": _iso_now(),
                "run_id": self.experiment_id,
                "experiment_id": self.experiment_id,
                "strategy": "kmeans_clustering",
                "market_id": market_id,
                "market_question": market_question,
                "market_end_date_raw": None,
                "side": side,
                "confidence": confidence,
                "horizon_minutes": 5,
                "rationale": f"K-Means cluster prediction (training_count={self.model.training_count})",
                "meta": {
                    "source": "experiment.kmeans",
                    "training_count": self.model.training_count,
                },
            },
        )
        print(f"[kmeans] predicted market_id={market_id} side={side} confidence={confidence}")

    async def _emit_error(self, error_code: str, message: str, context: dict[str, Any]) -> None:
        await self.nats.publish_json(
            "prediction.error.v1",
            {
                "event_type": "prediction.error.v1",
                "emitted_at": _iso_now(),
                "service": "experiment_kmeans",
                "error_code": error_code,
                "message": message,
                "context": context,
            },
        )

    def _fit_from_db(self) -> None:
        """Query DB for resolved markets with signal data and fit/refit model."""
        try:
            with psycopg.connect(self.db_dsn) as conn:
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
        except Exception as exc:
            print(f"[kmeans] DB query failed: {exc}")
            return

        # Group by market_id
        markets: dict[str, tuple[dict[str, dict], str]] = {}
        for market_id, signal_kind, payload_json, winning_side in rows:
            if market_id not in markets:
                markets[market_id] = ({}, winning_side)
            payload = payload_json if isinstance(payload_json, dict) else {}
            markets[market_id][0][signal_kind] = payload

        # Build training set
        training_rows = []
        for market_id, (signals, winning_side) in markets.items():
            vec = extract_feature_vector(signals)
            if vec is not None:
                training_rows.append((vec, winning_side))

        if not training_rows:
            print(f"[kmeans] no training data available ({len(markets)} resolved markets, 0 with signals)")
            return

        result = build_training_matrix(training_rows)
        if result is None:
            return
        X, y = result

        if self.model.fit(X, y):
            print(f"[kmeans] model fitted with {len(X)} markets, {self.model.n_clusters} clusters")
        else:
            print(f"[kmeans] insufficient data for fit ({len(X)} markets, need {self.model.cold_start_min})")


async def run_async() -> None:
    cfg = _load_config()
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    db_dsn = os.getenv("DATABASE_URL")
    if not db_dsn:
        raise RuntimeError("DATABASE_URL is required for kmeans experiment")

    nats_client = await PolyNats.connect(nats_url)
    runner = KMeansExperimentRunner(cfg, nats_client, db_dsn)
    await runner.run()


async def _emit_fatal_error(nats_url: str, message: str, tb: str) -> None:
    client = await PolyNats.connect(nats_url)
    try:
        await client.publish_json(
            "prediction.error.v1",
            {
                "event_type": "prediction.error.v1",
                "emitted_at": _iso_now(),
                "service": "experiment_kmeans",
                "error_code": "prediction.runtime.crash",
                "message": message,
                "context": {"traceback": tb},
            },
        )
    finally:
        await client.close()


def run() -> None:
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    try:
        asyncio.run(run_async())
    except Exception as exc:
        print(f"[kmeans] fatal error: {exc}", file=sys.stderr)
        traceback.print_exc()
        asyncio.run(_emit_fatal_error(nats_url, str(exc), traceback.format_exc()))
        raise
