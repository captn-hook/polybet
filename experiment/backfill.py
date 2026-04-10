"""Backfill signal_outputs for resolved markets using gamma_markets payload data.

Generates payload-derived signals (market_implied, market_metadata, outcome_labels,
question) and writes them to the DB + publishes to NATS so running experiments
can process them and generate predictions against already-resolved markets.

Can be run standalone or called from experiment main.py at startup.
"""

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

COMMON_PYTHON = Path(__file__).resolve().parent / "common" / "python"
if str(COMMON_PYTHON) not in sys.path:
    sys.path.insert(0, str(COMMON_PYTHON))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick(d: dict, keys: list[str]) -> Any:
    for k in keys:
        if k in d:
            return d[k]
    return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SIGNAL_TOPIC = "signal.computed.v1"


def _make_envelope(signal_id: str, signal_kind: str, market_id: str,
                   question: str | None, details: dict) -> dict:
    return {
        "event_type": SIGNAL_TOPIC,
        "emitted_at": _iso_now(),
        "signal_id": signal_id,
        "signal_kind": signal_kind,
        "market_id": market_id,
        "market_question": question,
        "status": "ok",
        "reason": "ok",
        **details,
    }


# ---------------------------------------------------------------------------
# Signal builders
# ---------------------------------------------------------------------------

def build_market_implied(market_id: str, question: str | None, payload: dict) -> dict | None:
    prices = _pick(payload, ["outcomePrices", "outcome_prices"])
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except json.JSONDecodeError:
            return None
    if not isinstance(prices, list) or len(prices) < 2:
        return None
    yes_p = _as_float(prices[0])
    no_p = _as_float(prices[1])
    if yes_p is None or no_p is None:
        return None
    total = yes_p + no_p
    if total <= 0:
        return None
    yes_share = yes_p / total
    no_share = no_p / total
    side = "YES" if yes_p >= no_p else "NO"
    confidence = max(yes_share, no_share)
    details = {
        "sentiment_side": side, "sentiment_confidence": confidence,
        "yes_probability": yes_p, "no_probability": no_p,
        "yes_share": yes_share, "no_share": no_share,
        "margin": abs(yes_share - no_share),
    }
    sid = "backfill-market-implied"
    return {
        "signal_id": sid, "signal_kind": "market_implied",
        "market_id": market_id, "market_question": question,
        "sentiment_side": side, "sentiment_confidence": confidence,
        "payload_json": _make_envelope(sid, "market_implied", market_id, question, details),
    }


def build_market_metadata(market_id: str, question: str | None,
                          payload: dict, event: dict) -> dict | None:
    details = {
        "event_id": _pick(payload, ["eventId", "event_id"]) or event.get("event_id"),
        "slug": event.get("slug"),
        "start_date_raw": event.get("start_date_raw"),
        "end_date_raw": event.get("end_date_raw"),
        "active": event.get("active"),
        "closed": event.get("closed"),
        "accepting_orders": _pick(payload, ["acceptingOrders", "accepting_orders"]),
        "volume": _as_float(event.get("volume") or payload.get("volume")),
        "liquidity": _as_float(event.get("liquidity") or payload.get("liquidity")),
        "condition_id": _pick(payload, ["conditionId", "condition_id"]),
        "question_id": _pick(payload, ["questionID", "questionId", "question_id"]),
        "enable_order_book": _pick(payload, ["enableOrderBook", "enable_order_book"]),
    }
    sid = "backfill-market-metadata"
    return {
        "signal_id": sid, "signal_kind": "market_metadata",
        "market_id": market_id, "market_question": question,
        "sentiment_side": None, "sentiment_confidence": None,
        "payload_json": _make_envelope(sid, "market_metadata", market_id, question, details),
    }


def build_outcome_labels(market_id: str, question: str | None, payload: dict) -> dict | None:
    outcomes = _pick(payload, ["outcomes", "Outcomes"])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            return None
    if not isinstance(outcomes, list) or len(outcomes) == 0:
        return None
    outcomes = [str(o) for o in outcomes]
    details = {"outcomes": outcomes, "outcome_count": len(outcomes)}
    sid = "backfill-outcome-labels"
    return {
        "signal_id": sid, "signal_kind": "outcome_labels",
        "market_id": market_id, "market_question": question,
        "sentiment_side": None, "sentiment_confidence": None,
        "payload_json": _make_envelope(sid, "outcome_labels", market_id, question, details),
    }


def build_question(market_id: str, question: str | None) -> dict | None:
    if not question:
        return None
    sid = "backfill-question"
    return {
        "signal_id": sid, "signal_kind": "question",
        "market_id": market_id, "market_question": question,
        "sentiment_side": None, "sentiment_confidence": None,
        "payload_json": _make_envelope(sid, "question", market_id, question, {"question": question}),
    }


def build_clob_microstructure(market_id: str, question: str | None, payload: dict) -> dict | None:
    spread = _as_float(_pick(payload, ["spread"]))
    last_trade_price = _as_float(_pick(payload, ["lastTradePrice", "last_trade_price"]))
    if spread is None and last_trade_price is None:
        return None
    sid = "backfill-clob-microstructure"
    return {
        "signal_id": sid, "signal_kind": "clob_microstructure",
        "market_id": market_id, "market_question": question,
        "sentiment_side": None, "sentiment_confidence": None,
        "payload_json": _make_envelope(sid, "clob_microstructure", market_id, question, {
            "spread": spread, "last_trade_price": last_trade_price,
        }),
    }


def build_orderbook_depth_derived(market_id: str, question: str | None, payload: dict) -> dict | None:
    best_bid = _as_float(_pick(payload, ["bestBid", "best_bid"]))
    best_ask = _as_float(_pick(payload, ["bestAsk", "best_ask"]))
    if best_bid is None and best_ask is None:
        return None
    imbalance = None
    weighted_mid = None
    if best_bid is not None and best_ask is not None and (best_bid + best_ask) > 0:
        weighted_mid = (best_bid + best_ask) / 2.0
        imbalance = (best_bid - best_ask) / (best_bid + best_ask)
    sid = "backfill-orderbook-depth-derived"
    return {
        "signal_id": sid, "signal_kind": "orderbook_depth_derived",
        "market_id": market_id, "market_question": question,
        "sentiment_side": None, "sentiment_confidence": None,
        "payload_json": _make_envelope(sid, "orderbook_depth_derived", market_id, question, {
            "imbalance": imbalance, "weighted_mid": weighted_mid,
            "best_bid": best_bid, "best_ask": best_ask,
            "total_bid_size": None, "total_ask_size": None, "slippage_at_100": None,
        }),
    }


ALL_BUILDERS = [
    build_market_implied, build_market_metadata, build_outcome_labels,
    build_question, build_clob_microstructure, build_orderbook_depth_derived,
]


# ---------------------------------------------------------------------------
# Backfill runner
# ---------------------------------------------------------------------------

_CANDIDATE_QUERY = """
    SELECT gm.market_id, m.question, gm.payload_json,
           m.slug, m.start_date_raw, m.end_date_raw,
           gm.active, gm.closed, gm.volume, gm.liquidity
    FROM gamma_markets gm
    JOIN market_outcomes mo ON mo.market_id = gm.market_id
    JOIN markets m ON m.market_id = gm.market_id
    WHERE mo.winning_side IN ('YES', 'NO')
      AND NOT EXISTS (
          SELECT 1 FROM signal_outputs so
          WHERE so.market_id = gm.market_id
            AND so.signal_id LIKE 'backfill-%%'
      )
    ORDER BY mo.resolved_at DESC
"""

_SNAPSHOT_QUERY = """
    SELECT DISTINCT ON (market_id) market_id, payload_json
    FROM market_snapshots ms
    WHERE ms.market_id = ANY(%s)
      AND ms.end_date_raw IS NOT NULL
      AND ms.observed_at < ms.end_date_raw::timestamptz
    ORDER BY market_id, observed_at ASC
"""


@dataclass
class BackfillResult:
    markets_processed: int
    signals_written: int
    signals_published: int
    signals_skipped: int

    def __str__(self) -> str:
        return (
            f"markets_processed={self.markets_processed} "
            f"signals_written={self.signals_written} "
            f"signals_published={self.signals_published} "
            f"signals_skipped={self.signals_skipped}"
        )


class MarketBackfiller:
    def __init__(self, db_dsn: str, nats_client: Any = None, throttle_ms: int = 10):
        self.db_dsn = db_dsn
        self.nats = nats_client
        self.throttle_s = throttle_ms / 1000.0

    def fetch_unbackfilled_markets(self, limit: int = 0) -> list[dict]:
        query = _CANDIDATE_QUERY
        params: tuple = ()
        if limit > 0:
            query += " LIMIT %s"
            params = (limit,)
        with psycopg.connect(self.db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                if not rows:
                    return rows
                # Fetch pre-resolution snapshots for these markets only
                market_ids = [r["market_id"] for r in rows]
                cur.execute(_SNAPSHOT_QUERY, (market_ids,))
                snapshots = {mid: pj for mid, pj in cur.fetchall()}
                for row in rows:
                    snap = snapshots.get(row["market_id"])
                    if snap and isinstance(snap, dict):
                        row["payload_json"] = snap
                return rows

    def build_signals_for_market(self, row: dict) -> list[dict]:
        market_id = row["market_id"]
        question = row["question"]
        payload = row["payload_json"] if isinstance(row["payload_json"], dict) else {}
        event = {
            "slug": row.get("slug"), "start_date_raw": row.get("start_date_raw"),
            "end_date_raw": row.get("end_date_raw"), "active": row.get("active"),
            "closed": row.get("closed"), "volume": row.get("volume"),
            "liquidity": row.get("liquidity"),
        }
        signals = []
        for builder in ALL_BUILDERS:
            if builder is build_market_metadata:
                sig = builder(market_id, question, payload, event)
            elif builder is build_question:
                sig = builder(market_id, question)
            else:
                sig = builder(market_id, question, payload)
            if sig:
                signals.append(sig)
        return signals

    def write_signals_to_db(self, signals: list[dict]) -> int:
        if not signals:
            return 0
        with psycopg.connect(self.db_dsn) as conn:
            with conn.cursor() as cur:
                for sig in signals:
                    cur.execute(
                        """INSERT INTO signal_outputs (
                            signal_id, signal_kind, market_id, market_question,
                            sentiment_side, sentiment_confidence, payload_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (sig["signal_id"], sig["signal_kind"], sig["market_id"],
                         sig["market_question"], sig["sentiment_side"],
                         sig["sentiment_confidence"], json.dumps(sig["payload_json"])),
                    )
            conn.commit()
        return len(signals)

    async def publish_signals_to_nats(self, signals: list[dict]) -> int:
        if not self.nats or not signals:
            return 0
        for sig in signals:
            subject = f"{SIGNAL_TOPIC}.{sig['signal_kind']}"
            await self.nats.publish_json(subject, sig["payload_json"])
        return len(signals)

    async def run(self, limit: int = 0, emit_nats: bool = True) -> BackfillResult:
        rows = self.fetch_unbackfilled_markets(limit)
        total_written = 0
        total_published = 0
        total_skipped = 0
        for row in rows:
            signals = self.build_signals_for_market(row)
            total_skipped += len(ALL_BUILDERS) - len(signals)
            total_written += self.write_signals_to_db(signals)
            if emit_nats and self.nats:
                total_published += await self.publish_signals_to_nats(signals)
                if self.throttle_s > 0:
                    await asyncio.sleep(self.throttle_s)
        return BackfillResult(
            markets_processed=len(rows), signals_written=total_written,
            signals_published=total_published, signals_skipped=total_skipped,
        )


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def _run_standalone() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Backfill signal_outputs for resolved markets")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--db-only", action="store_true", help="Write to DB only, skip NATS")
    parser.add_argument("--dsn", default=os.getenv(
        "DATABASE_URL", "postgresql://polybet:polybet_dev_password@postgres:5432/polybet"))
    args = parser.parse_args()

    from polybet_nats import PolyNats
    nats_client = None
    if not args.db_only:
        nats_url = os.getenv("NATS_URL", "nats://nats:4222")
        nats_client = await PolyNats.connect(nats_url)
    try:
        backfiller = MarketBackfiller(db_dsn=args.dsn, nats_client=nats_client)
        result = await backfiller.run(limit=args.limit, emit_nats=not args.db_only)
        print(f"[backfill] {result}")
    finally:
        if nats_client:
            await nats_client.close()


if __name__ == "__main__":
    asyncio.run(_run_standalone())
