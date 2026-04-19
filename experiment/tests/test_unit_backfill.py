from __future__ import annotations

import asyncio
import sys
from pathlib import Path

EXPERIMENT_DIR = Path(__file__).resolve().parents[1]
if str(EXPERIMENT_DIR) not in sys.path:
    sys.path.insert(0, str(EXPERIMENT_DIR))

from backfill.signals import (
    ALL_BUILDERS,
    BackfillResult,
    MarketBackfiller,
    build_clob_microstructure,
    build_market_implied,
    build_market_metadata,
    build_orderbook_depth_derived,
    build_outcome_labels,
    build_question,
)

# ---------------------------------------------------------------------------
# Sample payloads
# ---------------------------------------------------------------------------

_SAMPLE_PAYLOAD = {
    "outcomePrices": '["0.6", "0.4"]',
    "outcomes": '["Yes", "No"]',
    "eventId": "evt-123",
    "conditionId": "cond-456",
    "questionID": "q-789",
    "acceptingOrders": True,
    "enableOrderBook": True,
    "volume": 10000,
    "liquidity": 5000,
    "spread": 0.05,
    "lastTradePrice": 0.59,
    "bestBid": 0.57,
    "bestAsk": 0.62,
}

_SAMPLE_EVENT = {
    "slug": "will-x-happen",
    "start_date_raw": "2026-01-01T00:00:00Z",
    "end_date_raw": "2026-04-01T00:00:00Z",
    "active": True,
    "closed": False,
    "volume": 10000,
    "liquidity": 5000,
}


# ---------------------------------------------------------------------------
# build_market_implied
# ---------------------------------------------------------------------------

class TestBuildMarketImplied:
    def test_correct_structure(self):
        sig = build_market_implied("m1", "Will X?", _SAMPLE_PAYLOAD)
        assert sig is not None
        assert sig["signal_id"] == "backfill-market-implied"
        assert sig["signal_kind"] == "market_implied"
        assert sig["market_id"] == "m1"
        assert sig["sentiment_side"] == "YES"
        assert sig["sentiment_confidence"] == 0.6
        pj = sig["payload_json"]
        assert pj["event_type"] == "signal.computed.v1"
        assert pj["status"] == "ok"
        assert pj["reason"] == "ok"
        assert pj["yes_probability"] == 0.6
        assert pj["no_probability"] == 0.4
        assert "emitted_at" in pj

    def test_missing_prices_returns_none(self):
        assert build_market_implied("m1", "Q?", {}) is None

    def test_zero_total_returns_none(self):
        assert build_market_implied("m1", "Q?", {"outcomePrices": ["0", "0"]}) is None

    def test_list_prices(self):
        sig = build_market_implied("m1", "Q?", {"outcomePrices": [0.7, 0.3]})
        assert sig is not None
        assert sig["sentiment_side"] == "YES"
        assert abs(sig["sentiment_confidence"] - 0.7) < 1e-9

    def test_malformed_json_string(self):
        assert build_market_implied("m1", "Q?", {"outcomePrices": "not json"}) is None


# ---------------------------------------------------------------------------
# build_market_metadata
# ---------------------------------------------------------------------------

class TestBuildMarketMetadata:
    def test_extracts_all_fields(self):
        sig = build_market_metadata("m1", "Q?", _SAMPLE_PAYLOAD, _SAMPLE_EVENT)
        assert sig is not None
        assert sig["signal_kind"] == "market_metadata"
        pj = sig["payload_json"]
        assert pj["event_id"] == "evt-123"
        assert pj["slug"] == "will-x-happen"
        assert pj["condition_id"] == "cond-456"
        assert pj["volume"] == 10000.0
        assert pj["liquidity"] == 5000.0

    def test_always_returns_result(self):
        sig = build_market_metadata("m1", "Q?", {}, {})
        assert sig is not None
        assert sig["signal_kind"] == "market_metadata"


# ---------------------------------------------------------------------------
# build_outcome_labels
# ---------------------------------------------------------------------------

class TestBuildOutcomeLabels:
    def test_parses_json_string(self):
        sig = build_outcome_labels("m1", "Q?", {"outcomes": '["Yes", "No"]'})
        assert sig is not None
        assert sig["payload_json"]["outcomes"] == ["Yes", "No"]
        assert sig["payload_json"]["outcome_count"] == 2

    def test_list_input(self):
        sig = build_outcome_labels("m1", "Q?", {"outcomes": ["A", "B", "C"]})
        assert sig is not None
        assert sig["payload_json"]["outcome_count"] == 3

    def test_empty_returns_none(self):
        assert build_outcome_labels("m1", "Q?", {}) is None
        assert build_outcome_labels("m1", "Q?", {"outcomes": "[]"}) is None


# ---------------------------------------------------------------------------
# build_question
# ---------------------------------------------------------------------------

class TestBuildQuestion:
    def test_valid(self):
        sig = build_question("m1", "Will it rain?")
        assert sig is not None
        assert sig["payload_json"]["question"] == "Will it rain?"
        assert sig["signal_kind"] == "question"

    def test_empty_returns_none(self):
        assert build_question("m1", "") is None
        assert build_question("m1", None) is None


# ---------------------------------------------------------------------------
# MarketBackfiller.build_signals_for_market
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# build_clob_microstructure
# ---------------------------------------------------------------------------

class TestBuildClobMicrostructure:
    def test_extracts_spread_and_price(self):
        sig = build_clob_microstructure("m1", "Q?", _SAMPLE_PAYLOAD)
        assert sig is not None
        assert sig["signal_kind"] == "clob_microstructure"
        assert sig["payload_json"]["spread"] == 0.05
        assert sig["payload_json"]["last_trade_price"] == 0.59

    def test_missing_both_returns_none(self):
        assert build_clob_microstructure("m1", "Q?", {}) is None

    def test_partial_ok(self):
        sig = build_clob_microstructure("m1", "Q?", {"spread": 0.03})
        assert sig is not None
        assert sig["payload_json"]["spread"] == 0.03


# ---------------------------------------------------------------------------
# build_orderbook_depth_derived
# ---------------------------------------------------------------------------

class TestBuildOrderbookDepthDerived:
    def test_computes_mid_and_imbalance(self):
        sig = build_orderbook_depth_derived("m1", "Q?", _SAMPLE_PAYLOAD)
        assert sig is not None
        assert sig["signal_kind"] == "orderbook_depth_derived"
        pj = sig["payload_json"]
        assert pj["best_bid"] == 0.57
        assert pj["best_ask"] == 0.62
        assert abs(pj["weighted_mid"] - 0.595) < 1e-9
        assert pj["imbalance"] is not None

    def test_missing_both_returns_none(self):
        assert build_orderbook_depth_derived("m1", "Q?", {}) is None


# ---------------------------------------------------------------------------
# MarketBackfiller.build_signals_for_market
# ---------------------------------------------------------------------------

class TestBuildSignalsForMarket:
    def _make_row(self, **overrides):
        row = {
            "market_id": "m1", "question": "Will X?",
            "payload_json": _SAMPLE_PAYLOAD,
            **_SAMPLE_EVENT,
        }
        row.update(overrides)
        return row

    def test_all_six_kinds(self):
        bf = MarketBackfiller(db_dsn="unused")
        signals = bf.build_signals_for_market(self._make_row())
        kinds = {s["signal_kind"] for s in signals}
        assert kinds == {
            "market_implied", "market_metadata", "outcome_labels",
            "question", "clob_microstructure", "orderbook_depth_derived",
        }

    def test_partial_on_empty_payload(self):
        bf = MarketBackfiller(db_dsn="unused")
        signals = bf.build_signals_for_market(self._make_row(payload_json={}))
        kinds = {s["signal_kind"] for s in signals}
        assert "market_metadata" in kinds
        assert "question" in kinds
        assert "market_implied" not in kinds
        assert "clob_microstructure" not in kinds


# ---------------------------------------------------------------------------
# Cross-cutting
# ---------------------------------------------------------------------------

class TestBackfillPrefix:
    def test_all_builders_use_backfill_prefix(self):
        for builder in ALL_BUILDERS:
            if builder is build_market_metadata:
                sig = builder("m1", "Q?", _SAMPLE_PAYLOAD, _SAMPLE_EVENT)
            elif builder is build_question:
                sig = builder("m1", "Q?")
            else:
                sig = builder("m1", "Q?", _SAMPLE_PAYLOAD)
            assert sig is not None, f"{builder.__name__} returned None"
            assert sig["signal_id"].startswith("backfill-"), f"{builder.__name__}"


class TestNatsEmission:
    def test_publish_format(self):
        published = []

        class FakeNats:
            async def publish_json(self, subject, payload):
                published.append((subject, payload))

        bf = MarketBackfiller(db_dsn="unused", nats_client=FakeNats())
        sig = build_market_implied("m1", "Q?", {"outcomePrices": [0.6, 0.4]})
        asyncio.run(bf.publish_signals_to_nats([sig]))

        assert len(published) == 1
        subject, payload = published[0]
        assert subject == "signal.computed.v1.market_implied"
        assert payload["event_type"] == "signal.computed.v1"
        assert payload["signal_kind"] == "market_implied"
        assert payload["market_id"] == "m1"
        assert payload["status"] == "ok"
        assert "emitted_at" in payload


class TestBackfillResult:
    def test_fields(self):
        r = BackfillResult(markets_processed=10, signals_written=40,
                           signals_published=40, signals_skipped=0)
        assert r.markets_processed == 10
        assert "signals_written=40" in str(r)
