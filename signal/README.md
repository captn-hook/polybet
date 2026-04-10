# Signal Workers v0.2

Signal workers convert upstream market/context data into standardized signal events for experiments.

## Goal in pipeline

Signals make experiment inputs consistent: each signal publishes to `signal.computed.v1.<kind>` with either:
- `status=ok` with signal-specific fields, or
- `status=unavailable` with explicit reason.

NATS hierarchical subjects allow experiments to subscribe only to the signal kinds they need (e.g., `signal.computed.v1.market_implied`), while consumers that need all kinds use the wildcard `signal.computed.v1.*`.

## Implemented workers

### Base signals (consume `market.new_question.v1`)

1. `market_implied.py` — implied side/confidence from `outcomePrices`
2. `market_metadata.py` — discovery metadata (event/condition/question IDs, flags)
3. `outcome_labels.py` — normalized outcome labels from `outcomes`
4. `question.py` — market question text
5. `clob_microstructure.py` — CLOB API spread and last trade price
6. `open_interest.py` — Data API open interest by condition ID
7. `orderbook_depth.py` — CLOB API full order book
8. `price_history.py` — CLOB API historical price series
9. `trade_flow.py` — Data API recent trade activity

### Derived signals (consume parent signal from `signal.computed.v1.<parent_kind>`)

10. `orderbook_depth_derived.py` — imbalance, weighted mid, slippage from order book
11. `price_history_derived.py` — momentum, volatility, trend from price history
12. `trade_flow_derived.py` — net flow, buy/sell ratio, VWAP from trade data

All use `common/python/polybet_nats.py` and `signal_common.py`.

## Event contract

Published to `signal.computed.v1.<signal_kind>`:
- `event_type`, `emitted_at`, `signal_id`, `signal_kind`
- `market_id`, `market_question`
- `status`, `reason`
- signal-specific fields (e.g., `sentiment_side`, `sentiment_confidence`, `spread`, etc.)

## Configuration

- `SIGNAL_KIND` / `SIGNAL_ID` — identity
- `SIGNAL_OUTPUT_TOPIC` — base topic (default: `signal.computed.v1`), kind is appended
- `MARKET_NEW_QUESTION_TOPIC` — source topic (base signals: `market.new_question.v1`, derived: `signal.computed.v1.<parent>`)
- `CLOB_API_BASE` / `DATA_API_BASE` — external API endpoints

