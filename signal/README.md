# Signal Workers v0.1

Signal workers convert upstream market/context data into standardized signal events for experiments.

## Goal in pipeline

Signals make experiment inputs consistent: each signal emits `signal.computed.v1` with either:
- `status=ok` with side/confidence, or
- `status=unavailable` with explicit reason.

## Implemented workers

1. `signal/market_implied.py` (`market_implied`)
   - consumes `market.new_question.v1`
   - emits implied side/confidence from market `payload.outcomePrices`
2. `signal/pass_through.py` (`pass_through`)
   - consumes `market.new_question.v1`
   - emits simple pass-through signal events for control/testing flows
3. `signal/outcome_labels.py` (`outcome_labels`)
   - consumes `market.new_question.v1`
   - re-emits `payload.outcomes` as normalized labels metadata
4. `signal/market_metadata.py` (`market_metadata`)
   - consumes `market.new_question.v1`
   - re-emits discovery metadata (event/condition/question IDs and market flags)
5. `signal/clob_microstructure.py` (`clob_microstructure`)
   - consumes `market.new_question.v1`
   - fetches CLOB `/spread` and `/last-trade-price` by token ID and emits thin market microstructure fields
6. `signal/open_interest.py` (`open_interest`)
   - consumes `market.new_question.v1`
   - fetches Data API `/oi?market=...` by condition ID and emits current open interest

Both use `common/python/polybet_nats.py`.

## Runtime dependencies

- `NATS_URL`
- `MARKET_NEW_QUESTION_TOPIC`
- `SIGNAL_OUTPUT_TOPIC`
- `CLOB_API_BASE` (for `clob_microstructure`)
- `DATA_API_BASE` (for `open_interest`)
- optional `OLLAMA_BASE_URL` (for future embedding/synthesis paths)

## Event contract emitted

`signal.computed.v1` payload includes:
- `event_type`, `emitted_at`
- `signal_id`, `signal_kind`
- `market_id`, `market_question`
- `status`, `reason`
- when `status=ok`: `sentiment_side`, `sentiment_confidence` (+ sentiment metadata fields)

Fatal runtime paths emit:
- `signal.error.v1` with `service`, `error_code`, `message`, `context`.

## Configuration

High-value env:
- `SIGNAL_KIND`
- `SIGNAL_ID`
- `SIGNAL_OUTPUT_TOPIC`
- `MARKET_NEW_QUESTION_TOPIC`
- `NATS_URL`
- `SIGNAL_POLL_INTERVAL_SEC`

## Scaling guidance

- Keep workers stateless.
- Scale by source shard/topic partition.
- Keep `signal_id` deterministic for traceability and replay analysis.

