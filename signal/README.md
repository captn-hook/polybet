# Signal Workers v0.1

Signal workers convert upstream market/context data into standardized signal events for experiments.

## Goal in pipeline

Signals make experiment inputs consistent: each signal emits `signal.computed.v1` with either:
- `status=ok` with side/confidence, or
- `status=unavailable` with explicit reason.

## Implemented workers

1. `signal/main.py` (`polymarket_sentiment`)
   - consumes `market.new_question.v1`
   - computes sentiment from market outcome prices
2. `signal/pass_through.py` (`pass_through`)
   - consumes `market.new_question.v1`
   - emits simple pass-through signal events for control/testing flows

Both use `common/python/polybet_nats.py`.

## Runtime dependencies

- `NATS_URL`
- `MARKET_NEW_QUESTION_TOPIC`
- `SIGNAL_OUTPUT_TOPIC`
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

