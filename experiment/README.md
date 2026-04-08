# Experiment Workers v0.1 (Python)

Experiment workers consume standardized signals and emit prediction proposals used for scoring and comparison.

## Goal in pipeline

Experiments are the model/strategy layer: many configurations run in parallel against the same eligible market and signal surfaces so outputs can be compared safely.

## Worker behavior

Current runner: `experiment/exp_runner.py`

1. reads config from `EXPERIMENT_CONFIG_PATH`
2. subscribes to:
   - `market.new_question.v1`
   - `signal.computed.v1`
3. computes side/confidence from configured strategy mode
4. emits `prediction.proposed.v1`

Fatal runtime paths emit:
- `prediction.error.v1` with structured context.

## Implemented strategy modes

- `always_yes`
- `always_no`
- `probabilistic` / `random`
- `follow_signal_sentiment`
- `against_signal_sentiment`
- `pass_through`

For RNG modes, `EXPERIMENT_SEED` is required for reproducibility.

## Built-in configs

- `configs/default.yaml`
- `configs/always_yes.yaml`
- `configs/always_no.yaml`
- `configs/yes_75.yaml`
- `configs/no_75.yaml`
- `configs/follow_signal_sentiment.yaml`
- `configs/against_signal_sentiment.yaml`

## Runtime dependencies

- `NATS_URL`
- `EXPERIMENT_SIGNAL_TOPIC`
- `PREDICTION_OUTPUT_TOPIC`
- `MARKET_NEW_QUESTION_TOPIC`
- config file mount (`EXPERIMENT_CONFIG_PATH`)
- optional `OLLAMA_BASE_URL` for future model paths

## Event emitted

`prediction.proposed.v1` includes:
- `event_type`, `emitted_at`
- `run_id`
- `experiment_id`, `strategy`
- `market_id`, `market_question`, `market_end_date_raw`
- `side`, `confidence`
- `horizon_minutes`, `rationale`, `meta`

## Scaling guidance

- Run many experiment replicas with distinct config/seed.
- Keep configs externalized for replay and comparison.
- Use deterministic naming and run metadata for lineage.

## Tests

Unit tests:

```bash
.\.venv\Scripts\python.exe -m pytest -q tests -m "not integration and not e2e"
```

Integration tests:

```bash
.\.venv\Scripts\python.exe -m pytest -q tests -m integration
```

E2E:

```bash
$env:POLYBET_RUN_E2E="1"
.\.venv\Scripts\python.exe -m pytest -q tests\test_e2e_stack.py -m e2e
```

