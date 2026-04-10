# Experiment Workers v0.2 (Python)

Experiment workers consume standardized signals and emit prediction proposals used for scoring and comparison.

## Goal in pipeline

Experiments are the model/strategy layer: many configurations run in parallel against the same eligible market and signal surfaces so outputs can be compared safely.

## Runners

### exp_runner.py (strategy-based experiments)

1. Reads config from `EXPERIMENT_CONFIG_PATH`
2. Subscribes to `signal.computed.v1.<trigger_signal_kind>` (targeted — no client-side filtering)
3. Subscribes to `market.new_question.v1` for market metadata
4. Computes side/confidence from configured strategy mode
5. Per-market cooldown prevents duplicate predictions
6. Emits `prediction.proposed.v1`

### kmeans_runner.py (ML clustering experiment)

1. Trains K-Means model from DB (`signal_outputs` + `market_outcomes`)
2. Subscribes to `signal.computed.v1.*` (wildcard — all signal kinds)
3. Accumulates signals per market, predicts when complete or on timeout (partial predict with NaN imputation)
4. Refits model periodically on new resolutions (batched, non-blocking via thread pool)
5. Emits `prediction.proposed.v1`

### backfill.py (historical signal generation)

Generates signals from `gamma_markets` payload data for resolved markets. Uses pre-resolution snapshots from `market_snapshots` (earliest observation before market end time) to avoid data leakage.

Signal kinds generated: `market_implied`, `market_metadata`, `outcome_labels`, `question`, `clob_microstructure`, `orderbook_depth_derived`.

```bash
# Full backfill: write to DB + emit to NATS (run while experiments are listening)
python backfill.py

# DB-only (for startup, no NATS needed)
python backfill.py --db-only
```

## Active experiments

### Sentiment
- `exp-first-signal-sentiment` — follows market_implied signal, per-market cooldown (predicts on first signal)
- `exp-last-signal-sentiment` — follows market_implied signal, no cooldown (updates on latest signal including resolution-time emission)
- `exp-control-against-first-sentiment` — bets against first signal (control)

### ML
- `exp-kmeans-clustering` — K-Means clustering on 30 signal features, partial predict on timeout

### Controls
- `exp-control-always-yes` / `exp-control-always-no` — fixed side
- `exp-control-yes-75` / `exp-control-no-75` — 75% probabilistic (3 seeds: 21, 67, 420)
- `exp-control-random` — 50/50 random (3 seeds: 21, 67, 420)

## Runtime dependencies

- `NATS_URL`, `DATABASE_URL`
- `EXPERIMENT_SIGNAL_TOPIC` — base topic (default: `signal.computed.v1`), runner appends `.<kind>` or `.*`
- `TRIGGER_SIGNAL_KIND` — which signal kind triggers predictions (default: `question`)
- `PREDICTION_OUTPUT_TOPIC`, `MARKET_NEW_QUESTION_TOPIC`
- `EXPERIMENT_CONFIG_PATH` — config file path
- `BACKFILL_ENABLED` — `1` (default) or `0` to skip DB backfill on startup
- `EXPERIMENT_SEED` — required for RNG modes

## Tests

```bash
docker run --rm polybet-experiment:local python -m pytest tests/ -v -k unit
```

