# Polybet ML System v0.1

Polybet v0.1 is built to run **many ML experiments against Polymarket**, leveraging many different input signals, while keeping the pipeline scalable, reliable, and replayable.

## System goal and ownership model

Goal: run a scalable, reliable ML prediction pipeline for Polymarket where many signals and many experiments can be compared safely.

Ownership flow:
1. Discover eligible markets once (`market.new_question.v1`) in `input`.
2. Compute standardized signals (`signal.computed.v1`) in signal workers.
3. Generate experiment predictions (`prediction.proposed.v1`) in experiment workers.
4. Resolve market outcomes durably in `input` from gamma sync data.
5. Fail loudly via `*.error.v1`, and persist errors to `error_events`.
6. Keep system observable via `observe` endpoints and test suites.

Critical invariant:
- `market.resolution.changed.v1` is only emitted for markets that previously emitted `market.new_question.v1`.

## Architecture (v0.1)

1. **2 shared Rust crates**
   - `manager/crates/polybet-db`: migrations and DB schema runtime.
   - `manager/crates/polybet-events`: NATS/event client runtime.
2. **3 Rust singleton services**
   - `input` (`polybet-manager-input`)
   - `resolution` (`polybet-manager-resolution`)
   - `observe` (`polybet-manager-observe`)
3. **1 shared Python module**
   - `common/python/polybet_nats.py`
4. **2 Python worker patterns**
   - signal workers (`signal/`)
   - experiment workers (`experiment/`)
5. **Event system**
   - NATS JetStream for service-to-service communication.
6. **Durable database**
   - PostgreSQL + pgvector for persistence, replayability, and embeddings.

## Canonical events and producers/consumers

| Event | Emitted by | Consumed by | Durable write owner |
| --- | --- | --- | --- |
| `market.new_question.v1` | `input` | signal workers, experiment workers, `observe` | `input` (`markets`, snapshots, emit tracking) |
| `market.resolution.changed.v1` | `input` | `observe` | `input` (`market_outcomes`) |
| `signal.computed.v1` | signal workers | experiment workers, `observe` | signal path (`signal_outputs`) |
| `prediction.proposed.v1` | experiment workers | `resolution`, `observe` | `resolution` (`experiment_predictions`) |
| `market.error.v1` | `input` | `resolution`, `observe` | `resolution` (`error_events`) |
| `signal.error.v1` | signal workers | `resolution`, `observe` | `resolution` (`error_events`) |
| `prediction.error.v1` | experiment workers | `resolution`, `observe` | `resolution` (`error_events`) |
| `resolution.error.v1` | `resolution` | `observe` (+ self-consume path) | `resolution` (`error_events`) |

## Run the system

Prereqs:
1. Docker + Docker Compose
2. Optional host Ollama at `http://127.0.0.1:11434`
3. Optional `.env` copied from `.env.template`

Start:

```bash
docker compose up -d --build
docker compose ps
```

Key endpoints:
- Observe dashboard: `http://127.0.0.1:8000/dashboard`
- Observe status: `http://127.0.0.1:8000/status`
- Observe projection: `http://127.0.0.1:8000/observability`
- Input sync status: `http://127.0.0.1:8001/api/sync/status`

Stop:

```bash
docker compose down
```

## Run tests

Rust (manager workspace):

```bash
cd manager
cargo test --workspace
```

Python unit tests:

```bash
.\experiment\.venv\Scripts\python.exe -m pytest -q experiment\tests -m "not integration and not e2e"
```

Python integration tests:

```bash
.\experiment\.venv\Scripts\python.exe -m pytest -q experiment\tests -m integration
```

Python E2E:

```bash
$env:POLYBET_RUN_E2E="1"
.\experiment\.venv\Scripts\python.exe -m pytest -q experiment\tests\test_e2e_stack.py -m e2e
```

## Environment variables

Use `.env.template` as the source of truth. Most important groups:

1. Core runtime: `APP_ENV`, `NATS_URL`, `DATABASE_URL` (service-scoped in compose).
2. Ports: `MANAGER_PORT`, `INPUT_PORT`, `RESOLUTION_PORT`, `POSTGRES_PORT`, `NATS_PORT`.
3. Input sync behavior: `GAMMA_API_BASE`, `DATA_API_BASE`, `SYNC_INTERVAL_SECONDS`, `TOP_MARKETS_LIMIT`, `TOP_EVENTS_LIMIT`, `MARKET_MAX_MINUTES_TO_END`, `ZERO_ELIGIBLE_FAIL_STREAK`.
4. Signal workers: `SIGNAL_KIND`, `SIGNAL_ID`, `MARKET_NEW_QUESTION_TOPIC`, `SIGNAL_OUTPUT_TOPIC`.
5. Experiments: `EXPERIMENT_CONFIG_PATH`, `EXPERIMENT_SIGNAL_TOPIC`, `PREDICTION_OUTPUT_TOPIC`, `EXPERIMENT_SEED`.
6. Launcher (owned by input) + ops: `LAUNCHER_*`, backup vars, per-role DB credentials.

## Durable data and replayability

v0.1 keeps historical and latest-state surfaces needed for reruns:
- markets and history: `markets`, `market_snapshots`, `gamma_markets`
- events and history: `events`, `event_snapshots`, `gamma_events`
- outcomes: `market_outcomes` (owned by `input` from gamma-derived updates)
- retry scheduling state: `market_tracking` (for timed resolution retries/backoff)
- signals: `signal_outputs`
- predictions: `experiment_predictions`
- errors/audit: `error_events`

This allows experiments to be rerun against historical eligible markets, signals, and outcomes.

## Images built by compose

- `manager/Dockerfile.input` -> `polybet-manager-input:local`
- `manager/Dockerfile.resolution` -> `polybet-manager-resolution:local`
- `manager/Dockerfile.observe` -> `polybet-manager-observe:local`
- `signal/Dockerfile` -> `polybet-signal:local`
- `experiment/Dockerfile` -> `polybet-experiment:local`

## Component docs

- `manager/README.md`
- `signal/README.md`
- `experiment/README.md`
# polybet
