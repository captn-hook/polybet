# Manager Workspace v0.2 (Rust)

The manager workspace is the control plane for discovery, scoring, and observability.

## Workspace layout

Shared crates:
1. `crates/polybet-db`: migration + schema runtime.
2. `crates/polybet-events`: NATS/event runtime.

Singleton services:
1. `services/input` (`polybet-manager-input`)
2. `services/resolution` (`polybet-manager-resolution`)
3. `services/observe` (`polybet-manager-observe`)

## Service ownership

1. `input`
   - polls Gamma/Data surfaces
   - computes eligibility once
   - emits `market.new_question.v1`
   - emits `market.resolution.changed.v1` only for previously emitted markets
   - persists canonical/snapshot market data and gamma-derived market outcome updates
   - owns launcher reconcile loop and writes launcher state tables
2. `resolution`
   - consumes `prediction.proposed.v1` and persists `experiment_predictions`
   - consumes `signal.computed.v1.*` and persists `signal_outputs`
   - runs resolution sweep: fetches fresh market data from Gamma API, derives outcomes, writes `market_outcomes`
   - emits `signal.computed.v1.market_implied` with last-snapshot prices before recording each resolution
   - emits `market.resolution.changed.v1` on successful resolution
   - consumes and persists all `*.error.v1` to `error_events`
   - uses dual DB pools: sweep pool (heavy queries) + consumer pool (NATS writes) to avoid contention
3. `observe`
   - projects canonical events for operational visibility
   - serves `/dashboard`, `/status`, `/observability`
   - exposes launcher runtime views

## Build and run

```bash
cd manager
cargo run -p polybet-manager-input
cargo run -p polybet-manager-resolution
cargo run -p polybet-manager-observe
```

Workspace test:

```bash
cargo test --workspace
```

## Endpoints

- Input:
  - `GET /health`
  - `GET /status`
  - `GET /api/sync/status`
  - `POST /api/launcher/reconcile`
- Resolution:
  - `GET /health`
- Observe:
  - `GET /health`
  - `GET /status`
  - `GET /observability`
  - `GET /api/sync/status`
  - `GET /api/launcher/status`
  - `GET /dashboard`

## Environment (high-value)

Shared:
- `DATABASE_URL`
- `NATS_URL`
- `RUST_LOG`

Input-specific:
- `GAMMA_API_BASE`
- `DATA_API_BASE`
- `SYNC_INTERVAL_SECONDS`
- `TOP_MARKETS_LIMIT`
- `TOP_EVENTS_LIMIT`
- `MARKET_MAX_MINUTES_TO_END`
- `ZERO_ELIGIBLE_FAIL_STREAK`

Input-specific (launcher):
- `LAUNCHER_MANIFEST_PATH`
- `LAUNCHER_DOCKER_BASE`
- `LAUNCHER_EXPERIMENT_IMAGE`
- `LAUNCHER_EXPERIMENT_CONFIG_BIND`

## Operational notes

- `input` and `resolution` should remain singletons for authoritative ownership.
- `market_outcomes` is currently input-owned (gamma-derived updates during sync).
- `market_tracking` is reserved for resolution retry/backoff scheduling state.
- `observe` can scale horizontally.
- Use least-privilege DB roles:
  - `polybet_input` — read/write markets, gamma, sync state, launcher tables
  - `polybet_resolution` — read/write predictions, outcomes, tracking, signals, errors, gamma_markets
  - `polybet_observe` — read-only
  - `polybet_experiment` — SELECT all tables + INSERT on signal_outputs (for backfill)
- Column migrations skip ALTER TABLE on established databases (catalog check avoids AccessExclusiveLock contention)
- Resolution sweep prioritizes low-retry-count markets and refreshes stale payloads on retry

