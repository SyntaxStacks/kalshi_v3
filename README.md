# kalshi-v3

`kalshi-v3` is a greenfield Kalshi-only Rust trading platform. It is intentionally separate from the existing Python v2 repo and is now the active runtime. The system is backend-first, with split services for ingest, features, decisions, execution, training, and notifications.

## Scope

- `Kalshi` is the only exchange in phase 1.
- `Coinbase` is the public reference feed in phase 1.
- Strategy families are separated from day one:
  - `directional_settlement`
  - `pre_settlement_scalp`

## Workspace

- `crates/api`: operator API, health, metrics, runtime inspection
- `crates/ingest`: Kalshi and Coinbase ingest runtime
- `crates/feature`: feature materialization worker
- `crates/decision`: inference, calibration, lane routing, risk gating
- `crates/execution`: paper/live execution orchestration
- `crates/training`: historical import, replay, fitting, benchmark jobs
- `crates/notifier`: Discord and alert fanout
- `crates/common`: shared config, domain types, event contracts
- `crates/storage`: Postgres/Timescale access and migrations
- `crates/market-models`: baseline model interfaces and simple models
- `crates/replay`: replay and benchmark primitives

## Infrastructure

Local infra is defined in `docker-compose.yml`:

- TimescaleDB
- Redis
- NATS JetStream
- Prometheus
- Grafana

Ports are intentionally offset from the existing v2 stack so both repos can run side by side:

- TimescaleDB: `6432`
- Redis: `6380`
- NATS: `4223`
- Prometheus: `9091`
- Grafana: `3001`

## Getting started

1. Copy `.env.example` to `.env`
2. Start the full stack:

```powershell
.\ops\start_v3_compose.ps1
```

Stop it with:

```powershell
.\ops\stop_v3_compose.ps1
```

If you only want the infra layer without the Rust services, use:

```powershell
docker compose up -d
```

3. Run tests:

```powershell
cargo test
```

4. Start only the API:

```powershell
cargo run -p api
```

Or start the local worker binaries outside Compose:

```powershell
.\ops\start_v3_stack.ps1
```

Stop it with:

```powershell
.\ops\stop_v3_stack.ps1
```

The Compose path is the normal deployment path. Docker owns restart behavior, logs, and process supervision.

## Secrets for containerized deploys

For containerized deployment, prefer:

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_B64`

`KALSHI_PRIVATE_KEY_B64` should be the base64-encoded PEM content of the private key. This avoids Windows host-path issues inside containers. The older `KALSHI_PRIVATE_KEY_PATH` still works for direct local runs.

The API exposes:

- `GET /healthz`
- `GET /metrics`
- `GET /v1/runtime`
- `GET /v1/lanes/:lane_key`
- `POST /v1/operator/action`

Key operator actions:

- `retry_live_sync`
- `reconcile_now`
- `disable_live`
- `enable_live`
- `cancel_pending_live_orders`
- `flatten_live_positions`

## Operations

The operator surface is intentionally `LAN-only`. This repo does not add internet auth or TLS for phase 1. Keep the API bound to trusted interfaces and use firewall rules, VPN, or a private network segment if you need off-machine access.

See [OPERATIONS.md](OPERATIONS.md) for:

- Compose-based deployment
- LAN-only security expectations
- Prometheus/Grafana access
- live kill switch and flatten controls
- recommended operator workflow for the first live sessions

## Current status

This repo is no longer just a skeleton. It currently includes:

- the split workspace layout
- canonical domain/event types
- durable Timescale/Postgres schema and migrations
- running ingest, feature, decision, execution, training, and notifier services
- baseline model and replay primitives
- Compose deployment
- operator API and mobile-first operator UI
- live bankroll sync and live exchange truth sync
- paper trade lifecycle and directional opportunity flow

It still does not include full production readiness. Major remaining areas are:

- champion/challenger replay and promotion hardening
- full live order lifecycle with cancel/replace under stress
- complete live-scaled readiness criteria

## Data pipeline status

The live data path is now `websocket-first`:

- Kalshi market ticks and trade updates stream over websocket
- Coinbase reference prices stream over websocket
- REST remains in place as recovery and gap-fill, not the primary feed
- normalized ingest rows are persisted to `ingest_event_log` for replayability

Feature snapshots are also explicitly versioned. The current live feature schema is:

- `v3_ws_feature_v2`

That version is written onto both `market_feature_snapshot` and downstream `model_inference` rows so replay and inference can be tied back to the exact feature contract.
