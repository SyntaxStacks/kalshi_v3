# kalshi-v3

`kalshi-v3` is a Rust trading platform for Kalshi. It is built as a split-service system with durable storage, replayable data flow, operator controls, and separate market-family pipelines.

The repo currently supports:

- `crypto` short-horizon directional trading
- `weather` paper-trading research lanes
- live market ingest
- paper execution
- guarded live-micro routing infrastructure
- operator API and mobile-first operator UI

This repo is the greenfield successor to an older Python prototype. It is intentionally structured around reproducible data, lane-level readiness, and stricter execution controls.

## What It Does

Core runtime flow:

`market data -> features -> model inference -> opportunity decision -> execution intent -> trade lifecycle`

Current family split:

- `crypto`
  - Kalshi + Coinbase reference flow
  - directional settlement models
  - paper execution and live-micro execution plumbing
- `weather`
  - NWS-backed reference flow
  - temperature-threshold probability models
  - paper-only rollout with isolated family budget

## Architecture

Workspace crates:

- `crates/api`
  - operator API, health, metrics, runtime inspection, LAN UI
- `crates/ingest`
  - Kalshi websocket + REST recovery
  - Coinbase websocket reference ingest
  - weather reference ingestion
- `crates/feature`
  - feature materialization and snapshot persistence
- `crates/decision`
  - model routing, confidence, edge, sizing, lane gating
- `crates/execution`
  - paper execution, live order plumbing, reconciliation logic
- `crates/training`
  - historical import, replay, benchmark, promotion updates
- `crates/notifier`
  - Discord trade and alert notifications
- `crates/common`
  - shared domain types and config loading
- `crates/storage`
  - Timescale/Postgres queries, migrations, durable state
- `crates/market-models`
  - model interfaces and baseline/trained models
- `crates/replay`
  - replay policy and benchmark primitives

## Infrastructure

Local Compose stack:

- TimescaleDB / Postgres
- Redis
- NATS
- Prometheus
- Grafana

Default local ports:

- API: `8080`
- TimescaleDB: `6432`
- Redis: `6380`
- NATS: `4223`
- NATS monitor: `8223`
- Prometheus: `9091`
- Grafana: `3001`

## Quick Start

1. Copy `.env.example` to `.env`
2. Start the stack:

```powershell
.\ops\start_v3_compose.ps1
```

Stop it with:

```powershell
.\ops\stop_v3_compose.ps1
```

Run tests:

```powershell
cargo test --workspace
```

## Operator Surface

Primary routes:

- `/`
  - crypto operator screen
- `/weather`
  - weather operator screen
- `/healthz`
- `/metrics`
- `/v1/runtime`
- `/v1/dashboard?family=crypto|weather`
- `/v1/lanes/{lane_key}`
- `/v1/operator/action`

Key operator actions:

- `retry_live_sync`
- `reconcile_now`
- `disable_live`
- `enable_live`
- `cancel_pending_live_orders`
- `flatten_live_positions`

## Runtime Characteristics

Current ingest mode is `websocket-first`:

- Kalshi market ticks and trade updates stream over websocket
- Coinbase reference prices stream over websocket
- REST remains as recovery and gap-fill
- normalized ingest rows are persisted for replayability

Current feature/version contract:

- `v3_ws_feature_v2`

That feature version is written through snapshots and downstream inferences so replay and production decisions can be tied back to the exact feature schema.

## Configuration Notes

For containerized deploys, prefer:

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_B64`

`KALSHI_PRIVATE_KEY_B64` should contain the base64-encoded PEM contents of the Kalshi private key. This is more reliable in containers than host-path mounting.

The repo uses:

- `config/default.toml` for typed defaults
- `.env` for local overrides
- explicit env parsing in Rust rather than generic config merging

## Current State

Implemented:

- split-service Rust workspace
- durable schema and migrations
- websocket-first ingest
- feature snapshots and inference persistence
- lane-level readiness and promotion state
- paper trade lifecycle
- live bankroll sync
- live exchange-truth sync
- operator controls, alerts, and Discord notifications
- weather family isolation from crypto budgets and logic

Not yet complete:

- unrestricted live trading
- fully hardened live-scaled rollout
- full champion/challenger maturity across every family
- full scalp rollout as a first-class production lane family

In practice:

- `crypto` is the primary production family
- `weather` is an isolated paper/research family
- live trading infrastructure exists, but this repo should still be treated as an actively developing system, not a finished retail product

## Security / Deployment Boundary

The operator surface is currently designed for `LAN-only` use.

This repo does **not** yet claim:

- public internet hardening
- full auth/TLS exposure for the operator API
- unattended broad live-scale trading

If you run it outside a trusted local network, add your own firewalling, VPN, reverse proxy, and access controls.

More operational detail lives in [OPERATIONS.md](OPERATIONS.md).

## Repository Conventions

- Docker Compose is the normal runtime path
- `tasks.md` tracks remaining production-readiness work
- Prometheus/Grafana are included for runtime observability
- domain separation by `market_family` is intentional: weather and crypto do not share model logic or family budgets

## Disclaimer

This software is experimental trading infrastructure. It is not investment advice, not a hosted service, and not a claim of profitability.
