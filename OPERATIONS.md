# Operations

## Deployment model

`kalshi-v3` is operated as a `LAN-only` service in phase 1.

That means:

- bind the API only to trusted interfaces
- do not expose the operator API directly to the public internet
- use firewall rules, VPN, or a private subnet if remote access is needed
- do not treat the current HTTP surface as internet-hardened

Phase 1 deliberately defers public auth, TLS termination, and internet-facing access control.

## Preferred runtime

Use Docker Compose as the normal runtime:

```powershell
cd C:\code\kalshi-v3
.\ops\start_v3_compose.ps1
```

Stop it with:

```powershell
.\ops\stop_v3_compose.ps1
```

This starts:

- `api`
- `ingest`
- `feature`
- `decision`
- `execution`
- `training`
- `notifier`
- `timescaledb`
- `redis`
- `nats`
- `prometheus`
- `grafana`

## Local ports

- API: `8080`
- TimescaleDB: `6432`
- Redis: `6380`
- NATS: `4223`
- NATS monitor: `8223`
- Prometheus: `9091`
- Grafana: `3001`

## Health and runtime checks

Primary endpoints:

- `GET /healthz`
- `GET /metrics`
- `GET /v1/runtime`
- `GET /v1/dashboard`
- `GET /v1/lanes/{lane_key}`

Recommended quick checks:

```powershell
Invoke-WebRequest http://127.0.0.1:8080/healthz
Invoke-WebRequest http://127.0.0.1:8080/v1/runtime
Invoke-WebRequest http://127.0.0.1:8080/metrics
```

The runtime should now show websocket freshness indirectly through:

- `market_feed_age_seconds`
- `reference_feed_age_seconds`
- `feature_age_seconds`
- `live_exchange_sync_age_seconds`

If Kalshi or Coinbase websocket ingest stalls, those ages should rise before paper/live decisions are trusted.

## Prometheus and Grafana

Prometheus scrapes the API directly on the Compose network using:

- target: `api:8080`
- path: `/metrics`

Prometheus UI:

- `http://127.0.0.1:9091/`

Grafana UI:

- `http://127.0.0.1:3001/`
- default user: `admin`
- default password: `admin`

## Live safety controls

There are two layers of live protection:

1. config-level live controls in `.env`
2. operator overrides in the API/UI

Important flags:

- `LIVE_TRADING_ENABLED`
- `LIVE_ORDER_PLACEMENT_ENABLED`

Recommended default:

- keep both `false` until live reconciliation and lane readiness are explicitly approved

## Operator actions

The operator API supports:

- `retry_live_sync`
- `reconcile_now`
- `disable_live`
- `enable_live`
- `cancel_pending_live_orders`
- `flatten_live_positions`

Example:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8080/v1/operator/action `
  -ContentType "application/json" `
  -Body '{"action":"disable_live"}'
```

## First live-session workflow

Before enabling any live lane:

1. confirm `/healthz` is `ok`
2. confirm `/v1/runtime` shows no stale-feed alarms
3. confirm live bankroll sync and live exchange sync are both fresh
4. confirm there are no unexplained live trade exceptions
5. confirm operator override and config are aligned with the intended live mode
6. confirm the target lane is `live_micro`, not manually edited in the database

If anything looks wrong:

- `disable_live`
- `cancel_pending_live_orders`
- `flatten_live_positions`
- then `reconcile_now`

## Logging

Compose owns process supervision. Use:

```powershell
docker compose logs -f api
docker compose logs -f execution
docker compose logs -f ingest
```

Compose now rotates container logs with Docker `json-file` retention:

- `max-size=10m`
- `max-file=5`

That is the default deployment log policy for phase 1.

## Ingest mode

The phase-1 ingest path is now `websocket-first`:

- Kalshi websocket for market ticks and trades
- Coinbase websocket for reference prices
- REST recovery for gap-fill and sync

Normalized ingest rows are also persisted in Postgres table `ingest_event_log`. That table is the current replay/audit source for raw feed history.

## Current production boundary

The stack is currently suitable for:

- live data ingest
- paper execution
- operator monitoring
- live bankroll and exchange truth sync

It is not yet fully production-ready for unrestricted live trading. The remaining blockers are tracked in [tasks.md](tasks.md).
