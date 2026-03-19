# V3 Production Readiness Tasks

## Current State
- `v3` is live-data and paper-execution capable.
- Feature snapshots, decisions, bankrolls, and paper trades are flowing.
- Live bankroll sync works.
- Signed live order placement scaffolding exists, but live trading is still disabled.
- Live intent reconciliation now handles `submitted`, `acknowledged`, `partially_filled`,
  `cancel_pending`, `cancelled`, `rejected`, and `orphaned` transitions for the current entry flow.
- Stale live orders now age into explicit cleanup instead of lingering indefinitely.
- Operator surface is `LAN-only`.
- Initial production target is `directional_settlement` only.

## Phase 0: Correctness Blockers
- [x] Fix execution intent lifecycle so stale `pending` intents cannot accumulate forever.
- [x] Add a background intent reconciler that marks stale intents `superseded` or `expired`.
- [x] Promote live order metadata out of `metadata_json` into typed storage.
- [x] Add idempotent duplicate recovery for live orders beyond basic `409` handling.
- [x] Add explicit live trade reconciliation against exchange truth for open positions, resting orders, and recent fills.
- [x] Stop treating live bankroll as sufficient truth by itself.
- [x] Add a startup reconciliation pass that repairs stale execution intents after worker restarts before new intents are opened.
- [x] Add a hard kill switch in config and runtime so live order placement can be disabled without stopping other workers.

## Phase 1: Infrastructure and Runtime Hardening
- [x] Fix the NATS container healthcheck so it can report healthy.
- [x] Choose one eventing direction and implement it fully; do not keep publish-only NATS as pseudo-architecture.
- [x] Add worker heartbeats and last-success timestamps to durable storage.
- [x] Report worker health in `/healthz` and `/v1/runtime`.
- [x] Add stale-feed detection for Kalshi and Coinbase with explicit runtime/UI status.
- [x] Add worker-specific structured counters for ingest, feature, decision, execution, and live order failures.
- [x] Expose a Prometheus metrics endpoint from the Rust API and scrape it from Prometheus.
- [x] Add restart-safe log rotation/retention for deployed services.
- [x] Add integration checks in CI that boot Timescale/Postgres, Redis, and NATS and run migrations.

## Phase 2: Data and Feature Pipeline
- [x] Replace poll-based Kalshi ingest with websocket-first ingest plus REST recovery.
- [x] Replace Coinbase REST ticker proxy with websocket-based reference ingest.
- [x] Materialize settlement-aware Kalshi features: threshold distance, averaging-window path, settlement regime, and last-minute averaging proxies.
- [x] Expand venue quality beyond spread/depth heuristics to include quote staleness, update frequency, and deterioration.
- [x] Persist raw ingest events or normalized event rows so feature generation is replayable.
- [x] Version feature schemas explicitly and persist the version with inference rows.

## Phase 3: Modeling, Replay, and Promotion
- [x] Replace the current heuristic two-model setup with a real model registry and champion/challenger evaluation.
- [x] Build proper historical backfill into v3 for replay/training.
- [x] Upgrade replay to same-sample lane evaluation using forecast quality plus execution-adjusted PnL.
- [x] Add probability calibration by symbol and expiry regime.
- [x] Make lane promotion explicitly stateful with durable reason codes.
- [x] Support fast-ramp promotion from `live_micro` to `live_scaled` after an explicit short success window.
- [x] Add de-promotion back to `paper_active` or `quarantined` after bad live behavior.
- [ ] Keep `pre_settlement_scalp` as a phase-2 milestone after directional live readiness is stable.

## Phase 4: Execution and Risk
- [x] Add a real live order state machine with submitted, acknowledged, partially_filled, filled, cancel_pending, cancelled, rejected, and orphaned states.
- [x] Support partial fills and partial exits.
- [x] Add live exit order placement that reconciles actual exit fills before realized PnL is finalized.
- [x] Separate live and paper capital allocation fully in the risk path.
- [x] Add live per-lane exposure caps, per-symbol caps, and portfolio kill conditions.
- [x] Add rejection handling for low-notional live intents before execution.
- [x] Add explicit support for reduce-only exits, cancel/replace, and stale live order cleanup.
- [x] Add a manual operator override path to cancel all live orders and flatten all live positions.

### Phase 4 Notes
- Entry-side live intent reconciliation and stale-order cleanup are in place.
- Exit orders are now persisted onto open live trades when the exchange acknowledges an exit order without an immediate fill.
- Partial exits are now represented as closed child trades with the parent open trade quantity reduced in place.
- Execution now blocks duplicate live exits while an active exit order already exists on the trade.
- Entry and exit orders now support configurable `time_in_force` plus cancel/replace when a live order rests too long.
- A manual `flatten_live_positions` operator action now cancels resting live orders first and then submits reduce-only exits for positions it can resolve safely.
- Decision sizing now separates paper and live bankrolls, applies live lane/symbol caps, and blocks live routing when the live bankroll breaches the configured drawdown kill threshold.
- Remaining execution work is now mostly production proof rather than missing mechanics: stress validation, exchange-driven cleanup under adverse conditions, and final acceptance-gate evidence.

## Phase 5: Operator API and UI
- [x] Implement `/v1/lanes/{lane_key}` for real lane inspection.
- [x] Add operator views for live readiness, live orders, live positions, and live exceptions.
- [x] Show stale data clearly in the UI for bankrolls, readiness, opportunities, and feed freshness.
- [x] Surface intent status counts and blocked/rejected reasons in the UI.
- [x] Add an `attention needed` drill-down with exact lane, reason, recent PnL, and next criterion.
- [x] Add operator actions for retry live sync, reconcile now, disable live, and cancel pending live orders.
- [x] Keep the deployment model LAN-only and document firewall/VPN expectations instead of building internet auth/TLS now.

## Acceptance Gates
- [ ] Gate A: paper-stable
- [ ] Gate B: replay-stable
- [ ] Gate C: live-ready
- [ ] Gate D: live-micro ready
- [ ] Gate E: live-scaled ready

## Suggested Work Order
1. Finish remaining `Phase 3` by deciding whether and when to bring `pre_settlement_scalp` into v3 after directional live readiness is stable.
2. Prove the `Acceptance Gates` with runtime evidence before turning on `LIVE_TRADING_ENABLED`.
3. Do one more operator polish pass only if a real live-micro session exposes gaps.

## Notes
- `Phase 2` is now websocket-first and versioned:
  - Kalshi market and trade updates are websocket-driven with REST recovery.
  - Coinbase reference prices are websocket-driven with REST fallback.
  - Normalized ingest events are persisted in `ingest_event_log`.
  - Feature snapshots now carry explicit `feature_version` and the decision path persists that version into `model_inference`.
- NATS remains available for fanout, but it is no longer on the critical path for ingest correctness in phase 1.
- `Phase 3` has started to move from heuristics into durable model selection:
  - training now benchmarks the full supported directional model registry per lane instead of one fixed pair
  - lane champions are selected from a leaderboard, not a hardcoded challenger comparison
  - probability calibration is now persisted by `symbol + expiry_bucket + model_name` and applied in live decisioning
  - lane states now persist `promotion_reason` and write transition history into `lane_state_transition`
  - historical replay examples are imported incrementally from the frozen v2 SQLite archive into `historical_replay_example`
  - replay now stores durable `model_benchmark_run` history and `/v1/lanes/{lane_key}` exposes the latest ranked benchmark set
