# Execution Metric Contract

## Why this exists

`2a7dcb2` added execution-quality surfacing, but the current summary mixes:

- paper and live outcomes
- ambiguous intent statuses
- missing forecasts coerced to `0`
- unweighted replay averages

That makes the current panel useful for diagnostics, but unsafe for capital decisions.

This document defines:

- the current semantic bug
- the status audit
- the replacement metric contract
- the migration plan
- the exact summary queries to replace the current implementation
- the dashboard and Prometheus names that must change

## Blocking correctness bug

Current summary logic in `crates/storage/src/pg.rs` treats:

- `opened`
- `filled`
- `partially_filled`

as equivalent evidence of realized fill.

That is incorrect.

`opened` currently means "a trade row was opened from this intent," not "an exchange-confirmed fill occurred." It is used by the paper path and by repair logic that infers exposure from existing local trade rows.

Until this is fixed, `recent_actual_fill_rate` is not a trustworthy execution metric.

## Current execution_intent status audit

### Current statuses in use

Observed statuses in the codebase:

- `pending`
- `submitted`
- `acknowledged`
- `partially_filled`
- `filled`
- `cancel_pending`
- `cancelled`
- `rejected`
- `expired`
- `superseded`
- `orphaned`
- `opened`

### Current semantics

- `pending`
  - intent exists but has not been routed
- `submitted`
  - an order submission was attempted or is retryable, but no reliable open-trade evidence exists yet
- `acknowledged`
  - the venue accepted or is resting the order, but fill quantity may still be zero
- `partially_filled`
  - some quantity is filled, but not all submitted quantity
- `filled`
  - full submitted quantity is filled
- `cancel_pending`
  - cancellation was requested, terminal outcome not yet known
- `cancelled`
  - order cancelled with no further active execution expected
- `rejected`
  - order rejected or execution failed without opening exposure
- `expired`
  - intent became obsolete due to time or market state
- `superseded`
  - intent was replaced by a newer intent/order path
- `orphaned`
  - order lifecycle became inconsistent and needs reconciliation
- `opened`
  - local trade lifecycle row was created from the intent
  - this is not a reliable fill-status semantic
  - it is especially ambiguous because it spans both paper and repair logic

### Code paths that write `opened`

#### Paper execution path

- `crates/execution/src/main.rs`
  - paper path calls `open_trade_from_intent(...)`
- `crates/storage/src/pg.rs`
  - `open_trade_from_intent(...)` passes `"opened"`
  - `open_trade_from_intent_with_metadata(...)` also passes `"opened"`

#### Repair path

- `crates/storage/src/pg.rs`
  - `repair_pending_execution_intents_from_trades(...)`
  - updates any `pending` intent to `opened` if a `trade_lifecycle` row exists for the same decision

#### Live path

- direct live entry does **not** intentionally use `opened` for confirmed fills
- live entry uses:
  - `"filled"` when a fill is returned immediately
  - `next_status` from order reconciliation for later fills

Conclusion:

- `opened` must be treated as a local exposure marker, not as fill truth
- `opened` must be excluded from any fill-quality metric

## Metric contract

### expected_fill_probability

Definition:

- probability that the specific routed order version will receive `filled_quantity > 0`
- measured over the active intent/order version, before expiry or explicit cancellation
- nullable when no execution forecast was computed

Required properties:

- typed column on `execution_intent`
- never parsed from `stop_conditions_json`
- `NULL` means unknown or not computed
- must be scoped to:
  - mode
  - family
  - lane
  - order version

Required storage fields:

- `predicted_fill_probability double precision null`
- `predicted_slippage_bps double precision null`
- `predicted_queue_ahead double precision null`
- `execution_forecast_version text null`

### actual_fill_rate

Definition:

- quantity-weighted realized fill ratio
- `sum(filled_quantity) / sum(submitted_quantity)`
- computed only over intents with terminal or measurable outcomes
- must exclude ambiguous statuses like `opened`
- must be mode-aware

This metric is intentionally not binary.

If binary hit-rate is also needed, define a separate metric:

- `actual_fill_hit_rate`
  - `count(intents with filled_quantity > 0) / count(comparable intents)`

Required properties:

- based on typed quantities, not status buckets
- separate from paper and live by default
- live dashboard badge must use live evidence only

Required storage fields:

- `submitted_quantity double precision not null default 0`
- `accepted_quantity double precision not null default 0`
- `filled_quantity double precision not null default 0`
- `cancelled_quantity double precision not null default 0`
- `rejected_quantity double precision not null default 0`
- `avg_fill_price double precision null`
- `first_fill_at timestamptz null`
- `last_fill_at timestamptz null`
- `terminal_outcome text null`
- `promotion_state_at_creation text null`

## Query contract

### Rules for comparable execution samples

Execution-quality summaries must:

- exclude `opened`
- exclude rows where forecast is `NULL` from expected-fill comparisons
- default to `mode = 'live'` for operator-facing execution truth
- allow explicit slices by:
  - `market_family`
  - `lane_key`
  - `strategy_family`
  - `promotion_state_at_creation`
  - time-to-expiry bucket
  - spread bucket
  - liquidity bucket

### Replacement query: recent live execution truth

This query replaces the current `recent_expected_fill_probability` and `recent_actual_fill_rate` summary.

```sql
with recent as (
    select
        ei.id,
        ei.market_family,
        ei.mode,
        ei.lane_key,
        ei.strategy_family,
        ei.promotion_state_at_creation,
        ei.predicted_fill_probability,
        ei.predicted_slippage_bps,
        ei.execution_forecast_version,
        ei.submitted_quantity,
        ei.accepted_quantity,
        ei.filled_quantity,
        ei.cancelled_quantity,
        ei.rejected_quantity,
        ei.avg_fill_price,
        ei.status,
        ei.processed_at
    from execution_intent ei
    where ei.processed_at is not null
      and ei.mode = $1
      and ($2::text is null or ei.market_family = $2)
      and ($3::text is null or ei.lane_key = $3)
      and ($4::text is null or ei.strategy_family = $4)
      and ($5::text is null or ei.promotion_state_at_creation = $5)
      and ei.status in ('filled', 'partially_filled', 'cancelled', 'rejected', 'expired', 'superseded', 'orphaned')
    order by ei.processed_at desc, ei.id desc
    limit 500
),
comparable as (
    select *
    from recent
    where predicted_fill_probability is not null
      and submitted_quantity > 0
),
all_terminal as (
    select *
    from recent
    where submitted_quantity > 0
)
select
    count(*)::bigint as terminal_intent_count,
    count(*) filter (where filled_quantity > 0)::bigint as intents_with_fill_count,
    count(comparable.id)::bigint as predicted_sample_count,
    coalesce(avg(comparable.predicted_fill_probability), 0)::double precision
        as predicted_fill_probability_mean,
    coalesce(sum(all_terminal.filled_quantity) / nullif(sum(all_terminal.submitted_quantity), 0), 0)::double precision
        as actual_fill_rate,
    coalesce(avg(case when comparable.filled_quantity > 0 then 1.0 else 0.0 end), 0)::double precision
        as actual_fill_hit_rate,
    coalesce(sum(comparable.filled_quantity) / nullif(sum(comparable.submitted_quantity), 0), 0)::double precision
        as comparable_actual_fill_rate,
    coalesce(avg(comparable.predicted_slippage_bps), 0)::double precision
        as predicted_slippage_bps_mean,
    max(recent.processed_at) as as_of
from all_terminal
left join comparable on comparable.id = all_terminal.id;
```

Notes:

- parameter `$1` should be `live` for operator truth
- `paper` can be exposed as a separate diagnostic slice
- `opened` is intentionally excluded

### Replacement query: trade-weighted replay quality

This query replaces the current unweighted replay averages.

```sql
with latest_runs as (
    select distinct on (lane_key)
        id,
        lane_key,
        created_at
    from model_benchmark_run
    where ($1::text is null or market_family = $1)
    order by lane_key, created_at desc, id desc
),
champion_rows as (
    select
        lr.lane_key,
        lr.created_at,
        mbr.trade_count,
        mbr.sample_count,
        mbr.execution_pnl,
        mbr.fill_rate,
        mbr.slippage_bps,
        mbr.edge_realization_ratio
    from latest_runs lr
    join model_benchmark_result mbr on mbr.run_id = lr.id
    where mbr.rank = 1
)
select
    count(*)::bigint as replay_lane_count,
    coalesce(sum(trade_count), 0)::bigint as replay_trade_count,
    coalesce(sum(execution_pnl), 0)::double precision as replay_execution_pnl,
    coalesce(sum(fill_rate * trade_count) / nullif(sum(trade_count), 0), 0)::double precision
        as replay_trade_weighted_fill_rate,
    coalesce(sum(slippage_bps * trade_count) / nullif(sum(trade_count), 0), 0)::double precision
        as replay_trade_weighted_slippage_bps,
    coalesce(sum(edge_realization_ratio * trade_count) / nullif(sum(trade_count), 0), 0)::double precision
        as replay_trade_weighted_edge_realization_ratio,
    max(created_at) as replay_as_of
from champion_rows;
```

### Optional notional-weighted replay extension

If replay results later store notional or gross exposure, replace `trade_count` weighting with notional weighting:

- `sum(metric * gross_notional) / sum(gross_notional)`

That is preferred over trade-count weighting once available.

## Migration plan

### Phase 1: add typed execution forecast and outcome columns

Add to `execution_intent`:

- `predicted_fill_probability double precision`
- `predicted_slippage_bps double precision`
- `predicted_queue_ahead double precision`
- `execution_forecast_version text`
- `submitted_quantity double precision not null default 0`
- `accepted_quantity double precision not null default 0`
- `filled_quantity double precision not null default 0`
- `cancelled_quantity double precision not null default 0`
- `rejected_quantity double precision not null default 0`
- `avg_fill_price double precision`
- `first_fill_at timestamptz`
- `last_fill_at timestamptz`
- `terminal_outcome text`
- `promotion_state_at_creation text`

### Phase 2: write-path updates

Decision path:

- write `predicted_fill_probability`
- write `predicted_slippage_bps`
- write `predicted_queue_ahead`
- write `execution_forecast_version`
- write `submitted_quantity`
- write `promotion_state_at_creation`

Execution path:

- on paper open:
  - set `accepted_quantity = submitted_quantity`
  - set `filled_quantity = submitted_quantity`
  - set `terminal_outcome = 'paper_filled'`
- on live acknowledge:
  - set `accepted_quantity` only if the venue confirms resting/accepted quantity
- on live partial fill:
  - increment `filled_quantity`
  - set `first_fill_at` and `last_fill_at`
- on live full fill:
  - set `filled_quantity = submitted_quantity`
  - set `terminal_outcome = 'filled'`
- on cancel/reject:
  - populate `cancelled_quantity` or `rejected_quantity`
  - set `terminal_outcome`

### Phase 3: backfill

Backfill existing rows conservatively:

- never infer `predicted_fill_probability` from JSON into typed columns unless the parse succeeds exactly
- leave unknown historical forecasts as `NULL`
- set historical paper rows:
  - `submitted_quantity = trade_lifecycle.quantity`
  - `filled_quantity = trade_lifecycle.quantity`
  - `terminal_outcome = 'paper_filled'`
- set historical live rows from:
  - `trade_lifecycle.quantity`
  - exchange fill progress
  - `entry_fill_status`
- do not reinterpret `opened` as filled

### Phase 4: query and UI cutover

- replace current `execution_quality_summary(...)` query
- mark old metrics as deprecated
- require minimum sample thresholds before showing good/warn/bad badges

## UI and badge gating contract

Operator status badges must not render `Aligned`, `Watch`, or `Drift` unless:

- mode is `live`
- predicted sample count >= 20
- terminal intent count >= 20
- at least one non-zero filled quantity exists

If those thresholds are not met:

- badge text must be `Insufficient Sample`
- tone must be neutral or warn
- explanation must say the metric is diagnostic only

Replay-derived badge inputs must also require:

- replay trade count >= 50

Otherwise replay metrics must be labeled:

- `diagnostic only`

## Dashboard and metric renames

### Dashboard/API fields to rename

Current names are misleading and should be deprecated.

- `replay_edge_realization_ratio`
  - rename to `replay_trade_weighted_edge_realization_ratio_diag`
- `replay_fill_rate`
  - rename to `replay_trade_weighted_fill_rate_diag`
- `replay_slippage_bps`
  - rename to `replay_trade_weighted_slippage_bps_diag`
- `recent_expected_fill_probability`
  - rename to `recent_live_predicted_fill_probability_mean`
- `recent_actual_fill_rate`
  - rename to `recent_live_filled_quantity_ratio`
- `recent_filled_count`
  - rename to `recent_live_intents_with_fill_count`
- `recent_intent_count`
  - rename to `recent_live_terminal_intent_count`
- `recent_expected_fill_intent_count`
  - rename to `recent_live_predicted_fill_sample_count`

### Dashboard labels to rename

- panel title `Execution quality`
  - rename to `Execution truth`
- label `Actual Fill`
  - rename to `Filled Quantity Ratio`
- label `Expected Fill`
  - rename to `Predicted Fill Probability`
- label `Replay Edge Realization`
  - rename to `Replay Edge Realization (trade-weighted, diagnostic)`

### Prometheus metrics to rename

Current metrics in `crates/api/src/main.rs` should be deprecated:

- `kalshi_v3_replay_edge_realization_ratio`
- `kalshi_v3_recent_expected_fill_probability`
- `kalshi_v3_recent_actual_fill_rate`

Replace with:

- `kalshi_v3_replay_trade_weighted_edge_realization_ratio_diag`
- `kalshi_v3_replay_trade_weighted_fill_rate_diag`
- `kalshi_v3_replay_trade_weighted_slippage_bps_diag`
- `kalshi_v3_recent_live_predicted_fill_probability_mean`
- `kalshi_v3_recent_live_filled_quantity_ratio`
- `kalshi_v3_recent_live_actual_fill_hit_rate`
- `kalshi_v3_recent_live_terminal_intent_count`
- `kalshi_v3_recent_live_predicted_fill_sample_count`

## Immediate implementation order

1. add typed execution forecast and quantity-truth columns
2. stop reading `fill_probability` from `stop_conditions_json`
3. update decision and execution writers
4. replace the summary query
5. rename API/UI/Prometheus fields
6. gate badges on live-only minimum sample thresholds

