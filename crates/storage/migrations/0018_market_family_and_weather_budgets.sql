alter table market_feature_snapshot
    add column if not exists market_family text not null default 'crypto';

alter table model_inference
    add column if not exists market_family text not null default 'crypto';

alter table opportunity_decision
    add column if not exists market_family text not null default 'crypto';

alter table execution_intent
    add column if not exists market_family text not null default 'crypto';

alter table lane_state
    add column if not exists market_family text not null default 'crypto';

alter table lane_state_transition
    add column if not exists market_family text not null default 'crypto';

alter table trade_lifecycle
    add column if not exists market_family text not null default 'crypto';

alter table bankroll_snapshot
    add column if not exists market_family text not null default 'all';

alter table probability_calibration
    add column if not exists market_family text not null default 'crypto';

alter table historical_replay_example
    add column if not exists market_family text not null default 'crypto';

alter table model_benchmark_run
    add column if not exists market_family text not null default 'crypto';

alter table trained_model_artifact
    add column if not exists market_family text not null default 'crypto';

drop index if exists idx_market_feature_snapshot_market_created;
create index if not exists idx_market_feature_snapshot_family_market_created
    on market_feature_snapshot (market_family, market_id, created_at desc);

create index if not exists idx_model_inference_family_lane_created
    on model_inference (market_family, lane_key, created_at desc);

create index if not exists idx_opportunity_decision_family_lane_created
    on opportunity_decision (market_family, lane_key, created_at desc);

create index if not exists idx_lane_state_family_updated
    on lane_state (market_family, updated_at desc);

create index if not exists idx_trade_lifecycle_family_created
    on trade_lifecycle (market_family, created_at desc);

create index if not exists idx_bankroll_snapshot_family_scope_created
    on bankroll_snapshot (market_family, scope, created_at desc);

create index if not exists idx_probability_calibration_family_updated
    on probability_calibration (market_family, updated_at desc);

create index if not exists idx_historical_replay_family_symbol_recorded
    on historical_replay_example (market_family, symbol, strategy_family, recorded_at asc);

create index if not exists idx_model_benchmark_run_family_lane_created
    on model_benchmark_run (market_family, lane_key, created_at desc);

drop index if exists idx_trained_model_artifact_lookup;
create unique index if not exists uq_trained_model_artifact_family_lookup
    on trained_model_artifact (model_name, strategy_family, market_family, coalesce(symbol, ''));

create index if not exists idx_trained_model_artifact_family_lookup
    on trained_model_artifact (market_family, model_name, strategy_family, symbol, updated_at desc);
