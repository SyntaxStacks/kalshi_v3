create extension if not exists timescaledb;

create table if not exists market_feature_snapshot (
    id bigserial primary key,
    market_id bigint not null,
    exchange text not null,
    symbol text not null,
    window_minutes integer not null,
    seconds_to_expiry integer not null,
    time_to_expiry_bucket text not null,
    venue_features_json jsonb not null default '{}'::jsonb,
    settlement_features_json jsonb not null default '{}'::jsonb,
    external_reference_json jsonb not null default '{}'::jsonb,
    venue_quality_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists model_inference (
    id bigserial primary key,
    market_id bigint not null,
    lane_key text not null,
    strategy_family text not null,
    model_name text not null,
    raw_score double precision not null,
    raw_probability_yes double precision not null,
    calibrated_probability_yes double precision not null,
    raw_confidence double precision not null,
    calibrated_confidence double precision not null,
    feature_version text not null,
    rationale_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_model_inference_lane_created
    on model_inference (lane_key, created_at desc);

create table if not exists opportunity_decision (
    id bigserial primary key,
    market_id bigint not null,
    lane_key text not null,
    strategy_family text not null,
    model_name text not null,
    side text not null,
    market_prob double precision not null,
    model_prob double precision not null,
    edge double precision not null,
    confidence double precision not null,
    approved boolean not null,
    reasons_json jsonb not null default '[]'::jsonb,
    recommended_size double precision not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_opportunity_decision_lane_created
    on opportunity_decision (lane_key, created_at desc);

create table if not exists execution_intent (
    id bigserial primary key,
    decision_id bigint not null,
    mode text not null,
    entry_style text not null,
    target_ladder_json jsonb not null default '[]'::jsonb,
    timeout_seconds integer not null,
    force_exit_buffer_seconds integer not null,
    stop_conditions_json jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists lane_state (
    lane_key text primary key,
    promotion_state text not null,
    recent_pnl double precision not null default 0,
    recent_brier double precision not null default 0,
    recent_execution_quality double precision not null default 0,
    recent_replay_expectancy double precision not null default 0,
    quarantine_reason text,
    current_champion_model text,
    updated_at timestamptz not null default now()
);

create table if not exists trade_lifecycle (
    id bigserial primary key,
    parent_trade_id bigint,
    lane_key text not null,
    strategy_family text not null,
    mode text not null,
    status text not null,
    quantity double precision not null,
    entry_price double precision not null,
    exit_price double precision,
    realized_pnl double precision,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    closed_at timestamptz
);
