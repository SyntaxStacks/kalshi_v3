create table if not exists historical_replay_example (
    id bigserial primary key,
    source text not null,
    source_key text not null unique,
    exchange text not null,
    symbol text not null,
    strategy_family text not null,
    market_id bigint,
    market_slug text not null,
    recorded_at timestamptz not null,
    resolved_yes_probability double precision not null,
    market_prob double precision not null,
    time_to_expiry_bucket text not null,
    feature_version text not null,
    feature_vector_json jsonb not null default '{}'::jsonb,
    metadata_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_historical_replay_symbol_recorded
    on historical_replay_example (symbol, strategy_family, recorded_at asc);
