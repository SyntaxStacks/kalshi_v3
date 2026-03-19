create table if not exists trained_model_artifact (
    id bigserial primary key,
    model_name text not null,
    strategy_family text not null,
    symbol text,
    feature_version text not null,
    sample_count integer not null default 0,
    weights_json jsonb not null,
    metrics_json jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    unique (model_name, strategy_family, symbol)
);

create index if not exists idx_trained_model_artifact_lookup
    on trained_model_artifact (model_name, strategy_family, symbol, updated_at desc);
