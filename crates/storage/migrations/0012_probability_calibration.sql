create table if not exists probability_calibration (
    symbol text not null,
    strategy_family text not null,
    model_name text not null,
    expiry_bucket text not null,
    mean_error double precision not null default 0,
    sample_count integer not null default 0,
    updated_at timestamptz not null default now(),
    primary key (symbol, strategy_family, model_name, expiry_bucket)
);

create index if not exists idx_probability_calibration_updated
    on probability_calibration (updated_at desc);
