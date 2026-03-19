alter table if exists model_benchmark_result
    add column if not exists fill_rate double precision not null default 0,
    add column if not exists slippage_bps double precision not null default 0,
    add column if not exists edge_realization_ratio double precision not null default 0;
