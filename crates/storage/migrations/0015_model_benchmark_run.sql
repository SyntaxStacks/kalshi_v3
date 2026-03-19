create table if not exists model_benchmark_run (
    id bigserial primary key,
    lane_key text not null,
    symbol text not null,
    strategy_family text not null,
    source text not null,
    example_count integer not null,
    champion_model_name text,
    details_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_model_benchmark_run_lane_created
    on model_benchmark_run (lane_key, created_at desc);

create table if not exists model_benchmark_result (
    id bigserial primary key,
    run_id bigint not null references model_benchmark_run(id) on delete cascade,
    model_name text not null,
    rank integer not null,
    brier double precision not null,
    execution_pnl double precision not null,
    sample_count integer not null,
    trade_count integer not null,
    win_rate double precision not null,
    created_at timestamptz not null default now(),
    unique (run_id, model_name)
);

create index if not exists idx_model_benchmark_result_run_rank
    on model_benchmark_result (run_id, rank asc);
