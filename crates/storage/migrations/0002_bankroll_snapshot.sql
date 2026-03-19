create table if not exists bankroll_snapshot (
    id bigserial primary key,
    scope text not null,
    mode text not null,
    strategy_family text not null,
    bankroll double precision not null default 0,
    deployable_balance double precision not null default 0,
    open_exposure double precision not null default 0,
    realized_pnl double precision not null default 0,
    unrealized_pnl double precision not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_bankroll_snapshot_scope_created
    on bankroll_snapshot (scope, created_at desc);

