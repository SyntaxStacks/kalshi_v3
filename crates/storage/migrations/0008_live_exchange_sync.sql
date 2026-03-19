create table if not exists live_exchange_sync_status (
    source text primary key,
    synced_at timestamptz not null default now(),
    positions_count integer not null default 0,
    resting_orders_count integer not null default 0,
    recent_fills_count integer not null default 0,
    local_open_live_trades_count integer not null default 0,
    status text not null default 'unknown',
    issues_json jsonb not null default '[]'::jsonb,
    details_json jsonb not null default '{}'::jsonb
);

create table if not exists live_position_sync (
    market_ticker text primary key,
    position_count double precision not null default 0,
    resting_order_count integer not null default 0,
    fees_paid double precision not null default 0,
    market_exposure double precision not null default 0,
    realized_pnl double precision not null default 0,
    synced_at timestamptz not null default now(),
    details_json jsonb not null default '{}'::jsonb
);

create table if not exists live_order_sync (
    order_id text primary key,
    client_order_id text,
    market_ticker text,
    action text,
    side text,
    status text,
    count double precision not null default 0,
    fill_count double precision not null default 0,
    remaining_count double precision not null default 0,
    yes_price double precision not null default 0,
    no_price double precision not null default 0,
    expiration_time timestamptz,
    created_time timestamptz,
    synced_at timestamptz not null default now(),
    details_json jsonb not null default '{}'::jsonb
);

create table if not exists live_fill_sync (
    fill_id text primary key,
    order_id text,
    client_order_id text,
    market_ticker text,
    action text,
    side text,
    count double precision not null default 0,
    yes_price double precision not null default 0,
    no_price double precision not null default 0,
    fee_paid double precision not null default 0,
    created_time timestamptz,
    synced_at timestamptz not null default now(),
    details_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_live_order_sync_market_ticker
    on live_order_sync (market_ticker, status);

create index if not exists idx_live_fill_sync_created_time
    on live_fill_sync (created_time desc);
