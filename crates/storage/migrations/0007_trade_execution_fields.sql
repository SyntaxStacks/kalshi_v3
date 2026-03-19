alter table trade_lifecycle
    add column if not exists market_id bigint;

alter table trade_lifecycle
    add column if not exists market_ticker text;

alter table trade_lifecycle
    add column if not exists side text;

alter table trade_lifecycle
    add column if not exists timeout_seconds integer;

alter table trade_lifecycle
    add column if not exists force_exit_buffer_seconds integer;

alter table trade_lifecycle
    add column if not exists expires_at timestamptz;

alter table trade_lifecycle
    add column if not exists entry_fee double precision not null default 0;

alter table trade_lifecycle
    add column if not exists exit_fee double precision not null default 0;

alter table trade_lifecycle
    add column if not exists entry_exchange_order_id text;

alter table trade_lifecycle
    add column if not exists entry_client_order_id text;

alter table trade_lifecycle
    add column if not exists entry_fill_status text;

alter table trade_lifecycle
    add column if not exists exit_exchange_order_id text;

alter table trade_lifecycle
    add column if not exists exit_client_order_id text;

alter table trade_lifecycle
    add column if not exists exit_fill_status text;

update trade_lifecycle
set market_id = coalesce(market_id, (metadata_json->>'market_id')::bigint),
    side = coalesce(side, metadata_json->>'side'),
    timeout_seconds = coalesce(timeout_seconds, (metadata_json->>'timeout_seconds')::integer),
    force_exit_buffer_seconds = coalesce(force_exit_buffer_seconds, (metadata_json->>'force_exit_buffer_seconds')::integer),
    expires_at = coalesce(expires_at, (metadata_json->>'expires_at')::timestamptz),
    entry_fee = coalesce(entry_fee, (metadata_json->>'entry_fee')::double precision, 0),
    exit_fee = coalesce(exit_fee, (metadata_json->>'exit_fee')::double precision, 0),
    market_ticker = coalesce(market_ticker, metadata_json->>'market_ticker'),
    entry_exchange_order_id = coalesce(entry_exchange_order_id, metadata_json->>'entry_order_id'),
    entry_client_order_id = coalesce(entry_client_order_id, metadata_json->>'entry_client_order_id'),
    entry_fill_status = coalesce(entry_fill_status, metadata_json->>'entry_status'),
    exit_exchange_order_id = coalesce(exit_exchange_order_id, metadata_json->>'exit_order_id'),
    exit_client_order_id = coalesce(exit_client_order_id, metadata_json->>'exit_client_order_id'),
    exit_fill_status = coalesce(exit_fill_status, metadata_json->>'exit_status');

create index if not exists idx_trade_lifecycle_market_id
    on trade_lifecycle (market_id);

create index if not exists idx_trade_lifecycle_open_status
    on trade_lifecycle (status, closed_at);

create index if not exists idx_trade_lifecycle_entry_client_order_id
    on trade_lifecycle (entry_client_order_id);

create index if not exists idx_trade_lifecycle_exit_client_order_id
    on trade_lifecycle (exit_client_order_id);
