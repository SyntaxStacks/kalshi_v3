alter table trade_lifecycle
    add column if not exists decision_id bigint;

create index if not exists idx_trade_lifecycle_decision_id
    on trade_lifecycle (decision_id);
