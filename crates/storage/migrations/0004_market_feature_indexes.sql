create index if not exists idx_market_feature_snapshot_market_created
    on market_feature_snapshot (market_id, created_at desc, id desc);

create index if not exists idx_market_feature_snapshot_created
    on market_feature_snapshot (created_at desc, id desc);
