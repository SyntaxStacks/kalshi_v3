alter table market_feature_snapshot
    add column if not exists feature_version text not null default 'v3_feature_poll_v1';
