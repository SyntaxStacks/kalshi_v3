alter table trained_model_artifact
    add column if not exists expiry_regime text;

alter table model_benchmark_run
    add column if not exists expiry_regime text;

drop index if exists uq_trained_model_artifact_family_lookup;
create unique index if not exists uq_trained_model_artifact_family_regime_lookup
    on trained_model_artifact (
        model_name,
        strategy_family,
        market_family,
        coalesce(symbol, ''),
        coalesce(expiry_regime, '')
    );

drop index if exists idx_trained_model_artifact_family_lookup;
create index if not exists idx_trained_model_artifact_family_regime_lookup
    on trained_model_artifact (
        market_family,
        model_name,
        strategy_family,
        symbol,
        expiry_regime,
        updated_at desc
    );

create index if not exists idx_model_benchmark_run_family_lane_regime_created
    on model_benchmark_run (market_family, lane_key, expiry_regime, created_at desc);
