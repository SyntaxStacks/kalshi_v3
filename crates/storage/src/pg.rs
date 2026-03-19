use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{
    BankrollCard, DashboardSnapshot, ExecutionQualitySummary, FamilyExecutionTruthSummary,
    LaneExecutionTruthSummary, LaneInspectionSnapshot, LaneReplaySummary, LaneState, LaneTradeCard,
    LiveExceptionSnapshot, LiveExchangeSyncSummary, LiveFillCard, LiveIntentCard, LiveOrderCard,
    LivePositionCard, LiveTradeExceptionCard, MarketFamily, MarketFeatureSnapshotRecord,
    ModelInference, OpenTradeSummary, OperatorActionEvent, OperatorControlState, OpportunityCard,
    OpportunityDecision, PromotionState, ReadinessSummary, ReplayBenchmarkCard, StrategyFamily,
    TradeMode,
};
use serde::Serialize;
use serde_json::json;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgPoolOptions};

#[derive(Clone)]
pub struct Storage {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct PendingExecutionIntent {
    pub intent_id: i64,
    pub decision_id: i64,
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub strategy_family: StrategyFamily,
    pub side: String,
    pub market_prob: f64,
    pub model_prob: f64,
    pub confidence: f64,
    pub recommended_size: f64,
    pub mode: TradeMode,
    pub entry_style: String,
    pub timeout_seconds: i32,
    pub force_exit_buffer_seconds: i32,
    // Typed execution forecast semantics. Null means the forecast was not computed for this intent.
    pub predicted_fill_probability: Option<f64>,
    pub predicted_slippage_bps: Option<f64>,
    pub predicted_queue_ahead: Option<f64>,
    pub execution_forecast_version: Option<String>,
    // Quantity truth is stored separately from status because `opened` is not fill truth.
    pub submitted_quantity: f64,
    pub accepted_quantity: f64,
    pub filled_quantity: f64,
    pub cancelled_quantity: f64,
    pub rejected_quantity: f64,
    pub avg_fill_price: Option<f64>,
    pub first_fill_at: Option<DateTime<Utc>>,
    pub last_fill_at: Option<DateTime<Utc>>,
    pub terminal_outcome: Option<String>,
    pub promotion_state_at_creation: Option<PromotionState>,
    pub stop_conditions_json: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ActiveLiveExecutionIntent {
    pub intent_id: i64,
    pub decision_id: i64,
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub strategy_family: StrategyFamily,
    pub side: String,
    pub market_prob: f64,
    pub recommended_size: f64,
    pub mode: TradeMode,
    pub timeout_seconds: i32,
    pub force_exit_buffer_seconds: i32,
    pub created_at: DateTime<Utc>,
    pub status: String,
    pub market_ticker: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub fill_status: Option<String>,
    pub submitted_quantity: f64,
    pub accepted_quantity: f64,
    pub filled_quantity: f64,
    pub cancelled_quantity: f64,
    pub rejected_quantity: f64,
    pub avg_fill_price: Option<f64>,
    pub terminal_outcome: Option<String>,
    pub last_transition_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OpenTradeForExit {
    pub trade_id: i64,
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub market_ticker: Option<String>,
    pub lane_key: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub entry_fee: f64,
    pub strategy_family: StrategyFamily,
    pub mode: TradeMode,
    pub created_at: DateTime<Utc>,
    pub timeout_seconds: i32,
    pub force_exit_buffer_seconds: i32,
    pub expires_at: Option<DateTime<Utc>>,
    pub exit_exchange_order_id: Option<String>,
    pub exit_client_order_id: Option<String>,
    pub exit_fill_status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpenLiveTradeForReconciliation {
    pub trade_id: i64,
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub market_ticker: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub entry_fee: f64,
    pub created_at: DateTime<Utc>,
    pub entry_exchange_order_id: Option<String>,
    pub entry_client_order_id: Option<String>,
    pub exit_exchange_order_id: Option<String>,
    pub exit_client_order_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LiveTradeReconciliationSnapshot {
    pub has_position: bool,
    pub has_resting_order: bool,
    pub matched_exit_fill_quantity: f64,
    pub matched_exit_fill_price: Option<f64>,
    pub matched_exit_fill_fee: f64,
    pub matched_exit_fill_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct TradeExitProgress {
    pub closed_quantity: f64,
    pub exit_fee: f64,
}

#[derive(Debug, Clone)]
pub struct ClosedTradeNotification {
    pub trade_id: i64,
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub strategy_family: StrategyFamily,
    pub mode: TradeMode,
    pub market_ticker: Option<String>,
    pub market_title: Option<String>,
    pub side: Option<String>,
    pub status: String,
    pub close_reason: Option<String>,
    pub quantity: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub realized_pnl: f64,
    pub closed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SymbolLanePolicy {
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub promotion_state: PromotionState,
    pub promotion_reason: Option<String>,
    pub recent_pnl: f64,
    pub recent_brier: f64,
    pub recent_execution_quality: f64,
    pub recent_replay_expectancy: f64,
    pub quarantine_reason: Option<String>,
    pub current_champion_model: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RecentTradeMetrics {
    pub trade_count: i64,
    pub win_count: i64,
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerStatusCard {
    pub service: String,
    pub status: String,
    pub last_started_at: Option<DateTime<Utc>>,
    pub last_succeeded_at: Option<DateTime<Utc>>,
    pub last_failed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub details_json: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ProbabilityCalibrationCard {
    pub mean_error: f64,
    pub sample_count: i32,
}

#[derive(Debug, Clone)]
pub struct TrainedModelArtifactCard {
    pub model_name: String,
    pub market_family: MarketFamily,
    pub strategy_family: StrategyFamily,
    pub symbol: Option<String>,
    pub feature_version: String,
    pub sample_count: i32,
    pub weights: market_models::TrainedLinearWeights,
    pub metrics_json: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct HistoricalReplayExampleCard {
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub time_to_expiry_bucket: String,
    pub target_yes_probability: f64,
    pub market_prob: f64,
    pub features: market_models::FeatureVector,
}

#[derive(Debug, Clone)]
pub struct ModelBenchmarkRunInsert {
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub symbol: String,
    pub strategy_family: StrategyFamily,
    pub source: String,
    pub example_count: i32,
    pub champion_model_name: Option<String>,
    pub details_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ModelBenchmarkResultInsert {
    pub model_name: String,
    pub rank: i32,
    pub brier: f64,
    pub execution_pnl: f64,
    pub sample_count: i32,
    pub trade_count: i32,
    pub win_rate: f64,
    pub fill_rate: f64,
    pub slippage_bps: f64,
    pub edge_realization_ratio: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionIntentStateUpdate {
    pub market_ticker: Option<String>,
    pub side: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub fill_status: Option<String>,
    pub accepted_quantity: Option<f64>,
    pub filled_quantity: Option<f64>,
    pub cancelled_quantity: Option<f64>,
    pub rejected_quantity: Option<f64>,
    pub avg_fill_price: Option<f64>,
    pub first_fill_at: Option<DateTime<Utc>>,
    pub last_fill_at: Option<DateTime<Utc>>,
    pub terminal_outcome: Option<String>,
    pub details_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct LivePositionSyncRecord {
    pub market_ticker: String,
    pub position_count: f64,
    pub resting_order_count: i32,
    pub fees_paid: f64,
    pub market_exposure: f64,
    pub realized_pnl: f64,
    pub details_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct LiveOrderSyncRecord {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_ticker: Option<String>,
    pub action: Option<String>,
    pub side: Option<String>,
    pub status: Option<String>,
    pub count: f64,
    pub fill_count: f64,
    pub remaining_count: f64,
    pub yes_price: f64,
    pub no_price: f64,
    pub expiration_time: Option<DateTime<Utc>>,
    pub created_time: Option<DateTime<Utc>>,
    pub details_json: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct LiveFillSyncRecord {
    pub fill_id: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub market_ticker: Option<String>,
    pub action: Option<String>,
    pub side: Option<String>,
    pub count: f64,
    pub yes_price: f64,
    pub no_price: f64,
    pub fee_paid: f64,
    pub created_time: Option<DateTime<Utc>>,
    pub details_json: serde_json::Value,
}

impl Storage {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn healthcheck(&self) -> Result<()> {
        sqlx::query("select 1").execute(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub async fn ensure_bootstrap_state(
        &self,
        initial_paper_bankroll: f64,
        initial_live_bankroll: f64,
        initial_paper_crypto_budget: f64,
        initial_paper_weather_budget: f64,
        initial_live_crypto_budget: f64,
        initial_live_weather_budget: f64,
    ) -> Result<()> {
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Paper, MarketFamily::All),
            MarketFamily::All,
            initial_paper_bankroll,
            SqlTradeMode::Paper,
        )
        .await?;
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Live, MarketFamily::All),
            MarketFamily::All,
            initial_live_bankroll,
            SqlTradeMode::Live,
        )
        .await?;
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Paper, MarketFamily::Crypto),
            MarketFamily::Crypto,
            initial_paper_crypto_budget,
            SqlTradeMode::Paper,
        )
        .await?;
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Paper, MarketFamily::Weather),
            MarketFamily::Weather,
            initial_paper_weather_budget,
            SqlTradeMode::Paper,
        )
        .await?;
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Live, MarketFamily::Crypto),
            MarketFamily::Crypto,
            initial_live_crypto_budget,
            SqlTradeMode::Live,
        )
        .await?;
        self.ensure_bootstrap_bankroll(
            portfolio_scope(TradeMode::Live, MarketFamily::Weather),
            MarketFamily::Weather,
            initial_live_weather_budget,
            SqlTradeMode::Live,
        )
        .await?;
        Ok(())
    }

    pub async fn dashboard_snapshot(
        &self,
        family: Option<MarketFamily>,
    ) -> Result<DashboardSnapshot> {
        let bankrolls = self.list_latest_bankrolls(family).await?;
        let lanes = self.list_lane_states(family).await?;
        let open_trades = self.list_open_trades(family).await?;
        let opportunities = self.list_latest_opportunities(family, 6).await?;
        let execution_quality = self.execution_quality_summary(family).await?;
        let lane_execution_truth = self.lane_execution_truth_summaries(family, &lanes).await?;
        let family_execution_truth = summarize_family_execution_truth(&lane_execution_truth);
        let live_sync = self.latest_live_exchange_sync_summary().await?;
        let live_exceptions = self.live_exception_snapshot().await?;

        let readiness = ReadinessSummary {
            overall_status: overall_readiness_status(&lanes),
            shadow_count: count_state(&lanes, PromotionState::Shadow),
            paper_active_count: count_state(&lanes, PromotionState::PaperActive),
            live_micro_count: count_state(&lanes, PromotionState::LiveMicro),
            live_scaled_count: count_state(&lanes, PromotionState::LiveScaled),
            quarantined_count: count_state(&lanes, PromotionState::Quarantined),
            lanes,
        };

        Ok(DashboardSnapshot {
            market_family: family,
            bankrolls,
            readiness,
            open_trades,
            opportunities,
            execution_quality,
            family_execution_truth,
            lane_execution_truth,
            live_sync,
            live_exceptions,
        })
    }

    pub async fn insert_feature_snapshot(
        &self,
        snapshot: &MarketFeatureSnapshotRecord,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into market_feature_snapshot (
                market_id, market_family, exchange, symbol, window_minutes, seconds_to_expiry, time_to_expiry_bucket,
                feature_version, venue_features_json, settlement_features_json, external_reference_json,
                venue_quality_json, created_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13)
            "#,
        )
        .bind(snapshot.market_id)
        .bind(SqlMarketFamily::from(snapshot.market_family))
        .bind(&snapshot.exchange)
        .bind(&snapshot.symbol)
        .bind(snapshot.window_minutes)
        .bind(snapshot.seconds_to_expiry)
        .bind(&snapshot.time_to_expiry_bucket)
        .bind(&snapshot.feature_version)
        .bind(json!({
            "market_ticker": snapshot.market_ticker,
            "market_title": snapshot.market_title,
            "market_prob": snapshot.market_prob,
            "best_bid": snapshot.best_bid,
            "best_ask": snapshot.best_ask,
            "last_price": snapshot.last_price,
            "bid_size": snapshot.bid_size,
            "ask_size": snapshot.ask_size,
            "liquidity": snapshot.liquidity,
            "order_book_imbalance": snapshot.order_book_imbalance,
            "aggressive_buy_ratio": snapshot.aggressive_buy_ratio,
            "size_ahead": snapshot.size_ahead,
            "trade_rate": snapshot.trade_rate,
            "quote_churn": snapshot.quote_churn,
            "size_ahead_real": snapshot.size_ahead_real,
            "trade_consumption_rate": snapshot.trade_consumption_rate,
            "cancel_rate": snapshot.cancel_rate,
            "queue_decay_rate": snapshot.queue_decay_rate,
        }))
        .bind(json!({
            "seconds_to_expiry": snapshot.seconds_to_expiry,
            "time_to_expiry_bucket": snapshot.time_to_expiry_bucket,
            "settlement_regime": snapshot.settlement_regime,
            "averaging_window_progress": snapshot.averaging_window_progress,
            "threshold_distance_bps_proxy": snapshot.threshold_distance_bps_proxy,
            "distance_to_strike_bps": snapshot.distance_to_strike_bps,
            "time_decay_factor": snapshot.time_decay_factor,
            "last_minute_avg_proxy": snapshot.last_minute_avg_proxy,
            "weather_city": snapshot.weather_city,
            "weather_contract_kind": snapshot.weather_contract_kind,
            "weather_market_date": snapshot.weather_market_date,
            "weather_strike_type": snapshot.weather_strike_type,
            "weather_floor_strike": snapshot.weather_floor_strike,
            "weather_cap_strike": snapshot.weather_cap_strike,
        }))
        .bind(json!({
            "reference_price": snapshot.reference_price,
            "reference_previous_price": snapshot.reference_previous_price,
            "reference_price_change_bps": snapshot.reference_price_change_bps,
            "reference_velocity": snapshot.reference_velocity,
            "realized_vol_short": snapshot.realized_vol_short,
            "reference_yes_prob": snapshot.reference_yes_prob,
            "reference_gap_bps": snapshot.reference_gap_bps,
            "reference_age_seconds": snapshot.reference_age_seconds,
            "weather_forecast_temperature_f": snapshot.weather_forecast_temperature_f,
            "weather_observation_temperature_f": snapshot.weather_observation_temperature_f,
            "weather_reference_confidence": snapshot.weather_reference_confidence,
            "weather_reference_source": snapshot.weather_reference_source,
        }))
        .bind(json!({
            "spread_bps": snapshot.spread_bps,
            "venue_quality_score": snapshot.venue_quality_score,
            "market_data_age_seconds": snapshot.market_data_age_seconds,
        }))
        .bind(snapshot.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_ingest_event(
        &self,
        source: &str,
        topic: &str,
        entity_key: &str,
        payload_json: &serde_json::Value,
        observed_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into ingest_event_log (
                source, topic, entity_key, payload_json, observed_at
            )
            values ($1, $2, $3, $4::jsonb, $5)
            "#,
        )
        .bind(source)
        .bind(topic)
        .bind(entity_key)
        .bind(payload_json)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn historical_replay_example_count(&self) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>("select count(*) from historical_replay_example")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    pub async fn insert_historical_replay_examples(
        &self,
        rows: &[HistoricalReplayExampleInsert],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        const MAX_BIND_PARAMS: usize = 60_000;
        const PARAMS_PER_ROW: usize = 15;
        let chunk_size = (MAX_BIND_PARAMS / PARAMS_PER_ROW).max(1);
        let mut inserted = 0usize;

        for chunk in rows.chunks(chunk_size) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                insert into historical_replay_example (
                    source, source_key, market_family, exchange, symbol, strategy_family, market_id, market_slug,
                    recorded_at, resolved_yes_probability, market_prob, time_to_expiry_bucket,
                    feature_version, feature_vector_json, metadata_json
                )
                "#,
            );
            builder.push_values(chunk, |mut b, row| {
                b.push_bind(&row.source)
                    .push_bind(&row.source_key)
                    .push_bind(SqlMarketFamily::from(row.market_family))
                    .push_bind(&row.exchange)
                    .push_bind(&row.symbol)
                    .push_bind(SqlStrategyFamily::from(row.strategy_family))
                    .push_bind(row.market_id)
                    .push_bind(&row.market_slug)
                    .push_bind(row.recorded_at)
                    .push_bind(row.resolved_yes_probability)
                    .push_bind(row.market_prob)
                    .push_bind(&row.time_to_expiry_bucket)
                    .push_bind(&row.feature_version)
                    .push_bind(&row.feature_vector_json)
                    .push_bind(&row.metadata_json);
            });
            builder.push(" on conflict (source_key) do nothing");
            let result = builder.build().execute(&self.pool).await?;
            inserted += result.rows_affected() as usize;
        }

        Ok(inserted)
    }

    pub async fn list_historical_replay_examples(
        &self,
        market_family: MarketFamily,
        symbol: &str,
        strategy_family: StrategyFamily,
        limit: i64,
    ) -> Result<Vec<HistoricalReplayExampleCard>> {
        let rows = sqlx::query_as::<_, HistoricalReplayExampleRow>(
            r#"
            select market_family, lane_key, time_to_expiry_bucket, resolved_yes_probability, market_prob, feature_vector_json
            from (
                select
                    format(
                        'kalshi:%s:15:buy_yes:%s:%s',
                        symbol,
                        strategy_family,
                        coalesce(metadata_json->>'champion_hint', 'baseline_logit_v1')
                    ) as lane_key,
                    market_family,
                    time_to_expiry_bucket,
                    resolved_yes_probability,
                    market_prob,
                    feature_vector_json,
                    recorded_at
                from historical_replay_example
                where market_family = $1
                  and symbol = $2
                  and strategy_family = $3
                order by recorded_at asc
                limit $4
            ) ranked
            "#,
        )
        .bind(SqlMarketFamily::from(market_family))
        .bind(symbol.to_lowercase())
        .bind(SqlStrategyFamily::from(strategy_family))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn insert_model_benchmark_run(
        &self,
        run: &ModelBenchmarkRunInsert,
        results: &[ModelBenchmarkResultInsert],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let run_id = sqlx::query_scalar::<_, i64>(
            r#"
            insert into model_benchmark_run (
                lane_key, market_family, symbol, strategy_family, source, example_count,
                champion_model_name, details_json
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            returning id
            "#,
        )
        .bind(&run.lane_key)
        .bind(SqlMarketFamily::from(run.market_family))
        .bind(&run.symbol)
        .bind(SqlStrategyFamily::from(run.strategy_family))
        .bind(&run.source)
        .bind(run.example_count)
        .bind(&run.champion_model_name)
        .bind(&run.details_json)
        .fetch_one(&mut *tx)
        .await?;

        if !results.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                insert into model_benchmark_result (
                    run_id, model_name, rank, brier, execution_pnl,
                    sample_count, trade_count, win_rate, fill_rate, slippage_bps,
                    edge_realization_ratio
                )
                "#,
            );
            builder.push_values(results, |mut b, result| {
                b.push_bind(run_id)
                    .push_bind(&result.model_name)
                    .push_bind(result.rank)
                    .push_bind(result.brier)
                    .push_bind(result.execution_pnl)
                    .push_bind(result.sample_count)
                    .push_bind(result.trade_count)
                    .push_bind(result.win_rate)
                    .push_bind(result.fill_rate)
                    .push_bind(result.slippage_bps)
                    .push_bind(result.edge_realization_ratio);
            });
            builder.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn latest_model_benchmark_summary_for_lane(
        &self,
        lane_key: &str,
    ) -> Result<Option<LaneReplaySummary>> {
        let run = sqlx::query_as::<_, ModelBenchmarkRunRow>(
            r#"
            select id, lane_key, source, example_count, created_at
            from model_benchmark_run
            where lane_key = $1
            order by created_at desc, id desc
            limit 1
            "#,
        )
        .bind(lane_key)
        .fetch_optional(&self.pool)
        .await?;

        let Some(run) = run else {
            return Ok(None);
        };
        let source = run.source.clone();
        let created_at = run.created_at;
        let example_count = run.example_count;

        let results = sqlx::query_as::<_, ModelBenchmarkResultRow>(
            r#"
            select model_name, rank, brier, execution_pnl, sample_count, trade_count, win_rate
                 , fill_rate, slippage_bps, edge_realization_ratio
            from model_benchmark_result
            where run_id = $1
            order by rank asc, model_name asc
            "#,
        )
        .bind(run.id)
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(LaneReplaySummary {
            example_count,
            source: source.clone(),
            created_at,
            benchmarks: results
                .into_iter()
                .map(|row| ReplayBenchmarkCard {
                    model_name: row.model_name,
                    rank: row.rank,
                    brier: row.brier,
                    execution_pnl: row.execution_pnl,
                    sample_count: row.sample_count,
                    trade_count: row.trade_count,
                    win_rate: row.win_rate,
                    fill_rate: row.fill_rate,
                    slippage_bps: row.slippage_bps,
                    edge_realization_ratio: row.edge_realization_ratio,
                    source: source.clone(),
                    created_at,
                })
                .collect(),
        }))
    }

    pub async fn execution_quality_summary(
        &self,
        family: Option<MarketFamily>,
    ) -> Result<ExecutionQualitySummary> {
        let family_key = family.map(|value| SqlMarketFamily::from(value).as_key().to_string());
        // Replay metrics are trade-weighted diagnostics. Live execution truth is quantity-based and
        // intentionally excludes ambiguous statuses like `opened`.
        let replay_rows = sqlx::query_as::<_, ReplayExecutionQualityRow>(
            r#"
            with latest_runs as (
                select distinct on (lane_key)
                    id,
                    lane_key,
                    created_at
                from model_benchmark_run
                where ($1::text is null or market_family = $1)
                order by lane_key, created_at desc, id desc
            )
            select
                lr.created_at as replay_as_of,
                mbr.trade_count,
                mbr.fill_rate,
                mbr.slippage_bps,
                mbr.edge_realization_ratio
            from latest_runs lr
            join model_benchmark_result mbr on mbr.run_id = lr.id
            where mbr.rank = 1
            "#,
        )
        .bind(&family_key)
        .fetch_all(&self.pool)
        .await?;

        let intent_rows = sqlx::query_as::<_, LiveExecutionQualityRow>(
            r#"
            select
                ei.processed_at,
                ei.predicted_fill_probability,
                ei.submitted_quantity,
                ei.filled_quantity
            from execution_intent ei
            where ei.processed_at is not null
              and ei.mode = 'live'
              and ($1::text is null or ei.market_family = $1)
              and ei.status in ('filled', 'partially_filled', 'cancelled', 'rejected', 'expired', 'superseded', 'orphaned')
            order by ei.processed_at desc, ei.id desc
            limit 500
            "#,
        )
        .bind(&family_key)
        .fetch_all(&self.pool)
        .await?;

        let replay = summarize_replay_execution_quality(&replay_rows);
        let live = summarize_live_execution_quality(&intent_rows);
        Ok(ExecutionQualitySummary {
            as_of: replay.as_of.or(live.as_of).unwrap_or_else(Utc::now),
            replay_lane_count: replay.replay_lane_count,
            replay_trade_count: replay.replay_trade_count,
            replay_trade_weighted_edge_realization_ratio_diag: replay
                .replay_trade_weighted_edge_realization_ratio_diag,
            replay_trade_weighted_fill_rate_diag: replay.replay_trade_weighted_fill_rate_diag,
            replay_trade_weighted_slippage_bps_diag: replay.replay_trade_weighted_slippage_bps_diag,
            recent_live_terminal_intent_count: live.recent_live_terminal_intent_count,
            recent_live_intents_with_fill_count: live.recent_live_intents_with_fill_count,
            recent_live_predicted_fill_sample_count: live.recent_live_predicted_fill_sample_count,
            recent_live_predicted_fill_probability_mean: live
                .recent_live_predicted_fill_probability_mean,
            recent_live_filled_quantity_ratio: live.recent_live_filled_quantity_ratio,
            recent_live_actual_fill_hit_rate: live.recent_live_actual_fill_hit_rate,
            live_sample_sufficient: live.live_sample_sufficient,
            replay_sample_sufficient: replay.replay_sample_sufficient,
        })
    }

    pub async fn lane_execution_truth_summaries(
        &self,
        family: Option<MarketFamily>,
        lane_states: &[LaneState],
    ) -> Result<Vec<LaneExecutionTruthSummary>> {
        let family_key = family.map(|value| SqlMarketFamily::from(value).as_key().to_string());
        let live_rows = sqlx::query_as::<_, LiveLaneExecutionTruthRow>(
            r#"
            select
                od.market_family,
                od.lane_key,
                ei.predicted_fill_probability,
                ei.submitted_quantity,
                ei.filled_quantity
            from execution_intent ei
            join opportunity_decision od on od.id = ei.decision_id
            where ei.processed_at is not null
              and ei.mode = 'live'
              and ($1::text is null or od.market_family = $1)
              and ei.status in ('filled', 'partially_filled', 'cancelled', 'rejected', 'expired', 'superseded', 'orphaned')
            order by ei.processed_at desc, ei.id desc
            limit 1000
            "#,
        )
        .bind(&family_key)
        .fetch_all(&self.pool)
        .await?;

        let replay_rows = sqlx::query_as::<_, ReplayLaneExecutionTruthRow>(
            r#"
            with latest_runs as (
                select distinct on (lane_key)
                    id,
                    lane_key,
                    market_family,
                    created_at
                from model_benchmark_run
                where ($1::text is null or market_family = $1)
                order by lane_key, created_at desc, id desc
            )
            select
                lr.market_family,
                lr.lane_key,
                mbr.trade_count,
                mbr.fill_rate,
                mbr.edge_realization_ratio
            from latest_runs lr
            join model_benchmark_result mbr on mbr.run_id = lr.id
            where mbr.rank = 1
            "#,
        )
        .bind(&family_key)
        .fetch_all(&self.pool)
        .await?;

        Ok(build_lane_execution_truth_summaries(
            &live_rows,
            &replay_rows,
            lane_states,
        ))
    }

    pub async fn list_latest_feature_snapshots(
        &self,
        limit: i64,
    ) -> Result<Vec<MarketFeatureSnapshotRecord>> {
        let rows = sqlx::query_as::<_, FeatureSnapshotRow>(
            r#"
            with latest as (
                select distinct on (market_id)
                    id,
                    market_id,
                    market_family,
                    exchange,
                    symbol,
                    window_minutes,
                    seconds_to_expiry,
                    time_to_expiry_bucket,
                    feature_version,
                    venue_features_json,
                    settlement_features_json,
                    external_reference_json,
                    venue_quality_json,
                    created_at
                from market_feature_snapshot
                where created_at >= now() - interval '72 hours'
                order by market_id, created_at desc, id desc
            ),
            front_crypto as (
                select distinct on (symbol, window_minutes)
                    id,
                    market_id,
                    market_family,
                    exchange,
                    symbol,
                    window_minutes,
                    seconds_to_expiry,
                    time_to_expiry_bucket,
                    feature_version,
                    venue_features_json,
                    settlement_features_json,
                    external_reference_json,
                    venue_quality_json,
                    created_at
                from latest
                where market_family = 'crypto'
                  and seconds_to_expiry > 0
                order by symbol, window_minutes, seconds_to_expiry asc, created_at desc, id desc
            ),
            other_latest as (
                select
                    id,
                    market_id,
                    market_family,
                    exchange,
                    symbol,
                    window_minutes,
                    seconds_to_expiry,
                    time_to_expiry_bucket,
                    feature_version,
                    venue_features_json,
                    settlement_features_json,
                    external_reference_json,
                    venue_quality_json,
                    created_at
                from latest
                where market_family <> 'crypto'
                order by created_at desc, id desc
                limit $1
            )
            select *
            from (
                select * from front_crypto
                union all
                select * from other_latest
            ) decision_feed
            order by created_at desc, id desc
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn latest_feature_snapshot_for_market(
        &self,
        market_id: i64,
    ) -> Result<Option<MarketFeatureSnapshotRecord>> {
        let row = sqlx::query_as::<_, FeatureSnapshotRow>(
            r#"
            select
                id,
                market_id,
                market_family,
                exchange,
                symbol,
                window_minutes,
                seconds_to_expiry,
                time_to_expiry_bucket,
                feature_version,
                venue_features_json,
                settlement_features_json,
                external_reference_json,
                venue_quality_json,
                created_at
            from market_feature_snapshot
            where market_id = $1
            order by created_at desc, id desc
            limit 1
            "#,
        )
        .bind(market_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn latest_feature_snapshot_for_ticker(
        &self,
        market_ticker: &str,
    ) -> Result<Option<MarketFeatureSnapshotRecord>> {
        let row = sqlx::query_as::<_, FeatureSnapshotRow>(
            r#"
            select
                id,
                market_id,
                market_family,
                exchange,
                symbol,
                window_minutes,
                seconds_to_expiry,
                time_to_expiry_bucket,
                feature_version,
                venue_features_json,
                settlement_features_json,
                external_reference_json,
                venue_quality_json,
                created_at
            from market_feature_snapshot
            where venue_features_json->>'market_ticker' = $1
            order by created_at desc, id desc
            limit 1
            "#,
        )
        .bind(market_ticker)
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn latest_decision_at_for_lane(
        &self,
        lane_key: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let created_at = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
            select created_at
            from opportunity_decision
            where lane_key = $1
            order by created_at desc
            limit 1
            "#,
        )
        .bind(lane_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(created_at)
    }

    pub async fn insert_model_inference(&self, inference: &ModelInference) -> Result<i64> {
        let id = sqlx::query_scalar(
            r#"
            insert into model_inference (
                market_id, market_family, lane_key, strategy_family, model_name, raw_score, raw_probability_yes,
                calibrated_probability_yes, raw_confidence, calibrated_confidence, feature_version,
                rationale_json
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
            returning id
            "#,
        )
        .bind(inference.market_id)
        .bind(SqlMarketFamily::from(inference.market_family))
        .bind(&inference.lane_key)
        .bind(SqlStrategyFamily::from(inference.strategy_family))
        .bind(&inference.model_name)
        .bind(inference.raw_score)
        .bind(inference.raw_probability_yes)
        .bind(inference.calibrated_probability_yes)
        .bind(inference.raw_confidence)
        .bind(inference.calibrated_confidence)
        .bind(&inference.feature_version)
        .bind(&inference.rationale_json)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn upsert_probability_calibration(
        &self,
        market_family: MarketFamily,
        symbol: &str,
        strategy_family: StrategyFamily,
        model_name: &str,
        expiry_bucket: &str,
        mean_error: f64,
        sample_count: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into probability_calibration (
                symbol, market_family, strategy_family, model_name, expiry_bucket, mean_error, sample_count, updated_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, now())
            on conflict (symbol, strategy_family, model_name, expiry_bucket) do update
            set mean_error = excluded.mean_error,
                market_family = excluded.market_family,
                sample_count = excluded.sample_count,
                updated_at = now()
            "#,
        )
        .bind(symbol.to_lowercase())
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlStrategyFamily::from(strategy_family))
        .bind(model_name)
        .bind(expiry_bucket)
        .bind(mean_error)
        .bind(sample_count)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_probability_calibration(
        &self,
        market_family: MarketFamily,
        symbol: &str,
        strategy_family: StrategyFamily,
        model_name: &str,
        expiry_bucket: &str,
    ) -> Result<Option<ProbabilityCalibrationCard>> {
        let row = sqlx::query_as::<_, ProbabilityCalibrationRow>(
            r#"
            select mean_error, sample_count
            from probability_calibration
            where symbol = $1
              and market_family = $2
              and strategy_family = $3
              and model_name = $4
              and expiry_bucket = $5
            "#,
        )
        .bind(symbol.to_lowercase())
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlStrategyFamily::from(strategy_family))
        .bind(model_name)
        .bind(expiry_bucket)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn upsert_trained_model_artifact(
        &self,
        model_name: &str,
        market_family: MarketFamily,
        strategy_family: StrategyFamily,
        symbol: Option<&str>,
        feature_version: &str,
        sample_count: i32,
        weights: &market_models::TrainedLinearWeights,
        metrics_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into trained_model_artifact (
                model_name, market_family, strategy_family, symbol, feature_version, sample_count, weights_json, metrics_json, updated_at
            )
            values ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, now())
            on conflict (model_name, strategy_family, market_family, coalesce(symbol, '')) do update
            set feature_version = excluded.feature_version,
                sample_count = excluded.sample_count,
                weights_json = excluded.weights_json,
                metrics_json = excluded.metrics_json,
                updated_at = now()
            "#,
        )
        .bind(model_name)
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlStrategyFamily::from(strategy_family))
        .bind(symbol.map(|value| value.to_lowercase()))
        .bind(feature_version)
        .bind(sample_count)
        .bind(serde_json::to_value(weights)?)
        .bind(metrics_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_trained_model_artifact(
        &self,
        model_name: &str,
        market_family: MarketFamily,
        strategy_family: StrategyFamily,
        symbol: Option<&str>,
    ) -> Result<Option<TrainedModelArtifactCard>> {
        let row = sqlx::query_as::<_, TrainedModelArtifactRow>(
            r#"
            select model_name, market_family, strategy_family, symbol, feature_version, sample_count, weights_json, metrics_json, updated_at
            from trained_model_artifact
            where model_name = $1
              and market_family = $2
              and strategy_family = $3
              and symbol is not distinct from $4
            order by updated_at desc, id desc
            limit 1
            "#,
        )
        .bind(model_name)
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlStrategyFamily::from(strategy_family))
        .bind(symbol.map(|value| value.to_lowercase()))
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    pub async fn insert_opportunity_decision(&self, decision: &OpportunityDecision) -> Result<i64> {
        let id = sqlx::query_scalar(
            r#"
            insert into opportunity_decision (
                market_id, market_family, lane_key, strategy_family, model_name, side, market_prob, model_prob, edge,
                confidence, approved, reasons_json, recommended_size
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13)
            returning id
            "#,
        )
        .bind(decision.market_id)
        .bind(SqlMarketFamily::from(decision.market_family))
        .bind(&decision.lane_key)
        .bind(SqlStrategyFamily::from(decision.strategy_family))
        .bind(&decision.model_name)
        .bind(&decision.side)
        .bind(decision.market_prob)
        .bind(decision.model_prob)
        .bind(decision.edge)
        .bind(decision.confidence)
        .bind(decision.approved)
        .bind(json!(decision.reasons_json))
        .bind(decision.recommended_size)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn upsert_lane_state(&self, lane: &LaneState) -> Result<()> {
        let previous = sqlx::query_as::<_, LaneStateRow>(
            r#"
            select lane_key, market_family, promotion_state, promotion_reason, recent_pnl, recent_brier,
                   recent_execution_quality, recent_replay_expectancy, quarantine_reason, current_champion_model
            from lane_state
            where lane_key = $1
            "#,
        )
        .bind(&lane.lane_key)
        .fetch_optional(&self.pool)
        .await?;

        sqlx::query(
            r#"
            insert into lane_state (
                lane_key, market_family, promotion_state, recent_pnl, recent_brier, recent_execution_quality,
                recent_replay_expectancy, quarantine_reason, promotion_reason, current_champion_model, updated_at
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
            on conflict (lane_key) do update
            set market_family = excluded.market_family,
                promotion_state = excluded.promotion_state,
                recent_pnl = excluded.recent_pnl,
                recent_brier = excluded.recent_brier,
                recent_execution_quality = excluded.recent_execution_quality,
                recent_replay_expectancy = excluded.recent_replay_expectancy,
                quarantine_reason = excluded.quarantine_reason,
                promotion_reason = excluded.promotion_reason,
                current_champion_model = excluded.current_champion_model,
                updated_at = now()
            "#,
        )
        .bind(&lane.lane_key)
        .bind(SqlMarketFamily::from(lane.market_family))
        .bind(SqlPromotionState::from(lane.promotion_state))
        .bind(lane.recent_pnl)
        .bind(lane.recent_brier)
        .bind(lane.recent_execution_quality)
        .bind(lane.recent_replay_expectancy)
        .bind(&lane.quarantine_reason)
        .bind(&lane.promotion_reason)
        .bind(&lane.current_champion_model)
        .execute(&self.pool)
        .await?;
        let changed = previous
            .as_ref()
            .map(|row| {
                row.promotion_state != SqlPromotionState::from(lane.promotion_state)
                    || row.promotion_reason != lane.promotion_reason
                    || row.current_champion_model != lane.current_champion_model
            })
            .unwrap_or(true);
        if changed {
            sqlx::query(
                r#"
                insert into lane_state_transition (
                    lane_key, market_family, from_state, to_state, from_reason, to_reason, current_champion_model, details_json
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                "#,
            )
            .bind(&lane.lane_key)
            .bind(SqlMarketFamily::from(lane.market_family))
            .bind(previous.as_ref().map(|row| row.promotion_state.as_key()))
            .bind(SqlPromotionState::from(lane.promotion_state).as_key())
            .bind(previous.as_ref().and_then(|row| row.promotion_reason.clone()))
            .bind(&lane.promotion_reason)
            .bind(&lane.current_champion_model)
            .bind(json!({
                "recent_pnl": lane.recent_pnl,
                "recent_brier": lane.recent_brier,
                "recent_execution_quality": lane.recent_execution_quality,
                "recent_replay_expectancy": lane.recent_replay_expectancy,
                "quarantine_reason": lane.quarantine_reason,
            }))
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn latest_lane_policy_for_symbol(
        &self,
        market_family: MarketFamily,
        symbol: &str,
        strategy_family: StrategyFamily,
    ) -> Result<Option<SymbolLanePolicy>> {
        let pattern = format!("kalshi:{}:%", symbol.to_lowercase());
        let family = match strategy_family {
            StrategyFamily::DirectionalSettlement => "directional_settlement",
            StrategyFamily::PreSettlementScalp => "pre_settlement_scalp",
            StrategyFamily::Portfolio => "portfolio",
        };
        let row = sqlx::query_as::<_, LaneStateRow>(
            r#"
            select lane_key, market_family, promotion_state, promotion_reason, recent_pnl, recent_brier, recent_execution_quality,
                   recent_replay_expectancy, quarantine_reason, current_champion_model
            from lane_state
            where market_family = $1
              and lane_key like $2
              and lane_key like $3
              and current_champion_model is not null
            order by updated_at desc
            limit 1
            "#,
        )
        .bind(SqlMarketFamily::from(market_family))
        .bind(pattern)
        .bind(format!("%:{}:%", family))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn latest_portfolio_bankroll(&self, mode: TradeMode) -> Result<Option<BankrollCard>> {
        self.latest_family_bankroll(mode, MarketFamily::All).await
    }

    pub async fn latest_family_bankroll(
        &self,
        mode: TradeMode,
        market_family: MarketFamily,
    ) -> Result<Option<BankrollCard>> {
        let scope = match mode {
            TradeMode::Paper => portfolio_scope(TradeMode::Paper, market_family),
            TradeMode::Live => portfolio_scope(TradeMode::Live, market_family),
        };
        let row = sqlx::query_as::<_, BankrollRow>(
            r#"
            select scope, market_family, mode, strategy_family, bankroll, deployable_balance, open_exposure,
                   realized_pnl, unrealized_pnl, created_at
            from bankroll_snapshot
            where scope = $1 and mode = $2 and strategy_family = $3 and market_family = $4
            order by created_at desc, id desc
            limit 1
            "#,
        )
        .bind(scope)
        .bind(SqlTradeMode::from(mode))
        .bind(SqlStrategyFamily::Portfolio)
        .bind(SqlMarketFamily::from(market_family))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn latest_feature_snapshot_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let created_at = sqlx::query_scalar::<_, DateTime<Utc>>(
            r#"
            select created_at
            from market_feature_snapshot
            order by created_at desc
            limit 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(created_at)
    }

    pub async fn upsert_worker_started(
        &self,
        service: &str,
        details_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into worker_status (
                service, status, last_started_at, details_json, updated_at
            )
            values ($1, 'running', now(), $2::jsonb, now())
            on conflict (service) do update
            set status = 'running',
                last_started_at = now(),
                details_json = excluded.details_json,
                updated_at = now()
            "#,
        )
        .bind(service)
        .bind(details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_worker_success(
        &self,
        service: &str,
        details_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into worker_status (
                service, status, last_succeeded_at, last_error, details_json, updated_at
            )
            values ($1, 'ok', now(), null, $2::jsonb, now())
            on conflict (service) do update
            set status = 'ok',
                last_succeeded_at = now(),
                last_error = null,
                details_json = excluded.details_json,
                updated_at = now()
            "#,
        )
        .bind(service)
        .bind(details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_worker_failure(
        &self,
        service: &str,
        error: &str,
        details_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into worker_status (
                service, status, last_failed_at, last_error, details_json, updated_at
            )
            values ($1, 'failed', now(), $2, $3::jsonb, now())
            on conflict (service) do update
            set status = 'failed',
                last_failed_at = now(),
                last_error = excluded.last_error,
                details_json = excluded.details_json,
                updated_at = now()
            "#,
        )
        .bind(service)
        .bind(error)
        .bind(details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_worker_statuses(&self) -> Result<Vec<WorkerStatusCard>> {
        let rows = sqlx::query_as::<_, WorkerStatusRow>(
            r#"
            select
                service,
                status,
                last_started_at,
                last_succeeded_at,
                last_failed_at,
                last_error,
                details_json,
                updated_at
            from worker_status
            order by service asc
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn record_external_bankroll_snapshot(
        &self,
        mode: TradeMode,
        bankroll: f64,
        deployable_balance: f64,
        open_exposure: f64,
    ) -> Result<()> {
        let scope = match mode {
            TradeMode::Paper => portfolio_scope(TradeMode::Paper, MarketFamily::All),
            TradeMode::Live => portfolio_scope(TradeMode::Live, MarketFamily::All),
        };
        sqlx::query(
            r#"
            insert into bankroll_snapshot (
                scope, market_family, mode, strategy_family, bankroll, deployable_balance, open_exposure,
                realized_pnl, unrealized_pnl
            )
            values ($1, $2, $3, $4, $5, $6, $7, 0, 0)
            "#,
        )
        .bind(scope)
        .bind(SqlMarketFamily::All)
        .bind(SqlTradeMode::from(mode))
        .bind(SqlStrategyFamily::Portfolio)
        .bind(bankroll.max(0.0))
        .bind(deployable_balance.max(0.0))
        .bind(open_exposure.max(0.0))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn replace_live_positions(
        &self,
        synced_at: DateTime<Utc>,
        positions: &[LivePositionSyncRecord],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("delete from live_position_sync")
            .execute(&mut *tx)
            .await?;
        for position in positions {
            sqlx::query(
                r#"
                insert into live_position_sync (
                    market_ticker, position_count, resting_order_count, fees_paid,
                    market_exposure, realized_pnl, synced_at, details_json
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                "#,
            )
            .bind(&position.market_ticker)
            .bind(position.position_count)
            .bind(position.resting_order_count)
            .bind(position.fees_paid)
            .bind(position.market_exposure)
            .bind(position.realized_pnl)
            .bind(synced_at)
            .bind(&position.details_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_live_orders(
        &self,
        synced_at: DateTime<Utc>,
        orders: &[LiveOrderSyncRecord],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("delete from live_order_sync")
            .execute(&mut *tx)
            .await?;
        for order in orders {
            sqlx::query(
                r#"
                insert into live_order_sync (
                    order_id, client_order_id, market_ticker, action, side, status, count,
                    fill_count, remaining_count, yes_price, no_price, expiration_time,
                    created_time, synced_at, details_json
                )
                values (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15::jsonb
                )
                "#,
            )
            .bind(&order.order_id)
            .bind(&order.client_order_id)
            .bind(&order.market_ticker)
            .bind(&order.action)
            .bind(&order.side)
            .bind(&order.status)
            .bind(order.count)
            .bind(order.fill_count)
            .bind(order.remaining_count)
            .bind(order.yes_price)
            .bind(order.no_price)
            .bind(order.expiration_time)
            .bind(order.created_time)
            .bind(synced_at)
            .bind(&order.details_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_live_fills(
        &self,
        synced_at: DateTime<Utc>,
        fills: &[LiveFillSyncRecord],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("delete from live_fill_sync")
            .execute(&mut *tx)
            .await?;
        for fill in fills {
            sqlx::query(
                r#"
                insert into live_fill_sync (
                    fill_id, order_id, client_order_id, market_ticker, action, side, count,
                    yes_price, no_price, fee_paid, created_time, synced_at, details_json
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
                "#,
            )
            .bind(&fill.fill_id)
            .bind(&fill.order_id)
            .bind(&fill.client_order_id)
            .bind(&fill.market_ticker)
            .bind(&fill.action)
            .bind(&fill.side)
            .bind(fill.count)
            .bind(fill.yes_price)
            .bind(fill.no_price)
            .bind(fill.fee_paid)
            .bind(fill.created_time)
            .bind(synced_at)
            .bind(&fill.details_json)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_live_exchange_sync_status(
        &self,
        summary: &LiveExchangeSyncSummary,
        details_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into live_exchange_sync_status (
                source, synced_at, positions_count, resting_orders_count, recent_fills_count,
                local_open_live_trades_count, status, issues_json, details_json
            )
            values ('kalshi', $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
            on conflict (source) do update
            set synced_at = excluded.synced_at,
                positions_count = excluded.positions_count,
                resting_orders_count = excluded.resting_orders_count,
                recent_fills_count = excluded.recent_fills_count,
                local_open_live_trades_count = excluded.local_open_live_trades_count,
                status = excluded.status,
                issues_json = excluded.issues_json,
                details_json = excluded.details_json
            "#,
        )
        .bind(summary.synced_at)
        .bind(summary.positions_count as i32)
        .bind(summary.resting_orders_count as i32)
        .bind(summary.recent_fills_count as i32)
        .bind(summary.local_open_live_trades_count as i32)
        .bind(&summary.status)
        .bind(json!(summary.issues))
        .bind(details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_open_live_trades(&self) -> Result<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            select count(*)
            from trade_lifecycle
            where mode = 'live'
              and closed_at is null
              and status not in ('closed', 'cancelled')
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn latest_live_exchange_sync_summary(
        &self,
    ) -> Result<Option<LiveExchangeSyncSummary>> {
        let row = sqlx::query_as::<_, LiveExchangeSyncStatusRow>(
            r#"
            select
                source,
                synced_at,
                positions_count,
                resting_orders_count,
                recent_fills_count,
                local_open_live_trades_count,
                status,
                issues_json,
                details_json
            from live_exchange_sync_status
            where source = 'kalshi'
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn operator_control_state(&self) -> Result<Option<OperatorControlState>> {
        let row = sqlx::query_as::<_, OperatorControlStateRow>(
            r#"
            select live_order_placement_enabled, updated_by, note, updated_at
            from operator_control_state
            where control_key = 'global'
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn set_live_order_placement_enabled(
        &self,
        enabled: bool,
        updated_by: &str,
        note: Option<&str>,
    ) -> Result<OperatorControlState> {
        let row = sqlx::query_as::<_, OperatorControlStateRow>(
            r#"
            insert into operator_control_state (
                control_key,
                live_order_placement_enabled,
                updated_by,
                note,
                updated_at
            )
            values ('global', $1, $2, $3, now())
            on conflict (control_key) do update
            set live_order_placement_enabled = excluded.live_order_placement_enabled,
                updated_by = excluded.updated_by,
                note = excluded.note,
                updated_at = now()
            returning live_order_placement_enabled, updated_by, note, updated_at
            "#,
        )
        .bind(enabled)
        .bind(updated_by)
        .bind(note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into())
    }

    pub async fn record_operator_action_event(
        &self,
        action: &str,
        actor: &str,
        note: Option<&str>,
        payload_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into operator_action_event (
                action,
                actor,
                note,
                payload_json
            )
            values ($1, $2, $3, $4::jsonb)
            "#,
        )
        .bind(action)
        .bind(actor)
        .bind(note)
        .bind(payload_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_recent_operator_action_events(
        &self,
        limit: i64,
    ) -> Result<Vec<OperatorActionEvent>> {
        let rows = sqlx::query_as::<_, OperatorActionEventRow>(
            r#"
            select id, action, actor, note, payload_json, created_at
            from operator_action_event
            order by created_at desc, id desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn notification_delivery_exists(
        &self,
        notification_kind: &str,
        fingerprint: &str,
    ) -> Result<bool> {
        let delivered = sqlx::query_scalar::<_, bool>(
            r#"
            select exists(
                select 1
                from notification_delivery
                where notification_kind = $1
                  and fingerprint = $2
            )
            "#,
        )
        .bind(notification_kind)
        .bind(fingerprint)
        .fetch_one(&self.pool)
        .await?;
        Ok(delivered)
    }

    pub async fn record_notification_delivery(
        &self,
        notification_kind: &str,
        fingerprint: &str,
        payload_json: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into notification_delivery (
                notification_kind,
                fingerprint,
                payload_json
            )
            values ($1, $2, $3::jsonb)
            on conflict (notification_kind, fingerprint) do nothing
            "#,
        )
        .bind(notification_kind)
        .bind(fingerprint)
        .bind(payload_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn effective_live_order_placement_enabled(
        &self,
        config_enabled: bool,
    ) -> Result<bool> {
        let operator_enabled = self
            .operator_control_state()
            .await?
            .map(|state| state.live_order_placement_enabled)
            .unwrap_or(true);
        Ok(config_enabled && operator_enabled)
    }

    pub async fn live_exception_snapshot(&self) -> Result<LiveExceptionSnapshot> {
        Ok(LiveExceptionSnapshot {
            operator_control: self.operator_control_state().await?,
            positions: self.list_live_positions(6).await?,
            orders: self.list_live_orders(8).await?,
            recent_fills: self.list_recent_live_fills(8).await?,
            trade_exceptions: self.list_live_trade_exceptions(6).await?,
            live_intents: self.list_live_intents(8).await?,
        })
    }

    pub async fn list_live_positions(&self, limit: i64) -> Result<Vec<LivePositionCard>> {
        let rows = sqlx::query_as::<_, LivePositionSyncRow>(
            r#"
            select market_ticker, position_count, resting_order_count, market_exposure,
                   realized_pnl, synced_at
            from live_position_sync
            order by abs(market_exposure) desc, synced_at desc, market_ticker asc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_live_orders(&self, limit: i64) -> Result<Vec<LiveOrderCard>> {
        let rows = sqlx::query_as::<_, LiveOrderSyncListRow>(
            r#"
            select order_id, client_order_id, market_ticker, action, side, status, count,
                   fill_count, remaining_count, created_time, synced_at
            from live_order_sync
            order by created_time desc nulls last, synced_at desc, order_id desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_resting_live_orders(&self, limit: i64) -> Result<Vec<LiveOrderCard>> {
        let rows = sqlx::query_as::<_, LiveOrderSyncListRow>(
            r#"
            select order_id, client_order_id, market_ticker, action, side, status, count,
                   fill_count, remaining_count, created_time, synced_at
            from live_order_sync
            where coalesce(status, 'resting') not in ('cancelled', 'filled', 'rejected')
            order by created_time desc nulls last, synced_at desc, order_id desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_recent_live_fills(&self, limit: i64) -> Result<Vec<LiveFillCard>> {
        let rows = sqlx::query_as::<_, LiveFillSyncListRow>(
            r#"
            select fill_id, order_id, client_order_id, market_ticker, action, side, count,
                   fee_paid, created_time, synced_at
            from live_fill_sync
            order by created_time desc nulls last, synced_at desc, fill_id desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_live_intents(&self, limit: i64) -> Result<Vec<LiveIntentCard>> {
        let rows = sqlx::query_as::<_, LiveIntentRow>(
            r#"
            select
                ei.id as intent_id,
                od.lane_key,
                ei.mode,
                ei.status,
                ei.last_error,
                ei.market_ticker,
                ei.side,
                ei.client_order_id,
                ei.exchange_order_id,
                ei.fill_status,
                ei.created_at,
                ei.last_transition_at
            from execution_intent ei
            join opportunity_decision od on od.id = ei.decision_id
            where ei.mode = 'live'
            order by ei.last_transition_at desc, ei.id desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_live_trade_exceptions(
        &self,
        limit: usize,
    ) -> Result<Vec<LiveTradeExceptionCard>> {
        let open_trades = self.list_open_live_trades_for_reconciliation().await?;
        let mut out = Vec::new();
        for trade in open_trades {
            let snapshot = self.live_reconciliation_snapshot_for_trade(&trade).await?;
            if live_trade_needs_attention(&trade, &snapshot) {
                out.push(LiveTradeExceptionCard {
                    trade_id: trade.trade_id,
                    lane_key: trade.lane_key.clone(),
                    market_ticker: trade.market_ticker.clone(),
                    issue: "exchange truth mismatch".to_string(),
                    has_position: snapshot.has_position,
                    has_resting_order: snapshot.has_resting_order,
                    matched_exit_fill_quantity: snapshot.matched_exit_fill_quantity,
                    created_at: trade.created_at,
                });
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub async fn lane_inspection_snapshot(&self, lane_key: &str) -> Result<LaneInspectionSnapshot> {
        let lane_state: Option<LaneState> = sqlx::query_as::<_, LaneStateRow>(
            r#"
            select lane_key, market_family, promotion_state, promotion_reason, recent_pnl, recent_brier, recent_execution_quality,
                   recent_replay_expectancy, quarantine_reason, current_champion_model
            from lane_state
            where lane_key = $1
            "#,
        )
        .bind(lane_key)
        .fetch_optional(&self.pool)
        .await?
        .map(Into::into);

        let opportunity_rows = sqlx::query(
            r#"
            select
                od.id,
                od.lane_key,
                od.market_family,
                od.strategy_family,
                od.side,
                od.market_prob,
                od.model_prob,
                od.edge,
                od.confidence,
                od.approved,
                od.reasons_json,
                od.created_at,
                latest_intent.status as execution_status,
                case
                    when overlapping_trade.id is not null then 'existing_open_trade_for_lane'
                    else latest_intent.last_error
                end as execution_note
            from opportunity_decision od
            left join lateral (
                select id
                from trade_lifecycle tl
                where tl.lane_key = od.lane_key
                  and tl.created_at <= od.created_at
                  and coalesce(tl.closed_at, od.created_at + interval '100 years') >= od.created_at
                  and tl.status not in ('closed', 'cancelled')
                order by tl.created_at desc, tl.id desc
                limit 1
            ) overlapping_trade on true
            left join lateral (
                select status, last_error
                from execution_intent ei
                where ei.decision_id = od.id
                order by ei.created_at desc, ei.id desc
                limit 1
            ) latest_intent on true
            where od.lane_key = $1
            order by od.created_at desc, od.id desc
            limit 8
            "#,
        )
        .bind(lane_key)
        .fetch_all(&self.pool)
        .await?;
        let mut recent_opportunities = Vec::with_capacity(opportunity_rows.len());
        for row in opportunity_rows {
            let strategy_family =
                parse_strategy_family(row.try_get::<String, _>("strategy_family")?.as_str());
            let reasons_json: serde_json::Value = row.try_get("reasons_json")?;
            recent_opportunities.push(OpportunityCard {
                lane_key: row.try_get("lane_key")?,
                market_family: parse_market_family(&row.try_get::<String, _>("market_family")?),
                strategy_family,
                side: row.try_get("side")?,
                market_prob: row.try_get("market_prob")?,
                model_prob: row.try_get("model_prob")?,
                edge: row.try_get("edge")?,
                confidence: row.try_get("confidence")?,
                approved: row.try_get("approved")?,
                reasons: reasons_json
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                execution_status: row.try_get("execution_status")?,
                execution_note: row.try_get("execution_note")?,
                as_of: row.try_get("created_at")?,
            });
        }

        let trade_rows = sqlx::query_as::<_, LaneTradeRow>(
            r#"
            select id, status, mode, quantity, entry_price, exit_price, realized_pnl,
                   created_at, closed_at
            from trade_lifecycle
            where lane_key = $1
            order by created_at desc, id desc
            limit 8
            "#,
        )
        .bind(lane_key)
        .fetch_all(&self.pool)
        .await?;
        let replay_summary = self
            .latest_model_benchmark_summary_for_lane(lane_key)
            .await?;

        Ok(LaneInspectionSnapshot {
            lane_key: lane_key.to_string(),
            market_family: match &lane_state {
                Some(lane) => lane.market_family,
                None => MarketFamily::Crypto,
            },
            lane_state,
            recent_opportunities,
            recent_trades: trade_rows.into_iter().map(Into::into).collect(),
            replay_summary,
        })
    }

    pub async fn recent_trade_metrics_for_symbol(
        &self,
        symbol: &str,
        strategy_family: StrategyFamily,
        mode: TradeMode,
        limit: i64,
    ) -> Result<RecentTradeMetrics> {
        let family = match strategy_family {
            StrategyFamily::DirectionalSettlement => "directional_settlement",
            StrategyFamily::PreSettlementScalp => "pre_settlement_scalp",
            StrategyFamily::Portfolio => "portfolio",
        };
        let symbol_pattern = format!("kalshi:{}:%", symbol.to_lowercase());
        let family_pattern = format!("%:{}:%", family);
        let row = sqlx::query_as::<_, RecentTradeMetricsRow>(
            r#"
            with recent as (
                select realized_pnl
                from trade_lifecycle
                where lane_key like $1
                  and lane_key like $2
                  and mode = $3
                  and closed_at is not null
                order by closed_at desc, id desc
                limit $4
            )
            select
                count(*) as trade_count,
                coalesce(sum(case when coalesce(realized_pnl, 0) > 0 then 1 else 0 end), 0) as win_count,
                coalesce(sum(coalesce(realized_pnl, 0)), 0) as realized_pnl
            from recent
            "#,
        )
        .bind(symbol_pattern)
        .bind(family_pattern)
        .bind(SqlTradeMode::from(mode))
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into())
    }

    pub async fn recent_trade_metrics_for_lane(
        &self,
        lane_key: &str,
        mode: TradeMode,
        limit: i64,
    ) -> Result<RecentTradeMetrics> {
        let row = sqlx::query_as::<_, RecentTradeMetricsRow>(
            r#"
            with recent as (
                select realized_pnl
                from trade_lifecycle
                where lane_key = $1
                  and mode = $2
                  and closed_at is not null
                order by closed_at desc, id desc
                limit $3
            )
            select
                count(*) as trade_count,
                coalesce(sum(case when coalesce(realized_pnl, 0) > 0 then 1 else 0 end), 0) as win_count,
                coalesce(sum(coalesce(realized_pnl, 0)), 0) as realized_pnl
            from recent
            "#,
        )
        .bind(lane_key)
        .bind(SqlTradeMode::from(mode))
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into())
    }

    pub async fn has_open_trade_for_lane(&self, lane_key: &str) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            select exists(
                select 1
                from trade_lifecycle
                where lane_key = $1
                  and closed_at is null
                  and status not in ('closed', 'cancelled')
            )
            "#,
        )
        .bind(lane_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub async fn has_active_execution_intent_for_lane(&self, lane_key: &str) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            select exists(
                select 1
                from execution_intent ei
                join opportunity_decision od on od.id = ei.decision_id
                where od.lane_key = $1
                  and ei.status in ('pending', 'submitted', 'acknowledged', 'partially_filled', 'cancel_pending')
            )
            "#,
        )
        .bind(lane_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub async fn open_exposure_for_mode_and_lane(
        &self,
        mode: TradeMode,
        lane_key: &str,
    ) -> Result<f64> {
        let exposure = sqlx::query_scalar::<_, f64>(
            r#"
            select coalesce(sum(quantity * entry_price), 0)
            from trade_lifecycle
            where mode = $1
              and lane_key = $2
              and closed_at is null
              and status not in ('closed', 'cancelled')
            "#,
        )
        .bind(SqlTradeMode::from(mode))
        .bind(lane_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(exposure.max(0.0))
    }

    pub async fn open_exposure_for_mode_and_symbol(
        &self,
        mode: TradeMode,
        symbol: &str,
    ) -> Result<f64> {
        let exposure = sqlx::query_scalar::<_, f64>(
            r#"
            select coalesce(sum(quantity * entry_price), 0)
            from trade_lifecycle
            where mode = $1
              and split_part(lane_key, ':', 2) = $2
              and closed_at is null
              and status not in ('closed', 'cancelled')
            "#,
        )
        .bind(SqlTradeMode::from(mode))
        .bind(symbol.to_lowercase())
        .fetch_one(&self.pool)
        .await?;
        Ok(exposure.max(0.0))
    }

    pub async fn insert_execution_intent(
        &self,
        decision_id: i64,
        market_family: MarketFamily,
        mode: TradeMode,
        entry_style: &str,
        target_ladder_json: &[f64],
        timeout_seconds: i32,
        force_exit_buffer_seconds: i32,
        stop_conditions_json: &[String],
        predicted_fill_probability: Option<f64>,
        predicted_slippage_bps: Option<f64>,
        predicted_queue_ahead: Option<f64>,
        execution_forecast_version: Option<&str>,
        submitted_quantity: f64,
        promotion_state_at_creation: Option<PromotionState>,
    ) -> Result<i64> {
        let id = sqlx::query_scalar(
            r#"
            insert into execution_intent (
                decision_id, market_family, mode, entry_style, target_ladder_json, timeout_seconds,
                force_exit_buffer_seconds, stop_conditions_json, status,
                predicted_fill_probability, predicted_slippage_bps, predicted_queue_ahead,
                execution_forecast_version, submitted_quantity, promotion_state_at_creation
            )
            values ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::jsonb, 'pending', $9, $10, $11, $12, $13, $14)
            returning id
            "#,
        )
        .bind(decision_id)
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlTradeMode::from(mode))
        .bind(entry_style)
        .bind(json!(target_ladder_json))
        .bind(timeout_seconds)
        .bind(force_exit_buffer_seconds)
        .bind(json!(stop_conditions_json))
        .bind(predicted_fill_probability)
        .bind(predicted_slippage_bps)
        .bind(predicted_queue_ahead)
        .bind(execution_forecast_version)
        .bind(submitted_quantity.max(0.0))
        .bind(
            promotion_state_at_creation.map(|value| SqlPromotionState::from(value).as_key().to_string()),
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_pending_execution_intents(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingExecutionIntent>> {
        let rows = sqlx::query_as::<_, PendingExecutionIntentRow>(
            r#"
            select
                ei.id as intent_id,
                ei.decision_id,
                od.market_id,
                od.market_family,
                od.lane_key,
                od.strategy_family,
                od.side,
                od.market_prob,
                od.model_prob,
                od.confidence,
                od.recommended_size,
                ei.mode,
                ei.entry_style,
                ei.timeout_seconds,
                ei.force_exit_buffer_seconds,
                ei.predicted_fill_probability,
                ei.predicted_slippage_bps,
                ei.predicted_queue_ahead,
                ei.execution_forecast_version,
                ei.submitted_quantity,
                ei.accepted_quantity,
                ei.filled_quantity,
                ei.cancelled_quantity,
                ei.rejected_quantity,
                ei.avg_fill_price,
                ei.first_fill_at,
                ei.last_fill_at,
                ei.terminal_outcome,
                ei.promotion_state_at_creation,
                ei.stop_conditions_json,
                ei.created_at
            from execution_intent ei
            join opportunity_decision od on od.id = ei.decision_id
            left join trade_lifecycle tl on tl.decision_id = ei.decision_id
            where tl.id is null
              and ei.status = 'pending'
            order by ei.created_at asc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn execution_intent_by_id(
        &self,
        intent_id: i64,
    ) -> Result<Option<PendingExecutionIntent>> {
        let row = sqlx::query_as::<_, PendingExecutionIntentRow>(
            r#"
            select
                ei.id as intent_id,
                ei.decision_id,
                od.market_id,
                od.market_family,
                od.lane_key,
                od.strategy_family,
                od.side,
                od.market_prob,
                od.model_prob,
                od.confidence,
                od.recommended_size,
                ei.mode,
                ei.entry_style,
                ei.timeout_seconds,
                ei.force_exit_buffer_seconds,
                ei.predicted_fill_probability,
                ei.predicted_slippage_bps,
                ei.predicted_queue_ahead,
                ei.execution_forecast_version,
                ei.submitted_quantity,
                ei.accepted_quantity,
                ei.filled_quantity,
                ei.cancelled_quantity,
                ei.rejected_quantity,
                ei.avg_fill_price,
                ei.first_fill_at,
                ei.last_fill_at,
                ei.terminal_outcome,
                ei.promotion_state_at_creation,
                ei.stop_conditions_json,
                ei.created_at
            from execution_intent ei
            join opportunity_decision od on od.id = ei.decision_id
            where ei.id = $1
            "#,
        )
        .bind(intent_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_active_live_execution_intents(
        &self,
        limit: i64,
    ) -> Result<Vec<ActiveLiveExecutionIntent>> {
        let rows = sqlx::query_as::<_, ActiveLiveExecutionIntentRow>(
            r#"
            select
                ei.id as intent_id,
                ei.decision_id,
                od.market_id,
                od.market_family,
                od.lane_key,
                od.strategy_family,
                od.side,
                od.market_prob,
                od.recommended_size,
                ei.mode,
                ei.timeout_seconds,
                ei.force_exit_buffer_seconds,
                ei.created_at,
                ei.status,
                ei.market_ticker,
                ei.client_order_id,
                ei.exchange_order_id,
                ei.fill_status,
                ei.submitted_quantity,
                ei.accepted_quantity,
                ei.filled_quantity,
                ei.cancelled_quantity,
                ei.rejected_quantity,
                ei.avg_fill_price,
                ei.terminal_outcome,
                ei.last_transition_at
            from execution_intent ei
            join opportunity_decision od on od.id = ei.decision_id
            where ei.mode = 'live'
              and ei.status in ('pending', 'submitted', 'acknowledged', 'partially_filled', 'cancel_pending')
            order by ei.last_transition_at asc, ei.id asc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn repair_pending_execution_intents_from_trades(&self) -> Result<usize> {
        let result = sqlx::query(
            r#"
            update execution_intent ei
            set status = 'opened',
                last_error = null,
                processed_at = coalesce(ei.processed_at, now())
            where ei.status = 'pending'
              and exists (
                  select 1
                  from trade_lifecycle tl
                  where tl.decision_id = ei.decision_id
              )
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn open_trade_from_intent(
        &self,
        intent: &PendingExecutionIntent,
        quantity: f64,
        entry_price: f64,
    ) -> Result<i64> {
        self.open_trade_from_intent_with_metadata_and_status(
            intent,
            quantity,
            entry_price,
            &json!({}),
            "opened",
        )
        .await
    }

    pub async fn open_trade_from_intent_with_metadata(
        &self,
        intent: &PendingExecutionIntent,
        quantity: f64,
        entry_price: f64,
        extra_metadata: &serde_json::Value,
    ) -> Result<i64> {
        self.open_trade_from_intent_with_metadata_and_status(
            intent,
            quantity,
            entry_price,
            extra_metadata,
            "opened",
        )
        .await
    }

    pub async fn open_trade_from_intent_with_metadata_and_status(
        &self,
        intent: &PendingExecutionIntent,
        quantity: f64,
        entry_price: f64,
        extra_metadata: &serde_json::Value,
        status_on_open: &str,
    ) -> Result<i64> {
        let snapshot = self
            .latest_feature_snapshot_for_market(intent.market_id)
            .await?;
        let expires_at = snapshot.as_ref().map(|snapshot| {
            snapshot.created_at + chrono::Duration::seconds(snapshot.seconds_to_expiry as i64)
        });
        let market_ticker = extra_metadata
            .get("market_ticker")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.market_ticker.clone())
            });
        let metadata = json!({
            "market_id": intent.market_id,
            "side": intent.side,
            "intent_id": intent.intent_id,
            "timeout_seconds": intent.timeout_seconds,
            "force_exit_buffer_seconds": intent.force_exit_buffer_seconds,
            "market_ticker": market_ticker,
            "expires_at": expires_at,
        });
        let entry_fee = json_f64(extra_metadata, "entry_fee");
        let entry_exchange_order_id = extra_metadata
            .get("entry_order_id")
            .and_then(serde_json::Value::as_str);
        let entry_client_order_id = extra_metadata
            .get("entry_client_order_id")
            .and_then(serde_json::Value::as_str);
        let entry_fill_status = extra_metadata
            .get("entry_status")
            .and_then(serde_json::Value::as_str);
        let id = sqlx::query_scalar(
            r#"
            insert into trade_lifecycle (
                decision_id, market_family, lane_key, strategy_family, mode, status, quantity, entry_price,
                market_id, market_ticker, side, timeout_seconds, force_exit_buffer_seconds,
                expires_at, entry_fee, entry_exchange_order_id, entry_client_order_id,
                entry_fill_status, metadata_json
            )
            values (
                $1, $2, $3, $4, $5, 'open', $6, $7,
                $8, $9, $10, $11, $12,
                $13, $14, $15, $16,
                $17, $18::jsonb
            )
            returning id
            "#,
        )
        .bind(intent.decision_id)
        .bind(SqlMarketFamily::from(intent.market_family))
        .bind(&intent.lane_key)
        .bind(SqlStrategyFamily::from(intent.strategy_family))
        .bind(SqlTradeMode::from(intent.mode))
        .bind(quantity)
        .bind(entry_price)
        .bind(intent.market_id)
        .bind(market_ticker.as_deref())
        .bind(&intent.side)
        .bind(intent.timeout_seconds)
        .bind(intent.force_exit_buffer_seconds)
        .bind(expires_at)
        .bind(entry_fee)
        .bind(entry_exchange_order_id)
        .bind(entry_client_order_id)
        .bind(entry_fill_status)
        .bind(merge_json(metadata, extra_metadata.clone()))
        .fetch_one(&self.pool)
        .await?;
        self.transition_execution_intent(
            intent.intent_id,
            status_on_open,
            None,
            &ExecutionIntentStateUpdate {
                market_ticker: market_ticker.clone(),
                side: Some(intent.side.clone()),
                accepted_quantity: Some(quantity),
                filled_quantity: Some(quantity),
                avg_fill_price: Some(entry_price),
                first_fill_at: Some(Utc::now()),
                last_fill_at: Some(Utc::now()),
                terminal_outcome: Some(if intent.mode == TradeMode::Paper {
                    "paper_filled".to_string()
                } else if status_on_open == "partially_filled" {
                    "partially_filled".to_string()
                } else {
                    "filled".to_string()
                }),
                details_json: json!({
                    "trade_id": id,
                    "status_on_open": status_on_open,
                }),
                ..Default::default()
            },
        )
        .await?;
        if intent.mode == TradeMode::Paper {
            self.reconcile_bankroll_for_mode(intent.mode).await?;
        }
        Ok(id)
    }

    pub async fn upsert_open_live_trade_fill_progress(
        &self,
        decision_id: i64,
        quantity: f64,
        entry_price: f64,
        entry_fee: f64,
        entry_fill_status: &str,
        extra_metadata: &serde_json::Value,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            update trade_lifecycle
            set quantity = $2,
                entry_price = $3,
                entry_fee = $4,
                entry_fill_status = $5,
                metadata_json = coalesce(metadata_json, '{}'::jsonb) || $6::jsonb
            where decision_id = $1
              and mode = 'live'
              and closed_at is null
              and status = 'open'
            "#,
        )
        .bind(decision_id)
        .bind(quantity)
        .bind(entry_price)
        .bind(entry_fee)
        .bind(entry_fill_status)
        .bind(extra_metadata)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_execution_intent_status(
        &self,
        intent_id: i64,
        status: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.transition_execution_intent(
            intent_id,
            status,
            last_error,
            &ExecutionIntentStateUpdate::default(),
        )
        .await
    }

    pub async fn transition_execution_intent(
        &self,
        intent_id: i64,
        status: &str,
        last_error: Option<&str>,
        update: &ExecutionIntentStateUpdate,
    ) -> Result<()> {
        let previous_status = sqlx::query_scalar::<_, String>(
            r#"
            select status
            from execution_intent
            where id = $1
            "#,
        )
        .bind(intent_id)
        .fetch_optional(&self.pool)
        .await?;

        sqlx::query(
            r#"
            update execution_intent
            set status = $2,
                last_error = $3,
                market_ticker = coalesce($4, market_ticker),
                side = coalesce($5, side),
                client_order_id = coalesce($6, client_order_id),
                exchange_order_id = coalesce($7, exchange_order_id),
                fill_status = coalesce($8, fill_status),
                accepted_quantity = coalesce($9, accepted_quantity),
                filled_quantity = coalesce($10, filled_quantity),
                cancelled_quantity = coalesce($11, cancelled_quantity),
                rejected_quantity = coalesce($12, rejected_quantity),
                avg_fill_price = coalesce($13, avg_fill_price),
                first_fill_at = coalesce($14, first_fill_at),
                last_fill_at = coalesce($15, last_fill_at),
                terminal_outcome = coalesce($16, terminal_outcome),
                submitted_at = case when $2 = 'submitted' and submitted_at is null then now() else submitted_at end,
                acknowledged_at = case when $2 = 'acknowledged' and acknowledged_at is null then now() else acknowledged_at end,
                cancelled_at = case when $2 in ('cancelled', 'cancel_pending') and cancelled_at is null then now() else cancelled_at end,
                processed_at = case when $2 in ('opened', 'filled', 'partially_filled', 'cancelled', 'rejected', 'expired', 'superseded', 'orphaned') then now() else processed_at end,
                last_transition_at = now()
            where id = $1
            "#,
        )
        .bind(intent_id)
        .bind(status)
        .bind(last_error)
        .bind(&update.market_ticker)
        .bind(&update.side)
        .bind(&update.client_order_id)
        .bind(&update.exchange_order_id)
        .bind(&update.fill_status)
        .bind(update.accepted_quantity)
        .bind(update.filled_quantity)
        .bind(update.cancelled_quantity)
        .bind(update.rejected_quantity)
        .bind(update.avg_fill_price)
        .bind(update.first_fill_at)
        .bind(update.last_fill_at)
        .bind(&update.terminal_outcome)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            insert into execution_intent_transition (
                intent_id,
                from_status,
                to_status,
                last_error,
                details_json
            )
            values ($1, $2, $3, $4, $5::jsonb)
            "#,
        )
        .bind(intent_id)
        .bind(previous_status)
        .bind(status)
        .bind(last_error)
        .bind(&update.details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pending_execution_intent_status_counts(&self) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            select status, count(*) as count
            from execution_intent
            group by status
            order by status asc
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.try_get::<String, _>("status").unwrap_or_default(),
                    row.try_get::<i64, _>("count").unwrap_or_default(),
                )
            })
            .collect())
    }

    pub async fn list_open_trades_for_exit(&self) -> Result<Vec<OpenTradeForExit>> {
        let rows = sqlx::query_as::<_, OpenTradeForExitRow>(
            r#"
            select
                id,
                decision_id,
                market_family,
                lane_key,
                strategy_family,
                mode,
                quantity,
                entry_price,
                coalesce(entry_fee, (metadata_json->>'entry_fee')::double precision, 0) as entry_fee,
                coalesce(market_id, (metadata_json->>'market_id')::bigint) as market_id,
                market_ticker,
                coalesce(side, metadata_json->>'side') as side,
                created_at,
                coalesce(timeout_seconds, (metadata_json->>'timeout_seconds')::integer, 0) as timeout_seconds,
                coalesce(force_exit_buffer_seconds, (metadata_json->>'force_exit_buffer_seconds')::integer, 45) as force_exit_buffer_seconds,
                coalesce(expires_at, (metadata_json->>'expires_at')::timestamptz) as expires_at,
                exit_exchange_order_id,
                exit_client_order_id,
                exit_fill_status
            from trade_lifecycle
            where closed_at is null and status = 'open'
            order by created_at asc
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_open_live_trades_for_reconciliation(
        &self,
    ) -> Result<Vec<OpenLiveTradeForReconciliation>> {
        let rows = sqlx::query_as::<_, OpenLiveTradeForReconciliationRow>(
            r#"
            select
                id,
                market_family,
                lane_key,
                market_ticker,
                side,
                quantity,
                entry_price,
                entry_fee,
                created_at,
                entry_exchange_order_id,
                entry_client_order_id,
                exit_exchange_order_id,
                exit_client_order_id
            from trade_lifecycle
            where mode = 'live'
              and closed_at is null
              and status = 'open'
              and market_ticker is not null
              and side is not null
            order by created_at asc
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn live_reconciliation_snapshot_for_trade(
        &self,
        trade: &OpenLiveTradeForReconciliation,
    ) -> Result<LiveTradeReconciliationSnapshot> {
        let contract_side = if trade.side == "buy_no" { "no" } else { "yes" };

        let has_position = sqlx::query_scalar::<_, bool>(
            r#"
            select exists(
                select 1
                from live_position_sync
                where market_ticker = $1
                  and abs(position_count) >= 0.999
            )
            "#,
        )
        .bind(&trade.market_ticker)
        .fetch_one(&self.pool)
        .await?;

        let has_resting_order = sqlx::query_scalar::<_, bool>(
            r#"
            select exists(
                select 1
                from live_order_sync
                where market_ticker = $1
                  and (
                    ($2 is not null and order_id = $2)
                    or ($3 is not null and client_order_id = $3)
                    or ($4 is not null and order_id = $4)
                    or ($5 is not null and client_order_id = $5)
                  )
            )
            "#,
        )
        .bind(&trade.market_ticker)
        .bind(&trade.entry_exchange_order_id)
        .bind(&trade.entry_client_order_id)
        .bind(&trade.exit_exchange_order_id)
        .bind(&trade.exit_client_order_id)
        .fetch_one(&self.pool)
        .await?;

        let exit_fill = sqlx::query_as::<_, LiveExitFillAggregateRow>(
            r#"
            select
                coalesce(sum(count), 0) as fill_quantity,
                case when coalesce(sum(count), 0) > 0
                    then sum(count * case
                        when side = 'yes' then yes_price
                        else no_price
                    end) / sum(count)
                    else null end as avg_fill_price,
                coalesce(sum(fee_paid), 0) as total_fee,
                max(created_time) as latest_fill_time
            from live_fill_sync
            where market_ticker = $1
              and action = 'sell'
              and side = $2
              and created_time >= $3
              and (
                ($4 is not null and order_id = $4)
                or ($5 is not null and client_order_id = $5)
                or (
                    $4 is null and $5 is null
                    and $6 is null and $7 is null
                )
              )
            "#,
        )
        .bind(&trade.market_ticker)
        .bind(contract_side)
        .bind(trade.created_at)
        .bind(&trade.exit_exchange_order_id)
        .bind(&trade.exit_client_order_id)
        .bind(&trade.entry_exchange_order_id)
        .bind(&trade.entry_client_order_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(LiveTradeReconciliationSnapshot {
            has_position,
            has_resting_order,
            matched_exit_fill_quantity: exit_fill.fill_quantity,
            matched_exit_fill_price: exit_fill.avg_fill_price,
            matched_exit_fill_fee: exit_fill.total_fee,
            matched_exit_fill_time: exit_fill.latest_fill_time,
        })
    }

    pub async fn reconcile_live_trades_from_sync(&self) -> Result<(usize, usize)> {
        let open_live_trades = self.list_open_live_trades_for_reconciliation().await?;
        let mut reconciled = 0usize;
        let mut attention = 0usize;

        for trade in open_live_trades {
            let snapshot = self.live_reconciliation_snapshot_for_trade(&trade).await?;
            let exit_progress = self.trade_exit_progress(trade.trade_id).await?;
            let new_exit_quantity =
                (snapshot.matched_exit_fill_quantity - exit_progress.closed_quantity).max(0.0);
            let new_exit_fee = (snapshot.matched_exit_fill_fee - exit_progress.exit_fee).max(0.0);
            if new_exit_quantity >= 0.99 && snapshot.matched_exit_fill_price.is_some() {
                let delta_quantity = new_exit_quantity.min(trade.quantity);
                let exit_price = snapshot
                    .matched_exit_fill_price
                    .unwrap_or(trade.entry_price);
                let realized_pnl = ((exit_price - trade.entry_price) * delta_quantity)
                    - proportional_entry_fee(trade.entry_fee, delta_quantity, trade.quantity)
                    - new_exit_fee;
                let metadata = json!({
                    "exit_fee": new_exit_fee,
                    "exit_status": "exchange_reconciled_fill",
                    "exchange_reconciled_at": chrono::Utc::now(),
                    "exchange_exit_fill_time": snapshot.matched_exit_fill_time,
                    "exchange_exit_fill_quantity": snapshot.matched_exit_fill_quantity,
                    "exchange_exit_delta_quantity": delta_quantity,
                });
                if delta_quantity + 0.01 >= trade.quantity && !snapshot.has_position {
                    self.close_trade_with_metadata(
                        trade.trade_id,
                        exit_price,
                        realized_pnl,
                        "exchange_reconciled_exit",
                        &metadata,
                    )
                    .await?;
                } else {
                    self.partially_close_trade_with_metadata(
                        trade.trade_id,
                        delta_quantity,
                        exit_price,
                        realized_pnl,
                        "partial_exit",
                        &metadata,
                    )
                    .await?;
                }
                reconciled += 1;
                continue;
            }

            if live_trade_needs_attention(&trade, &snapshot) {
                attention += 1;
            }
        }

        Ok((reconciled, attention))
    }

    pub async fn trade_exit_progress(&self, trade_id: i64) -> Result<TradeExitProgress> {
        let row = sqlx::query_as::<_, TradeExitProgressRow>(
            r#"
            select
                coalesce(sum(quantity), 0) as closed_quantity,
                coalesce(sum(coalesce(exit_fee, 0)), 0) as exit_fee
            from trade_lifecycle
            where parent_trade_id = $1
              and closed_at is not null
            "#,
        )
        .bind(trade_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into())
    }

    pub async fn cancel_live_execution_intents(&self, reason: &str) -> Result<usize> {
        let intent_ids = sqlx::query_scalar::<_, i64>(
            r#"
            select id
            from execution_intent
            where mode = 'live'
              and status in ('pending', 'submitted', 'acknowledged', 'cancel_pending', 'partially_filled')
            order by created_at asc
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        for intent_id in &intent_ids {
            self.transition_execution_intent(
                *intent_id,
                "cancelled",
                Some(reason),
                &ExecutionIntentStateUpdate {
                    details_json: json!({"operator_action": "cancel_pending_live_orders"}),
                    ..Default::default()
                },
            )
            .await?;
        }

        Ok(intent_ids.len())
    }

    pub async fn close_trade(
        &self,
        trade_id: i64,
        exit_price: f64,
        realized_pnl: f64,
        status: &str,
    ) -> Result<()> {
        self.close_trade_with_metadata(trade_id, exit_price, realized_pnl, status, &json!({}))
            .await
    }

    pub async fn close_trade_with_metadata(
        &self,
        trade_id: i64,
        exit_price: f64,
        realized_pnl: f64,
        status: &str,
        extra_metadata: &serde_json::Value,
    ) -> Result<()> {
        let trade = sqlx::query_as::<_, CloseTradeRow>(
            r#"
            select lane_key, mode
            from trade_lifecycle
            where id = $1
            "#,
        )
        .bind(trade_id)
        .fetch_one(&self.pool)
        .await?;
        let exit_fee = json_f64(extra_metadata, "exit_fee");
        let exit_exchange_order_id = extra_metadata
            .get("exit_order_id")
            .and_then(serde_json::Value::as_str);
        let exit_client_order_id = extra_metadata
            .get("exit_client_order_id")
            .and_then(serde_json::Value::as_str);
        let exit_fill_status = extra_metadata
            .get("exit_status")
            .and_then(serde_json::Value::as_str);
        sqlx::query(
            r#"
            update trade_lifecycle
            set exit_price = $2,
                realized_pnl = $3,
                status = $4,
                exit_fee = coalesce($5, exit_fee, 0),
                exit_exchange_order_id = coalesce($6, exit_exchange_order_id),
                exit_client_order_id = coalesce($7, exit_client_order_id),
                exit_fill_status = coalesce($8, exit_fill_status),
                metadata_json = coalesce(metadata_json, '{}'::jsonb) || $9::jsonb,
                closed_at = now()
            where id = $1
            "#,
        )
        .bind(trade_id)
        .bind(exit_price)
        .bind(realized_pnl)
        .bind(status)
        .bind(exit_fee)
        .bind(exit_exchange_order_id)
        .bind(exit_client_order_id)
        .bind(exit_fill_status)
        .bind(extra_metadata)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            update lane_state
            set recent_pnl = recent_pnl + $2,
                updated_at = now()
            where lane_key = $1
            "#,
        )
        .bind(&trade.lane_key)
        .bind(realized_pnl)
        .execute(&self.pool)
        .await?;
        if TradeMode::from(trade.mode) == TradeMode::Paper {
            self.reconcile_bankroll_for_mode(trade.mode.into()).await?;
        }
        Ok(())
    }

    pub async fn partially_close_trade_with_metadata(
        &self,
        trade_id: i64,
        exit_quantity: f64,
        exit_price: f64,
        realized_pnl: f64,
        status: &str,
        extra_metadata: &serde_json::Value,
    ) -> Result<i64> {
        let trade = sqlx::query_as::<_, TradeLifecycleRow>(
            r#"
            select
                id,
                parent_trade_id,
                decision_id,
                lane_key,
                strategy_family,
                mode,
                status,
                quantity,
                entry_price,
                coalesce(entry_fee, 0) as entry_fee,
                market_id,
                market_ticker,
                side,
                timeout_seconds,
                force_exit_buffer_seconds,
                expires_at,
                entry_exchange_order_id,
                entry_client_order_id,
                entry_fill_status,
                metadata_json,
                created_at
            from trade_lifecycle
            where id = $1
              and closed_at is null
              and status = 'open'
            "#,
        )
        .bind(trade_id)
        .fetch_one(&self.pool)
        .await?;

        if exit_quantity + 0.01 >= trade.quantity {
            self.close_trade_with_metadata(
                trade_id,
                exit_price,
                realized_pnl,
                status,
                extra_metadata,
            )
            .await?;
            return Ok(trade_id);
        }

        let exit_fee = json_f64(extra_metadata, "exit_fee");
        let exit_exchange_order_id = extra_metadata
            .get("exit_order_id")
            .and_then(serde_json::Value::as_str);
        let exit_client_order_id = extra_metadata
            .get("exit_client_order_id")
            .and_then(serde_json::Value::as_str);
        let exit_fill_status = extra_metadata
            .get("exit_status")
            .and_then(serde_json::Value::as_str);

        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            update trade_lifecycle
            set quantity = quantity - $2,
                exit_fee = coalesce($3, exit_fee, 0),
                exit_exchange_order_id = coalesce($4, exit_exchange_order_id),
                exit_client_order_id = coalesce($5, exit_client_order_id),
                exit_fill_status = coalesce($6, exit_fill_status),
                metadata_json = coalesce(metadata_json, '{}'::jsonb) || $7::jsonb
            where id = $1
            "#,
        )
        .bind(trade_id)
        .bind(exit_quantity)
        .bind(exit_fee)
        .bind(exit_exchange_order_id)
        .bind(exit_client_order_id)
        .bind(exit_fill_status)
        .bind(json!({
            "last_partial_exit_at": chrono::Utc::now(),
            "last_partial_exit_quantity": exit_quantity,
        }))
        .execute(&mut *tx)
        .await?;

        let partial_trade_id = sqlx::query_scalar(
            r#"
            insert into trade_lifecycle (
                parent_trade_id,
                decision_id,
                lane_key,
                strategy_family,
                mode,
                status,
                quantity,
                entry_price,
                exit_price,
                realized_pnl,
                market_id,
                market_ticker,
                side,
                timeout_seconds,
                force_exit_buffer_seconds,
                expires_at,
                entry_fee,
                entry_exchange_order_id,
                entry_client_order_id,
                entry_fill_status,
                exit_fee,
                exit_exchange_order_id,
                exit_client_order_id,
                exit_fill_status,
                metadata_json,
                created_at,
                closed_at
            )
            values (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25::jsonb, $26, now()
            )
            returning id
            "#,
        )
        .bind(trade.id)
        .bind(trade.decision_id)
        .bind(&trade.lane_key)
        .bind(trade.strategy_family)
        .bind(trade.mode)
        .bind(status)
        .bind(exit_quantity)
        .bind(trade.entry_price)
        .bind(exit_price)
        .bind(realized_pnl)
        .bind(trade.market_id)
        .bind(&trade.market_ticker)
        .bind(&trade.side)
        .bind(trade.timeout_seconds)
        .bind(trade.force_exit_buffer_seconds)
        .bind(trade.expires_at)
        .bind(proportional_entry_fee(
            trade.entry_fee,
            exit_quantity,
            trade.quantity,
        ))
        .bind(&trade.entry_exchange_order_id)
        .bind(&trade.entry_client_order_id)
        .bind(&trade.entry_fill_status)
        .bind(exit_fee)
        .bind(exit_exchange_order_id)
        .bind(exit_client_order_id)
        .bind(exit_fill_status)
        .bind(merge_json(trade.metadata_json, extra_metadata.clone()))
        .bind(trade.created_at)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            update lane_state
            set recent_pnl = recent_pnl + $2,
                updated_at = now()
            where lane_key = $1
            "#,
        )
        .bind(&trade.lane_key)
        .bind(realized_pnl)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        if TradeMode::from(trade.mode) == TradeMode::Paper {
            self.reconcile_bankroll_for_mode(trade.mode.into()).await?;
        }
        Ok(partial_trade_id)
    }

    pub async fn update_live_trade_exit_order(
        &self,
        trade_id: i64,
        exit_exchange_order_id: Option<&str>,
        exit_client_order_id: Option<&str>,
        exit_fill_status: Option<&str>,
        extra_metadata: &serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            update trade_lifecycle
            set exit_exchange_order_id = coalesce($2, exit_exchange_order_id),
                exit_client_order_id = coalesce($3, exit_client_order_id),
                exit_fill_status = coalesce($4, exit_fill_status),
                metadata_json = coalesce(metadata_json, '{}'::jsonb) || $5::jsonb
            where id = $1
              and mode = 'live'
              and closed_at is null
            "#,
        )
        .bind(trade_id)
        .bind(exit_exchange_order_id)
        .bind(exit_client_order_id)
        .bind(exit_fill_status)
        .bind(extra_metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_unnotified_closed_trades(
        &self,
        limit: i64,
    ) -> Result<Vec<ClosedTradeNotification>> {
        let rows = sqlx::query_as::<_, ClosedTradeNotificationRow>(
            r#"
            select
                tl.id,
                tl.lane_key,
                tl.market_family,
                tl.strategy_family,
                tl.mode,
                coalesce(tl.market_ticker, latest_snapshot.market_ticker, tl.metadata_json->>'market_ticker') as market_ticker,
                latest_snapshot.market_title as market_title,
                coalesce(tl.side, tl.metadata_json->>'side') as side,
                tl.status,
                coalesce(tl.metadata_json->>'exit_reason', tl.metadata_json->>'close_reason') as close_reason,
                tl.quantity,
                tl.entry_price,
                tl.exit_price,
                tl.realized_pnl,
                tl.closed_at
            from trade_lifecycle tl
            left join lateral (
                select
                    venue_features_json->>'market_ticker' as market_ticker,
                    venue_features_json->>'market_title' as market_title
                from market_feature_snapshot mfs
                where mfs.market_id = tl.market_id
                order by mfs.created_at desc, mfs.id desc
                limit 1
            ) latest_snapshot on true
            where tl.closed_at is not null
              and coalesce(tl.metadata_json->>'notified', 'false') <> 'true'
            order by tl.closed_at asc, tl.id asc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn mark_trade_notified(&self, trade_id: i64) -> Result<()> {
        sqlx::query(
            r#"
            update trade_lifecycle
            set metadata_json = coalesce(metadata_json, '{}'::jsonb) || '{"notified": true}'::jsonb
            where id = $1
            "#,
        )
        .bind(trade_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_recent_feature_snapshots(
        &self,
        market_family: Option<MarketFamily>,
        symbol: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MarketFeatureSnapshotRecord>> {
        let rows = if let Some(symbol) = symbol {
            sqlx::query_as::<_, FeatureSnapshotRow>(
                r#"
                select
                    id,
                    market_id,
                    market_family,
                    exchange,
                    symbol,
                    window_minutes,
                    seconds_to_expiry,
                    time_to_expiry_bucket,
                    feature_version,
                    venue_features_json,
                    settlement_features_json,
                    external_reference_json,
                    venue_quality_json,
                    created_at
                from market_feature_snapshot
                where symbol = $1
                  and ($2::text is null or market_family = $2)
                order by created_at desc, id desc
                limit $3
                "#,
            )
            .bind(symbol)
            .bind(market_family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, FeatureSnapshotRow>(
                r#"
                select
                    id,
                    market_id,
                    market_family,
                    exchange,
                    symbol,
                    window_minutes,
                    seconds_to_expiry,
                    time_to_expiry_bucket,
                    feature_version,
                    venue_features_json,
                    settlement_features_json,
                    external_reference_json,
                    venue_quality_json,
                    created_at
                from market_feature_snapshot
                where ($1::text is null or market_family = $1)
                order by created_at desc, id desc
                limit $2
                "#,
            )
            .bind(market_family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter().rev().map(TryInto::try_into).collect()
    }

    pub async fn champion_model_for_symbol(
        &self,
        market_family: MarketFamily,
        symbol: &str,
        strategy_family: StrategyFamily,
    ) -> Result<Option<String>> {
        Ok(self
            .latest_lane_policy_for_symbol(market_family, symbol, strategy_family)
            .await?
            .and_then(|policy| policy.current_champion_model))
    }

    pub async fn reconcile_bankroll_for_mode(&self, mode: TradeMode) -> Result<()> {
        for family in [
            MarketFamily::All,
            MarketFamily::Crypto,
            MarketFamily::Weather,
        ] {
            let scope = portfolio_scope(mode, family);
            let base_bankroll = self
                .bootstrap_bankroll(scope, mode, family)
                .await?
                .unwrap_or_default();
            let aggregates = if family == MarketFamily::All {
                sqlx::query_as::<_, TradeAggregateRow>(
                    r#"
                    select
                        coalesce(sum(
                            case
                                when closed_at is null and status not in ('closed', 'cancelled')
                                then quantity * entry_price
                                else 0
                            end
                        ), 0) as open_exposure,
                        coalesce(sum(
                            case
                                when closed_at is not null then coalesce(realized_pnl, 0)
                                else 0
                            end
                        ), 0) as realized_pnl
                    from trade_lifecycle
                    where mode = $1
                    "#,
                )
                .bind(SqlTradeMode::from(mode))
                .fetch_one(&self.pool)
                .await?
            } else {
                sqlx::query_as::<_, TradeAggregateRow>(
                    r#"
                    select
                        coalesce(sum(
                            case
                                when closed_at is null and status not in ('closed', 'cancelled')
                                then quantity * entry_price
                                else 0
                            end
                        ), 0) as open_exposure,
                        coalesce(sum(
                            case
                                when closed_at is not null then coalesce(realized_pnl, 0)
                                else 0
                            end
                        ), 0) as realized_pnl
                    from trade_lifecycle
                    where mode = $1
                      and market_family = $2
                    "#,
                )
                .bind(SqlTradeMode::from(mode))
                .bind(SqlMarketFamily::from(family))
                .fetch_one(&self.pool)
                .await?
            };
            let bankroll = (base_bankroll + aggregates.realized_pnl).max(0.0);
            let deployable_balance = (bankroll - aggregates.open_exposure).max(0.0);
            sqlx::query(
                r#"
                insert into bankroll_snapshot (
                    scope, market_family, mode, strategy_family, bankroll, deployable_balance, open_exposure,
                    realized_pnl, unrealized_pnl
                )
                values ($1, $2, $3, $4, $5, $6, $7, $8, 0)
                "#,
            )
            .bind(scope)
            .bind(SqlMarketFamily::from(family))
            .bind(SqlTradeMode::from(mode))
            .bind(SqlStrategyFamily::Portfolio)
            .bind(bankroll)
            .bind(deployable_balance)
            .bind(aggregates.open_exposure)
            .bind(aggregates.realized_pnl)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn list_latest_bankrolls(
        &self,
        family: Option<MarketFamily>,
    ) -> Result<Vec<BankrollCard>> {
        let rows = sqlx::query_as::<_, BankrollRow>(
            r#"
            select scope, market_family, mode, strategy_family, bankroll, deployable_balance, open_exposure,
                   realized_pnl, unrealized_pnl, created_at
            from bankroll_snapshot
            where ($1::text is null or market_family = $1 or market_family = 'all')
            order by created_at desc
            "#,
        )
        .bind(family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for row in rows {
            let key = format!(
                "{}:{}:{}:{}",
                row.scope,
                row.market_family.as_key(),
                row.mode.as_key(),
                row.strategy_family.as_key()
            );
            if seen.insert(key) {
                out.push(row.into());
            }
        }
        Ok(out)
    }

    async fn list_lane_states(&self, family: Option<MarketFamily>) -> Result<Vec<LaneState>> {
        let rows = sqlx::query_as::<_, LaneStateRow>(
            r#"
            with ranked as (
                select
                    lane_key,
                    market_family,
                    promotion_state,
                    promotion_reason,
                    recent_pnl,
                    recent_brier,
                    recent_execution_quality,
                    recent_replay_expectancy,
                    quarantine_reason,
                    current_champion_model,
                    row_number() over (
                        partition by market_family, split_part(lane_key, ':', 2), split_part(lane_key, ':', 5)
                        order by updated_at desc
                    ) as rn
                from lane_state
                where ($1::text is null or market_family = $1)
            )
            select lane_key, market_family, promotion_state, promotion_reason, recent_pnl, recent_brier, recent_execution_quality,
                   recent_replay_expectancy, quarantine_reason, current_champion_model
            from ranked
            where rn = 1
            order by
                case promotion_state
                    when 'live_scaled' then 0
                    when 'live_micro' then 1
                    when 'paper_active' then 2
                    when 'shadow' then 3
                    when 'quarantined' then 4
                    else 5
                end,
                recent_replay_expectancy desc,
                recent_pnl desc
            limit 8
            "#,
        )
        .bind(family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn list_open_trades(
        &self,
        family: Option<MarketFamily>,
    ) -> Result<Vec<OpenTradeSummary>> {
        let rows = sqlx::query_as::<_, OpenTradeRow>(
            r#"
            select
                tl.id,
                tl.lane_key,
                tl.market_family,
                tl.strategy_family,
                tl.mode,
                coalesce(
                    tl.market_ticker,
                    latest_snapshot.market_ticker,
                    tl.metadata_json->>'market_ticker'
                ) as market_ticker,
                coalesce(
                    tl.metadata_json->>'market_title',
                    latest_snapshot.market_title
                ) as market_title,
                latest_snapshot.weather_city as weather_city,
                latest_snapshot.weather_contract_kind as weather_contract_kind,
                latest_snapshot.weather_market_date as weather_market_date,
                latest_snapshot.weather_strike_type as weather_strike_type,
                latest_snapshot.weather_floor_strike as weather_floor_strike,
                latest_snapshot.weather_cap_strike as weather_cap_strike,
                tl.quantity,
                tl.entry_price,
                tl.created_at,
                tl.status
            from trade_lifecycle tl
            left join lateral (
                select
                    venue_features_json->>'market_ticker' as market_ticker,
                    venue_features_json->>'market_title' as market_title,
                    settlement_features_json->>'weather_city' as weather_city,
                    settlement_features_json->>'weather_contract_kind' as weather_contract_kind,
                    settlement_features_json->>'weather_market_date' as weather_market_date,
                    settlement_features_json->>'weather_strike_type' as weather_strike_type,
                    (settlement_features_json->>'weather_floor_strike')::double precision as weather_floor_strike,
                    (settlement_features_json->>'weather_cap_strike')::double precision as weather_cap_strike
                from market_feature_snapshot mfs
                where mfs.market_id = tl.market_id
                order by mfs.created_at desc, mfs.id desc
                limit 1
            ) latest_snapshot on true
            where tl.closed_at is null and tl.status not in ('closed', 'cancelled')
              and ($1::text is null or tl.market_family = $1)
            order by tl.created_at desc, tl.id desc
            limit 12
            "#,
        )
        .bind(family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn list_latest_opportunities(
        &self,
        family: Option<MarketFamily>,
        limit: i64,
    ) -> Result<Vec<OpportunityCard>> {
        let freshness_cutoff = Utc::now() - opportunity_card_freshness_window(family);
        let rows = sqlx::query(
            r#"
            with ranked as (
                select
                    od.id,
                    od.lane_key,
                    od.market_family,
                    od.strategy_family,
                    od.side,
                    od.market_prob,
                    od.model_prob,
                    od.edge,
                    od.confidence,
                    od.approved,
                    od.reasons_json,
                    od.created_at,
                    latest_intent.status as execution_status,
                    case
                        when overlapping_trade.id is not null then 'existing_open_trade_for_lane'
                        else latest_intent.last_error
                    end as execution_note,
                    row_number() over (partition by od.lane_key order by od.created_at desc, od.id desc) as rn
                from opportunity_decision od
                left join lateral (
                    select id
                    from trade_lifecycle tl
                    where tl.lane_key = od.lane_key
                      and tl.created_at <= od.created_at
                      and coalesce(tl.closed_at, od.created_at + interval '100 years') >= od.created_at
                      and tl.status not in ('closed', 'cancelled')
                    order by tl.created_at desc, tl.id desc
                    limit 1
                ) overlapping_trade on true
                left join lateral (
                    select status, last_error
                    from execution_intent ei
                    where ei.decision_id = od.id
                    order by ei.created_at desc, ei.id desc
                    limit 1
                ) latest_intent on true
                where ($1::text is null or od.market_family = $1)
                  and od.created_at >= $2
            )
            select id, lane_key, market_family, strategy_family, side, market_prob, model_prob, edge, confidence, approved, reasons_json, created_at, execution_status, execution_note, rn
            from ranked
            where rn = 1
            order by created_at desc
            limit $3
            "#,
        )
        .bind(family.map(|value| SqlMarketFamily::from(value).as_key().to_string()))
        .bind(freshness_cutoff)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut opportunities = Vec::with_capacity(rows.len());
        for row in rows {
            let strategy_family =
                parse_strategy_family(row.try_get::<String, _>("strategy_family")?.as_str());
            let reasons_json: serde_json::Value = row.try_get("reasons_json")?;
            opportunities.push(OpportunityCard {
                lane_key: row.try_get("lane_key")?,
                market_family: parse_market_family(&row.try_get::<String, _>("market_family")?),
                strategy_family,
                side: row.try_get("side")?,
                market_prob: row.try_get("market_prob")?,
                model_prob: row.try_get("model_prob")?,
                edge: row.try_get("edge")?,
                confidence: row.try_get("confidence")?,
                approved: row.try_get("approved")?,
                reasons: reasons_json
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                execution_status: row.try_get("execution_status")?,
                execution_note: row.try_get("execution_note")?,
                as_of: row.try_get("created_at")?,
            });
        }
        Ok(opportunities)
    }

    async fn insert_bankroll_seed(
        &self,
        scope: &str,
        market_family: MarketFamily,
        bankroll: f64,
        mode: SqlTradeMode,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into bankroll_snapshot (
                scope, market_family, mode, strategy_family, bankroll, deployable_balance, open_exposure,
                realized_pnl, unrealized_pnl
            )
            values ($1, $2, $3, $4, $5, $5, 0, 0, 0)
            "#,
        )
        .bind(scope)
        .bind(SqlMarketFamily::from(market_family))
        .bind(mode)
        .bind(SqlStrategyFamily::Portfolio)
        .bind(bankroll)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ensure_bootstrap_bankroll(
        &self,
        scope: &str,
        market_family: MarketFamily,
        bankroll: f64,
        mode: SqlTradeMode,
    ) -> Result<()> {
        let snapshot_count: i64 = sqlx::query_scalar(
            r#"
            select count(*)
            from bankroll_snapshot
            where scope = $1 and market_family = $2 and mode = $3 and strategy_family = $4
            "#,
        )
        .bind(scope)
        .bind(SqlMarketFamily::from(market_family))
        .bind(mode)
        .bind(SqlStrategyFamily::Portfolio)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        if snapshot_count == 0 {
            self.insert_bankroll_seed(scope, market_family, bankroll, mode)
                .await?;
            return Ok(());
        }

        if snapshot_count == 1 {
            let row = sqlx::query_as::<_, BootstrapBankrollRow>(
                r#"
                select id, open_exposure, realized_pnl, unrealized_pnl
                from bankroll_snapshot
                where scope = $1 and market_family = $2 and mode = $3 and strategy_family = $4
                order by created_at desc
                limit 1
                "#,
            )
            .bind(scope)
            .bind(SqlMarketFamily::from(market_family))
            .bind(mode)
            .bind(SqlStrategyFamily::Portfolio)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = row {
                let is_bootstrap_only = row.open_exposure.abs() < f64::EPSILON
                    && row.realized_pnl.abs() < f64::EPSILON
                    && row.unrealized_pnl.abs() < f64::EPSILON;

                if is_bootstrap_only {
                    sqlx::query(
                        r#"
                        update bankroll_snapshot
                        set bankroll = $2, deployable_balance = $2
                        where id = $1
                        "#,
                    )
                    .bind(row.id)
                    .bind(bankroll)
                    .execute(&self.pool)
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn bootstrap_bankroll(
        &self,
        scope: &str,
        mode: TradeMode,
        market_family: MarketFamily,
    ) -> Result<Option<f64>> {
        let bankroll = sqlx::query_scalar::<_, f64>(
            r#"
            select bankroll
            from bankroll_snapshot
            where scope = $1 and market_family = $2 and mode = $3 and strategy_family = $4
            order by created_at asc, id asc
            limit 1
            "#,
        )
        .bind(scope)
        .bind(SqlMarketFamily::from(market_family))
        .bind(SqlTradeMode::from(mode))
        .bind(SqlStrategyFamily::Portfolio)
        .fetch_optional(&self.pool)
        .await?;
        Ok(bankroll)
    }
}

fn count_state(lanes: &[LaneState], state: PromotionState) -> i64 {
    lanes
        .iter()
        .filter(|lane| lane.promotion_state == state)
        .count() as i64
}

fn portfolio_scope(mode: TradeMode, market_family: MarketFamily) -> &'static str {
    match (mode, market_family) {
        (TradeMode::Paper, MarketFamily::All) => "paper",
        (TradeMode::Live, MarketFamily::All) => "live",
        (TradeMode::Paper, MarketFamily::Crypto) => "paper_crypto",
        (TradeMode::Paper, MarketFamily::Weather) => "paper_weather",
        (TradeMode::Live, MarketFamily::Crypto) => "live_crypto",
        (TradeMode::Live, MarketFamily::Weather) => "live_weather",
    }
}

fn opportunity_card_freshness_window(family: Option<MarketFamily>) -> chrono::Duration {
    match family {
        Some(MarketFamily::Crypto) => chrono::Duration::minutes(10),
        Some(MarketFamily::Weather) => chrono::Duration::minutes(30),
        _ => chrono::Duration::minutes(20),
    }
}

fn overall_readiness_status(lanes: &[LaneState]) -> String {
    if lanes
        .iter()
        .any(|lane| lane.promotion_state == PromotionState::LiveScaled)
    {
        return "live_active".to_string();
    }
    if lanes
        .iter()
        .any(|lane| lane.promotion_state == PromotionState::LiveMicro)
    {
        return "live_micro_active".to_string();
    }
    let paper_active = lanes
        .iter()
        .filter(|lane| lane.promotion_state == PromotionState::PaperActive)
        .count();
    let quarantined = lanes
        .iter()
        .filter(|lane| lane.promotion_state == PromotionState::Quarantined)
        .count();
    if paper_active > 0 && quarantined == 0 {
        return "promotion_candidate".to_string();
    }
    if quarantined > 0 {
        return "attention_needed".to_string();
    }
    "shadow_only".to_string()
}

#[derive(sqlx::FromRow)]
struct BankrollRow {
    scope: String,
    market_family: SqlMarketFamily,
    mode: SqlTradeMode,
    strategy_family: SqlStrategyFamily,
    bankroll: f64,
    deployable_balance: f64,
    open_exposure: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LaneStateRow {
    lane_key: String,
    market_family: SqlMarketFamily,
    promotion_state: SqlPromotionState,
    promotion_reason: Option<String>,
    recent_pnl: f64,
    recent_brier: f64,
    recent_execution_quality: f64,
    recent_replay_expectancy: f64,
    quarantine_reason: Option<String>,
    current_champion_model: Option<String>,
}

#[derive(sqlx::FromRow)]
struct OpenTradeRow {
    id: i64,
    lane_key: String,
    market_family: SqlMarketFamily,
    strategy_family: SqlStrategyFamily,
    mode: SqlTradeMode,
    market_ticker: Option<String>,
    market_title: Option<String>,
    weather_city: Option<String>,
    weather_contract_kind: Option<String>,
    weather_market_date: Option<String>,
    weather_strike_type: Option<String>,
    weather_floor_strike: Option<f64>,
    weather_cap_strike: Option<f64>,
    quantity: f64,
    entry_price: f64,
    created_at: DateTime<Utc>,
    status: String,
}

#[derive(sqlx::FromRow)]
struct LaneTradeRow {
    id: i64,
    market_family: SqlMarketFamily,
    status: String,
    mode: SqlTradeMode,
    quantity: f64,
    entry_price: f64,
    exit_price: Option<f64>,
    realized_pnl: Option<f64>,
    created_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct OpportunityRow {
    id: i64,
    lane_key: String,
    market_family: SqlMarketFamily,
    strategy_family: SqlStrategyFamily,
    side: String,
    market_prob: f64,
    model_prob: f64,
    edge: f64,
    confidence: f64,
    approved: bool,
    reasons_json: serde_json::Value,
    execution_status: Option<String>,
    execution_note: Option<String>,
    created_at: DateTime<Utc>,
    rn: i32,
}

#[derive(sqlx::FromRow)]
struct BootstrapBankrollRow {
    id: i64,
    open_exposure: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
}

#[derive(sqlx::FromRow)]
struct TradeAggregateRow {
    open_exposure: f64,
    realized_pnl: f64,
}

#[derive(sqlx::FromRow)]
struct RecentTradeMetricsRow {
    trade_count: i64,
    win_count: i64,
    realized_pnl: f64,
}

#[derive(sqlx::FromRow)]
struct WorkerStatusRow {
    service: String,
    status: String,
    last_started_at: Option<DateTime<Utc>>,
    last_succeeded_at: Option<DateTime<Utc>>,
    last_failed_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    details_json: serde_json::Value,
    updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ProbabilityCalibrationRow {
    mean_error: f64,
    sample_count: i32,
}

#[derive(sqlx::FromRow)]
struct TrainedModelArtifactRow {
    model_name: String,
    market_family: SqlMarketFamily,
    strategy_family: SqlStrategyFamily,
    symbol: Option<String>,
    feature_version: String,
    sample_count: i32,
    weights_json: serde_json::Value,
    metrics_json: serde_json::Value,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct HistoricalReplayExampleInsert {
    pub source: String,
    pub source_key: String,
    pub market_family: MarketFamily,
    pub exchange: String,
    pub symbol: String,
    pub strategy_family: StrategyFamily,
    pub market_id: Option<i64>,
    pub market_slug: String,
    pub recorded_at: DateTime<Utc>,
    pub resolved_yes_probability: f64,
    pub market_prob: f64,
    pub time_to_expiry_bucket: String,
    pub feature_version: String,
    pub feature_vector_json: serde_json::Value,
    pub metadata_json: serde_json::Value,
}

#[derive(sqlx::FromRow)]
struct HistoricalReplayExampleRow {
    market_family: SqlMarketFamily,
    lane_key: String,
    time_to_expiry_bucket: String,
    resolved_yes_probability: f64,
    market_prob: f64,
    feature_vector_json: serde_json::Value,
}

#[derive(sqlx::FromRow)]
struct ModelBenchmarkRunRow {
    id: i64,
    source: String,
    example_count: i32,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ModelBenchmarkResultRow {
    model_name: String,
    rank: i32,
    brier: f64,
    execution_pnl: f64,
    sample_count: i32,
    trade_count: i32,
    win_rate: f64,
    fill_rate: f64,
    slippage_bps: f64,
    edge_realization_ratio: f64,
}

#[derive(sqlx::FromRow)]
struct LiveExchangeSyncStatusRow {
    source: String,
    synced_at: DateTime<Utc>,
    positions_count: i32,
    resting_orders_count: i32,
    recent_fills_count: i32,
    local_open_live_trades_count: i32,
    status: String,
    issues_json: serde_json::Value,
    details_json: serde_json::Value,
}

#[derive(sqlx::FromRow)]
struct OperatorControlStateRow {
    live_order_placement_enabled: bool,
    updated_by: Option<String>,
    note: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct FeatureSnapshotRow {
    id: i64,
    market_id: i64,
    market_family: SqlMarketFamily,
    exchange: String,
    symbol: String,
    window_minutes: i32,
    seconds_to_expiry: i32,
    time_to_expiry_bucket: String,
    feature_version: String,
    venue_features_json: serde_json::Value,
    settlement_features_json: serde_json::Value,
    external_reference_json: serde_json::Value,
    venue_quality_json: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct PendingExecutionIntentRow {
    intent_id: i64,
    decision_id: i64,
    market_id: i64,
    market_family: SqlMarketFamily,
    lane_key: String,
    strategy_family: SqlStrategyFamily,
    side: String,
    market_prob: f64,
    model_prob: f64,
    confidence: f64,
    recommended_size: f64,
    mode: SqlTradeMode,
    entry_style: String,
    timeout_seconds: i32,
    force_exit_buffer_seconds: i32,
    predicted_fill_probability: Option<f64>,
    predicted_slippage_bps: Option<f64>,
    predicted_queue_ahead: Option<f64>,
    execution_forecast_version: Option<String>,
    submitted_quantity: f64,
    accepted_quantity: f64,
    filled_quantity: f64,
    cancelled_quantity: f64,
    rejected_quantity: f64,
    avg_fill_price: Option<f64>,
    first_fill_at: Option<DateTime<Utc>>,
    last_fill_at: Option<DateTime<Utc>>,
    terminal_outcome: Option<String>,
    promotion_state_at_creation: Option<String>,
    stop_conditions_json: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ActiveLiveExecutionIntentRow {
    intent_id: i64,
    decision_id: i64,
    market_id: i64,
    market_family: SqlMarketFamily,
    lane_key: String,
    strategy_family: SqlStrategyFamily,
    side: String,
    market_prob: f64,
    recommended_size: f64,
    mode: SqlTradeMode,
    timeout_seconds: i32,
    force_exit_buffer_seconds: i32,
    created_at: DateTime<Utc>,
    status: String,
    market_ticker: Option<String>,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    fill_status: Option<String>,
    submitted_quantity: f64,
    accepted_quantity: f64,
    filled_quantity: f64,
    cancelled_quantity: f64,
    rejected_quantity: f64,
    avg_fill_price: Option<f64>,
    terminal_outcome: Option<String>,
    last_transition_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct OpenTradeForExitRow {
    id: i64,
    decision_id: Option<i64>,
    market_family: SqlMarketFamily,
    lane_key: String,
    strategy_family: SqlStrategyFamily,
    mode: SqlTradeMode,
    quantity: f64,
    entry_price: f64,
    entry_fee: f64,
    market_id: i64,
    market_ticker: Option<String>,
    side: String,
    created_at: DateTime<Utc>,
    timeout_seconds: i32,
    force_exit_buffer_seconds: i32,
    expires_at: Option<DateTime<Utc>>,
    exit_exchange_order_id: Option<String>,
    exit_client_order_id: Option<String>,
    exit_fill_status: Option<String>,
}

#[derive(sqlx::FromRow)]
struct OpenLiveTradeForReconciliationRow {
    id: i64,
    market_family: SqlMarketFamily,
    lane_key: String,
    market_ticker: String,
    side: String,
    quantity: f64,
    entry_price: f64,
    entry_fee: f64,
    created_at: DateTime<Utc>,
    entry_exchange_order_id: Option<String>,
    entry_client_order_id: Option<String>,
    exit_exchange_order_id: Option<String>,
    exit_client_order_id: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LiveExitFillAggregateRow {
    fill_quantity: f64,
    avg_fill_price: Option<f64>,
    total_fee: f64,
    latest_fill_time: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct TradeExitProgressRow {
    closed_quantity: f64,
    exit_fee: f64,
}

#[derive(sqlx::FromRow)]
struct ClosedTradeNotificationRow {
    id: i64,
    lane_key: String,
    market_family: SqlMarketFamily,
    strategy_family: SqlStrategyFamily,
    mode: SqlTradeMode,
    market_ticker: Option<String>,
    market_title: Option<String>,
    side: Option<String>,
    status: String,
    close_reason: Option<String>,
    quantity: f64,
    entry_price: f64,
    exit_price: Option<f64>,
    realized_pnl: Option<f64>,
    closed_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct OperatorActionEventRow {
    id: i64,
    action: String,
    actor: Option<String>,
    note: Option<String>,
    payload_json: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct CloseTradeRow {
    lane_key: String,
    mode: SqlTradeMode,
}

#[allow(dead_code)]
#[derive(sqlx::FromRow)]
struct TradeLifecycleRow {
    id: i64,
    parent_trade_id: Option<i64>,
    decision_id: Option<i64>,
    lane_key: String,
    strategy_family: SqlStrategyFamily,
    mode: SqlTradeMode,
    status: String,
    quantity: f64,
    entry_price: f64,
    entry_fee: f64,
    market_id: Option<i64>,
    market_ticker: Option<String>,
    side: Option<String>,
    timeout_seconds: Option<i32>,
    force_exit_buffer_seconds: Option<i32>,
    expires_at: Option<DateTime<Utc>>,
    entry_exchange_order_id: Option<String>,
    entry_client_order_id: Option<String>,
    entry_fill_status: Option<String>,
    metadata_json: serde_json::Value,
    created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LivePositionSyncRow {
    market_ticker: String,
    position_count: f64,
    resting_order_count: i32,
    market_exposure: f64,
    realized_pnl: f64,
    synced_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LiveOrderSyncListRow {
    order_id: String,
    client_order_id: Option<String>,
    market_ticker: Option<String>,
    action: Option<String>,
    side: Option<String>,
    status: Option<String>,
    count: f64,
    fill_count: f64,
    remaining_count: f64,
    created_time: Option<DateTime<Utc>>,
    synced_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LiveFillSyncListRow {
    fill_id: String,
    order_id: Option<String>,
    client_order_id: Option<String>,
    market_ticker: Option<String>,
    action: Option<String>,
    side: Option<String>,
    count: f64,
    fee_paid: f64,
    created_time: Option<DateTime<Utc>>,
    synced_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LiveIntentRow {
    intent_id: i64,
    lane_key: String,
    mode: SqlTradeMode,
    status: String,
    last_error: Option<String>,
    market_ticker: Option<String>,
    side: Option<String>,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    fill_status: Option<String>,
    created_at: DateTime<Utc>,
    last_transition_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ReplayExecutionQualityRow {
    replay_as_of: DateTime<Utc>,
    trade_count: i32,
    fill_rate: f64,
    slippage_bps: f64,
    edge_realization_ratio: f64,
}

#[derive(sqlx::FromRow)]
struct LiveExecutionQualityRow {
    processed_at: DateTime<Utc>,
    predicted_fill_probability: Option<f64>,
    submitted_quantity: f64,
    filled_quantity: f64,
}

#[derive(sqlx::FromRow)]
struct ReplayLaneExecutionTruthRow {
    market_family: SqlMarketFamily,
    lane_key: String,
    trade_count: i32,
    fill_rate: f64,
    edge_realization_ratio: f64,
}

#[derive(sqlx::FromRow)]
struct LiveLaneExecutionTruthRow {
    market_family: SqlMarketFamily,
    lane_key: String,
    predicted_fill_probability: Option<f64>,
    submitted_quantity: f64,
    filled_quantity: f64,
}

#[derive(sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
enum SqlMarketFamily {
    All,
    Crypto,
    Weather,
}

impl SqlMarketFamily {
    fn as_key(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Crypto => "crypto",
            Self::Weather => "weather",
        }
    }
}

impl From<SqlMarketFamily> for MarketFamily {
    fn from(value: SqlMarketFamily) -> Self {
        match value {
            SqlMarketFamily::All => MarketFamily::All,
            SqlMarketFamily::Crypto => MarketFamily::Crypto,
            SqlMarketFamily::Weather => MarketFamily::Weather,
        }
    }
}

impl From<MarketFamily> for SqlMarketFamily {
    fn from(value: MarketFamily) -> Self {
        match value {
            MarketFamily::All => SqlMarketFamily::All,
            MarketFamily::Crypto => SqlMarketFamily::Crypto,
            MarketFamily::Weather => SqlMarketFamily::Weather,
        }
    }
}

#[derive(sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
enum SqlStrategyFamily {
    Portfolio,
    DirectionalSettlement,
    PreSettlementScalp,
}

impl SqlStrategyFamily {
    fn as_key(self) -> &'static str {
        match self {
            Self::Portfolio => "portfolio",
            Self::DirectionalSettlement => "directional_settlement",
            Self::PreSettlementScalp => "pre_settlement_scalp",
        }
    }
}

impl From<SqlStrategyFamily> for StrategyFamily {
    fn from(value: SqlStrategyFamily) -> Self {
        match value {
            SqlStrategyFamily::Portfolio => StrategyFamily::Portfolio,
            SqlStrategyFamily::DirectionalSettlement => StrategyFamily::DirectionalSettlement,
            SqlStrategyFamily::PreSettlementScalp => StrategyFamily::PreSettlementScalp,
        }
    }
}

impl From<StrategyFamily> for SqlStrategyFamily {
    fn from(value: StrategyFamily) -> Self {
        match value {
            StrategyFamily::Portfolio => SqlStrategyFamily::Portfolio,
            StrategyFamily::DirectionalSettlement => SqlStrategyFamily::DirectionalSettlement,
            StrategyFamily::PreSettlementScalp => SqlStrategyFamily::PreSettlementScalp,
        }
    }
}

#[derive(sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
enum SqlTradeMode {
    Paper,
    Live,
}

impl SqlTradeMode {
    fn as_key(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

impl From<SqlTradeMode> for TradeMode {
    fn from(value: SqlTradeMode) -> Self {
        match value {
            SqlTradeMode::Paper => TradeMode::Paper,
            SqlTradeMode::Live => TradeMode::Live,
        }
    }
}

impl From<TradeMode> for SqlTradeMode {
    fn from(value: TradeMode) -> Self {
        match value {
            TradeMode::Paper => SqlTradeMode::Paper,
            TradeMode::Live => SqlTradeMode::Live,
        }
    }
}

#[derive(sqlx::Type, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
enum SqlPromotionState {
    Shadow,
    PaperActive,
    LiveMicro,
    LiveScaled,
    Quarantined,
}

impl SqlPromotionState {
    fn as_key(self) -> &'static str {
        match self {
            Self::Shadow => "shadow",
            Self::PaperActive => "paper_active",
            Self::LiveMicro => "live_micro",
            Self::LiveScaled => "live_scaled",
            Self::Quarantined => "quarantined",
        }
    }
}

impl From<SqlPromotionState> for PromotionState {
    fn from(value: SqlPromotionState) -> Self {
        match value {
            SqlPromotionState::Shadow => PromotionState::Shadow,
            SqlPromotionState::PaperActive => PromotionState::PaperActive,
            SqlPromotionState::LiveMicro => PromotionState::LiveMicro,
            SqlPromotionState::LiveScaled => PromotionState::LiveScaled,
            SqlPromotionState::Quarantined => PromotionState::Quarantined,
        }
    }
}

impl From<PromotionState> for SqlPromotionState {
    fn from(value: PromotionState) -> Self {
        match value {
            PromotionState::Shadow => SqlPromotionState::Shadow,
            PromotionState::PaperActive => SqlPromotionState::PaperActive,
            PromotionState::LiveMicro => SqlPromotionState::LiveMicro,
            PromotionState::LiveScaled => SqlPromotionState::LiveScaled,
            PromotionState::Quarantined => SqlPromotionState::Quarantined,
        }
    }
}

impl From<BankrollRow> for BankrollCard {
    fn from(value: BankrollRow) -> Self {
        Self {
            scope: value.scope,
            market_family: value.market_family.into(),
            mode: value.mode.into(),
            strategy_family: value.strategy_family.into(),
            bankroll: value.bankroll,
            deployable_balance: value.deployable_balance,
            open_exposure: value.open_exposure,
            realized_pnl: value.realized_pnl,
            unrealized_pnl: value.unrealized_pnl,
            as_of: value.created_at,
        }
    }
}

impl From<LaneStateRow> for LaneState {
    fn from(value: LaneStateRow) -> Self {
        Self {
            lane_key: value.lane_key,
            market_family: value.market_family.into(),
            promotion_state: value.promotion_state.into(),
            promotion_reason: value.promotion_reason,
            recent_pnl: value.recent_pnl,
            recent_brier: value.recent_brier,
            recent_execution_quality: value.recent_execution_quality,
            recent_replay_expectancy: value.recent_replay_expectancy,
            quarantine_reason: value.quarantine_reason,
            current_champion_model: value.current_champion_model,
        }
    }
}

impl From<LaneStateRow> for SymbolLanePolicy {
    fn from(value: LaneStateRow) -> Self {
        Self {
            lane_key: value.lane_key,
            market_family: value.market_family.into(),
            promotion_state: value.promotion_state.into(),
            promotion_reason: value.promotion_reason,
            recent_pnl: value.recent_pnl,
            recent_brier: value.recent_brier,
            recent_execution_quality: value.recent_execution_quality,
            recent_replay_expectancy: value.recent_replay_expectancy,
            quarantine_reason: value.quarantine_reason,
            current_champion_model: value.current_champion_model,
        }
    }
}

impl From<OpenTradeRow> for OpenTradeSummary {
    fn from(value: OpenTradeRow) -> Self {
        Self {
            trade_id: value.id,
            lane_key: value.lane_key,
            market_family: value.market_family.into(),
            strategy_family: value.strategy_family.into(),
            mode: value.mode.into(),
            market_ticker: value.market_ticker,
            market_title: value.market_title,
            weather_city: value.weather_city,
            weather_contract_kind: value.weather_contract_kind,
            weather_market_date: value.weather_market_date,
            weather_strike_type: value.weather_strike_type,
            weather_floor_strike: value.weather_floor_strike,
            weather_cap_strike: value.weather_cap_strike,
            quantity: value.quantity,
            entry_price: value.entry_price,
            created_at: value.created_at,
            status: value.status,
        }
    }
}

impl From<LaneTradeRow> for LaneTradeCard {
    fn from(value: LaneTradeRow) -> Self {
        Self {
            trade_id: value.id,
            market_family: value.market_family.into(),
            status: value.status,
            mode: value.mode.into(),
            quantity: value.quantity,
            entry_price: value.entry_price,
            exit_price: value.exit_price,
            realized_pnl: value.realized_pnl,
            created_at: value.created_at,
            closed_at: value.closed_at,
        }
    }
}

impl From<OpportunityRow> for OpportunityCard {
    fn from(value: OpportunityRow) -> Self {
        let _ = value.id;
        let _ = value.rn;
        Self {
            lane_key: value.lane_key,
            market_family: value.market_family.into(),
            strategy_family: value.strategy_family.into(),
            side: value.side,
            market_prob: value.market_prob,
            model_prob: value.model_prob,
            edge: value.edge,
            confidence: value.confidence,
            approved: value.approved,
            reasons: value
                .reasons_json
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            execution_status: value.execution_status,
            execution_note: value.execution_note,
            as_of: value.created_at,
        }
    }
}

impl TryFrom<FeatureSnapshotRow> for MarketFeatureSnapshotRecord {
    type Error = anyhow::Error;

    fn try_from(value: FeatureSnapshotRow) -> Result<Self, Self::Error> {
        let _ = value.id;
        let venue = value.venue_features_json;
        let settlement = value.settlement_features_json;
        let external = value.external_reference_json;
        let quality = value.venue_quality_json;
        Ok(Self {
            market_id: value.market_id,
            market_family: value.market_family.into(),
            market_ticker: json_text(&venue, "market_ticker"),
            market_title: json_text(&venue, "market_title"),
            feature_version: value.feature_version,
            exchange: value.exchange,
            symbol: value.symbol,
            window_minutes: value.window_minutes,
            seconds_to_expiry: value.seconds_to_expiry,
            time_to_expiry_bucket: value.time_to_expiry_bucket,
            market_prob: json_f64(&venue, "market_prob"),
            best_bid: json_f64(&venue, "best_bid"),
            best_ask: json_f64(&venue, "best_ask"),
            last_price: json_f64(&venue, "last_price"),
            bid_size: json_f64(&venue, "bid_size"),
            ask_size: json_f64(&venue, "ask_size"),
            liquidity: json_f64(&venue, "liquidity"),
            order_book_imbalance: json_f64(&venue, "order_book_imbalance"),
            aggressive_buy_ratio: json_f64(&venue, "aggressive_buy_ratio"),
            spread_bps: json_f64(&quality, "spread_bps"),
            venue_quality_score: json_f64(&quality, "venue_quality_score"),
            reference_price: json_f64(&external, "reference_price"),
            reference_previous_price: json_f64(&external, "reference_previous_price"),
            reference_price_change_bps: json_f64(&external, "reference_price_change_bps"),
            reference_yes_prob: json_f64(&external, "reference_yes_prob"),
            reference_gap_bps: json_f64(&external, "reference_gap_bps"),
            threshold_distance_bps_proxy: json_f64(&settlement, "threshold_distance_bps_proxy"),
            distance_to_strike_bps: json_f64(&settlement, "distance_to_strike_bps"),
            reference_velocity: json_f64(&external, "reference_velocity"),
            realized_vol_short: json_f64(&external, "realized_vol_short"),
            time_decay_factor: json_f64(&settlement, "time_decay_factor"),
            size_ahead: json_f64(&venue, "size_ahead"),
            trade_rate: json_f64(&venue, "trade_rate"),
            quote_churn: json_f64(&venue, "quote_churn"),
            size_ahead_real: json_f64(&venue, "size_ahead_real"),
            trade_consumption_rate: json_f64(&venue, "trade_consumption_rate"),
            cancel_rate: json_f64(&venue, "cancel_rate"),
            queue_decay_rate: json_f64(&venue, "queue_decay_rate"),
            averaging_window_progress: json_f64(&settlement, "averaging_window_progress"),
            settlement_regime: json_text(&settlement, "settlement_regime"),
            last_minute_avg_proxy: json_f64(&settlement, "last_minute_avg_proxy"),
            market_data_age_seconds: json_i32(&quality, "market_data_age_seconds"),
            reference_age_seconds: json_i32(&external, "reference_age_seconds"),
            weather_city: json_optional_text(&settlement, "weather_city"),
            weather_contract_kind: json_optional_text(&settlement, "weather_contract_kind"),
            weather_market_date: json_optional_text(&settlement, "weather_market_date"),
            weather_strike_type: json_optional_text(&settlement, "weather_strike_type"),
            weather_floor_strike: json_optional_f64(&settlement, "weather_floor_strike"),
            weather_cap_strike: json_optional_f64(&settlement, "weather_cap_strike"),
            weather_forecast_temperature_f: json_optional_f64(
                &external,
                "weather_forecast_temperature_f",
            ),
            weather_observation_temperature_f: json_optional_f64(
                &external,
                "weather_observation_temperature_f",
            ),
            weather_reference_confidence: json_optional_f64(
                &external,
                "weather_reference_confidence",
            ),
            weather_reference_source: json_optional_text(&external, "weather_reference_source"),
            created_at: value.created_at,
        })
    }
}

impl TryFrom<HistoricalReplayExampleRow> for HistoricalReplayExampleCard {
    type Error = anyhow::Error;

    fn try_from(value: HistoricalReplayExampleRow) -> Result<Self, Self::Error> {
        let features = &value.feature_vector_json;
        Ok(Self {
            market_family: value.market_family.into(),
            lane_key: value.lane_key,
            time_to_expiry_bucket: value.time_to_expiry_bucket,
            target_yes_probability: value.resolved_yes_probability,
            market_prob: value.market_prob,
            features: market_models::FeatureVector {
                market_prob: json_f64(features, "market_prob"),
                reference_yes_prob: json_f64(features, "reference_yes_prob"),
                reference_gap_bps_scaled: json_f64(features, "reference_gap_bps_scaled"),
                threshold_distance_bps_scaled: json_f64(features, "threshold_distance_bps_scaled"),
                distance_to_strike_bps_scaled: json_f64(features, "distance_to_strike_bps_scaled"),
                reference_velocity_scaled: json_f64(features, "reference_velocity_scaled"),
                realized_vol_short_scaled: json_f64(features, "realized_vol_short_scaled"),
                time_decay_factor: json_f64(features, "time_decay_factor"),
                order_book_imbalance: json_f64(features, "order_book_imbalance"),
                aggressive_buy_ratio: json_f64(features, "aggressive_buy_ratio"),
                size_ahead_scaled: json_f64(features, "size_ahead_scaled"),
                trade_rate_scaled: json_f64(features, "trade_rate_scaled"),
                quote_churn_scaled: json_f64(features, "quote_churn_scaled"),
                size_ahead_real_scaled: json_f64(features, "size_ahead_real_scaled"),
                trade_consumption_rate_scaled: json_f64(features, "trade_consumption_rate_scaled"),
                cancel_rate_scaled: json_f64(features, "cancel_rate_scaled"),
                queue_decay_rate_scaled: json_f64(features, "queue_decay_rate_scaled"),
                spread_bps_scaled: json_f64(features, "spread_bps_scaled"),
                venue_quality_score: json_f64(features, "venue_quality_score"),
                market_data_age_scaled: json_f64(features, "market_data_age_scaled"),
                reference_age_scaled: json_f64(features, "reference_age_scaled"),
                averaging_window_progress: json_f64(features, "averaging_window_progress"),
                seconds_to_expiry_scaled: json_f64(features, "seconds_to_expiry_scaled"),
            },
        })
    }
}

impl From<PendingExecutionIntentRow> for PendingExecutionIntent {
    fn from(value: PendingExecutionIntentRow) -> Self {
        Self {
            intent_id: value.intent_id,
            decision_id: value.decision_id,
            market_id: value.market_id,
            market_family: value.market_family.into(),
            lane_key: value.lane_key,
            strategy_family: value.strategy_family.into(),
            side: value.side,
            market_prob: value.market_prob,
            model_prob: value.model_prob,
            confidence: value.confidence,
            recommended_size: value.recommended_size,
            mode: value.mode.into(),
            entry_style: value.entry_style,
            timeout_seconds: value.timeout_seconds,
            force_exit_buffer_seconds: value.force_exit_buffer_seconds,
            predicted_fill_probability: value.predicted_fill_probability,
            predicted_slippage_bps: value.predicted_slippage_bps,
            predicted_queue_ahead: value.predicted_queue_ahead,
            execution_forecast_version: value.execution_forecast_version,
            submitted_quantity: value.submitted_quantity,
            accepted_quantity: value.accepted_quantity,
            filled_quantity: value.filled_quantity,
            cancelled_quantity: value.cancelled_quantity,
            rejected_quantity: value.rejected_quantity,
            avg_fill_price: value.avg_fill_price,
            first_fill_at: value.first_fill_at,
            last_fill_at: value.last_fill_at,
            terminal_outcome: value.terminal_outcome,
            promotion_state_at_creation: value
                .promotion_state_at_creation
                .as_deref()
                .map(parse_promotion_state),
            stop_conditions_json: value
                .stop_conditions_json
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            created_at: value.created_at,
        }
    }
}

impl From<ActiveLiveExecutionIntentRow> for ActiveLiveExecutionIntent {
    fn from(value: ActiveLiveExecutionIntentRow) -> Self {
        Self {
            intent_id: value.intent_id,
            decision_id: value.decision_id,
            market_id: value.market_id,
            market_family: value.market_family.into(),
            lane_key: value.lane_key,
            strategy_family: value.strategy_family.into(),
            side: value.side,
            market_prob: value.market_prob,
            recommended_size: value.recommended_size,
            mode: value.mode.into(),
            timeout_seconds: value.timeout_seconds,
            force_exit_buffer_seconds: value.force_exit_buffer_seconds,
            created_at: value.created_at,
            status: value.status,
            market_ticker: value.market_ticker,
            client_order_id: value.client_order_id,
            exchange_order_id: value.exchange_order_id,
            fill_status: value.fill_status,
            submitted_quantity: value.submitted_quantity,
            accepted_quantity: value.accepted_quantity,
            filled_quantity: value.filled_quantity,
            cancelled_quantity: value.cancelled_quantity,
            rejected_quantity: value.rejected_quantity,
            avg_fill_price: value.avg_fill_price,
            terminal_outcome: value.terminal_outcome,
            last_transition_at: value.last_transition_at,
        }
    }
}

impl From<OpenTradeForExitRow> for OpenTradeForExit {
    fn from(value: OpenTradeForExitRow) -> Self {
        let _ = value.decision_id;
        Self {
            trade_id: value.id,
            market_id: value.market_id,
            market_family: value.market_family.into(),
            market_ticker: value.market_ticker,
            lane_key: value.lane_key,
            side: value.side,
            quantity: value.quantity,
            entry_price: value.entry_price,
            entry_fee: value.entry_fee,
            strategy_family: value.strategy_family.into(),
            mode: value.mode.into(),
            created_at: value.created_at,
            timeout_seconds: value.timeout_seconds,
            force_exit_buffer_seconds: value.force_exit_buffer_seconds,
            expires_at: value.expires_at,
            exit_exchange_order_id: value.exit_exchange_order_id,
            exit_client_order_id: value.exit_client_order_id,
            exit_fill_status: value.exit_fill_status,
        }
    }
}

impl From<OpenLiveTradeForReconciliationRow> for OpenLiveTradeForReconciliation {
    fn from(value: OpenLiveTradeForReconciliationRow) -> Self {
        Self {
            trade_id: value.id,
            market_family: value.market_family.into(),
            lane_key: value.lane_key,
            market_ticker: value.market_ticker,
            side: value.side,
            quantity: value.quantity,
            entry_price: value.entry_price,
            entry_fee: value.entry_fee,
            created_at: value.created_at,
            entry_exchange_order_id: value.entry_exchange_order_id,
            entry_client_order_id: value.entry_client_order_id,
            exit_exchange_order_id: value.exit_exchange_order_id,
            exit_client_order_id: value.exit_client_order_id,
        }
    }
}

impl From<ClosedTradeNotificationRow> for ClosedTradeNotification {
    fn from(value: ClosedTradeNotificationRow) -> Self {
        Self {
            trade_id: value.id,
            lane_key: value.lane_key,
            market_family: value.market_family.into(),
            strategy_family: value.strategy_family.into(),
            mode: value.mode.into(),
            market_ticker: value.market_ticker,
            market_title: value.market_title,
            side: value.side,
            status: value.status,
            close_reason: value.close_reason,
            quantity: value.quantity,
            entry_price: value.entry_price,
            exit_price: value.exit_price.unwrap_or_default(),
            realized_pnl: value.realized_pnl.unwrap_or_default(),
            closed_at: value.closed_at.unwrap_or_else(Utc::now),
        }
    }
}

impl From<OperatorActionEventRow> for OperatorActionEvent {
    fn from(value: OperatorActionEventRow) -> Self {
        Self {
            id: value.id,
            action: value.action,
            actor: value.actor,
            note: value.note,
            payload_json: value.payload_json,
            created_at: value.created_at,
        }
    }
}

impl From<TradeExitProgressRow> for TradeExitProgress {
    fn from(value: TradeExitProgressRow) -> Self {
        Self {
            closed_quantity: value.closed_quantity,
            exit_fee: value.exit_fee,
        }
    }
}

impl From<RecentTradeMetricsRow> for RecentTradeMetrics {
    fn from(value: RecentTradeMetricsRow) -> Self {
        Self {
            trade_count: value.trade_count,
            win_count: value.win_count,
            realized_pnl: value.realized_pnl,
        }
    }
}

impl From<WorkerStatusRow> for WorkerStatusCard {
    fn from(value: WorkerStatusRow) -> Self {
        Self {
            service: value.service,
            status: value.status,
            last_started_at: value.last_started_at,
            last_succeeded_at: value.last_succeeded_at,
            last_failed_at: value.last_failed_at,
            last_error: value.last_error,
            details_json: value.details_json,
            updated_at: value.updated_at,
        }
    }
}

impl From<ProbabilityCalibrationRow> for ProbabilityCalibrationCard {
    fn from(value: ProbabilityCalibrationRow) -> Self {
        Self {
            mean_error: value.mean_error,
            sample_count: value.sample_count,
        }
    }
}

impl TryFrom<TrainedModelArtifactRow> for TrainedModelArtifactCard {
    type Error = anyhow::Error;

    fn try_from(value: TrainedModelArtifactRow) -> Result<Self, Self::Error> {
        Ok(Self {
            model_name: value.model_name,
            market_family: value.market_family.into(),
            strategy_family: StrategyFamily::from(value.strategy_family),
            symbol: value.symbol,
            feature_version: value.feature_version,
            sample_count: value.sample_count,
            weights: serde_json::from_value(value.weights_json)?,
            metrics_json: value.metrics_json,
            updated_at: value.updated_at,
        })
    }
}

impl From<LiveExchangeSyncStatusRow> for LiveExchangeSyncSummary {
    fn from(value: LiveExchangeSyncStatusRow) -> Self {
        let _ = value.source;
        let _ = value.details_json;
        Self {
            synced_at: value.synced_at,
            positions_count: i64::from(value.positions_count),
            resting_orders_count: i64::from(value.resting_orders_count),
            recent_fills_count: i64::from(value.recent_fills_count),
            local_open_live_trades_count: i64::from(value.local_open_live_trades_count),
            status: value.status,
            issues: value
                .issues_json
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(ToOwned::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

impl From<OperatorControlStateRow> for OperatorControlState {
    fn from(value: OperatorControlStateRow) -> Self {
        Self {
            live_order_placement_enabled: value.live_order_placement_enabled,
            updated_by: value.updated_by,
            note: value.note,
            updated_at: value.updated_at,
        }
    }
}

impl From<LivePositionSyncRow> for LivePositionCard {
    fn from(value: LivePositionSyncRow) -> Self {
        Self {
            market_ticker: value.market_ticker,
            position_count: value.position_count,
            resting_order_count: value.resting_order_count,
            market_exposure: value.market_exposure,
            realized_pnl: value.realized_pnl,
            synced_at: value.synced_at,
        }
    }
}

impl From<LiveOrderSyncListRow> for LiveOrderCard {
    fn from(value: LiveOrderSyncListRow) -> Self {
        Self {
            order_id: value.order_id,
            client_order_id: value.client_order_id,
            market_ticker: value.market_ticker,
            action: value.action,
            side: value.side,
            status: value.status,
            count: value.count,
            fill_count: value.fill_count,
            remaining_count: value.remaining_count,
            created_time: value.created_time,
            synced_at: value.synced_at,
        }
    }
}

impl From<LiveFillSyncListRow> for LiveFillCard {
    fn from(value: LiveFillSyncListRow) -> Self {
        Self {
            fill_id: value.fill_id,
            order_id: value.order_id,
            client_order_id: value.client_order_id,
            market_ticker: value.market_ticker,
            action: value.action,
            side: value.side,
            count: value.count,
            fee_paid: value.fee_paid,
            created_time: value.created_time,
            synced_at: value.synced_at,
        }
    }
}

impl From<LiveIntentRow> for LiveIntentCard {
    fn from(value: LiveIntentRow) -> Self {
        Self {
            intent_id: value.intent_id,
            lane_key: value.lane_key,
            mode: value.mode.into(),
            status: value.status,
            last_error: value.last_error,
            market_ticker: value.market_ticker,
            side: value.side,
            client_order_id: value.client_order_id,
            exchange_order_id: value.exchange_order_id,
            fill_status: value.fill_status,
            created_at: value.created_at,
            last_transition_at: value.last_transition_at,
        }
    }
}

fn json_f64(value: &serde_json::Value, key: &str) -> f64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default()
}

fn json_optional_f64(value: &serde_json::Value, key: &str) -> Option<f64> {
    value.get(key).and_then(serde_json::Value::as_f64)
}

fn json_text(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn json_optional_text(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn json_i32(value: &serde_json::Value, key: &str) -> i32 {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_default() as i32
}

fn merge_json(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    match (base, extra) {
        (serde_json::Value::Object(mut base), serde_json::Value::Object(extra)) => {
            for (key, value) in extra {
                base.insert(key, value);
            }
            serde_json::Value::Object(base)
        }
        (base, serde_json::Value::Null) => base,
        (_, extra) => extra,
    }
}

fn parse_strategy_family(value: &str) -> StrategyFamily {
    match value {
        "directional_settlement" => StrategyFamily::DirectionalSettlement,
        "pre_settlement_scalp" => StrategyFamily::PreSettlementScalp,
        _ => StrategyFamily::Portfolio,
    }
}

fn parse_market_family(value: &str) -> MarketFamily {
    match value {
        "crypto" => MarketFamily::Crypto,
        "weather" => MarketFamily::Weather,
        _ => MarketFamily::All,
    }
}

fn parse_promotion_state(value: &str) -> PromotionState {
    match value {
        "paper_active" => PromotionState::PaperActive,
        "live_micro" => PromotionState::LiveMicro,
        "live_scaled" => PromotionState::LiveScaled,
        "quarantined" => PromotionState::Quarantined,
        _ => PromotionState::Shadow,
    }
}

#[derive(Default)]
struct ReplayExecutionQualityAggregate {
    as_of: Option<DateTime<Utc>>,
    replay_lane_count: i64,
    replay_trade_count: i64,
    replay_trade_weighted_edge_realization_ratio_diag: Option<f64>,
    replay_trade_weighted_fill_rate_diag: Option<f64>,
    replay_trade_weighted_slippage_bps_diag: Option<f64>,
    replay_sample_sufficient: bool,
}

#[derive(Default)]
struct LiveExecutionQualityAggregate {
    as_of: Option<DateTime<Utc>>,
    recent_live_terminal_intent_count: i64,
    recent_live_intents_with_fill_count: i64,
    recent_live_predicted_fill_sample_count: i64,
    recent_live_predicted_fill_probability_mean: Option<f64>,
    recent_live_filled_quantity_ratio: Option<f64>,
    recent_live_actual_fill_hit_rate: Option<f64>,
    live_sample_sufficient: bool,
}

#[derive(Default)]
struct LaneExecutionTruthAggregate {
    market_family: MarketFamily,
    lane_key: String,
    recent_live_terminal_intent_count: i64,
    recent_live_predicted_fill_sample_count: i64,
    recent_live_predicted_fill_probability_sum: f64,
    recent_live_submitted_quantity_total: f64,
    recent_live_filled_quantity_total: f64,
    recent_live_predicted_submitted_quantity_total: f64,
    recent_live_predicted_filled_quantity_total: f64,
    recent_live_predicted_filled_intent_count: i64,
    replay_trade_count: i64,
    replay_fill_rate_weighted_sum: f64,
    replay_edge_realization_weighted_sum: f64,
}

impl LaneExecutionTruthAggregate {
    fn from_live_row(row: &LiveLaneExecutionTruthRow) -> Self {
        let submitted = row.submitted_quantity.max(0.0);
        let filled = row.filled_quantity.clamp(0.0, submitted);
        let predicted_sample = if row.predicted_fill_probability.is_some() && submitted > 0.0 {
            1
        } else {
            0
        };
        let predicted_fill_probability_sum = row.predicted_fill_probability.unwrap_or_default();
        let predicted_submitted_total = if predicted_sample == 1 {
            submitted
        } else {
            0.0
        };
        let predicted_filled_total = if predicted_sample == 1 { filled } else { 0.0 };
        let predicted_filled_intent_count = if predicted_sample == 1 && filled > 0.0 {
            1
        } else {
            0
        };
        Self {
            market_family: MarketFamily::from(row.market_family),
            lane_key: row.lane_key.clone(),
            recent_live_terminal_intent_count: 1,
            recent_live_predicted_fill_sample_count: predicted_sample,
            recent_live_predicted_fill_probability_sum: predicted_fill_probability_sum,
            recent_live_submitted_quantity_total: submitted,
            recent_live_filled_quantity_total: filled,
            recent_live_predicted_submitted_quantity_total: predicted_submitted_total,
            recent_live_predicted_filled_quantity_total: predicted_filled_total,
            recent_live_predicted_filled_intent_count: predicted_filled_intent_count,
            ..Default::default()
        }
    }

    fn apply_live_row(&mut self, row: &LiveLaneExecutionTruthRow) {
        let submitted = row.submitted_quantity.max(0.0);
        let filled = row.filled_quantity.clamp(0.0, submitted);
        self.recent_live_terminal_intent_count += 1;
        self.recent_live_submitted_quantity_total += submitted;
        self.recent_live_filled_quantity_total += filled;
        if let Some(predicted_fill_probability) = row.predicted_fill_probability {
            if submitted > 0.0 {
                self.recent_live_predicted_fill_sample_count += 1;
                self.recent_live_predicted_fill_probability_sum += predicted_fill_probability;
                self.recent_live_predicted_submitted_quantity_total += submitted;
                self.recent_live_predicted_filled_quantity_total += filled;
                if filled > 0.0 {
                    self.recent_live_predicted_filled_intent_count += 1;
                }
            }
        }
    }

    fn attach_replay(&mut self, row: &ReplayLaneExecutionTruthRow) {
        let weight = i64::from(row.trade_count.max(0));
        if weight <= 0 {
            return;
        }
        self.replay_trade_count += weight;
        self.replay_fill_rate_weighted_sum += row.fill_rate * weight as f64;
        self.replay_edge_realization_weighted_sum += row.edge_realization_ratio * weight as f64;
    }
}

fn summarize_replay_execution_quality(
    rows: &[ReplayExecutionQualityRow],
) -> ReplayExecutionQualityAggregate {
    let replay_lane_count = rows.len() as i64;
    let replay_trade_count = rows
        .iter()
        .map(|row| i64::from(row.trade_count.max(0)))
        .sum::<i64>();
    let weighted_denominator = rows
        .iter()
        .map(|row| f64::from(row.trade_count.max(0)))
        .sum::<f64>();
    let trade_weighted_fill_rate =
        weighted_average_by_trade_count(rows.iter().map(|row| (row.fill_rate, row.trade_count)));
    let trade_weighted_slippage_bps =
        weighted_average_by_trade_count(rows.iter().map(|row| (row.slippage_bps, row.trade_count)));
    let trade_weighted_edge_realization_ratio = weighted_average_by_trade_count(
        rows.iter()
            .map(|row| (row.edge_realization_ratio, row.trade_count)),
    );
    ReplayExecutionQualityAggregate {
        as_of: rows.iter().map(|row| row.replay_as_of).max(),
        replay_lane_count,
        replay_trade_count,
        replay_trade_weighted_edge_realization_ratio_diag: trade_weighted_edge_realization_ratio,
        replay_trade_weighted_fill_rate_diag: trade_weighted_fill_rate,
        replay_trade_weighted_slippage_bps_diag: trade_weighted_slippage_bps,
        replay_sample_sufficient: replay_trade_count >= 50
            && replay_lane_count > 0
            && weighted_denominator > 0.0,
    }
}

fn summarize_live_execution_quality(
    rows: &[LiveExecutionQualityRow],
) -> LiveExecutionQualityAggregate {
    let recent_live_terminal_intent_count = rows.len() as i64;
    let recent_live_intents_with_fill_count =
        rows.iter().filter(|row| row.filled_quantity > 0.0).count() as i64;
    let predicted_rows = rows
        .iter()
        .filter(|row| row.predicted_fill_probability.is_some() && row.submitted_quantity > 0.0)
        .collect::<Vec<_>>();
    let recent_live_predicted_fill_sample_count = predicted_rows.len() as i64;
    let recent_live_predicted_fill_probability_mean = if predicted_rows.is_empty() {
        None
    } else {
        Some(
            predicted_rows
                .iter()
                .filter_map(|row| row.predicted_fill_probability)
                .sum::<f64>()
                / predicted_rows.len() as f64,
        )
    };
    let submitted_total = rows
        .iter()
        .map(|row| row.submitted_quantity.max(0.0))
        .sum::<f64>();
    let filled_total = rows
        .iter()
        .map(|row| {
            row.filled_quantity
                .clamp(0.0, row.submitted_quantity.max(0.0))
        })
        .sum::<f64>();
    let comparable_submitted_total = predicted_rows
        .iter()
        .map(|row| row.submitted_quantity.max(0.0))
        .sum::<f64>();
    let comparable_filled_ratio = if comparable_submitted_total > 0.0 {
        Some(
            predicted_rows
                .iter()
                .map(|row| {
                    row.filled_quantity
                        .clamp(0.0, row.submitted_quantity.max(0.0))
                })
                .sum::<f64>()
                / comparable_submitted_total,
        )
    } else {
        None
    };
    let recent_live_actual_fill_hit_rate = if predicted_rows.is_empty() {
        None
    } else {
        Some(
            predicted_rows
                .iter()
                .filter(|row| row.filled_quantity > 0.0)
                .count() as f64
                / predicted_rows.len() as f64,
        )
    };

    LiveExecutionQualityAggregate {
        as_of: rows.iter().map(|row| row.processed_at).max(),
        recent_live_terminal_intent_count,
        recent_live_intents_with_fill_count,
        recent_live_predicted_fill_sample_count,
        recent_live_predicted_fill_probability_mean,
        recent_live_filled_quantity_ratio: if submitted_total > 0.0 {
            Some(filled_total / submitted_total)
        } else {
            None
        },
        recent_live_actual_fill_hit_rate,
        live_sample_sufficient: recent_live_terminal_intent_count >= 20
            && recent_live_predicted_fill_sample_count >= 20
            && comparable_filled_ratio.unwrap_or_default() > 0.0,
    }
}

fn build_lane_execution_truth_summaries(
    live_rows: &[LiveLaneExecutionTruthRow],
    replay_rows: &[ReplayLaneExecutionTruthRow],
    lane_states: &[LaneState],
) -> Vec<LaneExecutionTruthSummary> {
    use std::collections::{BTreeMap, HashMap, HashSet};

    let mut aggregates: BTreeMap<String, LaneExecutionTruthAggregate> = BTreeMap::new();
    let lane_state_map = lane_states
        .iter()
        .map(|lane| (lane.lane_key.as_str(), lane))
        .collect::<HashMap<_, _>>();
    let tracked_lane_keys = lane_states
        .iter()
        .map(|lane| lane.lane_key.clone())
        .collect::<HashSet<_>>();
    for row in live_rows {
        aggregates
            .entry(row.lane_key.clone())
            .and_modify(|aggregate| aggregate.apply_live_row(row))
            .or_insert_with(|| LaneExecutionTruthAggregate::from_live_row(row));
    }

    for row in replay_rows {
        if !tracked_lane_keys.contains(&row.lane_key) && !aggregates.contains_key(&row.lane_key) {
            continue;
        }
        aggregates
            .entry(row.lane_key.clone())
            .and_modify(|aggregate| aggregate.attach_replay(row))
            .or_insert_with(|| {
                let mut aggregate = LaneExecutionTruthAggregate {
                    market_family: MarketFamily::from(row.market_family),
                    lane_key: row.lane_key.clone(),
                    ..Default::default()
                };
                aggregate.attach_replay(row);
                aggregate
            });
    }

    let mut summaries = aggregates
        .into_values()
        .map(|aggregate| {
            let lane_state = lane_state_map.get(aggregate.lane_key.as_str()).copied();
            lane_execution_truth_summary(aggregate, lane_state)
        })
        .collect::<Vec<_>>();

    summaries.sort_by(|left, right| {
        execution_truth_status_rank(&left.status)
            .cmp(&execution_truth_status_rank(&right.status))
            .then_with(|| {
                right
                    .recent_live_terminal_intent_count
                    .cmp(&left.recent_live_terminal_intent_count)
            })
            .then_with(|| left.lane_key.cmp(&right.lane_key))
    });
    summaries
}

fn lane_execution_truth_summary(
    aggregate: LaneExecutionTruthAggregate,
    lane_state: Option<&LaneState>,
) -> LaneExecutionTruthSummary {
    let predicted_fill_probability_mean = if aggregate.recent_live_predicted_fill_sample_count > 0 {
        Some(
            aggregate.recent_live_predicted_fill_probability_sum
                / aggregate.recent_live_predicted_fill_sample_count as f64,
        )
    } else {
        None
    };
    let filled_quantity_ratio = if aggregate.recent_live_submitted_quantity_total > 0.0 {
        Some(
            aggregate.recent_live_filled_quantity_total
                / aggregate.recent_live_submitted_quantity_total,
        )
    } else {
        None
    };
    let actual_fill_hit_rate = if aggregate.recent_live_predicted_fill_sample_count > 0 {
        Some(
            aggregate.recent_live_predicted_filled_intent_count as f64
                / aggregate.recent_live_predicted_fill_sample_count as f64,
        )
    } else {
        None
    };
    let replay_edge = if aggregate.replay_trade_count > 0 {
        Some(aggregate.replay_edge_realization_weighted_sum / aggregate.replay_trade_count as f64)
    } else {
        None
    };
    let replay_fill_rate = if aggregate.replay_trade_count > 0 {
        Some(aggregate.replay_fill_rate_weighted_sum / aggregate.replay_trade_count as f64)
    } else {
        None
    };
    let predicted_vs_realized_fill_gap = predicted_fill_probability_mean
        .zip(actual_fill_hit_rate)
        .map(|(predicted, actual)| actual - predicted);
    let live_vs_replay_fill_gap = replay_fill_rate
        .zip(actual_fill_hit_rate)
        .map(|(replay, live)| live - replay);
    let live_sample_sufficient = aggregate.recent_live_terminal_intent_count >= 5
        && aggregate.recent_live_predicted_fill_sample_count >= 5;
    let recommendation = lane_execution_truth_recommendation(
        live_sample_sufficient,
        lane_state.and_then(|lane| Some(lane.promotion_state)),
        filled_quantity_ratio,
        actual_fill_hit_rate,
        replay_edge,
        predicted_vs_realized_fill_gap,
        live_vs_replay_fill_gap,
    );

    LaneExecutionTruthSummary {
        lane_key: aggregate.lane_key,
        market_family: aggregate.market_family,
        mode: TradeMode::Live,
        promotion_state: lane_state.map(|lane| lane.promotion_state),
        current_champion_model: lane_state.and_then(|lane| lane.current_champion_model.clone()),
        recent_live_terminal_intent_count: aggregate.recent_live_terminal_intent_count,
        recent_live_predicted_fill_sample_count: aggregate.recent_live_predicted_fill_sample_count,
        recent_live_predicted_fill_probability_mean: predicted_fill_probability_mean,
        recent_live_filled_quantity_ratio: filled_quantity_ratio,
        recent_live_actual_fill_hit_rate: actual_fill_hit_rate,
        replay_trade_weighted_edge_realization_ratio_diag: replay_edge,
        predicted_vs_realized_fill_gap,
        live_vs_replay_fill_gap,
        status: recommendation.status,
        degrade_live_recommended: recommendation.degrade_live_recommended,
        block_promotion_recommended: recommendation.block_promotion_recommended,
        manual_reenable_required: recommendation.manual_reenable_required,
        recommended_size_multiplier: recommendation.recommended_size_multiplier,
        recommendation_reason: recommendation.recommendation_reason,
        live_sample_sufficient,
    }
}

fn summarize_family_execution_truth(
    lane_summaries: &[LaneExecutionTruthSummary],
) -> Vec<FamilyExecutionTruthSummary> {
    use std::collections::HashMap;

    #[derive(Default)]
    struct Aggregate {
        lane_count: i64,
        terminal_count: i64,
        predicted_count: i64,
        predicted_fill_probability_sum: f64,
        filled_quantity_ratio_weighted_sum: f64,
        filled_quantity_ratio_weight: i64,
        actual_fill_hit_rate_weighted_sum: f64,
        actual_fill_hit_rate_weight: i64,
        replay_edge_weighted_sum: f64,
        replay_edge_weight: i64,
        live_vs_replay_fill_gap_weighted_sum: f64,
        live_vs_replay_fill_gap_weight: i64,
        degraded_lane_count: i64,
        quarantine_candidate_count: i64,
        live_sample_sufficient_count: i64,
    }

    let mut grouped: HashMap<MarketFamily, Aggregate> = HashMap::new();
    for lane in lane_summaries {
        let aggregate = grouped.entry(lane.market_family).or_default();
        aggregate.lane_count += 1;
        aggregate.terminal_count += lane.recent_live_terminal_intent_count;
        aggregate.predicted_count += lane.recent_live_predicted_fill_sample_count;
        if let Some(predicted_mean) = lane.recent_live_predicted_fill_probability_mean {
            aggregate.predicted_fill_probability_sum +=
                predicted_mean * lane.recent_live_predicted_fill_sample_count as f64;
        }
        if let Some(filled_ratio) = lane.recent_live_filled_quantity_ratio {
            aggregate.filled_quantity_ratio_weighted_sum +=
                filled_ratio * lane.recent_live_terminal_intent_count as f64;
            aggregate.filled_quantity_ratio_weight += lane.recent_live_terminal_intent_count;
        }
        if let Some(actual_hit_rate) = lane.recent_live_actual_fill_hit_rate {
            aggregate.actual_fill_hit_rate_weighted_sum +=
                actual_hit_rate * lane.recent_live_predicted_fill_sample_count as f64;
            aggregate.actual_fill_hit_rate_weight += lane.recent_live_predicted_fill_sample_count;
        }
        if let Some(replay_edge) = lane.replay_trade_weighted_edge_realization_ratio_diag {
            let weight = lane.recent_live_terminal_intent_count.max(1);
            aggregate.replay_edge_weighted_sum += replay_edge * weight as f64;
            aggregate.replay_edge_weight += weight;
        }
        if let Some(gap) = lane.live_vs_replay_fill_gap {
            let weight = lane.recent_live_predicted_fill_sample_count.max(1);
            aggregate.live_vs_replay_fill_gap_weighted_sum += gap * weight as f64;
            aggregate.live_vs_replay_fill_gap_weight += weight;
        }
        if lane.degrade_live_recommended {
            aggregate.degraded_lane_count += 1;
        }
        if lane.status == "quarantine_candidate" {
            aggregate.quarantine_candidate_count += 1;
        }
        if lane.live_sample_sufficient {
            aggregate.live_sample_sufficient_count += 1;
        }
    }

    let mut summaries = grouped
        .into_iter()
        .map(|(market_family, aggregate)| {
            let predicted_mean = if aggregate.predicted_count > 0 {
                Some(aggregate.predicted_fill_probability_sum / aggregate.predicted_count as f64)
            } else {
                None
            };
            let filled_ratio = if aggregate.filled_quantity_ratio_weight > 0 {
                Some(
                    aggregate.filled_quantity_ratio_weighted_sum
                        / aggregate.filled_quantity_ratio_weight as f64,
                )
            } else {
                None
            };
            let actual_hit_rate = if aggregate.actual_fill_hit_rate_weight > 0 {
                Some(
                    aggregate.actual_fill_hit_rate_weighted_sum
                        / aggregate.actual_fill_hit_rate_weight as f64,
                )
            } else {
                None
            };
            let replay_edge = if aggregate.replay_edge_weight > 0 {
                Some(aggregate.replay_edge_weighted_sum / aggregate.replay_edge_weight as f64)
            } else {
                None
            };
            let live_vs_replay_fill_gap = if aggregate.live_vs_replay_fill_gap_weight > 0 {
                Some(
                    aggregate.live_vs_replay_fill_gap_weighted_sum
                        / aggregate.live_vs_replay_fill_gap_weight as f64,
                )
            } else {
                None
            };
            let status = if aggregate.quarantine_candidate_count > 0 {
                "quarantine_candidate"
            } else if aggregate.degraded_lane_count > 0 {
                "watch"
            } else if aggregate.live_sample_sufficient_count == 0 {
                "insufficient_sample"
            } else {
                "good"
            };
            FamilyExecutionTruthSummary {
                market_family,
                mode: TradeMode::Live,
                lane_count: aggregate.lane_count,
                recent_live_terminal_intent_count: aggregate.terminal_count,
                recent_live_predicted_fill_sample_count: aggregate.predicted_count,
                recent_live_predicted_fill_probability_mean: predicted_mean,
                recent_live_filled_quantity_ratio: filled_ratio,
                recent_live_actual_fill_hit_rate: actual_hit_rate,
                replay_trade_weighted_edge_realization_ratio_diag: replay_edge,
                live_vs_replay_fill_gap,
                status: status.to_string(),
                degraded_lane_count: aggregate.degraded_lane_count,
                quarantine_candidate_count: aggregate.quarantine_candidate_count,
                live_sample_sufficient: aggregate.live_sample_sufficient_count > 0,
            }
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        execution_truth_status_rank(&left.status)
            .cmp(&execution_truth_status_rank(&right.status))
            .then_with(|| {
                market_family_sort_key(left.market_family)
                    .cmp(market_family_sort_key(right.market_family))
            })
    });
    summaries
}

struct LaneExecutionTruthRecommendation {
    status: String,
    degrade_live_recommended: bool,
    block_promotion_recommended: bool,
    manual_reenable_required: bool,
    recommended_size_multiplier: Option<f64>,
    recommendation_reason: Option<String>,
}

fn lane_execution_truth_recommendation(
    live_sample_sufficient: bool,
    promotion_state: Option<PromotionState>,
    filled_quantity_ratio: Option<f64>,
    actual_fill_hit_rate: Option<f64>,
    replay_edge: Option<f64>,
    predicted_vs_realized_fill_gap: Option<f64>,
    live_vs_replay_fill_gap: Option<f64>,
) -> LaneExecutionTruthRecommendation {
    if !live_sample_sufficient {
        return LaneExecutionTruthRecommendation {
            status: "insufficient_sample".to_string(),
            degrade_live_recommended: false,
            block_promotion_recommended: false,
            manual_reenable_required: false,
            recommended_size_multiplier: None,
            recommendation_reason: Some("need_more_live_sample".to_string()),
        };
    }

    let actual_fill_hit_rate = actual_fill_hit_rate.unwrap_or_default();
    let filled_quantity_ratio = filled_quantity_ratio.unwrap_or_default();
    let predicted_gap = predicted_vs_realized_fill_gap.unwrap_or_default();
    let replay_gap = live_vs_replay_fill_gap.unwrap_or_default();
    let replay_edge = replay_edge.unwrap_or(0.0);

    if actual_fill_hit_rate < 0.25
        || filled_quantity_ratio < 0.35
        || predicted_gap < -0.30
        || replay_gap < -0.25
        || replay_edge < 0.0
    {
        let is_live_lane = matches!(
            promotion_state,
            Some(PromotionState::LiveMicro | PromotionState::LiveScaled)
        );
        return LaneExecutionTruthRecommendation {
            status: "quarantine_candidate".to_string(),
            degrade_live_recommended: true,
            block_promotion_recommended: true,
            manual_reenable_required: is_live_lane,
            recommended_size_multiplier: Some(0.25),
            recommendation_reason: Some(if replay_edge < 0.0 {
                "live_truth_invalidates_replay".to_string()
            } else if predicted_gap < -0.30 {
                "predicted_fills_overstated".to_string()
            } else if actual_fill_hit_rate < 0.25 {
                "live_fill_hit_rate_too_low".to_string()
            } else if filled_quantity_ratio < 0.35 {
                "filled_quantity_ratio_too_low".to_string()
            } else {
                "live_fill_vs_replay_gap_too_negative".to_string()
            }),
        };
    }

    if actual_fill_hit_rate < 0.50
        || filled_quantity_ratio < 0.55
        || predicted_gap < -0.15
        || replay_gap < -0.10
        || replay_edge < 0.5
    {
        return LaneExecutionTruthRecommendation {
            status: "watch".to_string(),
            degrade_live_recommended: true,
            block_promotion_recommended: true,
            manual_reenable_required: false,
            recommended_size_multiplier: Some(0.5),
            recommendation_reason: Some(if replay_edge < 0.5 {
                "replay_edge_diag_soft".to_string()
            } else if predicted_gap < -0.15 {
                "predicted_fill_gap_widening".to_string()
            } else if actual_fill_hit_rate < 0.50 {
                "live_fill_hit_rate_soft".to_string()
            } else if filled_quantity_ratio < 0.55 {
                "filled_quantity_ratio_soft".to_string()
            } else {
                "live_fill_vs_replay_gap_soft".to_string()
            }),
        };
    }

    LaneExecutionTruthRecommendation {
        status: "good".to_string(),
        degrade_live_recommended: false,
        block_promotion_recommended: false,
        manual_reenable_required: false,
        recommended_size_multiplier: Some(1.0),
        recommendation_reason: Some("live_truth_supports_lane".to_string()),
    }
}

fn execution_truth_status_rank(status: &str) -> i32 {
    match status {
        "quarantine_candidate" => 0,
        "watch" => 1,
        "good" => 2,
        _ => 3,
    }
}

fn market_family_sort_key(value: MarketFamily) -> &'static str {
    match value {
        MarketFamily::Crypto => "crypto",
        MarketFamily::Weather => "weather",
        MarketFamily::All => "all",
    }
}

fn weighted_average_by_trade_count<I>(items: I) -> Option<f64>
where
    I: Iterator<Item = (f64, i32)>,
{
    let mut weighted_sum = 0.0;
    let mut denominator = 0.0;
    for (value, trade_count) in items {
        let weight = f64::from(trade_count.max(0));
        if weight <= 0.0 {
            continue;
        }
        weighted_sum += value * weight;
        denominator += weight;
    }
    if denominator > 0.0 {
        Some(weighted_sum / denominator)
    } else {
        None
    }
}

fn live_trade_needs_attention(
    trade: &OpenLiveTradeForReconciliation,
    snapshot: &LiveTradeReconciliationSnapshot,
) -> bool {
    let trade_age_seconds = (chrono::Utc::now() - trade.created_at).num_seconds();
    trade_age_seconds >= 90
        && !snapshot.has_position
        && !snapshot.has_resting_order
        && snapshot.matched_exit_fill_quantity + 0.01 < trade.quantity
}

fn proportional_entry_fee(total_entry_fee: f64, exit_quantity: f64, total_quantity: f64) -> f64 {
    if total_entry_fee <= 0.0 || total_quantity <= 0.0 {
        0.0
    } else {
        (total_entry_fee * (exit_quantity / total_quantity)).max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LiveExecutionQualityRow, LiveLaneExecutionTruthRow, ReplayExecutionQualityRow,
        ReplayLaneExecutionTruthRow, SqlMarketFamily, build_lane_execution_truth_summaries,
        opportunity_card_freshness_window, summarize_family_execution_truth,
        summarize_live_execution_quality, summarize_replay_execution_quality,
    };
    use chrono::Utc;
    use common::{LaneState, MarketFamily, PromotionState};

    fn lane_state(lane_key: &str, promotion_state: PromotionState) -> LaneState {
        LaneState {
            lane_key: lane_key.to_string(),
            market_family: MarketFamily::Crypto,
            promotion_state,
            promotion_reason: Some("paper_ready".to_string()),
            recent_pnl: 0.0,
            recent_brier: 0.2,
            recent_execution_quality: 0.8,
            recent_replay_expectancy: 1.0,
            quarantine_reason: None,
            current_champion_model: Some("trained_linear_v1".to_string()),
        }
    }

    fn live_lane_row(
        lane_key: &str,
        predicted_fill_probability: Option<f64>,
        submitted_quantity: f64,
        filled_quantity: f64,
    ) -> LiveLaneExecutionTruthRow {
        LiveLaneExecutionTruthRow {
            market_family: SqlMarketFamily::Crypto,
            lane_key: lane_key.to_string(),
            predicted_fill_probability,
            submitted_quantity,
            filled_quantity,
        }
    }

    fn replay_lane_row(
        lane_key: &str,
        trade_count: i32,
        fill_rate: f64,
        edge_realization_ratio: f64,
    ) -> ReplayLaneExecutionTruthRow {
        ReplayLaneExecutionTruthRow {
            market_family: SqlMarketFamily::Crypto,
            lane_key: lane_key.to_string(),
            trade_count,
            fill_rate,
            edge_realization_ratio,
        }
    }

    #[test]
    fn opportunity_card_freshness_is_family_aware() {
        assert_eq!(
            opportunity_card_freshness_window(Some(MarketFamily::Crypto)).num_minutes(),
            10
        );
        assert_eq!(
            opportunity_card_freshness_window(Some(MarketFamily::Weather)).num_minutes(),
            30
        );
        assert_eq!(opportunity_card_freshness_window(None).num_minutes(), 20);
    }

    #[test]
    fn live_execution_summary_excludes_opened_and_preserves_null_forecasts() {
        let now = Utc::now();
        let rows = vec![
            LiveExecutionQualityRow {
                processed_at: now,
                predicted_fill_probability: Some(0.8),
                submitted_quantity: 10.0,
                filled_quantity: 10.0,
            },
            LiveExecutionQualityRow {
                processed_at: now,
                predicted_fill_probability: None,
                submitted_quantity: 5.0,
                filled_quantity: 0.0,
            },
        ];
        let summary = summarize_live_execution_quality(&rows);
        assert_eq!(summary.recent_live_terminal_intent_count, 2);
        assert_eq!(summary.recent_live_predicted_fill_sample_count, 1);
        assert_eq!(
            summary.recent_live_predicted_fill_probability_mean,
            Some(0.8)
        );
        assert_eq!(summary.recent_live_filled_quantity_ratio, Some(10.0 / 15.0));
        assert_eq!(summary.recent_live_actual_fill_hit_rate, Some(1.0));
    }

    #[test]
    fn live_execution_summary_handles_partial_fill_quantity_math() {
        let now = Utc::now();
        let rows = vec![
            LiveExecutionQualityRow {
                processed_at: now,
                predicted_fill_probability: Some(0.7),
                submitted_quantity: 10.0,
                filled_quantity: 3.0,
            },
            LiveExecutionQualityRow {
                processed_at: now,
                predicted_fill_probability: Some(0.6),
                submitted_quantity: 5.0,
                filled_quantity: 5.0,
            },
        ];
        let summary = summarize_live_execution_quality(&rows);
        assert_eq!(summary.recent_live_filled_quantity_ratio, Some(8.0 / 15.0));
        assert_eq!(summary.recent_live_actual_fill_hit_rate, Some(1.0));
    }

    #[test]
    fn replay_execution_summary_is_trade_weighted() {
        let now = Utc::now();
        let rows = vec![
            ReplayExecutionQualityRow {
                replay_as_of: now,
                trade_count: 100,
                fill_rate: 0.2,
                slippage_bps: 15.0,
                edge_realization_ratio: 0.5,
            },
            ReplayExecutionQualityRow {
                replay_as_of: now,
                trade_count: 1,
                fill_rate: 1.0,
                slippage_bps: 1.0,
                edge_realization_ratio: 2.0,
            },
        ];
        let summary = summarize_replay_execution_quality(&rows);
        let expected_fill_rate = ((0.2 * 100.0) + 1.0) / 101.0;
        let expected_edge_realization = ((0.5 * 100.0) + 2.0) / 101.0;
        assert_eq!(summary.replay_trade_count, 101);
        assert_eq!(
            summary.replay_trade_weighted_fill_rate_diag,
            Some(expected_fill_rate)
        );
        assert_eq!(
            summary.replay_trade_weighted_edge_realization_ratio_diag,
            Some(expected_edge_realization)
        );
    }

    #[test]
    fn live_execution_summary_requires_real_samples_for_confidence() {
        let now = Utc::now();
        let rows = (0..19)
            .map(|_| LiveExecutionQualityRow {
                processed_at: now,
                predicted_fill_probability: Some(0.7),
                submitted_quantity: 1.0,
                filled_quantity: 1.0,
            })
            .collect::<Vec<_>>();
        let summary = summarize_live_execution_quality(&rows);
        assert!(!summary.live_sample_sufficient);
    }

    #[test]
    fn live_execution_summary_tolerates_historical_rows_without_backfill() {
        let rows = vec![LiveExecutionQualityRow {
            processed_at: Utc::now(),
            predicted_fill_probability: None,
            submitted_quantity: 0.0,
            filled_quantity: 0.0,
        }];
        let summary = summarize_live_execution_quality(&rows);
        assert_eq!(summary.recent_live_terminal_intent_count, 1);
        assert_eq!(summary.recent_live_predicted_fill_probability_mean, None);
        assert_eq!(summary.recent_live_filled_quantity_ratio, None);
        assert!(!summary.live_sample_sufficient);
    }

    #[test]
    fn lane_execution_truth_marks_bad_live_lane_for_quarantine_candidate() {
        let lane = "kalshi:xrp:15:buy_no:directional_settlement:trained_linear_v1";
        let live_rows = (0..6)
            .map(|_| live_lane_row(lane, Some(0.8), 1.0, 0.0))
            .collect::<Vec<_>>();
        let replay_rows = vec![replay_lane_row(lane, 40, 0.7, 0.8)];
        let lane_states = vec![lane_state(lane, PromotionState::LiveMicro)];

        let summaries =
            build_lane_execution_truth_summaries(&live_rows, &replay_rows, &lane_states);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].status, "quarantine_candidate");
        assert!(summaries[0].degrade_live_recommended);
        assert!(summaries[0].block_promotion_recommended);
        assert!(summaries[0].manual_reenable_required);
        assert_eq!(summaries[0].recommended_size_multiplier, Some(0.25));
    }

    #[test]
    fn lane_execution_truth_ignores_replay_only_rows_without_tracking_scope() {
        let lane = "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1";
        let replay_rows = vec![replay_lane_row(lane, 100, 0.6, 0.9)];
        let summaries = build_lane_execution_truth_summaries(&[], &replay_rows, &[]);
        assert!(summaries.is_empty());
    }

    #[test]
    fn family_execution_truth_counts_degraded_lanes() {
        let bad_lane = "kalshi:sol:15:buy_no:directional_settlement:trained_linear_v1";
        let good_lane = "kalshi:xrp:15:buy_yes:directional_settlement:trained_linear_v1";
        let mut live_rows = (0..6)
            .map(|_| live_lane_row(bad_lane, Some(0.75), 1.0, 0.0))
            .collect::<Vec<_>>();
        live_rows.extend((0..6).map(|_| live_lane_row(good_lane, Some(0.65), 1.0, 1.0)));
        let replay_rows = vec![
            replay_lane_row(bad_lane, 40, 0.7, 0.4),
            replay_lane_row(good_lane, 50, 0.6, 0.9),
        ];
        let lane_states = vec![
            lane_state(bad_lane, PromotionState::PaperActive),
            lane_state(good_lane, PromotionState::PaperActive),
        ];

        let lane_summaries =
            build_lane_execution_truth_summaries(&live_rows, &replay_rows, &lane_states);
        let family_summaries = summarize_family_execution_truth(&lane_summaries);
        assert_eq!(family_summaries.len(), 1);
        assert_eq!(family_summaries[0].market_family, MarketFamily::Crypto);
        assert_eq!(family_summaries[0].degraded_lane_count, 1);
        assert_eq!(family_summaries[0].quarantine_candidate_count, 1);
        assert_eq!(family_summaries[0].status, "quarantine_candidate");
    }

    #[test]
    fn lane_execution_truth_hides_untracked_replay_only_challengers() {
        let active_lane = "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1";
        let challenger_lane =
            "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_contrarian_v1";
        let lane_states = vec![lane_state(active_lane, PromotionState::PaperActive)];
        let replay_rows = vec![
            replay_lane_row(active_lane, 100, 0.6, 0.8),
            replay_lane_row(challenger_lane, 100, 0.7, 1.1),
        ];

        let summaries = build_lane_execution_truth_summaries(&[], &replay_rows, &lane_states);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].lane_key, active_lane);
    }
}
