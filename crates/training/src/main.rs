use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use common::{
    AppConfig, ExpiryRegime, LaneState, MarketFamily, MarketFeatureSnapshotRecord, PromotionState,
    StrategyFamily, lane_key, lane_key_with_regime,
};
use market_models::{
    BASELINE_LOGIT_V1, FeatureVector, TRAINED_LINEAR_CONTRARIAN_V1, TRAINED_LINEAR_V1,
    TrainedLinearWeights, supported_models,
};
use replay::{
    BenchmarkLeaderboard, ReplayExample, ReplayPolicy, benchmark_model_set, benchmark_single_model,
};
use rusqlite::Connection;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use storage::{HistoricalReplayExampleInsert, ModelBenchmarkResultInsert, ModelBenchmarkRunInsert};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let storage = storage::Storage::connect(&config.database_url).await?;
    storage.migrate().await?;
    storage
        .upsert_worker_started("training", &serde_json::json!({}))
        .await?;

    info!("training_worker_started");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        interval.tick().await;
        match training_pass(&config, &storage).await {
            Ok(updated) => {
                storage
                    .upsert_worker_success(
                        "training",
                        &serde_json::json!({"updated_lane_count": updated}),
                    )
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("training", &error.to_string(), &serde_json::json!({}))
                    .await?;
                warn!(error = %error, "training_pass_failed");
            }
        }
    }
}

async fn training_pass(config: &AppConfig, storage: &storage::Storage) -> Result<usize> {
    let imported = backfill_historical_examples_from_v2(config, storage).await?;
    if imported > 0 {
        info!(imported, "historical_replay_examples_imported");
    }

    let mut snapshots = storage
        .list_recent_feature_snapshots(Some(MarketFamily::Crypto), None, 5000)
        .await?;
    snapshots.extend(
        storage
            .list_recent_feature_snapshots(Some(MarketFamily::Weather), None, 5000)
            .await?,
    );
    let mut by_symbol: BTreeMap<String, Vec<_>> = BTreeMap::new();
    let mut weather_by_symbol: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for snapshot in snapshots {
        match snapshot.market_family {
            MarketFamily::Weather => {
                weather_by_symbol
                    .entry(snapshot.symbol.to_lowercase())
                    .or_default()
                    .push(snapshot);
            }
            _ => {
                by_symbol
                    .entry(snapshot.symbol.to_lowercase())
                    .or_default()
                    .push(snapshot);
            }
        }
    }

    let mut updated = 0usize;
    for (symbol, snapshots) in by_symbol {
        let mut by_market: BTreeMap<i64, Vec<_>> = BTreeMap::new();
        for snapshot in snapshots {
            by_market
                .entry(snapshot.market_id)
                .or_default()
                .push(snapshot);
        }

        let mut examples: Vec<ReplayExample> = storage
            .list_historical_replay_examples(
                MarketFamily::Crypto,
                &symbol,
                StrategyFamily::DirectionalSettlement,
                100_000,
            )
            .await?
            .into_iter()
            .map(|example| ReplayExample {
                lane_key: example.lane_key,
                time_to_expiry_bucket: example.time_to_expiry_bucket,
                target_yes_probability: example.target_yes_probability,
                market_prob: example.market_prob,
                features: example.features,
            })
            .collect();
        let resolved_history_exists = !examples.is_empty();
        if !resolved_history_exists {
            for (_, mut market_snapshots) in by_market {
                market_snapshots.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                for pair in market_snapshots.windows(2) {
                    let current = &pair[0];
                    let next = &pair[1];
                    examples.push(ReplayExample {
                        lane_key: lane_key(
                            "kalshi",
                            &symbol,
                            current.window_minutes as u32,
                            "buy_yes",
                            StrategyFamily::DirectionalSettlement,
                            BASELINE_LOGIT_V1,
                        ),
                        time_to_expiry_bucket: current.time_to_expiry_bucket.clone(),
                        target_yes_probability: next.market_prob,
                        market_prob: current.market_prob,
                        features: feature_vector_from_snapshot(current),
                    });
                }
            }
        }
        if examples.len() < 4 {
            continue;
        }

        for regime in [ExpiryRegime::Early, ExpiryRegime::Mid, ExpiryRegime::Late] {
            let regime_examples = regime_examples(&examples, regime, &symbol);
            if regime_examples.len() < 4 {
                continue;
            }
            let regime_execution_quality = regime_examples
                .iter()
                .map(|example| example.features.venue_quality_score)
                .sum::<f64>()
                / regime_examples.len() as f64;

            let lane = lane_key_with_regime(
                "kalshi",
                &symbol,
                15,
                "buy_yes",
                StrategyFamily::DirectionalSettlement,
                BASELINE_LOGIT_V1,
                Some(regime),
            );
            let trained_weights = train_symbol_linear_weights(&regime_examples);
            let trained_policy = optimize_trained_policy(&lane, &regime_examples, &trained_weights);
            storage
                .upsert_trained_model_artifact(
                    TRAINED_LINEAR_V1,
                    MarketFamily::Crypto,
                    StrategyFamily::DirectionalSettlement,
                    Some(&symbol),
                    Some(regime),
                    "trained_linear_symbol_v1",
                    regime_examples.len() as i32,
                    &trained_weights,
                    &json!({
                        "symbol": symbol,
                        "expiry_regime": regime.as_key(),
                        "training_examples": regime_examples.len(),
                        "policy": {
                            "min_edge": trained_policy.min_edge,
                            "min_confidence": trained_policy.min_confidence,
                            "min_venue_quality": trained_policy.min_venue_quality,
                            "min_seconds_to_expiry_scaled": trained_policy.min_seconds_to_expiry_scaled,
                            "execution_cost": trained_policy.execution_cost
                        }
                    }),
                )
                .await?;
            let Some(leaderboard) = benchmark_model_set(
                &lane,
                supported_models(),
                &regime_examples,
                ReplayPolicy::directional_default(),
                Some(&trained_weights),
                Some(trained_policy),
            ) else {
                continue;
            };

            let Some(champion) = pick_champion(&leaderboard) else {
                continue;
            };
            let champion_lane = lane_key_with_regime(
                "kalshi",
                &symbol,
                15,
                "buy_yes",
                StrategyFamily::DirectionalSettlement,
                &champion.model_name,
                Some(regime),
            );
            persist_model_benchmark_run(
                storage,
                &champion_lane,
                &symbol,
                Some(regime),
                StrategyFamily::DirectionalSettlement,
                if resolved_history_exists {
                    "historical_resolved"
                } else {
                    "live_recent_snapshots"
                },
                regime_examples.len(),
                &leaderboard,
            )
            .await?;
            let paper_metrics = storage
                .recent_trade_metrics_for_lane(&champion_lane, common::TradeMode::Paper, 20)
                .await?;
            let live_metrics = storage
                .recent_trade_metrics_for_lane(&champion_lane, common::TradeMode::Live, 10)
                .await?;
            persist_calibration(
                storage,
                &symbol,
                StrategyFamily::DirectionalSettlement,
                &champion.model_name,
                &regime_examples,
                if uses_trained_linear_weights(&champion.model_name) {
                    Some(&trained_weights)
                } else {
                    None
                },
            )
            .await?;
            let base_promotion_state = classify_promotion_state(
                config,
                champion.brier,
                champion.execution_pnl,
                regime_execution_quality,
                paper_metrics.trade_count,
                paper_metrics.realized_pnl,
                paper_metrics.trade_count,
                paper_metrics.realized_pnl,
                live_metrics.trade_count,
                live_metrics.realized_pnl,
            );
            let promotion_state = if regime == ExpiryRegime::Late {
                PromotionState::Shadow
            } else {
                base_promotion_state
            };
            let promotion_reason = if regime == ExpiryRegime::Late {
                "late_regime_diagnostic_only".to_string()
            } else {
                promotion_reason(
                    config,
                    promotion_state,
                    champion.brier,
                    champion.execution_pnl,
                    paper_metrics.trade_count,
                    paper_metrics.realized_pnl,
                    paper_metrics.trade_count,
                    paper_metrics.realized_pnl,
                    live_metrics.trade_count,
                    live_metrics.realized_pnl,
                )
            };
            let quarantine_reason = if regime == ExpiryRegime::Late {
                Some("late_regime_diagnostic_only".to_string())
            } else {
                quarantine_reason(
                    config,
                    promotion_state,
                    champion.execution_pnl,
                    paper_metrics.trade_count,
                    paper_metrics.realized_pnl,
                )
            };
            storage
                .upsert_lane_state(&LaneState {
                    lane_key: champion_lane,
                    market_family: MarketFamily::Crypto,
                    promotion_state,
                    promotion_reason: Some(promotion_reason),
                    recent_pnl: paper_metrics.realized_pnl,
                    recent_brier: champion.brier,
                    recent_execution_quality: regime_execution_quality,
                    recent_replay_expectancy: champion.execution_pnl,
                    quarantine_reason,
                    current_champion_model: Some(champion.model_name.clone()),
                })
                .await?;
            updated += 1;
        }
    }

    for (symbol, snapshots) in weather_by_symbol {
        let latest = snapshots
            .iter()
            .max_by(|left, right| left.created_at.cmp(&right.created_at))
            .cloned();
        let Some(latest) = latest else {
            continue;
        };
        let promotion_state = if config.weather_paper_trading_enabled
            && latest.weather_forecast_temperature_f.is_some()
            && latest.weather_reference_confidence.unwrap_or_default() >= 0.25
        {
            PromotionState::PaperActive
        } else {
            PromotionState::Shadow
        };
        storage
            .upsert_lane_state(&LaneState {
                lane_key: lane_key(
                    "kalshi",
                    &symbol,
                    latest.window_minutes as u32,
                    "buy_yes",
                    StrategyFamily::DirectionalSettlement,
                    "weather_threshold_v1",
                ),
                market_family: MarketFamily::Weather,
                promotion_state,
                promotion_reason: Some(if matches!(promotion_state, PromotionState::PaperActive) {
                    "weather_paper_only_rollout".to_string()
                } else {
                    "weather_reference_not_ready".to_string()
                }),
                recent_pnl: 0.0,
                recent_brier: 0.0,
                recent_execution_quality: latest.venue_quality_score,
                recent_replay_expectancy: 0.0,
                quarantine_reason: None,
                current_champion_model: Some("weather_threshold_v1".to_string()),
            })
            .await?;
        updated += 1;
    }

    info!(updated, "training_pass_succeeded");
    Ok(updated)
}

async fn persist_model_benchmark_run(
    storage: &storage::Storage,
    lane_key: &str,
    symbol: &str,
    expiry_regime: Option<ExpiryRegime>,
    strategy_family: StrategyFamily,
    source: &str,
    example_count: usize,
    leaderboard: &BenchmarkLeaderboard,
) -> Result<()> {
    let champion = leaderboard.champion().map(|row| row.model_name.clone());
    let results = leaderboard
        .results
        .iter()
        .enumerate()
        .map(|(index, result)| ModelBenchmarkResultInsert {
            model_name: result.model_name.clone(),
            rank: index as i32 + 1,
            brier: result.brier,
            execution_pnl: result.execution_pnl,
            sample_count: result.sample_count as i32,
            trade_count: result.trade_count as i32,
            win_rate: result.win_rate,
            fill_rate: result.fill_rate,
            slippage_bps: result.slippage_bps,
            edge_realization_ratio: result.edge_realization_ratio,
        })
        .collect::<Vec<_>>();
    storage
        .insert_model_benchmark_run(
            &ModelBenchmarkRunInsert {
                lane_key: lane_key.to_string(),
                market_family: MarketFamily::Crypto,
                symbol: symbol.to_string(),
                expiry_regime,
                strategy_family,
                source: source.to_string(),
                example_count: example_count as i32,
                champion_model_name: champion,
                details_json: json!({
                    "registry_lane_key": leaderboard.lane_key,
                    "result_count": leaderboard.results.len(),
                }),
            },
            &results,
        )
        .await
}

async fn persist_calibration(
    storage: &storage::Storage,
    symbol: &str,
    strategy_family: StrategyFamily,
    model_name: &str,
    examples: &[ReplayExample],
    trained_weights: Option<&TrainedLinearWeights>,
) -> Result<()> {
    let mut grouped: BTreeMap<String, (f64, i32)> = BTreeMap::new();
    for example in examples {
        let output =
            market_models::run_model_with_weights(model_name, &example.features, trained_weights);
        let error = example.target_yes_probability - output.probability_yes;
        let entry = grouped
            .entry(example.time_to_expiry_bucket.clone())
            .or_insert((0.0, 0));
        entry.0 += error;
        entry.1 += 1;
    }

    for (expiry_bucket, (sum_error, sample_count)) in grouped {
        let mean_error = if sample_count > 0 {
            sum_error / f64::from(sample_count)
        } else {
            0.0
        };
        storage
            .upsert_probability_calibration(
                MarketFamily::Crypto,
                symbol,
                strategy_family,
                model_name,
                &expiry_bucket,
                mean_error,
                sample_count,
            )
            .await?;
    }
    Ok(())
}

fn train_symbol_linear_weights(examples: &[ReplayExample]) -> TrainedLinearWeights {
    let mut weights = TrainedLinearWeights {
        bias: 0.0,
        market_prob: -0.4,
        reference_yes_prob: 0.6,
        reference_gap_bps_scaled: 0.15,
        threshold_distance_bps_scaled: 0.12,
        distance_to_strike_bps_scaled: 0.22,
        reference_velocity_scaled: 0.10,
        realized_vol_short_scaled: 0.06,
        time_decay_factor: 0.04,
        order_book_imbalance: 0.10,
        aggressive_buy_ratio: 0.08,
        size_ahead_scaled: -0.06,
        trade_rate_scaled: 0.10,
        quote_churn_scaled: -0.05,
        spread_bps_scaled: -0.10,
        venue_quality_score: 0.05,
        market_data_age_scaled: -0.05,
        reference_age_scaled: -0.04,
        averaging_window_progress: 0.06,
        seconds_to_expiry_scaled: 0.04,
    };
    let learning_rate = 0.03;
    let regularization = 0.0005;
    let epochs = 6;

    for _ in 0..epochs {
        for example in examples {
            let target = example.target_yes_probability.clamp(0.0, 1.0);
            let score = linear_score(&weights, &example.features);
            let prediction = 1.0 / (1.0 + (-score).exp());
            let error = target - prediction;

            weights.bias += learning_rate * error;
            weights.market_prob += learning_rate
                * ((error * example.features.market_prob) - (regularization * weights.market_prob));
            weights.reference_yes_prob += learning_rate
                * ((error * example.features.reference_yes_prob)
                    - (regularization * weights.reference_yes_prob));
            weights.reference_gap_bps_scaled += learning_rate
                * ((error * example.features.reference_gap_bps_scaled)
                    - (regularization * weights.reference_gap_bps_scaled));
            weights.threshold_distance_bps_scaled += learning_rate
                * ((error * example.features.threshold_distance_bps_scaled)
                    - (regularization * weights.threshold_distance_bps_scaled));
            weights.distance_to_strike_bps_scaled += learning_rate
                * ((error * example.features.distance_to_strike_bps_scaled)
                    - (regularization * weights.distance_to_strike_bps_scaled));
            weights.reference_velocity_scaled += learning_rate
                * ((error * example.features.reference_velocity_scaled)
                    - (regularization * weights.reference_velocity_scaled));
            weights.realized_vol_short_scaled += learning_rate
                * ((error * example.features.realized_vol_short_scaled)
                    - (regularization * weights.realized_vol_short_scaled));
            weights.time_decay_factor += learning_rate
                * ((error * example.features.time_decay_factor)
                    - (regularization * weights.time_decay_factor));
            weights.order_book_imbalance += learning_rate
                * ((error * example.features.order_book_imbalance)
                    - (regularization * weights.order_book_imbalance));
            weights.aggressive_buy_ratio += learning_rate
                * ((error * example.features.aggressive_buy_ratio)
                    - (regularization * weights.aggressive_buy_ratio));
            weights.size_ahead_scaled += learning_rate
                * ((error * example.features.size_ahead_scaled)
                    - (regularization * weights.size_ahead_scaled));
            weights.trade_rate_scaled += learning_rate
                * ((error * example.features.trade_rate_scaled)
                    - (regularization * weights.trade_rate_scaled));
            weights.quote_churn_scaled += learning_rate
                * ((error * example.features.quote_churn_scaled)
                    - (regularization * weights.quote_churn_scaled));
            weights.spread_bps_scaled += learning_rate
                * ((error * example.features.spread_bps_scaled)
                    - (regularization * weights.spread_bps_scaled));
            weights.venue_quality_score += learning_rate
                * ((error * example.features.venue_quality_score)
                    - (regularization * weights.venue_quality_score));
            weights.market_data_age_scaled += learning_rate
                * ((error * example.features.market_data_age_scaled)
                    - (regularization * weights.market_data_age_scaled));
            weights.reference_age_scaled += learning_rate
                * ((error * example.features.reference_age_scaled)
                    - (regularization * weights.reference_age_scaled));
            weights.averaging_window_progress += learning_rate
                * ((error * example.features.averaging_window_progress)
                    - (regularization * weights.averaging_window_progress));
            weights.seconds_to_expiry_scaled += learning_rate
                * ((error * example.features.seconds_to_expiry_scaled)
                    - (regularization * weights.seconds_to_expiry_scaled));
        }
    }

    weights
}

fn optimize_trained_policy(
    lane_key: &str,
    examples: &[ReplayExample],
    trained_weights: &TrainedLinearWeights,
) -> ReplayPolicy {
    let default_policy = ReplayPolicy::directional_default();
    let min_edges = [0.04, 0.08, 0.12, 0.16, 0.20];
    let min_confidences = [0.50, 0.66, 0.80, 0.92];
    let min_qualities = [0.20, 0.40, 0.60];
    let min_seconds_to_expiry = [120.0 / 900.0, 270.0 / 900.0, 450.0 / 900.0];
    let mut best_policy = default_policy;
    let mut best_score = f64::NEG_INFINITY;
    let mut best_trade_count = usize::MAX;
    let mut best_brier = f64::INFINITY;

    for min_edge in min_edges {
        for min_confidence in min_confidences {
            for min_venue_quality in min_qualities {
                for min_seconds_to_expiry_scaled in min_seconds_to_expiry {
                    let policy = ReplayPolicy {
                        min_edge,
                        min_confidence,
                        min_venue_quality,
                        min_seconds_to_expiry_scaled,
                        execution_cost: default_policy.execution_cost,
                    };
                    let Some(result) = benchmark_single_model(
                        lane_key,
                        TRAINED_LINEAR_V1,
                        examples,
                        policy,
                        Some(trained_weights),
                    ) else {
                        continue;
                    };
                    let replay_score = if result.execution_pnl >= 0.0 {
                        result.execution_pnl - (result.trade_count as f64 * 0.0015)
                    } else {
                        (result.execution_pnl * 4.0) - (result.trade_count as f64 * 0.01)
                    };
                    let better = replay_score > best_score
                        || ((replay_score - best_score).abs() < f64::EPSILON
                            && result.execution_pnl > 0.0
                            && result.trade_count < best_trade_count)
                        || ((replay_score - best_score).abs() < f64::EPSILON
                            && result.trade_count == best_trade_count
                            && result.brier < best_brier);
                    if better {
                        best_policy = policy;
                        best_score = replay_score;
                        best_trade_count = result.trade_count;
                        best_brier = result.brier;
                    }
                }
            }
        }
    }

    let abstain_policy = ReplayPolicy {
        min_edge: 0.35,
        min_confidence: 0.97,
        min_venue_quality: 0.95,
        min_seconds_to_expiry_scaled: 0.80,
        execution_cost: default_policy.execution_cost,
    };
    if let Some(result) = benchmark_single_model(
        lane_key,
        TRAINED_LINEAR_V1,
        examples,
        abstain_policy,
        Some(trained_weights),
    ) {
        let replay_score = if result.execution_pnl >= 0.0 {
            result.execution_pnl - (result.trade_count as f64 * 0.0015)
        } else {
            (result.execution_pnl * 4.0) - (result.trade_count as f64 * 0.01)
        };
        let better = replay_score > best_score
            || ((replay_score - best_score).abs() < f64::EPSILON
                && result.trade_count < best_trade_count)
            || ((replay_score - best_score).abs() < f64::EPSILON
                && result.trade_count == best_trade_count
                && result.brier < best_brier);
        if better {
            best_policy = abstain_policy;
        }
    }

    best_policy
}

fn uses_trained_linear_weights(model_name: &str) -> bool {
    matches!(model_name, TRAINED_LINEAR_V1 | TRAINED_LINEAR_CONTRARIAN_V1)
}

fn regime_examples(
    examples: &[ReplayExample],
    regime: ExpiryRegime,
    symbol: &str,
) -> Vec<ReplayExample> {
    let lane = lane_key_with_regime(
        "kalshi",
        symbol,
        15,
        "buy_yes",
        StrategyFamily::DirectionalSettlement,
        BASELINE_LOGIT_V1,
        Some(regime),
    );
    examples
        .iter()
        .filter(|example| matches_regime_bucket(&example.time_to_expiry_bucket, regime))
        .cloned()
        .map(|mut example| {
            example.lane_key = lane.clone();
            example
        })
        .collect()
}

fn matches_regime_bucket(bucket: &str, regime: ExpiryRegime) -> bool {
    match regime {
        ExpiryRegime::Early => matches!(bucket, "5_to_10m" | "10m_plus"),
        ExpiryRegime::Mid => bucket == "2_to_5m",
        ExpiryRegime::Late => bucket == "under_2m",
    }
}

fn linear_score(weights: &TrainedLinearWeights, features: &FeatureVector) -> f64 {
    weights.bias
        + (weights.market_prob * features.market_prob)
        + (weights.reference_yes_prob * features.reference_yes_prob)
        + (weights.reference_gap_bps_scaled * features.reference_gap_bps_scaled)
        + (weights.threshold_distance_bps_scaled * features.threshold_distance_bps_scaled)
        + (weights.distance_to_strike_bps_scaled * features.distance_to_strike_bps_scaled)
        + (weights.reference_velocity_scaled * features.reference_velocity_scaled)
        + (weights.realized_vol_short_scaled * features.realized_vol_short_scaled)
        + (weights.time_decay_factor * features.time_decay_factor)
        + (weights.order_book_imbalance * features.order_book_imbalance)
        + (weights.aggressive_buy_ratio * features.aggressive_buy_ratio)
        + (weights.size_ahead_scaled * features.size_ahead_scaled)
        + (weights.trade_rate_scaled * features.trade_rate_scaled)
        + (weights.quote_churn_scaled * features.quote_churn_scaled)
        + (weights.spread_bps_scaled * features.spread_bps_scaled)
        + (weights.venue_quality_score * features.venue_quality_score)
        + (weights.market_data_age_scaled * features.market_data_age_scaled)
        + (weights.reference_age_scaled * features.reference_age_scaled)
        + (weights.averaging_window_progress * features.averaging_window_progress)
        + (weights.seconds_to_expiry_scaled * features.seconds_to_expiry_scaled)
}

fn feature_vector_from_snapshot(snapshot: &MarketFeatureSnapshotRecord) -> FeatureVector {
    FeatureVector {
        market_prob: snapshot.market_prob,
        reference_yes_prob: snapshot.reference_yes_prob,
        reference_gap_bps_scaled: (snapshot.reference_gap_bps / 1_000.0).clamp(-1.5, 1.5),
        threshold_distance_bps_scaled: (snapshot.threshold_distance_bps_proxy / 500.0)
            .clamp(-1.5, 1.5),
        distance_to_strike_bps_scaled: (snapshot.distance_to_strike_bps / 500.0).clamp(-2.5, 2.5),
        reference_velocity_scaled: (snapshot.reference_velocity / 25.0).clamp(-2.0, 2.0),
        realized_vol_short_scaled: (snapshot.realized_vol_short / 50.0).clamp(0.0, 2.0),
        time_decay_factor: snapshot.time_decay_factor.clamp(0.0, 1.0),
        order_book_imbalance: snapshot.order_book_imbalance,
        aggressive_buy_ratio: snapshot.aggressive_buy_ratio,
        size_ahead_scaled: (snapshot.size_ahead / 100.0).clamp(0.0, 3.0),
        trade_rate_scaled: (snapshot.trade_rate / 50.0).clamp(0.0, 3.0),
        quote_churn_scaled: snapshot.quote_churn.clamp(0.0, 3.0),
        size_ahead_real_scaled: (snapshot.size_ahead_real / 100.0).clamp(0.0, 5.0),
        trade_consumption_rate_scaled: (snapshot.trade_consumption_rate / 25.0).clamp(0.0, 5.0),
        cancel_rate_scaled: (snapshot.cancel_rate / 25.0).clamp(0.0, 5.0),
        queue_decay_rate_scaled: (snapshot.queue_decay_rate / 25.0).clamp(0.0, 5.0),
        spread_bps_scaled: (snapshot.spread_bps / 1_000.0).clamp(0.0, 2.0),
        venue_quality_score: snapshot.venue_quality_score,
        market_data_age_scaled: (snapshot.market_data_age_seconds as f64 / 20.0).clamp(0.0, 1.5),
        reference_age_scaled: (snapshot.reference_age_seconds as f64 / 20.0).clamp(0.0, 1.5),
        averaging_window_progress: snapshot.averaging_window_progress.clamp(0.0, 1.0),
        seconds_to_expiry_scaled: (snapshot.seconds_to_expiry as f64 / 900.0).clamp(0.0, 1.0),
    }
}

async fn backfill_historical_examples_from_v2(
    config: &AppConfig,
    storage: &storage::Storage,
) -> Result<usize> {
    let existing_count = storage.historical_replay_example_count().await?;
    let Some(path) = config.v2_reference_sqlite_path.clone() else {
        warn!("historical_replay_examples_skipped_missing_config");
        return Ok(0);
    };
    let sqlite_path = PathBuf::from(path);
    if !sqlite_path.exists() {
        warn!(path = %sqlite_path.display(), "historical_replay_examples_skipped_missing_file");
        return Ok(0);
    }

    let batch_size = config.historical_import_batch_size.max(500) as usize;
    info!(
        path = %sqlite_path.display(),
        existing_count,
        batch_size,
        "historical_replay_examples_import_started"
    );
    let offset = existing_count as usize;
    let path = sqlite_path.clone();
    let batch: Vec<HistoricalReplayExampleInsert> =
        tokio::task::spawn_blocking(move || load_v2_historical_batch(path, offset, batch_size))
            .await??;
    if batch.is_empty() {
        if existing_count > 0 {
            info!(existing_count, "historical_replay_examples_fully_loaded");
        }
        return Ok(0);
    }
    let inserted = storage.insert_historical_replay_examples(&batch).await?;
    info!(
        offset,
        batch_size,
        inserted,
        imported = existing_count + inserted as i64,
        "historical_replay_examples_batch_loaded"
    );
    Ok(inserted)
}

fn load_v2_historical_batch(
    sqlite_path: PathBuf,
    offset: usize,
    limit: usize,
) -> Result<Vec<HistoricalReplayExampleInsert>> {
    let connection = Connection::open(sqlite_path)?;
    let mut statement = connection.prepare(
        r#"
        SELECT
            s.id,
            s.market_id,
            s.market_slug,
            s.market_prob,
            s.recorded_at,
            s.end_date,
            s.microstructure_json,
            m.settled_yes_probability
        FROM market_state_snapshots s
        JOIN markets m ON m.id = s.market_id
        WHERE s.exchange = 'kalshi'
          AND m.settled_yes_probability IS NOT NULL
        ORDER BY s.recorded_at ASC, s.id ASC
        LIMIT ?1 OFFSET ?2
        "#,
    )?;

    let rows = statement.query_map([limit as i64, offset as i64], |row| {
        let recorded_at_raw: String = row.get("recorded_at")?;
        let end_date_raw: Option<String> = row.get("end_date")?;
        let market_slug: String = row.get("market_slug")?;
        let market_id: i64 = row.get("market_id")?;
        let snapshot_id: i64 = row.get("id")?;
        let market_prob: f64 = row.get::<_, Option<f64>>("market_prob")?.unwrap_or_default();
        let resolved_yes_probability = row
            .get::<_, Option<f64>>("settled_yes_probability")?
            .unwrap_or_default();
        let micro_raw: Option<String> = row.get("microstructure_json")?;
        let micro = micro_raw
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .unwrap_or_else(|| json!({}));
        let recorded_at = parse_v2_datetime(&recorded_at_raw)
            .ok_or_else(|| rusqlite::Error::InvalidColumnType(0, "recorded_at".into(), rusqlite::types::Type::Text))?;
        let symbol = parse_symbol(&market_slug, &micro);
        let seconds_to_expiry = end_date_raw
            .as_deref()
            .and_then(parse_v2_datetime)
            .map(|end_date| (end_date - recorded_at).num_seconds().max(0) as i32)
            .unwrap_or(900);
        let reference_yes_prob = micro
            .get("reference_yes_prob")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(market_prob);
        let reference_gap_bps = (reference_yes_prob - market_prob) * 10_000.0;
        let feature_age_seconds = micro
            .get("feature_age_seconds")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let reference_age_seconds = micro
            .get("coinbase_updated_at")
            .and_then(serde_json::Value::as_str)
            .and_then(parse_v2_datetime)
            .map(|updated| (recorded_at - updated).num_seconds().max(0) as f64)
            .unwrap_or(feature_age_seconds);
        let averaging_window_progress = if seconds_to_expiry <= 60 {
            ((60 - seconds_to_expiry) as f64 / 60.0).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let reference_velocity = micro
            .get("reference_price")
            .and_then(serde_json::Value::as_f64)
            .zip(
                micro.get("previous_reference_price")
                    .and_then(serde_json::Value::as_f64),
            )
            .map(|(current, previous)| (current - previous) / 5.0)
            .unwrap_or_default();
        let realized_vol_short = reference_velocity.abs() * 5.0;
        let spread_bps = micro
            .get("spread_bps")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(120.0);
        let distance_to_strike_bps = (((market_prob - 0.5) * 10_000.0) * 0.8).clamp(-3_000.0, 3_000.0);
        let time_decay_factor = (-0.0025 * seconds_to_expiry as f64).exp().clamp(0.0, 1.0);
        let size_ahead = micro
            .get("ask_size")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(25.0);
        let trade_rate = micro
            .get("trade_rate")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(12.0);
        let quote_churn = micro
            .get("quote_churn")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.2);
        let feature_vector_json = json!({
            "market_prob": market_prob,
            "reference_yes_prob": reference_yes_prob,
            "reference_gap_bps_scaled": (reference_gap_bps / 1_000.0).clamp(-1.5, 1.5),
            "threshold_distance_bps_scaled": (((market_prob - 0.5) * 10_000.0) / 500.0).clamp(-1.5, 1.5),
            "distance_to_strike_bps_scaled": (distance_to_strike_bps / 500.0).clamp(-2.5, 2.5),
            "reference_velocity_scaled": (reference_velocity / 25.0).clamp(-2.0, 2.0),
            "realized_vol_short_scaled": (realized_vol_short / 50.0).clamp(0.0, 2.0),
            "time_decay_factor": time_decay_factor,
            "order_book_imbalance": 0.0,
            "aggressive_buy_ratio": 0.5,
            "size_ahead_scaled": (size_ahead / 100.0).clamp(0.0, 3.0),
            "trade_rate_scaled": (trade_rate / 50.0).clamp(0.0, 3.0),
            "quote_churn_scaled": quote_churn.clamp(0.0, 3.0),
            "size_ahead_real_scaled": (size_ahead / 100.0).clamp(0.0, 5.0),
            "trade_consumption_rate_scaled": (trade_rate / 25.0).clamp(0.0, 5.0),
            "cancel_rate_scaled": (quote_churn / 2.0).clamp(0.0, 5.0),
            "queue_decay_rate_scaled": quote_churn.clamp(0.0, 5.0),
            "spread_bps_scaled": (spread_bps / 1_000.0).clamp(0.0, 2.0),
            "venue_quality_score": (1.0 - (feature_age_seconds / 60.0)).clamp(0.2, 0.9),
            "market_data_age_scaled": (feature_age_seconds / 20.0).clamp(0.0, 1.5),
            "reference_age_scaled": (reference_age_seconds / 20.0).clamp(0.0, 1.5),
            "averaging_window_progress": averaging_window_progress,
            "seconds_to_expiry_scaled": (seconds_to_expiry as f64 / 900.0).clamp(0.0, 1.0),
        });
        Ok(HistoricalReplayExampleInsert {
            source: "v2_sqlite".to_string(),
            source_key: format!("v2_sqlite:kalshi:{snapshot_id}"),
            market_family: MarketFamily::Crypto,
            exchange: "kalshi".to_string(),
            symbol,
            strategy_family: StrategyFamily::DirectionalSettlement,
            market_id: Some(market_id),
            market_slug: market_slug.clone(),
            recorded_at,
            resolved_yes_probability,
            market_prob,
            time_to_expiry_bucket: time_to_expiry_bucket(seconds_to_expiry),
            feature_version: "v2_sqlite_import_v1".to_string(),
            feature_vector_json,
            metadata_json: json!({
                "market_slug": market_slug,
                "source_snapshot_id": snapshot_id,
                "feature_age_seconds": feature_age_seconds,
                "reference_yes_prob": reference_yes_prob,
            }),
        })
    })?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn parse_v2_datetime(raw: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn parse_symbol(market_slug: &str, micro: &serde_json::Value) -> String {
    if let Some(product_id) = micro.get("product_id").and_then(serde_json::Value::as_str) {
        if let Some((symbol, _)) = product_id.split_once('-') {
            return symbol.to_lowercase();
        }
    }
    let slug = market_slug.to_ascii_lowercase();
    if slug.contains("btc") {
        "btc".to_string()
    } else if slug.contains("eth") {
        "eth".to_string()
    } else if slug.contains("sol") {
        "sol".to_string()
    } else if slug.contains("xrp") {
        "xrp".to_string()
    } else {
        "unknown".to_string()
    }
}

fn time_to_expiry_bucket(seconds_to_expiry: i32) -> String {
    if seconds_to_expiry < 120 {
        "under_2m".to_string()
    } else if seconds_to_expiry < 300 {
        "2_to_5m".to_string()
    } else if seconds_to_expiry < 600 {
        "5_to_10m".to_string()
    } else {
        "10m_plus".to_string()
    }
}

fn pick_champion(leaderboard: &BenchmarkLeaderboard) -> Option<&replay::ModelBenchmark> {
    leaderboard.champion()
}

fn classify_promotion_state(
    config: &AppConfig,
    champion_brier: f64,
    champion_pnl: f64,
    execution_quality: f64,
    paper_trade_count: i64,
    paper_realized_pnl: f64,
    symbol_paper_trade_count: i64,
    symbol_paper_realized_pnl: f64,
    live_trade_count: i64,
    live_realized_pnl: f64,
) -> PromotionState {
    let replay_is_usable = champion_brier <= config.paper_promotion_max_brier
        && champion_pnl >= config.paper_promotion_max_negative_replay
        && execution_quality >= config.min_venue_quality;
    let paper_behavior_is_ok =
        paper_trade_count == 0 || paper_realized_pnl >= config.paper_promotion_max_negative_pnl;
    let live_behavior_is_ok =
        live_trade_count == 0 || live_realized_pnl >= config.live_demote_max_negative_pnl;

    if paper_trade_count >= 3 && paper_realized_pnl < config.paper_promotion_max_negative_pnl {
        return PromotionState::Quarantined;
    }
    if live_trade_count >= 1 && live_realized_pnl < config.live_demote_max_negative_pnl {
        return PromotionState::PaperActive;
    }

    if replay_is_usable
        && live_behavior_is_ok
        && live_trade_count >= i64::from(config.live_scaled_min_examples)
        && live_realized_pnl >= config.live_scaled_min_pnl
        && champion_brier <= config.live_scaled_max_brier
    {
        return PromotionState::LiveScaled;
    }

    let symbol_fallback_examples = i64::from((config.live_micro_min_examples / 2).max(4));
    let symbol_fallback_pnl = (config.live_micro_min_pnl / 2.0).max(5.0);
    let fast_ramp_symbol_ready = paper_trade_count == 0
        && symbol_paper_trade_count >= symbol_fallback_examples
        && symbol_paper_realized_pnl >= symbol_fallback_pnl;

    if replay_is_usable
        && live_behavior_is_ok
        && champion_brier <= config.live_micro_max_brier
        && ((paper_trade_count >= i64::from(config.live_micro_min_examples)
            && paper_realized_pnl >= config.live_micro_min_pnl)
            || fast_ramp_symbol_ready)
    {
        return PromotionState::LiveMicro;
    }

    if replay_is_usable && paper_behavior_is_ok {
        return PromotionState::PaperActive;
    }

    PromotionState::Shadow
}

fn quarantine_reason(
    config: &AppConfig,
    promotion_state: PromotionState,
    champion_pnl: f64,
    paper_trade_count: i64,
    paper_realized_pnl: f64,
) -> Option<String> {
    if promotion_state != PromotionState::Quarantined {
        return None;
    }
    if paper_trade_count >= 3 && paper_realized_pnl < config.paper_promotion_max_negative_pnl {
        return Some("negative_recent_paper_pnl".to_string());
    }
    if champion_pnl < 0.0 {
        return Some("negative_recent_replay".to_string());
    }
    Some("lane_quarantined".to_string())
}

fn promotion_reason(
    config: &AppConfig,
    promotion_state: PromotionState,
    champion_brier: f64,
    champion_pnl: f64,
    paper_trade_count: i64,
    paper_realized_pnl: f64,
    symbol_paper_trade_count: i64,
    symbol_paper_realized_pnl: f64,
    live_trade_count: i64,
    live_realized_pnl: f64,
) -> String {
    match promotion_state {
        PromotionState::LiveScaled => "live_micro_passed_fast_ramp".to_string(),
        PromotionState::LiveMicro => {
            let symbol_fallback_examples = i64::from((config.live_micro_min_examples / 2).max(4));
            let symbol_fallback_pnl = (config.live_micro_min_pnl / 2.0).max(5.0);
            if paper_trade_count == 0
                && symbol_paper_trade_count >= symbol_fallback_examples
                && symbol_paper_realized_pnl >= symbol_fallback_pnl
            {
                "symbol_fast_ramp_ready_for_live_micro".to_string()
            } else {
                "paper_ready_for_live_micro".to_string()
            }
        }
        PromotionState::PaperActive => {
            if live_trade_count >= 1 && live_realized_pnl < 0.0 {
                "live_demoted_to_paper_active".to_string()
            } else {
                "paper_ready".to_string()
            }
        }
        PromotionState::Quarantined => {
            if paper_trade_count >= 3
                && paper_realized_pnl < config.paper_promotion_max_negative_pnl
            {
                "negative_recent_paper_pnl".to_string()
            } else if champion_pnl < 0.0 {
                "negative_recent_replay".to_string()
            } else {
                "lane_quarantined".to_string()
            }
        }
        PromotionState::Shadow => {
            if champion_brier > config.paper_promotion_max_brier {
                "replay_brier_too_high".to_string()
            } else if champion_pnl < 0.0 {
                "replay_expectancy_negative".to_string()
            } else {
                "awaiting_paper_readiness".to_string()
            }
        }
    }
}
