#![recursion_limit = "256"]

use anyhow::Result;
use chrono::{Duration, Utc};
use common::{
    AppConfig, ExecutionScore, ExecutionScoreInputs, ExpiryRegime, MarketFamily, ModelInference,
    OpportunityDecision, PromotionState, StrategyFamily, TradeMode, build_execution_score,
    compute_execution_score_bps, directional_raw_edge_bps, estimate_fill_probability, lane_key,
    lane_key_with_regime,
};
use market_data::{ContractSide, QueueSideSummary, QueueSnapshot};
use market_models::{
    BASELINE_LOGIT_V1, FeatureVector, SETTLEMENT_ANCHOR_V3, TRAINED_LINEAR_CONTRARIAN_V1,
    TRAINED_LINEAR_V1, run_model_with_weights,
};
use redis::AsyncCommands;
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use storage::TrainedModelArtifactCard;
use tracing::{info, warn};

use common::ExpiryRegime as Regime;

const WEATHER_MODEL_NAME: &str = "weather_threshold_v1";
const EXECUTION_SCORE_THRESHOLD_BPS: f64 = 10.0;
const REDIS_QUEUE_PREFIX: &str = "v3:queue:";

#[derive(Debug, Clone, Copy)]
struct TrainedDecisionPolicy {
    min_edge: f64,
    min_confidence: f64,
    min_venue_quality: f64,
    min_seconds_to_expiry_scaled: f64,
}

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
        .upsert_worker_started("decision", &json!({}))
        .await?;
    let run_once = std::env::var("DECISION_RUN_ONCE")
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True"))
        .unwrap_or(false);

    info!("decision_worker_started");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        config.decision_poll_seconds.max(5),
    ));
    loop {
        interval.tick().await;
        storage
            .upsert_worker_started("decision", &json!({"phase": "decision_pass"}))
            .await?;
        match decide_once(&config, &storage).await {
            Ok((decision_count, approved_count)) => {
                storage
                    .upsert_worker_success(
                        "decision",
                        &json!({"decision_count": decision_count, "approved_count": approved_count}),
                    )
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("decision", &error.to_string(), &json!({}))
                    .await?;
                warn!(error = %error, "decision_pass_failed");
            }
        }
        if run_once {
            info!("decision_worker_run_once_complete");
            break;
        }
    }
    Ok(())
}

async fn decide_once(config: &AppConfig, storage: &storage::Storage) -> Result<(usize, usize)> {
    let started_at = Instant::now();
    let redis_client = redis::Client::open(config.redis_url.clone())?;
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    info!("decision_pass_started");
    let features = select_decision_snapshots(storage.list_latest_feature_snapshots(128).await?);
    info!(
        feature_count = features.len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "decision_features_loaded"
    );
    let mut decisions = 0usize;
    let mut approved_count = 0usize;
    let mut trained_artifact_cache: HashMap<
        (String, Option<ExpiryRegime>),
        Option<TrainedModelArtifactCard>,
    > = HashMap::new();
    let paper_crypto_bankroll = storage
        .latest_family_bankroll(TradeMode::Paper, MarketFamily::Crypto)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(config.initial_paper_crypto_budget);
    let paper_weather_bankroll = storage
        .latest_family_bankroll(TradeMode::Paper, MarketFamily::Weather)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(config.initial_paper_weather_budget);
    let live_bankroll_card = storage.latest_portfolio_bankroll(TradeMode::Live).await?;
    let live_crypto_bankroll = storage
        .latest_family_bankroll(TradeMode::Live, MarketFamily::Crypto)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(config.initial_live_crypto_budget);
    let live_deployable_bankroll = live_bankroll_card
        .as_ref()
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(config.initial_live_bankroll);
    let live_total_bankroll = live_bankroll_card
        .as_ref()
        .map(|card| card.bankroll.max(0.0))
        .unwrap_or(config.initial_live_bankroll);
    let live_order_placement_enabled = storage
        .effective_live_order_placement_enabled(config.live_order_placement_enabled)
        .await?;
    let live_drawdown_guard_active = live_drawdown_triggered(
        live_total_bankroll,
        config.initial_live_bankroll,
        config.live_portfolio_drawdown_kill_pct,
    );
    let mut paper_crypto_remaining = paper_crypto_bankroll;
    let mut paper_weather_remaining = paper_weather_bankroll;
    let mut live_remaining = live_crypto_bankroll.min(live_deployable_bankroll);
    let mut lane_exposure_adjustments: HashMap<String, f64> = HashMap::new();
    let mut symbol_exposure_adjustments: HashMap<String, f64> = HashMap::new();
    info!(
        paper_crypto_bankroll,
        paper_weather_bankroll,
        live_deployable_bankroll,
        live_total_bankroll,
        live_drawdown_guard_active,
        elapsed_ms = started_at.elapsed().as_millis(),
        "decision_bankroll_loaded"
    );

    for snapshot in features {
        let snapshot_started_at = Instant::now();
        if snapshot.market_family == MarketFamily::Weather {
            let lane_policy = storage
                .latest_lane_policy_for_symbol(
                    MarketFamily::Weather,
                    &snapshot.symbol,
                    StrategyFamily::DirectionalSettlement,
                    None,
                )
                .await?;
            let model_name = lane_policy
                .as_ref()
                .and_then(|policy| policy.current_champion_model.clone())
                .unwrap_or_else(|| WEATHER_MODEL_NAME.to_string());
            let model_prob = snapshot.reference_yes_prob.clamp(0.01, 0.99);
            let edge = model_prob - snapshot.market_prob;
            let side = if edge >= 0.0 { "buy_yes" } else { "buy_no" };
            let lane = lane_key(
                "kalshi",
                &snapshot.symbol.to_lowercase(),
                snapshot.window_minutes as u32,
                side,
                StrategyFamily::DirectionalSettlement,
                &model_name,
            );
            if let Some(last_created_at) = storage
                .latest_decision_at_for_market(
                    &lane,
                    snapshot.market_id,
                    Some(&snapshot.market_ticker),
                )
                .await?
            {
                if should_skip_recent_lane_decision(last_created_at, config.decision_poll_seconds) {
                    continue;
                }
            }
            let confidence = (snapshot.weather_reference_confidence.unwrap_or(0.05) * 0.55
                + edge.abs() * 1.35
                + snapshot.venue_quality_score * 0.25
                - (snapshot.reference_age_seconds as f64 / 3_600.0) * 0.08
                - (snapshot.market_data_age_seconds as f64 / 300.0) * 0.04)
                .clamp(0.05, 0.95);
            let mut reasons = Vec::new();
            if snapshot.weather_forecast_temperature_f.is_none() {
                reasons.push("missing_weather_reference".to_string());
            }
            if edge.abs() < config.min_edge {
                reasons.push("edge_below_threshold".to_string());
            }
            if confidence < config.min_confidence {
                reasons.push("low_confidence".to_string());
            }
            if snapshot.venue_quality_score < config.min_venue_quality {
                reasons.push("low_venue_quality".to_string());
            }
            if snapshot.reference_age_seconds > 10_800 {
                reasons.push("stale_weather_reference".to_string());
            }
            let promotion_state = lane_policy
                .as_ref()
                .map(|policy| policy.promotion_state)
                .unwrap_or(if config.weather_paper_trading_enabled {
                    PromotionState::PaperActive
                } else {
                    PromotionState::Shadow
                });
            match promotion_state {
                PromotionState::Quarantined => reasons.push("lane_quarantined".to_string()),
                PromotionState::Shadow if !config.weather_paper_trading_enabled => {
                    reasons.push("weather_paper_disabled".to_string())
                }
                PromotionState::Shadow => reasons.push("lane_not_paper_ready".to_string()),
                PromotionState::PaperActive
                | PromotionState::LiveMicro
                | PromotionState::LiveScaled => {}
            }

            let position_cap = (paper_weather_remaining * config.max_position_pct).max(0.0);
            let raw_size = paper_weather_remaining
                * edge.abs()
                * confidence
                * snapshot.venue_quality_score
                * config.max_position_pct
                * 0.75;
            let recommended_size = if position_cap >= 5.0 {
                raw_size.clamp(5.0, position_cap)
            } else {
                position_cap
            };
            if recommended_size > paper_weather_remaining + f64::EPSILON {
                reasons.push("insufficient_deployable_balance".to_string());
            }
            if recommended_size < 5.0 {
                reasons.push("size_too_small".to_string());
            }
            let approved = reasons.is_empty();
            let inference = ModelInference {
                market_id: snapshot.market_id,
                market_family: MarketFamily::Weather,
                lane_key: lane.clone(),
                strategy_family: StrategyFamily::DirectionalSettlement,
                model_name: model_name.clone(),
                raw_score: edge,
                raw_probability_yes: model_prob,
                calibrated_probability_yes: model_prob,
                raw_confidence: confidence,
                calibrated_confidence: confidence,
                feature_version: snapshot.feature_version.clone(),
                rationale_json: json!({
                    "market_ticker": snapshot.market_ticker,
                    "market_title": snapshot.market_title,
                    "weather_city": snapshot.weather_city,
                    "weather_contract_kind": snapshot.weather_contract_kind,
                    "weather_market_date": snapshot.weather_market_date,
                    "weather_forecast_temperature_f": snapshot.weather_forecast_temperature_f,
                    "weather_observation_temperature_f": snapshot.weather_observation_temperature_f,
                    "weather_reference_confidence": snapshot.weather_reference_confidence,
                    "weather_reference_source": snapshot.weather_reference_source,
                    "weather_strike_type": snapshot.weather_strike_type,
                    "weather_floor_strike": snapshot.weather_floor_strike,
                    "weather_cap_strike": snapshot.weather_cap_strike,
                }),
            };
            storage.insert_model_inference(&inference).await?;
            let decision = OpportunityDecision {
                market_id: snapshot.market_id,
                market_family: MarketFamily::Weather,
                lane_key: lane.clone(),
                strategy_family: StrategyFamily::DirectionalSettlement,
                model_name,
                side: side.to_string(),
                market_prob: snapshot.market_prob,
                model_prob,
                edge,
                confidence,
                approved,
                reasons_json: reasons.clone(),
                recommended_size,
            };
            let decision_id = storage.insert_opportunity_decision(&decision).await?;
            decisions += 1;
            if approved
                && !storage
                    .has_open_trade_for_market(snapshot.market_id, Some(&snapshot.market_ticker))
                    .await?
                && !storage
                    .has_active_execution_intent_for_market(
                        snapshot.market_id,
                        Some(&snapshot.market_ticker),
                    )
                    .await?
            {
                storage
                    .insert_execution_intent(
                        decision_id,
                        MarketFamily::Weather,
                        TradeMode::Paper,
                        "midpoint",
                        &[1.0],
                        snapshot.seconds_to_expiry.max(7_200),
                        900,
                        &["expiry_exit".to_string()],
                        None,
                        None,
                        None,
                        None,
                        paper_contract_count(
                            recommended_size,
                            contract_price_for_side(side, snapshot.market_prob),
                        ),
                        Some(PromotionState::PaperActive),
                    )
                    .await?;
                paper_weather_remaining = (paper_weather_remaining - recommended_size).max(0.0);
                approved_count += 1;
            }
            info!(
                market_id = snapshot.market_id,
                symbol = snapshot.symbol,
                lane,
                approved,
                edge,
                confidence,
                snapshot_elapsed_ms = snapshot_started_at.elapsed().as_millis(),
                "weather_decision_snapshot_processed"
            );
            continue;
        }

        let lane_policy = storage
            .latest_lane_policy_for_symbol(
                snapshot.market_family,
                &snapshot.symbol,
                StrategyFamily::DirectionalSettlement,
                snapshot.expiry_regime,
            )
            .await?;
        let regime = snapshot
            .expiry_regime
            .unwrap_or_else(|| get_regime(i64::from(snapshot.seconds_to_expiry)));
        let fallback_lane_policy = if lane_policy.is_none() && regime != Regime::Late {
            storage
                .latest_lane_policy_for_symbol(
                    snapshot.market_family,
                    &snapshot.symbol,
                    StrategyFamily::DirectionalSettlement,
                    None,
                )
                .await?
        } else {
            None
        };
        let effective_lane_policy = lane_policy.as_ref().or(fallback_lane_policy.as_ref());
        let model_name = effective_lane_policy
            .as_ref()
            .and_then(|policy| policy.current_champion_model.clone())
            .unwrap_or_else(|| BASELINE_LOGIT_V1.to_string());
        let feature_vector = regime_adjusted_feature_vector(&snapshot, regime);
        let trained_artifact = if uses_trained_linear_weights(&model_name) {
            let cache_key = (snapshot.symbol.clone(), Some(regime));
            if !trained_artifact_cache.contains_key(&cache_key) {
                let mut artifact = storage
                    .latest_trained_model_artifact(
                        TRAINED_LINEAR_V1,
                        snapshot.market_family,
                        StrategyFamily::DirectionalSettlement,
                        Some(&snapshot.symbol),
                        Some(regime),
                    )
                    .await?;
                if artifact.is_none() && regime != Regime::Late {
                    artifact = storage
                        .latest_trained_model_artifact(
                            TRAINED_LINEAR_V1,
                            snapshot.market_family,
                            StrategyFamily::DirectionalSettlement,
                            Some(&snapshot.symbol),
                            None,
                        )
                        .await?;
                }
                trained_artifact_cache.insert(cache_key.clone(), artifact);
            }
            trained_artifact_cache.get(&cache_key).cloned().flatten()
        } else {
            None
        };
        let trained_policy = trained_artifact
            .as_ref()
            .and_then(trained_policy_from_artifact)
            .filter(|policy| !should_ignore_trained_policy(policy, regime));
        let model = run_model_with_weights(
            &model_name,
            &feature_vector,
            trained_artifact.as_ref().map(|artifact| &artifact.weights),
        );
        let crossing_probability = crossing_probability_v1(&feature_vector, regime);
        let model_probability_yes =
            blended_probability_yes(model.probability_yes, crossing_probability, regime);
        let calibration = storage
            .latest_probability_calibration(
                snapshot.market_family,
                &snapshot.symbol,
                StrategyFamily::DirectionalSettlement,
                &model_name,
                &snapshot.time_to_expiry_bucket,
            )
            .await?;
        let calibrated_probability_yes = if let Some(calibration) = &calibration {
            let weight = (f64::from(calibration.sample_count) / 24.0).clamp(0.0, 1.0);
            (model_probability_yes + (calibration.mean_error * weight)).clamp(0.01, 0.99)
        } else {
            model_probability_yes
        };

        let min_edge = trained_policy
            .map(|policy| policy.min_edge)
            .unwrap_or(config.min_edge);
        let min_confidence = trained_policy
            .map(|policy| policy.min_confidence)
            .unwrap_or(config.min_confidence);
        let min_venue_quality = trained_policy
            .map(|policy| policy.min_venue_quality)
            .unwrap_or(config.min_venue_quality);
        let min_seconds_to_expiry = trained_policy
            .map(|policy| (policy.min_seconds_to_expiry_scaled * 900.0).round() as i64)
            .unwrap_or(120)
            .max(0);
        let effective_min_confidence =
            regime_adjusted_min_confidence(model_name.as_str(), regime, min_confidence);
        let effective_min_seconds_to_expiry =
            regime_adjusted_min_seconds_to_expiry(regime, min_seconds_to_expiry);

        let signed_edge = calibrated_probability_yes - snapshot.market_prob;
        let preview_side = if signed_edge >= 0.0 {
            "buy_yes"
        } else {
            "buy_no"
        };
        let raw_edge_bps = directional_raw_edge_bps(
            preview_side,
            calibrated_probability_yes,
            snapshot.market_prob,
        );
        let queue_snapshot = load_queue_snapshot(&mut redis, &snapshot.market_ticker).await?;
        let execution_score = snapshot_execution_score(
            &snapshot,
            preview_side,
            raw_edge_bps,
            queue_snapshot.as_ref(),
            regime,
        );
        let current_execution_score = compute_execution_score_bps(&execution_score);
        let edge = signed_edge;
        let side = if edge >= 0.0 { "buy_yes" } else { "buy_no" };
        let lane = lane_key_with_regime(
            "kalshi",
            &snapshot.symbol.to_lowercase(),
            snapshot.window_minutes as u32,
            side,
            StrategyFamily::DirectionalSettlement,
            &model_name,
            Some(regime),
        );
        if let Some(last_created_at) = storage
            .latest_decision_at_for_market(&lane, snapshot.market_id, Some(&snapshot.market_ticker))
            .await?
        {
            if should_skip_recent_lane_decision(last_created_at, config.decision_poll_seconds) {
                info!(
                    market_id = snapshot.market_id,
                    symbol = snapshot.symbol,
                    lane = lane,
                    last_created_at = %last_created_at,
                    "decision_skipped_recent_lane"
                );
                continue;
            }
        }

        let mut reasons = Vec::new();
        if edge.abs() < min_edge {
            reasons.push("edge_below_threshold".to_string());
        }
        if model.confidence < effective_min_confidence {
            reasons.push("low_confidence".to_string());
        }
        if current_execution_score <= EXECUTION_SCORE_THRESHOLD_BPS {
            reasons.push("execution_score_below_threshold".to_string());
        }
        if snapshot.venue_quality_score < min_venue_quality {
            reasons.push("low_venue_quality".to_string());
        }
        if i64::from(snapshot.seconds_to_expiry) < effective_min_seconds_to_expiry {
            reasons.push("too_close_to_expiry".to_string());
        }
        let promotion_state = effective_lane_policy.map(|policy| policy.promotion_state);
        match promotion_state {
            Some(PromotionState::Quarantined) => {
                reasons.push("lane_quarantined".to_string());
            }
            Some(PromotionState::Shadow) => {
                reasons.push("lane_not_paper_ready".to_string());
            }
            Some(
                PromotionState::PaperActive
                | PromotionState::LiveMicro
                | PromotionState::LiveScaled,
            ) => {}
            None => {
                reasons.push("lane_untrained".to_string());
            }
        }
        if regime == Regime::Late {
            reasons.push("late_regime_diagnostic_only".to_string());
        }

        let intent_mode = target_intent_mode(config, live_order_placement_enabled, promotion_state);
        let bankroll = match intent_mode {
            TradeMode::Paper => paper_crypto_remaining,
            TradeMode::Live => live_remaining,
        };
        let theoretical_entry_price = contract_price_for_side(side, snapshot.market_prob);
        let executable_entry_price = estimated_live_entry_price(&snapshot, side);
        let position_cap = (bankroll * max_position_pct_for_mode(config, intent_mode)).max(0.0);
        let execution_quality_multiplier = (current_execution_score / 100.0).clamp(0.15, 1.25);
        let raw_size = bankroll
            * edge.abs()
            * model.confidence
            * snapshot.venue_quality_score
            * execution_quality_multiplier
            * max_position_pct_for_mode(config, intent_mode);
        let recommended_size = if position_cap >= 5.0 {
            raw_size.clamp(5.0, position_cap)
        } else {
            position_cap
        };
        if recommended_size > bankroll + f64::EPSILON {
            reasons.push("insufficient_deployable_balance".to_string());
        }
        if recommended_size < 5.0 {
            reasons.push("size_too_small".to_string());
        }
        if matches!(intent_mode, TradeMode::Live) {
            if live_drawdown_guard_active {
                reasons.push("live_portfolio_kill_switch".to_string());
            }

            let lane_exposure_key = exposure_key(intent_mode, &lane);
            let symbol_exposure_key = exposure_key(intent_mode, &snapshot.symbol.to_lowercase());
            let lane_exposure = storage
                .open_exposure_for_mode_and_lane(intent_mode, &lane)
                .await?
                + lane_exposure_adjustments
                    .get(&lane_exposure_key)
                    .copied()
                    .unwrap_or_default();
            let symbol_exposure = storage
                .open_exposure_for_mode_and_symbol(intent_mode, &snapshot.symbol)
                .await?
                + symbol_exposure_adjustments
                    .get(&symbol_exposure_key)
                    .copied()
                    .unwrap_or_default();
            let live_entry_price_for_gates = executable_entry_price.max(0.01);
            let lane_cap = (live_total_bankroll * config.live_max_lane_exposure_pct.max(0.0))
                .max(live_entry_price_for_gates);
            let symbol_cap = (live_total_bankroll * config.live_max_symbol_exposure_pct.max(0.0))
                .max(live_entry_price_for_gates);
            if lane_exposure + recommended_size > lane_cap + f64::EPSILON {
                reasons.push("lane_exposure_cap".to_string());
            }
            if symbol_exposure + recommended_size > symbol_cap + f64::EPSILON {
                reasons.push("symbol_exposure_cap".to_string());
            }
            if live_contract_count(recommended_size, live_entry_price_for_gates) < 1.0 {
                reasons.push("size_too_small_for_live_contract".to_string());
            }
        }
        let approved = reasons.is_empty();

        let inference = ModelInference {
            market_id: snapshot.market_id,
            market_family: snapshot.market_family,
            lane_key: lane.clone(),
            strategy_family: StrategyFamily::DirectionalSettlement,
            model_name: model_name.clone(),
            raw_score: model.raw_score,
            raw_probability_yes: model.probability_yes,
            calibrated_probability_yes,
            raw_confidence: model.confidence,
            calibrated_confidence: model.confidence,
            feature_version: snapshot.feature_version.clone(),
            rationale_json: json!({
                "market_ticker": snapshot.market_ticker,
                "market_title": snapshot.market_title,
                "calibration_mean_error": calibration.as_ref().map(|row| row.mean_error),
                "calibration_sample_count": calibration.as_ref().map(|row| row.sample_count),
                "trained_artifact_sample_count": trained_artifact.as_ref().map(|artifact| artifact.sample_count),
                "effective_min_edge": min_edge,
                "effective_min_confidence": effective_min_confidence,
                "effective_min_venue_quality": min_venue_quality,
                "effective_min_seconds_to_expiry": effective_min_seconds_to_expiry,
                "reference_yes_prob": snapshot.reference_yes_prob,
                "reference_gap_bps": snapshot.reference_gap_bps,
                "reference_price_change_bps": snapshot.reference_price_change_bps,
                "threshold_distance_bps_proxy": snapshot.threshold_distance_bps_proxy,
                "distance_to_strike_bps": snapshot.distance_to_strike_bps,
                "reference_velocity": snapshot.reference_velocity,
                "realized_vol_short": snapshot.realized_vol_short,
                "time_decay_factor": snapshot.time_decay_factor,
                "size_ahead": snapshot.size_ahead,
                "trade_rate": snapshot.trade_rate,
                "quote_churn": snapshot.quote_churn,
                "size_ahead_real": snapshot.size_ahead_real,
                "trade_consumption_rate": snapshot.trade_consumption_rate,
                "cancel_rate": snapshot.cancel_rate,
                "queue_decay_rate": snapshot.queue_decay_rate,
                "expiry_regime": regime_name(regime),
                "crossing_probability": crossing_probability,
                "raw_edge_bps": raw_edge_bps,
                "execution_score_bps": current_execution_score,
                "fill_probability": execution_score.fill_probability,
                "expected_slippage_bps": execution_score.expected_slippage_bps,
                "adverse_selection_bps": execution_score.adverse_selection_bps,
                "execution_score_threshold_bps": EXECUTION_SCORE_THRESHOLD_BPS,
                "queue_market_ticker": queue_snapshot.as_ref().map(|queue| queue.market_ticker.clone()),
                "queue_side": queue_contract_side(side).map(queue_side_name),
                "settlement_regime": snapshot.settlement_regime,
                "market_data_age_seconds": snapshot.market_data_age_seconds,
                "reference_age_seconds": snapshot.reference_age_seconds,
                "venue_quality_score": snapshot.venue_quality_score,
                "seconds_to_expiry": snapshot.seconds_to_expiry,
                "target_mode": match intent_mode {
                    TradeMode::Paper => "paper",
                    TradeMode::Live => "live",
                },
                "live_drawdown_guard_active": live_drawdown_guard_active,
                "theoretical_entry_price": theoretical_entry_price,
                "estimated_live_entry_price": executable_entry_price,
            }),
        };
        storage.insert_model_inference(&inference).await?;

        let decision = OpportunityDecision {
            market_id: snapshot.market_id,
            market_family: snapshot.market_family,
            lane_key: lane.clone(),
            strategy_family: StrategyFamily::DirectionalSettlement,
            model_name: model_name.clone(),
            side: side.to_string(),
            market_prob: snapshot.market_prob,
            model_prob: calibrated_probability_yes,
            edge,
            confidence: model.confidence,
            approved,
            reasons_json: reasons.clone(),
            recommended_size,
        };
        let decision_id = storage.insert_opportunity_decision(&decision).await?;
        decisions += 1;

        if approved
            && !storage
                .has_open_trade_for_market(snapshot.market_id, Some(&snapshot.market_ticker))
                .await?
            && !storage
                .has_active_execution_intent_for_market(
                    snapshot.market_id,
                    Some(&snapshot.market_ticker),
                )
                .await?
        {
            let stop_conditions = vec![
                "expiry_exit".to_string(),
                format!("execution_score_bps:{current_execution_score:.4}"),
                format!("execution_score_threshold_bps:{EXECUTION_SCORE_THRESHOLD_BPS:.4}"),
                format!("fill_probability:{:.4}", execution_score.fill_probability),
                format!(
                    "queue_size_ahead_real:{:.4}",
                    execution_score_size_ahead(&snapshot, queue_snapshot.as_ref(), side)
                ),
                format!(
                    "queue_trade_consumption_rate:{:.4}",
                    execution_score_trade_consumption_rate(
                        &snapshot,
                        queue_snapshot.as_ref(),
                        side
                    )
                ),
                format!(
                    "queue_cancel_rate:{:.4}",
                    execution_score_cancel_rate(&snapshot, queue_snapshot.as_ref(), side)
                ),
                format!("spread_bps:{:.4}", snapshot.spread_bps),
            ];
            storage
                .insert_execution_intent(
                    decision_id,
                    snapshot.market_family,
                    intent_mode,
                    if current_execution_score >= 40.0 {
                        "maker_priority"
                    } else {
                        "midpoint"
                    },
                    &[1.0],
                    snapshot.seconds_to_expiry.max(120),
                    45,
                    &stop_conditions,
                    Some(execution_score.fill_probability),
                    Some(execution_score.expected_slippage_bps),
                    Some(execution_score_size_ahead(
                        &snapshot,
                        queue_snapshot.as_ref(),
                        side,
                    )),
                    Some("queue_fill_v1"),
                    match intent_mode {
                        TradeMode::Paper => {
                            paper_contract_count(recommended_size, theoretical_entry_price)
                        }
                        TradeMode::Live => {
                            live_contract_count(recommended_size, executable_entry_price)
                        }
                    },
                    promotion_state,
                )
                .await?;
            match intent_mode {
                TradeMode::Paper => {
                    paper_crypto_remaining = (paper_crypto_remaining - recommended_size).max(0.0);
                }
                TradeMode::Live => {
                    live_remaining = (live_remaining - recommended_size).max(0.0);
                    let lane_exposure_key = exposure_key(intent_mode, &lane);
                    let symbol_exposure_key =
                        exposure_key(intent_mode, &snapshot.symbol.to_lowercase());
                    *lane_exposure_adjustments
                        .entry(lane_exposure_key)
                        .or_insert(0.0) += recommended_size;
                    *symbol_exposure_adjustments
                        .entry(symbol_exposure_key)
                        .or_insert(0.0) += recommended_size;
                }
            }
            approved_count += 1;
        }
        info!(
            market_id = snapshot.market_id,
            symbol = snapshot.symbol,
            lane,
            approved,
            edge,
            confidence = model.confidence,
            reason_count = reasons.len(),
            snapshot_elapsed_ms = snapshot_started_at.elapsed().as_millis(),
            total_elapsed_ms = started_at.elapsed().as_millis(),
            "decision_snapshot_processed"
        );
    }

    info!(
        decision_count = decisions,
        approved_count,
        elapsed_ms = started_at.elapsed().as_millis(),
        "decision_pass_succeeded"
    );
    Ok((decisions, approved_count))
}

fn select_decision_snapshots(
    features: Vec<common::MarketFeatureSnapshotRecord>,
) -> Vec<common::MarketFeatureSnapshotRecord> {
    let mut selected = Vec::new();
    let mut front_crypto: HashMap<
        (String, i32, Option<ExpiryRegime>),
        common::MarketFeatureSnapshotRecord,
    > = HashMap::new();

    for snapshot in features {
        if snapshot.market_family != MarketFamily::Crypto {
            selected.push(snapshot);
            continue;
        }
        if !is_eligible_crypto_decision_snapshot(&snapshot) {
            continue;
        }
        let key = (
            snapshot.symbol.to_lowercase(),
            snapshot.window_minutes,
            snapshot.expiry_regime,
        );
        match front_crypto.get(&key) {
            Some(existing) if !should_prefer_crypto_snapshot(existing, &snapshot) => {}
            _ => {
                front_crypto.insert(key, snapshot);
            }
        }
    }

    let mut front_crypto_snapshots: Vec<_> = front_crypto.into_values().collect();
    front_crypto_snapshots.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    selected.extend(front_crypto_snapshots);
    selected
}

fn is_eligible_crypto_decision_snapshot(snapshot: &common::MarketFeatureSnapshotRecord) -> bool {
    snapshot.market_family == MarketFamily::Crypto
        && snapshot.seconds_to_expiry > 0
        && snapshot.seconds_to_expiry <= crypto_decision_horizon_seconds(snapshot.window_minutes)
}

fn snapshot_staleness_rank(snapshot: &common::MarketFeatureSnapshotRecord) -> i32 {
    snapshot
        .market_data_age_seconds
        .max(0)
        .max(snapshot.reference_age_seconds.max(0))
}

fn snapshot_spread_rank_bps(snapshot: &common::MarketFeatureSnapshotRecord) -> f64 {
    if snapshot.spread_bps > 0.0 {
        snapshot.spread_bps
    } else {
        10_000.0
    }
}

fn normalized_book_prices_for_side(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
) -> (Option<f64>, Option<f64>) {
    let best_bid = valid_contract_book_price(snapshot.best_bid);
    let best_ask = valid_contract_book_price(snapshot.best_ask);
    if side == "buy_no" {
        (
            best_ask.map(|price| (1.0 - price).clamp(0.01, 0.99)),
            best_bid.map(|price| (1.0 - price).clamp(0.01, 0.99)),
        )
    } else {
        (best_bid, best_ask)
    }
}

fn valid_contract_book_price(price: f64) -> Option<f64> {
    if price.is_finite() && price > 0.0 && price < 1.0 {
        Some(price.clamp(0.01, 0.99))
    } else {
        None
    }
}

fn snapshot_has_tradable_book(snapshot: &common::MarketFeatureSnapshotRecord) -> bool {
    matches!(
        (
            valid_contract_book_price(snapshot.best_bid),
            valid_contract_book_price(snapshot.best_ask),
        ),
        (Some(best_bid), Some(best_ask)) if best_ask + f64::EPSILON >= best_bid
    )
}

fn should_prefer_crypto_snapshot(
    current: &common::MarketFeatureSnapshotRecord,
    candidate: &common::MarketFeatureSnapshotRecord,
) -> bool {
    let current_eligible = is_eligible_crypto_decision_snapshot(current);
    let candidate_eligible = is_eligible_crypto_decision_snapshot(candidate);
    if current_eligible != candidate_eligible {
        return candidate_eligible;
    }
    if !candidate_eligible {
        return false;
    }

    let current_staleness = snapshot_staleness_rank(current);
    let candidate_staleness = snapshot_staleness_rank(candidate);
    if candidate_staleness != current_staleness {
        return candidate_staleness < current_staleness;
    }

    let tradable_book_cmp = snapshot_has_tradable_book(candidate).cmp(&snapshot_has_tradable_book(current));
    if tradable_book_cmp != std::cmp::Ordering::Equal {
        return tradable_book_cmp.is_gt();
    }

    let venue_quality_cmp = candidate
        .venue_quality_score
        .total_cmp(&current.venue_quality_score);
    if venue_quality_cmp != std::cmp::Ordering::Equal {
        return venue_quality_cmp.is_gt();
    }

    let spread_cmp =
        snapshot_spread_rank_bps(candidate).total_cmp(&snapshot_spread_rank_bps(current));
    if spread_cmp != std::cmp::Ordering::Equal {
        return spread_cmp.is_lt();
    }

    if candidate.seconds_to_expiry != current.seconds_to_expiry {
        return candidate.seconds_to_expiry < current.seconds_to_expiry;
    }

    let created_at_cmp = candidate.created_at.cmp(&current.created_at);
    if created_at_cmp != std::cmp::Ordering::Equal {
        return created_at_cmp.is_gt();
    }

    let market_id_cmp = candidate.market_id.cmp(&current.market_id);
    if market_id_cmp != std::cmp::Ordering::Equal {
        return market_id_cmp.is_lt();
    }

    candidate.market_ticker < current.market_ticker
}

fn crypto_decision_horizon_seconds(window_minutes: i32) -> i32 {
    (window_minutes.max(1) * 60 * 3).max(45 * 60)
}

fn should_skip_recent_lane_decision(
    last_created_at: chrono::DateTime<Utc>,
    poll_seconds: u64,
) -> bool {
    last_created_at > Utc::now() - Duration::seconds(poll_seconds as i64 - 1)
}

fn target_intent_mode(
    config: &AppConfig,
    live_order_placement_enabled: bool,
    promotion_state: Option<PromotionState>,
) -> TradeMode {
    if config.live_trading_enabled
        && live_order_placement_enabled
        && matches!(
            promotion_state,
            Some(PromotionState::LiveMicro | PromotionState::LiveScaled)
        )
    {
        TradeMode::Live
    } else {
        TradeMode::Paper
    }
}

fn max_position_pct_for_mode(config: &AppConfig, mode: TradeMode) -> f64 {
    match mode {
        TradeMode::Paper => config.max_position_pct.max(0.01),
        TradeMode::Live => config.live_max_position_pct.max(0.005),
    }
}

fn live_drawdown_triggered(bankroll: f64, initial_bankroll: f64, kill_pct: f64) -> bool {
    if initial_bankroll <= 0.0 || kill_pct <= 0.0 {
        return false;
    }
    bankroll <= (initial_bankroll * (1.0 - kill_pct.clamp(0.0, 0.95)))
}

fn trained_policy_from_artifact(
    artifact: &TrainedModelArtifactCard,
) -> Option<TrainedDecisionPolicy> {
    let policy = artifact.metrics_json.get("policy")?;
    Some(TrainedDecisionPolicy {
        min_edge: policy.get("min_edge")?.as_f64()?,
        min_confidence: policy.get("min_confidence")?.as_f64()?,
        min_venue_quality: policy.get("min_venue_quality")?.as_f64()?,
        min_seconds_to_expiry_scaled: policy.get("min_seconds_to_expiry_scaled")?.as_f64()?,
    })
}

fn should_ignore_trained_policy(policy: &TrainedDecisionPolicy, regime: Regime) -> bool {
    regime != Regime::Late
        && policy.min_edge >= 0.35
        && policy.min_confidence >= 0.95
        && policy.min_venue_quality >= 0.95
        && policy.min_seconds_to_expiry_scaled >= 0.80
}

fn regime_adjusted_min_confidence(model_name: &str, regime: Regime, min_confidence: f64) -> f64 {
    match regime {
        Regime::Mid => min_confidence.min(0.25),
        Regime::Early if model_name == SETTLEMENT_ANCHOR_V3 => min_confidence.min(0.25),
        _ => min_confidence,
    }
}

fn regime_adjusted_min_seconds_to_expiry(regime: Regime, min_seconds_to_expiry: i64) -> i64 {
    match regime {
        Regime::Mid => min_seconds_to_expiry.min(120),
        _ => min_seconds_to_expiry,
    }
}

fn uses_trained_linear_weights(model_name: &str) -> bool {
    matches!(model_name, TRAINED_LINEAR_V1 | TRAINED_LINEAR_CONTRARIAN_V1)
}

fn exposure_key(mode: TradeMode, value: &str) -> String {
    let scope = match mode {
        TradeMode::Paper => "paper",
        TradeMode::Live => "live",
    };
    format!("{scope}:{value}")
}

fn live_contract_count(position_notional: f64, contract_price: f64) -> f64 {
    if contract_price <= 0.0 {
        0.0
    } else {
        (position_notional / contract_price.max(0.05)).floor()
    }
}

fn paper_contract_count(position_notional: f64, contract_price: f64) -> f64 {
    if contract_price <= 0.0 {
        0.0
    } else {
        (position_notional / contract_price.max(0.05)).max(1.0)
    }
}

fn contract_price_for_side(side: &str, market_prob: f64) -> f64 {
    if side == "buy_no" {
        (1.0 - market_prob).clamp(0.01, 0.99)
    } else {
        market_prob.clamp(0.01, 0.99)
    }
}

fn contract_book_ask_for_side(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
) -> Option<f64> {
    let (best_bid, best_ask) = normalized_book_prices_for_side(snapshot, side);
    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) if ask + f64::EPSILON >= bid => Some(ask),
        (None, Some(ask)) => Some(ask),
        _ => None,
    }
}

fn estimated_live_entry_price(snapshot: &common::MarketFeatureSnapshotRecord, side: &str) -> f64 {
    let theoretical = contract_price_for_side(side, snapshot.market_prob);
    contract_book_ask_for_side(snapshot, side).unwrap_or(theoretical)
}

fn build_feature_vector(snapshot: &common::MarketFeatureSnapshotRecord) -> FeatureVector {
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

fn regime_adjusted_feature_vector(
    snapshot: &common::MarketFeatureSnapshotRecord,
    regime: Regime,
) -> FeatureVector {
    let mut features = build_feature_vector(snapshot);
    match regime {
        Regime::Early => {
            features.reference_gap_bps_scaled *= 1.25;
            features.reference_velocity_scaled *= 1.20;
            features.trade_rate_scaled *= 1.10;
            features.order_book_imbalance *= 0.85;
            features.spread_bps_scaled *= 0.90;
        }
        Regime::Mid => {}
        Regime::Late => {
            features.order_book_imbalance *= 1.30;
            features.aggressive_buy_ratio =
                (0.5 + ((features.aggressive_buy_ratio - 0.5) * 1.20)).clamp(0.0, 1.0);
            features.spread_bps_scaled *= 1.20;
            features.reference_gap_bps_scaled *= 0.90;
            features.reference_velocity_scaled *= 0.85;
            features.quote_churn_scaled *= 1.15;
        }
    }
    features
}

fn get_regime(seconds_to_expiry: i64) -> Regime {
    Regime::from_seconds_to_expiry(seconds_to_expiry)
}

fn regime_name(regime: Regime) -> &'static str {
    regime.as_key()
}

fn crossing_probability_v1(features: &FeatureVector, regime: Regime) -> f64 {
    let regime_bias = match regime {
        Regime::Early => 0.18,
        Regime::Mid => 0.08,
        Regime::Late => -0.04,
    };
    let score = features.distance_to_strike_bps_scaled * 1.35
        + features.reference_velocity_scaled * 0.75
        + features.realized_vol_short_scaled * 0.60
        + features.order_book_imbalance * 0.45
        + ((features.aggressive_buy_ratio - 0.5) * 0.55)
        + features.time_decay_factor * 0.35
        + features.trade_rate_scaled * 0.18
        - features.spread_bps_scaled * 0.30
        - features.size_ahead_scaled * 0.12
        - features.quote_churn_scaled * 0.10
        + regime_bias;
    (1.0 / (1.0 + (-score).exp())).clamp(0.01, 0.99)
}

fn blended_probability_yes(
    model_probability_yes: f64,
    crossing_probability: f64,
    regime: Regime,
) -> f64 {
    let crossing_weight = match regime {
        Regime::Early => 0.45,
        Regime::Mid => 0.50,
        Regime::Late => 0.60,
    };
    ((model_probability_yes * (1.0 - crossing_weight)) + (crossing_probability * crossing_weight))
        .clamp(0.01, 0.99)
}

async fn load_queue_snapshot(
    redis: &mut redis::aio::MultiplexedConnection,
    market_ticker: &str,
) -> Result<Option<QueueSnapshot>> {
    let raw: Option<String> = redis
        .get(format!("{REDIS_QUEUE_PREFIX}{market_ticker}"))
        .await
        .ok();
    Ok(raw
        .as_deref()
        .and_then(|value| serde_json::from_str::<QueueSnapshot>(value).ok()))
}

fn queue_contract_side(side: &str) -> Option<ContractSide> {
    match side {
        "buy_yes" => Some(ContractSide::Yes),
        "buy_no" => Some(ContractSide::No),
        _ => None,
    }
}

fn queue_side_name(side: ContractSide) -> &'static str {
    match side {
        ContractSide::Yes => "yes",
        ContractSide::No => "no",
    }
}

fn queue_side_summary<'a>(
    queue_snapshot: Option<&'a QueueSnapshot>,
    side: &str,
) -> Option<&'a QueueSideSummary> {
    match (queue_snapshot, queue_contract_side(side)) {
        (Some(queue), Some(ContractSide::Yes)) => Some(&queue.yes),
        (Some(queue), Some(ContractSide::No)) => Some(&queue.no),
        _ => None,
    }
}

fn execution_score_size_ahead(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.size_ahead_real)
        .unwrap_or(snapshot.size_ahead_real.max(snapshot.size_ahead.max(1.0)))
        .max(1.0)
}

fn execution_score_trade_consumption_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.trade_consumption_rate)
        .unwrap_or(snapshot.trade_consumption_rate.max(snapshot.trade_rate))
        .max(0.0)
}

fn execution_score_cancel_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.cancel_rate)
        .unwrap_or(snapshot.cancel_rate.max(0.0))
        .max(0.0)
}

fn execution_score_queue_decay_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.queue_decay_rate)
        .unwrap_or(snapshot.queue_decay_rate.max(0.0))
        .max(0.0)
}

fn snapshot_execution_score(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
    raw_edge_bps: f64,
    queue_snapshot: Option<&QueueSnapshot>,
    regime: Regime,
) -> ExecutionScore {
    build_execution_score(ExecutionScoreInputs {
        raw_edge_bps,
        spread_bps: snapshot.spread_bps,
        aggressive_buy_ratio: snapshot.aggressive_buy_ratio,
        quote_churn: snapshot.quote_churn,
        size_ahead: execution_score_size_ahead(snapshot, queue_snapshot, side),
        trade_consumption_rate: execution_score_trade_consumption_rate(
            snapshot,
            queue_snapshot,
            side,
        ),
        cancel_rate: execution_score_cancel_rate(snapshot, queue_snapshot, side),
        queue_decay_rate: execution_score_queue_decay_rate(snapshot, queue_snapshot, side),
        seconds_to_expiry: f64::from(snapshot.seconds_to_expiry.max(1)),
        regime,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, ExecutionScore, MarketFamily, PromotionState, Regime, SETTLEMENT_ANCHOR_V3,
        TradeMode, TrainedDecisionPolicy, compute_execution_score_bps,
        crypto_decision_horizon_seconds, directional_raw_edge_bps, estimate_fill_probability,
        estimated_live_entry_price, get_regime, live_contract_count, live_drawdown_triggered,
        max_position_pct_for_mode, regime_adjusted_min_confidence,
        regime_adjusted_min_seconds_to_expiry, select_decision_snapshots,
        should_ignore_trained_policy, should_prefer_crypto_snapshot,
        should_skip_recent_lane_decision, target_intent_mode,
    };
    use chrono::{Duration, Utc};
    use common::MarketFeatureSnapshotRecord;

    fn test_config() -> AppConfig {
        AppConfig {
            app_env: "test".to_string(),
            api_bind_addr: "127.0.0.1:8080".to_string(),
            database_url: "postgres://test".to_string(),
            redis_url: "redis://test".to_string(),
            nats_url: "nats://test".to_string(),
            exchange: "kalshi".to_string(),
            reference_price_mode: "proxy".to_string(),
            reference_price_source: "coinbase_proxy".to_string(),
            reference_averaging_window_seconds: 60,
            nws_api_base: "https://api.weather.gov".to_string(),
            open_meteo_geocode_api_base: "https://geocoding-api.open-meteo.com/v1/search"
                .to_string(),
            weather_series_category: "Climate and Weather".to_string(),
            weather_series_title_patterns: vec!["Highest temperature".to_string()],
            weather_series_poll_limit: 4,
            weather_reference_refresh_seconds: 900,
            kalshi_api_base: "https://example.com".to_string(),
            kalshi_ws_url: "wss://example.com".to_string(),
            kalshi_api_key_id: None,
            kalshi_api_key_id_file: None,
            kalshi_private_key_path: None,
            kalshi_private_key_b64: None,
            v2_reference_sqlite_path: None,
            coinbase_ws_url: "wss://example.com".to_string(),
            paper_trading_enabled: true,
            weather_paper_trading_enabled: true,
            live_trading_enabled: true,
            live_order_placement_enabled: true,
            discord_webhook_url: None,
            initial_paper_bankroll: 1000.0,
            initial_live_bankroll: 1000.0,
            initial_paper_crypto_budget: 850.0,
            initial_paper_weather_budget: 150.0,
            initial_live_crypto_budget: 1000.0,
            initial_live_weather_budget: 0.0,
            kalshi_series_tickers: vec!["KXBTC15M".to_string()],
            reference_symbols: vec!["BTC".to_string()],
            market_poll_seconds: 15,
            feature_poll_seconds: 15,
            decision_poll_seconds: 15,
            execution_poll_seconds: 15,
            historical_import_batch_size: 5000,
            min_edge: 0.04,
            min_confidence: 0.42,
            min_venue_quality: 0.2,
            max_position_pct: 0.03,
            live_max_position_pct: 0.02,
            live_max_lane_exposure_pct: 0.03,
            live_max_symbol_exposure_pct: 0.05,
            live_portfolio_drawdown_kill_pct: 0.15,
            paper_promotion_max_negative_replay: -0.1,
            paper_promotion_max_negative_pnl: -5.0,
            paper_promotion_max_brier: 0.05,
            live_micro_min_examples: 8,
            live_micro_min_pnl: 15.0,
            live_micro_max_brier: 0.03,
            live_scaled_min_examples: 3,
            live_scaled_min_pnl: 8.0,
            live_scaled_max_brier: 0.025,
            live_demote_max_negative_pnl: -8.0,
            live_entry_time_in_force: "good_till_cancelled".to_string(),
            live_exit_time_in_force: "good_till_cancelled".to_string(),
            live_order_replace_enabled: true,
            live_order_replace_after_seconds: 20,
            live_order_stale_after_seconds: 45,
            market_stale_after_seconds: 45,
            reference_stale_after_seconds: 45,
            live_bankroll_stale_after_seconds: 300,
            live_sync_stale_after_seconds: 45,
            lane_truth_live_sample_min_terminal_intents: 5,
            lane_truth_live_sample_min_predicted_fill_samples: 5,
            lane_truth_watch_min_actual_fill_hit_rate: 0.50,
            lane_truth_watch_min_filled_quantity_ratio: 0.55,
            lane_truth_watch_min_predicted_vs_realized_fill_gap: -0.15,
            lane_truth_watch_min_live_vs_replay_fill_gap: -0.10,
            lane_truth_watch_min_replay_edge_realization_ratio_diag: 0.5,
            lane_truth_watch_recommended_size_multiplier: 0.5,
            lane_truth_quarantine_min_actual_fill_hit_rate: 0.25,
            lane_truth_quarantine_min_filled_quantity_ratio: 0.35,
            lane_truth_quarantine_min_predicted_vs_realized_fill_gap: -0.30,
            lane_truth_quarantine_min_live_vs_replay_fill_gap: -0.25,
            lane_truth_quarantine_min_replay_edge_realization_ratio_diag: 0.0,
            lane_truth_quarantine_recommended_size_multiplier: 0.25,
        }
    }

    #[test]
    fn target_intent_mode_requires_live_promotion() {
        let config = test_config();
        assert_eq!(
            target_intent_mode(&config, true, Some(PromotionState::LiveMicro)),
            TradeMode::Live
        );
        assert_eq!(
            target_intent_mode(&config, true, Some(PromotionState::PaperActive)),
            TradeMode::Paper
        );
    }

    #[test]
    fn live_drawdown_guard_triggers_after_threshold() {
        assert!(live_drawdown_triggered(840.0, 1000.0, 0.15));
        assert!(!live_drawdown_triggered(900.0, 1000.0, 0.15));
    }

    #[test]
    fn live_position_pct_is_separate_from_paper() {
        let config = test_config();
        assert_eq!(max_position_pct_for_mode(&config, TradeMode::Paper), 0.03);
        assert_eq!(max_position_pct_for_mode(&config, TradeMode::Live), 0.02);
    }

    #[test]
    fn live_contract_count_requires_whole_contract() {
        assert_eq!(live_contract_count(0.4, 0.55), 0.0);
        assert_eq!(live_contract_count(1.4, 0.55), 2.0);
    }

    fn sample_snapshot(
        market_family: MarketFamily,
        symbol: &str,
        ticker: &str,
        seconds_to_expiry: i32,
        created_offset_seconds: i64,
    ) -> MarketFeatureSnapshotRecord {
        MarketFeatureSnapshotRecord {
            market_id: i64::from(seconds_to_expiry),
            market_family,
            market_ticker: ticker.to_string(),
            market_title: ticker.to_string(),
            feature_version: "test".to_string(),
            exchange: "kalshi".to_string(),
            symbol: symbol.to_string(),
            window_minutes: 15,
            seconds_to_expiry,
            time_to_expiry_bucket: "under_15m".to_string(),
            expiry_regime: Some(Regime::from_seconds_to_expiry(i64::from(seconds_to_expiry))),
            market_prob: 0.5,
            best_bid: 0.49,
            best_ask: 0.51,
            last_price: 0.5,
            bid_size: 10.0,
            ask_size: 10.0,
            liquidity: 100.0,
            order_book_imbalance: 0.0,
            aggressive_buy_ratio: 0.5,
            spread_bps: 200.0,
            venue_quality_score: 0.7,
            reference_price: 1.0,
            reference_previous_price: 1.0,
            reference_price_change_bps: 0.0,
            reference_yes_prob: 0.52,
            reference_gap_bps: 200.0,
            threshold_distance_bps_proxy: 0.0,
            distance_to_strike_bps: 150.0,
            reference_velocity: 4.0,
            realized_vol_short: 8.0,
            time_decay_factor: 0.4,
            size_ahead: 20.0,
            trade_rate: 10.0,
            quote_churn: 0.2,
            size_ahead_real: 22.0,
            trade_consumption_rate: 9.5,
            cancel_rate: 2.0,
            queue_decay_rate: 4.0,
            averaging_window_progress: 0.0,
            settlement_regime: "mid_window".to_string(),
            last_minute_avg_proxy: 1.0,
            market_data_age_seconds: 0,
            reference_age_seconds: 0,
            weather_city: None,
            weather_contract_kind: None,
            weather_market_date: None,
            weather_strike_type: None,
            weather_floor_strike: None,
            weather_cap_strike: None,
            weather_forecast_temperature_f: None,
            weather_observation_temperature_f: None,
            weather_reference_confidence: None,
            weather_reference_source: None,
            created_at: Utc::now() + Duration::seconds(created_offset_seconds),
        }
    }

    #[test]
    fn decision_selection_keeps_only_front_crypto_market() {
        let selected = select_decision_snapshots(vec![
            sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-near", 600, 0),
            sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-far", 18_000, 5),
            sample_snapshot(MarketFamily::Weather, "chi", "KWCHI-temp", 7_200, 0),
        ]);

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .any(|row| row.market_ticker == "KXBTC15M-near")
        );
        assert!(
            !selected
                .iter()
                .any(|row| row.market_ticker == "KXBTC15M-far")
        );
    }

    #[test]
    fn decision_selection_keeps_one_crypto_market_per_regime_and_prefers_nearest_tradable_expiry() {
        let selected = select_decision_snapshots(vec![
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-expired", 0, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-late", 45, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-mid-low", 90, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-mid-high", 240, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-early-low", 389, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-early-high", 1_289, 0),
            sample_snapshot(MarketFamily::Crypto, "xrp", "KXXRP15M-too-far", 5_000, 0),
        ]);

        assert_eq!(selected.len(), 3);
        assert!(
            selected
                .iter()
                .any(|row| row.market_ticker == "KXXRP15M-late")
        );
        assert!(
            selected
                .iter()
                .any(|row| row.market_ticker == "KXXRP15M-mid-low")
        );
        assert!(
            selected
                .iter()
                .any(|row| row.market_ticker == "KXXRP15M-early-low")
        );
    }

    #[test]
    fn stale_but_newer_snapshot_does_not_beat_fresher_tradable_market() {
        let mut fresher = sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-fresh", 420, -30);
        fresher.market_data_age_seconds = 1;
        fresher.reference_age_seconds = 1;
        fresher.venue_quality_score = 0.75;
        fresher.spread_bps = 120.0;

        let mut stale_newer =
            sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-stale", 405, 0);
        stale_newer.market_data_age_seconds = 35;
        stale_newer.reference_age_seconds = 35;
        stale_newer.venue_quality_score = 0.80;
        stale_newer.spread_bps = 90.0;

        assert!(!should_prefer_crypto_snapshot(&fresher, &stale_newer));
        assert!(should_prefer_crypto_snapshot(&stale_newer, &fresher));
    }

    #[test]
    fn tradable_book_beats_bookless_contract_when_freshness_ties() {
        let mut tradable = sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-book", 420, 0);
        tradable.market_data_age_seconds = 2;
        tradable.reference_age_seconds = 2;
        tradable.best_bid = 0.44;
        tradable.best_ask = 0.46;
        tradable.venue_quality_score = 0.70;
        tradable.spread_bps = 120.0;

        let mut bookless =
            sample_snapshot(MarketFamily::Crypto, "btc", "KXBTC15M-no-book", 390, 5);
        bookless.market_data_age_seconds = 2;
        bookless.reference_age_seconds = 2;
        bookless.best_bid = 0.0;
        bookless.best_ask = 0.0;
        bookless.venue_quality_score = 0.70;
        bookless.spread_bps = 120.0;

        assert!(!should_prefer_crypto_snapshot(&tradable, &bookless));
        assert!(should_prefer_crypto_snapshot(&bookless, &tradable));
    }

    #[test]
    fn crypto_decision_horizon_matches_short_horizon_contracts() {
        assert_eq!(crypto_decision_horizon_seconds(15), 2700);
    }

    #[test]
    fn recent_lane_decisions_are_deduped_by_poll_window() {
        assert!(should_skip_recent_lane_decision(
            Utc::now() - Duration::seconds(5),
            15
        ));
        assert!(!should_skip_recent_lane_decision(
            Utc::now() - Duration::seconds(20),
            15
        ));
    }

    #[test]
    fn live_size_gate_uses_executable_book_price_not_only_theoretical_price() {
        let mut snapshot = sample_snapshot(MarketFamily::Crypto, "sol", "KXSOL15M-live", 240, 0);
        snapshot.market_prob = 0.92;
        snapshot.best_bid = 0.38;
        snapshot.best_ask = 0.40;

        let theoretical = super::contract_price_for_side("buy_yes", snapshot.market_prob);
        let executable = estimated_live_entry_price(&snapshot, "buy_yes");

        assert!(live_contract_count(0.50, theoretical) < 1.0);
        assert!(live_contract_count(0.50, executable) >= 1.0);
    }

    #[test]
    fn missing_book_for_live_gate_falls_back_to_theoretical_price() {
        let mut snapshot = sample_snapshot(MarketFamily::Crypto, "eth", "KXETH15M-live", 240, 0);
        snapshot.market_prob = 0.61;
        snapshot.best_bid = 0.0;
        snapshot.best_ask = 0.0;

        let theoretical = super::contract_price_for_side("buy_yes", snapshot.market_prob);
        assert_eq!(
            estimated_live_entry_price(&snapshot, "buy_yes"),
            theoretical
        );
    }

    #[test]
    fn live_size_gate_uses_available_book_ask_when_bid_is_missing() {
        let mut snapshot = sample_snapshot(MarketFamily::Crypto, "ada", "KXADA15M-live", 240, 0);
        snapshot.market_prob = 0.74;
        snapshot.best_bid = 0.0;
        snapshot.best_ask = 0.33;

        assert!((estimated_live_entry_price(&snapshot, "buy_yes") - 0.33).abs() < 1e-9);
    }

    #[test]
    fn live_size_gate_uses_complementary_yes_book_for_buy_no() {
        let mut snapshot = sample_snapshot(MarketFamily::Crypto, "ltc", "KXLTC15M-live", 240, 0);
        snapshot.market_prob = 0.68;
        snapshot.best_bid = 0.61;
        snapshot.best_ask = 0.0;

        assert!((estimated_live_entry_price(&snapshot, "buy_no") - 0.39).abs() < 1e-9);
    }

    #[test]
    fn inconsistent_book_for_live_gate_falls_back_safely() {
        let mut snapshot = sample_snapshot(MarketFamily::Crypto, "doge", "KXDOGE15M-live", 240, 0);
        snapshot.market_prob = 0.58;
        snapshot.best_bid = 0.62;
        snapshot.best_ask = 0.55;

        let theoretical = super::contract_price_for_side("buy_yes", snapshot.market_prob);
        assert_eq!(
            estimated_live_entry_price(&snapshot, "buy_yes"),
            theoretical
        );
    }

    #[test]
    fn expiry_regime_split_matches_expected_buckets() {
        assert_eq!(get_regime(301), Regime::Early);
        assert_eq!(get_regime(120), Regime::Mid);
        assert_eq!(get_regime(45), Regime::Late);
    }

    #[test]
    fn execution_score_penalizes_bad_fill_and_slippage() {
        let strong = compute_execution_score_bps(&ExecutionScore {
            raw_edge_bps: 90.0,
            fill_probability: 0.8,
            expected_slippage_bps: 10.0,
            adverse_selection_bps: 12.0,
        });
        let weak = compute_execution_score_bps(&ExecutionScore {
            raw_edge_bps: 40.0,
            fill_probability: 0.2,
            expected_slippage_bps: 15.0,
            adverse_selection_bps: 12.0,
        });
        assert!(strong > weak);
    }

    #[test]
    fn missing_microstructure_uses_regime_fallback_fill_probability() {
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 600.0, Regime::Early),
            0.55
        );
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 240.0, Regime::Mid),
            0.35
        );
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 30.0, Regime::Late),
            0.10
        );
    }

    #[test]
    fn abstain_policy_is_ignored_for_non_late_regimes() {
        let abstain = TrainedDecisionPolicy {
            min_edge: 0.35,
            min_confidence: 0.97,
            min_venue_quality: 0.95,
            min_seconds_to_expiry_scaled: 0.80,
        };
        assert!(should_ignore_trained_policy(&abstain, Regime::Early));
        assert!(should_ignore_trained_policy(&abstain, Regime::Mid));
        assert!(!should_ignore_trained_policy(&abstain, Regime::Late));
    }

    #[test]
    fn mid_regime_caps_confidence_and_expiry_thresholds() {
        assert_eq!(
            regime_adjusted_min_confidence("trained_linear_v1", Regime::Mid, 0.5),
            0.25
        );
        assert_eq!(regime_adjusted_min_seconds_to_expiry(Regime::Mid, 450), 120);
    }

    #[test]
    fn early_settlement_anchor_gets_modest_confidence_cap() {
        assert_eq!(
            regime_adjusted_min_confidence(SETTLEMENT_ANCHOR_V3, Regime::Early, 0.42),
            0.25
        );
        assert_eq!(
            regime_adjusted_min_confidence("trained_linear_v1", Regime::Early, 0.42),
            0.42
        );
    }

    #[test]
    fn directional_raw_edge_handles_buy_no() {
        assert_eq!(directional_raw_edge_bps("buy_yes", 0.62, 0.50), 1200.0);
        assert_eq!(directional_raw_edge_bps("buy_no", 0.38, 0.50), 1200.0);
    }
}
