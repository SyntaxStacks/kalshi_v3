use anyhow::Result;
use chrono::{Duration, Utc};
use common::{
    AppConfig, ModelInference, OpportunityDecision, PromotionState, StrategyFamily, TradeMode,
    lane_key,
};
use market_models::{
    BASELINE_LOGIT_V1, FeatureVector, TRAINED_LINEAR_CONTRARIAN_V1, TRAINED_LINEAR_V1,
    run_model_with_weights,
};
use serde_json::json;
use std::collections::HashMap;
use std::time::Instant;
use storage::TrainedModelArtifactCard;
use tracing::{info, warn};

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
    info!("decision_pass_started");
    let features = storage.list_latest_feature_snapshots(16).await?;
    info!(
        feature_count = features.len(),
        elapsed_ms = started_at.elapsed().as_millis(),
        "decision_features_loaded"
    );
    let mut decisions = 0usize;
    let mut approved_count = 0usize;
    let mut trained_artifact_cache: HashMap<String, Option<TrainedModelArtifactCard>> =
        HashMap::new();
    let paper_bankroll = storage
        .latest_portfolio_bankroll(TradeMode::Paper)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(config.initial_paper_bankroll);
    let live_bankroll_card = storage.latest_portfolio_bankroll(TradeMode::Live).await?;
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
    let mut paper_remaining = paper_bankroll;
    let mut live_remaining = live_deployable_bankroll;
    let mut lane_exposure_adjustments: HashMap<String, f64> = HashMap::new();
    let mut symbol_exposure_adjustments: HashMap<String, f64> = HashMap::new();
    info!(
        paper_bankroll,
        live_deployable_bankroll,
        live_total_bankroll,
        live_drawdown_guard_active,
        elapsed_ms = started_at.elapsed().as_millis(),
        "decision_bankroll_loaded"
    );

    for snapshot in features {
        let snapshot_started_at = Instant::now();
        let lane_policy = storage
            .latest_lane_policy_for_symbol(&snapshot.symbol, StrategyFamily::DirectionalSettlement)
            .await?;
        let model_name = lane_policy
            .as_ref()
            .and_then(|policy| policy.current_champion_model.clone())
            .unwrap_or_else(|| BASELINE_LOGIT_V1.to_string());
        let lane_yes = lane_key(
            "kalshi",
            &snapshot.symbol.to_lowercase(),
            snapshot.window_minutes as u32,
            "buy_yes",
            StrategyFamily::DirectionalSettlement,
            &model_name,
        );

        if let Some(last_created_at) = storage.latest_decision_at_for_lane(&lane_yes).await? {
            if last_created_at
                > Utc::now() - Duration::seconds(config.decision_poll_seconds as i64 - 1)
            {
                info!(
                    market_id = snapshot.market_id,
                    symbol = snapshot.symbol,
                    lane = lane_yes,
                    last_created_at = %last_created_at,
                    "decision_skipped_recent_lane"
                );
                continue;
            }
        }

        let feature_vector = FeatureVector {
            market_prob: snapshot.market_prob,
            reference_yes_prob: snapshot.reference_yes_prob,
            reference_gap_bps_scaled: (snapshot.reference_gap_bps / 1_000.0).clamp(-1.5, 1.5),
            threshold_distance_bps_scaled: (snapshot.threshold_distance_bps_proxy / 500.0)
                .clamp(-1.5, 1.5),
            order_book_imbalance: snapshot.order_book_imbalance,
            aggressive_buy_ratio: snapshot.aggressive_buy_ratio,
            venue_quality_score: snapshot.venue_quality_score,
            market_data_age_scaled: (snapshot.market_data_age_seconds as f64 / 20.0)
                .clamp(0.0, 1.5),
            reference_age_scaled: (snapshot.reference_age_seconds as f64 / 20.0)
                .clamp(0.0, 1.5),
            averaging_window_progress: snapshot.averaging_window_progress.clamp(0.0, 1.0),
            seconds_to_expiry_scaled: (snapshot.seconds_to_expiry as f64 / 900.0)
                .clamp(0.0, 1.0),
        };
        let trained_artifact = if uses_trained_linear_weights(&model_name) {
            if !trained_artifact_cache.contains_key(&snapshot.symbol) {
                let artifact = storage
                    .latest_trained_model_artifact(
                        TRAINED_LINEAR_V1,
                        StrategyFamily::DirectionalSettlement,
                        Some(&snapshot.symbol),
                    )
                    .await?;
                trained_artifact_cache.insert(snapshot.symbol.clone(), artifact);
            }
            trained_artifact_cache
                .get(&snapshot.symbol)
                .cloned()
                .flatten()
        } else {
            None
        };
        let trained_policy = trained_artifact
            .as_ref()
            .and_then(trained_policy_from_artifact);
        let model = run_model_with_weights(
            &model_name,
            &feature_vector,
            trained_artifact.as_ref().map(|artifact| &artifact.weights),
        );
        let calibration = storage
            .latest_probability_calibration(
                &snapshot.symbol,
                StrategyFamily::DirectionalSettlement,
                &model_name,
                &snapshot.time_to_expiry_bucket,
            )
            .await?;
        let calibrated_probability_yes = if let Some(calibration) = &calibration {
            let weight = (f64::from(calibration.sample_count) / 24.0).clamp(0.0, 1.0);
            (model.probability_yes + (calibration.mean_error * weight)).clamp(0.01, 0.99)
        } else {
            model.probability_yes
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

        let edge = calibrated_probability_yes - snapshot.market_prob;
        let side = if edge >= 0.0 { "buy_yes" } else { "buy_no" };
        let lane = lane_key(
            "kalshi",
            &snapshot.symbol.to_lowercase(),
            snapshot.window_minutes as u32,
            side,
            StrategyFamily::DirectionalSettlement,
            &model_name,
        );

        let mut reasons = Vec::new();
        if edge.abs() < min_edge {
            reasons.push("edge_below_threshold".to_string());
        }
        if model.confidence < min_confidence {
            reasons.push("low_confidence".to_string());
        }
        if snapshot.venue_quality_score < min_venue_quality {
            reasons.push("low_venue_quality".to_string());
        }
        if i64::from(snapshot.seconds_to_expiry) < min_seconds_to_expiry {
            reasons.push("too_close_to_expiry".to_string());
        }
        let promotion_state = lane_policy.as_ref().map(|policy| policy.promotion_state);
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

        let intent_mode = target_intent_mode(config, live_order_placement_enabled, promotion_state);
        let bankroll = match intent_mode {
            TradeMode::Paper => paper_remaining,
            TradeMode::Live => live_remaining,
        };
        let theoretical_entry_price = contract_price_for_side(side, snapshot.market_prob);
        let position_cap = (bankroll * max_position_pct_for_mode(config, intent_mode)).max(0.0);
        let raw_size = bankroll
            * edge.abs()
            * model.confidence
            * snapshot.venue_quality_score
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
            let lane_cap = (live_total_bankroll * config.live_max_lane_exposure_pct.max(0.0))
                .max(theoretical_entry_price);
            let symbol_cap = (live_total_bankroll * config.live_max_symbol_exposure_pct.max(0.0))
                .max(theoretical_entry_price);
            if lane_exposure + recommended_size > lane_cap + f64::EPSILON {
                reasons.push("lane_exposure_cap".to_string());
            }
            if symbol_exposure + recommended_size > symbol_cap + f64::EPSILON {
                reasons.push("symbol_exposure_cap".to_string());
            }
            if live_contract_count(recommended_size, theoretical_entry_price) < 1.0 {
                reasons.push("size_too_small_for_live_contract".to_string());
            }
        }
        let approved = reasons.is_empty();

        let inference = ModelInference {
            market_id: snapshot.market_id,
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
                "effective_min_confidence": min_confidence,
                "effective_min_venue_quality": min_venue_quality,
                "effective_min_seconds_to_expiry": min_seconds_to_expiry,
                "reference_yes_prob": snapshot.reference_yes_prob,
                "reference_gap_bps": snapshot.reference_gap_bps,
                "reference_price_change_bps": snapshot.reference_price_change_bps,
                "threshold_distance_bps_proxy": snapshot.threshold_distance_bps_proxy,
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
            }),
        };
        storage.insert_model_inference(&inference).await?;

        let decision = OpportunityDecision {
            market_id: snapshot.market_id,
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

        if approved && !storage.has_open_trade_for_lane(&lane).await? {
            storage
                .insert_execution_intent(
                    decision_id,
                    intent_mode,
                    "midpoint",
                    &[1.0],
                    snapshot.seconds_to_expiry.max(120),
                    45,
                    &["expiry_exit".to_string()],
                )
                .await?;
            match intent_mode {
                TradeMode::Paper => {
                    paper_remaining = (paper_remaining - recommended_size).max(0.0);
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

fn trained_policy_from_artifact(artifact: &TrainedModelArtifactCard) -> Option<TrainedDecisionPolicy> {
    let policy = artifact.metrics_json.get("policy")?;
    Some(TrainedDecisionPolicy {
        min_edge: policy.get("min_edge")?.as_f64()?,
        min_confidence: policy.get("min_confidence")?.as_f64()?,
        min_venue_quality: policy.get("min_venue_quality")?.as_f64()?,
        min_seconds_to_expiry_scaled: policy
            .get("min_seconds_to_expiry_scaled")?
            .as_f64()?,
    })
}

fn uses_trained_linear_weights(model_name: &str) -> bool {
    matches!(
        model_name,
        TRAINED_LINEAR_V1 | TRAINED_LINEAR_CONTRARIAN_V1
    )
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

fn contract_price_for_side(side: &str, market_prob: f64) -> f64 {
    if side == "buy_no" {
        (1.0 - market_prob).clamp(0.01, 0.99)
    } else {
        market_prob.clamp(0.01, 0.99)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, PromotionState, TradeMode, live_contract_count, live_drawdown_triggered,
        max_position_pct_for_mode, target_intent_mode,
    };

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
            kalshi_api_base: "https://example.com".to_string(),
            kalshi_ws_url: "wss://example.com".to_string(),
            kalshi_api_key_id: None,
            kalshi_api_key_id_file: None,
            kalshi_private_key_path: None,
            kalshi_private_key_b64: None,
            v2_reference_sqlite_path: None,
            coinbase_ws_url: "wss://example.com".to_string(),
            paper_trading_enabled: true,
            live_trading_enabled: true,
            live_order_placement_enabled: true,
            discord_webhook_url: None,
            initial_paper_bankroll: 1000.0,
            initial_live_bankroll: 1000.0,
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
}
