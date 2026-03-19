use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FeatureVector {
    pub market_prob: f64,
    pub reference_yes_prob: f64,
    pub reference_gap_bps_scaled: f64,
    pub threshold_distance_bps_scaled: f64,
    pub distance_to_strike_bps_scaled: f64,
    pub reference_velocity_scaled: f64,
    pub realized_vol_short_scaled: f64,
    pub time_decay_factor: f64,
    pub order_book_imbalance: f64,
    pub aggressive_buy_ratio: f64,
    pub size_ahead_scaled: f64,
    pub trade_rate_scaled: f64,
    pub quote_churn_scaled: f64,
    pub size_ahead_real_scaled: f64,
    pub trade_consumption_rate_scaled: f64,
    pub cancel_rate_scaled: f64,
    pub queue_decay_rate_scaled: f64,
    pub spread_bps_scaled: f64,
    pub venue_quality_score: f64,
    pub market_data_age_scaled: f64,
    pub reference_age_scaled: f64,
    pub averaging_window_progress: f64,
    pub seconds_to_expiry_scaled: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelOutput {
    pub raw_score: f64,
    pub probability_yes: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TrainedLinearWeights {
    pub bias: f64,
    pub market_prob: f64,
    pub reference_yes_prob: f64,
    pub reference_gap_bps_scaled: f64,
    pub threshold_distance_bps_scaled: f64,
    pub distance_to_strike_bps_scaled: f64,
    pub reference_velocity_scaled: f64,
    pub realized_vol_short_scaled: f64,
    pub time_decay_factor: f64,
    pub order_book_imbalance: f64,
    pub aggressive_buy_ratio: f64,
    pub size_ahead_scaled: f64,
    pub trade_rate_scaled: f64,
    pub quote_churn_scaled: f64,
    pub spread_bps_scaled: f64,
    pub venue_quality_score: f64,
    pub market_data_age_scaled: f64,
    pub reference_age_scaled: f64,
    pub averaging_window_progress: f64,
    pub seconds_to_expiry_scaled: f64,
}

pub const BASELINE_LOGIT_V1: &str = "baseline_logit_v1";
pub const BASELINE_CONTRARIAN_V1: &str = "baseline_contrarian_v1";
pub const FLOW_WEIGHTED_V2: &str = "flow_weighted_v2";
pub const FLOW_WEIGHTED_CONTRARIAN_V2: &str = "flow_weighted_contrarian_v2";
pub const SETTLEMENT_ANCHOR_V3: &str = "settlement_anchor_v3";
pub const SETTLEMENT_ANCHOR_CONTRARIAN_V3: &str = "settlement_anchor_contrarian_v3";
pub const TRAINED_LINEAR_V1: &str = "trained_linear_v1";
pub const TRAINED_LINEAR_CONTRARIAN_V1: &str = "trained_linear_contrarian_v1";

fn invert_output(output: ModelOutput, confidence_boost: f64) -> ModelOutput {
    ModelOutput {
        raw_score: -output.raw_score,
        probability_yes: (1.0 - output.probability_yes).clamp(0.001, 0.999),
        confidence: (output.confidence + confidence_boost).clamp(0.05, 0.95),
    }
}

pub fn baseline_logit(features: &FeatureVector) -> ModelOutput {
    let score = (features.reference_yes_prob - features.market_prob) * 2.2
        + (features.reference_gap_bps_scaled * 0.55)
        + (features.threshold_distance_bps_scaled * 0.35)
        + (features.distance_to_strike_bps_scaled * 0.45)
        + (features.reference_velocity_scaled * 0.28)
        + (features.realized_vol_short_scaled * 0.20)
        + (features.time_decay_factor * 0.12)
        + (features.order_book_imbalance * 0.9)
        + ((features.aggressive_buy_ratio - 0.5) * 0.8)
        - (features.size_ahead_scaled * 0.18)
        + (features.trade_rate_scaled * 0.22)
        - (features.quote_churn_scaled * 0.10)
        - (features.spread_bps_scaled * 0.16)
        + (features.venue_quality_score * 0.7)
        - (features.market_data_age_scaled * 0.45)
        - (features.reference_age_scaled * 0.35)
        + (features.averaging_window_progress * 0.2)
        + (features.seconds_to_expiry_scaled * 0.2);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.6
        + features.venue_quality_score * 0.25
        + features.reference_gap_bps_scaled.abs() * 0.12
        + features.distance_to_strike_bps_scaled.abs() * 0.08
        + features.reference_velocity_scaled.abs() * 0.05
        + features.averaging_window_progress * 0.05
        - features.spread_bps_scaled * 0.06
        - features.market_data_age_scaled * 0.08
        - features.reference_age_scaled * 0.05)
        .clamp(0.05, 0.95);
    ModelOutput {
        raw_score: score,
        probability_yes,
        confidence,
    }
}

pub fn flow_weighted_logit(features: &FeatureVector) -> ModelOutput {
    let score = (features.reference_yes_prob - features.market_prob) * 1.6
        + (features.reference_gap_bps_scaled * 0.45)
        + (features.threshold_distance_bps_scaled * 0.2)
        + (features.distance_to_strike_bps_scaled * 0.34)
        + (features.reference_velocity_scaled * 0.45)
        + (features.realized_vol_short_scaled * 0.16)
        + (features.order_book_imbalance * 1.35)
        + ((features.aggressive_buy_ratio - 0.5) * 1.2)
        - (features.size_ahead_scaled * 0.14)
        + (features.trade_rate_scaled * 0.28)
        - (features.quote_churn_scaled * 0.08)
        - (features.spread_bps_scaled * 0.20)
        + (features.venue_quality_score * 0.55)
        - (features.market_data_age_scaled * 0.4)
        - (features.reference_age_scaled * 0.3)
        + (features.averaging_window_progress * 0.3)
        + ((1.0 - features.seconds_to_expiry_scaled) * 0.35);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.45
        + features.venue_quality_score * 0.22
        + features.order_book_imbalance.abs() * 0.1
        + features.reference_gap_bps_scaled.abs() * 0.1
        + features.trade_rate_scaled * 0.05
        - features.spread_bps_scaled * 0.05
        - features.market_data_age_scaled * 0.08
        - features.reference_age_scaled * 0.05)
        .clamp(0.05, 0.95);
    ModelOutput {
        raw_score: score,
        probability_yes,
        confidence,
    }
}

pub fn settlement_anchor_logit(features: &FeatureVector) -> ModelOutput {
    let score = (features.reference_yes_prob - features.market_prob) * 2.6
        + (features.reference_gap_bps_scaled * 0.7)
        + (features.threshold_distance_bps_scaled * 0.5)
        + (features.distance_to_strike_bps_scaled * 0.52)
        + (features.reference_velocity_scaled * 0.22)
        + (features.realized_vol_short_scaled * 0.18)
        + (features.time_decay_factor * 0.10)
        - (features.size_ahead_scaled * 0.08)
        + (features.trade_rate_scaled * 0.14)
        - (features.quote_churn_scaled * 0.06)
        - (features.spread_bps_scaled * 0.14)
        + (features.venue_quality_score * 0.45)
        + (features.averaging_window_progress * 0.4)
        - (features.market_data_age_scaled * 0.35)
        - (features.reference_age_scaled * 0.25)
        + ((1.0 - features.seconds_to_expiry_scaled) * 0.15);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.5
        + features.reference_gap_bps_scaled.abs() * 0.14
        + features.distance_to_strike_bps_scaled.abs() * 0.08
        + features.venue_quality_score * 0.18
        + features.averaging_window_progress * 0.08
        - features.spread_bps_scaled * 0.05
        - features.market_data_age_scaled * 0.06)
        .clamp(0.05, 0.95);
    ModelOutput {
        raw_score: score,
        probability_yes,
        confidence,
    }
}

pub fn trained_linear(features: &FeatureVector, weights: &TrainedLinearWeights) -> ModelOutput {
    let score = weights.bias
        + (features.market_prob * weights.market_prob)
        + (features.reference_yes_prob * weights.reference_yes_prob)
        + (features.reference_gap_bps_scaled * weights.reference_gap_bps_scaled)
        + (features.threshold_distance_bps_scaled * weights.threshold_distance_bps_scaled)
        + (features.distance_to_strike_bps_scaled * weights.distance_to_strike_bps_scaled)
        + (features.reference_velocity_scaled * weights.reference_velocity_scaled)
        + (features.realized_vol_short_scaled * weights.realized_vol_short_scaled)
        + (features.time_decay_factor * weights.time_decay_factor)
        + (features.order_book_imbalance * weights.order_book_imbalance)
        + (features.aggressive_buy_ratio * weights.aggressive_buy_ratio)
        + (features.size_ahead_scaled * weights.size_ahead_scaled)
        + (features.trade_rate_scaled * weights.trade_rate_scaled)
        + (features.quote_churn_scaled * weights.quote_churn_scaled)
        + (features.spread_bps_scaled * weights.spread_bps_scaled)
        + (features.venue_quality_score * weights.venue_quality_score)
        + (features.market_data_age_scaled * weights.market_data_age_scaled)
        + (features.reference_age_scaled * weights.reference_age_scaled)
        + (features.averaging_window_progress * weights.averaging_window_progress)
        + (features.seconds_to_expiry_scaled * weights.seconds_to_expiry_scaled);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.75
        + features.venue_quality_score * 0.24
        + features.reference_gap_bps_scaled.abs() * 0.08
        + features.distance_to_strike_bps_scaled.abs() * 0.07
        - features.market_data_age_scaled * 0.06
        - features.spread_bps_scaled * 0.04
        - features.reference_age_scaled * 0.04)
        .clamp(0.05, 0.95);
    ModelOutput {
        raw_score: score,
        probability_yes,
        confidence,
    }
}

pub fn supported_models() -> &'static [&'static str] {
    &[
        BASELINE_LOGIT_V1,
        BASELINE_CONTRARIAN_V1,
        FLOW_WEIGHTED_V2,
        FLOW_WEIGHTED_CONTRARIAN_V2,
        SETTLEMENT_ANCHOR_V3,
        SETTLEMENT_ANCHOR_CONTRARIAN_V3,
        TRAINED_LINEAR_V1,
        TRAINED_LINEAR_CONTRARIAN_V1,
    ]
}

pub fn run_model(model_name: &str, features: &FeatureVector) -> ModelOutput {
    run_model_with_weights(model_name, features, None)
}

pub fn run_model_with_weights(
    model_name: &str,
    features: &FeatureVector,
    trained_weights: Option<&TrainedLinearWeights>,
) -> ModelOutput {
    match model_name {
        BASELINE_CONTRARIAN_V1 => invert_output(baseline_logit(features), 0.03),
        FLOW_WEIGHTED_V2 => flow_weighted_logit(features),
        FLOW_WEIGHTED_CONTRARIAN_V2 => invert_output(flow_weighted_logit(features), 0.03),
        SETTLEMENT_ANCHOR_V3 => settlement_anchor_logit(features),
        SETTLEMENT_ANCHOR_CONTRARIAN_V3 => invert_output(settlement_anchor_logit(features), 0.02),
        TRAINED_LINEAR_V1 => trained_weights
            .map(|weights| trained_linear(features, weights))
            .unwrap_or_else(|| baseline_logit(features)),
        TRAINED_LINEAR_CONTRARIAN_V1 => trained_weights
            .map(|weights| invert_output(trained_linear(features, weights), 0.02))
            .unwrap_or_else(|| invert_output(baseline_logit(features), 0.02)),
        _ => baseline_logit(features),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BASELINE_CONTRARIAN_V1, BASELINE_LOGIT_V1, FLOW_WEIGHTED_CONTRARIAN_V2, FLOW_WEIGHTED_V2,
        FeatureVector, SETTLEMENT_ANCHOR_CONTRARIAN_V3, SETTLEMENT_ANCHOR_V3,
        TRAINED_LINEAR_CONTRARIAN_V1, TRAINED_LINEAR_V1, TrainedLinearWeights, baseline_logit,
        run_model, run_model_with_weights, supported_models,
    };

    #[test]
    fn baseline_logit_responds_to_positive_flow() {
        let output = baseline_logit(&FeatureVector {
            market_prob: 0.42,
            reference_yes_prob: 0.56,
            reference_gap_bps_scaled: 0.70,
            threshold_distance_bps_scaled: 0.15,
            distance_to_strike_bps_scaled: 0.18,
            reference_velocity_scaled: 0.24,
            realized_vol_short_scaled: 0.12,
            time_decay_factor: 0.42,
            order_book_imbalance: 0.20,
            aggressive_buy_ratio: 0.66,
            size_ahead_scaled: 0.20,
            trade_rate_scaled: 0.38,
            quote_churn_scaled: 0.10,
            size_ahead_real_scaled: 0.22,
            trade_consumption_rate_scaled: 0.32,
            cancel_rate_scaled: 0.08,
            queue_decay_rate_scaled: 0.12,
            spread_bps_scaled: 0.16,
            venue_quality_score: 0.83,
            market_data_age_scaled: 0.05,
            reference_age_scaled: 0.04,
            averaging_window_progress: 0.25,
            seconds_to_expiry_scaled: 0.45,
        });
        assert!(output.probability_yes > 0.5);
        assert!(output.confidence > 0.2);
    }

    #[test]
    fn model_registry_supports_multiple_variants() {
        let features = FeatureVector {
            market_prob: 0.42,
            reference_yes_prob: 0.56,
            reference_gap_bps_scaled: 0.70,
            threshold_distance_bps_scaled: 0.15,
            distance_to_strike_bps_scaled: 0.18,
            reference_velocity_scaled: 0.24,
            realized_vol_short_scaled: 0.12,
            time_decay_factor: 0.42,
            order_book_imbalance: 0.20,
            aggressive_buy_ratio: 0.66,
            size_ahead_scaled: 0.20,
            trade_rate_scaled: 0.38,
            quote_churn_scaled: 0.10,
            size_ahead_real_scaled: 0.22,
            trade_consumption_rate_scaled: 0.32,
            cancel_rate_scaled: 0.08,
            queue_decay_rate_scaled: 0.12,
            spread_bps_scaled: 0.16,
            venue_quality_score: 0.83,
            market_data_age_scaled: 0.05,
            reference_age_scaled: 0.04,
            averaging_window_progress: 0.25,
            seconds_to_expiry_scaled: 0.45,
        };
        let v1 = run_model(BASELINE_LOGIT_V1, &features);
        let v1_inverse = run_model(BASELINE_CONTRARIAN_V1, &features);
        let v2 = run_model(FLOW_WEIGHTED_V2, &features);
        let v2_inverse = run_model(FLOW_WEIGHTED_CONTRARIAN_V2, &features);
        let v3 = run_model(SETTLEMENT_ANCHOR_V3, &features);
        let v3_inverse = run_model(SETTLEMENT_ANCHOR_CONTRARIAN_V3, &features);
        let trained_inverse = run_model(TRAINED_LINEAR_CONTRARIAN_V1, &features);
        assert!(v1.probability_yes > 0.5);
        assert!(v2.probability_yes > 0.5);
        assert!(v3.probability_yes > 0.5);
        assert!(v1_inverse.probability_yes < 0.5);
        assert!(v2_inverse.probability_yes < 0.5);
        assert!(v3_inverse.probability_yes < 0.5);
        assert!(trained_inverse.probability_yes < 0.5);
        assert_ne!(v1.raw_score, v2.raw_score);
        assert!(supported_models().contains(&SETTLEMENT_ANCHOR_V3));
        assert!(supported_models().contains(&SETTLEMENT_ANCHOR_CONTRARIAN_V3));
        assert!(supported_models().contains(&TRAINED_LINEAR_CONTRARIAN_V1));
    }

    #[test]
    fn trained_linear_uses_supplied_weights() {
        let features = FeatureVector {
            market_prob: 0.40,
            reference_yes_prob: 0.60,
            reference_gap_bps_scaled: 0.25,
            threshold_distance_bps_scaled: 0.10,
            distance_to_strike_bps_scaled: 0.22,
            reference_velocity_scaled: 0.18,
            realized_vol_short_scaled: 0.09,
            time_decay_factor: 0.36,
            order_book_imbalance: 0.15,
            aggressive_buy_ratio: 0.62,
            size_ahead_scaled: 0.24,
            trade_rate_scaled: 0.31,
            quote_churn_scaled: 0.08,
            size_ahead_real_scaled: 0.25,
            trade_consumption_rate_scaled: 0.28,
            cancel_rate_scaled: 0.06,
            queue_decay_rate_scaled: 0.10,
            spread_bps_scaled: 0.14,
            venue_quality_score: 0.85,
            market_data_age_scaled: 0.03,
            reference_age_scaled: 0.04,
            averaging_window_progress: 0.45,
            seconds_to_expiry_scaled: 0.55,
        };
        let weights = TrainedLinearWeights {
            bias: -0.15,
            market_prob: -1.2,
            reference_yes_prob: 1.5,
            reference_gap_bps_scaled: 0.3,
            threshold_distance_bps_scaled: 0.2,
            distance_to_strike_bps_scaled: 0.35,
            reference_velocity_scaled: 0.18,
            realized_vol_short_scaled: 0.07,
            time_decay_factor: 0.10,
            order_book_imbalance: 0.4,
            aggressive_buy_ratio: 0.5,
            size_ahead_scaled: -0.08,
            trade_rate_scaled: 0.16,
            quote_churn_scaled: -0.04,
            spread_bps_scaled: -0.10,
            venue_quality_score: 0.35,
            market_data_age_scaled: -0.2,
            reference_age_scaled: -0.1,
            averaging_window_progress: 0.15,
            seconds_to_expiry_scaled: 0.1,
        };
        let output = run_model_with_weights(TRAINED_LINEAR_V1, &features, Some(&weights));
        assert!(output.probability_yes > 0.5);
        assert!(supported_models().contains(&TRAINED_LINEAR_V1));
    }
}
