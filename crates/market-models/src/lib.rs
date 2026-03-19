use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub market_prob: f64,
    pub reference_yes_prob: f64,
    pub reference_gap_bps_scaled: f64,
    pub threshold_distance_bps_scaled: f64,
    pub order_book_imbalance: f64,
    pub aggressive_buy_ratio: f64,
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

pub const BASELINE_LOGIT_V1: &str = "baseline_logit_v1";
pub const BASELINE_CONTRARIAN_V1: &str = "baseline_contrarian_v1";
pub const FLOW_WEIGHTED_V2: &str = "flow_weighted_v2";
pub const FLOW_WEIGHTED_CONTRARIAN_V2: &str = "flow_weighted_contrarian_v2";
pub const SETTLEMENT_ANCHOR_V3: &str = "settlement_anchor_v3";
pub const SETTLEMENT_ANCHOR_CONTRARIAN_V3: &str = "settlement_anchor_contrarian_v3";

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
        + (features.order_book_imbalance * 0.9)
        + ((features.aggressive_buy_ratio - 0.5) * 0.8)
        + (features.venue_quality_score * 0.7)
        - (features.market_data_age_scaled * 0.45)
        - (features.reference_age_scaled * 0.35)
        + (features.averaging_window_progress * 0.2)
        + (features.seconds_to_expiry_scaled * 0.2);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.6
        + features.venue_quality_score * 0.25
        + features.reference_gap_bps_scaled.abs() * 0.12
        + features.averaging_window_progress * 0.05
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
        + (features.order_book_imbalance * 1.35)
        + ((features.aggressive_buy_ratio - 0.5) * 1.2)
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
        + (features.venue_quality_score * 0.45)
        + (features.averaging_window_progress * 0.4)
        - (features.market_data_age_scaled * 0.35)
        - (features.reference_age_scaled * 0.25)
        + ((1.0 - features.seconds_to_expiry_scaled) * 0.15);
    let probability_yes = 1.0 / (1.0 + (-score).exp());
    let confidence = ((probability_yes - features.market_prob).abs() * 1.5
        + features.reference_gap_bps_scaled.abs() * 0.14
        + features.venue_quality_score * 0.18
        + features.averaging_window_progress * 0.08
        - features.market_data_age_scaled * 0.06)
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
    ]
}

pub fn run_model(model_name: &str, features: &FeatureVector) -> ModelOutput {
    match model_name {
        BASELINE_CONTRARIAN_V1 => invert_output(baseline_logit(features), 0.03),
        FLOW_WEIGHTED_V2 => flow_weighted_logit(features),
        FLOW_WEIGHTED_CONTRARIAN_V2 => invert_output(flow_weighted_logit(features), 0.03),
        SETTLEMENT_ANCHOR_V3 => settlement_anchor_logit(features),
        SETTLEMENT_ANCHOR_CONTRARIAN_V3 => invert_output(settlement_anchor_logit(features), 0.02),
        _ => baseline_logit(features),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BASELINE_CONTRARIAN_V1, BASELINE_LOGIT_V1, FLOW_WEIGHTED_CONTRARIAN_V2,
        FLOW_WEIGHTED_V2, FeatureVector, SETTLEMENT_ANCHOR_CONTRARIAN_V3,
        SETTLEMENT_ANCHOR_V3, baseline_logit, run_model, supported_models,
    };

    #[test]
    fn baseline_logit_responds_to_positive_flow() {
        let output = baseline_logit(&FeatureVector {
            market_prob: 0.42,
            reference_yes_prob: 0.56,
            reference_gap_bps_scaled: 0.70,
            threshold_distance_bps_scaled: 0.15,
            order_book_imbalance: 0.20,
            aggressive_buy_ratio: 0.66,
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
            order_book_imbalance: 0.20,
            aggressive_buy_ratio: 0.66,
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
        assert!(v1.probability_yes > 0.5);
        assert!(v2.probability_yes > 0.5);
        assert!(v3.probability_yes > 0.5);
        assert!(v1_inverse.probability_yes < 0.5);
        assert!(v2_inverse.probability_yes < 0.5);
        assert!(v3_inverse.probability_yes < 0.5);
        assert_ne!(v1.raw_score, v2.raw_score);
        assert!(supported_models().contains(&SETTLEMENT_ANCHOR_V3));
        assert!(supported_models().contains(&SETTLEMENT_ANCHOR_CONTRARIAN_V3));
    }
}
