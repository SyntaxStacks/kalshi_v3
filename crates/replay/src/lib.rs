use chrono::{DateTime, Utc};
use market_models::{
    FeatureVector, TRAINED_LINEAR_CONTRARIAN_V1, TRAINED_LINEAR_V1, TrainedLinearWeights,
    run_model_with_weights,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySlice {
    pub lane_key: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub forecast_brier: f64,
    pub execution_adjusted_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelBenchmark {
    pub lane_key: String,
    pub model_name: String,
    pub brier: f64,
    pub execution_pnl: f64,
    pub sample_count: usize,
    pub trade_count: usize,
    pub win_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkLeaderboard {
    pub lane_key: String,
    pub results: Vec<ModelBenchmark>,
}

impl BenchmarkLeaderboard {
    pub fn champion(&self) -> Option<&ModelBenchmark> {
        self.results.first()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExample {
    pub lane_key: String,
    pub time_to_expiry_bucket: String,
    pub target_yes_probability: f64,
    pub market_prob: f64,
    pub features: FeatureVector,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReplayPolicy {
    pub min_edge: f64,
    pub min_confidence: f64,
    pub min_venue_quality: f64,
    pub min_seconds_to_expiry_scaled: f64,
    pub execution_cost: f64,
}

impl ReplayPolicy {
    pub fn directional_default() -> Self {
        Self {
            min_edge: 0.04,
            min_confidence: 0.42,
            min_venue_quality: 0.20,
            min_seconds_to_expiry_scaled: 120.0 / 900.0,
            execution_cost: 0.01,
        }
    }
}

pub fn benchmark_model_set(
    lane_key: &str,
    model_names: &[&str],
    examples: &[ReplayExample],
    policy: ReplayPolicy,
    trained_weights: Option<&TrainedLinearWeights>,
    trained_policy: Option<ReplayPolicy>,
) -> Option<BenchmarkLeaderboard> {
    if examples.is_empty() || model_names.is_empty() {
        return None;
    }

    let mut results = Vec::with_capacity(model_names.len());
    for model_name in model_names {
        let effective_policy =
            if *model_name == TRAINED_LINEAR_V1 || *model_name == TRAINED_LINEAR_CONTRARIAN_V1 {
            trained_policy.unwrap_or(policy)
        } else {
            policy
        };
        if let Some(result) =
            benchmark_single_model(lane_key, model_name, examples, effective_policy, trained_weights)
        {
            results.push(result);
        }
    }

    results.sort_by(|left, right| {
        right
            .execution_pnl
            .total_cmp(&left.execution_pnl)
            .then_with(|| left.brier.total_cmp(&right.brier))
            .then_with(|| right.trade_count.cmp(&left.trade_count))
            .then_with(|| left.model_name.cmp(&right.model_name))
    });

    Some(BenchmarkLeaderboard {
        lane_key: lane_key.to_string(),
        results,
    })
}

pub fn benchmark_single_model(
    lane_key: &str,
    model_name: &str,
    examples: &[ReplayExample],
    policy: ReplayPolicy,
    trained_weights: Option<&TrainedLinearWeights>,
) -> Option<ModelBenchmark> {
    if examples.is_empty() {
        return None;
    }

    let mut brier = 0.0;
    let mut execution_pnl = 0.0;
    let mut trade_count = 0usize;
    let mut winning_trades = 0usize;
    for example in examples {
        let output = run_model_with_weights(model_name, &example.features, trained_weights);
        brier += (output.probability_yes - example.target_yes_probability).powi(2);
        let trade_result = execution_delta(
            output.probability_yes,
            output.confidence,
            example.market_prob,
            example.target_yes_probability,
            example.features.venue_quality_score,
            example.features.seconds_to_expiry_scaled,
            policy,
        );
        execution_pnl += trade_result;
        if trade_result.abs() > f64::EPSILON {
            trade_count += 1;
            if trade_result > 0.0 {
                winning_trades += 1;
            }
        }
    }
    let sample_count = examples.len();
    Some(ModelBenchmark {
        lane_key: lane_key.to_string(),
        model_name: model_name.to_string(),
        brier: brier / sample_count as f64,
        execution_pnl,
        sample_count,
        trade_count,
        win_rate: if trade_count > 0 {
            winning_trades as f64 / trade_count as f64
        } else {
            0.0
        },
    })
}

fn execution_delta(
    model_prob: f64,
    confidence: f64,
    market_prob: f64,
    target_yes_probability: f64,
    venue_quality_score: f64,
    seconds_to_expiry_scaled: f64,
    policy: ReplayPolicy,
) -> f64 {
    let edge = model_prob - market_prob;

    if edge.abs() < policy.min_edge
        || confidence < policy.min_confidence
        || venue_quality_score < policy.min_venue_quality
        || seconds_to_expiry_scaled < policy.min_seconds_to_expiry_scaled
    {
        return 0.0;
    }

    let size_multiplier = (confidence * venue_quality_score).clamp(0.10, 1.0)
        * ((edge.abs() - policy.min_edge) / 0.20).clamp(0.15, 1.0);

    if edge >= policy.min_edge {
        ((target_yes_probability - market_prob) - policy.execution_cost) * size_multiplier
    } else if edge <= -policy.min_edge {
        (((1.0 - target_yes_probability) - (1.0 - market_prob)) - policy.execution_cost)
            * size_multiplier
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_models::FeatureVector;

    fn sample_example(market_prob: f64, target_yes_probability: f64, venue_quality_score: f64) -> ReplayExample {
        ReplayExample {
            lane_key: "kalshi:btc:15:buy_yes:directional_settlement:baseline_logit_v1".to_string(),
            time_to_expiry_bucket: "5_to_10m".to_string(),
            target_yes_probability,
            market_prob,
            features: FeatureVector {
                market_prob,
                reference_yes_prob: 0.62,
                reference_gap_bps_scaled: 0.7,
                threshold_distance_bps_scaled: 0.4,
                order_book_imbalance: 0.3,
                aggressive_buy_ratio: 0.66,
                venue_quality_score,
                market_data_age_scaled: 0.05,
                reference_age_scaled: 0.05,
                averaging_window_progress: 0.35,
                seconds_to_expiry_scaled: 0.55,
            },
        }
    }

    #[test]
    fn benchmark_gates_weak_quality_examples() {
        let weak = sample_example(0.50, 0.52, 0.10);
        let lane_key = weak.lane_key.clone();
        let board =
            benchmark_model_set(
                &lane_key,
                &[market_models::BASELINE_LOGIT_V1],
                &[weak],
                ReplayPolicy::directional_default(),
                None,
                None,
            )
            .expect("leaderboard");
        assert_eq!(board.results[0].trade_count, 0);
        assert_eq!(board.results[0].execution_pnl, 0.0);
    }

    #[test]
    fn benchmark_trades_high_quality_examples() {
        let strong = sample_example(0.40, 0.75, 0.85);
        let lane_key = strong.lane_key.clone();
        let board =
            benchmark_model_set(
                &lane_key,
                &[market_models::BASELINE_LOGIT_V1],
                &[strong],
                ReplayPolicy::directional_default(),
                None,
                None,
            )
            .expect("leaderboard");
        assert_eq!(board.results[0].trade_count, 1);
        assert!(board.results[0].execution_pnl > 0.0);
    }

    #[test]
    fn benchmark_gates_near_expiry_examples() {
        let mut late = sample_example(0.40, 0.80, 0.85);
        late.features.seconds_to_expiry_scaled = 60.0 / 900.0;
        let lane_key = late.lane_key.clone();
        let board = benchmark_model_set(
            &lane_key,
            &[market_models::BASELINE_LOGIT_V1],
            &[late],
            ReplayPolicy::directional_default(),
            None,
            None,
        )
        .expect("leaderboard");
        assert_eq!(board.results[0].trade_count, 0);
        assert_eq!(board.results[0].execution_pnl, 0.0);
    }
}
