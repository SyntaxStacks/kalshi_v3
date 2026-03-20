use crate::ExpiryRegime;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionScoreInputs {
    pub raw_edge_bps: f64,
    pub spread_bps: f64,
    pub aggressive_buy_ratio: f64,
    pub quote_churn: f64,
    pub size_ahead: f64,
    pub trade_consumption_rate: f64,
    pub cancel_rate: f64,
    pub queue_decay_rate: f64,
    pub seconds_to_expiry: f64,
    pub regime: ExpiryRegime,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionScore {
    pub raw_edge_bps: f64,
    pub fill_probability: f64,
    pub expected_slippage_bps: f64,
    pub adverse_selection_bps: f64,
}

pub fn fill_probability_fallback(regime: ExpiryRegime) -> f64 {
    match regime {
        ExpiryRegime::Early => 0.55,
        ExpiryRegime::Mid => 0.35,
        ExpiryRegime::Late => 0.10,
    }
}

pub fn estimate_fill_probability(
    size_ahead: f64,
    trade_consumption_rate: f64,
    cancel_rate: f64,
    seconds_to_expiry: f64,
    regime: ExpiryRegime,
) -> f64 {
    let effective_rate = (trade_consumption_rate + cancel_rate).max(0.0);
    if effective_rate <= f64::EPSILON {
        return fill_probability_fallback(regime);
    }
    let expected_fill_time = size_ahead.max(1.0) / effective_rate;
    if !expected_fill_time.is_finite() || expected_fill_time <= f64::EPSILON {
        return fill_probability_fallback(regime);
    }
    (seconds_to_expiry / expected_fill_time).clamp(0.0, 1.0)
}

pub fn directional_raw_edge_bps(side: &str, model_prob: f64, market_prob: f64) -> f64 {
    if side == "buy_no" {
        ((market_prob - model_prob) * 10_000.0).max(0.0)
    } else {
        ((model_prob - market_prob) * 10_000.0).max(0.0)
    }
}

pub fn build_execution_score(inputs: ExecutionScoreInputs) -> ExecutionScore {
    let fill_probability = estimate_fill_probability(
        inputs.size_ahead,
        inputs.trade_consumption_rate,
        inputs.cancel_rate,
        inputs.seconds_to_expiry,
        inputs.regime,
    );
    let fallback_spread_bps = match inputs.regime {
        ExpiryRegime::Early => 20.0,
        ExpiryRegime::Mid => 30.0,
        ExpiryRegime::Late => 45.0,
    };
    let spread_bps = if inputs.spread_bps > 0.0 {
        inputs.spread_bps
    } else {
        fallback_spread_bps
    };
    let expected_slippage_bps = spread_bps * 0.5;
    let adverse_selection_bps = (inputs.aggressive_buy_ratio * spread_bps)
        + ((inputs.cancel_rate + inputs.queue_decay_rate) * 10.0)
        + (inputs.quote_churn * 5.0);
    ExecutionScore {
        raw_edge_bps: inputs.raw_edge_bps,
        fill_probability,
        expected_slippage_bps,
        adverse_selection_bps,
    }
}

pub fn compute_execution_score_bps(score: &ExecutionScore) -> f64 {
    (score.raw_edge_bps * score.fill_probability)
        - score.expected_slippage_bps
        - score.adverse_selection_bps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_queue_flow_uses_regime_fallbacks() {
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 600.0, ExpiryRegime::Early),
            0.55
        );
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 240.0, ExpiryRegime::Mid),
            0.35
        );
        assert_eq!(
            estimate_fill_probability(10.0, 0.0, 0.0, 30.0, ExpiryRegime::Late),
            0.10
        );
    }

    #[test]
    fn execution_score_uses_same_formula_for_all_callers() {
        let score = build_execution_score(ExecutionScoreInputs {
            raw_edge_bps: 65.0,
            spread_bps: 0.0,
            aggressive_buy_ratio: 0.25,
            quote_churn: 0.1,
            size_ahead: 10.0,
            trade_consumption_rate: 0.0,
            cancel_rate: 0.0,
            queue_decay_rate: 0.0,
            seconds_to_expiry: 480.0,
            regime: ExpiryRegime::Early,
        });
        assert!(compute_execution_score_bps(&score) > 0.0);
        assert_eq!(score.fill_probability, 0.55);
        assert_eq!(score.expected_slippage_bps, 10.0);
    }
}
