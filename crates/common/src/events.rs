use serde::{Deserialize, Serialize};

pub mod subjects {
    pub const KALSHI_MARKET_TICK: &str = "kalshi.market_tick";
    pub const KALSHI_ORDERBOOK_DELTA: &str = "kalshi.orderbook_delta";
    pub const KALSHI_TRADE_PRINT: &str = "kalshi.trade_print";
    pub const COINBASE_MARKET_TICK: &str = "coinbase.market_tick";
    pub const REFERENCE_PRICE_TICK: &str = "reference.price_tick";
    pub const FEATURE_SNAPSHOT_CREATED: &str = "feature.snapshot.created";
    pub const MODEL_INFERENCE_CREATED: &str = "model.inference.created";
    pub const DECISION_CREATED: &str = "decision.created";
    pub const EXECUTION_INTENT_CREATED: &str = "execution.intent.created";
    pub const EXECUTION_FILL_RECORDED: &str = "execution.fill.recorded";
    pub const TRADE_LIFECYCLE_UPDATED: &str = "trade.lifecycle.updated";
    pub const LANE_STATE_UPDATED: &str = "lane.state.updated";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferencePriceTick {
    pub source: String,
    pub symbol: String,
    pub price: f64,
    pub averaging_window_seconds: u32,
    pub quality_score: f64,
}
