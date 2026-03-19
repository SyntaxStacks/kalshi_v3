use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum StrategyFamily {
    Portfolio,
    DirectionalSettlement,
    PreSettlementScalp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum MarketFamily {
    All,
    Crypto,
    Weather,
}

impl Default for MarketFamily {
    fn default() -> Self {
        Self::Crypto
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PromotionState {
    Shadow,
    PaperActive,
    LiveMicro,
    LiveScaled,
    Quarantined,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum TradeMode {
    Paper,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub service: String,
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInference {
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub strategy_family: StrategyFamily,
    pub model_name: String,
    pub raw_score: f64,
    pub raw_probability_yes: f64,
    pub calibrated_probability_yes: f64,
    pub raw_confidence: f64,
    pub calibrated_confidence: f64,
    pub feature_version: String,
    pub rationale_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeatureSnapshotRecord {
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub market_ticker: String,
    pub market_title: String,
    pub feature_version: String,
    pub exchange: String,
    pub symbol: String,
    pub window_minutes: i32,
    pub seconds_to_expiry: i32,
    pub time_to_expiry_bucket: String,
    pub market_prob: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub liquidity: f64,
    pub order_book_imbalance: f64,
    pub aggressive_buy_ratio: f64,
    pub spread_bps: f64,
    pub venue_quality_score: f64,
    pub reference_price: f64,
    pub reference_previous_price: f64,
    pub reference_price_change_bps: f64,
    pub reference_yes_prob: f64,
    pub reference_gap_bps: f64,
    pub threshold_distance_bps_proxy: f64,
    pub distance_to_strike_bps: f64,
    pub reference_velocity: f64,
    pub realized_vol_short: f64,
    pub time_decay_factor: f64,
    pub size_ahead: f64,
    pub trade_rate: f64,
    pub quote_churn: f64,
    pub size_ahead_real: f64,
    pub trade_consumption_rate: f64,
    pub cancel_rate: f64,
    pub queue_decay_rate: f64,
    pub averaging_window_progress: f64,
    pub settlement_regime: String,
    pub last_minute_avg_proxy: f64,
    pub market_data_age_seconds: i32,
    pub reference_age_seconds: i32,
    pub weather_city: Option<String>,
    pub weather_contract_kind: Option<String>,
    pub weather_market_date: Option<String>,
    pub weather_strike_type: Option<String>,
    pub weather_floor_strike: Option<f64>,
    pub weather_cap_strike: Option<f64>,
    pub weather_forecast_temperature_f: Option<f64>,
    pub weather_observation_temperature_f: Option<f64>,
    pub weather_reference_confidence: Option<f64>,
    pub weather_reference_source: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarketState {
    pub market_id: i64,
    #[serde(default)]
    pub market_family: MarketFamily,
    pub market_ticker: String,
    pub market_title: String,
    #[serde(default)]
    pub series_ticker: Option<String>,
    pub symbol: String,
    pub window_minutes: i32,
    pub market_prob: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_price: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub liquidity: f64,
    #[serde(default = "default_metadata_json")]
    pub metadata_json: serde_json::Value,
    pub close_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferencePriceState {
    #[serde(default)]
    pub market_family: MarketFamily,
    pub source: String,
    pub symbol: String,
    pub price: f64,
    pub previous_price: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherReferenceState {
    #[serde(default = "default_weather_market_family")]
    pub market_family: MarketFamily,
    pub reference_key: String,
    pub city: String,
    pub contract_kind: String,
    pub market_date: String,
    pub forecast_temperature_f: f64,
    pub observation_temperature_f: Option<f64>,
    pub confidence_score: f64,
    pub source: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityDecision {
    pub market_id: i64,
    pub market_family: MarketFamily,
    pub lane_key: String,
    pub strategy_family: StrategyFamily,
    pub model_name: String,
    pub side: String,
    pub market_prob: f64,
    pub model_prob: f64,
    pub edge: f64,
    pub confidence: f64,
    pub approved: bool,
    pub reasons_json: Vec<String>,
    pub recommended_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionIntent {
    pub decision_id: i64,
    pub mode: TradeMode,
    pub entry_style: String,
    pub target_ladder_json: Vec<f64>,
    pub timeout_seconds: i32,
    pub force_exit_buffer_seconds: i32,
    pub stop_conditions_json: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneState {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankrollCard {
    pub scope: String,
    pub market_family: MarketFamily,
    pub mode: TradeMode,
    pub strategy_family: StrategyFamily,
    pub bankroll: f64,
    pub deployable_balance: f64,
    pub open_exposure: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub as_of: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessSummary {
    pub overall_status: String,
    pub shadow_count: i64,
    pub paper_active_count: i64,
    pub live_micro_count: i64,
    pub live_scaled_count: i64,
    pub quarantined_count: i64,
    pub lanes: Vec<LaneState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTradeSummary {
    pub trade_id: i64,
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub strategy_family: StrategyFamily,
    pub mode: TradeMode,
    pub market_ticker: Option<String>,
    pub market_title: Option<String>,
    pub weather_city: Option<String>,
    pub weather_contract_kind: Option<String>,
    pub weather_market_date: Option<String>,
    pub weather_strike_type: Option<String>,
    pub weather_floor_strike: Option<f64>,
    pub weather_cap_strike: Option<f64>,
    pub quantity: f64,
    pub entry_price: f64,
    pub created_at: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityCard {
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub strategy_family: StrategyFamily,
    pub side: String,
    pub market_prob: f64,
    pub model_prob: f64,
    pub edge: f64,
    pub confidence: f64,
    pub approved: bool,
    pub reasons: Vec<String>,
    pub execution_status: Option<String>,
    pub execution_note: Option<String>,
    pub as_of: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveExchangeSyncSummary {
    pub synced_at: DateTime<Utc>,
    pub positions_count: i64,
    pub resting_orders_count: i64,
    pub recent_fills_count: i64,
    pub local_open_live_trades_count: i64,
    pub status: String,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorControlState {
    pub live_order_placement_enabled: bool,
    pub updated_by: Option<String>,
    pub note: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeAlarm {
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CriticalAlertNotification {
    pub alert_key: String,
    pub headline: String,
    pub severity: String,
    pub reason: String,
    pub subsystem: String,
    pub affected_lane: Option<String>,
    pub current_state: Option<String>,
    pub next_action: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorActionEvent {
    pub id: i64,
    pub action: String,
    pub actor: Option<String>,
    pub note: Option<String>,
    pub payload_json: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePositionCard {
    pub market_ticker: String,
    pub position_count: f64,
    pub resting_order_count: i32,
    pub market_exposure: f64,
    pub realized_pnl: f64,
    pub synced_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveOrderCard {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_ticker: Option<String>,
    pub action: Option<String>,
    pub side: Option<String>,
    pub status: Option<String>,
    pub count: f64,
    pub fill_count: f64,
    pub remaining_count: f64,
    pub created_time: Option<DateTime<Utc>>,
    pub synced_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveFillCard {
    pub fill_id: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub market_ticker: Option<String>,
    pub action: Option<String>,
    pub side: Option<String>,
    pub count: f64,
    pub fee_paid: f64,
    pub created_time: Option<DateTime<Utc>>,
    pub synced_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTradeExceptionCard {
    pub trade_id: i64,
    pub lane_key: String,
    pub market_ticker: String,
    pub issue: String,
    pub has_position: bool,
    pub has_resting_order: bool,
    pub matched_exit_fill_quantity: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveIntentCard {
    pub intent_id: i64,
    pub lane_key: String,
    pub mode: TradeMode,
    pub status: String,
    pub last_error: Option<String>,
    pub market_ticker: Option<String>,
    pub side: Option<String>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub fill_status: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_transition_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveExceptionSnapshot {
    pub operator_control: Option<OperatorControlState>,
    pub positions: Vec<LivePositionCard>,
    pub orders: Vec<LiveOrderCard>,
    pub recent_fills: Vec<LiveFillCard>,
    pub trade_exceptions: Vec<LiveTradeExceptionCard>,
    pub live_intents: Vec<LiveIntentCard>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneTradeCard {
    pub trade_id: i64,
    pub market_family: MarketFamily,
    pub status: String,
    pub mode: TradeMode,
    pub quantity: f64,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub created_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBenchmarkCard {
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
    pub source: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneReplaySummary {
    pub example_count: i32,
    pub source: String,
    pub created_at: DateTime<Utc>,
    pub benchmarks: Vec<ReplayBenchmarkCard>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneInspectionSnapshot {
    pub lane_key: String,
    pub market_family: MarketFamily,
    pub lane_state: Option<LaneState>,
    pub recent_opportunities: Vec<OpportunityCard>,
    pub recent_trades: Vec<LaneTradeCard>,
    pub replay_summary: Option<LaneReplaySummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionQualitySummary {
    pub as_of: DateTime<Utc>,
    pub replay_lane_count: i64,
    pub replay_trade_count: i64,
    pub replay_edge_realization_ratio: f64,
    pub replay_fill_rate: f64,
    pub replay_slippage_bps: f64,
    pub recent_intent_count: i64,
    pub recent_filled_count: i64,
    pub recent_expected_fill_intent_count: i64,
    pub recent_expected_fill_probability: f64,
    pub recent_actual_fill_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    pub market_family: Option<MarketFamily>,
    pub bankrolls: Vec<BankrollCard>,
    pub readiness: ReadinessSummary,
    pub open_trades: Vec<OpenTradeSummary>,
    pub opportunities: Vec<OpportunityCard>,
    pub execution_quality: ExecutionQualitySummary,
    pub live_sync: Option<LiveExchangeSyncSummary>,
    pub live_exceptions: LiveExceptionSnapshot,
}

fn default_metadata_json() -> serde_json::Value {
    serde_json::json!({})
}

fn default_weather_market_family() -> MarketFamily {
    MarketFamily::Weather
}

pub fn lane_key(
    exchange: &str,
    symbol: &str,
    window_minutes: u32,
    side: &str,
    strategy_family: StrategyFamily,
    model_name: &str,
) -> String {
    let family = match strategy_family {
        StrategyFamily::Portfolio => "portfolio",
        StrategyFamily::DirectionalSettlement => "directional_settlement",
        StrategyFamily::PreSettlementScalp => "pre_settlement_scalp",
    };
    format!(
        "{}:{}:{}:{}:{}:{}",
        exchange, symbol, window_minutes, side, family, model_name
    )
}

#[cfg(test)]
mod tests {
    use super::{StrategyFamily, lane_key};

    #[test]
    fn lane_key_is_stable() {
        assert_eq!(
            lane_key(
                "kalshi",
                "btc",
                15,
                "buy_yes",
                StrategyFamily::DirectionalSettlement,
                "baseline_logit_v1"
            ),
            "kalshi:btc:15:buy_yes:directional_settlement:baseline_logit_v1"
        );
    }
}
