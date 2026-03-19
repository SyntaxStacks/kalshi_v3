use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneTruthThresholdBand {
    // Minimum acceptable live actual fill-hit rate for the band.
    pub min_actual_fill_hit_rate: f64,
    // Minimum acceptable filled/submitted quantity ratio for the band.
    pub min_filled_quantity_ratio: f64,
    // Maximum tolerated negative gap between realized and predicted fill outcomes.
    pub min_predicted_vs_realized_fill_gap: f64,
    // Maximum tolerated negative gap between live fill behavior and replay diagnostics.
    pub min_live_vs_replay_fill_gap: f64,
    // Minimum replay edge diagnostic required for the band.
    pub min_replay_edge_realization_ratio_diag: f64,
    // Suggested size cap when the band is triggered.
    pub recommended_size_multiplier: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaneTruthRecommendationPolicy {
    // Minimum live sample needed before lane execution truth is actionable.
    pub live_sample_min_terminal_intents: i64,
    pub live_sample_min_predicted_fill_samples: i64,
    pub watch: LaneTruthThresholdBand,
    pub quarantine: LaneTruthThresholdBand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub app_env: String,
    pub api_bind_addr: String,
    pub database_url: String,
    pub redis_url: String,
    pub nats_url: String,
    pub exchange: String,
    pub reference_price_mode: String,
    pub reference_price_source: String,
    pub reference_averaging_window_seconds: u32,
    pub nws_api_base: String,
    pub open_meteo_geocode_api_base: String,
    pub weather_series_category: String,
    pub weather_series_title_patterns: Vec<String>,
    pub weather_series_poll_limit: usize,
    pub weather_reference_refresh_seconds: u64,
    pub kalshi_api_base: String,
    pub kalshi_ws_url: String,
    pub kalshi_api_key_id: Option<String>,
    pub kalshi_api_key_id_file: Option<String>,
    pub kalshi_private_key_path: Option<String>,
    pub kalshi_private_key_b64: Option<String>,
    pub v2_reference_sqlite_path: Option<String>,
    pub coinbase_ws_url: String,
    pub paper_trading_enabled: bool,
    pub weather_paper_trading_enabled: bool,
    pub live_trading_enabled: bool,
    pub live_order_placement_enabled: bool,
    pub discord_webhook_url: Option<String>,
    pub initial_paper_bankroll: f64,
    pub initial_live_bankroll: f64,
    pub initial_paper_crypto_budget: f64,
    pub initial_paper_weather_budget: f64,
    pub initial_live_crypto_budget: f64,
    pub initial_live_weather_budget: f64,
    pub kalshi_series_tickers: Vec<String>,
    pub reference_symbols: Vec<String>,
    pub market_poll_seconds: u64,
    pub feature_poll_seconds: u64,
    pub decision_poll_seconds: u64,
    pub execution_poll_seconds: u64,
    pub historical_import_batch_size: u32,
    pub min_edge: f64,
    pub min_confidence: f64,
    pub min_venue_quality: f64,
    pub max_position_pct: f64,
    pub live_max_position_pct: f64,
    pub live_max_lane_exposure_pct: f64,
    pub live_max_symbol_exposure_pct: f64,
    pub live_portfolio_drawdown_kill_pct: f64,
    pub paper_promotion_max_negative_replay: f64,
    pub paper_promotion_max_negative_pnl: f64,
    pub paper_promotion_max_brier: f64,
    pub live_micro_min_examples: u32,
    pub live_micro_min_pnl: f64,
    pub live_micro_max_brier: f64,
    pub live_scaled_min_examples: u32,
    pub live_scaled_min_pnl: f64,
    pub live_scaled_max_brier: f64,
    pub live_demote_max_negative_pnl: f64,
    pub live_entry_time_in_force: String,
    pub live_exit_time_in_force: String,
    pub live_order_replace_enabled: bool,
    pub live_order_replace_after_seconds: i64,
    pub live_order_stale_after_seconds: i64,
    pub market_stale_after_seconds: i64,
    pub reference_stale_after_seconds: i64,
    pub live_bankroll_stale_after_seconds: i64,
    pub live_sync_stale_after_seconds: i64,
    pub lane_truth_live_sample_min_terminal_intents: i64,
    pub lane_truth_live_sample_min_predicted_fill_samples: i64,
    pub lane_truth_watch_min_actual_fill_hit_rate: f64,
    pub lane_truth_watch_min_filled_quantity_ratio: f64,
    pub lane_truth_watch_min_predicted_vs_realized_fill_gap: f64,
    pub lane_truth_watch_min_live_vs_replay_fill_gap: f64,
    pub lane_truth_watch_min_replay_edge_realization_ratio_diag: f64,
    pub lane_truth_watch_recommended_size_multiplier: f64,
    pub lane_truth_quarantine_min_actual_fill_hit_rate: f64,
    pub lane_truth_quarantine_min_filled_quantity_ratio: f64,
    pub lane_truth_quarantine_min_predicted_vs_realized_fill_gap: f64,
    pub lane_truth_quarantine_min_live_vs_replay_fill_gap: f64,
    pub lane_truth_quarantine_min_replay_edge_realization_ratio_diag: f64,
    pub lane_truth_quarantine_recommended_size_multiplier: f64,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let _ = dotenvy::dotenv();
        let raw = fs::read_to_string("config/default.toml")
            .context("failed to read config/default.toml")?;
        let mut settings =
            toml::from_str::<Self>(&raw).context("failed to deserialize config/default.toml")?;
        settings.apply_env_overrides()?;
        Ok(settings)
    }

    pub fn lane_truth_recommendation_policy(&self) -> LaneTruthRecommendationPolicy {
        LaneTruthRecommendationPolicy {
            live_sample_min_terminal_intents: self.lane_truth_live_sample_min_terminal_intents,
            live_sample_min_predicted_fill_samples: self
                .lane_truth_live_sample_min_predicted_fill_samples,
            watch: LaneTruthThresholdBand {
                min_actual_fill_hit_rate: self.lane_truth_watch_min_actual_fill_hit_rate,
                min_filled_quantity_ratio: self.lane_truth_watch_min_filled_quantity_ratio,
                min_predicted_vs_realized_fill_gap: self
                    .lane_truth_watch_min_predicted_vs_realized_fill_gap,
                min_live_vs_replay_fill_gap: self.lane_truth_watch_min_live_vs_replay_fill_gap,
                min_replay_edge_realization_ratio_diag: self
                    .lane_truth_watch_min_replay_edge_realization_ratio_diag,
                recommended_size_multiplier: self.lane_truth_watch_recommended_size_multiplier,
            },
            quarantine: LaneTruthThresholdBand {
                min_actual_fill_hit_rate: self.lane_truth_quarantine_min_actual_fill_hit_rate,
                min_filled_quantity_ratio: self.lane_truth_quarantine_min_filled_quantity_ratio,
                min_predicted_vs_realized_fill_gap: self
                    .lane_truth_quarantine_min_predicted_vs_realized_fill_gap,
                min_live_vs_replay_fill_gap: self.lane_truth_quarantine_min_live_vs_replay_fill_gap,
                min_replay_edge_realization_ratio_diag: self
                    .lane_truth_quarantine_min_replay_edge_realization_ratio_diag,
                recommended_size_multiplier: self.lane_truth_quarantine_recommended_size_multiplier,
            },
        }
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        self.app_env = env_string("APP_ENV").unwrap_or_else(|| self.app_env.clone());
        self.api_bind_addr =
            env_string("API_BIND_ADDR").unwrap_or_else(|| self.api_bind_addr.clone());
        self.database_url = env_string("DATABASE_URL").unwrap_or_else(|| self.database_url.clone());
        self.redis_url = env_string("REDIS_URL").unwrap_or_else(|| self.redis_url.clone());
        self.nats_url = env_string("NATS_URL").unwrap_or_else(|| self.nats_url.clone());
        self.exchange = env_string("EXCHANGE").unwrap_or_else(|| self.exchange.clone());
        self.reference_price_mode =
            env_string("REFERENCE_PRICE_MODE").unwrap_or_else(|| self.reference_price_mode.clone());
        self.reference_price_source = env_string("REFERENCE_PRICE_SOURCE")
            .unwrap_or_else(|| self.reference_price_source.clone());
        self.reference_averaging_window_seconds = env_parse(
            "REFERENCE_AVERAGING_WINDOW_SECONDS",
            self.reference_averaging_window_seconds,
        )?;
        self.nws_api_base = env_string("NWS_API_BASE").unwrap_or_else(|| self.nws_api_base.clone());
        self.open_meteo_geocode_api_base = env_string("OPEN_METEO_GEOCODE_API_BASE")
            .unwrap_or_else(|| self.open_meteo_geocode_api_base.clone());
        self.weather_series_category = env_string("WEATHER_SERIES_CATEGORY")
            .unwrap_or_else(|| self.weather_series_category.clone());
        self.weather_series_title_patterns = env_csv("WEATHER_SERIES_TITLE_PATTERNS")
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| self.weather_series_title_patterns.clone());
        self.weather_series_poll_limit =
            env_parse("WEATHER_SERIES_POLL_LIMIT", self.weather_series_poll_limit)?;
        self.weather_reference_refresh_seconds = env_parse(
            "WEATHER_REFERENCE_REFRESH_SECONDS",
            self.weather_reference_refresh_seconds,
        )?;
        self.kalshi_api_base =
            env_string("KALSHI_API_BASE").unwrap_or_else(|| self.kalshi_api_base.clone());
        self.kalshi_ws_url =
            env_string("KALSHI_WS_URL").unwrap_or_else(|| self.kalshi_ws_url.clone());
        self.kalshi_api_key_id =
            env_optional_string("KALSHI_API_KEY_ID").or_else(|| self.kalshi_api_key_id.clone());
        self.kalshi_api_key_id_file = env_optional_string("KALSHI_API_KEY_ID_FILE")
            .or_else(|| self.kalshi_api_key_id_file.clone());
        self.kalshi_private_key_path = env_optional_string("KALSHI_PRIVATE_KEY_PATH")
            .or_else(|| self.kalshi_private_key_path.clone());
        self.kalshi_private_key_b64 = env_optional_string("KALSHI_PRIVATE_KEY_B64")
            .or_else(|| self.kalshi_private_key_b64.clone());
        self.v2_reference_sqlite_path = env_optional_string("V2_REFERENCE_SQLITE_PATH")
            .or_else(|| self.v2_reference_sqlite_path.clone());
        self.coinbase_ws_url =
            env_string("COINBASE_WS_URL").unwrap_or_else(|| self.coinbase_ws_url.clone());
        self.paper_trading_enabled =
            env_parse("PAPER_TRADING_ENABLED", self.paper_trading_enabled)?;
        self.weather_paper_trading_enabled = env_parse(
            "WEATHER_PAPER_TRADING_ENABLED",
            self.weather_paper_trading_enabled,
        )?;
        self.live_trading_enabled = env_parse("LIVE_TRADING_ENABLED", self.live_trading_enabled)?;
        self.live_order_placement_enabled = env_parse(
            "LIVE_ORDER_PLACEMENT_ENABLED",
            self.live_order_placement_enabled,
        )?;
        self.discord_webhook_url =
            env_optional_string("DISCORD_WEBHOOK_URL").or_else(|| self.discord_webhook_url.clone());
        self.initial_paper_bankroll =
            env_parse("INITIAL_PAPER_BANKROLL", self.initial_paper_bankroll)?;
        self.initial_live_bankroll =
            env_parse("INITIAL_LIVE_BANKROLL", self.initial_live_bankroll)?;
        self.initial_paper_crypto_budget = env_parse(
            "INITIAL_PAPER_CRYPTO_BUDGET",
            self.initial_paper_crypto_budget,
        )?;
        self.initial_paper_weather_budget = env_parse(
            "INITIAL_PAPER_WEATHER_BUDGET",
            self.initial_paper_weather_budget,
        )?;
        self.initial_live_crypto_budget = env_parse(
            "INITIAL_LIVE_CRYPTO_BUDGET",
            self.initial_live_crypto_budget,
        )?;
        self.initial_live_weather_budget = env_parse(
            "INITIAL_LIVE_WEATHER_BUDGET",
            self.initial_live_weather_budget,
        )?;
        self.kalshi_series_tickers = env_csv("KALSHI_SERIES_TICKERS")
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| self.kalshi_series_tickers.clone());
        self.reference_symbols = env_csv("REFERENCE_SYMBOLS")
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| self.reference_symbols.clone());
        self.market_poll_seconds = env_parse("MARKET_POLL_SECONDS", self.market_poll_seconds)?;
        self.feature_poll_seconds = env_parse("FEATURE_POLL_SECONDS", self.feature_poll_seconds)?;
        self.decision_poll_seconds =
            env_parse("DECISION_POLL_SECONDS", self.decision_poll_seconds)?;
        self.execution_poll_seconds =
            env_parse("EXECUTION_POLL_SECONDS", self.execution_poll_seconds)?;
        self.historical_import_batch_size = env_parse(
            "HISTORICAL_IMPORT_BATCH_SIZE",
            self.historical_import_batch_size,
        )?;
        self.min_edge = env_parse("MIN_EDGE", self.min_edge)?;
        self.min_confidence = env_parse("MIN_CONFIDENCE", self.min_confidence)?;
        self.min_venue_quality = env_parse("MIN_VENUE_QUALITY", self.min_venue_quality)?;
        self.max_position_pct = env_parse("MAX_POSITION_PCT", self.max_position_pct)?;
        self.live_max_position_pct =
            env_parse("LIVE_MAX_POSITION_PCT", self.live_max_position_pct)?;
        self.live_max_lane_exposure_pct = env_parse(
            "LIVE_MAX_LANE_EXPOSURE_PCT",
            self.live_max_lane_exposure_pct,
        )?;
        self.live_max_symbol_exposure_pct = env_parse(
            "LIVE_MAX_SYMBOL_EXPOSURE_PCT",
            self.live_max_symbol_exposure_pct,
        )?;
        self.live_portfolio_drawdown_kill_pct = env_parse(
            "LIVE_PORTFOLIO_DRAWDOWN_KILL_PCT",
            self.live_portfolio_drawdown_kill_pct,
        )?;
        self.paper_promotion_max_negative_replay = env_parse(
            "PAPER_PROMOTION_MAX_NEGATIVE_REPLAY",
            self.paper_promotion_max_negative_replay,
        )?;
        self.paper_promotion_max_negative_pnl = env_parse(
            "PAPER_PROMOTION_MAX_NEGATIVE_PNL",
            self.paper_promotion_max_negative_pnl,
        )?;
        self.paper_promotion_max_brier =
            env_parse("PAPER_PROMOTION_MAX_BRIER", self.paper_promotion_max_brier)?;
        self.live_micro_min_examples =
            env_parse("LIVE_MICRO_MIN_EXAMPLES", self.live_micro_min_examples)?;
        self.live_micro_min_pnl = env_parse("LIVE_MICRO_MIN_PNL", self.live_micro_min_pnl)?;
        self.live_micro_max_brier = env_parse("LIVE_MICRO_MAX_BRIER", self.live_micro_max_brier)?;
        self.live_scaled_min_examples =
            env_parse("LIVE_SCALED_MIN_EXAMPLES", self.live_scaled_min_examples)?;
        self.live_scaled_min_pnl = env_parse("LIVE_SCALED_MIN_PNL", self.live_scaled_min_pnl)?;
        self.live_scaled_max_brier =
            env_parse("LIVE_SCALED_MAX_BRIER", self.live_scaled_max_brier)?;
        self.live_demote_max_negative_pnl = env_parse(
            "LIVE_DEMOTE_MAX_NEGATIVE_PNL",
            self.live_demote_max_negative_pnl,
        )?;
        self.live_entry_time_in_force = env_string("LIVE_ENTRY_TIME_IN_FORCE")
            .unwrap_or_else(|| self.live_entry_time_in_force.clone());
        self.live_exit_time_in_force = env_string("LIVE_EXIT_TIME_IN_FORCE")
            .unwrap_or_else(|| self.live_exit_time_in_force.clone());
        self.live_order_replace_enabled = env_parse(
            "LIVE_ORDER_REPLACE_ENABLED",
            self.live_order_replace_enabled,
        )?;
        self.live_order_replace_after_seconds = env_parse(
            "LIVE_ORDER_REPLACE_AFTER_SECONDS",
            self.live_order_replace_after_seconds,
        )?;
        self.live_order_stale_after_seconds = env_parse(
            "LIVE_ORDER_STALE_AFTER_SECONDS",
            self.live_order_stale_after_seconds,
        )?;
        self.market_stale_after_seconds = env_parse(
            "MARKET_STALE_AFTER_SECONDS",
            self.market_stale_after_seconds,
        )?;
        self.reference_stale_after_seconds = env_parse(
            "REFERENCE_STALE_AFTER_SECONDS",
            self.reference_stale_after_seconds,
        )?;
        self.live_bankroll_stale_after_seconds = env_parse(
            "LIVE_BANKROLL_STALE_AFTER_SECONDS",
            self.live_bankroll_stale_after_seconds,
        )?;
        self.live_sync_stale_after_seconds = env_parse(
            "LIVE_SYNC_STALE_AFTER_SECONDS",
            self.live_sync_stale_after_seconds,
        )?;
        self.lane_truth_live_sample_min_terminal_intents = env_parse(
            "LANE_TRUTH_LIVE_SAMPLE_MIN_TERMINAL_INTENTS",
            self.lane_truth_live_sample_min_terminal_intents,
        )?;
        self.lane_truth_live_sample_min_predicted_fill_samples = env_parse(
            "LANE_TRUTH_LIVE_SAMPLE_MIN_PREDICTED_FILL_SAMPLES",
            self.lane_truth_live_sample_min_predicted_fill_samples,
        )?;
        self.lane_truth_watch_min_actual_fill_hit_rate = env_parse(
            "LANE_TRUTH_WATCH_MIN_ACTUAL_FILL_HIT_RATE",
            self.lane_truth_watch_min_actual_fill_hit_rate,
        )?;
        self.lane_truth_watch_min_filled_quantity_ratio = env_parse(
            "LANE_TRUTH_WATCH_MIN_FILLED_QUANTITY_RATIO",
            self.lane_truth_watch_min_filled_quantity_ratio,
        )?;
        self.lane_truth_watch_min_predicted_vs_realized_fill_gap = env_parse(
            "LANE_TRUTH_WATCH_MIN_PREDICTED_VS_REALIZED_FILL_GAP",
            self.lane_truth_watch_min_predicted_vs_realized_fill_gap,
        )?;
        self.lane_truth_watch_min_live_vs_replay_fill_gap = env_parse(
            "LANE_TRUTH_WATCH_MIN_LIVE_VS_REPLAY_FILL_GAP",
            self.lane_truth_watch_min_live_vs_replay_fill_gap,
        )?;
        self.lane_truth_watch_min_replay_edge_realization_ratio_diag = env_parse(
            "LANE_TRUTH_WATCH_MIN_REPLAY_EDGE_REALIZATION_RATIO_DIAG",
            self.lane_truth_watch_min_replay_edge_realization_ratio_diag,
        )?;
        self.lane_truth_watch_recommended_size_multiplier = env_parse(
            "LANE_TRUTH_WATCH_RECOMMENDED_SIZE_MULTIPLIER",
            self.lane_truth_watch_recommended_size_multiplier,
        )?;
        self.lane_truth_quarantine_min_actual_fill_hit_rate = env_parse(
            "LANE_TRUTH_QUARANTINE_MIN_ACTUAL_FILL_HIT_RATE",
            self.lane_truth_quarantine_min_actual_fill_hit_rate,
        )?;
        self.lane_truth_quarantine_min_filled_quantity_ratio = env_parse(
            "LANE_TRUTH_QUARANTINE_MIN_FILLED_QUANTITY_RATIO",
            self.lane_truth_quarantine_min_filled_quantity_ratio,
        )?;
        self.lane_truth_quarantine_min_predicted_vs_realized_fill_gap = env_parse(
            "LANE_TRUTH_QUARANTINE_MIN_PREDICTED_VS_REALIZED_FILL_GAP",
            self.lane_truth_quarantine_min_predicted_vs_realized_fill_gap,
        )?;
        self.lane_truth_quarantine_min_live_vs_replay_fill_gap = env_parse(
            "LANE_TRUTH_QUARANTINE_MIN_LIVE_VS_REPLAY_FILL_GAP",
            self.lane_truth_quarantine_min_live_vs_replay_fill_gap,
        )?;
        self.lane_truth_quarantine_min_replay_edge_realization_ratio_diag = env_parse(
            "LANE_TRUTH_QUARANTINE_MIN_REPLAY_EDGE_REALIZATION_RATIO_DIAG",
            self.lane_truth_quarantine_min_replay_edge_realization_ratio_diag,
        )?;
        self.lane_truth_quarantine_recommended_size_multiplier = env_parse(
            "LANE_TRUTH_QUARANTINE_RECOMMENDED_SIZE_MULTIPLIER",
            self.lane_truth_quarantine_recommended_size_multiplier,
        )?;
        Ok(())
    }
}

fn env_string(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn env_optional_string(name: &str) -> Option<String> {
    env_string(name)
}

fn env_csv(name: &str) -> Option<Vec<String>> {
    env_string(name).map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn env_parse<T>(name: &str, fallback: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env_string(name) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("failed to parse env var {name}")),
        None => Ok(fallback),
    }
}
