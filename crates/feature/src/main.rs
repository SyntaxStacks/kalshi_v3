use anyhow::Result;
use chrono::Utc;
use common::{
    AppConfig, KalshiMarketState, MarketFamily, MarketFeatureSnapshotRecord, ReferencePriceState,
    WeatherReferenceState,
};
use market_data::QueueSnapshot;
use redis::AsyncCommands;
use tracing::{info, warn};

const REDIS_MARKETS_KEY: &str = "v3:kalshi:markets";
const REDIS_QUEUE_PREFIX: &str = "v3:queue:";
const REDIS_WEATHER_REFERENCE_PREFIX: &str = "v3:weather:reference:";
const CRYPTO_FEATURE_VERSION: &str = "v3_ws_feature_v3";
const WEATHER_FEATURE_VERSION: &str = "v3_weather_feature_v1";

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let storage = storage::Storage::connect(&config.database_url).await?;
    storage.migrate().await?;
    let redis_client = redis::Client::open(config.redis_url.clone())?;
    storage
        .upsert_worker_started("feature", &serde_json::json!({}))
        .await?;

    info!("feature_worker_started");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        config.feature_poll_seconds.max(5),
    ));
    loop {
        interval.tick().await;
        match materialize_once(&config, &storage, &redis_client).await {
            Ok(written) => {
                storage
                    .upsert_worker_success(
                        "feature",
                        &serde_json::json!({"snapshot_count": written}),
                    )
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("feature", &error.to_string(), &serde_json::json!({}))
                    .await?;
                warn!(error = %error, "feature_materialization_failed");
            }
        }
    }
}

async fn materialize_once(
    config: &AppConfig,
    storage: &storage::Storage,
    redis_client: &redis::Client,
) -> Result<usize> {
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let raw_markets: Option<String> = redis.get(REDIS_MARKETS_KEY).await.ok();
    let Some(raw_markets) = raw_markets else {
        return Ok(0);
    };
    let markets: Vec<KalshiMarketState> = serde_json::from_str(&raw_markets)?;
    let now = Utc::now();
    let mut written = 0usize;

    for market in markets {
        let seconds_to_expiry = (market.close_at - now).num_seconds().max(0) as i32;
        let spread = ((market.best_ask - market.best_bid).max(0.0) * 10_000.0).round();
        let depth_total = market.bid_size + market.ask_size;
        let order_book_imbalance = if depth_total > 0.0 {
            ((market.bid_size - market.ask_size) / depth_total).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let aggressive_buy_ratio = (0.5 + (order_book_imbalance * 0.35)).clamp(0.05, 0.95);
        let market_data_age_seconds = (now - market.created_at).num_seconds().max(0) as i32;
        let snapshot = match market.market_family {
            MarketFamily::Weather => {
                let metadata = weather_metadata(&market);
                let reference_key = metadata
                    .as_ref()
                    .map(|meta| weather_reference_key(meta))
                    .unwrap_or_default();
                let raw_reference: Option<String> = if reference_key.is_empty() {
                    None
                } else {
                    redis
                        .get(format!("{REDIS_WEATHER_REFERENCE_PREFIX}{reference_key}"))
                        .await
                        .ok()
                };
                let reference = raw_reference
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<WeatherReferenceState>(raw).ok());
                build_weather_snapshot(
                    &market,
                    seconds_to_expiry,
                    spread,
                    order_book_imbalance,
                    aggressive_buy_ratio,
                    market_data_age_seconds,
                    now,
                    reference,
                    metadata,
                )
            }
            _ => {
                let reference_key = format!("v3:reference:{}", market.symbol.to_uppercase());
                let raw_reference: Option<String> = redis.get(reference_key).await.ok();
                let reference = raw_reference
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<ReferencePriceState>(raw).ok());
                let raw_queue: Option<String> = redis
                    .get(format!("{REDIS_QUEUE_PREFIX}{}", market.market_ticker))
                    .await
                    .ok();
                let queue_snapshot = raw_queue
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<QueueSnapshot>(raw).ok());
                build_crypto_snapshot(
                    &market,
                    seconds_to_expiry,
                    spread,
                    depth_total,
                    order_book_imbalance,
                    aggressive_buy_ratio,
                    market_data_age_seconds,
                    now,
                    reference,
                    queue_snapshot,
                )
            }
        };
        storage.insert_feature_snapshot(&snapshot).await?;
        written += 1;
    }

    info!(
        snapshot_count = written,
        "feature_materialization_succeeded"
    );
    let _ = config;
    Ok(written)
}

fn build_crypto_snapshot(
    market: &KalshiMarketState,
    seconds_to_expiry: i32,
    spread: f64,
    depth_total: f64,
    order_book_imbalance: f64,
    aggressive_buy_ratio: f64,
    market_data_age_seconds: i32,
    now: chrono::DateTime<Utc>,
    reference: Option<ReferencePriceState>,
    queue_snapshot: Option<QueueSnapshot>,
) -> MarketFeatureSnapshotRecord {
    let strike_price = crypto_strike_price(market).unwrap_or(0.0);
    let (
        reference_price,
        reference_previous_price,
        reference_price_change_bps,
        reference_yes_prob,
        reference_age_seconds,
        last_minute_avg_proxy,
        threshold_distance_bps_proxy,
    ) = if let Some(reference) = reference {
        let price_change_bps = if reference.previous_price > 0.0 {
            ((reference.price - reference.previous_price) / reference.previous_price) * 10_000.0
        } else {
            0.0
        };
        let momentum_bias = (price_change_bps / 200.0).clamp(-0.2, 0.2);
        let reference_age_seconds = (now - reference.updated_at).num_seconds().max(0) as i32;
        let last_minute_avg_proxy = if reference.previous_price > 0.0 {
            ((reference.price * 0.7) + (reference.previous_price * 0.3)).max(0.0)
        } else {
            reference.price
        };
        let threshold_distance_bps_proxy = if last_minute_avg_proxy > 0.0 {
            ((reference.price - last_minute_avg_proxy) / last_minute_avg_proxy) * 10_000.0
        } else {
            0.0
        };
        (
            reference.price,
            reference.previous_price,
            price_change_bps,
            (market.market_prob + momentum_bias).clamp(0.01, 0.99),
            reference_age_seconds,
            last_minute_avg_proxy,
            threshold_distance_bps_proxy,
        )
    } else {
        (0.0, 0.0, 0.0, market.market_prob, 9_999, 0.0, 0.0)
    };
    let distance_to_strike_bps = if strike_price > 0.0 && reference_price > 0.0 {
        ((reference_price - strike_price) / strike_price) * 10_000.0
    } else {
        threshold_distance_bps_proxy
    };
    let reference_velocity = (reference_price - reference_previous_price) / 5.0;
    let realized_vol_short = stddev(&[
        reference_previous_price,
        last_minute_avg_proxy,
        reference_price,
    ]);
    let time_decay_factor = (-0.0025 * seconds_to_expiry as f64).exp().clamp(0.0, 1.0);
    let queue_features = queue_features(
        queue_snapshot.as_ref(),
        depth_total,
        spread,
        market_data_age_seconds,
    );
    let size_ahead = queue_features.size_ahead_real.max(market.ask_size.max(1.0));
    let trade_rate = queue_features.trade_consumption_rate;
    let quote_churn = queue_features.quote_churn;
    let reference_gap_bps = (reference_yes_prob - market.market_prob) * 10_000.0;
    let freshness_score = (1.0 - (market_data_age_seconds as f64 / 20.0)).clamp(0.0, 1.0) * 0.6
        + (1.0 - (reference_age_seconds as f64 / 20.0)).clamp(0.0, 1.0) * 0.4;
    let spread_score = (1.0 - (spread / 1_500.0)).clamp(0.0, 1.0);
    let depth_score = (depth_total / (depth_total + 500.0)).clamp(0.0, 1.0);
    let stability_score = (1.0 - (reference_price_change_bps.abs() / 250.0)).clamp(0.0, 1.0);
    let settlement_regime = settlement_regime(seconds_to_expiry).to_string();
    let averaging_window_progress = if seconds_to_expiry <= 60 {
        ((60 - seconds_to_expiry) as f64 / 60.0).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let venue_quality_score = (spread_score * 0.35
        + depth_score * 0.20
        + freshness_score * 0.30
        + stability_score * 0.15)
        .clamp(0.05, 0.99);

    MarketFeatureSnapshotRecord {
        market_id: market.market_id,
        market_family: market.market_family,
        market_ticker: market.market_ticker.clone(),
        market_title: market.market_title.clone(),
        feature_version: CRYPTO_FEATURE_VERSION.to_string(),
        exchange: "kalshi".to_string(),
        symbol: market.symbol.clone(),
        window_minutes: market.window_minutes,
        seconds_to_expiry,
        time_to_expiry_bucket: expiry_bucket(seconds_to_expiry).to_string(),
        market_prob: market.market_prob,
        best_bid: market.best_bid,
        best_ask: market.best_ask,
        last_price: market.last_price,
        bid_size: market.bid_size,
        ask_size: market.ask_size,
        liquidity: market.liquidity,
        order_book_imbalance,
        aggressive_buy_ratio,
        spread_bps: spread,
        venue_quality_score,
        reference_price,
        reference_previous_price,
        reference_price_change_bps,
        reference_yes_prob,
        reference_gap_bps,
        threshold_distance_bps_proxy,
        distance_to_strike_bps,
        reference_velocity,
        realized_vol_short,
        time_decay_factor,
        size_ahead,
        trade_rate,
        quote_churn,
        size_ahead_real: queue_features.size_ahead_real,
        trade_consumption_rate: queue_features.trade_consumption_rate,
        cancel_rate: queue_features.cancel_rate,
        queue_decay_rate: queue_features.queue_decay_rate,
        averaging_window_progress,
        settlement_regime,
        last_minute_avg_proxy,
        market_data_age_seconds,
        reference_age_seconds,
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
        created_at: now,
    }
}

fn build_weather_snapshot(
    market: &KalshiMarketState,
    seconds_to_expiry: i32,
    spread: f64,
    order_book_imbalance: f64,
    aggressive_buy_ratio: f64,
    market_data_age_seconds: i32,
    now: chrono::DateTime<Utc>,
    reference: Option<WeatherReferenceState>,
    metadata: Option<WeatherMarketMetadata>,
) -> MarketFeatureSnapshotRecord {
    let depth_total = market.bid_size + market.ask_size;
    let spread_score = (1.0 - (spread / 2_500.0)).clamp(0.0, 1.0);
    let depth_score = (depth_total / (depth_total + 150.0)).clamp(0.0, 1.0);
    let freshness_score = (1.0 - (market_data_age_seconds as f64 / 60.0)).clamp(0.0, 1.0);
    let (reference_age_seconds, confidence_score, forecast_temp, observation_temp, source) =
        if let Some(reference) = &reference {
            (
                (now - reference.updated_at).num_seconds().max(0) as i32,
                reference.confidence_score,
                reference.forecast_temperature_f,
                reference.observation_temperature_f,
                Some(reference.source.clone()),
            )
        } else {
            (9_999, 0.05, 0.0, None, None)
        };
    let meta = metadata.unwrap_or_else(|| WeatherMarketMetadata {
        city: market.symbol.clone(),
        contract_kind: "daily_temperature".to_string(),
        market_date: now.date_naive().format("%Y-%m-%d").to_string(),
        strike_type: "between".to_string(),
        floor_strike: None,
        cap_strike: None,
    });
    let probability_yes = weather_probability_yes(&meta, forecast_temp, confidence_score);
    let reference_gap_bps = (probability_yes - market.market_prob) * 10_000.0;
    let threshold_distance = weather_threshold_distance(&meta, forecast_temp);
    let venue_quality_score = (spread_score * 0.30
        + depth_score * 0.15
        + freshness_score * 0.20
        + confidence_score * 0.35)
        .clamp(0.05, 0.99);
    let settlement_regime = weather_settlement_regime(seconds_to_expiry).to_string();
    let averaging_window_progress = (1.0 - (seconds_to_expiry as f64 / 86_400.0)).clamp(0.0, 1.0);

    MarketFeatureSnapshotRecord {
        market_id: market.market_id,
        market_family: MarketFamily::Weather,
        market_ticker: market.market_ticker.clone(),
        market_title: market.market_title.clone(),
        feature_version: WEATHER_FEATURE_VERSION.to_string(),
        exchange: "kalshi".to_string(),
        symbol: market.symbol.clone(),
        window_minutes: market.window_minutes,
        seconds_to_expiry,
        time_to_expiry_bucket: weather_expiry_bucket(seconds_to_expiry).to_string(),
        market_prob: market.market_prob,
        best_bid: market.best_bid,
        best_ask: market.best_ask,
        last_price: market.last_price,
        bid_size: market.bid_size,
        ask_size: market.ask_size,
        liquidity: market.liquidity,
        order_book_imbalance,
        aggressive_buy_ratio,
        spread_bps: spread,
        venue_quality_score,
        reference_price: forecast_temp,
        reference_previous_price: observation_temp.unwrap_or(forecast_temp),
        reference_price_change_bps: 0.0,
        reference_yes_prob: probability_yes,
        reference_gap_bps,
        threshold_distance_bps_proxy: threshold_distance * 100.0,
        distance_to_strike_bps: threshold_distance * 100.0,
        reference_velocity: 0.0,
        realized_vol_short: 0.0,
        time_decay_factor: (-0.0001 * seconds_to_expiry as f64).exp().clamp(0.0, 1.0),
        size_ahead: market.ask_size.max(1.0),
        trade_rate: 0.0,
        quote_churn: 0.0,
        size_ahead_real: market.ask_size.max(1.0),
        trade_consumption_rate: 0.0,
        cancel_rate: 0.0,
        queue_decay_rate: 0.0,
        averaging_window_progress,
        settlement_regime,
        last_minute_avg_proxy: observation_temp.unwrap_or(forecast_temp),
        market_data_age_seconds,
        reference_age_seconds,
        weather_city: Some(meta.city.clone()),
        weather_contract_kind: Some(meta.contract_kind.clone()),
        weather_market_date: Some(meta.market_date.clone()),
        weather_strike_type: Some(meta.strike_type.clone()),
        weather_floor_strike: meta.floor_strike,
        weather_cap_strike: meta.cap_strike,
        weather_forecast_temperature_f: if forecast_temp > 0.0 {
            Some(forecast_temp)
        } else {
            None
        },
        weather_observation_temperature_f: observation_temp,
        weather_reference_confidence: Some(confidence_score),
        weather_reference_source: source,
        created_at: now,
    }
}

#[derive(Clone)]
struct WeatherMarketMetadata {
    city: String,
    contract_kind: String,
    market_date: String,
    strike_type: String,
    floor_strike: Option<f64>,
    cap_strike: Option<f64>,
}

fn weather_metadata(market: &KalshiMarketState) -> Option<WeatherMarketMetadata> {
    if market.market_family != MarketFamily::Weather {
        return None;
    }
    Some(WeatherMarketMetadata {
        city: market.metadata_json.get("city")?.as_str()?.to_string(),
        contract_kind: market
            .metadata_json
            .get("contract_kind")?
            .as_str()?
            .to_string(),
        market_date: market
            .metadata_json
            .get("market_date")?
            .as_str()?
            .to_string(),
        strike_type: market
            .metadata_json
            .get("strike_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("between")
            .to_string(),
        floor_strike: market
            .metadata_json
            .get("floor_strike")
            .and_then(serde_json::Value::as_f64),
        cap_strike: market
            .metadata_json
            .get("cap_strike")
            .and_then(serde_json::Value::as_f64),
    })
}

fn weather_reference_key(metadata: &WeatherMarketMetadata) -> String {
    format!(
        "{}:{}:{}",
        metadata.city.to_lowercase().replace(' ', "_"),
        metadata.market_date,
        metadata.contract_kind
    )
}

fn weather_probability_yes(
    metadata: &WeatherMarketMetadata,
    forecast_temperature_f: f64,
    confidence_score: f64,
) -> f64 {
    let spread = (8.0 - (confidence_score * 5.0)).clamp(1.5, 8.0);
    match metadata.strike_type.as_str() {
        "greater" => logistic_probability(
            forecast_temperature_f - metadata.floor_strike.unwrap_or(forecast_temperature_f),
            spread,
        ),
        "less" => logistic_probability(
            metadata.cap_strike.unwrap_or(forecast_temperature_f) - forecast_temperature_f,
            spread,
        ),
        _ => {
            let lower = logistic_probability(
                forecast_temperature_f - metadata.floor_strike.unwrap_or(forecast_temperature_f),
                spread,
            );
            let upper = logistic_probability(
                forecast_temperature_f - metadata.cap_strike.unwrap_or(forecast_temperature_f),
                spread,
            );
            (lower - upper).clamp(0.01, 0.99)
        }
    }
}

fn weather_threshold_distance(
    metadata: &WeatherMarketMetadata,
    forecast_temperature_f: f64,
) -> f64 {
    match metadata.strike_type.as_str() {
        "greater" => {
            forecast_temperature_f - metadata.floor_strike.unwrap_or(forecast_temperature_f)
        }
        "less" => metadata.cap_strike.unwrap_or(forecast_temperature_f) - forecast_temperature_f,
        _ => {
            let midpoint = match (metadata.floor_strike, metadata.cap_strike) {
                (Some(floor), Some(cap)) => (floor + cap) / 2.0,
                (Some(floor), None) => floor,
                (None, Some(cap)) => cap,
                _ => forecast_temperature_f,
            };
            forecast_temperature_f - midpoint
        }
    }
}

fn logistic_probability(delta: f64, scale: f64) -> f64 {
    (1.0 / (1.0 + (-(delta / scale.max(0.5))).exp())).clamp(0.01, 0.99)
}

fn weather_expiry_bucket(seconds_to_expiry: i32) -> &'static str {
    match seconds_to_expiry {
        0..=21_600 => "under_6h",
        21_601..=43_200 => "6h_to_12h",
        43_201..=86_400 => "12h_to_24h",
        _ => "over_24h",
    }
}

fn weather_settlement_regime(seconds_to_expiry: i32) -> &'static str {
    match seconds_to_expiry {
        0..=10_800 => "same_day_settlement",
        10_801..=43_200 => "overnight_settlement",
        _ => "forecast_window",
    }
}

fn crypto_strike_price(market: &KalshiMarketState) -> Option<f64> {
    market
        .metadata_json
        .get("target_price")
        .and_then(serde_json::Value::as_f64)
        .or_else(|| parse_title_target_price(&market.market_title))
        .or_else(|| parse_title_target_price(&market.market_ticker))
}

fn parse_title_target_price(raw: &str) -> Option<f64> {
    let marker = raw.find('$')?;
    let numeric = raw[marker + 1..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '.' || *ch == ',')
        .collect::<String>()
        .replace(',', "");
    numeric.parse::<f64>().ok()
}

fn stddev(values: &[f64]) -> f64 {
    let filtered = values
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
        .collect::<Vec<_>>();
    if filtered.len() < 2 {
        return 0.0;
    }
    let mean = filtered.iter().sum::<f64>() / filtered.len() as f64;
    let variance = filtered
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / filtered.len() as f64;
    variance.sqrt()
}

struct QueueFeatures {
    size_ahead_real: f64,
    trade_consumption_rate: f64,
    cancel_rate: f64,
    queue_decay_rate: f64,
    quote_churn: f64,
}

fn queue_features(
    queue_snapshot: Option<&QueueSnapshot>,
    depth_total: f64,
    spread: f64,
    market_data_age_seconds: i32,
) -> QueueFeatures {
    if let Some(queue) = queue_snapshot {
        let size_ahead_real =
            ((queue.yes.size_ahead_real + queue.no.size_ahead_real) / 2.0).max(1.0);
        let trade_consumption_rate =
            ((queue.yes.trade_consumption_rate + queue.no.trade_consumption_rate) / 2.0).max(0.0);
        let cancel_rate = ((queue.yes.cancel_rate + queue.no.cancel_rate) / 2.0).max(0.0);
        let queue_decay_rate =
            ((queue.yes.queue_decay_rate + queue.no.queue_decay_rate) / 2.0).max(0.0);
        let quote_churn = (cancel_rate + queue_decay_rate).clamp(0.0, 5.0);
        return QueueFeatures {
            size_ahead_real,
            trade_consumption_rate,
            cancel_rate,
            queue_decay_rate,
            quote_churn,
        };
    }

    let size_ahead_real = depth_total.max(1.0);
    let trade_consumption_rate =
        (depth_total / f64::from(market_data_age_seconds.max(1))).clamp(0.0, 500.0);
    let queue_decay_rate = (spread / 250.0).clamp(0.0, 5.0);
    let cancel_rate = (queue_decay_rate * 0.5).clamp(0.0, 5.0);
    let quote_churn = (cancel_rate + queue_decay_rate).clamp(0.0, 5.0);
    QueueFeatures {
        size_ahead_real,
        trade_consumption_rate,
        cancel_rate,
        queue_decay_rate,
        quote_churn,
    }
}

fn expiry_bucket(seconds_to_expiry: i32) -> &'static str {
    match seconds_to_expiry {
        0..=119 => "under_2m",
        120..=299 => "2_to_5m",
        300..=599 => "5_to_10m",
        _ => "10m_plus",
    }
}

fn settlement_regime(seconds_to_expiry: i32) -> &'static str {
    match seconds_to_expiry {
        0..=60 => "averaging_window",
        61..=180 => "pre_settlement",
        181..=600 => "late_window",
        _ => "mid_window",
    }
}
