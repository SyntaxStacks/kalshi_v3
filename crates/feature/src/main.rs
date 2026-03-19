use anyhow::Result;
use chrono::Utc;
use common::{AppConfig, KalshiMarketState, MarketFeatureSnapshotRecord, ReferencePriceState};
use redis::AsyncCommands;
use tracing::{info, warn};

const REDIS_MARKETS_KEY: &str = "v3:kalshi:markets";
const FEATURE_VERSION: &str = "v3_ws_feature_v2";

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
        let reference_key = format!("v3:reference:{}", market.symbol.to_uppercase());
        let raw_reference: Option<String> = redis.get(reference_key).await.ok();
        let reference = raw_reference
            .as_deref()
            .and_then(|raw| serde_json::from_str::<ReferencePriceState>(raw).ok());

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

        let snapshot = MarketFeatureSnapshotRecord {
            market_id: market.market_id,
            market_ticker: market.market_ticker,
            market_title: market.market_title,
            feature_version: FEATURE_VERSION.to_string(),
            exchange: "kalshi".to_string(),
            symbol: market.symbol,
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
            averaging_window_progress,
            settlement_regime,
            last_minute_avg_proxy,
            market_data_age_seconds,
            reference_age_seconds,
            created_at: now,
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
