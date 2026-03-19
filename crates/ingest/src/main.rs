use anyhow::Result;
use async_nats::Client as NatsClient;
use chrono::{DateTime, Utc};
use common::{
    AppConfig, KalshiMarketState, MarketFamily, ReferencePriceState, TradeMode,
    WeatherReferenceState, events::ReferencePriceTick, kalshi_api_key_id, kalshi_private_key,
    kalshi_sign_rest_request, kalshi_sign_ws_connect_request, kalshi_timestamp_ms, subjects,
};
use futures::{SinkExt, StreamExt};
use market_data::{
    ContractSide, OrderBookDelta, OrderBookSnapshot, OrderBookState, PriceLevel, QueueSnapshot,
    TradePrint,
};
use redis::AsyncCommands;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use storage::{LiveFillSyncRecord, LiveOrderSyncRecord, LivePositionSyncRecord};
use tokio::{
    sync::RwLock,
    time::{Duration, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};
use tracing::{info, warn};

const REDIS_MARKETS_KEY: &str = "v3:kalshi:markets";
const REDIS_QUEUE_PREFIX: &str = "v3:queue:";
const REDIS_WEATHER_REFERENCE_PREFIX: &str = "v3:weather:reference:";
const REDIS_TTL_SECONDS: u64 = 300;
const WS_RECONNECT_MAX_SECONDS: u64 = 30;

type MarketCache = Arc<RwLock<HashMap<String, KalshiMarketState>>>;
type OrderBookCache = Arc<RwLock<HashMap<String, OrderBookState>>>;
type ReferenceCache = Arc<RwLock<HashMap<String, ReferencePriceState>>>;
type WeatherReferenceCache = Arc<RwLock<HashMap<String, WeatherReferenceState>>>;

const WEATHER_HIGH_KIND: &str = "daily_high_temperature";
const WEATHER_LOW_KIND: &str = "daily_low_temperature";
const WEATHER_MODEL_NAME: &str = "weather_threshold_v1";

#[derive(Debug, Clone)]
struct SeriesDiscovery {
    market_family: MarketFamily,
    series_ticker: String,
    series_title: Option<String>,
}

#[derive(Debug, Clone)]
struct WeatherSeriesMetadata {
    city: String,
    contract_kind: String,
}

#[derive(Debug, Clone)]
struct WeatherMarketMetadata {
    city: String,
    contract_kind: String,
    market_date: String,
    strike_type: String,
    floor_strike: Option<f64>,
    cap_strike: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OpenMeteoGeocodeResponse {
    results: Option<Vec<OpenMeteoGeocodeResult>>,
}

#[derive(Debug, Deserialize)]
struct OpenMeteoGeocodeResult {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Deserialize)]
struct NwsPointsResponse {
    properties: NwsPointsProperties,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsPointsProperties {
    forecast_hourly: String,
    observation_stations: String,
}

#[derive(Debug, Deserialize)]
struct NwsHourlyForecastResponse {
    properties: NwsHourlyForecastProperties,
}

#[derive(Debug, Deserialize)]
struct NwsHourlyForecastProperties {
    periods: Vec<NwsHourlyForecastPeriod>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsHourlyForecastPeriod {
    start_time: String,
    temperature: Option<f64>,
    temperature_unit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NwsStationsResponse {
    features: Vec<NwsStationFeature>,
}

#[derive(Debug, Deserialize)]
struct NwsStationFeature {
    properties: NwsStationProperties,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsStationProperties {
    station_identifier: String,
}

#[derive(Debug, Deserialize)]
struct NwsObservationResponse {
    properties: NwsObservationProperties,
}

#[derive(Debug, Deserialize)]
struct NwsObservationProperties {
    temperature: NwsQuantitativeValue,
}

#[derive(Debug, Deserialize)]
struct NwsQuantitativeValue {
    value: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let nats = async_nats::connect(&config.nats_url).await.ok();
    let redis_client = redis::Client::open(config.redis_url.clone())?;
    let storage = storage::Storage::connect(&config.database_url).await?;
    storage.migrate().await?;
    let http = HttpClient::builder().user_agent("kalshi-v3/0.1").build()?;
    storage
        .upsert_worker_started("ingest", &serde_json::json!({"exchange": config.exchange}))
        .await?;

    let market_cache: MarketCache = Arc::new(RwLock::new(HashMap::new()));
    let orderbook_cache: OrderBookCache = Arc::new(RwLock::new(HashMap::new()));
    let reference_cache: ReferenceCache = Arc::new(RwLock::new(HashMap::new()));
    let weather_reference_cache: WeatherReferenceCache = Arc::new(RwLock::new(HashMap::new()));

    let bootstrap_markets = fetch_kalshi_markets(&config, &http).await?;
    refresh_market_cache(
        &redis_client,
        &storage,
        nats.as_ref(),
        &market_cache,
        bootstrap_markets,
        "bootstrap",
    )
    .await?;

    info!(
        exchange = %config.exchange,
        series = ?config.kalshi_series_tickers,
        "ingest_worker_started"
    );
    let run_once = std::env::var("INGEST_RUN_ONCE")
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "True"))
        .unwrap_or(false);

    if !run_once {
        tokio::spawn(run_kalshi_websocket(
            config.clone(),
            redis_client.clone(),
            storage.clone(),
            nats.clone(),
            market_cache.clone(),
            orderbook_cache.clone(),
        ));
        tokio::spawn(run_coinbase_websocket(
            config.clone(),
            redis_client.clone(),
            storage.clone(),
            nats.clone(),
            reference_cache.clone(),
        ));
    }

    let mut interval =
        tokio::time::interval(Duration::from_secs(config.market_poll_seconds.max(5)));
    loop {
        interval.tick().await;
        storage
            .upsert_worker_started(
                "ingest",
                &serde_json::json!({"exchange": config.exchange, "phase": "recovery_sync"}),
            )
            .await?;
        match recovery_sync_once(
            &config,
            &http,
            &redis_client,
            nats.as_ref(),
            &storage,
            &market_cache,
            &reference_cache,
            &weather_reference_cache,
        )
        .await
        {
            Ok((market_count, reference_count, live_balance_synced, live_exchange_synced)) => {
                storage
                    .upsert_worker_success(
                        "ingest",
                        &serde_json::json!({
                            "market_count": market_count,
                            "reference_count": reference_count,
                            "live_balance_synced": live_balance_synced,
                            "live_exchange_synced": live_exchange_synced,
                        }),
                    )
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("ingest", &error.to_string(), &serde_json::json!({}))
                    .await?;
                warn!(error = %error, "ingest_recovery_failed");
            }
        }
        if run_once {
            info!("ingest_worker_run_once_complete");
            break;
        }
    }
    Ok(())
}

async fn recovery_sync_once(
    config: &AppConfig,
    http: &HttpClient,
    redis_client: &redis::Client,
    nats: Option<&NatsClient>,
    storage: &storage::Storage,
    market_cache: &MarketCache,
    reference_cache: &ReferenceCache,
    weather_reference_cache: &WeatherReferenceCache,
) -> Result<(usize, usize, bool, bool)> {
    let markets = fetch_kalshi_markets(config, http).await?;
    refresh_market_cache(
        redis_client,
        storage,
        nats,
        market_cache,
        markets,
        "recovery",
    )
    .await?;

    let now = Utc::now();
    let fallback_after_seconds = (config.market_poll_seconds.max(15) * 2) as i64;
    let mut reference_count = 0usize;
    for symbol in &config.reference_symbols {
        let symbol_key = symbol.to_uppercase();
        let should_refresh = {
            let cache = reference_cache.read().await;
            cache
                .get(&symbol_key)
                .is_none_or(|state| (now - state.updated_at).num_seconds() > fallback_after_seconds)
        };
        if should_refresh {
            if let Some(state) = fetch_reference_state(http, redis_client, symbol).await? {
                upsert_reference_state(
                    redis_client,
                    storage,
                    nats,
                    reference_cache,
                    state,
                    "rest_recovery",
                )
                .await?;
            }
        }
        reference_count += reference_cache.read().await.contains_key(&symbol_key) as usize;
    }

    let weather_markets = market_cache
        .read()
        .await
        .values()
        .filter(|market| market.market_family == MarketFamily::Weather)
        .cloned()
        .collect::<Vec<_>>();
    let weather_reference_count = sync_weather_reference_states(
        config,
        http,
        redis_client,
        storage,
        nats,
        weather_reference_cache,
        &weather_markets,
    )
    .await?;
    reference_count += weather_reference_count;

    let live_balance_synced = match fetch_live_balance(config, http).await {
        Ok(Some(balance)) => {
            storage
                .record_external_bankroll_snapshot(
                    TradeMode::Live,
                    balance.bankroll,
                    balance.available_balance,
                    balance.open_exposure,
                )
                .await?;
            true
        }
        Ok(None) => false,
        Err(error) => {
            warn!(error = %error, "live_balance_sync_failed");
            false
        }
    };

    let live_exchange_synced = match fetch_live_exchange_state(config, http, storage).await {
        Ok(value) => value,
        Err(error) => {
            warn!(error = %error, "live_exchange_sync_failed");
            false
        }
    };

    let market_count = market_cache.read().await.len();
    info!(
        market_count,
        reference_count, live_balance_synced, live_exchange_synced, "ingest_recovery_succeeded"
    );
    Ok((
        market_count,
        reference_count,
        live_balance_synced,
        live_exchange_synced,
    ))
}

async fn run_kalshi_websocket(
    config: AppConfig,
    redis_client: redis::Client,
    storage: storage::Storage,
    nats: Option<NatsClient>,
    market_cache: MarketCache,
    orderbook_cache: OrderBookCache,
) {
    let mut backoff_seconds = 1u64;
    loop {
        match kalshi_websocket_session(
            &config,
            &redis_client,
            &storage,
            nats.as_ref(),
            &market_cache,
            &orderbook_cache,
        )
        .await
        {
            Ok(()) => backoff_seconds = 1,
            Err(error) => {
                warn!(error = %error, backoff_seconds, "kalshi_websocket_session_failed");
                sleep(Duration::from_secs(backoff_seconds)).await;
                backoff_seconds = (backoff_seconds * 2).min(WS_RECONNECT_MAX_SECONDS);
            }
        }
    }
}

async fn kalshi_websocket_session(
    config: &AppConfig,
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    market_cache: &MarketCache,
    orderbook_cache: &OrderBookCache,
) -> Result<()> {
    let mut request = config.kalshi_ws_url.clone().into_client_request()?;
    if let Some(api_key_id) = kalshi_api_key_id(config)? {
        if let Some(private_key) = kalshi_private_key(config)? {
            let timestamp = kalshi_timestamp_ms();
            let signature =
                kalshi_sign_ws_connect_request(&private_key, &timestamp, "/trade-api/ws/v2")?;
            let headers = request.headers_mut();
            headers.insert(
                HeaderName::from_static("kalshi-access-key"),
                HeaderValue::from_str(&api_key_id)?,
            );
            headers.insert(
                HeaderName::from_static("kalshi-access-timestamp"),
                HeaderValue::from_str(&timestamp)?,
            );
            headers.insert(
                HeaderName::from_static("kalshi-access-signature"),
                HeaderValue::from_str(&signature)?,
            );
        }
    }

    let (mut socket, _) = connect_async(request).await?;
    info!("kalshi_websocket_connected");

    socket
        .send(Message::Text(
            json!({"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"]}})
                .to_string()
                .into(),
        ))
        .await?;

    let trade_tickers = current_market_tickers(market_cache).await;
    if !trade_tickers.is_empty() {
        socket
            .send(Message::Text(
                json!({
                    "id": 2,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["trade"],
                        "market_tickers": trade_tickers
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
        socket
            .send(Message::Text(
                json!({
                    "id": 3,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": trade_tickers
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
    }

    while let Some(message) = socket.next().await {
        let message = message?;
        match message {
            Message::Text(text) => {
                if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    process_kalshi_ws_message(
                        redis_client,
                        storage,
                        nats,
                        market_cache,
                        orderbook_cache,
                        &payload,
                    )
                    .await?;
                }
            }
            Message::Binary(bytes) => {
                if let Ok(payload) = serde_json::from_slice::<Value>(&bytes) {
                    process_kalshi_ws_message(
                        redis_client,
                        storage,
                        nats,
                        market_cache,
                        orderbook_cache,
                        &payload,
                    )
                    .await?;
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(frame) => {
                warn!(?frame, "kalshi_websocket_closed");
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn process_kalshi_ws_message(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    market_cache: &MarketCache,
    orderbook_cache: &OrderBookCache,
    payload: &Value,
) -> Result<()> {
    let msg_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(msg) = payload.get("msg") else {
        return Ok(());
    };

    match msg_type {
        "ticker" => {
            let Some(market_ticker) = msg.get("market_ticker").and_then(Value::as_str) else {
                return Ok(());
            };
            let existing = market_cache.read().await.get(market_ticker).cloned();
            let Some(market) = normalize_market_from_ws_ticker(msg, existing.as_ref()) else {
                return Ok(());
            };
            upsert_market_state(
                redis_client,
                storage,
                nats,
                market_cache,
                market,
                "kalshi_ws_ticker",
            )
            .await?;
        }
        "trade" => {
            let Some(market_ticker) = msg.get("market_ticker").and_then(Value::as_str) else {
                return Ok(());
            };
            if let Some(trade) = normalize_trade_print(msg) {
                apply_trade_print(
                    redis_client,
                    storage,
                    nats,
                    orderbook_cache,
                    trade,
                    "kalshi_ws_trade",
                )
                .await?;
            }
            let Some(updated_market) =
                update_market_from_trade_msg(market_cache, market_ticker, msg).await
            else {
                return Ok(());
            };
            upsert_market_state(
                redis_client,
                storage,
                nats,
                market_cache,
                updated_market,
                "kalshi_ws_trade",
            )
            .await?;
        }
        "orderbook_snapshot" => {
            if let Some(snapshot) = normalize_orderbook_snapshot(msg) {
                apply_orderbook_snapshot(
                    redis_client,
                    storage,
                    nats,
                    orderbook_cache,
                    snapshot,
                    "kalshi_ws_orderbook_snapshot",
                )
                .await?;
            }
        }
        "orderbook_delta" => {
            if let Some(delta) = normalize_orderbook_delta(msg) {
                apply_orderbook_delta(
                    redis_client,
                    storage,
                    nats,
                    orderbook_cache,
                    delta,
                    "kalshi_ws_orderbook_delta",
                )
                .await?;
            }
        }
        "error" => {
            warn!(payload = %payload, "kalshi_websocket_error_message");
        }
        _ => {}
    }

    Ok(())
}

async fn run_coinbase_websocket(
    config: AppConfig,
    redis_client: redis::Client,
    storage: storage::Storage,
    nats: Option<NatsClient>,
    reference_cache: ReferenceCache,
) {
    let mut backoff_seconds = 1u64;
    loop {
        match coinbase_websocket_session(
            &config,
            &redis_client,
            &storage,
            nats.as_ref(),
            &reference_cache,
        )
        .await
        {
            Ok(()) => backoff_seconds = 1,
            Err(error) => {
                warn!(error = %error, backoff_seconds, "coinbase_websocket_session_failed");
                sleep(Duration::from_secs(backoff_seconds)).await;
                backoff_seconds = (backoff_seconds * 2).min(WS_RECONNECT_MAX_SECONDS);
            }
        }
    }
}

async fn coinbase_websocket_session(
    config: &AppConfig,
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    reference_cache: &ReferenceCache,
) -> Result<()> {
    let (mut socket, _) = connect_async(config.coinbase_ws_url.as_str()).await?;
    info!("coinbase_websocket_connected");

    let products: Vec<String> = config
        .reference_symbols
        .iter()
        .map(|symbol| format!("{}-USD", symbol.to_uppercase()))
        .collect();
    socket
        .send(Message::Text(
            json!({
                "type": "subscribe",
                "product_ids": products,
                "channels": ["ticker"]
            })
            .to_string()
            .into(),
        ))
        .await?;

    while let Some(message) = socket.next().await {
        let message = message?;
        match message {
            Message::Text(text) => {
                if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    process_coinbase_ws_message(
                        redis_client,
                        storage,
                        nats,
                        reference_cache,
                        &payload,
                    )
                    .await?;
                }
            }
            Message::Binary(bytes) => {
                if let Ok(payload) = serde_json::from_slice::<Value>(&bytes) {
                    process_coinbase_ws_message(
                        redis_client,
                        storage,
                        nats,
                        reference_cache,
                        &payload,
                    )
                    .await?;
                }
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(frame) => {
                warn!(?frame, "coinbase_websocket_closed");
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn process_coinbase_ws_message(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    reference_cache: &ReferenceCache,
    payload: &Value,
) -> Result<()> {
    let msg_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if msg_type != "ticker" {
        return Ok(());
    }
    let Some(product_id) = payload.get("product_id").and_then(Value::as_str) else {
        return Ok(());
    };
    let Some(symbol) = product_id.split('-').next().map(str::to_uppercase) else {
        return Ok(());
    };
    let Some(price) = parse_f64(payload.get("price")) else {
        return Ok(());
    };
    let updated_at = payload
        .get("time")
        .and_then(parse_time)
        .unwrap_or_else(Utc::now);
    let previous_price = reference_cache
        .read()
        .await
        .get(&symbol)
        .map(|state| state.price)
        .unwrap_or(price);

    let state = ReferencePriceState {
        market_family: MarketFamily::Crypto,
        source: "coinbase_ws".to_string(),
        symbol,
        price,
        previous_price,
        updated_at,
    };
    upsert_reference_state(
        redis_client,
        storage,
        nats,
        reference_cache,
        state,
        "coinbase_ws_ticker",
    )
    .await
}

async fn refresh_market_cache(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    market_cache: &MarketCache,
    markets: Vec<KalshiMarketState>,
    source: &str,
) -> Result<()> {
    for market in markets {
        upsert_market_state(redis_client, storage, nats, market_cache, market, source).await?;
    }
    Ok(())
}

async fn upsert_market_state(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    market_cache: &MarketCache,
    market: KalshiMarketState,
    source: &str,
) -> Result<()> {
    let snapshot = {
        let mut cache = market_cache.write().await;
        cache.insert(market.market_ticker.clone(), market.clone());
        cache.values().cloned().collect::<Vec<_>>()
    };

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis
        .set_ex(
            REDIS_MARKETS_KEY,
            serde_json::to_string(&snapshot)?,
            REDIS_TTL_SECONDS,
        )
        .await?;

    if let Some(client) = nats {
        let payload = serde_json::to_vec(&market)?;
        if let Err(error) = client
            .publish(subjects::KALSHI_MARKET_TICK, payload.into())
            .await
        {
            warn!(error = %error, "kalshi_market_tick_publish_failed");
        }
    }

    storage
        .insert_ingest_event(
            "kalshi",
            source,
            &market.market_ticker,
            &serde_json::to_value(&market)?,
            market.created_at,
        )
        .await?;
    Ok(())
}

async fn apply_orderbook_snapshot(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    orderbook_cache: &OrderBookCache,
    snapshot: OrderBookSnapshot,
    source: &str,
) -> Result<()> {
    let queue_snapshot = {
        let mut cache = orderbook_cache.write().await;
        let state = cache
            .entry(snapshot.market_ticker.clone())
            .or_insert_with(|| OrderBookState::new(snapshot.market_ticker.clone()));
        state.apply_snapshot(snapshot.clone());
        state.queue_snapshot()
    };
    upsert_queue_snapshot(redis_client, storage, nats, queue_snapshot, source).await
}

async fn apply_orderbook_delta(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    orderbook_cache: &OrderBookCache,
    delta: OrderBookDelta,
    source: &str,
) -> Result<()> {
    let queue_snapshot = {
        let mut cache = orderbook_cache.write().await;
        let state = cache
            .entry(delta.market_ticker.clone())
            .or_insert_with(|| OrderBookState::new(delta.market_ticker.clone()));
        state.apply_delta(delta.clone());
        state.queue_snapshot()
    };
    upsert_queue_snapshot(redis_client, storage, nats, queue_snapshot, source).await
}

async fn apply_trade_print(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    orderbook_cache: &OrderBookCache,
    trade: TradePrint,
    source: &str,
) -> Result<()> {
    let queue_snapshot = {
        let mut cache = orderbook_cache.write().await;
        let state = cache
            .entry(trade.market_ticker.clone())
            .or_insert_with(|| OrderBookState::new(trade.market_ticker.clone()));
        state.apply_trade(trade.clone());
        state.queue_snapshot()
    };
    upsert_queue_snapshot(redis_client, storage, nats, queue_snapshot, source).await
}

async fn upsert_queue_snapshot(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    snapshot: QueueSnapshot,
    source: &str,
) -> Result<()> {
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis
        .set_ex(
            format!("{REDIS_QUEUE_PREFIX}{}", snapshot.market_ticker),
            serde_json::to_string(&snapshot)?,
            REDIS_TTL_SECONDS,
        )
        .await?;

    if let Some(client) = nats {
        let subject = if source.contains("trade") {
            subjects::KALSHI_TRADE_PRINT
        } else {
            subjects::KALSHI_ORDERBOOK_DELTA
        };
        if let Err(error) = client
            .publish(subject, serde_json::to_vec(&snapshot)?.into())
            .await
        {
            warn!(error = %error, "kalshi_queue_snapshot_publish_failed");
        }
    }

    storage
        .insert_ingest_event(
            "kalshi",
            source,
            &snapshot.market_ticker,
            &serde_json::to_value(&snapshot)?,
            snapshot.updated_at,
        )
        .await?;
    Ok(())
}

async fn upsert_reference_state(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    reference_cache: &ReferenceCache,
    state: ReferencePriceState,
    source: &str,
) -> Result<()> {
    {
        let mut cache = reference_cache.write().await;
        cache.insert(state.symbol.clone(), state.clone());
    }

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let key = reference_key(&state.symbol);
    let _: () = redis
        .set_ex(key, serde_json::to_string(&state)?, REDIS_TTL_SECONDS)
        .await?;

    if let Some(client) = nats {
        let tick = ReferencePriceTick {
            source: state.source.clone(),
            symbol: state.symbol.clone(),
            price: state.price,
            averaging_window_seconds: 60,
            quality_score: 1.0,
        };
        if let Err(error) = client
            .publish(
                subjects::REFERENCE_PRICE_TICK,
                serde_json::to_vec(&tick)?.into(),
            )
            .await
        {
            warn!(error = %error, "reference_price_tick_publish_failed");
        }
    }

    storage
        .insert_ingest_event(
            "coinbase",
            source,
            &state.symbol,
            &serde_json::to_value(&state)?,
            state.updated_at,
        )
        .await?;
    Ok(())
}

async fn current_market_tickers(market_cache: &MarketCache) -> Vec<String> {
    market_cache
        .read()
        .await
        .keys()
        .cloned()
        .collect::<Vec<_>>()
}

async fn update_market_from_trade_msg(
    market_cache: &MarketCache,
    market_ticker: &str,
    msg: &Value,
) -> Option<KalshiMarketState> {
    let existing = market_cache.read().await.get(market_ticker).cloned()?;
    let trade_price = parse_f64_from_candidates(
        msg,
        &[
            "yes_price_dollars",
            "yes_price",
            "price_dollars",
            "price",
            "last_price_dollars",
        ],
    )?;
    let updated_at = msg
        .get("created_time")
        .and_then(parse_time)
        .or_else(|| msg.get("time").and_then(parse_time))
        .unwrap_or_else(Utc::now);
    Some(KalshiMarketState {
        last_price: trade_price.clamp(0.01, 0.99),
        created_at: updated_at,
        ..existing
    })
}

fn normalize_trade_print(msg: &Value) -> Option<TradePrint> {
    let market_ticker = msg.get("market_ticker")?.as_str()?.to_string();
    let side = parse_contract_side(
        msg.get("side")
            .or_else(|| msg.get("taker_side"))
            .or_else(|| msg.get("maker_side")),
    )?;
    let price = parse_f64_from_candidates(
        msg,
        &[
            "yes_price_dollars",
            "yes_price",
            "price_dollars",
            "price",
            "last_price_dollars",
        ],
    )?;
    let quantity = parse_f64_from_candidates(msg, &["count", "quantity", "size", "fill_count"])?;
    let ts = msg
        .get("created_time")
        .and_then(parse_time)
        .or_else(|| msg.get("time").and_then(parse_time))
        .or_else(|| msg.get("ts").and_then(parse_time))
        .unwrap_or_else(Utc::now);
    Some(TradePrint {
        market_ticker,
        side,
        price,
        quantity,
        ts,
    })
}

fn normalize_orderbook_snapshot(msg: &Value) -> Option<OrderBookSnapshot> {
    let market_ticker = msg.get("market_ticker")?.as_str()?.to_string();
    let yes = parse_price_levels(
        msg.get("yes")
            .or_else(|| msg.get("yes_levels"))
            .or_else(|| msg.get("yes_book")),
    );
    let no = parse_price_levels(
        msg.get("no")
            .or_else(|| msg.get("no_levels"))
            .or_else(|| msg.get("no_book")),
    );
    let ts = msg
        .get("ts")
        .and_then(parse_time)
        .or_else(|| msg.get("time").and_then(parse_time))
        .or_else(|| msg.get("created_time").and_then(parse_time))
        .unwrap_or_else(Utc::now);
    Some(OrderBookSnapshot {
        market_ticker,
        yes,
        no,
        ts,
    })
}

fn normalize_orderbook_delta(msg: &Value) -> Option<OrderBookDelta> {
    let market_ticker = msg.get("market_ticker")?.as_str()?.to_string();
    let side = parse_contract_side(msg.get("side"))?;
    let price =
        parse_f64_from_candidates(msg, &["price_dollars", "price", "yes_price", "no_price"])?;
    let delta_size = parse_f64_from_candidates(
        msg,
        &["delta", "delta_size", "quantity_delta", "count_delta"],
    )?;
    let ts = msg
        .get("ts")
        .and_then(parse_time)
        .or_else(|| msg.get("time").and_then(parse_time))
        .or_else(|| msg.get("created_time").and_then(parse_time))
        .unwrap_or_else(Utc::now);
    Some(OrderBookDelta {
        market_ticker,
        side,
        price,
        delta_size,
        ts,
    })
}

fn parse_contract_side(value: Option<&Value>) -> Option<ContractSide> {
    match value.and_then(Value::as_str)?.to_ascii_lowercase().as_str() {
        "yes" | "buy_yes" => Some(ContractSide::Yes),
        "no" | "buy_no" => Some(ContractSide::No),
        _ => None,
    }
}

fn parse_price_levels(value: Option<&Value>) -> Vec<PriceLevel> {
    value
        .and_then(Value::as_array)
        .map(|levels| {
            levels
                .iter()
                .filter_map(|level| {
                    if let Some(items) = level.as_array() {
                        let price = parse_f64(items.first())?;
                        let total_size = parse_f64(items.get(1))?;
                        return Some(PriceLevel { price, total_size });
                    }
                    Some(PriceLevel {
                        price: parse_f64_from_candidates(level, &["price_dollars", "price"])?,
                        total_size: parse_f64_from_candidates(
                            level,
                            &["count", "quantity", "size", "total_size"],
                        )?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn fetch_kalshi_markets(
    config: &AppConfig,
    http: &HttpClient,
) -> Result<Vec<KalshiMarketState>> {
    let mut out = Vec::new();
    let discovered_series = discover_series(config, http).await?;
    for series in discovered_series {
        let url = format!("{}/markets", config.kalshi_api_base.trim_end_matches('/'));
        let mut cursor: Option<String> = None;
        let mut pages_fetched = 0usize;
        let mut found_near_term_crypto = false;

        loop {
            let mut request = http.get(&url).query(&[
                ("series_ticker", series.series_ticker.as_str()),
                ("limit", "50"),
            ]);
            if let Some(cursor_value) = cursor.as_deref() {
                request = request.query(&[("cursor", cursor_value)]);
            }

            let response = match request.send().await?.error_for_status() {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        error = %error,
                        series_ticker = %series.series_ticker,
                        market_family = ?series.market_family,
                        cursor = cursor,
                        "series_market_fetch_failed"
                    );
                    break;
                }
            };
            let payload: Value = response.json().await?;
            let Some(markets) = payload.get("markets").and_then(Value::as_array) else {
                break;
            };

            for item in markets {
                if let Some(market) =
                    normalize_market(item, series.market_family, series.series_title.as_deref())
                {
                    if series.market_family == MarketFamily::Crypto
                        && market.close_at <= Utc::now() + chrono::Duration::hours(1)
                    {
                        found_near_term_crypto = true;
                    }
                    out.push(market);
                }
            }

            pages_fetched += 1;
            let next_cursor = payload
                .get("cursor")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if !should_fetch_next_market_page(
                series.market_family,
                found_near_term_crypto,
                pages_fetched,
                next_cursor.is_some(),
            ) {
                break;
            }
            cursor = next_cursor;
        }
    }
    out.sort_by(|left, right| left.close_at.cmp(&right.close_at));
    Ok(out)
}

fn should_fetch_next_market_page(
    market_family: MarketFamily,
    found_near_term_crypto: bool,
    pages_fetched: usize,
    has_next_cursor: bool,
) -> bool {
    if !has_next_cursor {
        return false;
    }
    match market_family {
        MarketFamily::Crypto => !found_near_term_crypto && pages_fetched < 4,
        _ => false,
    }
}

async fn discover_series(config: &AppConfig, http: &HttpClient) -> Result<Vec<SeriesDiscovery>> {
    let mut out = config
        .kalshi_series_tickers
        .iter()
        .map(|series_ticker| SeriesDiscovery {
            market_family: MarketFamily::Crypto,
            series_ticker: series_ticker.clone(),
            series_title: None,
        })
        .collect::<Vec<_>>();

    match fetch_weather_series(config, http).await {
        Ok(weather_series) => out.extend(weather_series),
        Err(error) => {
            warn!(error = %error, "weather_series_discovery_failed");
        }
    }
    Ok(out)
}

async fn fetch_weather_series(
    config: &AppConfig,
    http: &HttpClient,
) -> Result<Vec<SeriesDiscovery>> {
    let url = format!("{}/series", config.kalshi_api_base.trim_end_matches('/'));
    let response = http
        .get(url)
        .query(&[("limit", "1000")])
        .send()
        .await?
        .error_for_status()?;
    let payload: Value = response.json().await?;
    let Some(series) = payload.get("series").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for item in series {
        let Some(category) = item.get("category").and_then(Value::as_str) else {
            continue;
        };
        if !category.eq_ignore_ascii_case(&config.weather_series_category) {
            continue;
        }
        let Some(title) = item.get("title").and_then(Value::as_str) else {
            continue;
        };
        let title_matches = config
            .weather_series_title_patterns
            .iter()
            .any(|pattern| title.to_lowercase().contains(&pattern.to_lowercase()));
        if !title_matches {
            continue;
        }
        let Some(ticker) = item.get("ticker").and_then(Value::as_str) else {
            continue;
        };
        if !seen.insert(ticker.to_string()) {
            continue;
        }
        out.push(SeriesDiscovery {
            market_family: MarketFamily::Weather,
            series_ticker: ticker.to_string(),
            series_title: Some(title.to_string()),
        });
        if out.len() >= config.weather_series_poll_limit {
            break;
        }
    }
    Ok(out)
}

async fn sync_weather_reference_states(
    config: &AppConfig,
    http: &HttpClient,
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    weather_reference_cache: &WeatherReferenceCache,
    markets: &[KalshiMarketState],
) -> Result<usize> {
    let mut seen = HashSet::new();
    let now = Utc::now();
    for market in markets {
        let Some(metadata) = weather_metadata_from_market(market) else {
            continue;
        };
        let reference_key = weather_reference_cache_key(
            &metadata.city,
            &metadata.market_date,
            &metadata.contract_kind,
        );
        if !seen.insert(reference_key.clone()) {
            continue;
        }
        let should_refresh = {
            let cache = weather_reference_cache.read().await;
            cache.get(&reference_key).is_none_or(|state| {
                state.source != "nws_hourly_forecast"
                    || (now - state.updated_at).num_seconds()
                        > config.weather_reference_refresh_seconds as i64
            })
        };
        if !should_refresh {
            continue;
        }
        let weather_state = match fetch_weather_reference_state(config, http, &metadata).await {
            Ok(state) => state,
            Err(error) => {
                warn!(
                    error = %error,
                    city = %metadata.city,
                    contract_kind = %metadata.contract_kind,
                    market_date = %metadata.market_date,
                    "weather_reference_refresh_failed"
                );
                continue;
            }
        };
        if let Some(state) = weather_state {
            upsert_weather_reference_state(
                redis_client,
                storage,
                nats,
                weather_reference_cache,
                state,
                "nws_hourly_forecast",
            )
            .await?;
        }
    }

    Ok(weather_reference_cache.read().await.len())
}

async fn upsert_weather_reference_state(
    redis_client: &redis::Client,
    storage: &storage::Storage,
    nats: Option<&NatsClient>,
    weather_reference_cache: &WeatherReferenceCache,
    state: WeatherReferenceState,
    source: &str,
) -> Result<()> {
    {
        let mut cache = weather_reference_cache.write().await;
        cache.insert(state.reference_key.clone(), state.clone());
    }

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let key = format!("{}{}", REDIS_WEATHER_REFERENCE_PREFIX, state.reference_key);
    let _: () = redis
        .set_ex(key, serde_json::to_string(&state)?, REDIS_TTL_SECONDS)
        .await?;

    if let Some(client) = nats {
        let tick = ReferencePriceTick {
            source: state.source.clone(),
            symbol: state.reference_key.clone(),
            price: state.forecast_temperature_f,
            averaging_window_seconds: 86_400,
            quality_score: state.confidence_score,
        };
        if let Err(error) = client
            .publish(
                subjects::REFERENCE_PRICE_TICK,
                serde_json::to_vec(&tick)?.into(),
            )
            .await
        {
            warn!(error = %error, "weather_reference_tick_publish_failed");
        }
    }

    storage
        .insert_ingest_event(
            "weather_public",
            source,
            &state.reference_key,
            &serde_json::to_value(&state)?,
            state.updated_at,
        )
        .await?;
    Ok(())
}

async fn fetch_weather_reference_state(
    config: &AppConfig,
    http: &HttpClient,
    metadata: &WeatherMarketMetadata,
) -> Result<Option<WeatherReferenceState>> {
    let Some((latitude, longitude)) =
        resolve_weather_city_coordinates(config, http, &metadata.city).await?
    else {
        return Ok(None);
    };
    let points_url = format!(
        "{}/points/{latitude},{longitude}",
        config.nws_api_base.trim_end_matches('/')
    );
    let points: NwsPointsResponse = http
        .get(points_url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let hourly: NwsHourlyForecastResponse = http
        .get(points.properties.forecast_hourly.as_str())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let forecast_temperatures = hourly
        .properties
        .periods
        .iter()
        .filter(|period| period.start_time.starts_with(&metadata.market_date))
        .filter_map(|period| {
            period.temperature.map(|value| {
                nws_temperature_to_fahrenheit(value, period.temperature_unit.as_deref())
            })
        })
        .collect::<Vec<_>>();
    let forecast_temperature_f = match metadata.contract_kind.as_str() {
        WEATHER_LOW_KIND => forecast_temperatures.iter().copied().reduce(f64::min),
        _ => forecast_temperatures.iter().copied().reduce(f64::max),
    };
    let Some(forecast_temperature_f) = forecast_temperature_f else {
        return Ok(None);
    };

    let station_response = http
        .get(points.properties.observation_stations.as_str())
        .send()
        .await?
        .error_for_status()?;
    let stations: NwsStationsResponse = station_response.json().await?;
    let observation_temperature_f = if let Some(station_id) = stations
        .features
        .first()
        .map(|feature| feature.properties.station_identifier.clone())
    {
        let observation_url = format!(
            "{}/stations/{station_id}/observations/latest",
            config.nws_api_base.trim_end_matches('/')
        );
        let observation: NwsObservationResponse = http
            .get(observation_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        observation
            .properties
            .temperature
            .value
            .map(celsius_to_fahrenheit)
    } else {
        None
    };

    let days_out = date_days_from_now(&metadata.market_date);
    let hourly_coverage = (forecast_temperatures.len() as f64 / 24.0).clamp(0.0, 1.0);
    let observation_bonus = if observation_temperature_f.is_some() {
        0.06
    } else {
        0.0
    };
    let confidence_score = (0.72 + (hourly_coverage * 0.16) + observation_bonus
        - (days_out as f64 * 0.04))
        .clamp(0.35, 0.96);
    Ok(Some(WeatherReferenceState {
        market_family: MarketFamily::Weather,
        reference_key: weather_reference_cache_key(
            &metadata.city,
            &metadata.market_date,
            &metadata.contract_kind,
        ),
        city: metadata.city.clone(),
        contract_kind: metadata.contract_kind.clone(),
        market_date: metadata.market_date.clone(),
        forecast_temperature_f,
        observation_temperature_f,
        confidence_score,
        source: "nws_hourly_forecast".to_string(),
        updated_at: Utc::now(),
    }))
}

async fn resolve_weather_city_coordinates(
    config: &AppConfig,
    http: &HttpClient,
    city: &str,
) -> Result<Option<(f64, f64)>> {
    if let Some(point) = canonical_weather_city(city)
        .as_deref()
        .and_then(weather_city_coordinates)
    {
        return Ok(Some(point));
    }
    geocode_city(config, http, city).await
}

async fn geocode_city(
    config: &AppConfig,
    http: &HttpClient,
    city: &str,
) -> Result<Option<(f64, f64)>> {
    let response = http
        .get(config.open_meteo_geocode_api_base.as_str())
        .query(&[
            ("name", city),
            ("count", "1"),
            ("language", "en"),
            ("format", "json"),
        ])
        .send()
        .await?
        .error_for_status()?;
    let payload: OpenMeteoGeocodeResponse = response.json().await?;
    Ok(payload
        .results
        .and_then(|mut results| results.drain(..).next())
        .map(|result| (result.latitude, result.longitude)))
}

async fn fetch_live_balance(config: &AppConfig, http: &HttpClient) -> Result<Option<LiveBalance>> {
    let Some(payload) = signed_get_json(config, http, "/portfolio/balance").await? else {
        warn!("live_balance_sync_skipped_missing_credentials");
        return Ok(None);
    };

    let available_balance = cents_to_dollars(payload.get("balance"));
    let portfolio_value = cents_to_dollars(payload.get("portfolio_value"));
    let bankroll = (available_balance + portfolio_value).max(0.0);
    Ok(Some(LiveBalance {
        bankroll,
        available_balance: available_balance.max(0.0),
        open_exposure: portfolio_value.max(0.0),
    }))
}

async fn fetch_live_exchange_state(
    config: &AppConfig,
    http: &HttpClient,
    storage: &storage::Storage,
) -> Result<bool> {
    let synced_at = Utc::now();
    let Some(positions_payload) =
        signed_get_json(config, http, "/portfolio/positions?limit=200").await?
    else {
        return Ok(false);
    };
    let Some(orders_payload) =
        signed_get_json(config, http, "/portfolio/orders?status=resting&limit=200").await?
    else {
        return Ok(false);
    };
    let Some(fills_payload) = signed_get_json(config, http, "/portfolio/fills?limit=100").await?
    else {
        return Ok(false);
    };

    let positions = normalize_live_positions(&positions_payload);
    let orders = normalize_live_orders(&orders_payload);
    let fills = normalize_live_fills(&fills_payload);

    storage
        .replace_live_positions(synced_at, &positions)
        .await?;
    storage.replace_live_orders(synced_at, &orders).await?;
    storage.replace_live_fills(synced_at, &fills).await?;

    let local_open_live_trades_count = storage.count_open_live_trades().await?;
    let resting_orders_count = orders
        .iter()
        .filter(|order| {
            order
                .status
                .as_deref()
                .map(|status| status.eq_ignore_ascii_case("resting"))
                .unwrap_or(true)
        })
        .count() as i64;
    let mut issues = Vec::new();
    if local_open_live_trades_count > 0 && positions.is_empty() && resting_orders_count == 0 {
        issues.push("live_trades_without_exchange_positions_or_orders".to_string());
    }
    let status = if issues.is_empty() {
        "ok".to_string()
    } else {
        "attention_needed".to_string()
    };
    storage
        .record_live_exchange_sync_status(
            &common::LiveExchangeSyncSummary {
                synced_at,
                positions_count: positions.len() as i64,
                resting_orders_count,
                recent_fills_count: fills.len() as i64,
                local_open_live_trades_count,
                status: status.clone(),
                issues: issues.clone(),
            },
            &serde_json::json!({
                "positions_count": positions.len(),
                "resting_orders_count": resting_orders_count,
                "recent_fills_count": fills.len(),
                "status": status,
                "issues": issues,
            }),
        )
        .await?;
    Ok(true)
}

async fn fetch_reference_state(
    http: &HttpClient,
    redis_client: &redis::Client,
    symbol: &str,
) -> Result<Option<ReferencePriceState>> {
    let url = format!(
        "https://api.exchange.coinbase.com/products/{}-USD/ticker",
        symbol.to_uppercase()
    );
    let response = http.get(url).send().await?.error_for_status()?;
    let payload: Value = response.json().await?;
    let Some(price) = payload
        .get("price")
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<f64>().ok())
    else {
        return Ok(None);
    };

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let previous: Option<String> = redis.get(reference_key(symbol)).await.ok();
    let previous_price = previous
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ReferencePriceState>(raw).ok())
        .map(|state| state.price)
        .unwrap_or(price);

    Ok(Some(ReferencePriceState {
        market_family: MarketFamily::Crypto,
        source: "coinbase_proxy".to_string(),
        symbol: symbol.to_uppercase(),
        price,
        previous_price,
        updated_at: Utc::now(),
    }))
}

fn normalize_market(
    value: &Value,
    market_family: MarketFamily,
    series_title: Option<&str>,
) -> Option<KalshiMarketState> {
    let ticker = value.get("ticker")?.as_str()?.to_string();
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_lowercase();
    if !matches!(status.as_str(), "active" | "initialized") {
        return None;
    }
    let close_at = parse_time(value.get("close_time")?)?;
    if close_at <= Utc::now() {
        return None;
    }

    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or(&ticker)
        .to_string();
    let weather_metadata = if market_family == MarketFamily::Weather {
        normalize_weather_metadata(value, &title, series_title)?
    } else {
        WeatherMarketMetadata {
            city: String::new(),
            contract_kind: String::new(),
            market_date: String::new(),
            strike_type: String::new(),
            floor_strike: None,
            cap_strike: None,
        }
    };
    let symbol = if market_family == MarketFamily::Weather {
        weather_symbol(&weather_metadata.city, &weather_metadata.contract_kind)
    } else {
        infer_crypto_symbol(&ticker, &title)?
    };
    let best_bid = to_f64(value.get("yes_bid_dollars"));
    let best_ask = to_f64(value.get("yes_ask_dollars"));
    let last_price = to_f64(value.get("last_price_dollars"));
    let market_prob = midpoint(best_bid, best_ask, last_price);
    let bid_size = to_f64(value.get("yes_bid_size_fp"));
    let ask_size = to_f64(value.get("yes_ask_size_fp"));
    let liquidity = bid_size + ask_size + to_f64(value.get("open_interest_fp"));

    Some(KalshiMarketState {
        market_id: stable_market_id(&ticker),
        market_family,
        market_ticker: ticker,
        market_title: title,
        series_ticker: value
            .get("event_ticker")
            .and_then(Value::as_str)
            .and_then(|event| event.split('-').next())
            .map(ToOwned::to_owned),
        symbol,
        window_minutes: 15,
        market_prob,
        best_bid,
        best_ask,
        last_price,
        bid_size,
        ask_size,
        liquidity,
        metadata_json: if market_family == MarketFamily::Weather {
            json!({
                "city": weather_metadata.city,
                "contract_kind": weather_metadata.contract_kind,
                "market_date": weather_metadata.market_date,
                "strike_type": weather_metadata.strike_type,
                "floor_strike": weather_metadata.floor_strike,
                "cap_strike": weather_metadata.cap_strike,
                "series_title": series_title,
                "family_model_name": WEATHER_MODEL_NAME,
            })
        } else {
            json!({})
        },
        close_at,
        created_at: Utc::now(),
    })
}

fn normalize_market_from_ws_ticker(
    msg: &Value,
    existing: Option<&KalshiMarketState>,
) -> Option<KalshiMarketState> {
    let ticker = msg.get("market_ticker")?.as_str()?.to_string();
    let existing = existing?;
    let best_bid = parse_f64_from_candidates(msg, &["yes_bid_dollars", "yes_bid"])
        .unwrap_or(existing.best_bid);
    let best_ask = parse_f64_from_candidates(msg, &["yes_ask_dollars", "yes_ask"])
        .unwrap_or(existing.best_ask);
    let last_price = parse_f64_from_candidates(
        msg,
        &[
            "last_price_dollars",
            "price_dollars",
            "price",
            "yes_price_dollars",
        ],
    )
    .unwrap_or(existing.last_price);
    let bid_size = parse_f64_from_candidates(
        msg,
        &["yes_bid_size_fp", "yes_bid_size", "yes_bid_quantity"],
    )
    .unwrap_or(existing.bid_size);
    let ask_size = parse_f64_from_candidates(
        msg,
        &["yes_ask_size_fp", "yes_ask_size", "yes_ask_quantity"],
    )
    .unwrap_or(existing.ask_size);
    let open_interest = parse_f64_from_candidates(msg, &["open_interest_fp", "open_interest"])
        .unwrap_or(existing.liquidity - existing.bid_size - existing.ask_size);
    let created_at = msg
        .get("ts")
        .and_then(parse_time)
        .or_else(|| msg.get("time").and_then(parse_time))
        .unwrap_or_else(Utc::now);

    Some(KalshiMarketState {
        market_id: existing.market_id,
        market_family: existing.market_family,
        market_ticker: ticker,
        market_title: existing.market_title.clone(),
        series_ticker: existing.series_ticker.clone(),
        symbol: existing.symbol.clone(),
        window_minutes: existing.window_minutes,
        market_prob: midpoint(best_bid, best_ask, last_price),
        best_bid,
        best_ask,
        last_price,
        bid_size,
        ask_size,
        liquidity: bid_size + ask_size + open_interest.max(0.0),
        metadata_json: existing.metadata_json.clone(),
        close_at: existing.close_at,
        created_at,
    })
}

fn reference_key(symbol: &str) -> String {
    format!("v3:reference:{}", symbol.to_uppercase())
}

fn weather_reference_cache_key(city: &str, market_date: &str, contract_kind: &str) -> String {
    format!(
        "{}:{}:{}",
        normalize_weather_city(city),
        market_date,
        contract_kind
    )
}

fn normalize_weather_city(city: &str) -> String {
    city.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .replace("__", "_")
}

fn weather_symbol(city: &str, contract_kind: &str) -> String {
    let suffix = if contract_kind == WEATHER_LOW_KIND {
        "low_temp"
    } else {
        "high_temp"
    };
    format!("{}_{}", normalize_weather_city(city), suffix)
}

fn infer_crypto_symbol(ticker: &str, title: &str) -> Option<String> {
    for candidate in ["BTC", "ETH", "SOL", "XRP"] {
        if ticker.contains(candidate) || title.to_uppercase().contains(candidate) {
            return Some(candidate.to_string());
        }
    }
    None
}

fn normalize_weather_metadata(
    value: &Value,
    market_title: &str,
    series_title: Option<&str>,
) -> Option<WeatherMarketMetadata> {
    let metadata = series_title
        .and_then(parse_weather_series_metadata)
        .or_else(|| {
            parse_weather_series_metadata(market_title)
                .or_else(|| parse_weather_market_city(market_title))
        })?;
    let event_ticker = value
        .get("event_ticker")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let market_date = parse_market_date_from_event_ticker(event_ticker)?;
    Some(WeatherMarketMetadata {
        city: metadata.city,
        contract_kind: metadata.contract_kind,
        market_date,
        strike_type: value
            .get("strike_type")
            .and_then(Value::as_str)
            .unwrap_or("between")
            .to_string(),
        floor_strike: parse_f64(value.get("floor_strike")),
        cap_strike: parse_f64(value.get("cap_strike")),
    })
}

fn parse_weather_series_metadata(title: &str) -> Option<WeatherSeriesMetadata> {
    let normalized = title.to_lowercase();
    let contract_kind = if normalized.contains("highest temperature")
        || normalized.contains("high temp")
        || normalized.contains("max temperature")
        || normalized.contains("maximum temperature")
        || normalized.contains("daily high temperature")
    {
        WEATHER_HIGH_KIND.to_string()
    } else if normalized.contains("lowest temperature")
        || normalized.contains("low temp")
        || normalized.contains("minimum temperature")
        || normalized.contains("min temperature")
        || normalized.contains("daily low temperature")
    {
        WEATHER_LOW_KIND.to_string()
    } else {
        return None;
    };

    let city = title
        .split_once(" in ")
        .map(|(_, tail)| tail.trim().to_string())
        .or_else(|| canonical_weather_city(title))
        .filter(|value| !value.is_empty())?;
    Some(WeatherSeriesMetadata {
        city,
        contract_kind,
    })
}

fn parse_weather_market_city(title: &str) -> Option<WeatherSeriesMetadata> {
    let lower = title.to_lowercase();
    let city = if let Some((_, tail)) = title.split_once(" in ") {
        tail.split(" be ").next().map(str::trim)
    } else {
        None
    }
    .and_then(canonical_weather_city)?;
    let contract_kind = if lower.contains("high temp") || lower.contains("highest temperature") {
        WEATHER_HIGH_KIND.to_string()
    } else if lower.contains("low temp") || lower.contains("lowest temperature") {
        WEATHER_LOW_KIND.to_string()
    } else {
        return None;
    };
    Some(WeatherSeriesMetadata {
        city,
        contract_kind,
    })
}

fn canonical_weather_city(input: &str) -> Option<String> {
    let mut city = input.trim().to_string();
    for suffix in [
        " Daily High Temperature",
        " Daily Low Temperature",
        " Highest Temperature",
        " Lowest Temperature",
        " Maximum Temperature",
        " Minimum Temperature",
        " Max Temperature",
        " Min Temperature",
        " High Temperature",
        " Low Temperature",
        " High Temp",
        " Low Temp",
    ] {
        if city.to_lowercase().ends_with(&suffix.to_lowercase()) {
            let keep_len = city.len().saturating_sub(suffix.len());
            city = city[..keep_len].trim().to_string();
            break;
        }
    }
    let canonical = match city.trim().to_ascii_uppercase().as_str() {
        "NYC" => "New York City".to_string(),
        "PHIL" => "Philadelphia".to_string(),
        "LAX" => "Los Angeles".to_string(),
        "SEA" => "Seattle".to_string(),
        "SATX" => "San Antonio".to_string(),
        "ATL" => "Atlanta".to_string(),
        "AUS" => "Austin".to_string(),
        "HOU" => "Houston".to_string(),
        "MIA" => "Miami".to_string(),
        "DEN" => "Denver".to_string(),
        "DAL" => "Dallas".to_string(),
        "CHI" => "Chicago".to_string(),
        value if !value.is_empty() => city.trim().to_string(),
        _ => return None,
    };
    Some(canonical)
}

fn parse_market_date_from_event_ticker(event_ticker: &str) -> Option<String> {
    let raw = event_ticker.split('-').nth(1)?;
    if raw.len() != 7 {
        return None;
    }
    let year = format!("20{}", &raw[0..2]);
    let month = match &raw[2..5] {
        "JAN" => "01",
        "FEB" => "02",
        "MAR" => "03",
        "APR" => "04",
        "MAY" => "05",
        "JUN" => "06",
        "JUL" => "07",
        "AUG" => "08",
        "SEP" => "09",
        "OCT" => "10",
        "NOV" => "11",
        "DEC" => "12",
        _ => return None,
    };
    let day = &raw[5..7];
    Some(format!("{year}-{month}-{day}"))
}

fn weather_metadata_from_market(market: &KalshiMarketState) -> Option<WeatherMarketMetadata> {
    if market.market_family != MarketFamily::Weather {
        return None;
    }
    Some(WeatherMarketMetadata {
        city: market
            .metadata_json
            .get("city")
            .and_then(Value::as_str)?
            .to_string(),
        contract_kind: market
            .metadata_json
            .get("contract_kind")
            .and_then(Value::as_str)?
            .to_string(),
        market_date: market
            .metadata_json
            .get("market_date")
            .and_then(Value::as_str)?
            .to_string(),
        strike_type: market
            .metadata_json
            .get("strike_type")
            .and_then(Value::as_str)
            .unwrap_or("between")
            .to_string(),
        floor_strike: parse_f64(market.metadata_json.get("floor_strike")),
        cap_strike: parse_f64(market.metadata_json.get("cap_strike")),
    })
}

fn date_days_from_now(market_date: &str) -> i64 {
    chrono::NaiveDate::parse_from_str(market_date, "%Y-%m-%d")
        .ok()
        .map(|date| {
            let today = Utc::now().date_naive();
            (date - today).num_days().max(0)
        })
        .unwrap_or_default()
}

fn weather_city_coordinates(city: &str) -> Option<(f64, f64)> {
    match city.to_ascii_lowercase().as_str() {
        "new york city" => Some((40.7128, -74.0060)),
        "philadelphia" => Some((39.9526, -75.1652)),
        "los angeles" => Some((34.0522, -118.2437)),
        "seattle" => Some((47.6062, -122.3321)),
        "san antonio" => Some((29.4241, -98.4936)),
        "atlanta" => Some((33.7490, -84.3880)),
        "austin" => Some((30.2672, -97.7431)),
        "houston" => Some((29.7604, -95.3698)),
        "miami" => Some((25.7617, -80.1918)),
        "denver" => Some((39.7392, -104.9903)),
        "dallas" => Some((32.7767, -96.7970)),
        "chicago" => Some((41.8781, -87.6298)),
        "minneapolis" => Some((44.9778, -93.2650)),
        _ => None,
    }
}

fn nws_temperature_to_fahrenheit(value: f64, unit: Option<&str>) -> f64 {
    match unit.unwrap_or("F").to_ascii_uppercase().as_str() {
        "C" | "CELSIUS" => celsius_to_fahrenheit(value),
        _ => value,
    }
}

fn celsius_to_fahrenheit(value: f64) -> f64 {
    (value * 9.0 / 5.0) + 32.0
}

fn stable_market_id(input: &str) -> i64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & 0x7fff_ffff_ffff_ffff) as i64
}

fn parse_time(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64).or_else(|| {
        value
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<f64>().ok())
    })
}

fn parse_f64_from_candidates(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| parse_f64(value.get(*key)))
}

fn to_f64(value: Option<&Value>) -> f64 {
    parse_f64(value).unwrap_or_default()
}

fn to_i64(value: Option<&Value>) -> i64 {
    value
        .and_then(Value::as_i64)
        .or_else(|| {
            value
                .and_then(Value::as_str)
                .and_then(|raw| raw.parse::<i64>().ok())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::should_fetch_next_market_page;
    use common::MarketFamily;

    #[test]
    fn crypto_market_pagination_continues_until_near_term_market_is_found() {
        assert!(should_fetch_next_market_page(
            MarketFamily::Crypto,
            false,
            1,
            true
        ));
        assert!(!should_fetch_next_market_page(
            MarketFamily::Crypto,
            true,
            1,
            true
        ));
        assert!(!should_fetch_next_market_page(
            MarketFamily::Crypto,
            false,
            4,
            true
        ));
    }

    #[test]
    fn weather_market_pagination_stays_single_page() {
        assert!(!should_fetch_next_market_page(
            MarketFamily::Weather,
            false,
            1,
            true
        ));
    }
}

fn cents_to_dollars(value: Option<&Value>) -> f64 {
    to_f64(value) / 100.0
}

fn midpoint(best_bid: f64, best_ask: f64, last_price: f64) -> f64 {
    if best_bid > 0.0 && best_ask > 0.0 {
        return ((best_bid + best_ask) / 2.0).clamp(0.01, 0.99);
    }
    if last_price > 0.0 {
        return last_price.clamp(0.01, 0.99);
    }
    0.5
}

struct LiveBalance {
    bankroll: f64,
    available_balance: f64,
    open_exposure: f64,
}

async fn signed_get_json(
    config: &AppConfig,
    http: &HttpClient,
    path: &str,
) -> Result<Option<Value>> {
    let Some(api_key_id) = kalshi_api_key_id(config)? else {
        return Ok(None);
    };
    let Some(private_key) = kalshi_private_key(config)? else {
        return Ok(None);
    };
    let timestamp = kalshi_timestamp_ms();
    let signature = kalshi_sign_rest_request(&private_key, &timestamp, "GET", path)?;
    let url = format!("{}{}", config.kalshi_api_base.trim_end_matches('/'), path);
    let payload: Value = http
        .get(url)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("KALSHI-ACCESS-KEY", api_key_id)
        .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
        .header("KALSHI-ACCESS-SIGNATURE", signature)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(Some(payload))
}

fn normalize_live_positions(payload: &Value) -> Vec<LivePositionSyncRecord> {
    payload
        .get("market_positions")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let market_ticker = item.get("ticker").and_then(Value::as_str)?.to_string();
                    Some(LivePositionSyncRecord {
                        market_ticker,
                        position_count: to_f64(item.get("position")),
                        resting_order_count: to_i64(item.get("resting_orders_count")) as i32,
                        fees_paid: cents_to_dollars(item.get("fees_paid")),
                        market_exposure: cents_to_dollars(item.get("market_exposure")),
                        realized_pnl: cents_to_dollars(item.get("realized_pnl")),
                        details_json: item.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_live_orders(payload: &Value) -> Vec<LiveOrderSyncRecord> {
    payload
        .get("orders")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    Some(LiveOrderSyncRecord {
                        order_id: item.get("order_id")?.as_str()?.to_string(),
                        client_order_id: item
                            .get("client_order_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        market_ticker: item
                            .get("ticker")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        action: item
                            .get("action")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        side: item
                            .get("side")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        status: item
                            .get("status")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        count: to_f64(item.get("count")),
                        fill_count: to_f64(item.get("fill_count")),
                        remaining_count: to_f64(item.get("remaining_count")),
                        yes_price: cents_to_dollars(item.get("yes_price")),
                        no_price: cents_to_dollars(item.get("no_price")),
                        expiration_time: item.get("expiration_time").and_then(parse_time),
                        created_time: item.get("created_time").and_then(parse_time),
                        details_json: item.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_live_fills(payload: &Value) -> Vec<LiveFillSyncRecord> {
    payload
        .get("fills")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let fill_id = item
                        .get("fill_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            item.get("fill_group_id")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        })?;
                    Some(LiveFillSyncRecord {
                        fill_id,
                        order_id: item
                            .get("order_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        client_order_id: item
                            .get("client_order_id")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        market_ticker: item
                            .get("ticker")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        action: item
                            .get("action")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        side: item
                            .get("side")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        count: to_f64(item.get("count")),
                        yes_price: cents_to_dollars(item.get("yes_price")),
                        no_price: cents_to_dollars(item.get("no_price")),
                        fee_paid: cents_to_dollars(item.get("fee_paid")),
                        created_time: item.get("created_time").and_then(parse_time),
                        details_json: item.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}
