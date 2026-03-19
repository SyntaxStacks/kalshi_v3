use anyhow::Result;
use async_nats::Client as NatsClient;
use chrono::{DateTime, Utc};
use common::{
    AppConfig, KalshiMarketState, ReferencePriceState, TradeMode, events::ReferencePriceTick,
    kalshi_api_key_id, kalshi_private_key, kalshi_sign_rest_request,
    kalshi_sign_ws_connect_request, kalshi_timestamp_ms, subjects,
};
use futures::{SinkExt, StreamExt};
use redis::AsyncCommands;
use reqwest::Client as HttpClient;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
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
const REDIS_TTL_SECONDS: u64 = 300;
const WS_RECONNECT_MAX_SECONDS: u64 = 30;

type MarketCache = Arc<RwLock<HashMap<String, KalshiMarketState>>>;
type ReferenceCache = Arc<RwLock<HashMap<String, ReferencePriceState>>>;

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
    let reference_cache: ReferenceCache = Arc::new(RwLock::new(HashMap::new()));

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
        match recovery_sync_once(
            &config,
            &http,
            &redis_client,
            nats.as_ref(),
            &storage,
            &market_cache,
            &reference_cache,
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
) {
    let mut backoff_seconds = 1u64;
    loop {
        match kalshi_websocket_session(
            &config,
            &redis_client,
            &storage,
            nats.as_ref(),
            &market_cache,
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
    }

    while let Some(message) = socket.next().await {
        let message = message?;
        match message {
            Message::Text(text) => {
                if let Ok(payload) = serde_json::from_str::<Value>(&text) {
                    process_kalshi_ws_message(redis_client, storage, nats, market_cache, &payload)
                        .await?;
                }
            }
            Message::Binary(bytes) => {
                if let Ok(payload) = serde_json::from_slice::<Value>(&bytes) {
                    process_kalshi_ws_message(redis_client, storage, nats, market_cache, &payload)
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

async fn fetch_kalshi_markets(
    config: &AppConfig,
    http: &HttpClient,
) -> Result<Vec<KalshiMarketState>> {
    let mut out = Vec::new();
    for series in &config.kalshi_series_tickers {
        let url = format!("{}/markets", config.kalshi_api_base.trim_end_matches('/'));
        let response = http
            .get(url)
            .query(&[("series_ticker", series.as_str()), ("limit", "50")])
            .send()
            .await?
            .error_for_status()?;
        let payload: Value = response.json().await?;
        let Some(markets) = payload.get("markets").and_then(Value::as_array) else {
            continue;
        };
        for item in markets {
            if let Some(market) = normalize_market(item) {
                out.push(market);
            }
        }
    }
    out.sort_by(|left, right| left.close_at.cmp(&right.close_at));
    Ok(out)
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
        source: "coinbase_proxy".to_string(),
        symbol: symbol.to_uppercase(),
        price,
        previous_price,
        updated_at: Utc::now(),
    }))
}

fn normalize_market(value: &Value) -> Option<KalshiMarketState> {
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
    let symbol = infer_symbol(&ticker, &title)?;
    let best_bid = to_f64(value.get("yes_bid_dollars"));
    let best_ask = to_f64(value.get("yes_ask_dollars"));
    let last_price = to_f64(value.get("last_price_dollars"));
    let market_prob = midpoint(best_bid, best_ask, last_price);
    let bid_size = to_f64(value.get("yes_bid_size_fp"));
    let ask_size = to_f64(value.get("yes_ask_size_fp"));
    let liquidity = bid_size + ask_size + to_f64(value.get("open_interest_fp"));

    Some(KalshiMarketState {
        market_id: stable_market_id(&ticker),
        market_ticker: ticker,
        market_title: title,
        symbol,
        window_minutes: 15,
        market_prob,
        best_bid,
        best_ask,
        last_price,
        bid_size,
        ask_size,
        liquidity,
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
        market_ticker: ticker,
        market_title: existing.market_title.clone(),
        symbol: existing.symbol.clone(),
        window_minutes: existing.window_minutes,
        market_prob: midpoint(best_bid, best_ask, last_price),
        best_bid,
        best_ask,
        last_price,
        bid_size,
        ask_size,
        liquidity: bid_size + ask_size + open_interest.max(0.0),
        close_at: existing.close_at,
        created_at,
    })
}

fn reference_key(symbol: &str) -> String {
    format!("v3:reference:{}", symbol.to_uppercase())
}

fn infer_symbol(ticker: &str, title: &str) -> Option<String> {
    for candidate in ["BTC", "ETH", "SOL", "XRP"] {
        if ticker.contains(candidate) || title.to_uppercase().contains(candidate) {
            return Some(candidate.to_string());
        }
    }
    None
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
