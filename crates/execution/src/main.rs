use anyhow::Result;
use chrono::Utc;
use common::{
    AppConfig, ExecutionScoreInputs, ExpiryRegime, MarketFamily, StrategyFamily, TradeMode,
    build_execution_score, compute_execution_score_bps, directional_raw_edge_bps,
    estimate_fill_probability, kalshi_api_key_id, kalshi_private_key, kalshi_sign_rest_request,
    kalshi_timestamp_ms,
};
use market_data::{ContractSide, QueueSideSummary, QueueSnapshot};
use redis::AsyncCommands;
use reqwest::{Client, StatusCode};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use serde_json::json;
use storage::{ActiveLiveExecutionIntent, ExecutionIntentStateUpdate, OpenTradeForExit};
use tracing::{info, warn};

const EXECUTION_SCORE_THRESHOLD_BPS: f64 = 10.0;
const REDIS_QUEUE_PREFIX: &str = "v3:queue:";
const PRICE_TICK: f64 = 0.01;
const ENTRY_MAX_SPREAD_BPS: f64 = 250.0;
const EXIT_MAX_SPREAD_BPS: f64 = 400.0;
const ENTRY_MAX_QUEUE_AHEAD: f64 = 125.0;
const ENTRY_MAX_CROSS_QUEUE_AHEAD: f64 = 40.0;
const MIN_POST_FEE_EDGE_BPS: f64 = 5.0;
const CROSS_EXECUTION_SCORE_BPS: f64 = 40.0;
const URGENT_EXIT_SECONDS: i32 = 90;
const EXECUTION_SCORE_DIVERGENCE_WARN_BPS: f64 = 5.0;

#[derive(Clone)]
struct LiveKalshiClient {
    http: Client,
    api_base: String,
    api_key_id: String,
    private_key: RsaPrivateKey,
}

#[derive(Clone)]
struct PublicKalshiClient {
    http: Client,
    api_base: String,
}

struct LiveFillExecution {
    order_id: String,
    client_order_id: String,
    quantity: f64,
    fill_price: f64,
    fees: f64,
    status: String,
}

enum LiveOrderPlacement {
    Filled(LiveFillExecution),
    NoFill {
        reason: String,
        order_id: Option<String>,
        client_order_id: Option<String>,
        status: Option<String>,
    },
    Retryable {
        reason: String,
        order_id: Option<String>,
        client_order_id: Option<String>,
        status: Option<String>,
        orphaned: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouterAction {
    Skip,
    Join,
    ImproveOneTick,
    Cross,
}

#[derive(Debug, Clone)]
struct RoutingDecision {
    action: RouterAction,
    limit_price: f64,
    time_in_force: String,
    reason: Option<String>,
    queue_ahead: f64,
    post_fee_edge_bps: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct ContractBook {
    best_bid: f64,
    best_ask: f64,
}

#[derive(Deserialize)]
struct KalshiOrderEnvelope {
    order: KalshiOrder,
}

#[derive(Deserialize, Default)]
struct KalshiOrdersEnvelope {
    #[serde(default)]
    orders: Vec<KalshiOrder>,
}

#[derive(Deserialize, Clone)]
struct KalshiOrder {
    order_id: String,
    #[serde(default)]
    ticker: String,
    #[serde(default)]
    client_order_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    yes_price_dollars: String,
    #[serde(default)]
    no_price_dollars: String,
    #[serde(default)]
    fill_count_fp: String,
    #[serde(default)]
    taker_fill_cost_dollars: String,
    #[serde(default)]
    maker_fill_cost_dollars: String,
    #[serde(default)]
    taker_fees_dollars: String,
    #[serde(default)]
    maker_fees_dollars: String,
    #[serde(default)]
    created_time: String,
    #[serde(default)]
    last_update_time: String,
}

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
    let public_client = build_public_client(&config)?;
    let live_client = build_live_client(&config)?;
    storage
        .upsert_worker_started(
            "execution",
            &json!({
                "live_client_ready": live_client.is_some(),
                "live_trading_enabled": config.live_trading_enabled,
                "live_order_placement_enabled": config.live_order_placement_enabled
            }),
        )
        .await?;

    info!(
        live_client_ready = live_client.is_some(),
        "execution_worker_started"
    );
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(
        config.execution_poll_seconds.max(5),
    ));
    loop {
        interval.tick().await;
        storage
            .upsert_worker_started(
                "execution",
                &json!({
                    "phase": "execute",
                    "live_client_ready": live_client.is_some(),
                    "live_order_placement_enabled": config.live_order_placement_enabled
                }),
            )
            .await?;
        match execute_once(
            &storage,
            &redis_client,
            &public_client,
            live_client.as_ref(),
            config.live_order_placement_enabled,
            &config.live_entry_time_in_force,
            &config.live_exit_time_in_force,
            config.live_order_replace_enabled,
            config.live_order_replace_after_seconds,
            config.live_order_stale_after_seconds,
            config.market_stale_after_seconds,
            config.reference_stale_after_seconds,
        )
        .await
        {
            Ok((
                repaired,
                reconciled,
                live_reconciled,
                live_attention,
                live_intents_synced,
                live_orders_cleaned,
                live_exit_synced,
                live_exit_replaced,
                opened,
                rejected,
                closed,
            )) => {
                storage
                    .upsert_worker_success(
                        "execution",
                        &json!({
                            "repaired": repaired,
                            "reconciled": reconciled,
                            "live_reconciled": live_reconciled,
                            "live_attention": live_attention,
                            "live_intents_synced": live_intents_synced,
                            "live_orders_cleaned": live_orders_cleaned,
                            "live_exit_synced": live_exit_synced,
                            "live_exit_replaced": live_exit_replaced,
                            "opened": opened,
                            "rejected": rejected,
                            "closed": closed
                        }),
                    )
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("execution", &error.to_string(), &json!({}))
                    .await?;
                warn!(error = %error, "execution_pass_failed");
            }
        }
    }
}

async fn execute_once(
    storage: &storage::Storage,
    redis_client: &redis::Client,
    public_client: &PublicKalshiClient,
    live_client: Option<&LiveKalshiClient>,
    live_order_placement_enabled_config: bool,
    live_entry_time_in_force: &str,
    live_exit_time_in_force: &str,
    live_order_replace_enabled: bool,
    live_order_replace_after_seconds: i64,
    live_order_stale_after_seconds: i64,
    market_stale_after_seconds: i64,
    reference_stale_after_seconds: i64,
) -> Result<(
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
    usize,
)> {
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    storage
        .reconcile_bankroll_for_mode(common::TradeMode::Paper)
        .await?;
    let repaired = storage
        .repair_pending_execution_intents_from_trades()
        .await?;
    let reconciled = reconcile_pending_intents(storage).await?;
    let (live_reconciled, live_attention) = storage.reconcile_live_trades_from_sync().await?;
    let (live_intents_synced, live_orders_cleaned) = reconcile_active_live_intents(
        storage,
        &mut redis,
        live_client,
        live_entry_time_in_force,
        live_order_replace_enabled,
        live_order_replace_after_seconds,
        live_order_stale_after_seconds,
        market_stale_after_seconds,
        reference_stale_after_seconds,
    )
    .await?;
    let (live_exit_synced, live_exit_replaced) = reconcile_active_live_exit_orders(
        storage,
        live_client,
        live_exit_time_in_force,
        live_order_replace_enabled,
        live_order_replace_after_seconds,
        live_order_stale_after_seconds,
        market_stale_after_seconds,
    )
    .await?;
    let live_order_placement_enabled = storage
        .effective_live_order_placement_enabled(live_order_placement_enabled_config)
        .await?;

    let mut paper_crypto_remaining = storage
        .latest_family_bankroll(TradeMode::Paper, MarketFamily::Crypto)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(0.0);
    let mut paper_weather_remaining = storage
        .latest_family_bankroll(TradeMode::Paper, MarketFamily::Weather)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(0.0);
    let mut live_remaining = storage
        .latest_family_bankroll(TradeMode::Live, MarketFamily::Crypto)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(0.0);

    let mut intents = storage.list_pending_execution_intents(12).await?;
    intents.sort_by(|left, right| {
        pending_intent_priority(right)
            .total_cmp(&pending_intent_priority(left))
            .then_with(|| left.created_at.cmp(&right.created_at))
    });
    let mut opened = 0usize;
    let mut rejected = 0usize;
    for intent in intents {
        if storage
            .has_open_trade_for_market(intent.market_id, intent.market_ticker.as_deref())
            .await?
        {
            storage
                .mark_execution_intent_status(
                    intent.intent_id,
                    "rejected",
                    Some("existing_open_trade_for_market"),
                )
                .await?;
            rejected += 1;
            continue;
        }

        let theoretical_entry_price = contract_price_for_side(&intent.side, intent.market_prob);
        let position_notional = intent.recommended_size.max(0.0);
        let remaining_budget = match intent.mode {
            TradeMode::Paper => match intent.market_family {
                MarketFamily::Weather => paper_weather_remaining,
                _ => paper_crypto_remaining,
            },
            TradeMode::Live => live_remaining,
        };
        if position_notional <= 0.0 || remaining_budget + f64::EPSILON < position_notional {
            storage
                .mark_execution_intent_status(
                    intent.intent_id,
                    "rejected",
                    Some("insufficient_deployable_balance"),
                )
                .await?;
            rejected += 1;
            continue;
        }

        match intent.mode {
            TradeMode::Paper => {
                if intent.market_family == MarketFamily::Crypto {
                    let Some(snapshot) = storage
                        .latest_feature_snapshot_for_market(intent.market_id)
                        .await?
                    else {
                        storage
                            .mark_execution_intent_status(
                                intent.intent_id,
                                "rejected",
                                Some("missing_feature_snapshot"),
                            )
                            .await?;
                        rejected += 1;
                        continue;
                    };
                    let queue_snapshot =
                        load_queue_snapshot(&mut redis, &snapshot.market_ticker).await?;
                    let threshold = execution_score_threshold(&intent);
                    let current_score =
                        current_crypto_execution_score(&intent, &snapshot, queue_snapshot.as_ref())
                            .unwrap_or(f64::NEG_INFINITY);
                    maybe_warn_execution_score_divergence(&intent, current_score);
                    if current_score <= threshold {
                        storage
                            .mark_execution_intent_status(
                                intent.intent_id,
                                "rejected",
                                Some("execution_score_below_threshold"),
                            )
                            .await?;
                        rejected += 1;
                        continue;
                    }
                }
                let quantity = if intent.submitted_quantity > 0.0 {
                    intent.submitted_quantity
                } else {
                    (position_notional / theoretical_entry_price.max(0.05)).max(1.0)
                };
                storage
                    .open_trade_from_intent(&intent, quantity, theoretical_entry_price)
                    .await?;
                match intent.market_family {
                    MarketFamily::Weather => {
                        paper_weather_remaining = (paper_weather_remaining
                            - (quantity * theoretical_entry_price))
                            .max(0.0);
                    }
                    _ => {
                        paper_crypto_remaining = (paper_crypto_remaining
                            - (quantity * theoretical_entry_price))
                            .max(0.0);
                    }
                }
                opened += 1;
            }
            TradeMode::Live => {
                if !live_order_placement_enabled {
                    storage
                        .transition_execution_intent(
                            intent.intent_id,
                            "cancelled",
                            Some("live_order_placement_disabled"),
                            &ExecutionIntentStateUpdate {
                                details_json: json!({
                                    "operator_disabled": true,
                                }),
                                ..Default::default()
                            },
                        )
                        .await?;
                    rejected += 1;
                    continue;
                }
                let Some(live_client) = live_client else {
                    warn!(
                        intent_id = intent.intent_id,
                        lane = %intent.lane_key,
                        "live_execution_skipped_missing_client"
                    );
                    continue;
                };
                let Some(snapshot) = storage
                    .latest_feature_snapshot_for_market(intent.market_id)
                    .await?
                else {
                    storage
                        .mark_execution_intent_status(
                            intent.intent_id,
                            "rejected",
                            Some("missing_feature_snapshot"),
                        )
                        .await?;
                    rejected += 1;
                    continue;
                };
                let queue_snapshot =
                    load_queue_snapshot(&mut redis, &snapshot.market_ticker).await?;
                let threshold = execution_score_threshold(&intent);
                let current_score =
                    current_crypto_execution_score(&intent, &snapshot, queue_snapshot.as_ref())
                        .unwrap_or(f64::NEG_INFINITY);
                maybe_warn_execution_score_divergence(&intent, current_score);
                if intent.market_family == MarketFamily::Crypto && current_score <= threshold {
                    storage
                        .mark_execution_intent_status(
                            intent.intent_id,
                            "rejected",
                            Some("execution_score_below_threshold"),
                        )
                        .await?;
                    rejected += 1;
                    continue;
                }
                let quantity = if intent.submitted_quantity > 0.0 {
                    intent.submitted_quantity.floor()
                } else {
                    (position_notional / theoretical_entry_price.max(0.05)).floor()
                };
                if quantity < 1.0 {
                    storage
                        .mark_execution_intent_status(
                            intent.intent_id,
                            "rejected",
                            Some("size_too_small_for_live_contract"),
                        )
                        .await?;
                    rejected += 1;
                    continue;
                }
                let route = route_live_entry(
                    &snapshot,
                    &intent.side,
                    queue_snapshot.as_ref(),
                    intent.model_prob,
                    intent.predicted_fill_probability,
                    intent.predicted_slippage_bps,
                    current_score,
                    threshold,
                    live_entry_time_in_force,
                    market_stale_after_seconds,
                    reference_stale_after_seconds,
                );
                if route.action == RouterAction::Skip {
                    storage
                        .mark_execution_intent_status(
                            intent.intent_id,
                            "rejected",
                            route.reason.as_deref(),
                        )
                        .await?;
                    rejected += 1;
                    continue;
                }

                let client_order_id = format!("v3-live-entry-{}", intent.intent_id);
                storage
                    .transition_execution_intent(
                        intent.intent_id,
                        "submitted",
                        None,
                        &ExecutionIntentStateUpdate {
                            market_ticker: Some(snapshot.market_ticker.clone()),
                            side: Some(intent.side.clone()),
                            client_order_id: Some(client_order_id.clone()),
                            accepted_quantity: Some(0.0),
                            filled_quantity: Some(0.0),
                            cancelled_quantity: Some(0.0),
                            rejected_quantity: Some(0.0),
                            details_json: json!({
                                "market_ticker": snapshot.market_ticker.clone(),
                                "quantity": quantity,
                                "entry_price": route.limit_price,
                                "routing_action": format!("{:?}", route.action),
                                "queue_ahead": route.queue_ahead,
                                "post_fee_edge_bps": route.post_fee_edge_bps,
                                "time_in_force": route.time_in_force.clone(),
                            }),
                            ..Default::default()
                        },
                    )
                    .await?;

                match place_live_entry(
                    live_client,
                    &snapshot.market_ticker,
                    &intent.side,
                    quantity as i64,
                    route.limit_price,
                    &client_order_id,
                    &route.time_in_force,
                )
                .await?
                {
                    LiveOrderPlacement::Filled(fill) => {
                        storage
                            .transition_execution_intent(
                                intent.intent_id,
                                "acknowledged",
                                None,
                                &ExecutionIntentStateUpdate {
                                    market_ticker: Some(snapshot.market_ticker.clone()),
                                    side: Some(intent.side.clone()),
                                    client_order_id: Some(fill.client_order_id.clone()),
                                    exchange_order_id: Some(fill.order_id.clone()),
                                    fill_status: Some(fill.status.clone()),
                                    details_json: json!({
                                        "phase": "entry_acknowledged",
                                        "order_id": fill.order_id.clone(),
                                    }),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        let live_metadata = json!({
                            "market_ticker": snapshot.market_ticker.clone(),
                            "execution_mode": "live",
                            "entry_order_id": fill.order_id.clone(),
                            "entry_client_order_id": fill.client_order_id.clone(),
                            "entry_fee": fill.fees,
                            "entry_status": fill.status.clone(),
                        });
                        storage
                            .open_trade_from_intent_with_metadata_and_status(
                                &intent,
                                fill.quantity,
                                fill.fill_price,
                                &live_metadata,
                                "filled",
                            )
                            .await?;
                        live_remaining = (live_remaining
                            - ((fill.quantity * fill.fill_price) + fill.fees))
                            .max(0.0);
                        opened += 1;
                    }
                    LiveOrderPlacement::NoFill {
                        reason,
                        order_id,
                        client_order_id,
                        status,
                    } => {
                        let intent_status = match status.as_deref() {
                            Some("cancelled") | Some("canceled") => "cancelled",
                            Some("acknowledged") | Some("resting") | Some("open") => "acknowledged",
                            _ => "rejected",
                        };
                        storage
                            .transition_execution_intent(
                                intent.intent_id,
                                intent_status,
                                Some(&reason),
                                &ExecutionIntentStateUpdate {
                                    market_ticker: Some(snapshot.market_ticker.clone()),
                                    side: Some(intent.side.clone()),
                                    client_order_id,
                                    exchange_order_id: order_id,
                                    fill_status: status,
                                    accepted_quantity: Some(if intent_status == "acknowledged" {
                                        quantity
                                    } else {
                                        0.0
                                    }),
                                    filled_quantity: Some(0.0),
                                    cancelled_quantity: Some(if intent_status == "cancelled" {
                                        quantity
                                    } else {
                                        0.0
                                    }),
                                    rejected_quantity: Some(if intent_status == "rejected" {
                                        quantity
                                    } else {
                                        0.0
                                    }),
                                    terminal_outcome: terminal_outcome_for_status(intent_status),
                                    details_json: json!({
                                        "phase": "entry_no_fill",
                                    }),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        rejected += 1;
                    }
                    LiveOrderPlacement::Retryable {
                        reason,
                        order_id,
                        client_order_id,
                        status,
                        orphaned,
                    } => {
                        let intent_status = if orphaned { "orphaned" } else { "submitted" };
                        storage
                            .transition_execution_intent(
                                intent.intent_id,
                                intent_status,
                                Some(&reason),
                                &ExecutionIntentStateUpdate {
                                    market_ticker: Some(snapshot.market_ticker.clone()),
                                    side: Some(intent.side.clone()),
                                    client_order_id,
                                    exchange_order_id: order_id,
                                    fill_status: status,
                                    details_json: json!({
                                        "phase": "entry_retryable",
                                        "orphaned": orphaned,
                                    }),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        warn!(
                            intent_id = intent.intent_id,
                            lane = %intent.lane_key,
                            error = %reason,
                            "live_entry_retryable_failure"
                        );
                    }
                }
            }
        }
    }

    let open_trades = storage.list_open_trades_for_exit().await?;
    let mut closed = 0usize;
    for trade in open_trades {
        let now = chrono::Utc::now();
        let trade_age_seconds = (chrono::Utc::now() - trade.created_at).num_seconds();
        if should_force_reconcile_stale_paper_crypto_trade(&trade, trade_age_seconds) {
            if let Some(settlement) =
                settle_paper_crypto_trade_from_market_result(public_client, &trade).await?
            {
                close_trade_for_mode(
                    storage,
                    live_client,
                    &trade,
                    settlement.exit_price,
                    &settlement.status,
                    settlement.realized_pnl,
                    live_exit_time_in_force,
                    market_stale_after_seconds,
                )
                .await?;
                closed += 1;
                continue;
            }
            close_trade_for_mode(
                storage,
                live_client,
                &trade,
                trade.entry_price,
                "stale_trade_reconcile",
                0.0,
                live_exit_time_in_force,
                market_stale_after_seconds,
            )
            .await?;
            closed += 1;
            continue;
        }
        if let Some(snapshot) = storage
            .latest_feature_snapshot_for_market(trade.market_id)
            .await?
        {
            if trade.expires_at.is_none() && trade_age_seconds >= 20 * 60 {
                let status = "expiry_reconcile";
                if trade.mode == TradeMode::Live && live_trade_has_active_exit_order(&trade) {
                    continue;
                }
                close_trade_for_mode(
                    storage,
                    live_client,
                    &trade,
                    trade.entry_price,
                    status,
                    0.0,
                    live_exit_time_in_force,
                    market_stale_after_seconds,
                )
                .await?;
                closed += 1;
                continue;
            }
            let should_timeout =
                trade.timeout_seconds > 0 && trade_age_seconds >= i64::from(trade.timeout_seconds);
            let should_force_exit = snapshot.seconds_to_expiry <= trade.force_exit_buffer_seconds;

            if should_timeout || should_force_exit {
                if trade.mode == TradeMode::Live && live_trade_has_active_exit_order(&trade) {
                    continue;
                }
                let status = exit_status_label(trade.strategy_family, should_timeout);
                let exit_price = contract_price_for_side(&trade.side, snapshot.market_prob);
                close_trade_for_mode(
                    storage,
                    live_client,
                    &trade,
                    exit_price,
                    status,
                    0.0,
                    live_exit_time_in_force,
                    market_stale_after_seconds,
                )
                .await?;
                closed += 1;
            }
        } else if trade
            .expires_at
            .map(|expires_at| now >= expires_at)
            .unwrap_or_else(|| (now - trade.created_at).num_minutes() >= 20)
        {
            if trade.mode == TradeMode::Live && live_trade_has_active_exit_order(&trade) {
                continue;
            }
            close_trade_for_mode(
                storage,
                live_client,
                &trade,
                trade.entry_price,
                "expiry_reconcile",
                0.0,
                live_exit_time_in_force,
                market_stale_after_seconds,
            )
            .await?;
            closed += 1;
        }
    }

    info!(
        repaired,
        reconciled,
        live_reconciled,
        live_attention,
        live_intents_synced,
        live_orders_cleaned,
        live_exit_synced,
        live_exit_replaced,
        opened,
        rejected,
        closed,
        "execution_pass_succeeded"
    );
    Ok((
        repaired,
        reconciled,
        live_reconciled,
        live_attention,
        live_intents_synced,
        live_orders_cleaned,
        live_exit_synced,
        live_exit_replaced,
        opened,
        rejected,
        closed,
    ))
}

async fn reconcile_pending_intents(storage: &storage::Storage) -> Result<usize> {
    let intents = storage.list_pending_execution_intents(200).await?;
    let mut reconciled = 0usize;
    for intent in intents {
        let mut status = None;
        let mut reason = None;
        if storage
            .has_open_trade_for_market(intent.market_id, intent.market_ticker.as_deref())
            .await?
        {
            status = Some("superseded");
            reason = Some("open_trade_exists_for_market");
        } else if let Some(snapshot) = storage
            .latest_feature_snapshot_for_market(intent.market_id)
            .await?
        {
            if snapshot.seconds_to_expiry <= intent.force_exit_buffer_seconds {
                status = Some("expired");
                reason = Some("market_too_close_to_expiry");
            }
        } else if (chrono::Utc::now() - intent.created_at).num_minutes() >= 20 {
            status = Some("expired");
            reason = Some("missing_market_snapshot");
        }

        if status.is_none() {
            if let Some(replacement) = storage
                .latest_replacement_decision_for_lane(&intent.lane_key, intent.created_at)
                .await?
            {
                if should_supersede_pending_intent(
                    intent.market_id,
                    intent.market_ticker.as_deref(),
                    &replacement,
                ) {
                    status = Some("superseded");
                    reason = Some("approved_replacement_for_same_market");
                }
            }
        }

        if let Some(status) = status {
            storage
                .mark_execution_intent_status(intent.intent_id, status, reason)
                .await?;
            reconciled += 1;
        }
    }
    Ok(reconciled)
}

async fn reconcile_active_live_intents(
    storage: &storage::Storage,
    redis: &mut redis::aio::MultiplexedConnection,
    live_client: Option<&LiveKalshiClient>,
    entry_time_in_force: &str,
    replace_enabled: bool,
    replace_after_seconds: i64,
    stale_after_seconds: i64,
    market_stale_after_seconds: i64,
    reference_stale_after_seconds: i64,
) -> Result<(usize, usize)> {
    let Some(live_client) = live_client else {
        return Ok((0, 0));
    };

    let intents = storage.list_active_live_execution_intents(64).await?;
    let mut synced = 0usize;
    let mut cleaned = 0usize;
    for intent in intents {
        let age_seconds = (chrono::Utc::now() - intent.last_transition_at).num_seconds();
        let Some(order_id) = intent.exchange_order_id.as_deref() else {
            if intent.status == "submitted" && age_seconds >= stale_after_seconds {
                storage
                    .transition_execution_intent(
                        intent.intent_id,
                        "orphaned",
                        Some("submitted_without_exchange_order_id"),
                        &ExecutionIntentStateUpdate {
                            market_ticker: intent.market_ticker.clone(),
                            side: Some(intent.side.clone()),
                            client_order_id: intent.client_order_id.clone(),
                            details_json: json!({
                                "phase": "live_intent_reconcile",
                                "age_seconds": age_seconds,
                            }),
                            ..Default::default()
                        },
                    )
                    .await?;
                synced += 1;
                cleaned += 1;
            }
            continue;
        };

        let order = match fetch_order(live_client, order_id).await {
            Ok(order) => order,
            Err(error) => {
                warn!(
                    intent_id = intent.intent_id,
                    order_id,
                    error = %error,
                    "live_intent_fetch_failed"
                );
                if age_seconds >= stale_after_seconds {
                    storage
                        .transition_execution_intent(
                            intent.intent_id,
                            "orphaned",
                            Some("exchange_order_lookup_failed"),
                            &ExecutionIntentStateUpdate {
                                market_ticker: intent.market_ticker.clone(),
                                side: Some(intent.side.clone()),
                                client_order_id: intent.client_order_id.clone(),
                                exchange_order_id: intent.exchange_order_id.clone(),
                                details_json: json!({
                                    "phase": "live_intent_reconcile",
                                    "age_seconds": age_seconds,
                                }),
                                ..Default::default()
                            },
                        )
                        .await?;
                    synced += 1;
                    cleaned += 1;
                }
                continue;
            }
        };

        let synced_this_intent = sync_live_intent_from_exchange(storage, &intent, &order).await?;
        synced += synced_this_intent;

        let intent_state = classify_live_intent_state(&order);
        if replace_enabled
            && age_seconds >= replace_after_seconds
            && matches!(intent_state, "acknowledged" | "partially_filled")
            && order_is_resting(&order.status)
            && !time_in_force_is_fill_or_kill(entry_time_in_force)
        {
            if replace_live_entry_order(
                storage,
                redis,
                live_client,
                &intent,
                &order,
                entry_time_in_force,
                market_stale_after_seconds,
                reference_stale_after_seconds,
            )
            .await?
            {
                synced += 1;
                cleaned += 1;
                continue;
            }
        }

        if age_seconds >= stale_after_seconds
            && matches!(intent_state, "acknowledged" | "partially_filled")
            && order_is_resting(&order.status)
        {
            storage
                .transition_execution_intent(
                    intent.intent_id,
                    "cancel_pending",
                    Some("stale_live_order"),
                    &ExecutionIntentStateUpdate {
                        market_ticker: Some(
                            intent
                                .market_ticker
                                .clone()
                                .unwrap_or_else(|| order_market_ticker(&order)),
                        ),
                        side: Some(intent.side.clone()),
                        client_order_id: Some(order_client_order_id(
                            &order,
                            intent.client_order_id.as_deref(),
                        )),
                        exchange_order_id: Some(order.order_id.clone()),
                        fill_status: Some(order.status.clone()),
                        details_json: json!({
                            "phase": "cancel_stale_live_order",
                            "age_seconds": age_seconds,
                        }),
                        ..Default::default()
                    },
                )
                .await?;

            match cancel_live_order(live_client, &order.order_id).await {
                Ok(true) => {
                    match fetch_order(live_client, &order.order_id).await {
                        Ok(refreshed) => {
                            synced += sync_live_intent_from_exchange(storage, &intent, &refreshed)
                                .await?;
                        }
                        Err(_) => {
                            storage
                                .transition_execution_intent(
                                    intent.intent_id,
                                    "cancelled",
                                    Some("stale_live_order_cancelled"),
                                    &ExecutionIntentStateUpdate {
                                        market_ticker: Some(
                                            intent
                                                .market_ticker
                                                .clone()
                                                .unwrap_or_else(|| order_market_ticker(&order)),
                                        ),
                                        side: Some(intent.side.clone()),
                                        client_order_id: Some(order_client_order_id(
                                            &order,
                                            intent.client_order_id.as_deref(),
                                        )),
                                        exchange_order_id: Some(order.order_id.clone()),
                                        fill_status: Some("cancelled".to_string()),
                                        details_json: json!({
                                            "phase": "cancel_stale_live_order",
                                            "age_seconds": age_seconds,
                                        }),
                                        ..Default::default()
                                    },
                                )
                                .await?;
                            synced += 1;
                        }
                    }
                    cleaned += 1;
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        intent_id = intent.intent_id,
                        order_id = %order.order_id,
                        error = %error,
                        "cancel_stale_live_order_failed"
                    );
                }
            }
        }
    }

    Ok((synced, cleaned))
}

async fn sync_live_intent_from_exchange(
    storage: &storage::Storage,
    intent: &ActiveLiveExecutionIntent,
    order: &KalshiOrder,
) -> Result<usize> {
    let next_status = classify_live_intent_state(order);
    let fill_quantity = parse_fp(&order.fill_count_fp);
    let fill_price = contract_fill_price(order, contract_side(&intent.side));
    let fees = first_non_zero(&[&order.taker_fees_dollars, &order.maker_fees_dollars]);
    let client_order_id = order_client_order_id(order, intent.client_order_id.as_deref());
    let market_ticker = intent
        .market_ticker
        .clone()
        .unwrap_or_else(|| order_market_ticker(order));
    let mut transitioned_via_trade_open = false;

    if fill_quantity >= 1.0 {
        if storage
            .has_open_trade_for_market(intent.market_id, intent.market_ticker.as_deref())
            .await?
        {
            storage
                .upsert_open_live_trade_fill_progress(
                    intent.decision_id,
                    fill_quantity,
                    fill_price,
                    fees,
                    next_status,
                    &json!({
                        "market_ticker": market_ticker,
                        "execution_mode": "live",
                        "entry_order_id": order.order_id,
                        "entry_client_order_id": client_order_id,
                        "entry_fee": fees,
                        "entry_status": order.status,
                    }),
                )
                .await?;
        } else if let Some(intent_record) = storage.execution_intent_by_id(intent.intent_id).await?
        {
            storage
                .open_trade_from_intent_with_metadata_and_status(
                    &intent_record,
                    fill_quantity,
                    fill_price,
                    &json!({
                        "market_ticker": market_ticker,
                        "execution_mode": "live",
                        "entry_order_id": order.order_id,
                        "entry_client_order_id": client_order_id,
                        "entry_fee": fees,
                        "entry_status": order.status,
                    }),
                    next_status,
                )
                .await?;
            transitioned_via_trade_open = true;
        }
    }

    if !transitioned_via_trade_open
        && (intent.status != next_status
            || intent.fill_status.as_deref() != Some(order.status.as_str())
            || intent.exchange_order_id.as_deref() != Some(order.order_id.as_str()))
    {
        storage
            .transition_execution_intent(
                intent.intent_id,
                next_status,
                None,
                &ExecutionIntentStateUpdate {
                    market_ticker: Some(market_ticker),
                    side: Some(intent.side.clone()),
                    client_order_id: Some(client_order_id),
                    exchange_order_id: Some(order.order_id.clone()),
                    fill_status: Some(order.status.clone()),
                    accepted_quantity: Some(
                        if matches!(next_status, "acknowledged" | "partially_filled" | "filled") {
                            intent.submitted_quantity.max(fill_quantity)
                        } else {
                            intent.accepted_quantity
                        },
                    ),
                    filled_quantity: Some(fill_quantity.max(intent.filled_quantity)),
                    cancelled_quantity: Some(if next_status == "cancelled" {
                        (intent.submitted_quantity - fill_quantity).max(0.0)
                    } else {
                        intent.cancelled_quantity
                    }),
                    rejected_quantity: Some(if next_status == "rejected" {
                        intent.submitted_quantity.max(0.0)
                    } else {
                        intent.rejected_quantity
                    }),
                    avg_fill_price: if fill_quantity > 0.0 {
                        Some(fill_price)
                    } else {
                        intent.avg_fill_price
                    },
                    first_fill_at: if fill_quantity > 0.0 && intent.filled_quantity <= 0.0 {
                        Some(Utc::now())
                    } else {
                        None
                    },
                    last_fill_at: if fill_quantity > 0.0 {
                        Some(Utc::now())
                    } else {
                        None
                    },
                    terminal_outcome: terminal_outcome_for_status(next_status),
                    details_json: json!({
                        "phase": "live_intent_sync",
                        "fill_quantity": fill_quantity,
                        "fill_price": fill_price,
                        "fees": fees,
                    }),
                },
            )
            .await?;
        return Ok(1);
    }

    Ok(0)
}

async fn replace_live_entry_order(
    storage: &storage::Storage,
    redis: &mut redis::aio::MultiplexedConnection,
    live_client: &LiveKalshiClient,
    intent: &ActiveLiveExecutionIntent,
    order: &KalshiOrder,
    time_in_force: &str,
    market_stale_after_seconds: i64,
    reference_stale_after_seconds: i64,
) -> Result<bool> {
    let Some(snapshot) = storage
        .latest_feature_snapshot_for_market(intent.market_id)
        .await?
    else {
        return Ok(false);
    };
    if snapshot.seconds_to_expiry <= intent.force_exit_buffer_seconds {
        storage
            .transition_execution_intent(
                intent.intent_id,
                "expired",
                Some("replace_skipped_market_too_close_to_expiry"),
                &ExecutionIntentStateUpdate {
                    market_ticker: Some(snapshot.market_ticker.clone()),
                    side: Some(intent.side.clone()),
                    client_order_id: Some(order_client_order_id(
                        order,
                        intent.client_order_id.as_deref(),
                    )),
                    exchange_order_id: Some(order.order_id.clone()),
                    fill_status: Some(order.status.clone()),
                    details_json: json!({
                        "phase": "replace_live_entry_order",
                        "replaced": false,
                    }),
                    ..Default::default()
                },
            )
            .await?;
        return Ok(true);
    }

    if !cancel_live_order(live_client, &order.order_id).await? {
        return Ok(false);
    }

    let client_order_id = next_replacement_client_order_id(
        intent.client_order_id.as_deref(),
        &format!("v3-live-entry-{}", intent.intent_id),
    );
    let quantity = remaining_live_entry_quantity(intent);
    if quantity < 1 {
        storage
            .transition_execution_intent(
                intent.intent_id,
                "cancelled",
                Some("no_residual_quantity_after_replace"),
                &ExecutionIntentStateUpdate {
                    market_ticker: Some(snapshot.market_ticker.clone()),
                    side: Some(intent.side.clone()),
                    client_order_id: Some(client_order_id),
                    exchange_order_id: Some(order.order_id.clone()),
                    fill_status: Some("cancelled".to_string()),
                    cancelled_quantity: Some(
                        intent.cancelled_quantity.max(0.0)
                            + (intent.submitted_quantity.max(0.0)
                                - intent.filled_quantity.max(0.0)
                                - intent.cancelled_quantity.max(0.0)
                                - intent.rejected_quantity.max(0.0))
                            .max(0.0),
                    ),
                    details_json: json!({
                        "phase": "replace_live_entry_order",
                        "replaced": false,
                    }),
                    ..Default::default()
                },
            )
            .await?;
        return Ok(true);
    }
    let queue_snapshot = load_queue_snapshot(redis, &snapshot.market_ticker).await?;
    let pending_record = storage.execution_intent_by_id(intent.intent_id).await?;
    let threshold = pending_record
        .as_ref()
        .map(execution_score_threshold)
        .unwrap_or(EXECUTION_SCORE_THRESHOLD_BPS);
    let current_score = if intent.market_family == MarketFamily::Crypto {
        pending_record
            .as_ref()
            .and_then(|record| {
                current_crypto_execution_score(record, &snapshot, queue_snapshot.as_ref())
            })
            .unwrap_or(f64::NEG_INFINITY)
    } else {
        f64::INFINITY
    };
    if let Some(record) = pending_record.as_ref() {
        maybe_warn_execution_score_divergence(record, current_score);
    }
    let model_prob = pending_record
        .as_ref()
        .map(|record| record.model_prob)
        .unwrap_or(snapshot.market_prob);
    let route = route_live_entry(
        &snapshot,
        &intent.side,
        queue_snapshot.as_ref(),
        model_prob,
        pending_record
            .as_ref()
            .and_then(|record| record.predicted_fill_probability),
        pending_record
            .as_ref()
            .and_then(|record| record.predicted_slippage_bps),
        current_score,
        threshold,
        time_in_force,
        market_stale_after_seconds,
        reference_stale_after_seconds,
    );
    if route.action == RouterAction::Skip {
        storage
            .transition_execution_intent(
                intent.intent_id,
                "cancelled",
                route.reason.as_deref(),
                &ExecutionIntentStateUpdate {
                    market_ticker: Some(snapshot.market_ticker.clone()),
                    side: Some(intent.side.clone()),
                    client_order_id: Some(client_order_id),
                    exchange_order_id: Some(order.order_id.clone()),
                    fill_status: Some("cancelled".to_string()),
                    cancelled_quantity: Some(intent.cancelled_quantity.max(0.0) + quantity as f64),
                    details_json: json!({
                        "phase": "replace_live_entry_order",
                        "replaced": false,
                        "routing_action": format!("{:?}", route.action),
                    }),
                    ..Default::default()
                },
            )
            .await?;
        return Ok(true);
    }

    storage
        .transition_execution_intent(
            intent.intent_id,
            "submitted",
            Some("live_order_replaced"),
            &ExecutionIntentStateUpdate {
                market_ticker: Some(snapshot.market_ticker.clone()),
                side: Some(intent.side.clone()),
                client_order_id: Some(client_order_id.clone()),
                fill_status: Some("replaced".to_string()),
                details_json: json!({
                    "phase": "replace_live_entry_order",
                    "replaced_order_id": order.order_id,
                    "quantity": quantity,
                    "entry_price": route.limit_price,
                    "routing_action": format!("{:?}", route.action),
                    "queue_ahead": route.queue_ahead,
                    "post_fee_edge_bps": route.post_fee_edge_bps,
                    "time_in_force": route.time_in_force.clone(),
                }),
                ..Default::default()
            },
        )
        .await?;

    let placement = place_live_entry(
        live_client,
        &snapshot.market_ticker,
        &intent.side,
        quantity,
        route.limit_price,
        &client_order_id,
        &route.time_in_force,
    )
    .await?;
    handle_live_entry_placement_result(storage, intent, &snapshot.market_ticker, placement).await?;
    Ok(true)
}

async fn handle_live_entry_placement_result(
    storage: &storage::Storage,
    intent: &ActiveLiveExecutionIntent,
    market_ticker: &str,
    placement: LiveOrderPlacement,
) -> Result<()> {
    match placement {
        LiveOrderPlacement::Filled(fill) => {
            storage
                .transition_execution_intent(
                    intent.intent_id,
                    "acknowledged",
                    None,
                    &ExecutionIntentStateUpdate {
                        market_ticker: Some(market_ticker.to_string()),
                        side: Some(intent.side.clone()),
                        client_order_id: Some(fill.client_order_id.clone()),
                        exchange_order_id: Some(fill.order_id.clone()),
                        fill_status: Some(fill.status.clone()),
                        details_json: json!({
                            "phase": "entry_acknowledged",
                            "order_id": fill.order_id.clone(),
                        }),
                        ..Default::default()
                    },
                )
                .await?;

            let live_metadata = json!({
                "market_ticker": market_ticker,
                "execution_mode": "live",
                "entry_order_id": fill.order_id.clone(),
                "entry_client_order_id": fill.client_order_id.clone(),
                "entry_fee": fill.fees,
                "entry_status": fill.status.clone(),
            });
            if storage
                .has_open_trade_for_market(intent.market_id, intent.market_ticker.as_deref())
                .await?
            {
                storage
                    .upsert_open_live_trade_fill_progress(
                        intent.decision_id,
                        fill.quantity,
                        fill.fill_price,
                        fill.fees,
                        &fill.status,
                        &live_metadata,
                    )
                    .await?;
            } else if let Some(intent_record) =
                storage.execution_intent_by_id(intent.intent_id).await?
            {
                storage
                    .open_trade_from_intent_with_metadata_and_status(
                        &intent_record,
                        fill.quantity,
                        fill.fill_price,
                        &live_metadata,
                        "filled",
                    )
                    .await?;
            }
        }
        LiveOrderPlacement::NoFill {
            reason,
            order_id,
            client_order_id,
            status,
        } => {
            let intent_status = match status.as_deref() {
                Some("cancelled") | Some("canceled") => "cancelled",
                Some("acknowledged") | Some("resting") | Some("open") => "acknowledged",
                _ => "rejected",
            };
            storage
                .transition_execution_intent(
                    intent.intent_id,
                    intent_status,
                    Some(&reason),
                    &ExecutionIntentStateUpdate {
                        market_ticker: Some(market_ticker.to_string()),
                        side: Some(intent.side.clone()),
                        client_order_id,
                        exchange_order_id: order_id,
                        fill_status: status,
                        accepted_quantity: Some(if intent_status == "acknowledged" {
                            intent.submitted_quantity.max(0.0)
                        } else {
                            0.0
                        }),
                        filled_quantity: Some(intent.filled_quantity.max(0.0)),
                        cancelled_quantity: Some(if intent_status == "cancelled" {
                            (intent.submitted_quantity - intent.filled_quantity).max(0.0)
                        } else {
                            intent.cancelled_quantity.max(0.0)
                        }),
                        rejected_quantity: Some(if intent_status == "rejected" {
                            intent.submitted_quantity.max(0.0)
                        } else {
                            intent.rejected_quantity.max(0.0)
                        }),
                        terminal_outcome: terminal_outcome_for_status(intent_status),
                        details_json: json!({
                            "phase": "entry_no_fill",
                        }),
                        ..Default::default()
                    },
                )
                .await?;
        }
        LiveOrderPlacement::Retryable {
            reason,
            order_id,
            client_order_id,
            status,
            orphaned,
        } => {
            let intent_status = if orphaned { "orphaned" } else { "submitted" };
            storage
                .transition_execution_intent(
                    intent.intent_id,
                    intent_status,
                    Some(&reason),
                    &ExecutionIntentStateUpdate {
                        market_ticker: Some(market_ticker.to_string()),
                        side: Some(intent.side.clone()),
                        client_order_id,
                        exchange_order_id: order_id,
                        fill_status: status,
                        details_json: json!({
                            "phase": "entry_retryable",
                            "orphaned": orphaned,
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            warn!(
                intent_id = intent.intent_id,
                lane = %intent.lane_key,
                error = %reason,
                "live_entry_retryable_failure"
            );
        }
    }
    Ok(())
}

async fn reconcile_active_live_exit_orders(
    storage: &storage::Storage,
    live_client: Option<&LiveKalshiClient>,
    exit_time_in_force: &str,
    replace_enabled: bool,
    replace_after_seconds: i64,
    stale_after_seconds: i64,
    market_stale_after_seconds: i64,
) -> Result<(usize, usize)> {
    let Some(live_client) = live_client else {
        return Ok((0, 0));
    };

    let open_trades = storage.list_open_trades_for_exit().await?;
    let mut synced = 0usize;
    let mut replaced = 0usize;
    for trade in open_trades
        .into_iter()
        .filter(|trade| trade.mode == TradeMode::Live && live_trade_has_active_exit_order(trade))
    {
        let Some(order_id) = trade.exit_exchange_order_id.as_deref() else {
            continue;
        };
        let order = match fetch_order(live_client, order_id).await {
            Ok(order) => order,
            Err(error) => {
                warn!(
                    trade_id = trade.trade_id,
                    order_id,
                    error = %error,
                    "live_exit_order_fetch_failed"
                );
                storage
                    .update_live_trade_exit_order(
                        trade.trade_id,
                        trade.exit_exchange_order_id.as_deref(),
                        trade.exit_client_order_id.as_deref(),
                        Some("orphaned"),
                        &json!({
                            "phase": "live_exit_order_reconcile",
                            "fetch_failed": true,
                        }),
                    )
                    .await?;
                synced += 1;
                continue;
            }
        };

        let next_status = classify_live_intent_state(&order);
        storage
            .update_live_trade_exit_order(
                trade.trade_id,
                Some(&order.order_id),
                Some(&order_client_order_id(
                    &order,
                    trade.exit_client_order_id.as_deref(),
                )),
                Some(&order.status),
                &json!({
                    "phase": "live_exit_order_reconcile",
                    "intent_state": next_status,
                    "fill_quantity": parse_fp(&order.fill_count_fp),
                }),
            )
            .await?;
        synced += 1;

        let age_seconds = order_age_seconds(&order)
            .unwrap_or_else(|| (chrono::Utc::now() - trade.created_at).num_seconds().max(0));

        if replace_enabled
            && age_seconds >= replace_after_seconds
            && order_is_resting(&order.status)
            && !time_in_force_is_fill_or_kill(exit_time_in_force)
        {
            if replace_live_exit_order(
                storage,
                live_client,
                &trade,
                &order,
                exit_time_in_force,
                market_stale_after_seconds,
            )
            .await?
            {
                replaced += 1;
                continue;
            }
        }

        if age_seconds >= stale_after_seconds && order_is_resting(&order.status) {
            if cancel_live_order(live_client, &order.order_id).await? {
                storage
                    .update_live_trade_exit_order(
                        trade.trade_id,
                        Some(&order.order_id),
                        Some(&order_client_order_id(
                            &order,
                            trade.exit_client_order_id.as_deref(),
                        )),
                        Some("cancelled"),
                        &json!({
                            "phase": "cancel_stale_live_exit_order",
                            "age_seconds": age_seconds,
                        }),
                    )
                    .await?;
                synced += 1;
            }
        }
    }

    Ok((synced, replaced))
}

async fn replace_live_exit_order(
    storage: &storage::Storage,
    live_client: &LiveKalshiClient,
    trade: &OpenTradeForExit,
    order: &KalshiOrder,
    time_in_force: &str,
    market_stale_after_seconds: i64,
) -> Result<bool> {
    if !cancel_live_order(live_client, &order.order_id).await? {
        return Ok(false);
    }
    let Some(snapshot) = storage
        .latest_feature_snapshot_for_market(trade.market_id)
        .await?
    else {
        storage
            .update_live_trade_exit_order(
                trade.trade_id,
                Some(&order.order_id),
                Some(&order_client_order_id(
                    order,
                    trade.exit_client_order_id.as_deref(),
                )),
                Some("cancelled"),
                &json!({
                    "phase": "replace_live_exit_order",
                    "replaced": false,
                    "reason": "missing_feature_snapshot",
                }),
            )
            .await?;
        return Ok(true);
    };

    if snapshot.seconds_to_expiry <= trade.force_exit_buffer_seconds {
        storage
            .update_live_trade_exit_order(
                trade.trade_id,
                Some(&order.order_id),
                Some(&order_client_order_id(
                    order,
                    trade.exit_client_order_id.as_deref(),
                )),
                Some("cancelled"),
                &json!({
                    "phase": "replace_live_exit_order",
                    "replaced": false,
                    "reason": "market_too_close_to_expiry",
                }),
            )
            .await?;
        return Ok(true);
    }

    let quantity = trade.quantity.round() as i64;
    if quantity <= 0 {
        return Ok(true);
    }
    let route = route_live_exit(
        &snapshot,
        &trade.side,
        time_in_force,
        market_stale_after_seconds,
        trade.force_exit_buffer_seconds,
    );
    if route.action == RouterAction::Skip {
        storage
            .update_live_trade_exit_order(
                trade.trade_id,
                Some(&order.order_id),
                Some(&order_client_order_id(
                    order,
                    trade.exit_client_order_id.as_deref(),
                )),
                Some("cancelled"),
                &json!({
                    "phase": "replace_live_exit_order",
                    "replaced": false,
                    "reason": route.reason,
                }),
            )
            .await?;
        return Ok(true);
    }
    let client_order_id = next_replacement_client_order_id(
        trade.exit_client_order_id.as_deref(),
        &format!("v3-live-exit-{}", trade.trade_id),
    );
    let placement = place_live_exit_with_client_order_id(
        live_client,
        trade
            .market_ticker
            .as_deref()
            .unwrap_or(&snapshot.market_ticker),
        &trade.side,
        quantity,
        route.limit_price,
        &client_order_id,
        &route.time_in_force,
    )
    .await?;
    handle_live_exit_placement_result(storage, trade, placement, "replaced_live_exit").await?;
    Ok(true)
}

async fn handle_live_exit_placement_result(
    storage: &storage::Storage,
    trade: &OpenTradeForExit,
    placement: LiveOrderPlacement,
    close_status: &str,
) -> Result<()> {
    match placement {
        LiveOrderPlacement::Filled(fill) => {
            let realized_quantity = fill.quantity.min(trade.quantity);
            let realized_pnl = ((fill.fill_price - trade.entry_price) * realized_quantity)
                - proportional_entry_fee(trade.entry_fee, realized_quantity, trade.quantity)
                - fill.fees;
            let metadata = json!({
                "execution_mode": "live",
                "exit_order_id": fill.order_id,
                "exit_client_order_id": fill.client_order_id,
                "exit_fee": fill.fees,
                "exit_status": fill.status,
                "exit_quantity": realized_quantity,
            });
            if realized_quantity + 0.01 >= trade.quantity {
                storage
                    .close_trade_with_metadata(
                        trade.trade_id,
                        fill.fill_price,
                        realized_pnl,
                        close_status,
                        &metadata,
                    )
                    .await?;
            } else {
                storage
                    .partially_close_trade_with_metadata(
                        trade.trade_id,
                        realized_quantity,
                        fill.fill_price,
                        realized_pnl,
                        "partial_exit",
                        &metadata,
                    )
                    .await?;
            }
        }
        LiveOrderPlacement::NoFill {
            reason,
            order_id,
            client_order_id,
            status,
        } => {
            storage
                .update_live_trade_exit_order(
                    trade.trade_id,
                    order_id.as_deref(),
                    client_order_id.as_deref(),
                    status.as_deref(),
                    &json!({
                        "execution_mode": "live",
                        "exit_status": status,
                        "exit_reason": reason,
                    }),
                )
                .await?;
        }
        LiveOrderPlacement::Retryable {
            reason,
            order_id,
            client_order_id,
            status,
            orphaned,
        } => {
            storage
                .update_live_trade_exit_order(
                    trade.trade_id,
                    order_id.as_deref(),
                    client_order_id.as_deref(),
                    status.as_deref(),
                    &json!({
                        "execution_mode": "live",
                        "exit_status": status,
                        "exit_reason": reason,
                        "orphaned": orphaned,
                    }),
                )
                .await?;
            warn!(
                trade_id = trade.trade_id,
                reason = %reason,
                "live_exit_retryable_failure"
            );
        }
    }

    Ok(())
}

async fn close_trade_for_mode(
    storage: &storage::Storage,
    live_client: Option<&LiveKalshiClient>,
    trade: &OpenTradeForExit,
    theoretical_exit_price: f64,
    status: &str,
    fallback_realized_pnl: f64,
    exit_time_in_force: &str,
    market_stale_after_seconds: i64,
) -> Result<()> {
    match trade.mode {
        TradeMode::Paper => {
            let realized_pnl = if status == "market_result_reconcile" {
                fallback_realized_pnl
            } else {
                (theoretical_exit_price - trade.entry_price) * trade.quantity
            };
            storage
                .close_trade(trade.trade_id, theoretical_exit_price, realized_pnl, status)
                .await?;
        }
        TradeMode::Live => {
            let Some(live_client) = live_client else {
                warn!(
                    trade_id = trade.trade_id,
                    "live_exit_skipped_missing_client"
                );
                return Ok(());
            };
            let Some(snapshot) = storage
                .latest_feature_snapshot_for_market(trade.market_id)
                .await?
            else {
                warn!(
                    trade_id = trade.trade_id,
                    "live_exit_skipped_missing_snapshot"
                );
                return Ok(());
            };
            let quantity = trade.quantity.round() as i64;
            if quantity <= 0 {
                warn!(
                    trade_id = trade.trade_id,
                    "live_exit_skipped_invalid_quantity"
                );
                return Ok(());
            }
            let route = route_live_exit(
                &snapshot,
                &trade.side,
                exit_time_in_force,
                market_stale_after_seconds,
                trade.force_exit_buffer_seconds,
            );
            if route.action == RouterAction::Skip {
                warn!(
                    trade_id = trade.trade_id,
                    reason = ?route.reason,
                    "live_exit_skipped_by_router"
                );
                return Ok(());
            }
            match place_live_exit(
                live_client,
                &snapshot.market_ticker,
                &trade.side,
                quantity,
                route.limit_price,
                trade.trade_id,
                &route.time_in_force,
            )
            .await?
            {
                placement => {
                    handle_live_exit_placement_result(storage, trade, placement, status).await?
                }
            }
        }
    }
    let _ = fallback_realized_pnl;
    Ok(())
}

fn build_public_client(config: &AppConfig) -> Result<PublicKalshiClient> {
    let http = Client::builder().user_agent("kalshi-v3/0.1").build()?;
    Ok(PublicKalshiClient {
        http,
        api_base: config.kalshi_api_base.trim_end_matches('/').to_string(),
    })
}

fn build_live_client(config: &AppConfig) -> Result<Option<LiveKalshiClient>> {
    if !config.live_trading_enabled {
        return Ok(None);
    }
    let Some(api_key_id) = kalshi_api_key_id(config)? else {
        return Ok(None);
    };
    let Some(private_key) = kalshi_private_key(config)? else {
        return Ok(None);
    };
    let http = Client::builder().user_agent("kalshi-v3/0.1").build()?;
    Ok(Some(LiveKalshiClient {
        http,
        api_base: config.kalshi_api_base.trim_end_matches('/').to_string(),
        api_key_id,
        private_key,
    }))
}

struct SettlementReconcile {
    exit_price: f64,
    realized_pnl: f64,
    status: String,
}

async fn settle_paper_crypto_trade_from_market_result(
    public_client: &PublicKalshiClient,
    trade: &OpenTradeForExit,
) -> Result<Option<SettlementReconcile>> {
    if trade.mode != TradeMode::Paper || trade.market_family != MarketFamily::Crypto {
        return Ok(None);
    }
    let Some(market_ticker) = trade.market_ticker.as_deref() else {
        return Ok(None);
    };
    let url = format!("{}/markets/{}", public_client.api_base, market_ticker);
    let payload: serde_json::Value = public_client
        .http
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let Some(market) = payload.get("market") else {
        return Ok(None);
    };
    let status = market
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(status.as_str(), "finalized" | "settled" | "resolved") {
        return Ok(None);
    }
    let result = market
        .get("result")
        .and_then(serde_json::Value::as_str)
        .map(|value| value.to_ascii_lowercase());
    let resolved_yes = match result.as_deref() {
        Some("yes") => Some(true),
        Some("no") => Some(false),
        _ => market
            .get("settlement_value_dollars")
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| raw.parse::<f64>().ok())
            .map(|value| value >= 0.5),
    };
    let Some(resolved_yes) = resolved_yes else {
        return Ok(None);
    };
    let exit_price = settlement_contract_price(&trade.side, resolved_yes);
    let realized_pnl = (exit_price - trade.entry_price) * trade.quantity;
    Ok(Some(SettlementReconcile {
        exit_price,
        realized_pnl,
        status: "market_result_reconcile".to_string(),
    }))
}

async fn place_live_entry(
    client: &LiveKalshiClient,
    market_ticker: &str,
    side: &str,
    quantity: i64,
    contract_price: f64,
    client_order_id: &str,
    time_in_force: &str,
) -> Result<LiveOrderPlacement> {
    let contract_side = contract_side(side);
    let body = order_payload(
        market_ticker,
        "buy",
        contract_side,
        quantity,
        contract_price,
        client_order_id,
        false,
        time_in_force,
    );
    place_live_order(
        client,
        "/portfolio/orders",
        market_ticker,
        &body,
        client_order_id,
        contract_side,
    )
    .await
}

async fn place_live_exit(
    client: &LiveKalshiClient,
    market_ticker: &str,
    side: &str,
    quantity: i64,
    contract_price: f64,
    trade_id: i64,
    time_in_force: &str,
) -> Result<LiveOrderPlacement> {
    let client_order_id = format!("v3-live-exit-{trade_id}");
    place_live_exit_with_client_order_id(
        client,
        market_ticker,
        side,
        quantity,
        contract_price,
        &client_order_id,
        time_in_force,
    )
    .await
}

async fn place_live_exit_with_client_order_id(
    client: &LiveKalshiClient,
    market_ticker: &str,
    side: &str,
    quantity: i64,
    contract_price: f64,
    client_order_id: &str,
    time_in_force: &str,
) -> Result<LiveOrderPlacement> {
    let contract_side = contract_side(side);
    let body = order_payload(
        market_ticker,
        "sell",
        contract_side,
        quantity,
        contract_price,
        client_order_id,
        true,
        time_in_force,
    );
    place_live_order(
        client,
        "/portfolio/orders",
        market_ticker,
        &body,
        client_order_id,
        contract_side,
    )
    .await
}

async fn place_live_order(
    client: &LiveKalshiClient,
    path: &str,
    market_ticker: &str,
    body: &serde_json::Value,
    client_order_id: &str,
    contract_side: &str,
) -> Result<LiveOrderPlacement> {
    let response = signed_request(client, "POST", path, Some(body)).await?;
    if response.status() == StatusCode::CONFLICT {
        return recover_duplicate_live_order(client, market_ticker, client_order_id, contract_side)
            .await;
    }
    if response.status().is_client_error() {
        return Ok(LiveOrderPlacement::NoFill {
            reason: format!(
                "client_error:{}:{}",
                response.status(),
                response.text().await?
            ),
            order_id: None,
            client_order_id: Some(client_order_id.to_string()),
            status: Some("rejected".to_string()),
        });
    }
    if response.status().is_server_error() {
        return Ok(LiveOrderPlacement::Retryable {
            reason: format!(
                "server_error:{}:{}",
                response.status(),
                response.text().await?
            ),
            order_id: None,
            client_order_id: Some(client_order_id.to_string()),
            status: None,
            orphaned: false,
        });
    }
    let envelope: KalshiOrderEnvelope = response.error_for_status()?.json().await?;
    let order = match fetch_order(client, &envelope.order.order_id).await {
        Ok(fetched) => fetched,
        Err(error) => {
            warn!(
                order_id = %envelope.order.order_id,
                error = %error,
                "live_order_fetch_after_create_failed"
            );
            envelope.order
        }
    };

    let quantity = parse_fp(&order.fill_count_fp);
    if quantity < 1.0 {
        return Ok(LiveOrderPlacement::NoFill {
            reason: format!("order_status:{}", order.status),
            order_id: Some(order.order_id.clone()),
            client_order_id: Some(if order.client_order_id.is_empty() {
                client_order_id.to_string()
            } else {
                order.client_order_id.clone()
            }),
            status: Some(order.status),
        });
    }

    Ok(LiveOrderPlacement::Filled(LiveFillExecution {
        order_id: order.order_id.clone(),
        client_order_id: if order.client_order_id.is_empty() {
            client_order_id.to_string()
        } else {
            order.client_order_id.clone()
        },
        quantity,
        fill_price: contract_fill_price(&order, contract_side),
        fees: first_non_zero(&[&order.taker_fees_dollars, &order.maker_fees_dollars]),
        status: order.status,
    }))
}

async fn recover_duplicate_live_order(
    client: &LiveKalshiClient,
    market_ticker: &str,
    client_order_id: &str,
    contract_side: &str,
) -> Result<LiveOrderPlacement> {
    let path = format!("/portfolio/orders?ticker={market_ticker}&limit=100");
    let response = signed_request(client, "GET", &path, None).await?;
    if response.status().is_client_error() {
        return Ok(LiveOrderPlacement::NoFill {
            reason: format!(
                "duplicate_lookup_client_error:{}:{}",
                response.status(),
                response.text().await?
            ),
            order_id: None,
            client_order_id: Some(client_order_id.to_string()),
            status: Some("rejected".to_string()),
        });
    }
    if response.status().is_server_error() {
        return Ok(LiveOrderPlacement::Retryable {
            reason: format!(
                "duplicate_lookup_server_error:{}:{}",
                response.status(),
                response.text().await?
            ),
            order_id: None,
            client_order_id: Some(client_order_id.to_string()),
            status: None,
            orphaned: false,
        });
    }

    let envelope: KalshiOrdersEnvelope = response.error_for_status()?.json().await?;
    let Some(order) = envelope
        .orders
        .into_iter()
        .find(|order| order.client_order_id == client_order_id)
    else {
        return Ok(LiveOrderPlacement::Retryable {
            reason: format!("duplicate_client_order_id_not_found:{client_order_id}"),
            order_id: None,
            client_order_id: Some(client_order_id.to_string()),
            status: None,
            orphaned: true,
        });
    };

    let quantity = parse_fp(&order.fill_count_fp);
    if quantity < 1.0 {
        return Ok(LiveOrderPlacement::NoFill {
            reason: format!("duplicate_order_status:{}", order.status),
            order_id: Some(order.order_id.clone()),
            client_order_id: Some(if order.client_order_id.is_empty() {
                client_order_id.to_string()
            } else {
                order.client_order_id.clone()
            }),
            status: Some(order.status),
        });
    }

    Ok(LiveOrderPlacement::Filled(LiveFillExecution {
        order_id: order.order_id.clone(),
        client_order_id: if order.client_order_id.is_empty() {
            client_order_id.to_string()
        } else {
            order.client_order_id.clone()
        },
        quantity,
        fill_price: contract_fill_price(&order, contract_side),
        fees: first_non_zero(&[&order.taker_fees_dollars, &order.maker_fees_dollars]),
        status: order.status,
    }))
}

async fn fetch_order(client: &LiveKalshiClient, order_id: &str) -> Result<KalshiOrder> {
    let path = format!("/portfolio/orders/{order_id}");
    let response = signed_request(client, "GET", &path, None).await?;
    let envelope: KalshiOrderEnvelope = response.error_for_status()?.json().await?;
    Ok(envelope.order)
}

async fn cancel_live_order(client: &LiveKalshiClient, order_id: &str) -> Result<bool> {
    let path = format!("/portfolio/orders/{order_id}");
    let response = signed_request(client, "DELETE", &path, None).await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(true);
    }
    if response.status().is_client_error() || response.status().is_server_error() {
        return Ok(false);
    }
    Ok(true)
}

async fn signed_request(
    client: &LiveKalshiClient,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<reqwest::Response> {
    let timestamp = kalshi_timestamp_ms();
    let signature = kalshi_sign_rest_request(&client.private_key, &timestamp, method, path)?;
    let url = format!("{}{}", client.api_base, path);
    let mut request = match method {
        "GET" => client.http.get(url),
        "POST" => client.http.post(url),
        "DELETE" => client.http.delete(url),
        _ => client.http.get(url),
    }
    .header("Accept", "application/json")
    .header("Content-Type", "application/json")
    .header("KALSHI-ACCESS-KEY", &client.api_key_id)
    .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
    .header("KALSHI-ACCESS-SIGNATURE", signature);

    if let Some(body) = body {
        request = request.json(body);
    }
    Ok(request.send().await?)
}

fn order_payload(
    market_ticker: &str,
    action: &str,
    side: &str,
    quantity: i64,
    contract_price: f64,
    client_order_id: &str,
    reduce_only: bool,
    time_in_force: &str,
) -> serde_json::Value {
    let price_cents = (contract_price * 100.0).round().clamp(1.0, 99.0) as i64;
    let mut payload = json!({
        "ticker": market_ticker,
        "action": action,
        "side": side,
        "count": quantity,
        "type": "limit",
        "time_in_force": time_in_force,
        "client_order_id": client_order_id,
        "self_trade_prevention_type": "taker_at_cross",
    });

    if side == "yes" {
        payload["yes_price"] = json!(price_cents);
    } else {
        payload["no_price"] = json!(price_cents);
    }
    if action == "buy" {
        payload["buy_max_cost"] = json!(quantity * price_cents);
    }
    if reduce_only {
        payload["reduce_only"] = json!(true);
    }
    payload
}

fn contract_side(side: &str) -> &'static str {
    if side == "buy_no" { "no" } else { "yes" }
}

fn contract_price_for_side(side: &str, market_prob: f64) -> f64 {
    if side == "buy_no" {
        (1.0 - market_prob).clamp(0.01, 0.99)
    } else {
        market_prob.clamp(0.01, 0.99)
    }
}

fn contract_book_for_side(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
) -> ContractBook {
    let (best_bid, best_ask) = if contract_side(side) == "no" {
        (
            (1.0 - snapshot.best_ask).clamp(0.01, 0.99),
            (1.0 - snapshot.best_bid).clamp(0.01, 0.99),
        )
    } else {
        (
            snapshot.best_bid.clamp(0.01, 0.99),
            snapshot.best_ask.clamp(0.01, 0.99),
        )
    };
    ContractBook {
        best_bid: best_bid.min(best_ask),
        best_ask: best_ask.max(best_bid),
    }
}

fn book_spread_bps(book: ContractBook) -> f64 {
    ((book.best_ask - book.best_bid).max(0.0) * 10_000.0).max(0.0)
}

fn route_limit_price(book: ContractBook, order_action: &str, action: RouterAction) -> f64 {
    let spread = (book.best_ask - book.best_bid).max(0.0);
    match (order_action, action) {
        (_, RouterAction::Skip) => 0.0,
        ("buy", RouterAction::Join) => book.best_bid,
        ("buy", RouterAction::Cross) => book.best_ask,
        ("buy", RouterAction::ImproveOneTick) => {
            if spread >= PRICE_TICK * 2.0 {
                (book.best_bid + PRICE_TICK)
                    .min(book.best_ask - PRICE_TICK)
                    .clamp(0.01, 0.99)
            } else {
                book.best_bid
            }
        }
        ("sell", RouterAction::Join) => book.best_ask,
        ("sell", RouterAction::Cross) => book.best_bid,
        ("sell", RouterAction::ImproveOneTick) => {
            if spread >= PRICE_TICK * 2.0 {
                (book.best_ask - PRICE_TICK)
                    .max(book.best_bid + PRICE_TICK)
                    .clamp(0.01, 0.99)
            } else {
                book.best_ask
            }
        }
        _ => 0.0,
    }
}

fn buy_side_post_fee_edge_bps(
    fair_price: f64,
    execution_price: f64,
    predicted_slippage_bps: f64,
    adverse_selection_bps: f64,
) -> f64 {
    ((fair_price - execution_price) * 10_000.0) - predicted_slippage_bps - adverse_selection_bps
}

fn route_skip(reason: &str) -> RoutingDecision {
    RoutingDecision {
        action: RouterAction::Skip,
        limit_price: 0.0,
        time_in_force: "good_till_cancelled".to_string(),
        reason: Some(reason.to_string()),
        queue_ahead: 0.0,
        post_fee_edge_bps: None,
    }
}

fn same_market_identity(
    market_id: i64,
    market_ticker: Option<&str>,
    other_market_id: i64,
    other_market_ticker: Option<&str>,
) -> bool {
    market_id == other_market_id
        || match (market_ticker, other_market_ticker) {
            (Some(left), Some(right)) if !left.trim().is_empty() && !right.trim().is_empty() => {
                left == right
            }
            _ => false,
        }
}

fn should_supersede_pending_intent(
    intent_market_id: i64,
    intent_market_ticker: Option<&str>,
    replacement: &storage::ReplacementDecisionCandidate,
) -> bool {
    replacement.approved
        && same_market_identity(
            intent_market_id,
            intent_market_ticker,
            replacement.market_id,
            replacement.market_ticker.as_deref(),
        )
}

fn route_live_entry(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
    queue_snapshot: Option<&QueueSnapshot>,
    model_prob: f64,
    predicted_fill_probability: Option<f64>,
    predicted_slippage_bps: Option<f64>,
    current_execution_score_bps: f64,
    execution_score_threshold_bps: f64,
    default_time_in_force: &str,
    market_stale_after_seconds: i64,
    reference_stale_after_seconds: i64,
) -> RoutingDecision {
    if i64::from(snapshot.market_data_age_seconds.max(0)) > market_stale_after_seconds {
        return route_skip("stale_market_snapshot");
    }
    if i64::from(snapshot.reference_age_seconds.max(0)) > reference_stale_after_seconds {
        return route_skip("stale_reference_snapshot");
    }
    if current_execution_score_bps <= execution_score_threshold_bps {
        return route_skip("execution_score_below_threshold");
    }

    let book = contract_book_for_side(snapshot, side);
    let spread_bps = book_spread_bps(book);
    if spread_bps > ENTRY_MAX_SPREAD_BPS {
        return route_skip("spread_above_entry_cap");
    }

    let queue_ahead = queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.size_ahead_real.max(0.0))
        .unwrap_or(snapshot.size_ahead_real.max(snapshot.size_ahead).max(0.0));
    if queue_ahead > ENTRY_MAX_QUEUE_AHEAD {
        return route_skip("queue_ahead_above_cap");
    }

    let regime = execution_regime(snapshot);
    let fill_probability = predicted_fill_probability.unwrap_or_else(|| {
        estimate_fill_probability(
            queue_ahead.max(1.0),
            execution_queue_trade_consumption_rate(snapshot, queue_snapshot, side),
            execution_queue_cancel_rate(snapshot, queue_snapshot, side),
            f64::from(snapshot.seconds_to_expiry.max(1)),
            regime,
        )
    });
    let slippage_bps = predicted_slippage_bps.unwrap_or(snapshot.spread_bps * 0.5);
    let adverse_selection_bps = (snapshot.aggressive_buy_ratio * snapshot.spread_bps)
        + ((execution_queue_cancel_rate(snapshot, queue_snapshot, side)
            + execution_queue_decay_rate(snapshot, queue_snapshot, side))
            * 10.0)
        + (snapshot.quote_churn * 5.0);
    let fair_price = contract_price_for_side(side, model_prob);

    let join_price = route_limit_price(book, "buy", RouterAction::Join);
    let join_edge_bps =
        buy_side_post_fee_edge_bps(fair_price, join_price, slippage_bps, adverse_selection_bps);
    if join_edge_bps < MIN_POST_FEE_EDGE_BPS {
        return route_skip("post_fee_edge_below_minimum");
    }

    let improve_price = route_limit_price(book, "buy", RouterAction::ImproveOneTick);
    let improve_edge_bps = buy_side_post_fee_edge_bps(
        fair_price,
        improve_price,
        slippage_bps,
        adverse_selection_bps,
    );
    let cross_price = route_limit_price(book, "buy", RouterAction::Cross);
    let cross_edge_bps =
        buy_side_post_fee_edge_bps(fair_price, cross_price, slippage_bps, adverse_selection_bps);

    if current_execution_score_bps >= CROSS_EXECUTION_SCORE_BPS
        && fill_probability >= 0.75
        && queue_ahead <= ENTRY_MAX_CROSS_QUEUE_AHEAD
        && cross_edge_bps >= MIN_POST_FEE_EDGE_BPS
    {
        return RoutingDecision {
            action: RouterAction::Cross,
            limit_price: cross_price,
            time_in_force: "fill_or_kill".to_string(),
            reason: None,
            queue_ahead,
            post_fee_edge_bps: Some(cross_edge_bps),
        };
    }

    if improve_edge_bps >= MIN_POST_FEE_EDGE_BPS && spread_bps >= PRICE_TICK * 20_000.0 {
        return RoutingDecision {
            action: RouterAction::ImproveOneTick,
            limit_price: improve_price,
            time_in_force: default_time_in_force.to_string(),
            reason: None,
            queue_ahead,
            post_fee_edge_bps: Some(improve_edge_bps),
        };
    }

    RoutingDecision {
        action: RouterAction::Join,
        limit_price: join_price,
        time_in_force: default_time_in_force.to_string(),
        reason: None,
        queue_ahead,
        post_fee_edge_bps: Some(join_edge_bps),
    }
}

fn route_live_exit(
    snapshot: &common::MarketFeatureSnapshotRecord,
    side: &str,
    default_time_in_force: &str,
    market_stale_after_seconds: i64,
    force_exit_buffer_seconds: i32,
) -> RoutingDecision {
    if i64::from(snapshot.market_data_age_seconds.max(0)) > market_stale_after_seconds * 2 {
        return route_skip("stale_market_snapshot");
    }

    let book = contract_book_for_side(snapshot, side);
    let spread_bps = book_spread_bps(book);
    let urgent = snapshot.seconds_to_expiry <= URGENT_EXIT_SECONDS.max(force_exit_buffer_seconds);

    if urgent || spread_bps > EXIT_MAX_SPREAD_BPS {
        return RoutingDecision {
            action: RouterAction::Cross,
            limit_price: route_limit_price(book, "sell", RouterAction::Cross),
            time_in_force: "fill_or_kill".to_string(),
            reason: None,
            queue_ahead: 0.0,
            post_fee_edge_bps: None,
        };
    }

    if spread_bps >= PRICE_TICK * 20_000.0 {
        return RoutingDecision {
            action: RouterAction::ImproveOneTick,
            limit_price: route_limit_price(book, "sell", RouterAction::ImproveOneTick),
            time_in_force: default_time_in_force.to_string(),
            reason: None,
            queue_ahead: 0.0,
            post_fee_edge_bps: None,
        };
    }

    RoutingDecision {
        action: RouterAction::Join,
        limit_price: route_limit_price(book, "sell", RouterAction::Join),
        time_in_force: default_time_in_force.to_string(),
        reason: None,
        queue_ahead: 0.0,
        post_fee_edge_bps: None,
    }
}

fn remaining_live_entry_quantity(intent: &ActiveLiveExecutionIntent) -> i64 {
    let submitted = intent.submitted_quantity.max(0.0);
    let accounted = intent.filled_quantity.max(0.0)
        + intent.cancelled_quantity.max(0.0)
        + intent.rejected_quantity.max(0.0);
    (submitted - accounted).floor().max(0.0) as i64
}

fn average_fill_cost_per_contract(total_fill_cost: f64, fill_quantity: f64) -> Option<f64> {
    if total_fill_cost <= 0.0 || fill_quantity <= 0.0 {
        None
    } else {
        Some((total_fill_cost / fill_quantity).clamp(0.01, 0.99))
    }
}

fn pending_intent_priority(intent: &storage::PendingExecutionIntent) -> f64 {
    stop_condition_value(&intent.stop_conditions_json, "execution_score_bps").unwrap_or_default()
        + intent
            .predicted_fill_probability
            .or_else(|| stop_condition_value(&intent.stop_conditions_json, "fill_probability"))
            .unwrap_or_default()
            * 25.0
}

fn execution_score_threshold(intent: &storage::PendingExecutionIntent) -> f64 {
    stop_condition_value(
        &intent.stop_conditions_json,
        "execution_score_threshold_bps",
    )
    .unwrap_or(EXECUTION_SCORE_THRESHOLD_BPS)
}

fn stop_condition_value(stop_conditions: &[String], key: &str) -> Option<f64> {
    stop_conditions.iter().find_map(|condition| {
        let (prefix, raw_value) = condition.split_once(':')?;
        if prefix == key {
            raw_value.parse::<f64>().ok()
        } else {
            None
        }
    })
}

fn maybe_warn_execution_score_divergence(
    intent: &storage::PendingExecutionIntent,
    recomputed_score_bps: f64,
) {
    let decision_score_bps =
        stop_condition_value(&intent.stop_conditions_json, "execution_score_bps");
    if let Some(decision_score_bps) = decision_score_bps {
        let divergence_bps = (decision_score_bps - recomputed_score_bps).abs();
        if divergence_bps >= EXECUTION_SCORE_DIVERGENCE_WARN_BPS {
            warn!(
                intent_id = intent.intent_id,
                decision_id = intent.decision_id,
                market_id = intent.market_id,
                market_ticker = intent.market_ticker.as_deref().unwrap_or(""),
                decision_execution_score_bps = decision_score_bps,
                execution_recomputed_score_bps = recomputed_score_bps,
                divergence_bps,
                "execution_score_diverged_from_decision"
            );
        } else {
            info!(
                intent_id = intent.intent_id,
                decision_id = intent.decision_id,
                market_id = intent.market_id,
                market_ticker = intent.market_ticker.as_deref().unwrap_or(""),
                decision_execution_score_bps = decision_score_bps,
                execution_recomputed_score_bps = recomputed_score_bps,
                "execution_score_consistent_with_decision"
            );
        }
    }
}

async fn load_queue_snapshot(
    redis: &mut redis::aio::MultiplexedConnection,
    market_ticker: &str,
) -> Result<Option<QueueSnapshot>> {
    let raw: Option<String> = redis
        .get(format!("{REDIS_QUEUE_PREFIX}{market_ticker}"))
        .await
        .ok();
    Ok(raw
        .as_deref()
        .and_then(|value| serde_json::from_str::<QueueSnapshot>(value).ok()))
}

fn queue_contract_side(side: &str) -> Option<ContractSide> {
    match side {
        "buy_yes" => Some(ContractSide::Yes),
        "buy_no" => Some(ContractSide::No),
        _ => None,
    }
}

fn queue_side_summary<'a>(
    queue_snapshot: Option<&'a QueueSnapshot>,
    side: &str,
) -> Option<&'a QueueSideSummary> {
    match (queue_snapshot, queue_contract_side(side)) {
        (Some(queue), Some(ContractSide::Yes)) => Some(&queue.yes),
        (Some(queue), Some(ContractSide::No)) => Some(&queue.no),
        _ => None,
    }
}

fn execution_queue_trade_consumption_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.trade_consumption_rate)
        .unwrap_or(snapshot.trade_consumption_rate.max(snapshot.trade_rate))
        .max(0.0)
}

fn execution_queue_cancel_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.cancel_rate)
        .unwrap_or(snapshot.cancel_rate.max(0.0))
        .max(0.0)
}

fn execution_queue_decay_rate(
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
    side: &str,
) -> f64 {
    queue_side_summary(queue_snapshot, side)
        .map(|queue| queue.queue_decay_rate)
        .unwrap_or(snapshot.queue_decay_rate.max(0.0))
        .max(0.0)
}

fn execution_regime(snapshot: &common::MarketFeatureSnapshotRecord) -> ExpiryRegime {
    snapshot.expiry_regime.unwrap_or_else(|| {
        ExpiryRegime::from_seconds_to_expiry(i64::from(snapshot.seconds_to_expiry))
    })
}

fn current_crypto_execution_score(
    intent: &storage::PendingExecutionIntent,
    snapshot: &common::MarketFeatureSnapshotRecord,
    queue_snapshot: Option<&QueueSnapshot>,
) -> Option<f64> {
    if intent.market_family != MarketFamily::Crypto {
        return None;
    }
    let raw_edge_bps =
        directional_raw_edge_bps(&intent.side, intent.model_prob, snapshot.market_prob);
    let queue_summary = queue_side_summary(queue_snapshot, &intent.side);
    let score = build_execution_score(ExecutionScoreInputs {
        raw_edge_bps,
        spread_bps: snapshot.spread_bps,
        aggressive_buy_ratio: snapshot.aggressive_buy_ratio,
        quote_churn: snapshot.quote_churn,
        size_ahead: queue_summary
            .map(|queue| queue.size_ahead_real)
            .unwrap_or(snapshot.size_ahead_real.max(snapshot.size_ahead.max(1.0)))
            .max(1.0),
        trade_consumption_rate: queue_summary
            .map(|queue| queue.trade_consumption_rate)
            .unwrap_or(snapshot.trade_consumption_rate.max(snapshot.trade_rate))
            .max(0.0),
        cancel_rate: queue_summary
            .map(|queue| queue.cancel_rate)
            .unwrap_or(snapshot.cancel_rate.max(0.0))
            .max(0.0),
        queue_decay_rate: queue_summary
            .map(|queue| queue.queue_decay_rate)
            .unwrap_or(snapshot.queue_decay_rate.max(0.0))
            .max(0.0),
        seconds_to_expiry: f64::from(snapshot.seconds_to_expiry.max(1)),
        regime: execution_regime(snapshot),
    });
    Some(compute_execution_score_bps(&score))
}

fn settlement_contract_price(side: &str, resolved_yes: bool) -> f64 {
    match (side == "buy_no", resolved_yes) {
        (true, true) => 0.0,
        (true, false) => 1.0,
        (false, true) => 1.0,
        (false, false) => 0.0,
    }
}

fn exit_status_label(strategy_family: StrategyFamily, should_timeout: bool) -> &'static str {
    match (strategy_family, should_timeout) {
        (StrategyFamily::PreSettlementScalp, true) => "timeout_exit",
        (StrategyFamily::PreSettlementScalp, false) => "pre_expiry_flatten",
        (StrategyFamily::DirectionalSettlement, true) => "timeout_exit",
        (StrategyFamily::DirectionalSettlement, false) => "pre_expiry_flatten",
        (StrategyFamily::Portfolio, true) => "timeout_exit",
        (StrategyFamily::Portfolio, false) => "pre_expiry_flatten",
    }
}

fn live_trade_has_active_exit_order(trade: &OpenTradeForExit) -> bool {
    let Some(status) = trade.exit_fill_status.as_deref() else {
        return false;
    };
    matches!(
        status.to_ascii_lowercase().as_str(),
        "submitted"
            | "acknowledged"
            | "resting"
            | "open"
            | "pending"
            | "partially_filled"
            | "cancel_pending"
            | "cancel_requested"
    ) && (trade.exit_exchange_order_id.is_some() || trade.exit_client_order_id.is_some())
}

fn time_in_force_is_fill_or_kill(time_in_force: &str) -> bool {
    matches!(
        time_in_force.trim().to_ascii_lowercase().as_str(),
        "fill_or_kill" | "fok"
    )
}

fn next_replacement_client_order_id(current: Option<&str>, fallback_prefix: &str) -> String {
    let base = current.unwrap_or(fallback_prefix);
    if let Some((prefix, raw_number)) = base.rsplit_once("-r")
        && let Ok(number) = raw_number.parse::<u32>()
    {
        return format!("{prefix}-r{}", number + 1);
    }
    format!("{base}-r1")
}

fn order_age_seconds(order: &KalshiOrder) -> Option<i64> {
    parse_order_time(&order.last_update_time)
        .or_else(|| parse_order_time(&order.created_time))
        .map(|ts| (chrono::Utc::now() - ts).num_seconds().max(0))
}

fn parse_order_time(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if raw.trim().is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&chrono::Utc))
}

fn contract_fill_price(order: &KalshiOrder, contract_side: &str) -> f64 {
    let fill_quantity = parse_fp(&order.fill_count_fp);
    let from_fill_cost = first_non_zero(&[
        &order.taker_fill_cost_dollars,
        &order.maker_fill_cost_dollars,
    ]);
    if let Some(avg_price) = average_fill_cost_per_contract(from_fill_cost, fill_quantity) {
        return avg_price;
    }
    if contract_side == "yes" {
        parse_fp(&order.yes_price_dollars).clamp(0.01, 0.99)
    } else {
        parse_fp(&order.no_price_dollars).clamp(0.01, 0.99)
    }
}

fn first_non_zero(values: &[&str]) -> f64 {
    values
        .iter()
        .map(|value| parse_fp(value))
        .find(|value| *value > 0.0)
        .unwrap_or_default()
}

fn classify_live_intent_state(order: &KalshiOrder) -> &'static str {
    let status = order.status.to_ascii_lowercase();
    let fill_quantity = parse_fp(&order.fill_count_fp);
    if fill_quantity >= 1.0 {
        if matches!(
            status.as_str(),
            "filled" | "executed" | "complete" | "closed"
        ) {
            "filled"
        } else {
            "partially_filled"
        }
    } else if matches!(
        status.as_str(),
        "resting" | "open" | "acknowledged" | "pending"
    ) {
        "acknowledged"
    } else if matches!(status.as_str(), "cancel_pending" | "cancel_requested") {
        "cancel_pending"
    } else if matches!(status.as_str(), "cancelled" | "canceled") {
        "cancelled"
    } else if matches!(status.as_str(), "rejected" | "failed" | "error") {
        "rejected"
    } else {
        "submitted"
    }
}

fn terminal_outcome_for_status(status: &str) -> Option<String> {
    match status {
        "filled" => Some("filled".to_string()),
        "partially_filled" => Some("partially_filled".to_string()),
        "cancelled" => Some("cancelled".to_string()),
        "rejected" => Some("rejected".to_string()),
        "expired" => Some("expired".to_string()),
        "superseded" => Some("superseded".to_string()),
        "orphaned" => Some("orphaned".to_string()),
        _ => None,
    }
}

fn order_is_resting(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "resting" | "open" | "acknowledged" | "pending"
    )
}

fn order_client_order_id(order: &KalshiOrder, fallback: Option<&str>) -> String {
    if order.client_order_id.trim().is_empty() {
        fallback.unwrap_or_default().to_string()
    } else {
        order.client_order_id.clone()
    }
}

fn order_market_ticker(order: &KalshiOrder) -> String {
    order.ticker.clone()
}

fn proportional_entry_fee(total_entry_fee: f64, exit_quantity: f64, total_quantity: f64) -> f64 {
    if total_entry_fee <= 0.0 || total_quantity <= 0.0 {
        0.0
    } else {
        (total_entry_fee * (exit_quantity / total_quantity)).max(0.0)
    }
}

fn should_force_reconcile_stale_paper_crypto_trade(
    trade: &OpenTradeForExit,
    trade_age_seconds: i64,
) -> bool {
    trade.mode == TradeMode::Paper
        && trade.market_family == MarketFamily::Crypto
        && trade_age_seconds >= paper_crypto_trade_age_limit_seconds(&trade.lane_key)
}

fn paper_crypto_trade_age_limit_seconds(lane_key: &str) -> i64 {
    let window_minutes = lane_key
        .split(':')
        .nth(2)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(15)
        .max(1);
    (window_minutes * 60 + 5 * 60).max(20 * 60)
}

fn parse_fp(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use common::{ExpiryRegime, PromotionState};

    fn sample_open_trade(exit_fill_status: Option<&str>) -> OpenTradeForExit {
        OpenTradeForExit {
            trade_id: 1,
            market_id: 1,
            market_family: MarketFamily::Crypto,
            market_ticker: Some("KXBTC15M-TEST".to_string()),
            lane_key: "kalshi:btc:15:buy_yes:directional_settlement:settlement_anchor_v3"
                .to_string(),
            side: "buy_yes".to_string(),
            quantity: 2.0,
            entry_price: 0.45,
            entry_fee: 0.0,
            strategy_family: StrategyFamily::DirectionalSettlement,
            mode: TradeMode::Live,
            created_at: Utc::now(),
            timeout_seconds: 30,
            force_exit_buffer_seconds: 10,
            expires_at: None,
            exit_exchange_order_id: Some("order-1".to_string()),
            exit_client_order_id: Some("client-1".to_string()),
            exit_fill_status: exit_fill_status.map(ToOwned::to_owned),
        }
    }

    fn sample_live_intent() -> ActiveLiveExecutionIntent {
        ActiveLiveExecutionIntent {
            intent_id: 1,
            decision_id: 1,
            market_id: 1,
            market_family: MarketFamily::Crypto,
            lane_key: "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1:mid"
                .to_string(),
            strategy_family: StrategyFamily::DirectionalSettlement,
            side: "buy_yes".to_string(),
            market_prob: 0.50,
            recommended_size: 25.0,
            mode: TradeMode::Live,
            timeout_seconds: 120,
            force_exit_buffer_seconds: 45,
            created_at: Utc::now(),
            status: "acknowledged".to_string(),
            market_ticker: Some("KXBTC15M-TEST".to_string()),
            client_order_id: Some("v3-live-entry-1".to_string()),
            exchange_order_id: Some("order-1".to_string()),
            fill_status: Some("resting".to_string()),
            submitted_quantity: 10.0,
            accepted_quantity: 10.0,
            filled_quantity: 0.0,
            cancelled_quantity: 0.0,
            rejected_quantity: 0.0,
            avg_fill_price: None,
            terminal_outcome: None,
            last_transition_at: Utc::now(),
        }
    }

    fn sample_pending_intent() -> storage::PendingExecutionIntent {
        storage::PendingExecutionIntent {
            intent_id: 1,
            decision_id: 1,
            market_id: 1,
            market_ticker: Some("KXBTC15M-TEST".to_string()),
            market_family: MarketFamily::Crypto,
            lane_key: "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1:mid"
                .to_string(),
            strategy_family: StrategyFamily::DirectionalSettlement,
            side: "buy_yes".to_string(),
            market_prob: 0.50,
            model_prob: 0.56,
            confidence: 0.72,
            recommended_size: 25.0,
            mode: TradeMode::Live,
            entry_style: "maker_priority".to_string(),
            timeout_seconds: 120,
            force_exit_buffer_seconds: 45,
            predicted_fill_probability: Some(0.35),
            predicted_slippage_bps: Some(15.0),
            predicted_queue_ahead: Some(10.0),
            execution_forecast_version: Some("queue_fill_v1".to_string()),
            submitted_quantity: 10.0,
            accepted_quantity: 0.0,
            filled_quantity: 0.0,
            cancelled_quantity: 0.0,
            rejected_quantity: 0.0,
            avg_fill_price: None,
            first_fill_at: None,
            last_fill_at: None,
            terminal_outcome: None,
            promotion_state_at_creation: Some(PromotionState::LiveMicro),
            stop_conditions_json: vec![
                "execution_score_bps:42.5".to_string(),
                "execution_score_threshold_bps:10.0".to_string(),
            ],
            created_at: Utc::now(),
        }
    }

    fn sample_snapshot() -> common::MarketFeatureSnapshotRecord {
        common::MarketFeatureSnapshotRecord {
            market_id: 1,
            market_family: MarketFamily::Crypto,
            market_ticker: "KXBTC15M-TEST".to_string(),
            market_title: "BTC 15m".to_string(),
            feature_version: "v1".to_string(),
            exchange: "kalshi".to_string(),
            symbol: "btc".to_string(),
            window_minutes: 15,
            seconds_to_expiry: 240,
            time_to_expiry_bucket: "2_to_5m".to_string(),
            expiry_regime: Some(ExpiryRegime::Mid),
            market_prob: 0.50,
            best_bid: 0.46,
            best_ask: 0.48,
            last_price: 0.47,
            bid_size: 25.0,
            ask_size: 18.0,
            liquidity: 43.0,
            order_book_imbalance: 0.15,
            aggressive_buy_ratio: 0.25,
            spread_bps: 200.0,
            venue_quality_score: 0.72,
            reference_price: 100_000.0,
            reference_previous_price: 99_950.0,
            reference_price_change_bps: 5.0,
            reference_yes_prob: 0.55,
            reference_gap_bps: 75.0,
            threshold_distance_bps_proxy: 10.0,
            distance_to_strike_bps: 22.0,
            reference_velocity: 4.0,
            realized_vol_short: 8.0,
            time_decay_factor: 0.8,
            size_ahead: 10.0,
            trade_rate: 6.0,
            quote_churn: 0.1,
            size_ahead_real: 10.0,
            trade_consumption_rate: 6.0,
            cancel_rate: 3.0,
            queue_decay_rate: 1.0,
            averaging_window_progress: 0.5,
            settlement_regime: "active".to_string(),
            last_minute_avg_proxy: 0.5,
            market_data_age_seconds: 2,
            reference_age_seconds: 1,
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
            created_at: Utc::now(),
        }
    }

    fn sample_queue_snapshot(size_ahead: f64) -> QueueSnapshot {
        let summary = QueueSideSummary {
            best_price: Some(0.46),
            total_size: 20.0,
            size_ahead_real: size_ahead,
            trade_consumption_rate: 6.0,
            cancel_rate: 3.0,
            queue_decay_rate: 1.0,
            updated_at: Utc::now(),
        };
        QueueSnapshot {
            market_ticker: "KXBTC15M-TEST".to_string(),
            yes: summary.clone(),
            no: summary,
            updated_at: Utc::now(),
        }
    }

    fn sample_kalshi_order(
        fill_count: f64,
        total_fill_cost: f64,
        contract_side: &str,
    ) -> KalshiOrder {
        KalshiOrder {
            order_id: "order-1".to_string(),
            ticker: "KXBTC15M-TEST".to_string(),
            client_order_id: "client-1".to_string(),
            status: "filled".to_string(),
            yes_price_dollars: if contract_side == "yes" {
                "0.44".to_string()
            } else {
                "0.56".to_string()
            },
            no_price_dollars: if contract_side == "no" {
                "0.44".to_string()
            } else {
                "0.56".to_string()
            },
            fill_count_fp: fill_count.to_string(),
            taker_fill_cost_dollars: total_fill_cost.to_string(),
            maker_fill_cost_dollars: "0".to_string(),
            taker_fees_dollars: "0.02".to_string(),
            maker_fees_dollars: "0".to_string(),
            created_time: Utc::now().to_rfc3339(),
            last_update_time: Utc::now().to_rfc3339(),
        }
    }

    #[test]
    fn detects_active_live_exit_order_states() {
        assert!(live_trade_has_active_exit_order(&sample_open_trade(Some(
            "acknowledged"
        ))));
        assert!(live_trade_has_active_exit_order(&sample_open_trade(Some(
            "partially_filled"
        ))));
        assert!(!live_trade_has_active_exit_order(&sample_open_trade(Some(
            "cancelled"
        ))));
        assert!(!live_trade_has_active_exit_order(&sample_open_trade(None)));
    }

    #[test]
    fn replacement_client_order_id_increments_suffix() {
        assert_eq!(
            next_replacement_client_order_id(Some("v3-live-entry-10"), "fallback"),
            "v3-live-entry-10-r1"
        );
        assert_eq!(
            next_replacement_client_order_id(Some("v3-live-entry-10-r1"), "fallback"),
            "v3-live-entry-10-r2"
        );
    }

    #[test]
    fn stale_crypto_paper_trade_uses_lane_window_cap() {
        assert_eq!(
            paper_crypto_trade_age_limit_seconds(
                "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1"
            ),
            1200
        );
    }

    #[test]
    fn stale_crypto_paper_trade_detection_ignores_live_trades() {
        let mut trade = sample_open_trade(None);
        trade.mode = TradeMode::Paper;
        assert!(should_force_reconcile_stale_paper_crypto_trade(
            &trade, 1900
        ));
        trade.mode = TradeMode::Live;
        assert!(!should_force_reconcile_stale_paper_crypto_trade(
            &trade, 1900
        ));
    }

    #[test]
    fn settlement_contract_price_matches_yes_and_no_contracts() {
        assert_eq!(settlement_contract_price("buy_yes", true), 1.0);
        assert_eq!(settlement_contract_price("buy_yes", false), 0.0);
        assert_eq!(settlement_contract_price("buy_no", true), 0.0);
        assert_eq!(settlement_contract_price("buy_no", false), 1.0);
    }

    #[test]
    fn stop_condition_value_extracts_numeric_thresholds() {
        let stop_conditions = vec![
            "expiry_exit".to_string(),
            "execution_score_bps:42.5".to_string(),
            "fill_probability:0.75".to_string(),
        ];
        assert_eq!(
            stop_condition_value(&stop_conditions, "execution_score_bps"),
            Some(42.5)
        );
        assert_eq!(
            stop_condition_value(&stop_conditions, "fill_probability"),
            Some(0.75)
        );
    }

    #[test]
    fn approved_crypto_with_missing_queue_data_stays_tradable_in_execution() {
        let intent = sample_pending_intent();
        let mut snapshot = sample_snapshot();
        snapshot.market_prob = 0.50;
        snapshot.spread_bps = 0.0;
        snapshot.trade_consumption_rate = 0.0;
        snapshot.trade_rate = 0.0;
        snapshot.cancel_rate = 0.0;
        snapshot.queue_decay_rate = 0.0;
        let recomputed = current_crypto_execution_score(&intent, &snapshot, None).unwrap();
        let expected = compute_execution_score_bps(&build_execution_score(ExecutionScoreInputs {
            raw_edge_bps: directional_raw_edge_bps(
                &intent.side,
                intent.model_prob,
                snapshot.market_prob,
            ),
            spread_bps: snapshot.spread_bps,
            aggressive_buy_ratio: snapshot.aggressive_buy_ratio,
            quote_churn: snapshot.quote_churn,
            size_ahead: snapshot.size_ahead_real.max(snapshot.size_ahead.max(1.0)),
            trade_consumption_rate: 0.0,
            cancel_rate: 0.0,
            queue_decay_rate: 0.0,
            seconds_to_expiry: f64::from(snapshot.seconds_to_expiry),
            regime: ExpiryRegime::Mid,
        }));
        assert!((recomputed - expected).abs() < 1e-9);
        assert!(recomputed > EXECUTION_SCORE_THRESHOLD_BPS);
    }

    #[test]
    fn pending_intent_is_not_superseded_by_newer_blocked_decision() {
        let replacement = storage::ReplacementDecisionCandidate {
            decision_id: 2,
            market_id: 1,
            market_ticker: Some("KXBTC15M-TEST".to_string()),
            approved: false,
            created_at: Utc::now(),
        };
        assert!(!should_supersede_pending_intent(
            1,
            Some("KXBTC15M-TEST"),
            &replacement
        ));
    }

    #[test]
    fn pending_intent_is_not_superseded_by_newer_approved_decision_for_different_market() {
        let replacement = storage::ReplacementDecisionCandidate {
            decision_id: 2,
            market_id: 2,
            market_ticker: Some("KXBTC15M-OTHER".to_string()),
            approved: true,
            created_at: Utc::now(),
        };
        assert!(!should_supersede_pending_intent(
            1,
            Some("KXBTC15M-TEST"),
            &replacement
        ));
    }

    #[test]
    fn pending_intent_can_be_superseded_by_newer_approved_decision_for_same_market() {
        let replacement = storage::ReplacementDecisionCandidate {
            decision_id: 2,
            market_id: 1,
            market_ticker: Some("KXBTC15M-TEST".to_string()),
            approved: true,
            created_at: Utc::now(),
        };
        assert!(should_supersede_pending_intent(
            1,
            Some("KXBTC15M-TEST"),
            &replacement
        ));
    }

    #[test]
    fn different_markets_in_same_lane_do_not_match_for_suppression() {
        assert!(!same_market_identity(
            1,
            Some("KXBTC15M-TEST"),
            2,
            Some("KXBTC15M-OTHER"),
        ));
    }

    #[test]
    fn same_market_duplicate_still_matches_for_suppression() {
        assert!(same_market_identity(
            1,
            Some("KXBTC15M-TEST"),
            1,
            Some("KXBTC15M-OTHER"),
        ));
        assert!(same_market_identity(
            99,
            Some("KXBTC15M-TEST"),
            100,
            Some("KXBTC15M-TEST"),
        ));
    }

    #[test]
    fn partial_fill_followed_by_replace_uses_residual_quantity_only() {
        let mut intent = sample_live_intent();
        intent.filled_quantity = 3.0;
        intent.cancelled_quantity = 1.0;
        assert_eq!(remaining_live_entry_quantity(&intent), 6);
    }

    #[test]
    fn replacement_never_exceeds_original_intended_quantity() {
        let mut intent = sample_live_intent();
        intent.filled_quantity = 6.0;
        intent.cancelled_quantity = 5.0;
        assert_eq!(remaining_live_entry_quantity(&intent), 0);
    }

    #[test]
    fn duplicate_recovery_residual_quantity_cannot_inflate_exposure() {
        let mut intent = sample_live_intent();
        intent.filled_quantity = 4.0;
        let residual = remaining_live_entry_quantity(&intent);
        assert!(residual as f64 + intent.filled_quantity <= intent.submitted_quantity);
    }

    #[test]
    fn multi_contract_fill_price_is_averaged_for_one_contract() {
        let order = sample_kalshi_order(1.0, 0.37, "yes");
        assert!((contract_fill_price(&order, "yes") - 0.37).abs() < 1e-9);
    }

    #[test]
    fn multi_contract_fill_price_is_averaged_for_two_contracts() {
        let order = sample_kalshi_order(2.0, 0.74, "yes");
        assert!((contract_fill_price(&order, "yes") - 0.37).abs() < 1e-9);
    }

    #[test]
    fn multi_contract_fill_price_is_averaged_for_five_contracts() {
        let order = sample_kalshi_order(5.0, 1.85, "yes");
        assert!((contract_fill_price(&order, "yes") - 0.37).abs() < 1e-9);
    }

    #[test]
    fn multi_contract_fill_price_is_averaged_for_ten_contracts() {
        let order = sample_kalshi_order(10.0, 3.7, "yes");
        assert!((contract_fill_price(&order, "yes") - 0.37).abs() < 1e-9);
    }

    #[test]
    fn modest_edge_and_acceptable_queue_join_the_bid() {
        let snapshot = sample_snapshot();
        let queue = sample_queue_snapshot(12.0);
        let route = route_live_entry(
            &snapshot,
            "buy_yes",
            Some(&queue),
            0.495,
            Some(0.55),
            Some(40.0),
            18.0,
            10.0,
            "good_till_cancelled",
            15,
            15,
        );
        assert_eq!(route.action, RouterAction::Join);
        assert_eq!(route.limit_price, 0.46);
    }

    #[test]
    fn favorable_spread_and_good_ev_improves_one_tick() {
        let mut snapshot = sample_snapshot();
        snapshot.best_bid = 0.42;
        snapshot.best_ask = 0.44;
        snapshot.spread_bps = 200.0;
        let queue = sample_queue_snapshot(8.0);
        let route = route_live_entry(
            &snapshot,
            "buy_yes",
            Some(&queue),
            0.56,
            Some(0.58),
            Some(20.0),
            25.0,
            10.0,
            "good_till_cancelled",
            15,
            15,
        );
        assert_eq!(route.action, RouterAction::ImproveOneTick);
        assert_eq!(route.limit_price, 0.43);
    }

    #[test]
    fn urgent_exit_near_expiry_crosses() {
        let mut snapshot = sample_snapshot();
        snapshot.seconds_to_expiry = 45;
        let route = route_live_exit(&snapshot, "buy_yes", "good_till_cancelled", 15, 45);
        assert_eq!(route.action, RouterAction::Cross);
        assert_eq!(route.time_in_force, "fill_or_kill");
        assert_eq!(route.limit_price, 0.46);
    }

    #[test]
    fn stale_snapshot_or_bad_spread_skips_entry() {
        let mut snapshot = sample_snapshot();
        snapshot.market_data_age_seconds = 30;
        let queue = sample_queue_snapshot(10.0);
        let route = route_live_entry(
            &snapshot,
            "buy_yes",
            Some(&queue),
            0.56,
            Some(0.6),
            Some(20.0),
            35.0,
            10.0,
            "good_till_cancelled",
            15,
            15,
        );
        assert_eq!(route.action, RouterAction::Skip);
    }

    #[test]
    fn router_does_not_cross_when_ev_guardrail_fails() {
        let mut snapshot = sample_snapshot();
        snapshot.best_bid = 0.58;
        snapshot.best_ask = 0.60;
        snapshot.spread_bps = 200.0;
        let queue = sample_queue_snapshot(5.0);
        let route = route_live_entry(
            &snapshot,
            "buy_yes",
            Some(&queue),
            0.595,
            Some(0.85),
            Some(40.0),
            55.0,
            10.0,
            "good_till_cancelled",
            15,
            15,
        );
        assert_ne!(route.action, RouterAction::Cross);
    }
}
