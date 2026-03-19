use anyhow::Result;
use common::{
    AppConfig, StrategyFamily, TradeMode, kalshi_api_key_id, kalshi_private_key,
    kalshi_sign_rest_request, kalshi_timestamp_ms,
};
use reqwest::{Client, StatusCode};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use serde_json::json;
use storage::{ActiveLiveExecutionIntent, ExecutionIntentStateUpdate, OpenTradeForExit};
use tracing::{info, warn};

#[derive(Clone)]
struct LiveKalshiClient {
    http: Client,
    api_base: String,
    api_key_id: String,
    private_key: RsaPrivateKey,
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
        match execute_once(
            &storage,
            live_client.as_ref(),
            config.live_order_placement_enabled,
            &config.live_entry_time_in_force,
            &config.live_exit_time_in_force,
            config.live_order_replace_enabled,
            config.live_order_replace_after_seconds,
            config.live_order_stale_after_seconds,
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
    live_client: Option<&LiveKalshiClient>,
    live_order_placement_enabled_config: bool,
    live_entry_time_in_force: &str,
    live_exit_time_in_force: &str,
    live_order_replace_enabled: bool,
    live_order_replace_after_seconds: i64,
    live_order_stale_after_seconds: i64,
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
        live_client,
        live_entry_time_in_force,
        live_order_replace_enabled,
        live_order_replace_after_seconds,
        live_order_stale_after_seconds,
    )
    .await?;
    let (live_exit_synced, live_exit_replaced) = reconcile_active_live_exit_orders(
        storage,
        live_client,
        live_exit_time_in_force,
        live_order_replace_enabled,
        live_order_replace_after_seconds,
        live_order_stale_after_seconds,
    )
    .await?;
    let live_order_placement_enabled = storage
        .effective_live_order_placement_enabled(live_order_placement_enabled_config)
        .await?;

    let mut paper_remaining = storage
        .latest_portfolio_bankroll(TradeMode::Paper)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(0.0);
    let mut live_remaining = storage
        .latest_portfolio_bankroll(TradeMode::Live)
        .await?
        .map(|card| card.deployable_balance.max(0.0))
        .unwrap_or(0.0);

    let intents = storage.list_pending_execution_intents(12).await?;
    let mut opened = 0usize;
    let mut rejected = 0usize;
    for intent in intents {
        if storage.has_open_trade_for_lane(&intent.lane_key).await? {
            storage
                .mark_execution_intent_status(
                    intent.intent_id,
                    "rejected",
                    Some("existing_open_trade_for_lane"),
                )
                .await?;
            rejected += 1;
            continue;
        }

        let theoretical_entry_price = contract_price_for_side(&intent.side, intent.market_prob);
        let position_notional = intent.recommended_size.max(0.0);
        let remaining_budget = match intent.mode {
            TradeMode::Paper => paper_remaining,
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
                let quantity = (position_notional / theoretical_entry_price.max(0.05)).max(1.0);
                storage
                    .open_trade_from_intent(&intent, quantity, theoretical_entry_price)
                    .await?;
                paper_remaining = (paper_remaining - (quantity * theoretical_entry_price)).max(0.0);
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
                let quantity = (position_notional / theoretical_entry_price.max(0.05)).floor();
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
                            details_json: json!({
                                "market_ticker": snapshot.market_ticker.clone(),
                                "quantity": quantity,
                                "entry_price": theoretical_entry_price,
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
                    theoretical_entry_price,
                    &client_order_id,
                    live_entry_time_in_force,
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
                                    details_json: json!({
                                        "phase": "entry_no_fill",
                                    }),
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
        if let Some(snapshot) = storage
            .latest_feature_snapshot_for_market(trade.market_id)
            .await?
        {
            let trade_age_seconds = (chrono::Utc::now() - trade.created_at).num_seconds();
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
        if storage.has_open_trade_for_lane(&intent.lane_key).await? {
            status = Some("superseded");
            reason = Some("open_trade_exists");
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
            if let Some(last_created_at) = storage
                .latest_decision_at_for_lane(&intent.lane_key)
                .await?
            {
                if last_created_at > intent.created_at {
                    status = Some("superseded");
                    reason = Some("newer_decision_exists");
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
    live_client: Option<&LiveKalshiClient>,
    entry_time_in_force: &str,
    replace_enabled: bool,
    replace_after_seconds: i64,
    stale_after_seconds: i64,
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
                live_client,
                &intent,
                &order,
                entry_time_in_force,
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
        if storage.has_open_trade_for_lane(&intent.lane_key).await? {
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
    live_client: &LiveKalshiClient,
    intent: &ActiveLiveExecutionIntent,
    order: &KalshiOrder,
    time_in_force: &str,
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
    let entry_price = contract_price_for_side(&intent.side, snapshot.market_prob);
    let quantity = (intent.recommended_size.max(0.0) / entry_price.max(0.05)).floor() as i64;
    if quantity < 1 {
        storage
            .transition_execution_intent(
                intent.intent_id,
                "rejected",
                Some("size_too_small_for_live_contract"),
                &ExecutionIntentStateUpdate {
                    market_ticker: Some(snapshot.market_ticker.clone()),
                    side: Some(intent.side.clone()),
                    client_order_id: Some(client_order_id),
                    exchange_order_id: Some(order.order_id.clone()),
                    fill_status: Some("cancelled".to_string()),
                    details_json: json!({
                        "phase": "replace_live_entry_order",
                        "replaced": false,
                    }),
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
                    "entry_price": entry_price,
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
        entry_price,
        &client_order_id,
        time_in_force,
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
            if storage.has_open_trade_for_lane(&intent.lane_key).await? {
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
            } else if let Some(intent_record) = storage.execution_intent_by_id(intent.intent_id).await?
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
                        details_json: json!({
                            "phase": "entry_no_fill",
                        }),
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

        let age_seconds = order_age_seconds(&order).unwrap_or_else(|| {
            (chrono::Utc::now() - trade.created_at).num_seconds().max(0)
        });

        if replace_enabled
            && age_seconds >= replace_after_seconds
            && order_is_resting(&order.status)
            && !time_in_force_is_fill_or_kill(exit_time_in_force)
        {
            if replace_live_exit_order(storage, live_client, &trade, &order, exit_time_in_force)
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
    let exit_price = contract_price_for_side(&trade.side, snapshot.market_prob);
    let client_order_id = next_replacement_client_order_id(
        trade.exit_client_order_id.as_deref(),
        &format!("v3-live-exit-{}", trade.trade_id),
    );
    let placement = place_live_exit_with_client_order_id(
        live_client,
        trade.market_ticker.as_deref().unwrap_or(&snapshot.market_ticker),
        &trade.side,
        quantity,
        exit_price,
        &client_order_id,
        time_in_force,
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
) -> Result<()> {
    match trade.mode {
        TradeMode::Paper => {
            let realized_pnl = (theoretical_exit_price - trade.entry_price) * trade.quantity;
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
            match place_live_exit(
                live_client,
                &snapshot.market_ticker,
                &trade.side,
                quantity,
                theoretical_exit_price,
                trade.trade_id,
                exit_time_in_force,
            )
            .await?
            {
                placement => handle_live_exit_placement_result(storage, trade, placement, status).await?,
            }
        }
    }
    let _ = fallback_realized_pnl;
    Ok(())
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
    let from_fill_cost = first_non_zero(&[
        &order.taker_fill_cost_dollars,
        &order.maker_fill_cost_dollars,
    ]);
    if from_fill_cost > 0.0 {
        return from_fill_cost.clamp(0.01, 0.99);
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

fn parse_fp(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_open_trade(exit_fill_status: Option<&str>) -> OpenTradeForExit {
        OpenTradeForExit {
            trade_id: 1,
            market_id: 1,
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

    #[test]
    fn detects_active_live_exit_order_states() {
        assert!(live_trade_has_active_exit_order(&sample_open_trade(Some("acknowledged"))));
        assert!(live_trade_has_active_exit_order(&sample_open_trade(Some("partially_filled"))));
        assert!(!live_trade_has_active_exit_order(&sample_open_trade(Some("cancelled"))));
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
}
