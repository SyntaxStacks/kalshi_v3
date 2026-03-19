use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use common::{
    AppConfig, HealthSnapshot, MarketFamily, RuntimeAlarm, kalshi_api_key_id, kalshi_private_key,
    kalshi_sign_rest_request, kalshi_timestamp_ms,
};
use reqwest::Client;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use storage::{
    LiveFillSyncRecord, LiveOrderSyncRecord, LivePositionSyncRecord, Storage, WorkerStatusCard,
};
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Clone)]
struct AppState {
    config: Arc<AppConfig>,
    storage: Storage,
    live_client: Option<Arc<LiveKalshiClient>>,
}

#[derive(Clone)]
struct LiveKalshiClient {
    http: Client,
    api_base: String,
    api_key_id: String,
    private_key: RsaPrivateKey,
}

#[derive(Deserialize)]
struct OperatorActionRequest {
    action: String,
    note: Option<String>,
}

#[derive(Deserialize)]
struct DashboardQuery {
    family: Option<String>,
}

#[derive(Serialize)]
struct AcceptanceGateStatus {
    gate: String,
    status: String,
    message: String,
}

fn parse_market_family_query(value: &str) -> Option<MarketFamily> {
    match value {
        "crypto" => Some(MarketFamily::Crypto),
        "weather" => Some(MarketFamily::Weather),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Arc::new(AppConfig::load()?);
    let storage = Storage::connect(&config.database_url).await?;
    storage.migrate().await?;
    storage
        .ensure_bootstrap_state(
            config.initial_paper_bankroll,
            config.initial_live_bankroll,
            config.initial_paper_crypto_budget,
            config.initial_paper_weather_budget,
            config.initial_live_crypto_budget,
            config.initial_live_weather_budget,
        )
        .await?;
    storage
        .upsert_worker_started("api", &json!({"bind_addr": config.api_bind_addr}))
        .await?;
    let live_client = build_live_client(&config)?;

    let state = AppState {
        config,
        storage,
        live_client: live_client.map(Arc::new),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/weather", get(weather_index))
        .route("/assets/app.css", get(styles))
        .route("/assets/app.js", get(script))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/v1/dashboard", get(dashboard))
        .route("/v1/runtime", get(runtime))
        .route("/v1/lanes/{lane_key}", get(lane))
        .route("/v1/operator/action", post(operator_action))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    let addr: SocketAddr = state.config.api_bind_addr.parse()?;
    info!(%addr, "api_starting");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz(State(state): State<AppState>) -> Json<HealthSnapshot> {
    let status = match state.storage.healthcheck().await {
        Ok(_) => {
            let workers = state
                .storage
                .list_worker_statuses()
                .await
                .unwrap_or_default();
            if workers_healthy(&workers, &state.config) {
                "ok"
            } else {
                "degraded"
            }
        }
        Err(_) => "degraded",
    };
    let _ = state
        .storage
        .upsert_worker_success("api", &json!({"route": "healthz", "status": status}))
        .await;
    Json(HealthSnapshot {
        service: "api".to_string(),
        status: status.to_string(),
        timestamp: Utc::now(),
    })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let workers = state
        .storage
        .list_worker_statuses()
        .await
        .unwrap_or_default();
    let feature_age_seconds = state
        .storage
        .latest_feature_snapshot_timestamp()
        .await
        .ok()
        .flatten()
        .map(|ts| (Utc::now() - ts).num_seconds().max(0))
        .unwrap_or(-1);
    let live_bankroll_age_seconds = state
        .storage
        .latest_portfolio_bankroll(common::TradeMode::Live)
        .await
        .ok()
        .flatten()
        .map(|card| (Utc::now() - card.as_of).num_seconds().max(0))
        .unwrap_or(-1);
    let live_exchange_sync = state
        .storage
        .latest_live_exchange_sync_summary()
        .await
        .ok()
        .flatten();
    let live_exchange_sync_age_seconds = live_exchange_sync
        .as_ref()
        .map(|sync| (Utc::now() - sync.synced_at).num_seconds().max(0))
        .unwrap_or(-1);
    let operator_control = state.storage.operator_control_state().await.ok().flatten();
    let execution_quality = state.storage.execution_quality_summary(None).await.ok();
    let effective_live_order_placement_enabled = state
        .storage
        .effective_live_order_placement_enabled(state.config.live_order_placement_enabled)
        .await
        .unwrap_or(false);
    let intent_status_counts = state
        .storage
        .pending_execution_intent_status_counts()
        .await
        .unwrap_or_default();
    let dashboard = state.storage.dashboard_snapshot(None).await.ok();

    let mut out = String::new();
    append_help(
        &mut out,
        "kalshi_v3_feature_age_seconds",
        "Age in seconds of the latest persisted feature snapshot.",
        "gauge",
    );
    append_metric(
        &mut out,
        "kalshi_v3_feature_age_seconds",
        &[],
        feature_age_seconds as f64,
    );

    append_help(
        &mut out,
        "kalshi_v3_live_bankroll_age_seconds",
        "Age in seconds of the latest live bankroll snapshot.",
        "gauge",
    );
    append_metric(
        &mut out,
        "kalshi_v3_live_bankroll_age_seconds",
        &[],
        live_bankroll_age_seconds as f64,
    );

    append_help(
        &mut out,
        "kalshi_v3_live_exchange_sync_age_seconds",
        "Age in seconds of the latest live exchange sync.",
        "gauge",
    );
    append_metric(
        &mut out,
        "kalshi_v3_live_exchange_sync_age_seconds",
        &[],
        live_exchange_sync_age_seconds as f64,
    );

    append_help(
        &mut out,
        "kalshi_v3_live_order_placement_enabled",
        "Whether live order placement is currently enabled after config and operator overrides.",
        "gauge",
    );
    append_metric(
        &mut out,
        "kalshi_v3_live_order_placement_enabled",
        &[],
        if effective_live_order_placement_enabled {
            1.0
        } else {
            0.0
        },
    );

    append_help(
        &mut out,
        "kalshi_v3_operator_live_enabled",
        "Whether the operator override currently allows live order placement.",
        "gauge",
    );
    append_metric(
        &mut out,
        "kalshi_v3_operator_live_enabled",
        &[],
        if operator_control
            .as_ref()
            .map(|control| control.live_order_placement_enabled)
            .unwrap_or(true)
        {
            1.0
        } else {
            0.0
        },
    );

    if let Some(quality) = execution_quality {
        // These metrics are split into live quantity truth and replay diagnostics so operators do
        // not mistake paper/opened statuses for exchange-confirmed fills.
        append_help(
            &mut out,
            "kalshi_v3_replay_trade_weighted_edge_realization_ratio_diag",
            "Trade-weighted replay edge realization ratio across latest champion lanes. Diagnostic only until proven comparable to live.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_recent_live_predicted_fill_probability_mean",
            "Mean predicted fill probability across recent comparable live intents. Null forecasts are excluded.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_recent_live_filled_quantity_ratio",
            "Quantity-weighted live fill ratio across recent terminal live intents.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_recent_live_actual_fill_hit_rate",
            "Binary live fill hit rate across recent comparable live intents.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_recent_live_terminal_intent_count",
            "Recent terminal live intent count used for execution-truth summaries.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_recent_live_predicted_fill_sample_count",
            "Recent live intent count with non-null predicted fill forecasts.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_replay_trade_weighted_fill_rate_diag",
            "Trade-weighted replay fill rate across champion lanes. Diagnostic only.",
            "gauge",
        );
        append_help(
            &mut out,
            "kalshi_v3_replay_trade_weighted_slippage_bps_diag",
            "Trade-weighted replay slippage across champion lanes. Diagnostic only.",
            "gauge",
        );
        if let Some(value) = quality.replay_trade_weighted_edge_realization_ratio_diag {
            append_metric(
                &mut out,
                "kalshi_v3_replay_trade_weighted_edge_realization_ratio_diag",
                &[],
                value,
            );
            append_help(
                &mut out,
                "kalshi_v3_replay_edge_realization_ratio",
                "DEPRECATED alias for kalshi_v3_replay_trade_weighted_edge_realization_ratio_diag.",
                "gauge",
            );
            append_metric(
                &mut out,
                "kalshi_v3_replay_edge_realization_ratio",
                &[],
                value,
            );
        }
        if let Some(value) = quality.replay_trade_weighted_fill_rate_diag {
            append_metric(
                &mut out,
                "kalshi_v3_replay_trade_weighted_fill_rate_diag",
                &[],
                value,
            );
        }
        if let Some(value) = quality.replay_trade_weighted_slippage_bps_diag {
            append_metric(
                &mut out,
                "kalshi_v3_replay_trade_weighted_slippage_bps_diag",
                &[],
                value,
            );
        }
        if let Some(value) = quality.recent_live_predicted_fill_probability_mean {
            append_metric(
                &mut out,
                "kalshi_v3_recent_live_predicted_fill_probability_mean",
                &[],
                value,
            );
            append_help(
                &mut out,
                "kalshi_v3_recent_expected_fill_probability",
                "DEPRECATED alias for kalshi_v3_recent_live_predicted_fill_probability_mean.",
                "gauge",
            );
            append_metric(
                &mut out,
                "kalshi_v3_recent_expected_fill_probability",
                &[],
                value,
            );
        }
        if let Some(value) = quality.recent_live_filled_quantity_ratio {
            append_metric(
                &mut out,
                "kalshi_v3_recent_live_filled_quantity_ratio",
                &[],
                value,
            );
            append_help(
                &mut out,
                "kalshi_v3_recent_actual_fill_rate",
                "DEPRECATED alias for kalshi_v3_recent_live_filled_quantity_ratio.",
                "gauge",
            );
            append_metric(&mut out, "kalshi_v3_recent_actual_fill_rate", &[], value);
        }
        if let Some(value) = quality.recent_live_actual_fill_hit_rate {
            append_metric(
                &mut out,
                "kalshi_v3_recent_live_actual_fill_hit_rate",
                &[],
                value,
            );
        }
        append_metric(
            &mut out,
            "kalshi_v3_recent_live_terminal_intent_count",
            &[],
            quality.recent_live_terminal_intent_count as f64,
        );
        append_metric(
            &mut out,
            "kalshi_v3_recent_live_predicted_fill_sample_count",
            &[],
            quality.recent_live_predicted_fill_sample_count as f64,
        );
    }

    append_help(
        &mut out,
        "kalshi_v3_worker_up",
        "Worker health state where 1 means the worker is not failed and 0 means failed.",
        "gauge",
    );
    append_help(
        &mut out,
        "kalshi_v3_worker_age_seconds",
        "Seconds since the worker last updated its durable status row.",
        "gauge",
    );
    for worker in &workers {
        let worker_up = if worker.status == "failed" { 0.0 } else { 1.0 };
        append_metric(
            &mut out,
            "kalshi_v3_worker_up",
            &[("service", worker.service.as_str())],
            worker_up,
        );
        append_metric(
            &mut out,
            "kalshi_v3_worker_age_seconds",
            &[("service", worker.service.as_str())],
            (Utc::now() - worker.updated_at).num_seconds().max(0) as f64,
        );
    }

    append_help(
        &mut out,
        "kalshi_v3_execution_intent_status_total",
        "Count of execution intents by current status.",
        "gauge",
    );
    for (status, count) in intent_status_counts {
        append_metric(
            &mut out,
            "kalshi_v3_execution_intent_status_total",
            &[("status", status.as_str())],
            count as f64,
        );
    }

    if let Some(sync) = live_exchange_sync {
        append_help(
            &mut out,
            "kalshi_v3_live_exchange_sync_positions",
            "Count of synced live exchange positions.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_live_exchange_sync_positions",
            &[],
            sync.positions_count as f64,
        );
        append_help(
            &mut out,
            "kalshi_v3_live_exchange_sync_resting_orders",
            "Count of synced live resting orders.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_live_exchange_sync_resting_orders",
            &[],
            sync.resting_orders_count as f64,
        );
        append_help(
            &mut out,
            "kalshi_v3_live_exchange_sync_recent_fills",
            "Count of synced recent live fills.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_live_exchange_sync_recent_fills",
            &[],
            sync.recent_fills_count as f64,
        );
    }

    if let Some(dashboard) = dashboard {
        append_help(
            &mut out,
            "kalshi_v3_open_trades",
            "Count of currently open trades.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_open_trades",
            &[],
            dashboard.open_trades.len() as f64,
        );

        append_help(
            &mut out,
            "kalshi_v3_opportunities_total",
            "Count of latest surfaced opportunities.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_opportunities_total",
            &[],
            dashboard.opportunities.len() as f64,
        );

        append_help(
            &mut out,
            "kalshi_v3_readiness_lanes",
            "Count of lanes by promotion state.",
            "gauge",
        );
        append_metric(
            &mut out,
            "kalshi_v3_readiness_lanes",
            &[("state", "shadow")],
            dashboard.readiness.shadow_count as f64,
        );
        append_metric(
            &mut out,
            "kalshi_v3_readiness_lanes",
            &[("state", "paper_active")],
            dashboard.readiness.paper_active_count as f64,
        );
        append_metric(
            &mut out,
            "kalshi_v3_readiness_lanes",
            &[("state", "live_micro")],
            dashboard.readiness.live_micro_count as f64,
        );
        append_metric(
            &mut out,
            "kalshi_v3_readiness_lanes",
            &[("state", "live_scaled")],
            dashboard.readiness.live_scaled_count as f64,
        );
        append_metric(
            &mut out,
            "kalshi_v3_readiness_lanes",
            &[("state", "quarantined")],
            dashboard.readiness.quarantined_count as f64,
        );
    }

    let _ = state
        .storage
        .upsert_worker_success("api", &json!({"route": "metrics"}))
        .await;
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

async fn weather_index() -> Html<&'static str> {
    Html(include_str!("../assets/index.html"))
}

async fn styles() -> impl IntoResponse {
    (
        [("content-type", "text/css; charset=utf-8")],
        include_str!("../assets/app.css"),
    )
}

async fn script() -> impl IntoResponse {
    (
        [("content-type", "application/javascript; charset=utf-8")],
        include_str!("../assets/app.js"),
    )
}

async fn dashboard(
    Query(query): Query<DashboardQuery>,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let family = query.family.as_deref().and_then(parse_market_family_query);
    let _ = state
        .storage
        .upsert_worker_success(
            "api",
            &json!({"route": "dashboard", "family": query.family}),
        )
        .await;
    match state.storage.dashboard_snapshot(family).await {
        Ok(snapshot) => Json(json!(snapshot)),
        Err(error) => Json(json!({
            "bankrolls": [],
            "readiness": {
                "overall_status": "unavailable",
                "shadow_count": 0,
                "paper_active_count": 0,
                "live_micro_count": 0,
                "live_scaled_count": 0,
                "quarantined_count": 0,
                "lanes": []
            },
            "open_trades": [],
            "opportunities": [],
            "live_sync": null,
            "live_exceptions": {
                "operator_control": null,
                "positions": [],
                "orders": [],
                "recent_fills": [],
                "trade_exceptions": [],
                "live_intents": []
            },
            "error": error.to_string()
        })),
    }
}

async fn runtime(State(state): State<AppState>) -> Json<serde_json::Value> {
    let _ = state
        .storage
        .upsert_worker_success("api", &json!({"route": "runtime"}))
        .await;
    let workers = state
        .storage
        .list_worker_statuses()
        .await
        .unwrap_or_default();
    let feature_age_seconds = state
        .storage
        .latest_feature_snapshot_timestamp()
        .await
        .ok()
        .flatten()
        .map(|ts| (Utc::now() - ts).num_seconds().max(0));
    let live_bankroll_age_seconds = state
        .storage
        .latest_portfolio_bankroll(common::TradeMode::Live)
        .await
        .ok()
        .flatten()
        .map(|card| (Utc::now() - card.as_of).num_seconds().max(0));
    let intent_status_counts = state
        .storage
        .pending_execution_intent_status_counts()
        .await
        .unwrap_or_default();
    let live_exchange_sync = state
        .storage
        .latest_live_exchange_sync_summary()
        .await
        .ok()
        .flatten();
    let execution_quality = state.storage.execution_quality_summary(None).await.ok();
    let live_exchange_sync_age_seconds = live_exchange_sync
        .as_ref()
        .map(|sync| (Utc::now() - sync.synced_at).num_seconds().max(0));
    let operator_control = state.storage.operator_control_state().await.ok().flatten();
    let effective_live_order_placement_enabled = state
        .storage
        .effective_live_order_placement_enabled(state.config.live_order_placement_enabled)
        .await
        .unwrap_or(false);
    let dashboard = state.storage.dashboard_snapshot(None).await.ok();
    let reference_feed_age_seconds = workers
        .iter()
        .find(|worker| worker.service == "ingest")
        .map(|worker| (Utc::now() - worker.updated_at).num_seconds().max(0));
    let alarms = build_runtime_alarms(
        &state.config,
        &workers,
        feature_age_seconds,
        reference_feed_age_seconds,
        live_bankroll_age_seconds,
        live_exchange_sync_age_seconds,
        live_exchange_sync.as_ref(),
    );
    let acceptance_gates = build_acceptance_gates(
        &state.config,
        dashboard.as_ref(),
        &workers,
        &alarms,
        &intent_status_counts,
        live_bankroll_age_seconds,
        live_exchange_sync.as_ref(),
        effective_live_order_placement_enabled,
    );
    Json(json!({
        "service": "api",
        "exchange": state.config.exchange,
        "reference_price_mode": state.config.reference_price_mode,
        "reference_price_source": state.config.reference_price_source,
        "paper_trading_enabled": state.config.paper_trading_enabled,
        "live_trading_enabled": state.config.live_trading_enabled,
        "live_order_placement_enabled": effective_live_order_placement_enabled,
        "config_live_order_placement_enabled": state.config.live_order_placement_enabled,
        "operator_control": operator_control,
        "feature_age_seconds": feature_age_seconds,
        "market_feed_age_seconds": feature_age_seconds,
        "reference_feed_age_seconds": reference_feed_age_seconds,
        "live_bankroll_age_seconds": live_bankroll_age_seconds,
        "live_exchange_sync_age_seconds": live_exchange_sync_age_seconds,
        "live_exchange_sync": live_exchange_sync,
        "execution_quality": execution_quality,
        "intent_status_counts": intent_status_counts,
        "alarms": alarms,
        "acceptance_gates": acceptance_gates,
        "worker_statuses": workers,
    }))
}

async fn lane(
    Path(lane_key): Path<String>,
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let _ = state
        .storage
        .upsert_worker_success("api", &json!({"route": "lane", "lane_key": lane_key}))
        .await;
    match state.storage.lane_inspection_snapshot(&lane_key).await {
        Ok(snapshot) => Json(json!(snapshot)),
        Err(error) => Json(json!({
            "lane_key": lane_key,
            "error": error.to_string()
        })),
    }
}

async fn operator_action(
    State(state): State<AppState>,
    Json(request): Json<OperatorActionRequest>,
) -> Json<serde_json::Value> {
    let action = request.action.as_str();
    let note = request.note.as_deref();
    let result = match action {
        "disable_live" => {
            let control = state
                .storage
                .set_live_order_placement_enabled(
                    false,
                    "operator_api",
                    note.or(Some("manual disable")),
                )
                .await;
            control
                .map(|control| json!({"ok": true, "action": action, "operator_control": control}))
        }
        "enable_live" => {
            let control = state
                .storage
                .set_live_order_placement_enabled(
                    true,
                    "operator_api",
                    note.or(Some("manual enable")),
                )
                .await;
            control
                .map(|control| json!({"ok": true, "action": action, "operator_control": control}))
        }
        "retry_live_sync" => perform_live_sync(&state)
            .await
            .map(|payload| json!({"ok": true, "action": action, "result": payload})),
        "reconcile_now" => {
            let repaired = state
                .storage
                .repair_pending_execution_intents_from_trades()
                .await;
            let reconciled = state.storage.reconcile_live_trades_from_sync().await;
            match (repaired, reconciled) {
                (Ok(repaired), Ok((live_reconciled, live_attention))) => Ok(json!({
                    "ok": true,
                    "action": action,
                    "result": {
                        "repaired_intents": repaired,
                        "live_reconciled": live_reconciled,
                        "live_attention": live_attention
                    }
                })),
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        }
        "cancel_pending_live_orders" => {
            async {
                let mut exchange_cancelled = 0usize;
                let mut exchange_cancel_failed = Vec::new();
                if let Some(client) = state.live_client.as_ref() {
                    let orders = state.storage.list_resting_live_orders(100).await?;
                    for order in orders {
                        match cancel_live_order(client, &order.order_id).await {
                            Ok(true) => {
                                exchange_cancelled += 1;
                            }
                            Ok(false) => {
                                exchange_cancel_failed.push(order.order_id);
                            }
                            Err(error) => {
                                exchange_cancel_failed.push(format!("{}:{error}", order.order_id));
                            }
                        }
                    }
                }

                let local_cancelled = state
                    .storage
                    .cancel_live_execution_intents("operator_cancelled")
                    .await?;
                let live_sync = perform_live_sync(&state).await.ok();
                Ok(json!({
                    "ok": true,
                    "action": action,
                    "result": {
                        "exchange_cancelled": exchange_cancelled,
                        "exchange_cancel_failed": exchange_cancel_failed,
                        "local_cancelled": local_cancelled,
                        "live_sync": live_sync
                    }
                }))
            }
            .await
        }
        "flatten_live_positions" => {
            async {
                let mut exchange_cancelled = 0usize;
                let mut flatten_submitted = 0usize;
                let mut flatten_skipped = Vec::new();

                if let Some(client) = state.live_client.as_ref() {
                    let orders = state.storage.list_resting_live_orders(100).await?;
                    for order in orders {
                        if cancel_live_order(client, &order.order_id)
                            .await
                            .unwrap_or(false)
                        {
                            exchange_cancelled += 1;
                        }
                    }

                    let positions = state.storage.list_live_positions(100).await?;
                    let local_live_trades = state
                        .storage
                        .list_open_live_trades_for_reconciliation()
                        .await?;
                    for position in positions {
                        if position.position_count.abs() < 0.999 {
                            continue;
                        }
                        let inferred_side = local_live_trades
                            .iter()
                            .find(|trade| trade.market_ticker == position.market_ticker)
                            .map(|trade| trade.side.clone());
                        let Some(side) = inferred_side else {
                            flatten_skipped.push(json!({
                                "market_ticker": position.market_ticker,
                                "reason": "unknown_position_side",
                            }));
                            continue;
                        };
                        let Some(snapshot) = state
                            .storage
                            .latest_feature_snapshot_for_ticker(&position.market_ticker)
                            .await?
                        else {
                            flatten_skipped.push(json!({
                                "market_ticker": position.market_ticker,
                                "reason": "missing_feature_snapshot",
                            }));
                            continue;
                        };
                        let exit_price = contract_price_for_side(&side, snapshot.market_prob);
                        let client_order_id = format!(
                            "v3-manual-flatten-{}-{}",
                            position.market_ticker,
                            Utc::now().timestamp_millis()
                        );
                        if place_live_exit_with_client_order_id(
                            client,
                            &position.market_ticker,
                            &side,
                            position.position_count.round() as i64,
                            exit_price,
                            &client_order_id,
                        )
                        .await
                        .is_ok()
                        {
                            flatten_submitted += 1;
                        } else {
                            flatten_skipped.push(json!({
                                "market_ticker": position.market_ticker,
                                "reason": "exit_submission_failed",
                            }));
                        }
                    }
                }

                let live_sync = perform_live_sync(&state).await.ok();
                Ok(json!({
                    "ok": true,
                    "action": action,
                    "result": {
                        "exchange_cancelled": exchange_cancelled,
                        "flatten_submitted": flatten_submitted,
                        "flatten_skipped": flatten_skipped,
                        "live_sync": live_sync
                    }
                }))
            }
            .await
        }
        _ => Ok(json!({
            "ok": false,
            "action": action,
            "error": "unsupported_operator_action"
        })),
    };

    let payload = match result {
        Ok(payload) => payload,
        Err(error) => json!({
            "ok": false,
            "action": action,
            "error": error.to_string()
        }),
    };

    let _ = state
        .storage
        .record_operator_action_event(action, "operator_api", note, &payload)
        .await;

    let _ = state
        .storage
        .upsert_worker_success(
            "api",
            &json!({"route": "operator_action", "action": action}),
        )
        .await;
    Json(payload)
}

fn worker_health_issues(workers: &[WorkerStatusCard], config: &AppConfig) -> Vec<String> {
    let required = [
        ("ingest", (config.market_poll_seconds as i64 * 3).max(30)),
        ("feature", (config.feature_poll_seconds as i64 * 3).max(30)),
        (
            "decision",
            (config.decision_poll_seconds as i64 * 3).max(30),
        ),
        (
            "execution",
            (config.execution_poll_seconds as i64 * 3).max(30),
        ),
        ("notifier", 60),
    ];

    required
        .iter()
        .filter_map(|(service, ttl_seconds)| {
            let Some(worker) = workers.iter().find(|worker| worker.service == *service) else {
                return Some(format!("{service}:missing"));
            };
            if worker.status == "failed" {
                return Some(format!("{service}:failed"));
            }
            let age_seconds = (Utc::now() - worker.updated_at).num_seconds();
            if age_seconds > *ttl_seconds {
                return Some(format!("{service}:stale:{age_seconds}s"));
            }
            None
        })
        .collect()
}

fn workers_healthy(workers: &[WorkerStatusCard], config: &AppConfig) -> bool {
    worker_health_issues(workers, config).is_empty()
}

fn build_runtime_alarms(
    config: &AppConfig,
    workers: &[WorkerStatusCard],
    market_feed_age_seconds: Option<i64>,
    reference_feed_age_seconds: Option<i64>,
    live_bankroll_age_seconds: Option<i64>,
    live_exchange_sync_age_seconds: Option<i64>,
    live_exchange_sync: Option<&common::LiveExchangeSyncSummary>,
) -> Vec<RuntimeAlarm> {
    let mut alarms = Vec::new();

    if let Some(age) = market_feed_age_seconds {
        if age > config.market_stale_after_seconds {
            alarms.push(RuntimeAlarm {
                code: "market_feed_stale".to_string(),
                severity: "critical".to_string(),
                message: format!("Market feature flow is {}s old.", age),
            });
        }
    } else {
        alarms.push(RuntimeAlarm {
            code: "market_feed_unknown".to_string(),
            severity: "warning".to_string(),
            message: "Market feature freshness is unknown.".to_string(),
        });
    }

    if let Some(age) = reference_feed_age_seconds {
        if age > config.reference_stale_after_seconds {
            alarms.push(RuntimeAlarm {
                code: "reference_feed_stale".to_string(),
                severity: "critical".to_string(),
                message: format!("Reference feed is {}s old.", age),
            });
        }
    } else {
        alarms.push(RuntimeAlarm {
            code: "reference_feed_unknown".to_string(),
            severity: "warning".to_string(),
            message: "Reference feed freshness is unknown.".to_string(),
        });
    }

    if let Some(age) = live_bankroll_age_seconds {
        if age > config.live_bankroll_stale_after_seconds {
            alarms.push(RuntimeAlarm {
                code: "live_bankroll_stale".to_string(),
                severity: "warning".to_string(),
                message: format!("Live bankroll is {}s old.", age),
            });
        }
    }

    if let Some(age) = live_exchange_sync_age_seconds {
        if age > config.live_sync_stale_after_seconds {
            alarms.push(RuntimeAlarm {
                code: "live_exchange_sync_stale".to_string(),
                severity: "critical".to_string(),
                message: format!("Live exchange sync is {}s old.", age),
            });
        }
    }

    if let Some(sync) = live_exchange_sync {
        for issue in &sync.issues {
            alarms.push(RuntimeAlarm {
                code: "live_exchange_issue".to_string(),
                severity: "warning".to_string(),
                message: issue.clone(),
            });
        }
    }

    for worker in workers {
        if worker.status == "failed" {
            alarms.push(RuntimeAlarm {
                code: format!("worker_failed_{}", worker.service),
                severity: "critical".to_string(),
                message: format!(
                    "{} failed{}",
                    worker.service,
                    worker
                        .last_error
                        .as_deref()
                        .map(|error| format!(": {error}"))
                        .unwrap_or_default()
                ),
            });
        }
    }

    alarms
}

fn build_acceptance_gates(
    config: &AppConfig,
    dashboard: Option<&common::DashboardSnapshot>,
    workers: &[WorkerStatusCard],
    alarms: &[RuntimeAlarm],
    intent_status_counts: &[(String, i64)],
    live_bankroll_age_seconds: Option<i64>,
    live_exchange_sync: Option<&common::LiveExchangeSyncSummary>,
    effective_live_order_placement_enabled: bool,
) -> Vec<AcceptanceGateStatus> {
    let orphaned_intents = status_count(intent_status_counts, "orphaned");
    let pending_intents = status_count(intent_status_counts, "pending");
    let worker_issues = worker_health_issues(workers, config);
    let gate_a_passed = worker_issues.is_empty()
        && alarms.is_empty()
        && orphaned_intents == 0
        && pending_intents == 0;

    let gate_b_passed = dashboard
        .map(|snapshot| {
            !snapshot.readiness.lanes.is_empty()
                && snapshot.readiness.lanes.iter().all(|lane| {
                    lane.current_champion_model.is_some()
                        && lane.recent_replay_expectancy >= 0.0
                        && lane.recent_brier <= config.paper_promotion_max_brier
                })
        })
        .unwrap_or(false);

    let gate_c_passed = live_exchange_sync
        .map(|sync| sync.status == "ok" && sync.issues.is_empty())
        .unwrap_or(false)
        && live_bankroll_age_seconds
            .map(|age| age <= config.live_bankroll_stale_after_seconds)
            .unwrap_or(false);

    let (live_micro_count, live_scaled_count) = dashboard
        .map(|snapshot| {
            (
                snapshot.readiness.live_micro_count,
                snapshot.readiness.live_scaled_count,
            )
        })
        .unwrap_or((0, 0));

    let gate_d_passed = gate_c_passed
        && live_micro_count > 0
        && config.live_trading_enabled
        && effective_live_order_placement_enabled;

    let gate_e_passed = gate_d_passed && live_scaled_count > 0;

    vec![
        AcceptanceGateStatus {
            gate: "A".to_string(),
            status: gate_status(gate_a_passed),
            message: if gate_a_passed {
                "Paper loop is healthy, alarms are clear, and no orphaned/pending intents remain."
                    .to_string()
            } else {
                format!(
                    "Paper stability still needs proof: worker_issues=[{}], alarms={}, pending_intents={}, orphaned_intents={}.",
                    worker_issues.join(", "),
                    alarms.len(),
                    pending_intents,
                    orphaned_intents
                )
            },
        },
        AcceptanceGateStatus {
            gate: "B".to_string(),
            status: gate_status(gate_b_passed),
            message: if gate_b_passed {
                "Replay champions are populated and current lanes meet replay-quality thresholds."
                    .to_string()
            } else {
                "Replay stability is still pending: some lanes lack a champion, positive replay expectancy, or acceptable Brier.".to_string()
            },
        },
        AcceptanceGateStatus {
            gate: "C".to_string(),
            status: gate_status(gate_c_passed),
            message: if gate_c_passed {
                "Live bankroll and exchange-truth sync are fresh and currently consistent."
                    .to_string()
            } else {
                "Live readiness is still pending: bankroll freshness and exchange reconciliation need to stay green together.".to_string()
            },
        },
        AcceptanceGateStatus {
            gate: "D".to_string(),
            status: gate_status(gate_d_passed),
            message: if gate_d_passed {
                "At least one lane is ready for live-micro and live routing is armed.".to_string()
            } else {
                format!(
                    "Live-micro is not ready yet: live_micro_count={}, live_trading_enabled={}, live_order_placement_enabled={}.",
                    live_micro_count,
                    config.live_trading_enabled,
                    effective_live_order_placement_enabled
                )
            },
        },
        AcceptanceGateStatus {
            gate: "E".to_string(),
            status: gate_status(gate_e_passed),
            message: if gate_e_passed {
                "At least one lane has reached live-scaled with automated promotion path intact."
                    .to_string()
            } else {
                format!(
                    "Live-scaled is not ready yet: live_scaled_count={}.",
                    live_scaled_count
                )
            },
        },
    ]
}

fn gate_status(passed: bool) -> String {
    if passed {
        "passed".to_string()
    } else {
        "pending".to_string()
    }
}

fn status_count(counts: &[(String, i64)], target: &str) -> i64 {
    counts
        .iter()
        .find_map(|(status, count)| (status == target).then_some(*count))
        .unwrap_or_default()
}

fn build_live_client(config: &AppConfig) -> Result<Option<LiveKalshiClient>> {
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

async fn perform_live_sync(state: &AppState) -> Result<Value> {
    let Some(client) = state.live_client.as_ref() else {
        return Ok(json!({
            "live_balance_synced": false,
            "live_exchange_synced": false,
            "reason": "missing_kalshi_credentials"
        }));
    };

    let mut live_balance_synced = false;
    let mut live_exchange_synced = false;

    if let Some(balance) = fetch_live_balance(client).await? {
        state
            .storage
            .record_external_bankroll_snapshot(
                common::TradeMode::Live,
                balance.bankroll,
                balance.available_balance,
                balance.open_exposure,
            )
            .await?;
        live_balance_synced = true;
    }

    if sync_live_exchange_state(client, &state.storage).await? {
        live_exchange_synced = true;
    }

    Ok(json!({
        "live_balance_synced": live_balance_synced,
        "live_exchange_synced": live_exchange_synced
    }))
}

async fn fetch_live_balance(client: &LiveKalshiClient) -> Result<Option<LiveBalance>> {
    let Some(payload) = signed_get_json(client, "/portfolio/balance").await? else {
        return Ok(None);
    };

    let available_balance = cents_to_dollars(payload.get("balance"));
    let portfolio_value = cents_to_dollars(payload.get("portfolio_value"));
    Ok(Some(LiveBalance {
        bankroll: (available_balance + portfolio_value).max(0.0),
        available_balance: available_balance.max(0.0),
        open_exposure: portfolio_value.max(0.0),
    }))
}

async fn sync_live_exchange_state(client: &LiveKalshiClient, storage: &Storage) -> Result<bool> {
    let synced_at = Utc::now();
    let Some(positions_payload) = signed_get_json(client, "/portfolio/positions?limit=200").await?
    else {
        return Ok(false);
    };
    let Some(orders_payload) =
        signed_get_json(client, "/portfolio/orders?status=resting&limit=200").await?
    else {
        return Ok(false);
    };
    let Some(fills_payload) = signed_get_json(client, "/portfolio/fills?limit=100").await? else {
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
            &json!({
                "positions_count": positions.len(),
                "resting_orders_count": resting_orders_count,
                "recent_fills_count": fills.len(),
                "status": status,
                "issues": issues
            }),
        )
        .await?;

    Ok(true)
}

async fn signed_get_json(client: &LiveKalshiClient, path: &str) -> Result<Option<Value>> {
    let response = signed_request(client, "GET", path, None).await?;
    Ok(Some(response.error_for_status()?.json().await?))
}

async fn cancel_live_order(client: &LiveKalshiClient, order_id: &str) -> Result<bool> {
    let path = format!("/portfolio/orders/{order_id}");
    let response = signed_request(client, "DELETE", &path, None).await?;
    if response.status().is_success() {
        return Ok(true);
    }
    Ok(false)
}

async fn place_live_exit_with_client_order_id(
    client: &LiveKalshiClient,
    market_ticker: &str,
    side: &str,
    quantity: i64,
    contract_price: f64,
    client_order_id: &str,
) -> Result<()> {
    if quantity <= 0 {
        return Ok(());
    }
    let contract_side = if side == "buy_no" { "no" } else { "yes" };
    let price_cents = (contract_price * 100.0).round().clamp(1.0, 99.0) as i64;
    let mut body = json!({
        "ticker": market_ticker,
        "action": "sell",
        "side": contract_side,
        "count": quantity,
        "type": "limit",
        "time_in_force": "fill_or_kill",
        "client_order_id": client_order_id,
        "self_trade_prevention_type": "taker_at_cross",
        "reduce_only": true,
    });
    if contract_side == "yes" {
        body["yes_price"] = json!(price_cents);
    } else {
        body["no_price"] = json!(price_cents);
    }
    signed_request(client, "POST", "/portfolio/orders", Some(&body))
        .await?
        .error_for_status()?;
    Ok(())
}

fn contract_price_for_side(side: &str, market_prob: f64) -> f64 {
    if side == "buy_no" {
        (1.0 - market_prob).clamp(0.01, 0.99)
    } else {
        market_prob.clamp(0.01, 0.99)
    }
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

fn append_help(out: &mut String, metric: &str, help: &str, metric_type: &str) {
    out.push_str("# HELP ");
    out.push_str(metric);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(metric);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

fn append_metric(out: &mut String, metric: &str, labels: &[(&str, &str)], value: f64) {
    out.push_str(metric);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, raw_value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            for ch in raw_value.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    _ => out.push(ch),
                }
            }
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
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

fn parse_time(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn to_f64(value: Option<&Value>) -> f64 {
    value
        .and_then(Value::as_f64)
        .or_else(|| {
            value
                .and_then(Value::as_str)
                .and_then(|raw| raw.parse::<f64>().ok())
        })
        .unwrap_or_default()
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

struct LiveBalance {
    bankroll: f64,
    available_balance: f64,
    open_exposure: f64,
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptanceGateStatus, build_acceptance_gates, worker_health_issues, workers_healthy,
    };
    use chrono::{Duration, Utc};
    use common::{
        AppConfig, DashboardSnapshot, ExecutionQualitySummary, LaneState, LiveExceptionSnapshot,
        LiveExchangeSyncSummary, MarketFamily, PromotionState, ReadinessSummary, RuntimeAlarm,
    };
    use serde_json::json;
    use storage::WorkerStatusCard;

    fn test_config() -> AppConfig {
        AppConfig {
            app_env: "test".to_string(),
            api_bind_addr: "127.0.0.1:8080".to_string(),
            database_url: "postgres://test".to_string(),
            redis_url: "redis://test".to_string(),
            nats_url: "nats://test".to_string(),
            exchange: "kalshi".to_string(),
            reference_price_mode: "proxy".to_string(),
            reference_price_source: "coinbase_proxy".to_string(),
            reference_averaging_window_seconds: 60,
            nws_api_base: "https://api.weather.gov".to_string(),
            open_meteo_geocode_api_base: "https://geocoding-api.open-meteo.com/v1/search"
                .to_string(),
            weather_series_category: "Climate and Weather".to_string(),
            weather_series_title_patterns: vec!["Highest temperature".to_string()],
            weather_series_poll_limit: 4,
            weather_reference_refresh_seconds: 900,
            kalshi_api_base: "https://example.com".to_string(),
            kalshi_ws_url: "wss://example.com".to_string(),
            kalshi_api_key_id: None,
            kalshi_api_key_id_file: None,
            kalshi_private_key_path: None,
            kalshi_private_key_b64: None,
            v2_reference_sqlite_path: None,
            coinbase_ws_url: "wss://example.com".to_string(),
            paper_trading_enabled: true,
            weather_paper_trading_enabled: true,
            live_trading_enabled: false,
            live_order_placement_enabled: false,
            discord_webhook_url: None,
            initial_paper_bankroll: 1000.0,
            initial_live_bankroll: 1000.0,
            initial_paper_crypto_budget: 850.0,
            initial_paper_weather_budget: 150.0,
            initial_live_crypto_budget: 1000.0,
            initial_live_weather_budget: 0.0,
            kalshi_series_tickers: vec!["KXBTC15M".to_string()],
            reference_symbols: vec!["BTC".to_string()],
            market_poll_seconds: 15,
            feature_poll_seconds: 15,
            decision_poll_seconds: 15,
            execution_poll_seconds: 15,
            historical_import_batch_size: 5000,
            min_edge: 0.04,
            min_confidence: 0.42,
            min_venue_quality: 0.2,
            max_position_pct: 0.03,
            live_max_position_pct: 0.02,
            live_max_lane_exposure_pct: 0.03,
            live_max_symbol_exposure_pct: 0.05,
            live_portfolio_drawdown_kill_pct: 0.15,
            paper_promotion_max_negative_replay: -0.1,
            paper_promotion_max_negative_pnl: -5.0,
            paper_promotion_max_brier: 0.25,
            live_micro_min_examples: 8,
            live_micro_min_pnl: 15.0,
            live_micro_max_brier: 0.03,
            live_scaled_min_examples: 3,
            live_scaled_min_pnl: 8.0,
            live_scaled_max_brier: 0.025,
            live_demote_max_negative_pnl: -8.0,
            live_entry_time_in_force: "good_till_cancelled".to_string(),
            live_exit_time_in_force: "good_till_cancelled".to_string(),
            live_order_replace_enabled: true,
            live_order_replace_after_seconds: 20,
            live_order_stale_after_seconds: 45,
            market_stale_after_seconds: 45,
            reference_stale_after_seconds: 45,
            live_bankroll_stale_after_seconds: 300,
            live_sync_stale_after_seconds: 45,
        }
    }

    fn worker(service: &str, status: &str, age_seconds: i64) -> WorkerStatusCard {
        WorkerStatusCard {
            service: service.to_string(),
            status: status.to_string(),
            last_started_at: Some(Utc::now() - Duration::seconds(age_seconds)),
            last_succeeded_at: Some(Utc::now() - Duration::seconds(age_seconds)),
            last_failed_at: None,
            last_error: None,
            details_json: json!({}),
            updated_at: Utc::now() - Duration::seconds(age_seconds),
        }
    }

    fn healthy_workers() -> Vec<WorkerStatusCard> {
        vec![
            worker("ingest", "ok", 5),
            worker("feature", "ok", 5),
            worker("decision", "ok", 5),
            worker("execution", "ok", 5),
            worker("training", "ok", 30),
            worker("notifier", "ok", 10),
        ]
    }

    fn dashboard_with_lane(state: PromotionState, replay: f64, brier: f64) -> DashboardSnapshot {
        DashboardSnapshot {
            market_family: Some(MarketFamily::Crypto),
            bankrolls: Vec::new(),
            readiness: ReadinessSummary {
                overall_status: "paper_active".to_string(),
                shadow_count: 0,
                paper_active_count: if state == PromotionState::PaperActive {
                    1
                } else {
                    0
                },
                live_micro_count: if state == PromotionState::LiveMicro {
                    1
                } else {
                    0
                },
                live_scaled_count: if state == PromotionState::LiveScaled {
                    1
                } else {
                    0
                },
                quarantined_count: if state == PromotionState::Quarantined {
                    1
                } else {
                    0
                },
                lanes: vec![LaneState {
                    lane_key: "kalshi:btc:15:buy_yes:directional_settlement:trained_linear_v1"
                        .to_string(),
                    market_family: MarketFamily::Crypto,
                    promotion_state: state,
                    promotion_reason: Some("paper_ready".to_string()),
                    recent_pnl: 1.0,
                    recent_brier: brier,
                    recent_execution_quality: 0.9,
                    recent_replay_expectancy: replay,
                    quarantine_reason: None,
                    current_champion_model: Some("trained_linear_v1".to_string()),
                }],
            },
            open_trades: Vec::new(),
            opportunities: Vec::new(),
            execution_quality: ExecutionQualitySummary {
                as_of: Utc::now(),
                replay_lane_count: 1,
                replay_trade_count: 4,
                replay_trade_weighted_edge_realization_ratio_diag: Some(1.0),
                replay_trade_weighted_fill_rate_diag: Some(0.6),
                replay_trade_weighted_slippage_bps_diag: Some(12.0),
                recent_live_terminal_intent_count: 10,
                recent_live_intents_with_fill_count: 6,
                recent_live_predicted_fill_sample_count: 8,
                recent_live_predicted_fill_probability_mean: Some(0.58),
                recent_live_filled_quantity_ratio: Some(0.60),
                recent_live_actual_fill_hit_rate: Some(0.60),
                live_sample_sufficient: false,
                replay_sample_sufficient: false,
            },
            family_execution_truth: Vec::new(),
            lane_execution_truth: Vec::new(),
            live_sync: None,
            live_exceptions: LiveExceptionSnapshot {
                operator_control: None,
                positions: Vec::new(),
                orders: Vec::new(),
                recent_fills: Vec::new(),
                trade_exceptions: Vec::new(),
                live_intents: Vec::new(),
            },
        }
    }

    fn gate_status(gates: &[AcceptanceGateStatus], gate: &str) -> String {
        gates
            .iter()
            .find(|item| item.gate == gate)
            .map(|item| item.status.clone())
            .unwrap_or_else(|| "missing".to_string())
    }

    #[test]
    fn workers_healthy_requires_recent_notifier() {
        let config = test_config();
        assert!(workers_healthy(&healthy_workers(), &config));
        assert!(worker_health_issues(&healthy_workers(), &config).is_empty());

        let mut stale = healthy_workers();
        stale[5].updated_at = Utc::now() - Duration::seconds(90);
        assert!(!workers_healthy(&stale, &config));
        assert_eq!(
            worker_health_issues(&stale, &config),
            vec!["notifier:stale:90s".to_string()]
        );
    }

    #[test]
    fn workers_healthy_does_not_require_training_for_gate_a() {
        let config = test_config();
        let mut workers = healthy_workers();
        let training = workers
            .iter_mut()
            .find(|worker| worker.service == "training")
            .expect("training worker present");
        training.updated_at = Utc::now() - Duration::seconds(600);
        training.status = "running".to_string();

        assert!(workers_healthy(&workers, &config));
        assert!(worker_health_issues(&workers, &config).is_empty());
    }

    #[test]
    fn acceptance_gate_a_fails_when_alarm_present() {
        let config = test_config();
        let gates = build_acceptance_gates(
            &config,
            Some(&dashboard_with_lane(PromotionState::PaperActive, 10.0, 0.2)),
            &healthy_workers(),
            &[RuntimeAlarm {
                code: "market_feed_stale".to_string(),
                severity: "critical".to_string(),
                message: "stale".to_string(),
            }],
            &[],
            Some(10),
            None,
            false,
        );
        assert_eq!(gate_status(&gates, "A"), "pending");
    }

    #[test]
    fn acceptance_gate_b_requires_non_negative_replay() {
        let config = test_config();
        let passing = build_acceptance_gates(
            &config,
            Some(&dashboard_with_lane(PromotionState::PaperActive, 1.0, 0.2)),
            &healthy_workers(),
            &[],
            &[],
            Some(10),
            None,
            false,
        );
        assert_eq!(gate_status(&passing, "B"), "passed");

        let failing = build_acceptance_gates(
            &config,
            Some(&dashboard_with_lane(PromotionState::PaperActive, -1.0, 0.2)),
            &healthy_workers(),
            &[],
            &[],
            Some(10),
            None,
            false,
        );
        assert_eq!(gate_status(&failing, "B"), "pending");
    }

    #[test]
    fn acceptance_gate_d_requires_live_enabled_and_promoted_lane() {
        let config = test_config();
        let gates = build_acceptance_gates(
            &config,
            Some(&dashboard_with_lane(PromotionState::LiveMicro, 1.0, 0.2)),
            &healthy_workers(),
            &[],
            &[],
            Some(10),
            Some(&LiveExchangeSyncSummary {
                synced_at: Utc::now(),
                positions_count: 0,
                resting_orders_count: 0,
                recent_fills_count: 0,
                local_open_live_trades_count: 0,
                status: "ok".to_string(),
                issues: Vec::new(),
            }),
            false,
        );
        assert_eq!(gate_status(&gates, "D"), "pending");
    }
}
