use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, State},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use common::{
    AppConfig, HealthSnapshot, RuntimeAlarm, kalshi_api_key_id, kalshi_private_key,
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

#[derive(Serialize)]
struct AcceptanceGateStatus {
    gate: String,
    status: String,
    message: String,
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
        .ensure_bootstrap_state(config.initial_paper_bankroll, config.initial_live_bankroll)
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
    let dashboard = state.storage.dashboard_snapshot().await.ok();

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

async fn dashboard(State(state): State<AppState>) -> Json<serde_json::Value> {
    let _ = state
        .storage
        .upsert_worker_success("api", &json!({"route": "dashboard"}))
        .await;
    match state.storage.dashboard_snapshot().await {
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
    let live_exchange_sync_age_seconds = live_exchange_sync
        .as_ref()
        .map(|sync| (Utc::now() - sync.synced_at).num_seconds().max(0));
    let operator_control = state.storage.operator_control_state().await.ok().flatten();
    let effective_live_order_placement_enabled = state
        .storage
        .effective_live_order_placement_enabled(state.config.live_order_placement_enabled)
        .await
        .unwrap_or(false);
    let dashboard = state.storage.dashboard_snapshot().await.ok();
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
            let mut exchange_cancelled = 0usize;
            if let Some(client) = state.live_client.as_ref() {
                let orders = state.storage.list_resting_live_orders(100).await;
                if let Ok(orders) = orders {
                    for order in orders {
                        if cancel_live_order(client, &order.order_id)
                            .await
                            .unwrap_or(false)
                        {
                            exchange_cancelled += 1;
                        }
                    }
                }
            }
            let local_cancelled = state
                .storage
                .cancel_live_execution_intents("operator_cancelled")
                .await;
            let live_sync = perform_live_sync(&state).await.ok();
            match local_cancelled {
                Ok(local_cancelled) => Ok(json!({
                    "ok": true,
                    "action": action,
                    "result": {
                        "exchange_cancelled": exchange_cancelled,
                        "local_cancelled": local_cancelled,
                        "live_sync": live_sync
                    }
                })),
                Err(error) => Err(error),
            }
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
        .upsert_worker_success(
            "api",
            &json!({"route": "operator_action", "action": action}),
        )
        .await;
    Json(payload)
}

fn workers_healthy(workers: &[WorkerStatusCard], config: &AppConfig) -> bool {
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
        ("training", 180),
        ("notifier", 60),
    ];

    required.iter().all(|(service, ttl_seconds)| {
        let Some(worker) = workers.iter().find(|worker| worker.service == *service) else {
            return false;
        };
        if worker.status == "failed" {
            return false;
        }
        (Utc::now() - worker.updated_at).num_seconds() <= *ttl_seconds
    })
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
    let gate_a_passed = workers_healthy(workers, config)
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
                    "Paper stability still needs proof: healthy_workers={}, alarms={}, pending_intents={}, orphaned_intents={}.",
                    workers_healthy(workers, config),
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
