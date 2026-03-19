use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{AppConfig, CriticalAlertNotification, TradeMode};
use reqwest::Client;
use serde_json::{Value, json};
use storage::{ClosedTradeNotification, Storage};
use tracing::{info, warn};

const ALERT_NOTIFICATION_KIND: &str = "critical_alert";

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let storage = Storage::connect(&config.database_url).await?;
    storage.migrate().await?;
    let client = Client::new();
    storage
        .upsert_worker_started(
            "notifier",
            &json!({"webhook_configured": config.discord_webhook_url.as_ref().is_some_and(|value| !value.is_empty())}),
        )
        .await?;

    info!(
        webhook_configured = config
            .discord_webhook_url
            .as_ref()
            .is_some_and(|value| !value.is_empty()),
        "notifier_worker_started"
    );

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
    loop {
        interval.tick().await;
        storage
            .upsert_worker_started("notifier", &json!({"phase": "notify"}))
            .await?;
        match notify_once(&config, &storage, &client).await {
            Ok(sent_count) => {
                storage
                    .upsert_worker_success("notifier", &json!({"sent_count": sent_count}))
                    .await?;
            }
            Err(error) => {
                storage
                    .upsert_worker_failure("notifier", &error.to_string(), &json!({}))
                    .await?;
                warn!(error = %error, "notifier_pass_failed");
            }
        }
    }
}

async fn notify_once(config: &AppConfig, storage: &Storage, client: &Client) -> Result<usize> {
    let Some(webhook) = config
        .discord_webhook_url
        .as_ref()
        .filter(|value| !value.is_empty())
    else {
        return Ok(0);
    };

    let mut sent_count = 0usize;

    for trade in storage.list_unnotified_closed_trades(20).await? {
        let body = build_trade_webhook(&trade, &config.app_env);
        client
            .post(webhook)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        storage.mark_trade_notified(trade.trade_id).await?;
        sent_count += 1;
    }

    for alert in build_critical_alert_candidates(config, storage).await? {
        if storage
            .notification_delivery_exists(ALERT_NOTIFICATION_KIND, &alert.alert_key)
            .await?
        {
            continue;
        }
        let body = build_alert_webhook(&alert, &config.app_env);
        client
            .post(webhook)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        storage
            .record_notification_delivery(
                ALERT_NOTIFICATION_KIND,
                &alert.alert_key,
                &serde_json::to_value(&alert)?,
            )
            .await?;
        sent_count += 1;
    }

    Ok(sent_count)
}

async fn build_critical_alert_candidates(
    config: &AppConfig,
    storage: &Storage,
) -> Result<Vec<CriticalAlertNotification>> {
    let workers = storage.list_worker_statuses().await?;
    let feature_timestamp = storage.latest_feature_snapshot_timestamp().await?;
    let live_exchange_sync = storage.latest_live_exchange_sync_summary().await?;
    let operator_control = storage.operator_control_state().await?;
    let live_exceptions = storage.live_exception_snapshot().await?;
    let operator_actions = storage.list_recent_operator_action_events(20).await?;
    let now = Utc::now();

    let market_feed_age_seconds =
        feature_timestamp.map(|timestamp| (now - timestamp).num_seconds().max(0));
    let reference_worker = workers.iter().find(|worker| worker.service == "ingest");
    let reference_feed_age_seconds =
        reference_worker.map(|worker| (now - worker.updated_at).num_seconds().max(0));
    let live_exchange_sync_age_seconds = live_exchange_sync
        .as_ref()
        .map(|sync| (now - sync.synced_at).num_seconds().max(0));

    let mut alerts = Vec::new();

    if let Some(control) = operator_control
        .as_ref()
        .filter(|control| !control.live_order_placement_enabled)
    {
        alerts.push(CriticalAlertNotification {
            alert_key: format!("live_disabled:{}", control.updated_at.timestamp()),
            headline: "ALERT | LIVE DISABLED".to_string(),
            severity: "critical".to_string(),
            reason: control
                .note
                .clone()
                .unwrap_or_else(|| "Live order placement was disabled.".to_string()),
            subsystem: "operator".to_string(),
            affected_lane: None,
            current_state: Some(format!(
                "operator override off by {}",
                control.updated_by.as_deref().unwrap_or("unknown")
            )),
            next_action: Some(
                "Inspect runtime health and re-enable live explicitly when safe.".to_string(),
            ),
            occurred_at: control.updated_at,
        });
    }

    if market_feed_age_seconds
        .map(|age| age > config.market_stale_after_seconds)
        .unwrap_or(false)
    {
        alerts.push(CriticalAlertNotification {
            alert_key: format!(
                "market_feed_stale:{}",
                feature_timestamp
                    .map(|timestamp| timestamp.timestamp())
                    .unwrap_or_default()
            ),
            headline: "ALERT | MARKET FEED STALE".to_string(),
            severity: "critical".to_string(),
            reason: format!(
                "Market feature flow is {}s old.",
                market_feed_age_seconds.unwrap_or_default()
            ),
            subsystem: "market_data".to_string(),
            affected_lane: None,
            current_state: feature_timestamp.map(|timestamp| {
                format!(
                    "latest feature snapshot {}",
                    timestamp.format("%Y-%m-%d %H:%M:%S UTC")
                )
            }),
            next_action: Some(
                "Check ingest and feature workers before trusting new entries.".to_string(),
            ),
            occurred_at: feature_timestamp.unwrap_or(now),
        });
    }

    if reference_feed_age_seconds
        .map(|age| age > config.reference_stale_after_seconds)
        .unwrap_or(false)
    {
        let occurred_at = reference_worker
            .map(|worker| worker.updated_at)
            .unwrap_or(now);
        alerts.push(CriticalAlertNotification {
            alert_key: format!("reference_feed_stale:{}", occurred_at.timestamp()),
            headline: "ALERT | REFERENCE FEED STALE".to_string(),
            severity: "critical".to_string(),
            reason: format!(
                "Reference ingest is {}s old.",
                reference_feed_age_seconds.unwrap_or_default()
            ),
            subsystem: "reference_data".to_string(),
            affected_lane: None,
            current_state: reference_worker.map(|worker| {
                format!(
                    "ingest worker updated {}",
                    worker.updated_at.format("%Y-%m-%d %H:%M:%S UTC")
                )
            }),
            next_action: Some(
                "Validate Coinbase/reference connectivity before resuming live confidence."
                    .to_string(),
            ),
            occurred_at,
        });
    }

    if live_exchange_sync_age_seconds
        .map(|age| age > config.live_sync_stale_after_seconds)
        .unwrap_or(false)
    {
        let occurred_at = live_exchange_sync
            .as_ref()
            .map(|sync| sync.synced_at)
            .unwrap_or(now);
        alerts.push(CriticalAlertNotification {
            alert_key: format!("live_exchange_sync_stale:{}", occurred_at.timestamp()),
            headline: "ALERT | LIVE SYNC STALE".to_string(),
            severity: "critical".to_string(),
            reason: format!(
                "Live exchange sync is {}s old.",
                live_exchange_sync_age_seconds.unwrap_or_default()
            ),
            subsystem: "reconciliation".to_string(),
            affected_lane: None,
            current_state: live_exchange_sync.as_ref().map(|sync| {
                format!(
                    "positions={} orders={} fills={}",
                    sync.positions_count, sync.resting_orders_count, sync.recent_fills_count
                )
            }),
            next_action: Some(
                "Run live sync and confirm exchange truth before trading live.".to_string(),
            ),
            occurred_at,
        });
    }

    if let Some(sync) = live_exchange_sync.as_ref() {
        for issue in &sync.issues {
            alerts.push(CriticalAlertNotification {
                alert_key: format!(
                    "live_exchange_issue:{}:{}",
                    sync.synced_at.timestamp(),
                    issue
                ),
                headline: "ALERT | RECONCILIATION ISSUE".to_string(),
                severity: "warning".to_string(),
                reason: issue.clone(),
                subsystem: "reconciliation".to_string(),
                affected_lane: None,
                current_state: Some(format!(
                    "positions={} orders={} fills={}",
                    sync.positions_count, sync.resting_orders_count, sync.recent_fills_count
                )),
                next_action: Some(
                    "Inspect live exceptions and run reconcile_now if the issue persists."
                        .to_string(),
                ),
                occurred_at: sync.synced_at,
            });
        }
    }

    if !live_exceptions.trade_exceptions.is_empty() {
        let newest = live_exceptions
            .trade_exceptions
            .iter()
            .max_by_key(|trade| trade.trade_id)
            .expect("trade exceptions not empty");
        alerts.push(CriticalAlertNotification {
            alert_key: format!(
                "trade_exception:{}:{}",
                live_exceptions.trade_exceptions.len(),
                newest.trade_id
            ),
            headline: "ALERT | RECONCILIATION ISSUE".to_string(),
            severity: "critical".to_string(),
            reason: format!(
                "{} live trade(s) do not match exchange truth.",
                live_exceptions.trade_exceptions.len()
            ),
            subsystem: "execution".to_string(),
            affected_lane: Some(newest.lane_key.clone()),
            current_state: Some(format!(
                "{} on {}",
                newest.issue, newest.market_ticker
            )),
            next_action: Some("Run reconcile_now and inspect the affected live lane before enabling new live orders.".to_string()),
            occurred_at: newest.created_at,
        });
    }

    for worker in workers.iter().filter(|worker| worker.status == "failed") {
        alerts.push(CriticalAlertNotification {
            alert_key: format!(
                "worker_failed:{}:{}",
                worker.service,
                worker
                    .last_failed_at
                    .map(|timestamp| timestamp.timestamp())
                    .unwrap_or_default()
            ),
            headline: "ALERT | WORKER FAILED".to_string(),
            severity: "critical".to_string(),
            reason: worker
                .last_error
                .clone()
                .unwrap_or_else(|| format!("{} failed without an error payload.", worker.service)),
            subsystem: worker.service.clone(),
            affected_lane: None,
            current_state: Some(format!(
                "last success {}",
                worker
                    .last_succeeded_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "never".to_string())
            )),
            next_action: Some(
                "Inspect worker logs and restore health before trusting automation.".to_string(),
            ),
            occurred_at: worker.last_failed_at.unwrap_or(worker.updated_at),
        });
    }

    for action in operator_actions {
        let Some(alert) = operator_action_to_alert(&action) else {
            continue;
        };
        alerts.push(alert);
    }

    Ok(alerts)
}

fn operator_action_to_alert(
    action: &common::OperatorActionEvent,
) -> Option<CriticalAlertNotification> {
    match action.action.as_str() {
        "cancel_pending_live_orders" => Some(CriticalAlertNotification {
            alert_key: format!("operator_action:{}", action.id),
            headline: "ALERT | LIVE ORDERS CANCELLED".to_string(),
            severity: "warning".to_string(),
            reason: action
                .note
                .clone()
                .unwrap_or_else(|| "Operator cancelled pending live orders.".to_string()),
            subsystem: "operator".to_string(),
            affected_lane: None,
            current_state: Some(format_operator_action_state(action)),
            next_action: Some(
                "Confirm live orders are gone from exchange sync before resuming live placement."
                    .to_string(),
            ),
            occurred_at: action.created_at,
        }),
        "flatten_live_positions" => Some(CriticalAlertNotification {
            alert_key: format!("operator_action:{}", action.id),
            headline: "ALERT | FLATTEN LIVE POSITIONS".to_string(),
            severity: "critical".to_string(),
            reason: action
                .note
                .clone()
                .unwrap_or_else(|| "Operator triggered a live flatten action.".to_string()),
            subsystem: "operator".to_string(),
            affected_lane: None,
            current_state: Some(format_operator_action_state(action)),
            next_action: Some(
                "Verify exchange positions are flat and keep live disabled until reconciled."
                    .to_string(),
            ),
            occurred_at: action.created_at,
        }),
        _ => None,
    }
}

fn build_trade_webhook(trade: &ClosedTradeNotification, app_env: &str) -> Value {
    let title = format!(
        "{} | {} | {}",
        trade_outcome_label(trade.realized_pnl),
        mode_label_upper(trade.mode),
        format_signed_money_no_plus_for_zero(trade.realized_pnl)
    );
    let market = trade
        .market_title
        .clone()
        .or_else(|| trade.market_ticker.clone())
        .unwrap_or_else(|| compact_lane_summary(&trade.lane_key));
    let description = build_trade_summary(trade);

    let mut fields = Vec::new();
    push_field(&mut fields, "Market", Some(market), false);
    push_field(
        &mut fields,
        "Lane",
        Some(compact_lane_summary(&trade.lane_key)),
        false,
    );
    push_field(&mut fields, "Side", trade.side.clone(), true);
    push_field(
        &mut fields,
        "Entry -> Exit",
        Some(format!(
            "{} -> {}",
            format_contract_price(trade.entry_price),
            format_contract_price(trade.exit_price)
        )),
        true,
    );
    push_field(
        &mut fields,
        "Qty",
        Some(format_quantity(trade.quantity)),
        true,
    );
    push_field(
        &mut fields,
        "Strategy",
        Some(strategy_family_label(trade.strategy_family)),
        true,
    );

    json!({
        "embeds": [{
            "title": title,
            "description": description,
            "color": trade_color(trade.realized_pnl),
            "fields": fields,
            "footer": {
                "text": format!(
                    "Trade #{} • {} • {} • {}",
                    trade.trade_id,
                    mode_label_lower(trade.mode),
                    app_env,
                    humanize_status(&trade.status)
                )
            },
            "timestamp": trade.closed_at.to_rfc3339(),
        }]
    })
}

fn build_alert_webhook(alert: &CriticalAlertNotification, app_env: &str) -> Value {
    let mut fields = Vec::new();
    push_field(
        &mut fields,
        "Subsystem",
        Some(alert.subsystem.clone()),
        true,
    );
    push_field(&mut fields, "State", alert.current_state.clone(), false);
    push_field(
        &mut fields,
        "Lane",
        alert
            .affected_lane
            .clone()
            .map(|lane| compact_lane_summary(&lane)),
        false,
    );
    push_field(&mut fields, "Next Action", alert.next_action.clone(), false);

    json!({
        "embeds": [{
            "title": alert.headline,
            "description": alert.reason,
            "color": alert_color(&alert.severity),
            "fields": fields,
            "footer": {
                "text": format!("{} • {} • {}", alert.alert_key, alert.severity, app_env)
            },
            "timestamp": alert.occurred_at.to_rfc3339(),
        }]
    })
}

fn build_trade_summary(trade: &ClosedTradeNotification) -> String {
    let setup = compact_lane_summary(&trade.lane_key);
    let side = trade.side.clone().unwrap_or_else(|| "trade".to_string());
    let movement = if trade.realized_pnl > 0.0 {
        "after a favorable move"
    } else if trade.realized_pnl < 0.0 {
        "after an adverse move"
    } else {
        "flat on exit"
    };
    let mut summary = format!(
        "Closed {} {} in {} {}.",
        setup,
        side,
        mode_label_lower(trade.mode),
        movement
    );
    let reason = trade
        .close_reason
        .clone()
        .unwrap_or_else(|| humanize_status(&trade.status));
    if !reason.is_empty() {
        summary.push_str(&format!(" Reason: {}.", humanize_status(&reason)));
    }
    summary
}

fn trade_outcome_label(realized_pnl: f64) -> &'static str {
    if realized_pnl > 0.0 {
        "WIN"
    } else if realized_pnl < 0.0 {
        "LOSS"
    } else {
        "FLAT"
    }
}

fn trade_color(realized_pnl: f64) -> u32 {
    if realized_pnl > 0.0 {
        0x16A34A
    } else if realized_pnl < 0.0 {
        0xDC2626
    } else {
        0xD97706
    }
}

fn alert_color(severity: &str) -> u32 {
    match severity {
        "critical" => 0xDC2626,
        "warning" => 0xF59E0B,
        _ => 0x64748B,
    }
}

fn mode_label_upper(mode: TradeMode) -> &'static str {
    match mode {
        TradeMode::Paper => "PAPER",
        TradeMode::Live => "LIVE",
    }
}

fn mode_label_lower(mode: TradeMode) -> &'static str {
    match mode {
        TradeMode::Paper => "paper",
        TradeMode::Live => "live",
    }
}

fn strategy_family_label(strategy_family: common::StrategyFamily) -> String {
    match strategy_family {
        common::StrategyFamily::DirectionalSettlement => "directional_settlement".to_string(),
        common::StrategyFamily::PreSettlementScalp => "pre_settlement_scalp".to_string(),
        common::StrategyFamily::Portfolio => "portfolio".to_string(),
    }
}

fn compact_lane_summary(lane_key: &str) -> String {
    let parts: Vec<&str> = lane_key.split(':').collect();
    if parts.len() >= 6 {
        let regime = if parts.len() >= 7 {
            format!(" · {}", parts[6])
        } else {
            String::new()
        };
        return format!(
            "{} {}m{} · {}",
            parts[1].to_uppercase(),
            parts[2],
            regime,
            parts[5]
        );
    }
    lane_key.to_string()
}

fn format_signed_money_no_plus_for_zero(value: f64) -> String {
    if value > 0.0 {
        format!("+${value:.2}")
    } else if value < 0.0 {
        format!("-${:.2}", value.abs())
    } else {
        "$0.00".to_string()
    }
}

fn format_contract_price(value: f64) -> String {
    format!("{:.1}c", (value * 100.0).max(0.0))
}

fn format_quantity(value: f64) -> String {
    if (value.fract()).abs() < 0.001 {
        format!("{}", value.round() as i64)
    } else {
        format!("{value:.2}")
    }
}

fn humanize_status(raw: &str) -> String {
    raw.replace('_', " ")
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn push_field(fields: &mut Vec<Value>, name: &str, value: Option<String>, inline: bool) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        fields.push(json!({
            "name": name,
            "value": value,
            "inline": inline,
        }));
    }
}

fn format_operator_action_state(action: &common::OperatorActionEvent) -> String {
    let result = action
        .payload_json
        .get("result")
        .cloned()
        .unwrap_or_else(|| json!({}));
    match action.action.as_str() {
        "cancel_pending_live_orders" => format!(
            "exchange_cancelled={} local_cancelled={}",
            result
                .get("exchange_cancelled")
                .and_then(|value| value.as_u64())
                .unwrap_or_default(),
            result
                .get("local_cancelled")
                .and_then(|value| value.as_u64())
                .unwrap_or_default()
        ),
        "flatten_live_positions" => format!(
            "exchange_cancelled={} flatten_submitted={}",
            result
                .get("exchange_cancelled")
                .and_then(|value| value.as_u64())
                .unwrap_or_default(),
            result
                .get("flatten_submitted")
                .and_then(|value| value.as_u64())
                .unwrap_or_default()
        ),
        _ => action.action.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_alert_webhook, build_trade_webhook, compact_lane_summary, format_contract_price,
    };
    use chrono::TimeZone;
    use common::{CriticalAlertNotification, MarketFamily, StrategyFamily, TradeMode};
    use serde_json::Value;
    use storage::ClosedTradeNotification;

    fn sample_trade(realized_pnl: f64, mode: TradeMode) -> ClosedTradeNotification {
        ClosedTradeNotification {
            trade_id: 42,
            lane_key: "kalshi:xrp:15:buy_yes:directional_settlement:trained_linear_v1".to_string(),
            market_family: MarketFamily::Crypto,
            strategy_family: StrategyFamily::DirectionalSettlement,
            mode,
            market_ticker: Some("KXBTCD-26MAR181315-T55125.99".to_string()),
            market_title: Some("XRP price up in next 15 min?".to_string()),
            side: Some("buy_yes".to_string()),
            status: "timeout_exit".to_string(),
            close_reason: Some("timeout_exit".to_string()),
            quantity: 7.0,
            entry_price: 0.34,
            exit_price: 0.51,
            realized_pnl,
            closed_at: chrono::Utc
                .with_ymd_and_hms(2026, 3, 18, 20, 15, 0)
                .single()
                .expect("valid timestamp"),
        }
    }

    fn first_embed(body: &Value) -> &Value {
        &body["embeds"][0]
    }

    #[test]
    fn win_trade_embed_is_green_and_clear() {
        let body = build_trade_webhook(&sample_trade(12.34, TradeMode::Paper), "development");
        let embed = first_embed(&body);
        assert_eq!(embed["title"], "WIN | PAPER | +$12.34");
        assert_eq!(embed["color"], 0x16A34A);
        assert_eq!(embed["fields"][0]["name"], "Market");
    }

    #[test]
    fn loss_trade_embed_is_red_and_live_prominent() {
        let body = build_trade_webhook(&sample_trade(-5.60, TradeMode::Live), "production");
        let embed = first_embed(&body);
        assert_eq!(embed["title"], "LOSS | LIVE | -$5.60");
        assert_eq!(embed["color"], 0xDC2626);
        assert!(
            embed["description"]
                .as_str()
                .expect("description")
                .contains("live")
        );
    }

    #[test]
    fn flat_trade_embed_is_neutral() {
        let body = build_trade_webhook(&sample_trade(0.0, TradeMode::Paper), "development");
        let embed = first_embed(&body);
        assert_eq!(embed["title"], "FLAT | PAPER | $0.00");
        assert_eq!(embed["color"], 0xD97706);
    }

    #[test]
    fn missing_market_title_falls_back_cleanly() {
        let mut trade = sample_trade(1.25, TradeMode::Paper);
        trade.market_title = None;
        trade.market_ticker = Some("KXRP".to_string());
        let body = build_trade_webhook(&trade, "development");
        let embed = first_embed(&body);
        assert_eq!(embed["fields"][0]["value"], "KXRP");
    }

    #[test]
    fn alert_embed_renders_next_action_and_state() {
        let alert = CriticalAlertNotification {
            alert_key: "reconciliation:1:42".to_string(),
            headline: "ALERT | RECONCILIATION ISSUE".to_string(),
            severity: "critical".to_string(),
            reason: "1 live trade does not match exchange truth.".to_string(),
            subsystem: "execution".to_string(),
            affected_lane: Some(
                "kalshi:xrp:15:buy_yes:directional_settlement:trained_linear_v1".to_string(),
            ),
            current_state: Some("exchange truth mismatch on XRP".to_string()),
            next_action: Some("Run reconcile_now.".to_string()),
            occurred_at: chrono::Utc
                .with_ymd_and_hms(2026, 3, 18, 20, 20, 0)
                .single()
                .expect("valid timestamp"),
        };
        let body = build_alert_webhook(&alert, "production");
        let embed = first_embed(&body);
        assert_eq!(embed["title"], "ALERT | RECONCILIATION ISSUE");
        assert_eq!(embed["color"], 0xDC2626);
        assert_eq!(embed["fields"][0]["name"], "Subsystem");
    }

    #[test]
    fn compact_lane_summary_is_mobile_friendly() {
        assert_eq!(
            compact_lane_summary("kalshi:xrp:15:buy_yes:directional_settlement:trained_linear_v1"),
            "XRP 15m · trained_linear_v1"
        );
        assert_eq!(format_contract_price(0.345), "34.5c");
    }
}
