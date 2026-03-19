use anyhow::Result;
use common::AppConfig;
use reqwest::Client;
use serde_json::json;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = AppConfig::load()?;
    let storage = storage::Storage::connect(&config.database_url).await?;
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

async fn notify_once(
    config: &AppConfig,
    storage: &storage::Storage,
    client: &Client,
) -> Result<usize> {
    let Some(webhook) = config
        .discord_webhook_url
        .as_ref()
        .filter(|value| !value.is_empty())
    else {
        return Ok(0);
    };
    let trades = storage.list_unnotified_closed_trades(20).await?;
    let mut sent_count = 0usize;
    for trade in trades {
        let headline = if trade.realized_pnl > 0.0 {
            "WIN"
        } else if trade.realized_pnl < 0.0 {
            "LOSS"
        } else {
            "FLAT"
        };
        let body = json!({
            "content": format!(
                "{} {} {} | pnl {} | qty {:.2} | {} -> {} | {}",
                headline,
                stringify_mode(trade.mode),
                trade.lane_key,
                format_money(trade.realized_pnl),
                trade.quantity,
                format_money(trade.entry_price),
                format_money(trade.exit_price),
                trade.closed_at.format("%Y-%m-%d %H:%M:%S UTC")
            )
        });
        client
            .post(webhook)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        storage.mark_trade_notified(trade.trade_id).await?;
        sent_count += 1;
    }
    Ok(sent_count)
}

fn stringify_mode(mode: common::TradeMode) -> &'static str {
    match mode {
        common::TradeMode::Paper => "paper",
        common::TradeMode::Live => "live",
    }
}

fn format_money(value: f64) -> String {
    if value >= 0.0 {
        format!("+${value:.2}")
    } else {
        format!("-${:.2}", value.abs())
    }
}
