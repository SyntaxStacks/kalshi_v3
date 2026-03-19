use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

const DEFAULT_WINDOW_SECONDS: i64 = 15;
const MAX_LEVELS: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PriceLevel {
    pub price: f64,
    pub total_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct OrderBookSide {
    pub levels: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContractSide {
    Yes,
    No,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub market_ticker: String,
    pub yes: Vec<PriceLevel>,
    pub no: Vec<PriceLevel>,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub market_ticker: String,
    pub side: ContractSide,
    pub price: f64,
    pub delta_size: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradePrint {
    pub market_ticker: String,
    pub side: ContractSide,
    pub price: f64,
    pub quantity: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueState {
    pub price: f64,
    pub size_ahead: f64,
    pub last_update_ts: DateTime<Utc>,
    pub trade_consumption_rate: f64,
    pub cancel_rate: f64,
    pub queue_decay_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueSideSummary {
    pub best_price: Option<f64>,
    pub total_size: f64,
    pub size_ahead_real: f64,
    pub trade_consumption_rate: f64,
    pub cancel_rate: f64,
    pub queue_decay_rate: f64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueSnapshot {
    pub market_ticker: String,
    pub yes: QueueSideSummary,
    pub no: QueueSideSummary,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookState {
    pub market_ticker: String,
    pub yes: OrderBookSide,
    pub no: OrderBookSide,
    pub last_update_ts: DateTime<Utc>,
    window_seconds: i64,
    recent_trades: VecDeque<TradePrint>,
    recent_events: VecDeque<QueueEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueEvent {
    ts: DateTime<Utc>,
    side: ContractSide,
    price: f64,
    trade_volume: f64,
    cancel_volume: f64,
    new_ahead_volume: f64,
}

impl OrderBookState {
    pub fn new(market_ticker: impl Into<String>) -> Self {
        Self {
            market_ticker: market_ticker.into(),
            yes: OrderBookSide::default(),
            no: OrderBookSide::default(),
            last_update_ts: Utc::now(),
            window_seconds: DEFAULT_WINDOW_SECONDS,
            recent_trades: VecDeque::new(),
            recent_events: VecDeque::new(),
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: OrderBookSnapshot) {
        self.market_ticker = snapshot.market_ticker;
        self.yes.levels = normalize_levels(snapshot.yes);
        self.no.levels = normalize_levels(snapshot.no);
        self.last_update_ts = snapshot.ts;
        self.prune(snapshot.ts);
    }

    pub fn apply_delta(&mut self, delta: OrderBookDelta) {
        let removed = apply_side_delta(self.side_mut(delta.side), delta.price, delta.delta_size);
        let matched_trade = self.consume_recent_trade(delta.side, delta.price, removed, delta.ts);
        let cancel_volume = (removed - matched_trade).max(0.0);
        let new_ahead_volume = delta.delta_size.max(0.0);
        if removed > 0.0 || new_ahead_volume > 0.0 {
            self.recent_events.push_back(QueueEvent {
                ts: delta.ts,
                side: delta.side,
                price: delta.price,
                trade_volume: matched_trade,
                cancel_volume,
                new_ahead_volume,
            });
        }
        self.last_update_ts = delta.ts;
        self.prune(delta.ts);
    }

    pub fn apply_trade(&mut self, trade: TradePrint) {
        self.last_update_ts = trade.ts;
        self.recent_trades.push_back(trade);
        self.prune(self.last_update_ts);
    }

    pub fn queue_state(&self, side: ContractSide, price: f64, own_size: f64) -> QueueState {
        let total_size = self
            .side(side)
            .levels
            .iter()
            .find(|level| nearly_equal(level.price, price))
            .map(|level| level.total_size)
            .unwrap_or_default();
        let size_ahead = (total_size - own_size.max(0.0)).max(0.0);
        let (trade_consumption_rate, cancel_rate, queue_decay_rate) =
            self.metrics_for_level(side, price, self.last_update_ts);
        QueueState {
            price,
            size_ahead,
            last_update_ts: self.last_update_ts,
            trade_consumption_rate,
            cancel_rate,
            queue_decay_rate,
        }
    }

    pub fn queue_snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            market_ticker: self.market_ticker.clone(),
            yes: self.side_summary(ContractSide::Yes),
            no: self.side_summary(ContractSide::No),
            updated_at: self.last_update_ts,
        }
    }

    fn side_summary(&self, side: ContractSide) -> QueueSideSummary {
        let book_side = self.side(side);
        let best_price = book_side.levels.first().map(|level| level.price);
        let total_size = book_side
            .levels
            .first()
            .map(|level| level.total_size.max(0.0))
            .unwrap_or_default();
        let (trade_consumption_rate, cancel_rate, queue_decay_rate) =
            if let Some(price) = best_price {
                self.metrics_for_level(side, price, self.last_update_ts)
            } else {
                (0.0, 0.0, 0.0)
            };
        QueueSideSummary {
            best_price,
            total_size,
            size_ahead_real: total_size,
            trade_consumption_rate,
            cancel_rate,
            queue_decay_rate,
            updated_at: self.last_update_ts,
        }
    }

    fn metrics_for_level(
        &self,
        side: ContractSide,
        price: f64,
        now: DateTime<Utc>,
    ) -> (f64, f64, f64) {
        let window_seconds = self.window_seconds.max(1) as f64;
        let mut trade_volume = 0.0;
        let mut cancel_volume = 0.0;
        let mut new_ahead_volume = 0.0;
        for event in self.recent_events.iter().filter(|event| {
            event.side == side
                && nearly_equal(event.price, price)
                && (now - event.ts).num_seconds() <= self.window_seconds
        }) {
            trade_volume += event.trade_volume;
            cancel_volume += event.cancel_volume;
            new_ahead_volume += event.new_ahead_volume;
        }
        let trade_consumption_rate = trade_volume / window_seconds;
        let cancel_rate = cancel_volume / window_seconds;
        let queue_decay_rate =
            ((trade_volume + cancel_volume) - new_ahead_volume).max(0.0) / window_seconds;
        (trade_consumption_rate, cancel_rate, queue_decay_rate)
    }

    fn consume_recent_trade(
        &mut self,
        side: ContractSide,
        price: f64,
        mut amount: f64,
        now: DateTime<Utc>,
    ) -> f64 {
        if amount <= 0.0 {
            return 0.0;
        }
        let mut matched = 0.0;
        for trade in self.recent_trades.iter_mut() {
            if trade.side == side
                && nearly_equal(trade.price, price)
                && (now - trade.ts).num_seconds().abs() <= 2
                && trade.quantity > 0.0
            {
                let consumed = amount.min(trade.quantity);
                trade.quantity -= consumed;
                matched += consumed;
                amount -= consumed;
                if amount <= 0.0 {
                    break;
                }
            }
        }
        matched
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        while self
            .recent_trades
            .front()
            .is_some_and(|event| (now - event.ts).num_seconds() > self.window_seconds)
        {
            self.recent_trades.pop_front();
        }
        while self
            .recent_events
            .front()
            .is_some_and(|event| (now - event.ts).num_seconds() > self.window_seconds)
        {
            self.recent_events.pop_front();
        }
    }

    fn side(&self, side: ContractSide) -> &OrderBookSide {
        match side {
            ContractSide::Yes => &self.yes,
            ContractSide::No => &self.no,
        }
    }

    fn side_mut(&mut self, side: ContractSide) -> &mut OrderBookSide {
        match side {
            ContractSide::Yes => &mut self.yes,
            ContractSide::No => &mut self.no,
        }
    }
}

fn apply_side_delta(side: &mut OrderBookSide, price: f64, delta_size: f64) -> f64 {
    let mut removed = 0.0;
    if let Some(level) = side
        .levels
        .iter_mut()
        .find(|level| nearly_equal(level.price, price))
    {
        if delta_size < 0.0 {
            removed = (-delta_size).min(level.total_size.max(0.0));
        }
        level.total_size = (level.total_size + delta_size).max(0.0);
    } else if delta_size > 0.0 {
        side.levels.push(PriceLevel {
            price,
            total_size: delta_size,
        });
    }
    side.levels.retain(|level| level.total_size > 0.0);
    side.levels
        .sort_by(|left, right| right.price.total_cmp(&left.price));
    side.levels.truncate(MAX_LEVELS);
    removed
}

fn normalize_levels(levels: Vec<PriceLevel>) -> Vec<PriceLevel> {
    let mut levels = levels
        .into_iter()
        .filter(|level| level.total_size > 0.0)
        .collect::<Vec<_>>();
    levels.sort_by(|left, right| right.price.total_cmp(&left.price));
    levels.truncate(MAX_LEVELS);
    levels
}

fn nearly_equal(left: f64, right: f64) -> bool {
    (left - right).abs() < 0.000_001
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn snapshot_then_delta_updates_book() {
        let now = Utc::now();
        let mut state = OrderBookState::new("KXBTC15M-TEST");
        state.apply_snapshot(OrderBookSnapshot {
            market_ticker: "KXBTC15M-TEST".to_string(),
            yes: vec![PriceLevel {
                price: 0.55,
                total_size: 120.0,
            }],
            no: vec![PriceLevel {
                price: 0.45,
                total_size: 90.0,
            }],
            ts: now,
        });
        state.apply_delta(OrderBookDelta {
            market_ticker: "KXBTC15M-TEST".to_string(),
            side: ContractSide::Yes,
            price: 0.55,
            delta_size: -20.0,
            ts: now + Duration::seconds(1),
        });
        assert_eq!(state.yes.levels[0].total_size, 100.0);
    }

    #[test]
    fn trade_and_delta_drive_queue_metrics() {
        let now = Utc::now();
        let mut state = OrderBookState::new("KXETH15M-TEST");
        state.apply_snapshot(OrderBookSnapshot {
            market_ticker: "KXETH15M-TEST".to_string(),
            yes: vec![PriceLevel {
                price: 0.60,
                total_size: 80.0,
            }],
            no: vec![],
            ts: now,
        });
        state.apply_trade(TradePrint {
            market_ticker: "KXETH15M-TEST".to_string(),
            side: ContractSide::Yes,
            price: 0.60,
            quantity: 15.0,
            ts: now + Duration::seconds(1),
        });
        state.apply_delta(OrderBookDelta {
            market_ticker: "KXETH15M-TEST".to_string(),
            side: ContractSide::Yes,
            price: 0.60,
            delta_size: -20.0,
            ts: now + Duration::seconds(1),
        });
        let queue = state.queue_snapshot();
        assert!(queue.yes.trade_consumption_rate > 0.0);
        assert!(queue.yes.cancel_rate >= 0.0);
    }

    #[test]
    fn queue_state_subtracts_own_size_from_size_ahead() {
        let now = Utc::now();
        let mut state = OrderBookState::new("KXXRP15M-TEST");
        state.apply_snapshot(OrderBookSnapshot {
            market_ticker: "KXXRP15M-TEST".to_string(),
            yes: vec![PriceLevel {
                price: 0.48,
                total_size: 50.0,
            }],
            no: vec![],
            ts: now,
        });
        let queue = state.queue_state(ContractSide::Yes, 0.48, 5.0);
        assert_eq!(queue.size_ahead, 45.0);
    }
}
