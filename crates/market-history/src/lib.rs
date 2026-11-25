//! Decimal aggregation and the versioned history API shared with et-rs.
pub mod hyperliquid;
pub use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MINUTE_MS: u64 = 60_000;

pub fn period_ms(period: &str) -> Result<u64, String> {
    match period {
        "1m" => Ok(MINUTE_MS),
        "5m" => Ok(5 * MINUTE_MS),
        "15m" => Ok(15 * MINUTE_MS),
        "30m" => Ok(30 * MINUTE_MS),
        "1h" => Ok(60 * MINUTE_MS),
        _ => Err(format!("unsupported period: {period}")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u8,
    pub volume_precision: u8,
    pub min_tick_size: Decimal,
}

/// Both Binance REST and WS aggTrade payloads use these field names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTrade {
    #[serde(rename = "a")]
    pub id: u64,
    #[serde(rename = "T")]
    pub time: u64,
    #[serde(rename = "p")]
    pub price: Decimal,
    #[serde(rename = "q")]
    pub quantity: Decimal,
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

impl AggTrade {
    pub fn validate(&self) -> Result<(), String> {
        if self.price <= Decimal::ZERO || self.quantity <= Decimal::ZERO {
            return Err("trade price and quantity must be positive".into());
        }
        if self.id > i64::MAX as u64 || self.time > i64::MAX as u64 {
            return Err("trade identifier/time out of range".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Level {
    pub bid: Decimal,
    pub ask: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub timestamp: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub first: (u64, u64),
    pub last: (u64, u64),
    pub levels: BTreeMap<Decimal, Level>,
}

impl Candle {
    pub fn new(trade: &AggTrade, interval: u64) -> Self {
        let mut result = Self {
            timestamp: trade.time / interval * interval,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: Decimal::ZERO,
            first: (trade.time, trade.id),
            last: (trade.time, trade.id),
            levels: BTreeMap::new(),
        };
        result.apply(trade);
        result
    }

    /// Caller must deduplicate by aggregate ID before applying a trade.
    pub fn apply(&mut self, trade: &AggTrade) {
        let order = (trade.time, trade.id);
        if order < self.first {
            self.first = order;
            self.open = trade.price;
        }
        if order >= self.last {
            self.last = order;
            self.close = trade.price;
        }
        self.high = self.high.max(trade.price);
        self.low = self.low.min(trade.price);
        self.volume += trade.quantity;
        let level = self.levels.entry(trade.price).or_default();
        if trade.buyer_is_maker {
            level.bid += trade.quantity;
        } else {
            level.ask += trade.quantity;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.first < self.first {
            self.first = other.first;
            self.open = other.open;
        }
        if other.last > self.last {
            self.last = other.last;
            self.close = other.close;
        }
        self.high = self.high.max(other.high);
        self.low = self.low.min(other.low);
        self.volume += other.volume;
        for (price, value) in &other.levels {
            let level = self.levels.entry(*price).or_default();
            level.bid += value.bid;
            level.ask += value.ask;
        }
    }
}

pub fn rollup(minutes: impl IntoIterator<Item = Candle>, interval: u64) -> Vec<Candle> {
    let mut bars = BTreeMap::<u64, Candle>::new();
    for mut candle in minutes {
        candle.timestamp = candle.timestamp / interval * interval;
        if let Some(existing) = bars.get_mut(&candle.timestamp) {
            existing.merge(&candle);
        } else {
            bars.insert(candle.timestamp, candle);
        }
    }
    bars.into_values().collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct History {
    pub symbol: Symbol,
    pub period: String,
    pub bars: Vec<Candle>,
    pub last_aggregate_id: Option<u64>,
    pub last_trade_time: Option<u64>,
    pub coverage_start: u64,
    pub has_more: bool,
}

/// History watermark and direct WS updates must share the same aggTrade ID space.
pub struct LiveSeries {
    pub last_id: Option<u64>,
    interval: u64,
    pub current: Option<Candle>,
}

impl LiveSeries {
    pub fn from_history(history: &History) -> Result<Self, String> {
        if history.last_aggregate_id.is_some() && history.bars.is_empty() {
            return Err(
                "Server footprint coverage has not completed the first interval yet".into(),
            );
        }
        Ok(Self {
            last_id: history.last_aggregate_id,
            interval: period_ms(&history.period)?,
            current: history.bars.last().cloned(),
        })
    }

    pub fn apply(&mut self, trade: &AggTrade) -> Result<bool, String> {
        trade.validate()?;
        if self.last_id.is_some_and(|id| trade.id <= id) {
            return Ok(false);
        }
        if self.last_id.is_some_and(|id| trade.id != id + 1) {
            return Err("live aggTrade gap; reload server history".into());
        }
        let bucket = trade.time / self.interval * self.interval;
        if self
            .current
            .as_ref()
            .is_some_and(|bar| bar.timestamp > bucket)
        {
            return Err("late trade targets an older candle; reload server history".into());
        }
        match &mut self.current {
            Some(bar) if bar.timestamp == bucket => bar.apply(trade),
            _ => self.current = Some(Candle::new(trade, self.interval)),
        }
        self.last_id = Some(trade.id);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn trade(id: u64, time: u64, price: &str, sell: bool) -> AggTrade {
        AggTrade {
            id,
            time,
            price: price.parse().unwrap(),
            quantity: "0.1".parse().unwrap(),
            buyer_is_maker: sell,
        }
    }
    #[test]
    fn event_time_precision_and_side_survive_rollup() {
        let mut minute = Candle::new(&trade(2, 60_001, "0.00000123", true), MINUTE_MS);
        minute.apply(&trade(1, 60_000, "0.00000122", false));
        assert_eq!(minute.open.to_string(), "0.00000122");
        assert_eq!(minute.close.to_string(), "0.00000123");
        let next = Candle::new(&trade(3, 120_000, "0.00000123", false), MINUTE_MS);
        let bars = rollup([minute, next], period_ms("5m").unwrap());
        assert_eq!(bars[0].timestamp, 0);
        assert_eq!(bars[0].volume.to_string(), "0.3");
        let level = &bars[0].levels[&"0.00000123".parse::<Decimal>().unwrap()];
        assert_eq!(level.bid, level.ask);
        let json = serde_json::to_string(&bars[0]).unwrap();
        let decoded: Candle = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.levels.len(), 2);
    }
    #[test]
    fn handoff_deduplicates_and_rejects_missing_trades() {
        let t = trade(10, 60_001, "100", false);
        let mut live = LiveSeries {
            last_id: Some(10),
            interval: MINUTE_MS,
            current: Some(Candle::new(&t, MINUTE_MS)),
        };
        assert!(!live.apply(&t).unwrap());
        assert!(live.apply(&trade(12, 60_002, "101", true)).is_err());
        assert_eq!(live.last_id, Some(10));
        assert!(live.apply(&trade(11, 60_002, "101", true)).unwrap());
        assert_eq!(live.current.as_ref().unwrap().volume.to_string(), "0.2");
        assert!(live.apply(&trade(12, 120_000, "102", false)).unwrap());
        assert_eq!(live.current.as_ref().unwrap().timestamp, 120_000);
    }
}
