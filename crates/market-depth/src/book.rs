use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::OrderBookSnapshot;
use serde::Deserialize;

use crate::{HeatmapData, HeatmapDepthUpdate, HeatmapOrderRun, HeatmapRunClose};

pub const HEATMAP_RETENTION_MS: u64 = 30 * 60 * 1_000;
pub const DEPTH_LEVELS_PER_SIDE: usize = 1_000;
const RUN_QUANTITY_TOLERANCE: f64 = 0.15;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinanceDepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceDepthDelta {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub final_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Copy)]
pub struct GroupedDepthLevel {
    pub price_ticks: i64,
    pub quantity: f64,
    pub is_bid: bool,
}

pub struct LocalOrderBook {
    min_tick_size: f64,
    last_update_id: u64,
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
}

impl LocalOrderBook {
    pub fn from_snapshot(
        snapshot: BinanceDepthSnapshot,
        min_tick_size: f64,
    ) -> Result<Self, String> {
        let mut book = Self {
            min_tick_size,
            last_update_id: snapshot.last_update_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        };
        apply_raw_levels(&mut book.bids, &snapshot.bids, min_tick_size)?;
        apply_raw_levels(&mut book.asks, &snapshot.asks, min_tick_size)?;
        Ok(book)
    }

    pub fn apply_delta(&mut self, delta: BinanceDepthDelta) -> Result<bool, String> {
        if delta.final_update_id <= self.last_update_id {
            return Ok(false);
        }

        let expected = self.last_update_id.saturating_add(1);
        if delta.first_update_id > expected || delta.final_update_id < expected {
            return Err(format!(
                "Binance depth sequence gap: expected {expected}, received {}-{}",
                delta.first_update_id, delta.final_update_id
            ));
        }

        apply_raw_levels(&mut self.bids, &delta.bids, self.min_tick_size)?;
        apply_raw_levels(&mut self.asks, &delta.asks, self.min_tick_size)?;
        self.last_update_id = delta.final_update_id;
        Ok(true)
    }

    pub fn mid_price(&self) -> Option<f64> {
        let bid = self.bids.last_key_value()?.0;
        let ask = self.asks.first_key_value()?.0;
        Some((*bid + *ask) as f64 * self.min_tick_size / 2.0)
    }

    pub fn best_prices(&self) -> (Option<f64>, Option<f64>) {
        (
            self.bids
                .last_key_value()
                .map(|(ticks, _)| *ticks as f64 * self.min_tick_size),
            self.asks
                .first_key_value()
                .map(|(ticks, _)| *ticks as f64 * self.min_tick_size),
        )
    }

    pub fn snapshot(&self, symbol: &str, timestamp: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            symbol: symbol.to_owned(),
            timestamp,
            tick_size: self.min_tick_size,
            bids: self
                .bids
                .iter()
                .rev()
                .take(100)
                .map(|(p, q)| [*p as f64 * self.min_tick_size, *q])
                .collect(),
            asks: self
                .asks
                .iter()
                .take(100)
                .map(|(p, q)| [*p as f64 * self.min_tick_size, *q])
                .collect(),
        }
    }

    pub fn grouped_levels(&self, heatmap_tick_size: f64) -> Vec<GroupedDepthLevel> {
        let group_ticks = (heatmap_tick_size / self.min_tick_size).round().max(1.0) as i64;
        let mut grouped = BTreeMap::<(i64, bool), f64>::new();

        for (ticks, quantity) in self.bids.iter().rev().take(DEPTH_LEVELS_PER_SIDE) {
            let price_ticks = ticks.div_euclid(group_ticks) * group_ticks;
            *grouped.entry((price_ticks, true)).or_default() += quantity;
        }
        for (ticks, quantity) in self.asks.iter().take(DEPTH_LEVELS_PER_SIDE) {
            let price_ticks = ticks.div_euclid(group_ticks) * group_ticks
                + i64::from(ticks.rem_euclid(group_ticks) != 0) * group_ticks;
            *grouped.entry((price_ticks, false)).or_default() += quantity;
        }

        grouped
            .into_iter()
            .map(|((price_ticks, is_bid), quantity)| GroupedDepthLevel {
                price_ticks: price_ticks / group_ticks,
                quantity,
                is_bid,
            })
            .collect()
    }
}

fn apply_raw_levels(
    side: &mut BTreeMap<i64, f64>,
    levels: &[[String; 2]],
    min_tick_size: f64,
) -> Result<(), String> {
    for [price_text, quantity_text] in levels {
        let price = price_text
            .parse::<f64>()
            .map_err(|error| format!("invalid Binance depth price: {error}"))?;
        let quantity = quantity_text
            .parse::<f64>()
            .map_err(|error| format!("invalid Binance depth quantity: {error}"))?;
        let ticks = (price / min_tick_size).round() as i64;
        if quantity == 0.0 {
            side.remove(&ticks);
        } else {
            side.insert(ticks, quantity);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RunKey {
    price_ticks: i64,
    is_bid: bool,
}

pub struct HeatmapCache {
    /// Latest exact book from the same depth connection; independent of heatmap tolerance.
    pub order_book: Option<OrderBookSnapshot>,
    tick_size: f64,
    latest_time: u64,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    next_run_id: u64,
    completed: VecDeque<HeatmapOrderRun>,
    active: HashMap<RunKey, HeatmapOrderRun>,
}

impl HeatmapCache {
    pub fn new(min_tick_size: f64) -> Self {
        Self {
            order_book: None,
            tick_size: min_tick_size,
            latest_time: 0,
            best_bid: None,
            best_ask: None,
            next_run_id: 1,
            completed: VecDeque::new(),
            active: HashMap::new(),
        }
    }

    pub fn configure_tick_size(&mut self, mid_price: f64, min_tick_size: f64) {
        if self.latest_time == 0 {
            self.tick_size = choose_heatmap_tick_size(mid_price, min_tick_size);
        }
    }

    pub fn tick_size(&self) -> f64 {
        self.tick_size
    }

    pub fn apply_levels(
        &mut self,
        symbol: &str,
        timestamp: u64,
        levels: Vec<GroupedDepthLevel>,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
    ) -> HeatmapDepthUpdate {
        let mut previous = std::mem::take(&mut self.active);
        let mut next = HashMap::with_capacity(levels.len());
        let mut started_runs = Vec::new();
        let mut closed_runs = Vec::new();

        for level in levels {
            if !level.quantity.is_finite() || level.quantity <= 0.0 {
                continue;
            }
            let key = RunKey {
                price_ticks: level.price_ticks,
                is_bid: level.is_bid,
            };
            match previous.remove(&key) {
                Some(mut run) if quantities_are_similar(run.quantity, level.quantity) => {
                    run.end_time = timestamp;
                    next.insert(key, run);
                }
                Some(mut run) => {
                    close_run(&mut run, timestamp, &mut closed_runs);
                    self.completed.push_back(run);
                    let run = self.start_run(timestamp, level);
                    started_runs.push(run.clone());
                    next.insert(key, run);
                }
                None => {
                    let run = self.start_run(timestamp, level);
                    started_runs.push(run.clone());
                    next.insert(key, run);
                }
            }
        }

        for (_, mut run) in previous {
            close_run(&mut run, timestamp, &mut closed_runs);
            self.completed.push_back(run);
        }

        self.active = next;
        self.latest_time = timestamp;
        self.best_bid = best_bid;
        self.best_ask = best_ask;
        self.prune(timestamp.saturating_sub(HEATMAP_RETENTION_MS));

        HeatmapDepthUpdate {
            symbol: symbol.to_string(),
            tick_size: self.tick_size,
            timestamp,
            best_bid,
            best_ask,
            started_runs,
            closed_runs,
        }
    }

    /// The window collector only needs active quantities, not rendered run history.
    pub fn discard_completed(&mut self) {
        self.completed.clear();
    }

    /// Active tolerance-filtered quantities, identical to the desktop heatmap.
    pub fn active_levels(&self) -> Vec<(f64, f64, bool)> {
        let mut levels: Vec<_> = self
            .active
            .values()
            .map(|r| (r.price, r.quantity, r.is_bid))
            .collect();
        levels.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.2.cmp(&b.2)));
        levels
    }

    pub fn data(&self, symbol: &str) -> HeatmapData {
        let mut runs = Vec::with_capacity(self.completed.len() + self.active.len());
        runs.extend(self.completed.iter().cloned());
        runs.extend(self.active.values().cloned());
        HeatmapData {
            symbol: symbol.to_string(),
            tick_size: self.tick_size,
            latest_time: self.latest_time,
            best_bid: self.best_bid,
            best_ask: self.best_ask,
            runs,
        }
    }

    fn start_run(&mut self, timestamp: u64, level: GroupedDepthLevel) -> HeatmapOrderRun {
        let run = HeatmapOrderRun {
            id: self.next_run_id,
            start_time: timestamp,
            end_time: timestamp,
            price: level.price_ticks as f64 * self.tick_size,
            quantity: level.quantity,
            is_bid: level.is_bid,
            active: true,
        };
        self.next_run_id = self.next_run_id.saturating_add(1);
        run
    }

    fn prune(&mut self, oldest_time: u64) {
        while self
            .completed
            .front()
            .is_some_and(|run| run.end_time < oldest_time)
        {
            self.completed.pop_front();
        }
    }
}

fn close_run(run: &mut HeatmapOrderRun, timestamp: u64, closed: &mut Vec<HeatmapRunClose>) {
    run.end_time = timestamp;
    run.active = false;
    closed.push(HeatmapRunClose {
        id: run.id,
        end_time: timestamp,
    });
}

fn quantities_are_similar(left: f64, right: f64) -> bool {
    if left == right {
        return true;
    }
    let base = left.abs().max(f64::EPSILON);
    (left - right).abs() / base <= RUN_QUANTITY_TOLERANCE
}

fn choose_heatmap_tick_size(mid_price: f64, min_tick_size: f64) -> f64 {
    let target = (mid_price * 0.000025).max(min_tick_size);
    let multiplier = (target / min_tick_size).max(1.0);
    let magnitude = 10_f64.powf(multiplier.log10().floor());
    let normalized = multiplier / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };
    min_tick_size * nice * magnitude
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price_ticks: i64, quantity: f64, is_bid: bool) -> GroupedDepthLevel {
        GroupedDepthLevel {
            price_ticks,
            quantity,
            is_bid,
        }
    }

    #[test]
    fn local_book_rejects_depth_sequence_gaps() {
        let snapshot = BinanceDepthSnapshot {
            last_update_id: 10,
            bids: vec![["100.0".into(), "2.0".into()]],
            asks: vec![["101.0".into(), "3.0".into()]],
        };
        let mut book = LocalOrderBook::from_snapshot(snapshot, 0.1).expect("book");
        let gap = BinanceDepthDelta {
            event_time: 1,
            first_update_id: 12,
            final_update_id: 13,
            bids: Vec::new(),
            asks: Vec::new(),
        };

        assert!(book.apply_delta(gap).is_err());
    }

    #[test]
    fn local_book_discards_stale_updates_then_accepts_snapshot_bridge() {
        let snapshot = BinanceDepthSnapshot {
            last_update_id: 10,
            bids: vec![["100.0".into(), "2.0".into()]],
            asks: vec![["101.0".into(), "3.0".into()]],
        };
        let mut book = LocalOrderBook::from_snapshot(snapshot, 0.1).expect("book");
        let stale = BinanceDepthDelta {
            event_time: 1,
            first_update_id: 8,
            final_update_id: 10,
            bids: Vec::new(),
            asks: Vec::new(),
        };
        let bridge = BinanceDepthDelta {
            event_time: 2,
            first_update_id: 9,
            final_update_id: 11,
            bids: vec![["100.0".into(), "0.0".into()]],
            asks: vec![["100.5".into(), "4.0".into()]],
        };

        assert!(!book.apply_delta(stale).expect("stale update"));
        assert!(book.apply_delta(bridge).expect("bridging update"));
        assert_eq!(book.best_prices(), (None, Some(100.5)));
    }

    #[test]
    fn stable_quantities_extend_a_single_run() {
        let mut cache = HeatmapCache::new(1.0);
        let first = cache.apply_levels("BTCUSDT", 100, vec![level(100, 10.0, true)], None, None);
        let second = cache.apply_levels("BTCUSDT", 200, vec![level(100, 11.0, true)], None, None);

        assert_eq!(first.started_runs.len(), 1);
        assert!(second.started_runs.is_empty());
        assert!(second.closed_runs.is_empty());
        let data = cache.data("BTCUSDT");
        assert_eq!(data.runs.len(), 1);
        assert_eq!(data.runs[0].end_time, 200);
        assert!(data.runs[0].active);
    }

    #[test]
    fn changed_and_removed_levels_close_runs() {
        let mut cache = HeatmapCache::new(1.0);
        cache.apply_levels("BTCUSDT", 100, vec![level(100, 10.0, true)], None, None);
        let changed = cache.apply_levels("BTCUSDT", 200, vec![level(100, 20.0, true)], None, None);
        let removed = cache.apply_levels("BTCUSDT", 300, Vec::new(), None, None);

        assert_eq!(changed.started_runs.len(), 1);
        assert_eq!(changed.closed_runs.len(), 1);
        assert_eq!(removed.closed_runs.len(), 1);
        assert!(cache.data("BTCUSDT").runs.iter().all(|run| !run.active));
    }

    #[test]
    fn automatic_tick_size_uses_nice_exchange_tick_multiple() {
        assert_eq!(choose_heatmap_tick_size(67_000.0, 0.01), 2.0);
        assert_eq!(choose_heatmap_tick_size(0.5, 0.0001), 0.0001);
    }
}

#[cfg(test)]
mod order_book_tests {
    use super::*;
    #[test]
    fn exact_snapshot_retains_small_changes_and_removes_zero_levels() {
        let mut book = LocalOrderBook::from_snapshot(
            BinanceDepthSnapshot {
                last_update_id: 10,
                bids: vec![["99.99".into(), "2".into()], ["99.98".into(), "3".into()]],
                asks: vec![["100.01".into(), "4".into()]],
            },
            0.01,
        )
        .unwrap();
        assert!(book
            .apply_delta(BinanceDepthDelta {
                event_time: 11,
                first_update_id: 11,
                final_update_id: 11,
                bids: vec![
                    ["99.99".into(), "2.01".into()],
                    ["99.98".into(), "0".into()]
                ],
                asks: vec![]
            })
            .unwrap());
        let snapshot = book.snapshot("TEST", 11);
        assert_eq!(snapshot.bids.len(), 1);
        assert!((snapshot.bids[0][0] - 99.99).abs() < 1e-9);
        assert_eq!(snapshot.bids[0][1], 2.01);
        assert!((snapshot.asks[0][0] - 100.01).abs() < 1e-9);
        assert_eq!(snapshot.asks[0][1], 4.0);
        assert!(book
            .apply_delta(BinanceDepthDelta {
                event_time: 13,
                first_update_id: 13,
                final_update_id: 13,
                bids: vec![],
                asks: vec![]
            })
            .is_err());
    }
    #[test]
    fn exact_snapshot_is_bounded_and_sorted_nearest_first() {
        let book = LocalOrderBook::from_snapshot(
            BinanceDepthSnapshot {
                last_update_id: 1,
                bids: (1..=200).map(|p| [p.to_string(), "1".into()]).collect(),
                asks: (201..=400).map(|p| [p.to_string(), "1".into()]).collect(),
            },
            1.0,
        )
        .unwrap();
        let snapshot = book.snapshot("TEST", 1);
        assert_eq!(snapshot.bids.len(), 100);
        assert_eq!(snapshot.asks.len(), 100);
        assert_eq!(snapshot.bids[0][0], 200.0);
        assert_eq!(snapshot.asks[0][0], 201.0);
    }
}
