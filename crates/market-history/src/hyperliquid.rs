//! Versioned, factual perpetual evidence. Quantities are base units; USD notionals
//! use Hyperliquid's mark price, never the Binance spot price. No signal scores.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Evidence {
    pub version: u32,
    pub source: String,
    pub market: String,
    pub coin: String,
    pub spot_symbol: String,
    pub server_time_ms: i64,
    pub started_ms: Option<i64>,
    pub enabled: bool,
    pub supported: bool,
    pub connected: bool,
    pub gap_count: i64,
    pub last_gap_ms: Option<i64>,
    pub error: Option<String>,
    pub mark_price: Option<f64>,
    pub open_interest: Option<f64>,
    pub funding_rate: Option<f64>,
    pub context_ms: Option<i64>,
    pub discovered_addresses: i64,
    pub sampled_addresses: i64,
    pub fresh_addresses: i64,
    pub stale_addresses: i64,
    pub oldest_snapshot_ms: Option<i64>,
    pub newest_snapshot_ms: Option<i64>,
    pub freshness_ms: i64,
    pub long_quantity: f64,
    pub short_quantity: f64,
    pub missing_entry_positions: usize,
    pub missing_liquidation_positions: usize,
    pub bucket_width: Option<f64>,
    pub entry_buckets: Vec<Bucket>,
    pub liquidation_buckets: Vec<Bucket>,
    pub largest_positions: Vec<Position>,
    #[serde(default)]
    pub positions: Vec<Position>,
    #[serde(default)]
    pub history: History,
    pub activity: Activity,
    #[serde(default)]
    pub selection: Selection,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Selection {
    pub version: u32,
    pub budget: i64,
    pub min_position_usd: f64,
    pub selected_addresses: i64,
    pub maker_excluded_addresses: i64,
    pub frequent_excluded_addresses: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Activity {
    pub start_ms: i64,
    pub end_ms: i64,
    pub trade_count: i64,
    pub buy_quantity: f64,
    pub sell_quantity: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub low: f64,
    pub high: f64,
    pub long_quantity: f64,
    pub short_quantity: f64,
    pub positions: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub address: String,
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub liquidation_price: Option<f64>,
    pub observed_ms: i64,
    pub exchange_ms: i64,
}

/// Exchange fills, retained separately from the public trade stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    #[serde(default)]
    pub address: String,
    pub coin: String,
    pub time: i64,
    pub tid: u64,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub start_position: String,
    pub closed_pnl: String,
    pub fee: String,
    pub fee_token: String,
    pub dir: String,
}
impl Fill {
    pub fn valid(&self) -> bool {
        let finite = |s: &str| s.parse::<f64>().is_ok_and(f64::is_finite);
        self.time > 0
            && !self.coin.is_empty()
            && matches!(self.side.as_str(), "B" | "A")
            && [
                &self.px,
                &self.sz,
                &self.start_position,
                &self.closed_pnl,
                &self.fee,
            ]
            .into_iter()
            .all(|s| finite(s))
            // Outcome settlement may have zero payout; ordinary executions require a price.
            && (self.px.parse::<f64>().unwrap() > 0.0
                || self.dir == "Settlement" && self.px.parse::<f64>().unwrap() == 0.0)
            && self.sz.parse::<f64>().unwrap() > 0.0
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct History {
    pub fills: Vec<Fill>,
    pub start_ms: i64,
    pub end_ms: i64,
    pub truncated: bool,
    pub sampled_addresses: usize,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub struct ClosedTrade {
    pub address: String,
    pub time: i64,
    pub pnl: f64,
}

/// Witnessed opening of each current position cycle, aligned to its snapshot.
/// Adds/partial closes preserve the opening; reversals start a new cycle.
/// Missing openings, discontinuities or quantity mismatches remain unknown.
pub fn position_entry_times(data: &Evidence) -> std::collections::BTreeMap<&str, i64> {
    use std::collections::{BTreeMap, HashSet};
    let positions = if data.positions.is_empty() {
        &data.largest_positions
    } else {
        &data.positions
    };
    let positions: BTreeMap<_, _> = positions.iter().map(|p| (p.address.as_str(), p)).collect();
    let mut fills: Vec<_> = data
        .history
        .fills
        .iter()
        .filter(|f| {
            f.coin == data.coin
                && f.valid()
                && positions
                    .get(f.address.as_str())
                    .is_some_and(|p| f.time <= p.exchange_ms)
        })
        .collect();
    fills.sort_by_key(|f| (f.time, f.tid));
    let mut seen = HashSet::new();
    let mut cycles = BTreeMap::<&str, (f64, Option<i64>)>::new();
    for f in fills {
        if !seen.insert((f.address.as_str(), f.tid)) {
            continue;
        }
        let start = f.start_position.parse::<f64>().unwrap();
        let size = f.sz.parse::<f64>().unwrap();
        let end = start + if f.side == "B" { size } else { -size };
        let eps = 1e-8_f64.max(size * 1e-8);
        let cycle = cycles.entry(&f.address).or_insert((start, None));
        if (cycle.0 - start).abs() > eps {
            cycle.1 = None;
        }
        if end.abs() < eps {
            cycle.1 = None;
        } else if start.abs() < eps || start * end < 0.0 {
            cycle.1 = Some(f.time);
        }
        cycle.0 = end;
    }
    cycles
        .into_iter()
        .filter_map(|(address, (quantity, opened))| {
            let p = positions[address];
            let eps = 1e-8_f64.max(p.quantity.abs() * 1e-8);
            ((p.quantity - quantity).abs() <= eps)
                .then_some(opened)
                .flatten()
                .map(|t| (address, t))
        })
        .collect()
}

/// Only flat-to-flat (or reversal) cycles with a witnessed opening qualify.
/// Partial closes accumulate; missing fills invalidate the current cycle.
/// Net USD results require USDC fees. Funding is outside this statistic.
pub fn closed_trades(fills: &[Fill]) -> Vec<ClosedTrade> {
    use std::collections::{BTreeMap, HashSet};
    let mut sorted: Vec<_> = fills.iter().filter(|f| f.valid()).collect();
    sorted.sort_by_key(|f| (f.time, f.tid));
    let mut seen = HashSet::new();
    let mut cycles = BTreeMap::<(&str, &str), (f64, f64, bool)>::new();
    let mut result = Vec::new();
    for f in sorted {
        if !seen.insert((&f.address, &f.coin, f.tid)) {
            continue;
        }
        let start = f.start_position.parse::<f64>().unwrap();
        let size = f.sz.parse::<f64>().unwrap();
        let signed = if f.side == "B" { size } else { -size };
        let end = start + signed;
        let eps = 1e-8_f64.max(size * 1e-8);
        let cycle = cycles
            .entry((&f.address, &f.coin))
            .or_insert((start, 0.0, false));
        if (cycle.0 - start).abs() > eps {
            *cycle = (start, 0.0, false);
        }
        if start.abs() < eps {
            *cycle = (start, 0.0, true);
        }
        cycle.2 &= f.fee_token == "USDC";
        let reversal = start * end < 0.0;
        let close_fee = if reversal {
            f.fee.parse::<f64>().unwrap() * start.abs() / size
        } else {
            f.fee.parse::<f64>().unwrap()
        };
        cycle.1 += f.closed_pnl.parse::<f64>().unwrap() - close_fee;
        if start.abs() >= eps && (end.abs() < eps || reversal) {
            if cycle.2 {
                result.push(ClosedTrade {
                    address: f.address.clone(),
                    time: f.time,
                    pnl: cycle.1,
                });
            }
            *cycle = (
                end,
                if reversal {
                    -(f.fee.parse::<f64>().unwrap() - close_fee)
                } else {
                    0.0
                },
                reversal && f.fee_token == "USDC",
            );
        } else {
            cycle.0 = end;
        }
    }
    result
}

#[cfg(test)]
mod history_tests {
    use super::*;
    fn fill(tid: u64, start: f64, size: f64, side: &str, pnl: f64) -> Fill {
        Fill {
            address: "a".into(),
            coin: "BTC".into(),
            time: tid as i64,
            tid,
            side: side.into(),
            px: "100".into(),
            sz: size.to_string(),
            start_position: start.to_string(),
            closed_pnl: pnl.to_string(),
            fee: "1".into(),
            fee_token: "USDC".into(),
            dir: "".into(),
        }
    }
    fn current(quantity: f64, snapshot: i64, fills: Vec<Fill>) -> Evidence {
        Evidence {
            coin: "BTC".into(),
            positions: vec![Position {
                address: "a".into(),
                quantity,
                entry_price: Some(100.0),
                liquidation_price: None,
                observed_ms: snapshot,
                exchange_ms: snapshot,
            }],
            history: History {
                fills,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    #[test]
    fn entry_time_survives_adds_partial_closes_and_resets_on_reversal() {
        let mut data = current(
            2.0,
            10,
            vec![
                fill(1, 0.0, 2.0, "B", 0.0),
                fill(2, 2.0, 1.0, "B", 0.0),
                fill(3, 3.0, 1.0, "A", 5.0),
            ],
        );
        data.history.fills.push(data.history.fills[1].clone());
        data.history.fills.reverse();
        assert_eq!(position_entry_times(&data).get("a"), Some(&1));
        data.history.fills.push(fill(4, 2.0, 3.0, "A", 10.0));
        data.positions[0].quantity = -1.0;
        assert_eq!(position_entry_times(&data).get("a"), Some(&4));
        data.history.fills.push(fill(5, -1.0, 1.0, "B", 0.0));
        assert!(position_entry_times(&data).is_empty());
    }
    #[test]
    fn entry_time_requires_matching_snapshot_and_witnessed_continuous_history() {
        assert!(
            position_entry_times(&current(1.0, 10, vec![fill(1, 2.0, 1.0, "A", 0.0)])).is_empty()
        );
        assert!(position_entry_times(&current(
            1.0,
            10,
            vec![fill(1, 0.0, 2.0, "B", 0.0), fill(3, 3.0, 2.0, "A", 0.0)]
        ))
        .is_empty());
        assert!(
            position_entry_times(&current(3.0, 10, vec![fill(1, 0.0, 2.0, "B", 0.0)])).is_empty()
        );
        let data = current(
            2.0,
            2,
            vec![
                fill(1, 0.0, 2.0, "B", 0.0),
                fill(3, 2.0, 2.0, "A", 0.0),
                fill(4, 0.0, 2.0, "B", 0.0),
            ],
        );
        assert_eq!(position_entry_times(&data).get("a"), Some(&1));
    }
    #[test]
    fn partial_closes_are_one_trade_with_all_fees_and_deduplication() {
        let mut fills = vec![
            fill(1, 0.0, 2.0, "B", 0.0),
            fill(2, 2.0, 1.0, "A", 10.0),
            fill(3, 1.0, 1.0, "A", -2.0),
        ];
        fills.push(fills[1].clone());
        fills.reverse();
        let trades = closed_trades(&fills);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].pnl, 5.0);
    }
    #[test]
    fn reversals_split_fees_and_start_new_cycles() {
        let trades = closed_trades(&[
            fill(1, 0.0, 1.0, "B", 0.0),
            fill(2, 1.0, 2.0, "A", 10.0),
            fill(3, -1.0, 1.0, "B", -5.0),
        ]);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].pnl, 8.5);
        assert_eq!(trades[1].pnl, -6.5);
    }
    #[test]
    fn missing_openings_gaps_open_cycles_and_non_usdc_fees_do_not_count() {
        assert!(closed_trades(&[fill(1, 1.0, 1.0, "A", 10.0)]).is_empty());
        assert!(
            closed_trades(&[fill(1, 0.0, 2.0, "B", 0.0), fill(3, 1.0, 1.0, "A", 10.0)]).is_empty()
        );
        assert!(closed_trades(&[fill(1, 0.0, 2.0, "B", 0.0)]).is_empty());
        let mut open = fill(1, 0.0, 1.0, "B", 0.0);
        open.fee_token = "HYPE".into();
        assert!(closed_trades(&[open, fill(2, 1.0, 1.0, "A", 10.0)]).is_empty());
    }
}

#[cfg(test)]
mod settlement_tests {
    use super::*;
    #[test]
    fn zero_price_settlement_does_not_reject_the_whole_address_history() {
        // Captured exchange settlement: losing outcome contracts settle at zero.
        let mut fill: Fill = serde_json::from_value(serde_json::json!({
            "coin":"#68320","px":"0.0","sz":"102.0","side":"A",
            "time":1791063726267_i64,"startPosition":"102.0","dir":"Settlement",
            "closedPnl":"-19.80636","fee":"0.0","tid":541992059932418_u64,"feeToken":"USDC"
        }))
        .unwrap();
        assert!(fill.valid());
        fill.dir = "Close Long".into();
        assert!(!fill.valid());
        fill.dir = "Settlement".into();
        fill.px = "-1".into();
        assert!(!fill.valid());
    }
}
