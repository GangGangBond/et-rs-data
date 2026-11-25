//! Transparent sample selection from public executions and confirmed snapshots.
//! High frequency alone is insufficient: split fills are common for large orders.
use crate::{now_ms, store::Store};
use anyhow::Result;
use serde::Serialize;
use sqlx::{types::Json, Row};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Features {
    pub trades: i64,
    pub maker_trades: i64,
    pub active_seconds: i64,
    pub active_minutes: i64,
    pub span_ms: i64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub position_notional: f64,
    pub snapshot_known: bool,
    pub closed_trades: i64,
    pub winning_trades: i64,
}
impl Features {
    fn behavior_pattern(&self) -> Option<&'static str> {
        let turnover = self.buy_notional + self.sell_notional;
        if turnover <= 0.0 || self.span_ms < 300_000 || self.active_minutes < 5 {
            return None;
        }
        let buy_share = self.buy_notional / turnover;
        let balanced = (0.35..=0.65).contains(&buy_share);
        // Require a known low-inventory/high-turnover state, not just a burst of
        // executions. A single directional sweep is not classified as HFT/MM.
        if balanced {
            if self.trades >= 300
                && self.active_seconds >= 120
                && self.maker_trades as f64 / self.trades as f64 >= 0.8
            {
                return Some("maker_two_sided_high_turnover");
            }
            if self.trades >= 600 && self.active_seconds >= 300 {
                return Some("frequent_two_sided_high_turnover");
            }
        }
        None
    }

    pub fn excluded_reason(&self) -> Option<&'static str> {
        let turnover = self.buy_notional + self.sell_notional;
        if !self.snapshot_known || turnover < 5.0 * self.position_notional.max(1_000.0) {
            return None;
        }
        self.behavior_pattern()
    }
}

#[derive(Clone)]
struct Holding {
    address: String,
    coin: String,
    notional: f64,
    attempted_ms: Option<i64>,
    performance: f64,
}

/// Equal per-coin quotas, then round-robin rank filling. An address observed in
/// several coins consumes one slot, so duplicates free slots for other accounts.
fn choose(holdings: &[Holding], coins: &[String], limit: usize) -> BTreeSet<String> {
    if limit == 0 {
        return BTreeSet::new();
    }
    let mut ranks: BTreeMap<&str, Vec<&Holding>> =
        coins.iter().map(|c| (c.as_str(), Vec::new())).collect();
    for h in holdings {
        if let Some(items) = ranks.get_mut(h.coin.as_str()) {
            items.push(h);
        }
    }
    for items in ranks.values_mut() {
        items.sort_by(|a, b| {
            b.performance
                .total_cmp(&a.performance)
                .then(b.notional.total_cmp(&a.notional))
                .then(a.attempted_ms.cmp(&b.attempted_ms))
                .then(a.address.cmp(&b.address))
        });
    }
    let mut selected = BTreeSet::new();
    let max_rank = ranks.values().map(Vec::len).max().unwrap_or(0);
    for rank in 0..max_rank {
        for items in ranks.values() {
            if let Some(h) = items.get(rank) {
                selected.insert(h.address.clone());
            }
            if selected.len() >= limit {
                return selected;
            }
        }
    }
    selected
}

pub async fn refresh(store: &Store, max_addresses: i64, min_notional: f64) -> Result<()> {
    let now = now_ms() as i64;
    let mut tx = store.pool.begin().await?;
    sqlx::query("UPDATE hl_status SET min_position_usd=$1,sample_budget=$2 WHERE id=1")
        .bind(min_notional)
        .bind(max_addresses)
        .execute(&mut *tx)
        .await?;
    let coins: Vec<String> =
        sqlx::query_scalar("SELECT coin FROM hl_symbols WHERE enabled ORDER BY coin")
            .fetch_all(&mut *tx)
            .await?;
    let rows=sqlx::query("SELECT a.address,a.snapshot_ms,a.attempted_ms,a.last_seen_ms,COALESCE(sum(abs(p.quantity)*COALESCE((s.context->>'markPx')::numeric,p.entry_price,0)),0)::float8 AS position_notional FROM hl_addresses a JOIN hl_address_coins c USING(address) JOIN hl_symbols s USING(coin) LEFT JOIN hl_positions p ON p.address=a.address AND p.coin=c.coin WHERE s.enabled GROUP BY a.address").fetch_all(&mut *tx).await?;
    let stats=sqlx::query("SELECT address,sum(trades)::bigint AS trades,sum(maker_trades)::bigint AS makers,sum(bit_count(active_seconds::bit(64)))::bigint AS seconds,count(*) AS minutes,(max(last_ms)-min(first_ms))::bigint AS span,sum(buy_notional)::float8 AS buys,sum(sell_notional)::float8 AS sells FROM hl_address_activity_1m WHERE bucket_ms >= $1 GROUP BY address").bind(now-3_600_000).fetch_all(&mut *tx).await?;
    let mut features = HashMap::new();
    for row in stats {
        features.insert(
            row.get::<String, _>("address"),
            Features {
                trades: row.get("trades"),
                maker_trades: row.get("makers"),
                active_seconds: row.get("seconds"),
                active_minutes: row.get("minutes"),
                span_ms: row.get("span"),
                buy_notional: row.get("buys"),
                sell_notional: row.get("sells"),
                ..Features::default()
            },
        );
    }
    let mut accounts = HashMap::new();
    let mut performance = HashMap::new();
    for row in sqlx::query("SELECT c.address,count(*) AS closed,count(*) FILTER(WHERE c.pnl > 0) AS wins FROM hl_closed_cycles c JOIN hl_fill_sync f USING(address) WHERE c.time_ms >= $1 AND f.error IS NULL AND f.next_ms >= f.updated_ms GROUP BY c.address")
        .bind(now - 30 * 86_400_000).fetch_all(&mut *tx).await? {
        performance.insert(row.get::<String,_>("address"), (row.get::<i64,_>("wins"), row.get::<i64,_>("closed")));
    }
    let mut excluded = BTreeSet::new();
    for row in rows {
        let address: String = row.get("address");
        let f = features.entry(address.clone()).or_default();
        (f.winning_trades, f.closed_trades) =
            performance.get(&address).copied().unwrap_or_default();
        f.position_notional = row.get("position_notional");
        f.snapshot_known = row
            .get::<Option<i64>, _>("snapshot_ms")
            .is_some_and(|t| t >= now - 900_000);
        let reason = f.excluded_reason();
        // Retain a recorded exclusion during insufficient observations. It can
        // be reconsidered when a new substantial observation window is present.
        sqlx::query("INSERT INTO hl_address_selection(address,excluded_reason,evidence,evaluated_ms) VALUES($1,$2,$3,$4) ON CONFLICT(address) DO UPDATE SET excluded_reason=CASE WHEN $2 IS NOT NULL THEN $2 WHEN $5 >= 100 AND $6 >= 300000 AND $7 THEN NULL ELSE hl_address_selection.excluded_reason END,evidence=excluded.evidence,evaluated_ms=excluded.evaluated_ms")
            .bind(&address).bind(reason).bind(Json(&f)).bind(now).bind(f.trades).bind(f.span_ms).bind(f.snapshot_known).execute(&mut *tx).await?;
        accounts.insert(
            address,
            (
                row.get::<Option<i64>, _>("attempted_ms"),
                f.snapshot_known,
                row.get::<i64, _>("last_seen_ms"),
            ),
        );
    }
    for user in sqlx::query_scalar::<_, String>(
        "SELECT address FROM hl_address_selection WHERE excluded_reason IS NOT NULL",
    )
    .fetch_all(&mut *tx)
    .await?
    {
        excluded.insert(user);
    }
    let mut holdings = Vec::new();
    for row in sqlx::query("SELECT p.address,p.coin,(abs(p.quantity)*COALESCE((s.context->>'markPx')::numeric,p.entry_price,0))::float8 AS value FROM hl_positions p JOIN hl_symbols s USING(coin) WHERE s.enabled").fetch_all(&mut *tx).await?{
        let user:String=row.get("address");let value:f64=row.get("value");
        if !excluded.contains(&user) && value>=min_notional && value.is_finite(){
            let (wins, closed) = performance.get(&user).copied().unwrap_or_default();
            holdings.push(Holding{attempted_ms:accounts.get(&user).and_then(|a|a.0),performance:performance_rank(wins, closed),address:user,coin:row.get("coin"),notional:value});
        }
    }
    // Reserve ~20% of the request pool for exploration instead of repeatedly
    // querying only incumbents. Known positions remain eligible when inactive.
    let limit = max_addresses as usize;
    let discovery_slots = if limit > 1 { (limit / 5).max(1) } else { 0 };
    let mut selected = choose(&holdings, &coins, limit - discovery_slots);
    let mut discovery: Vec<_> = accounts
        .iter()
        .filter(|(a, _)| !excluded.contains(*a) && !selected.contains(*a))
        .map(|(a, v)| (a.clone(), *v))
        .collect();
    // Never sampled first; oldest attempted next. Require recent activity only
    // for exploration of flat/unknown accounts, not confirmed holding accounts.
    let holders: BTreeSet<_> = holdings.iter().map(|h| h.address.as_str()).collect();
    discovery.retain(|(user, v)| v.2 >= now - 86_400_000 || holders.contains(user.as_str()));
    discovery.sort_by(|a, b| {
        a.1 .0
            .cmp(&b.1 .0)
            .then(b.1 .2.cmp(&a.1 .2))
            .then(a.0.cmp(&b.0))
    });
    // Half the exploration slots verify sustained two-sided candidates whose
    // inventory is still unknown/stale. Otherwise those accounts can sit behind
    // thousands of new addresses and never have enough evidence to classify.
    let assessment_slots = (discovery_slots / 2).max(1).min(limit - selected.len());
    for (user, _) in discovery
        .iter()
        .filter(|(user, _)| {
            features
                .get(user)
                .is_some_and(|f| !f.snapshot_known && f.behavior_pattern().is_some())
        })
        .take(assessment_slots)
    {
        selected.insert(user.clone());
    }
    for (user, _) in discovery {
        if selected.len() >= limit {
            break;
        }
        selected.insert(user);
    }
    sqlx::query("UPDATE hl_address_selection SET selected=false WHERE selected")
        .execute(&mut *tx)
        .await?;
    let selected: Vec<_> = selected.into_iter().collect();
    sqlx::query("UPDATE hl_address_selection SET selected=true WHERE address=ANY($1)")
        .bind(&selected)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    tracing::info!(
        selected = selected.len(),
        excluded = excluded.len(),
        min_notional,
        "Hyperliquid position sample refreshed"
    );
    Ok(())
}

/// Conservative 95% Wilson lower bound ranks observed win rates with sample size.
/// This is a sampling priority, not a forecast of the address's next trade.
fn performance_rank(wins: i64, closed: i64) -> f64 {
    if closed <= 0 {
        return 0.0;
    }
    let n = closed as f64;
    let p = wins.clamp(0, closed) as f64 / n;
    let z2 = 1.96_f64.powi(2);
    ((p + z2 / (2.0 * n) - 1.96 * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt()) / (1.0 + z2 / n))
        .max(0.0)
}

/// Rebuild one synced account's derived cycles, in the same transaction as fills.
pub async fn refresh_performance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    user: &str,
    now: i64,
) -> Result<()> {
    let rows = sqlx::query("SELECT f.fill FROM hl_fills f JOIN hl_symbols s USING(coin) WHERE f.address=$1 AND f.time_ms >= $2 AND s.enabled ORDER BY f.time_ms,f.tid,f.coin")
        .bind(user).bind(now - 30 * 86_400_000).fetch_all(&mut **tx).await?;
    let fills: Vec<market_history::hyperliquid::Fill> = rows
        .into_iter()
        .map(|r| serde_json::from_value(r.get::<Json<serde_json::Value>, _>("fill").0))
        .collect::<std::result::Result<_, _>>()?;
    let cycles = market_history::hyperliquid::closed_trades(&fills);
    sqlx::query("DELETE FROM hl_closed_cycles WHERE address=$1")
        .bind(user)
        .execute(&mut **tx)
        .await?;
    for (batch_index, batch) in cycles.chunks(500).enumerate() {
        let mut query = sqlx::QueryBuilder::new(
            "INSERT INTO hl_closed_cycles(address,cycle_index,time_ms,pnl) ",
        );
        query.push_values(batch.iter().enumerate(), |mut b, (index, c)| {
            b.push_bind(user)
                .push_bind((batch_index * 500 + index) as i64)
                .push_bind(c.time)
                .push_bind(c.pnl);
        });
        query.build().execute(&mut **tx).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn win_rate_priority_accounts_for_completed_sample_size() {
        assert!(performance_rank(20, 30) > performance_rank(1, 1));
        assert!(performance_rank(70, 100) > performance_rank(7, 10));
        assert_eq!(performance_rank(0, 0), 0.0);
        assert_eq!(performance_rank(0, 100), 0.0);
        assert!(performance_rank(70, 100) > performance_rank(50, 100));
    }
    fn busy() -> Features {
        Features {
            trades: 1000,
            maker_trades: 900,
            active_seconds: 500,
            active_minutes: 20,
            span_ms: 1_200_000,
            buy_notional: 100_000.0,
            sell_notional: 100_000.0,
            position_notional: 10_000.0,
            snapshot_known: true,
            ..Default::default()
        }
    }
    #[test]
    fn exclusion_requires_sustained_two_sided_turnover_not_large_split_orders() {
        assert_eq!(
            busy().excluded_reason(),
            Some("maker_two_sided_high_turnover")
        );
        let mut f = busy();
        f.maker_trades = 200;
        assert_eq!(
            f.excluded_reason(),
            Some("frequent_two_sided_high_turnover")
        );
        f.buy_notional = 199_000.0;
        f.sell_notional = 1_000.0;
        assert_eq!(f.excluded_reason(), None);
        f = busy();
        f.active_seconds = 2;
        assert_eq!(f.excluded_reason(), None);
        f = busy();
        f.snapshot_known = false;
        assert_eq!(f.excluded_reason(), None);
        assert!(
            f.behavior_pattern().is_some(),
            "unknown inventory requires assessment, not automatic exclusion"
        );
        f = busy();
        f.position_notional = 100_000.0;
        assert_eq!(f.excluded_reason(), None);
    }
    #[test]
    fn per_coin_sampling_preserves_holdings_and_deduplicates_multicoin_accounts() {
        let h = |address: &str, coin: &str, notional| Holding {
            address: address.into(),
            coin: coin.into(),
            notional,
            attempted_ms: None,
            performance: 0.0,
        };
        let holdings = vec![
            h("large", "BTC", 1000.0),
            h("large", "ETH", 2000.0),
            h("btc2", "BTC", 500.0),
            h("eth2", "ETH", 200.0),
            h("smallcoin", "SOL", 1.0),
        ];
        let selected = choose(&holdings, &["BTC".into(), "ETH".into(), "SOL".into()], 3);
        assert_eq!(
            selected,
            BTreeSet::from(["large".into(), "smallcoin".into(), "btc2".into()])
        );
    }
}
