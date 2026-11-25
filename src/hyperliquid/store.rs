use super::model::{address, decimal, positive_optional, AccountState, Trade};
use crate::{now_ms, store::Store};
use anyhow::{ensure, Result};
use market_history::{
    hyperliquid::{Activity, Bucket, Evidence, Position, Selection},
    Decimal,
};
use serde_json::Value;
use sqlx::{types::Json, QueryBuilder, Row};
use std::collections::{BTreeMap, BTreeSet};

pub async fn save_trades(store: &Store, trades: &[Trade]) -> Result<()> {
    if trades.is_empty() {
        return Ok(());
    }
    let now = now_ms();
    for trade in trades {
        trade.validate(now)?;
    }
    let mut tx = store.pool.begin().await?;
    for chunk in trades.chunks(500) {
        let mut users = BTreeMap::<String, (i64, i64)>::new();
        let mut associations = BTreeSet::new();
        for t in chunk {
            for user in &t.users {
                let user = address(user)?;
                users
                    .entry(user.clone())
                    .and_modify(|(first, last)| {
                        *first = (*first).min(t.time as i64);
                        *last = (*last).max(t.time as i64);
                    })
                    .or_insert((t.time as i64, t.time as i64));
                associations.insert((user, t.coin.clone()));
            }
        }
        let mut addresses =
            QueryBuilder::new("INSERT INTO hl_addresses(address,first_seen_ms,last_seen_ms) ");
        addresses.push_values(&users, |mut b, (user, (first, last))| {
            b.push_bind(user).push_bind(first).push_bind(last);
        });
        addresses.push(" ON CONFLICT(address) DO UPDATE SET first_seen_ms=LEAST(hl_addresses.first_seen_ms,excluded.first_seen_ms),last_seen_ms=GREATEST(hl_addresses.last_seen_ms,excluded.last_seen_ms)").build().execute(&mut *tx).await?;
        let mut coins = QueryBuilder::new("INSERT INTO hl_address_coins(address,coin) ");
        coins.push_values(&associations, |mut b, (user, coin)| {
            b.push_bind(user).push_bind(coin);
        });
        coins
            .push(" ON CONFLICT DO NOTHING")
            .build()
            .execute(&mut *tx)
            .await?;
        // tid is not sequential. Identity is (block time, coin, tid), not hash.
        let mut insert = QueryBuilder::new(
            "INSERT INTO hl_trades(coin,time_ms,tid,side,price,quantity,buyer,seller) ",
        );
        insert.push_values(chunk, |mut b, t| {
            b.push_bind(&t.coin)
                .push_bind(t.time as i64)
                .push_bind(t.tid as i64)
                .push_bind(&t.side);
            b.push_bind(&t.px).push_unseparated("::text::numeric");
            b.push_bind(&t.sz).push_unseparated("::text::numeric");
            b.push_bind(t.users[0].to_ascii_lowercase())
                .push_bind(t.users[1].to_ascii_lowercase());
        });
        let inserted=insert.push(" ON CONFLICT DO NOTHING RETURNING time_ms,side,price::text,quantity::text,buyer,seller").build().fetch_all(&mut *tx).await?;
        // Only newly inserted executions contribute to behavior statistics.
        // A burst of split fills counts as one active second, avoiding a pure
        // fill-count proxy for high frequency.
        let mut activity =
            BTreeMap::<(String, i64), (i64, i64, Decimal, Decimal, i64, i64, i64)>::new();
        for row in inserted {
            let time: i64 = row.get("time_ms");
            let side: String = row.get("side");
            let value = decimal(&row.get::<String, _>("price"))?
                * decimal(&row.get::<String, _>("quantity"))?;
            for (user, buy) in [
                (row.get::<String, _>("buyer"), true),
                (row.get::<String, _>("seller"), false),
            ] {
                let a = activity.entry((user, time / 60000 * 60000)).or_insert((
                    0,
                    0,
                    Decimal::ZERO,
                    Decimal::ZERO,
                    0,
                    time,
                    time,
                ));
                a.0 += 1;
                a.1 += i64::from((buy && side == "A") || (!buy && side == "B"));
                if buy {
                    a.2 += value;
                } else {
                    a.3 += value;
                }
                a.4 |= 1_i64 << ((time / 1000) % 60);
                a.5 = a.5.min(time);
                a.6 = a.6.max(time);
            }
        }
        if !activity.is_empty() {
            let mut update=QueryBuilder::new("INSERT INTO hl_address_activity_1m(address,bucket_ms,trades,maker_trades,buy_notional,sell_notional,active_seconds,first_ms,last_ms) ");
            update.push_values(
                &activity,
                |mut b, ((user, bucket), (trades, makers, buy, sell, seconds, first, last))| {
                    b.push_bind(user)
                        .push_bind(bucket)
                        .push_bind(trades)
                        .push_bind(makers);
                    b.push_bind(buy.to_string())
                        .push_unseparated("::text::numeric");
                    b.push_bind(sell.to_string())
                        .push_unseparated("::text::numeric");
                    b.push_bind(seconds).push_bind(first).push_bind(last);
                },
            );
            update.push(" ON CONFLICT(address,bucket_ms) DO UPDATE SET trades=hl_address_activity_1m.trades+excluded.trades,maker_trades=hl_address_activity_1m.maker_trades+excluded.maker_trades,buy_notional=hl_address_activity_1m.buy_notional+excluded.buy_notional,sell_notional=hl_address_activity_1m.sell_notional+excluded.sell_notional,active_seconds=hl_address_activity_1m.active_seconds | excluded.active_seconds,first_ms=LEAST(hl_address_activity_1m.first_ms,excluded.first_ms),last_ms=GREATEST(hl_address_activity_1m.last_ms,excluded.last_ms)").build().execute(&mut *tx).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

pub async fn save_snapshot(store: &Store, user: &str, raw: Value, observed: u64) -> Result<()> {
    let user = address(user)?;
    let state: AccountState = serde_json::from_value(raw.clone())?;
    state.validate(observed)?;
    let mut tx = store.pool.begin().await?;
    let previous =
        sqlx::query("SELECT snapshot_ms,exchange_ms FROM hl_addresses WHERE address=$1 FOR UPDATE")
            .bind(&user)
            .fetch_one(&mut *tx)
            .await?;
    if previous
        .get::<Option<i64>, _>("snapshot_ms")
        .is_some_and(|t| t > observed as i64)
        || previous
            .get::<Option<i64>, _>("exchange_ms")
            .is_some_and(|t| t > state.time as i64)
    {
        return Ok(());
    }
    // A flat account must remove old positions atomically, not leave ghost liquidity.
    sqlx::query("DELETE FROM hl_positions WHERE address=$1")
        .bind(&user)
        .execute(&mut *tx)
        .await?;
    for item in state.asset_positions {
        let p = item.position;
        let quantity = decimal(&p.szi)?;
        if quantity == Decimal::ZERO {
            continue;
        }
        let entry = positive_optional(&p.entry_px)?.map(|d| d.to_string());
        let liquidation = positive_optional(&p.liquidation_px)?.map(|d| d.to_string());
        sqlx::query("INSERT INTO hl_positions(address,coin,quantity,entry_price,liquidation_price,margin_used,leverage,exchange_ms,observed_ms) VALUES($1,$2,$3::text::numeric,$4::text::numeric,$5::text::numeric,$6::text::numeric,$7,$8,$9)")
            .bind(&user).bind(&p.coin).bind(quantity.to_string()).bind(entry).bind(liquidation).bind(&p.margin_used).bind(Json(p.leverage)).bind(state.time as i64).bind(observed as i64).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO hl_address_coins(address,coin) VALUES($1,$2) ON CONFLICT DO NOTHING",
        )
        .bind(&user)
        .bind(&p.coin)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("INSERT INTO hl_snapshots(address,observed_ms,exchange_ms,state) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING").bind(&user).bind(observed as i64).bind(state.time as i64).bind(Json(raw)).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE hl_addresses SET snapshot_ms=$2,exchange_ms=$3,error=NULL WHERE address=$1",
    )
    .bind(&user)
    .bind(observed as i64)
    .bind(state.time as i64)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

fn number(value: &str) -> Result<f64> {
    let n: f64 = value.parse()?;
    ensure!(n.is_finite(), "non-finite numeric evidence");
    Ok(n)
}
fn context_number(value: &Value, key: &str) -> Option<f64> {
    value
        .get(key)?
        .as_str()?
        .parse::<f64>()
        .ok()
        .filter(|n| n.is_finite())
}

pub fn distribution(positions: &[Position], step: f64, liquidation: bool) -> Vec<Bucket> {
    if !step.is_finite() || step <= 0.0 {
        return Vec::new();
    }
    let mut buckets = BTreeMap::<i64, Bucket>::new();
    for p in positions {
        let price = if liquidation {
            p.liquidation_price
        } else {
            p.entry_price
        };
        let Some(price) = price.filter(|p| p.is_finite() && *p > 0.0) else {
            continue;
        };
        let key = (price / step).floor() as i64;
        let b = buckets.entry(key).or_insert_with(|| Bucket {
            low: key as f64 * step,
            high: (key + 1) as f64 * step,
            ..Bucket::default()
        });
        if p.quantity > 0.0 {
            b.long_quantity += p.quantity;
        } else {
            b.short_quantity += -p.quantity;
        }
        b.positions += 1;
    }
    buckets.into_values().collect()
}

/// One consistent read of the selected accounts' last confirmed positions.
/// Observation age is coverage metadata, never an implicit position closure.
pub async fn evidence(store: &Store, symbol: &str) -> Result<Evidence> {
    evidence_with_details(store, symbol, true).await
}

pub async fn evidence_with_details(store: &Store, symbol: &str, details: bool) -> Result<Evidence> {
    let now = now_ms() as i64;
    let freshness = 900_000;
    let mut tx = store.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let status = sqlx::query("SELECT * FROM hl_status WHERE id=1")
        .fetch_one(&mut *tx)
        .await?;
    let mut out = Evidence {
        version: 1,
        source: "Hyperliquid".into(),
        market: "perpetual".into(),
        spot_symbol: symbol.into(),
        server_time_ms: now,
        started_ms: status.get("started_ms"),
        enabled: status.get("enabled"),
        connected: status.get("connected"),
        gap_count: status.get("gap_count"),
        last_gap_ms: status.get("last_gap_ms"),
        error: status.get("error"),
        freshness_ms: freshness,
        activity: Activity {
            start_ms: now - 900_000,
            end_ms: now,
            ..Activity::default()
        },
        selection: Selection {
            version: 1,
            budget: status.get("sample_budget"),
            min_position_usd: status.get("min_position_usd"),
            ..Selection::default()
        },
        ..Evidence::default()
    };
    let Some(meta) = sqlx::query(
        "SELECT coin,context,context_ms FROM hl_symbols WHERE spot_symbol=$1 AND enabled",
    )
    .bind(symbol)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(out);
    };
    out.supported = true;
    out.coin = meta.get("coin");
    out.context_ms = meta.get("context_ms");
    let context = meta.get::<Option<Json<Value>>, _>("context");
    let last_mark = context
        .as_ref()
        .and_then(|c| context_number(&c.0, "markPx"))
        .filter(|n| *n > 0.0);
    if out.context_ms.is_some_and(|t| now - t <= 180_000) {
        if let Some(Json(context)) = &context {
            out.mark_price = context_number(&context, "markPx").filter(|n| *n > 0.0);
            out.open_interest = context_number(&context, "openInterest").filter(|n| *n >= 0.0);
            out.funding_rate = context_number(&context, "funding");
        }
    }
    let selected_coverage=sqlx::query("SELECT count(*) FILTER(WHERE sel.selected AND sel.excluded_reason IS NULL) AS selected,count(*) FILTER(WHERE sel.excluded_reason='maker_two_sided_high_turnover') AS makers,count(*) FILTER(WHERE sel.excluded_reason='frequent_two_sided_high_turnover') AS frequent FROM hl_address_coins c JOIN hl_address_selection sel USING(address) WHERE c.coin=$1").bind(&out.coin).fetch_one(&mut *tx).await?;
    out.selection.selected_addresses = selected_coverage.get("selected");
    out.selection.maker_excluded_addresses = selected_coverage.get("makers");
    out.selection.frequent_excluded_addresses = selected_coverage.get("frequent");
    let coverage = sqlx::query("SELECT count(*) AS discovered,count(a.snapshot_ms) AS sampled,count(*) FILTER(WHERE sel.selected AND sel.excluded_reason IS NULL AND a.snapshot_ms >= $2 AND a.exchange_ms >= $2) AS fresh,count(*) FILTER(WHERE sel.selected AND sel.excluded_reason IS NULL AND a.snapshot_ms IS NOT NULL AND (a.snapshot_ms < $2 OR a.exchange_ms IS NULL OR a.exchange_ms < $2)) AS stale,min(a.snapshot_ms) FILTER(WHERE sel.selected AND sel.excluded_reason IS NULL) AS oldest,max(a.snapshot_ms) FILTER(WHERE sel.selected AND sel.excluded_reason IS NULL) AS newest FROM hl_address_coins c JOIN hl_addresses a USING(address) LEFT JOIN hl_address_selection sel USING(address) WHERE c.coin=$1")
        .bind(&out.coin).bind(now-freshness).fetch_one(&mut *tx).await?;
    out.discovered_addresses = coverage.get("discovered");
    out.sampled_addresses = coverage.get("sampled");
    out.fresh_addresses = coverage.get("fresh");
    out.stale_addresses = coverage.get("stale");
    out.oldest_snapshot_ms = coverage.get("oldest");
    out.newest_snapshot_ms = coverage.get("newest");
    let mut positions = Vec::new();
    if out.enabled {
        for row in sqlx::query("SELECT p.address,p.quantity::text,p.entry_price::text,p.liquidation_price::text,p.observed_ms,p.exchange_ms FROM hl_positions p JOIN hl_address_selection sel USING(address) JOIN hl_symbols s USING(coin) WHERE p.coin=$1 AND sel.selected AND sel.excluded_reason IS NULL AND abs(p.quantity)*COALESCE((s.context->>'markPx')::numeric,p.entry_price,0) >= $2").bind(&out.coin).bind(out.selection.min_position_usd).fetch_all(&mut *tx).await? {
            let p = Position { address:row.get("address"), quantity:number(&row.get::<String,_>("quantity"))?, entry_price:row.get::<Option<String>,_>("entry_price").as_deref().map(number).transpose()?, liquidation_price:row.get::<Option<String>,_>("liquidation_price").as_deref().map(number).transpose()?, observed_ms:row.get("observed_ms"), exchange_ms:row.get("exchange_ms") };
            if p.quantity > 0.0 { out.long_quantity += p.quantity; } else { out.short_quantity -= p.quantity; }
            out.missing_entry_positions += usize::from(p.entry_price.is_none());
            out.missing_liquidation_positions += usize::from(p.liquidation_price.is_none());
            positions.push(p);
        }
    }
    if let Some(mark) = last_mark {
        // A delayed context update still provides a stable price grid. It does
        // not refresh mark_price/OI or the original account observation times.
        let step = mark * 0.0025;
        out.bucket_width = Some(step);
        out.entry_buckets = distribution(&positions, step, false);
        out.liquidation_buckets = distribution(&positions, step, true);
    }
    positions.sort_by(|a, b| b.quantity.abs().total_cmp(&a.quantity.abs()));
    if details {
        out.positions = positions.clone();
    }
    positions.truncate(20);
    out.largest_positions = positions;
    let activity = sqlx::query("SELECT count(*) AS n,COALESCE(sum(quantity) FILTER(WHERE side='B'),0)::text AS buy,COALESCE(sum(quantity) FILTER(WHERE side='A'),0)::text AS sell FROM hl_trades WHERE coin=$1 AND time_ms BETWEEN $2 AND $3").bind(&out.coin).bind(out.activity.start_ms).bind(now).fetch_one(&mut *tx).await?;
    out.activity.trade_count = activity.get("n");
    out.activity.buy_quantity = number(&activity.get::<String, _>("buy"))?;
    out.activity.sell_quantity = number(&activity.get::<String, _>("sell"))?;
    if details {
        out.history.start_ms = now - 30 * 86_400_000;
        out.history.end_ms = now;
        let rows = sqlx::query("SELECT f.fill FROM hl_fills f JOIN hl_address_selection s USING(address) WHERE f.coin=$1 AND f.time_ms >= $2 AND s.selected AND s.excluded_reason IS NULL ORDER BY f.time_ms DESC,f.tid DESC LIMIT 10001")
        .bind(&out.coin).bind(out.history.start_ms).fetch_all(&mut *tx).await?;
        out.history.truncated = rows.len() > 10000;
        for row in rows.into_iter().take(10000) {
            out.history
                .fills
                .push(serde_json::from_value(row.get::<Json<Value>, _>("fill").0)?);
        }
        out.history.fills.sort_by_key(|f| (f.time, f.tid));
        out.history.sampled_addresses = sqlx::query_scalar::<_,i64>("SELECT count(*) FROM hl_fill_sync f JOIN hl_address_selection s USING(address) JOIN hl_address_coins c USING(address) WHERE c.coin=$1 AND s.selected AND s.excluded_reason IS NULL AND f.updated_ms >= $2 AND f.error IS NULL AND f.next_ms >= $3")
        .bind(&out.coin).bind(now-freshness).bind(now-60_000).fetch_one(&mut *tx).await? as usize;
        if out.history.sampled_addresses < out.selection.selected_addresses as usize {
            out.history.error = Some("Some address histories are pending or unavailable".into());
        }
    }
    tx.commit().await?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distribution_keeps_sides_and_unknown_liquidations_separate() {
        let p = |quantity, entry_price, liquidation_price| Position {
            address: "x".into(),
            quantity,
            entry_price,
            liquidation_price,
            observed_ms: 1,
            exchange_ms: 1,
        };
        let positions = vec![
            p(2.0, Some(105.0), Some(80.0)),
            p(-3.0, Some(109.0), Some(130.0)),
            p(7.0, Some(100.0), None),
        ];
        let entries = distribution(&positions, 10.0, false);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].long_quantity, 9.0);
        assert_eq!(entries[0].short_quantity, 3.0);
        let liquidations = distribution(&positions, 10.0, true);
        assert_eq!(liquidations.len(), 2);
        assert_eq!(
            liquidations
                .iter()
                .map(|b| b.long_quantity + b.short_quantity)
                .sum::<f64>(),
            5.0
        );
        assert!(distribution(&positions, f64::NAN, true).is_empty());
    }
}
