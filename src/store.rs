use anyhow::{bail, Context, Result};
use market_history::{rollup, AggTrade, Candle, History, Symbol, MINUTE_MS};
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use std::collections::BTreeMap;

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(url)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn configure(&self, items: &[(Symbol, Option<i32>)], source: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for (symbol, rank) in items {
            sqlx::query("INSERT INTO symbols(symbol,metadata,market_cap_rank,ranking_source) VALUES($1,$2,$3,$4) ON CONFLICT(symbol) DO UPDATE SET metadata=excluded.metadata, enabled=true, market_cap_rank=excluded.market_cap_rank, ranking_source=excluded.ranking_source, configured_at=now()")
                .bind(&symbol.symbol).bind(Json(symbol)).bind(rank).bind(source).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO collector_progress(symbol) VALUES($1) ON CONFLICT DO NOTHING")
                .bind(&symbol.symbol)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn enabled(&self) -> Result<Vec<Symbol>> {
        let rows: Vec<(Json<Symbol>,)> = sqlx::query_as("SELECT metadata FROM symbols WHERE enabled ORDER BY market_cap_rank NULLS LAST, symbol").fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|(symbol,)| symbol.0).collect())
    }

    pub async fn checkpoint(&self, symbol: &str) -> Result<Option<u64>> {
        let id: Option<i64> =
            sqlx::query_scalar("SELECT last_id FROM collector_progress WHERE symbol=$1")
                .bind(symbol)
                .fetch_one(&self.pool)
                .await?;
        Ok(id.map(|id| id as u64))
    }

    /// Candle mutations and the consumed ID commit together. Replay is idempotent.
    pub async fn apply(
        &self,
        symbol: &str,
        trades: &[AggTrade],
        coverage_start: u64,
    ) -> Result<()> {
        if trades.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT last_id,coverage_start FROM collector_progress WHERE symbol=$1 FOR UPDATE",
        )
        .bind(symbol)
        .fetch_one(&mut *tx)
        .await?;
        let mut last = row.get::<Option<i64>, _>("last_id").map(|id| id as u64);
        let original = last;
        let mut dirty = BTreeMap::<u64, Candle>::new();
        let mut last_time = 0;
        for trade in trades {
            trade.validate().map_err(anyhow::Error::msg)?;
            if last.is_some_and(|id| trade.id <= id) {
                continue;
            }
            if last.is_some_and(|id| trade.id != id + 1) {
                bail!("{symbol}: missing trades before {}", trade.id);
            }
            let bucket = trade.time / MINUTE_MS * MINUTE_MS;
            if let std::collections::btree_map::Entry::Vacant(entry) = dirty.entry(bucket) {
                let existing: Option<Json<Candle>> =
                    sqlx::query_scalar("SELECT data FROM candles_1m WHERE symbol=$1 AND bucket=$2")
                        .bind(symbol)
                        .bind(bucket as i64)
                        .fetch_optional(&mut *tx)
                        .await?;
                if let Some(candle) = existing {
                    entry.insert(candle.0);
                }
            }
            if let Some(candle) = dirty.get_mut(&bucket) {
                candle.apply(trade);
            } else {
                dirty.insert(bucket, Candle::new(trade, MINUTE_MS));
            }
            last = Some(trade.id);
            last_time = trade.time;
        }
        if last == original {
            return Ok(());
        }
        for (bucket, candle) in dirty {
            sqlx::query("INSERT INTO candles_1m(symbol,bucket,data) VALUES($1,$2,$3) ON CONFLICT(symbol,bucket) DO UPDATE SET data=excluded.data")
                .bind(symbol).bind(bucket as i64).bind(Json(candle)).execute(&mut *tx).await?;
        }
        sqlx::query("UPDATE collector_progress SET last_id=$2,last_trade_time=$3,coverage_start=CASE WHEN last_id IS NULL THEN $4 ELSE coverage_start END,updated_at=now() WHERE symbol=$1")
            .bind(symbol).bind(last.map(|id| id as i64)).bind(last_time as i64).bind(coverage_start as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn history(
        &self,
        symbol: &str,
        period: &str,
        before: Option<u64>,
        count: usize,
    ) -> Result<History> {
        let interval = market_history::period_ms(period).map_err(anyhow::Error::msg)?;
        let count = count.clamp(1, 300);
        let upper = before
            .map(|t| t / interval * interval)
            .unwrap_or(i64::MAX as u64);
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query("SELECT s.metadata,p.last_id,p.last_trade_time,p.coverage_start FROM symbols s JOIN collector_progress p USING(symbol) WHERE s.symbol=$1")
            .bind(symbol).fetch_optional(&mut *tx).await?.context("symbol is not configured on server")?;
        let coverage = row.get::<i64, _>("coverage_start") as u64;
        // Bounded reverse index scan, including enough minutes for count+1 periods.
        let minutes: Vec<(Json<Candle>,)> = sqlx::query_as("SELECT data FROM candles_1m WHERE symbol=$1 AND bucket<$2 AND bucket>=$3 ORDER BY bucket DESC LIMIT $4")
            .bind(symbol).bind(upper.min(i64::MAX as u64) as i64).bind(coverage as i64).bind(((count + 1) as u64 * interval / MINUTE_MS) as i64).fetch_all(&mut *tx).await?;
        let mut bars = rollup(minutes.into_iter().map(|(bar,)| bar.0), interval);
        // An initial/retention boundary may bisect a larger interval; omit that partial bar.
        bars.retain(|bar| bar.timestamp >= coverage);
        let has_more = bars.len() > count;
        if has_more {
            bars.drain(..bars.len() - count);
        }
        let history = History {
            symbol: row.get::<Json<Symbol>, _>("metadata").0,
            period: period.into(),
            bars,
            last_aggregate_id: row.get::<Option<i64>, _>("last_id").map(|id| id as u64),
            last_trade_time: row
                .get::<Option<i64>, _>("last_trade_time")
                .map(|time| time as u64),
            coverage_start: coverage,
            has_more,
        };
        tx.commit().await?;
        Ok(history)
    }

    /// Delete in small batches, keeping the current checkpoint minute for recovery.
    pub async fn prune(&self, days: u32) -> Result<u64> {
        let cutoff =
            crate::now_ms().saturating_sub(u64::from(days) * 86_400_000) / MINUTE_MS * MINUTE_MS;
        let mut deleted = 0;
        for symbol in self.enabled().await? {
            let mut tx = self.pool.begin().await?;
            let row = sqlx::query(
                "SELECT last_trade_time FROM collector_progress WHERE symbol=$1 FOR UPDATE",
            )
            .bind(&symbol.symbol)
            .fetch_one(&mut *tx)
            .await?;
            let last: Option<i64> = row.get("last_trade_time");
            let Some(last) = last else {
                continue;
            };
            let bound = cutoff.min(last as u64 / MINUTE_MS * MINUTE_MS);
            let result = sqlx::query("DELETE FROM candles_1m WHERE symbol=$1 AND bucket IN (SELECT bucket FROM candles_1m WHERE symbol=$1 AND bucket<$2 ORDER BY bucket LIMIT 5000)")
                .bind(&symbol.symbol).bind(bound as i64).execute(&mut *tx).await?;
            deleted += result.rows_affected();
            // Advance coverage only to the actual oldest remaining minute.
            sqlx::query("UPDATE collector_progress SET coverage_start=GREATEST(coverage_start, COALESCE((SELECT MIN(bucket) FROM candles_1m WHERE symbol=$1),coverage_start)) WHERE symbol=$1")
                .bind(&symbol.symbol).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        Ok(deleted)
    }
}
