//! Continuously collected five-minute depth windows, independent of connected clients.
use crate::{exchange::Exchange, store::Store};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use market_depth::{
    book::{BinanceDepthDelta, HeatmapCache, LocalOrderBook},
    DepthSample, DepthWindow,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, watch, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Default)]
struct Window {
    generation: u64,
    latest_ms: u64,
    samples: VecDeque<DepthSample>,
}
#[derive(Clone, Default)]
pub struct DepthCache(Arc<RwLock<HashMap<String, Window>>>);
impl DepthCache {
    async fn reset(&self, symbol: &str) {
        let mut all = self.0.write().await;
        let window = all.entry(symbol.into()).or_default();
        window.generation = crate::now_ms().max(window.generation.saturating_add(1));
        window.samples.clear();
        window.latest_ms = 0;
    }
    async fn push(&self, symbol: &str, sample: DepthSample) {
        let mut all = self.0.write().await;
        let Some(window) = all.get_mut(symbol) else {
            return;
        };
        if !sample.valid() || sample.observed_ms < window.latest_ms {
            return;
        }
        if window
            .samples
            .back()
            .is_some_and(|last| sample.second() > last.second() + 2 || sample.tick != last.tick)
        {
            window.generation = crate::now_ms().max(window.generation.saturating_add(1));
            window.samples.clear();
        }
        window.latest_ms = sample.observed_ms;
        let second = sample.second();
        if window
            .samples
            .back()
            .is_none_or(|last| last.second() < second)
        {
            window.samples.push_back(sample);
        }
        while window
            .samples
            .front()
            .is_some_and(|s| s.second() + market_depth::window::RETENTION_SECONDS < second)
        {
            window.samples.pop_front();
        }
    }
    pub async fn get(
        &self,
        symbol: &str,
        generation: Option<u64>,
        after: Option<u64>,
    ) -> Option<DepthWindow> {
        let now = crate::now_ms();
        let all = self.0.read().await;
        let window = all.get(symbol)?;
        let reset = generation != Some(window.generation)
            || after.is_none()
            || after.is_some_and(|a| {
                window.samples.front().is_some_and(|s| a + 1 < s.second())
                    || window.samples.back().is_some_and(|s| a > s.second())
            });
        Some(DepthWindow {
            version: market_depth::window::VERSION,
            symbol: symbol.into(),
            generation: window.generation,
            reset,
            server_time: now,
            latest_ms: window.latest_ms,
            samples: window
                .samples
                .iter()
                .filter(|s| {
                    s.second() + market_depth::window::RETENTION_SECONDS >= now / 1000
                        && (reset || after.is_some_and(|a| s.second() > a))
                })
                .cloned()
                .collect(),
        })
    }
}

struct Reader(tokio::task::JoinHandle<()>);
impl Drop for Reader {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn session(exchange: &Exchange, cache: &DepthCache, symbol: &str, tick: f64) -> Result<()> {
    // Read and buffer WS before REST, so the first accepted delta bridges the snapshot ID.
    let url = format!(
        "{}/{}@depth@100ms",
        exchange.ws_url.trim_end_matches('/'),
        symbol.to_ascii_lowercase()
    );
    let (mut socket, _) =
        tokio::time::timeout(Duration::from_secs(20), connect_async(url)).await??;
    let (tx, mut rx) = mpsc::channel(2048);
    let _reader = Reader(tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_secs(5), socket.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    let Ok(delta) = serde_json::from_str::<BinanceDepthDelta>(&text) else {
                        break;
                    };
                    if tx.try_send(delta).is_err() {
                        break;
                    }
                }
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Ok(Some(Ok(Message::Pong(_)))) => {}
                _ => break,
            }
        }
    }));
    let mut book = LocalOrderBook::from_snapshot(exchange.depth(symbol).await?, tick)
        .map_err(anyhow::Error::msg)?;
    let mut heatmap = HeatmapCache::new(tick);
    let mut observed_ms = 0;
    let mut sampling = tokio::time::interval(Duration::from_secs(1));
    sampling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            delta = rx.recv() => {
                let delta = delta.context("depth reader stopped")?;
                let timestamp = delta.event_time;
                // Only the bridged book may be sampled; buffered events never advance receipt freshness.
                if !book.apply_delta(delta).map_err(anyhow::Error::msg)? { continue; }
                if timestamp > crate::now_ms().saturating_add(5000) { bail!("future depth event"); }
                let (Some(bid), Some(ask)) = book.best_prices() else { bail!("empty depth side"); };
                if ask <= bid { bail!("crossed depth book"); }
                heatmap.configure_tick_size((bid+ask)/2.0, tick);
                let width=heatmap.tick_size();
                heatmap.apply_levels(symbol, timestamp, book.grouped_levels(width), Some(bid), Some(ask));
                heatmap.discard_completed();
                observed_ms = timestamp;
            }
            _ = sampling.tick(), if observed_ms != 0 => {
                let timestamp = crate::now_ms();
                // Match local sampling: unchanged books remain usable only within the existing 5s freshness bound.
                let sample = sample_book(&book, &heatmap, timestamp, observed_ms)
                    .context("depth book expired before sampling")?;
                cache.push(symbol, sample).await;
            }
        }
    }
}

fn sample_book(
    book: &LocalOrderBook,
    heatmap: &HeatmapCache,
    timestamp: u64,
    observed_ms: u64,
) -> Option<DepthSample> {
    let sample = DepthSample {
        timestamp,
        observed_ms,
        price: book.mid_price()?,
        tick: heatmap.tick_size(),
        levels: heatmap.active_levels(),
    };
    sample.valid().then_some(sample)
}

pub async fn run(
    store: Store,
    exchange: Exchange,
    cache: DepthCache,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut workers = HashMap::<String, tokio::task::JoinHandle<()>>::new();
    let mut refresh = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = refresh.tick() => {
                let enabled = match store.enabled().await { Ok(s) => s, Err(error) => { tracing::warn!(%error, "depth symbols unavailable"); continue; } };
                workers.retain(|symbol, task| {
                    let keep = enabled.iter().any(|s| &s.symbol == symbol) && !task.is_finished();
                    if !keep { task.abort(); }
                    keep
                });
                cache.0.write().await.retain(|symbol,_| enabled.iter().any(|s| &s.symbol == symbol));
                for symbol in enabled {
                    if workers.contains_key(&symbol.symbol) { continue; }
                    let tick = symbol.min_tick_size.to_string().parse::<f64>()?;
                    let source = exchange.clone();
                    let shared = cache.clone();
                    let key = symbol.symbol.clone();
                    workers.insert(key.clone(), tokio::spawn(async move {
                        loop {
                            shared.reset(&key).await;
                            if let Err(error) = session(&source, &shared, &key, tick).await {
                                tracing::warn!(symbol=%key, %error, "depth collector reconnecting; window invalidated");
                            }
                            shared.reset(&key).await;
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }));
                }
            }
        }
    }
    for (_, task) in workers {
        task.abort();
        let _ = task.await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(second: u64) -> DepthSample {
        DepthSample {
            timestamp: second * 1000,
            observed_ms: second * 1000,
            price: 100.0,
            tick: 1.0,
            levels: vec![(99.0, 1.0, true), (101.0, 2.0, false)],
        }
    }
    #[tokio::test]
    async fn five_minute_bootstrap_incremental_overlap_and_reconnect() {
        let cache = DepthCache::default();
        let end = crate::now_ms() / 1000;
        cache.reset("BTCUSDT").await;
        for second in end - 400..=end - 1 {
            cache.push("BTCUSDT", sample(second)).await;
        }
        let full = cache.get("BTCUSDT", None, None).await.unwrap();
        full.validate("BTCUSDT", crate::now_ms() as i64).unwrap();
        assert!(full.reset);
        assert_eq!(full.samples.len(), 300);
        let generation = full.generation;
        cache.push("BTCUSDT", sample(end - 1)).await;
        cache.push("BTCUSDT", sample(end)).await;
        let next = cache
            .get("BTCUSDT", Some(generation), Some(end - 1))
            .await
            .unwrap();
        assert!(!next.reset);
        assert_eq!(next.samples.len(), 1);
        assert_eq!(next.samples[0].second(), end);
        assert!(cache
            .get("BTCUSDT", Some(generation), Some(end))
            .await
            .unwrap()
            .samples
            .is_empty());
        assert!(
            cache
                .get("BTCUSDT", Some(generation), Some(end - 500))
                .await
                .unwrap()
                .reset
        );
        cache.reset("BTCUSDT").await;
        let restart = cache
            .get("BTCUSDT", Some(generation), Some(end))
            .await
            .unwrap();
        assert!(restart.reset);
        assert_ne!(restart.generation, generation);
        assert_eq!(restart.latest_ms, 0);
        assert!(restart.samples.is_empty());
        assert!(cache.get("UNKNOWN", None, None).await.is_none());
    }
    #[tokio::test]
    async fn observation_gap_and_tick_change_invalidate_previous_seconds() {
        let cache = DepthCache::default();
        let end = crate::now_ms() / 1000;
        cache.reset("BTCUSDT").await;
        cache.push("BTCUSDT", sample(end - 5)).await;
        let generation = cache.get("BTCUSDT", None, None).await.unwrap().generation;
        cache.push("BTCUSDT", sample(end - 1)).await;
        let gap = cache
            .get("BTCUSDT", Some(generation), Some(end - 5))
            .await
            .unwrap();
        assert!(gap.reset);
        assert_eq!(gap.samples.len(), 1);
        let mut changed = sample(end);
        changed.tick = 2.0;
        cache.push("BTCUSDT", changed).await;
        let tick = cache
            .get("BTCUSDT", Some(gap.generation), Some(end - 1))
            .await
            .unwrap();
        assert!(tick.reset);
        assert_eq!(tick.samples.len(), 1);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    #[tokio::test]
    async fn authenticated_bootstrap_and_cursor_api_without_database_access() {
        let depth = DepthCache::default();
        depth.reset("BTCUSDT").await;
        let second = crate::now_ms() / 1000;
        depth
            .push(
                "BTCUSDT",
                DepthSample {
                    timestamp: second * 1000,
                    observed_ms: second * 1000,
                    price: 100.0,
                    tick: 1.0,
                    levels: vec![(99.0, 1.0, true), (101.0, 1.0, false)],
                },
            )
            .await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = crate::api::router(crate::api::Api {
            store: Store {
                pool: sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://test:test@127.0.0.1:1/test")
                    .unwrap(),
            },
            token: Some("unit-test-token".into()),
            depth,
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let url = format!("http://{addr}/v1/depth");
        assert_eq!(
            client
                .get(&url)
                .query(&[("symbol", "BTCUSDT")])
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        let full: DepthWindow = client
            .get(&url)
            .bearer_auth("unit-test-token")
            .query(&[("symbol", "BTCUSDT")])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(full.reset);
        assert_eq!(full.samples.len(), 1);
        full.validate("BTCUSDT", crate::now_ms() as i64).unwrap();
        let delta: DepthWindow = client
            .get(&url)
            .bearer_auth("unit-test-token")
            .query(&[
                ("symbol", "BTCUSDT".into()),
                ("generation", full.generation.to_string()),
                ("after", second.to_string()),
            ])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!delta.reset);
        assert!(delta.samples.is_empty());
        assert_eq!(
            client
                .get(&url)
                .bearer_auth("unit-test-token")
                .query(&[("symbol", "UNKNOWN")])
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        assert_eq!(
            client
                .get(&url)
                .bearer_auth("unit-test-token")
                .query(&[("symbol", "BTCUSDT"), ("after", "18446744073709551615")])
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        server.abort();
    }
}

#[cfg(test)]
mod sampling_tests {
    use super::*;
    #[tokio::test]
    async fn unchanged_fresh_book_fills_seconds_but_expired_book_cannot_fill_a_gap() {
        let book = LocalOrderBook::from_snapshot(
            market_depth::book::BinanceDepthSnapshot {
                last_update_id: 1,
                bids: vec![["99".into(), "1".into()]],
                asks: vec![["101".into(), "2".into()]],
            },
            1.0,
        )
        .unwrap();
        let mut heatmap = HeatmapCache::new(1.0);
        heatmap.apply_levels(
            "BTCUSDT",
            100_000,
            book.grouped_levels(1.0),
            Some(99.0),
            Some(101.0),
        );
        let cache = DepthCache::default();
        cache.reset("BTCUSDT").await;
        for second in 100..=105 {
            let sample = sample_book(&book, &heatmap, second * 1000, 100_000).unwrap();
            assert_eq!(sample.observed_ms, 100_000);
            cache.push("BTCUSDT", sample).await;
        }
        let all = cache.0.read().await;
        let window = &all["BTCUSDT"];
        assert_eq!(window.samples.len(), 6);
        assert_eq!(
            window.latest_ms, 100_000,
            "sampling must never forge a new exchange update time"
        );
        assert!(sample_book(&book, &heatmap, 106_000, 100_000).is_none());
        assert!(sample_book(&book, &heatmap, 100_000, 106_000).is_none());
    }
}
