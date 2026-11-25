use crate::{exchange::Exchange, store::Store};
use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use market_history::{AggTrade, MINUTE_MS};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct ReaderTask(JoinHandle<()>);
impl Drop for ReaderTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn receive_trades(
    exchange: &Exchange,
    symbol: &str,
) -> Result<(mpsc::Receiver<AggTrade>, ReaderTask)> {
    let url = format!(
        "{}/{}@aggTrade",
        exchange.ws_url.trim_end_matches('/'),
        symbol.to_ascii_lowercase()
    );
    let (mut socket, _) =
        tokio::time::timeout(Duration::from_secs(20), connect_async(url)).await??;
    let (tx, rx) = mpsc::channel(8192);
    let task = tokio::spawn(async move {
        loop {
            let next = tokio::time::timeout(Duration::from_secs(90), socket.next()).await;
            match next {
                Ok(Some(Ok(Message::Text(text)))) => {
                    let trade = match serde_json::from_str::<AggTrade>(&text) {
                        Ok(trade) => trade,
                        Err(_) => break,
                    };
                    // Overflow forces reconnect + REST recovery, never silent data loss.
                    if tx.try_send(trade).is_err() {
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
    });
    Ok((rx, ReaderTask(task)))
}

async fn recover(
    exchange: &Exchange,
    store: &Store,
    symbol: &str,
    target: &AggTrade,
    bootstrap_minutes: u32,
) -> Result<()> {
    let start = target
        .time
        .saturating_sub(u64::from(bootstrap_minutes) * MINUTE_MS)
        / MINUTE_MS
        * MINUTE_MS;
    loop {
        let checkpoint = store.checkpoint(symbol).await?;
        if checkpoint.is_some_and(|id| id >= target.id) {
            return Ok(());
        }
        if checkpoint == target.id.checked_sub(1) {
            store
                .apply(symbol, std::slice::from_ref(target), start)
                .await?;
            return Ok(());
        }
        let page = exchange
            .trades(symbol, checkpoint.map(|id| id + 1), Some(start))
            .await?;
        let page: Vec<_> = page
            .into_iter()
            .filter(|trade| trade.id <= target.id)
            .collect();
        if page.is_empty() {
            bail!(
                "{symbol}: missing aggTrade history before {}; checkpoint preserved",
                target.id
            );
        }
        store.apply(symbol, &page, start).await?;
        if store.checkpoint(symbol).await? == checkpoint {
            bail!("{symbol}: REST pagination made no progress");
        }
    }
}

async fn session(
    exchange: &Exchange,
    store: &Store,
    symbol: &str,
    bootstrap_minutes: u32,
) -> Result<()> {
    let (mut rx, _reader) = receive_trades(exchange, symbol).await?;
    let first = tokio::time::timeout(Duration::from_secs(90), rx.recv())
        .await?
        .context("trade stream closed before first message")?;
    recover(exchange, store, symbol, &first, bootstrap_minutes).await?;
    tracing::info!(%symbol, "collector caught up");
    let mut last = store
        .checkpoint(symbol)
        .await?
        .context("checkpoint missing after recovery")?;
    let mut pending = Vec::new();
    let mut flush = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = flush.tick() => {
                store.apply(symbol, &pending, 0).await?;
                pending.clear();
            }
            trade = rx.recv() => {
                let Some(trade) = trade else {
                    store.apply(symbol, &pending, 0).await?;
                    bail!("trade stream disconnected; reconnecting from persisted ID");
                };
                if trade.id <= last { continue; }
                if trade.id != last + 1 {
                    store.apply(symbol, &pending, 0).await?;
                    pending.clear();
                    recover(exchange, store, symbol, &trade, bootstrap_minutes).await?;
                    last = store.checkpoint(symbol).await?.unwrap();
                } else {
                    trade.validate().map_err(anyhow::Error::msg)?;
                    last = trade.id;
                    pending.push(trade);
                    if pending.len() >= 1000 {
                        store.apply(symbol, &pending, 0).await?;
                        pending.clear();
                    }
                }
            }
        }
    }
}

pub async fn run(
    store: Store,
    exchange: Exchange,
    bootstrap_minutes: u32,
    retention_days: u32,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut workers = HashMap::<String, JoinHandle<()>>::new();
    let mut refresh = tokio::time::interval(Duration::from_secs(5));
    let mut cleanup = tokio::time::interval(Duration::from_secs(300));
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = refresh.tick() => {
                let enabled = match store.enabled().await { Ok(items) => items, Err(error) => { tracing::error!(%error, "cannot refresh symbols"); continue; } };
                workers.retain(|symbol, task| {
                    let keep = enabled.iter().any(|item| &item.symbol == symbol) && !task.is_finished();
                    if !keep { task.abort(); }
                    keep
                });
                for symbol in enabled {
                    if workers.contains_key(&symbol.symbol) { continue; }
                    let db = store.clone();
                    let source = exchange.clone();
                    let key = symbol.symbol.clone();
                    let task = tokio::spawn(async move {
                        loop {
                            if let Err(error) = session(&source, &db, &symbol.symbol, bootstrap_minutes).await {
                                tracing::warn!(symbol=%symbol.symbol, %error, "collector will retry");
                            }
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    });
                    workers.insert(key, task);
                }
            }
            _ = cleanup.tick(), if retention_days > 0 => {
                if let Err(error) = store.prune(retention_days).await { tracing::warn!(%error, "retention cleanup failed"); }
            }
        }
    }
    // Pending trades are recoverable from the last atomically committed ID.
    for (_, task) in workers {
        task.abort();
        let _ = task.await;
    }
    Ok(())
}
