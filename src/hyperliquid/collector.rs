use super::{model::Trade, save_snapshot, save_trades};
use crate::{now_ms, store::Store};
use anyhow::{bail, ensure, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sqlx::types::Json;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct SnapshotTask(tokio::task::JoinHandle<Result<()>>);
impl Drop for SnapshotTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone)]
pub struct Config {
    pub enabled: bool,
    pub max_addresses: i64,
    pub min_position_notional: f64,
    pub api_url: String,
    pub ws_url: String,
}
impl Config {
    pub fn from_env() -> Result<Self> {
        let enabled = match std::env::var("ET_HL_ENABLED")
            .unwrap_or_else(|_| "false".into())
            .as_str()
        {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => bail!("ET_HL_ENABLED must be true/false"),
        };
        let max_addresses = std::env::var("ET_HL_MAX_ADDRESSES")
            .unwrap_or_else(|_| "256".into())
            .parse::<i64>()?;
        ensure!(
            (1..=2048).contains(&max_addresses),
            "ET_HL_MAX_ADDRESSES must be 1..2048"
        );
        let min_position_notional = std::env::var("ET_HL_MIN_POSITION_USD")
            .unwrap_or_else(|_| "10000".into())
            .parse::<f64>()?;
        ensure!(
            min_position_notional.is_finite() && min_position_notional >= 0.0,
            "ET_HL_MIN_POSITION_USD must be finite and nonnegative"
        );
        Ok(Self {
            enabled,
            max_addresses,
            min_position_notional,
            api_url: std::env::var("ET_HL_API_URL")
                .unwrap_or_else(|_| "https://api.hyperliquid.xyz/info".into()),
            ws_url: std::env::var("ET_HL_WS_URL")
                .unwrap_or_else(|_| "wss://api.hyperliquid.xyz/ws".into()),
        })
    }
}

async fn info(client: &reqwest::Client, config: &Config, body: Value) -> Result<Value> {
    let response = client.post(&config.api_url).json(&body).send().await?;
    ensure!(
        response.status().is_success(),
        "Hyperliquid info HTTP {}",
        response.status()
    );
    ensure!(
        response.content_length().is_none_or(|n| n <= 8_000_000),
        "oversized Hyperliquid info response"
    );
    let bytes = response.bytes().await?;
    ensure!(
        bytes.len() <= 8_000_000,
        "oversized Hyperliquid info response"
    );
    Ok(serde_json::from_slice(&bytes)?)
}

async fn refresh_symbols(
    store: &Store,
    client: &reqwest::Client,
    config: &Config,
) -> Result<Vec<String>> {
    let raw = info(client, config, json!({"type":"metaAndAssetCtxs"})).await?;
    let universe = raw
        .get(0)
        .and_then(|v| v.get("universe"))
        .and_then(Value::as_array)
        .context("missing perpetual universe")?;
    let contexts = raw
        .get(1)
        .and_then(Value::as_array)
        .context("missing perpetual contexts")?;
    ensure!(
        universe.len() == contexts.len(),
        "perpetual metadata/context mismatch"
    );
    // Exact base-asset matches only. No guessed multiplier or symbol conversions.
    let enabled = store.enabled().await?;
    let mut found = BTreeMap::new();
    for (meta, context) in universe.iter().zip(contexts) {
        if meta.get("isDelisted").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let Some(coin) = meta.get("name").and_then(Value::as_str) else {
            continue;
        };
        if let Some(spot) = enabled
            .iter()
            .find(|s| s.base_asset == coin && s.quote_asset == "USDT")
        {
            found.insert(coin.to_owned(), (spot.symbol.clone(), context.clone()));
        }
    }
    let mut tx = store.pool.begin().await?;
    sqlx::query("UPDATE hl_symbols SET enabled=false")
        .execute(&mut *tx)
        .await?;
    for (coin, (spot, context)) in &found {
        sqlx::query("INSERT INTO hl_symbols(coin,spot_symbol,context,context_ms) VALUES($1,$2,$3,$4) ON CONFLICT(coin) DO UPDATE SET spot_symbol=excluded.spot_symbol,enabled=true,context=excluded.context,context_ms=excluded.context_ms")
            .bind(coin).bind(spot).bind(Json(context)).bind(now_ms() as i64).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(found.into_keys().collect())
}

async fn collect(
    store: &Store,
    config: &Config,
    coins: &[String],
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    if coins.is_empty() {
        tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(Duration::from_secs(60))=>{}};
        return Ok(());
    }
    let (mut ws, _) =
        tokio::time::timeout(Duration::from_secs(20), connect_async(&config.ws_url)).await??;
    for coin in coins {
        ws.send(Message::Text(
            json!({"method":"subscribe","subscription":{"type":"trades","coin":coin}})
                .to_string()
                .into(),
        ))
        .await?;
    }
    sqlx::query("UPDATE hl_status SET connected=true,error=NULL WHERE id=1")
        .execute(&store.pool)
        .await?;
    tracing::info!(?coins, "Hyperliquid live trades connected");
    let mut pending = Vec::new();
    let mut flush = tokio::time::interval(Duration::from_secs(1));
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    let mut cleanup = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(300),
        Duration::from_secs(300),
    );
    let mut refresh = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut last_message = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _=stop.changed()=>{save_trades(store,&pending).await?;return Ok(());}
            _=flush.tick()=>{save_trades(store,&pending).await?;pending.clear();}
            _=cleanup.tick()=>{if let Err(error)=prune(store).await{tracing::warn!(%error,"Hyperliquid retention cleanup failed");}}
            _=ping.tick()=>{
                ensure!(last_message.elapsed()<Duration::from_secs(90),"Hyperliquid websocket heartbeat expired");
                ws.send(Message::Text(json!({"method":"ping"}).to_string().into())).await?;
            }
            _=refresh.tick()=>{
                match refresh_symbols(store,&client,config).await {
                    Ok(updated) if updated!=coins=>{save_trades(store,&pending).await?;return Ok(());}
                    Ok(_)=>{}
                    Err(error)=>{tracing::warn!(%error,"Hyperliquid metadata refresh failed; retaining subscriptions");}
                }
            }
            message=ws.next()=>{
                let message=message.context("Hyperliquid websocket closed")??;
                last_message=tokio::time::Instant::now();
                match message {
                    Message::Text(text)=>{
                        ensure!(text.len()<=8_000_000,"oversized Hyperliquid websocket message");
                        let event:Value=serde_json::from_str(&text)?;
                        if event["channel"]=="error" {bail!("Hyperliquid subscription rejected");}
                        if event["channel"]=="trades" {
                            let trades:Vec<Trade>=serde_json::from_value(event["data"].clone())?;
                            for trade in trades {
                                ensure!(coins.contains(&trade.coin),"unexpected coin on subscribed trade stream");
                                pending.push(trade);
                            }
                            if pending.len()>=500 {save_trades(store,&pending).await?;pending.clear();}
                        }
                    }
                    Message::Ping(payload)=>ws.send(Message::Pong(payload)).await?,
                    Message::Close(_)=>{save_trades(store,&pending).await?;bail!("Hyperliquid websocket disconnected");}
                    _=>{}
                }
            }
        }
    }
}

async fn snapshots_once(
    store: Store,
    config: Config,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut cadence = tokio::time::interval(Duration::from_secs(2));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut selected_at = None;
    let mut audit_counter = 0_u32;
    loop {
        tokio::select! {_=stop.changed()=>break,_=cadence.tick()=>{}}
        if selected_at
            .is_none_or(|at: tokio::time::Instant| at.elapsed() >= Duration::from_secs(60))
        {
            super::selection::refresh(&store, config.max_addresses, config.min_position_notional)
                .await?;
            selected_at = Some(tokio::time::Instant::now());
        }
        let now = now_ms() as i64;
        audit_counter = (audit_counter + 1) % 30;
        // Exclusions are reversible; ~one request per minute can recheck an
        // excluded account once an hour, within the same fixed request cadence.
        let audit: Option<String> = if audit_counter == 0 {
            sqlx::query_scalar("SELECT a.address FROM hl_addresses a JOIN hl_address_selection s USING(address) WHERE s.excluded_reason IS NOT NULL AND a.last_seen_ms >= $1 AND (a.attempted_ms IS NULL OR a.attempted_ms < $2) ORDER BY a.attempted_ms NULLS FIRST,a.address LIMIT 1").bind(now-86_400_000).bind(now-3_600_000).fetch_optional(&store.pool).await?
        } else {
            None
        };
        let user=match audit {
            Some(user)=>Some(user),
            None=>sqlx::query_scalar::<_,String>("SELECT a.address FROM hl_addresses a JOIN hl_address_selection s USING(address) WHERE s.selected AND s.excluded_reason IS NULL AND (a.attempted_ms IS NULL OR a.attempted_ms < $1) ORDER BY a.attempted_ms NULLS FIRST,a.address LIMIT 1")
                .bind(now-240_000).fetch_optional(&store.pool).await?,
        };
        let Some(user) = user else {
            continue;
        };
        sqlx::query("UPDATE hl_addresses SET attempted_ms=$2 WHERE address=$1")
            .bind(&user)
            .bind(now)
            .execute(&store.pool)
            .await?;
        let fetched = tokio::select! {_=stop.changed()=>break,result=info(&client,&config,json!({"type":"clearinghouseState","user":user}))=>result};
        let result = match fetched {
            Ok(raw) => save_snapshot(&store, &user, raw, now_ms()).await,
            Err(error) => Err(error),
        };
        if result.is_ok() {
            if let Err(error) = collect_fills(&store, &client, &config, &user).await {
                tracing::warn!(%user,%error,"Hyperliquid fill collection failed");
                sqlx::query("UPDATE hl_fill_sync SET error=$2 WHERE address=$1")
                    .bind(&user)
                    .bind(error.to_string())
                    .execute(&store.pool)
                    .await?;
            }
        }
        if let Err(error) = result {
            tracing::warn!(%user,%error,"Hyperliquid account snapshot failed; old state retains its original age");
            sqlx::query("UPDATE hl_addresses SET error=$2 WHERE address=$1")
                .bind(&user)
                .bind(error.to_string())
                .execute(&store.pool)
                .await?;
            tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
        }
    }
    Ok(())
}

async fn collect_fills(
    store: &Store,
    client: &reqwest::Client,
    config: &Config,
    user: &str,
) -> Result<()> {
    let now = now_ms() as i64;
    let start = sqlx::query_scalar::<_, i64>("SELECT CASE WHEN next_ms >= updated_ms THEN next_ms-60000 ELSE next_ms END FROM hl_fill_sync WHERE address=$1")
        .bind(user)
        .fetch_optional(&store.pool)
        .await?
        .unwrap_or(now - 30 * 86_400_000)
        .max(now - 30 * 86_400_000);
    sqlx::query("INSERT INTO hl_fill_sync(address,next_ms,updated_ms,error) VALUES($1,$2,0,'pending') ON CONFLICT(address) DO NOTHING").bind(user).bind(start).execute(&store.pool).await?;
    let raw = info(client, config, json!({"type":"userFillsByTime","user":user,"startTime":start,"endTime":now,"aggregateByTime":false})).await?;
    let mut fills: Vec<market_history::hyperliquid::Fill> = serde_json::from_value(raw)?;
    ensure!(
        fills.len() <= 2000
            && fills.iter().all(|f| f.valid()
                && f.time >= start
                && f.time <= now + 5000
                && f.tid <= i64::MAX as u64),
        "invalid Hyperliquid fills"
    );
    let next = if fills.len() == 2000 {
        fills.iter().map(|f| f.time).max().unwrap_or(start)
    } else {
        now
    };
    ensure!(
        fills.len() < 2000 || next > start,
        "Hyperliquid fill pagination stalled"
    );
    let mut tx = store.pool.begin().await?;
    for f in &mut fills {
        f.address = user.to_owned();
        sqlx::query("INSERT INTO hl_fills(address,coin,time_ms,tid,fill) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING").bind(user).bind(&f.coin).bind(f.time).bind(f.tid as i64).bind(Json(&*f)).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE hl_fill_sync SET next_ms=$2,updated_ms=$3,error=NULL WHERE address=$1")
        .bind(user)
        .bind(next)
        .bind(now)
        .execute(&mut *tx)
        .await?;
    if next == now {
        super::selection::refresh_performance(&mut tx, user, now).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn snapshots(store: Store, config: Config, mut stop: watch::Receiver<bool>) -> Result<()> {
    while !*stop.borrow() {
        if let Err(error) = snapshots_once(store.clone(), config.clone(), stop.clone()).await {
            tracing::warn!(%error,"Hyperliquid account selection/snapshot worker will retry");
        }
        if *stop.borrow() {
            break;
        }
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(10))=>{}}
    }
    Ok(())
}

async fn prune(store: &Store) -> Result<()> {
    let cutoff = now_ms() as i64 - 30 * 86_400_000;
    sqlx::query("DELETE FROM hl_closed_cycles WHERE ctid IN (SELECT ctid FROM hl_closed_cycles WHERE time_ms<$1 LIMIT 10000)").bind(cutoff).execute(&store.pool).await?;
    sqlx::query("DELETE FROM hl_fills WHERE ctid IN (SELECT ctid FROM hl_fills WHERE time_ms<$1 LIMIT 10000)").bind(cutoff).execute(&store.pool).await?;
    // Small independent batches avoid holding the shared pool for a long delete.
    sqlx::query("DELETE FROM hl_trades WHERE ctid IN (SELECT ctid FROM hl_trades WHERE time_ms<$1 LIMIT 10000)").bind(cutoff).execute(&store.pool).await?;
    sqlx::query("DELETE FROM hl_snapshots WHERE ctid IN (SELECT ctid FROM hl_snapshots WHERE observed_ms<$1 LIMIT 10000)").bind(cutoff).execute(&store.pool).await?;
    sqlx::query("DELETE FROM hl_address_activity_1m WHERE ctid IN (SELECT ctid FROM hl_address_activity_1m WHERE bucket_ms<$1 LIMIT 10000)").bind(now_ms() as i64-86_400_000).execute(&store.pool).await?;
    Ok(())
}

async fn run_once(store: Store, config: Config, mut stop: watch::Receiver<bool>) -> Result<()> {
    sqlx::query("UPDATE hl_status SET enabled=$1,connected=false,error=NULL, gap_count=gap_count+CASE WHEN $1 AND started_ms IS NOT NULL THEN 1 ELSE 0 END,last_gap_ms=CASE WHEN $1 AND started_ms IS NOT NULL THEN $2 ELSE last_gap_ms END,started_ms=CASE WHEN $1 THEN COALESCE(started_ms,$2) ELSE started_ms END WHERE id=1")
        .bind(config.enabled).bind(now_ms() as i64).execute(&store.pool).await?;
    if !config.enabled {
        let _ = stop.changed().await;
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut snapshot_task = SnapshotTask(tokio::spawn(snapshots(
        store.clone(),
        config.clone(),
        stop.clone(),
    )));
    let mut prune_at = tokio::time::Instant::now();
    loop {
        if *stop.borrow() {
            break;
        }
        if prune_at.elapsed() >= Duration::from_secs(300) {
            if let Err(error) = prune(&store).await {
                tracing::warn!(%error,"Hyperliquid retention cleanup failed");
            }
            prune_at = tokio::time::Instant::now();
        }
        if snapshot_task.0.is_finished() {
            tracing::warn!("Hyperliquid snapshot worker stopped; restarting");
            let _ = (&mut snapshot_task.0).await;
            snapshot_task = SnapshotTask(tokio::spawn(snapshots(
                store.clone(),
                config.clone(),
                stop.clone(),
            )));
        }
        let result = match tokio::select! {_=stop.changed()=>break,result=refresh_symbols(&store,&client,&config)=>result}
        {
            Ok(coins) => collect(&store, &config, &coins, stop.clone()).await,
            Err(error) => Err(error),
        };
        if *stop.borrow() {
            break;
        }
        let error = result.err().map(|e| e.to_string());
        if let Some(error) = &error {
            tracing::warn!(%error,"Hyperliquid stream interrupted; gap recorded, reconnecting");
        }
        sqlx::query("UPDATE hl_status SET connected=false,gap_count=gap_count+1,last_gap_ms=$1,error=$2 WHERE id=1").bind(now_ms() as i64).bind(error).execute(&store.pool).await?;
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(5))=>{}}
    }
    snapshot_task.0.abort();
    let _ = (&mut snapshot_task.0).await;
    sqlx::query("UPDATE hl_status SET connected=false WHERE id=1")
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub async fn run(store: Store, mut config: Config, mut stop: watch::Receiver<bool>) -> Result<()> {
    // A stale environment or library caller cannot reactivate paused collection.
    config.enabled &= cfg!(feature = "hyperliquid");
    while !*stop.borrow() {
        if let Err(error) = run_once(store.clone(), config.clone(), stop.clone()).await {
            tracing::error!(%error,"Hyperliquid worker failed; retrying independently of spot collection");
            let _ = sqlx::query("UPDATE hl_status SET connected=false,error=$1 WHERE id=1")
                .bind(error.to_string())
                .execute(&store.pool)
                .await;
        }
        if *stop.borrow() {
            break;
        }
        tokio::select! {_=stop.changed()=>break,_=tokio::time::sleep(Duration::from_secs(10))=>{}}
    }
    Ok(())
}
