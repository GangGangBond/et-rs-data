use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use et_data_service::{
    api::{router, Api},
    collector,
    exchange::Exchange,
    store::Store,
};
use sqlx::{Connection, Row};

#[derive(Parser)]
#[command(version, about = "Personal Binance Spot footprint history server")]
struct Cli {
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Migrate, seed ten ranked eligible pairs only if the database is empty.
    Init,
    /// Manage persisted collection symbols. Changes take effect within 5 seconds.
    Symbols {
        #[command(subcommand)]
        command: Symbols,
    },
    Serve {
        #[arg(long, env = "ET_DATA_BIND", default_value = "127.0.0.1:8088")]
        bind: String,
        #[arg(long, env = "ET_DATA_TOKEN", hide_env_values = true)]
        token: Option<String>,
        #[arg(long, env = "ET_DATA_BOOTSTRAP_MINUTES", default_value_t = 60, value_parser = clap::value_parser!(u32).range(1..=10080))]
        bootstrap_minutes: u32,
        /// Zero disables expiry. Affects footprints and their OHLC together.
        #[arg(long, env = "ET_DATA_RETENTION_DAYS", default_value_t = 30)]
        retention_days: u32,
    },
}

#[derive(Subcommand)]
enum Symbols {
    List,
    Add {
        symbols: Vec<String>,
    },
    /// Disable collection; keep all existing history.
    Remove {
        symbols: Vec<String>,
    },
    /// Add/enable the current top N eligible pairs, without deleting others.
    SeedTop {
        #[arg(long, default_value_t=10, value_parser=clap::value_parser!(u32).range(1..=100))]
        count: u32,
    },
}

async fn seed(store: &Store, exchange: &Exchange, count: usize) -> Result<()> {
    let pairs = exchange.top_symbols(count).await?;
    let values: Vec<_> = pairs
        .into_iter()
        .map(|(symbol, rank)| (symbol, Some(rank)))
        .collect();
    store
        .configure(
            &values,
            "CoinGecko market_cap_desc; exclude stablecoins/wrapped-tokens; Binance Spot USDT",
        )
        .await?;
    for (symbol, rank) in values {
        println!("{}\trank={}", symbol.symbol, rank.unwrap());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "et_data_service=info".into()),
        )
        .init();
    let cli = Cli::parse();
    #[cfg(feature = "hyperliquid")]
    let hl_config = et_data_service::hyperliquid::Config::from_env()?;
    let store = Store::connect(&cli.database_url)
        .await
        .context("connect/migrate PostgreSQL")?;
    let exchange = Exchange::new()?;
    match cli.command {
        Command::Init => {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM symbols").fetch_one(&store.pool).await?;
            if count == 0 { seed(&store, &exchange, 10).await?; }
            else { println!("Database initialized; existing symbol configuration preserved."); }
        }
        Command::Symbols { command } => match command {
            Symbols::SeedTop { count } => seed(&store, &exchange, count as usize).await?,
            Symbols::List => {
                for row in sqlx::query("SELECT s.symbol,s.enabled,s.market_cap_rank,p.last_id,p.last_trade_time,p.coverage_start FROM symbols s LEFT JOIN collector_progress p USING(symbol) ORDER BY s.market_cap_rank NULLS LAST,s.symbol").fetch_all(&store.pool).await? {
                    println!("{}\tenabled={}\trank={:?}\tlast_id={:?}\tlast_trade_ms={:?}\tcoverage_start_ms={:?}", row.get::<String,_>("symbol"), row.get::<bool,_>("enabled"), row.get::<Option<i32>,_>("market_cap_rank"), row.get::<Option<i64>,_>("last_id"), row.get::<Option<i64>,_>("last_trade_time"), row.get::<Option<i64>,_>("coverage_start"));
                }
            }
            Symbols::Add { symbols } => {
                if symbols.is_empty() { bail!("provide at least one symbol, e.g. BTCUSDT"); }
                let available = exchange.symbols().await?;
                let mut values = Vec::new();
                for symbol in symbols { values.push((available.get(&symbol.to_ascii_uppercase()).with_context(|| format!("unsupported Binance Spot USDT symbol: {symbol}"))?.clone(), None)); }
                store.configure(&values, "CLI manual selection").await?;
                for (symbol, _) in values { println!("enabled {}", symbol.symbol); }
            }
            Symbols::Remove { symbols } => {
                if symbols.is_empty() { bail!("provide at least one symbol"); }
                for symbol in symbols {
                    let result = sqlx::query("UPDATE symbols SET enabled=false,configured_at=now() WHERE symbol=$1").bind(symbol.to_ascii_uppercase()).execute(&store.pool).await?;
                    println!("{symbol}: {} disabled; history retained", result.rows_affected());
                }
            }
        },
        Command::Serve { bind, token, bootstrap_minutes, retention_days } => {
            // Separate connection holds the lock throughout the process lifetime.
            let mut lease = sqlx::PgConnection::connect(&cli.database_url).await?;
            let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(4815172342)").fetch_one(&mut lease).await?;
            if !acquired { bail!("another et-data-service collector already owns this database"); }
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let depth = et_data_service::depth::DepthCache::default();
            let depth_worker = tokio::spawn(et_data_service::depth::run(store.clone(), exchange.clone(), depth.clone(), stop_rx.clone()));
            #[cfg(feature = "hyperliquid")]
            let hl_worker = tokio::spawn(et_data_service::hyperliquid::run(store.clone(), hl_config, stop_rx.clone()));
            #[cfg(not(feature = "hyperliquid"))]
            sqlx::query("UPDATE hl_status SET enabled=false,connected=false,error=NULL WHERE id=1")
                .execute(&store.pool).await?;
            let worker = tokio::spawn(collector::run(store.clone(), exchange, bootstrap_minutes, retention_days, stop_rx));
            tracing::info!(%bind, "history API ready; initialize symbols with `init` if needed");
            let shutdown = async move {
                #[cfg(unix)]
                {
                    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
                    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
                }
                #[cfg(not(unix))]
                let _ = tokio::signal::ctrl_c().await;
                let _ = stop_tx.send(true);
            };
            let token = token.filter(|value| !value.is_empty());
            axum::serve(listener, router(Api { store, token, depth })).with_graceful_shutdown(shutdown).await?;
            worker.await??;
            depth_worker.await??;
            #[cfg(feature = "hyperliquid")]
            hl_worker.await??;
            lease.close().await?;
        }
    }
    Ok(())
}
