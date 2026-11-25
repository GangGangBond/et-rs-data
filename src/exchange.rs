use anyhow::{bail, Context, Result};
use market_history::{AggTrade, Symbol};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Exchange {
    pub http: reqwest::Client,
    pub rest_url: String,
    pub ws_url: String,
    gate: Arc<Mutex<tokio::time::Instant>>,
}

impl Exchange {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent("et-data-service/0.1 (personal market history)")
                .timeout(Duration::from_secs(20))
                .build()?,
            rest_url: std::env::var("BINANCE_REST_URL")
                .unwrap_or_else(|_| "https://api.binance.com".into()),
            ws_url: std::env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws".into()),
            gate: Arc::new(Mutex::new(tokio::time::Instant::now())),
        })
    }

    /// A conservative global budget shared by all symbol workers and retries.
    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> Result<T> {
        let mut last = String::new();
        for attempt in 0..5 {
            {
                let mut next = self.gate.lock().await;
                tokio::time::sleep_until(*next).await;
                *next = tokio::time::Instant::now() + Duration::from_millis(500);
            }
            let response = self
                .http
                .get(format!("{}{}", self.rest_url.trim_end_matches('/'), path))
                .query(params)
                .send()
                .await?;
            let status = response.status();
            if status.is_success() {
                return Ok(response.json().await?);
            }
            if status.as_u16() == 429 || status.as_u16() == 418 || status.is_server_error() {
                let seconds = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2_u64.pow(attempt + 1));
                *self.gate.lock().await =
                    tokio::time::Instant::now() + Duration::from_secs(seconds.max(1));
                last = format!("Binance HTTP {status}");
            } else {
                bail!("Binance {}: {}", status, response.text().await?);
            }
        }
        bail!("{last}; retry budget exhausted")
    }

    pub async fn symbols(&self) -> Result<HashMap<String, Symbol>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Info {
            symbol: String,
            status: String,
            base_asset: String,
            quote_asset: String,
            base_asset_precision: u8,
            is_spot_trading_allowed: bool,
            filters: Vec<serde_json::Value>,
        }
        #[derive(Deserialize)]
        struct Response {
            symbols: Vec<Info>,
        }
        let response: Response = self
            .get(
                "/api/v3/exchangeInfo",
                &[("showPermissionSets".into(), "false".into())],
            )
            .await?;
        let mut symbols = HashMap::new();
        for item in response.symbols {
            if item.status != "TRADING"
                || !item.is_spot_trading_allowed
                || item.quote_asset != "USDT"
            {
                continue;
            }
            let tick = item
                .filters
                .iter()
                .find(|f| f["filterType"] == "PRICE_FILTER")
                .and_then(|f| f["tickSize"].as_str())
                .context("missing price tick")?;
            let min_tick_size: market_history::Decimal = tick.parse()?;
            if min_tick_size <= market_history::Decimal::ZERO {
                continue;
            }
            let symbol = Symbol {
                symbol: item.symbol,
                base_asset: item.base_asset,
                quote_asset: item.quote_asset,
                price_precision: min_tick_size.normalize().scale() as u8,
                volume_precision: item.base_asset_precision,
                min_tick_size,
            };
            symbols.insert(symbol.symbol.clone(), symbol);
        }
        Ok(symbols)
    }

    pub async fn depth(&self, symbol: &str) -> Result<market_depth::book::BinanceDepthSnapshot> {
        self.get(
            "/api/v3/depth",
            &[
                ("symbol".into(), symbol.into()),
                ("limit".into(), "1000".into()),
            ],
        )
        .await
    }

    pub async fn trades(
        &self,
        symbol: &str,
        from_id: Option<u64>,
        start: Option<u64>,
    ) -> Result<Vec<AggTrade>> {
        let mut query = vec![
            ("symbol".into(), symbol.into()),
            ("limit".into(), "1000".into()),
        ];
        if let Some(id) = from_id {
            query.push(("fromId".into(), id.to_string()));
        } else if let Some(time) = start {
            query.push(("startTime".into(), time.to_string()));
        }
        self.get("/api/v3/aggTrades", &query).await
    }

    pub async fn top_symbols(&self, count: usize) -> Result<Vec<(Symbol, i32)>> {
        #[derive(Deserialize)]
        struct Coin {
            id: String,
            symbol: String,
            market_cap_rank: Option<i32>,
        }
        let base = std::env::var("COINGECKO_URL")
            .unwrap_or_else(|_| "https://api.coingecko.com/api/v3".into());
        let fetch = |category: Option<&str>| {
            let mut request = self.http.get(format!("{base}/coins/markets")).query(&[
                ("vs_currency", "usd"),
                ("order", "market_cap_desc"),
                ("per_page", "250"),
                ("page", "1"),
            ]);
            if let Some(category) = category {
                request = request.query(&[("category", category)]);
            }
            if let Ok(key) = std::env::var("COINGECKO_API_KEY") {
                if !key.is_empty() {
                    request = request.header("x-cg-demo-api-key", key);
                }
            }
            request
        };
        let markets: Vec<Coin> = fetch(None).send().await?.error_for_status()?.json().await?;
        let mut excluded = HashSet::new();
        for category in ["stablecoins", "wrapped-tokens"] {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let coins: Vec<Coin> = fetch(Some(category))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            excluded.extend(coins.into_iter().map(|coin| coin.id));
        }
        let tradable = self.symbols().await?;
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for coin in markets {
            if excluded.contains(&coin.id) {
                continue;
            }
            let pair = format!("{}USDT", coin.symbol.to_ascii_uppercase());
            if let (Some(symbol), Some(rank)) = (tradable.get(&pair), coin.market_cap_rank) {
                if seen.insert(pair) {
                    result.push((symbol.clone(), rank));
                }
            }
            if result.len() == count {
                return Ok(result);
            }
        }
        bail!(
            "only {} eligible pairs found; no symbols changed",
            result.len()
        )
    }
}
