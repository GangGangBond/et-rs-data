use crate::store::Store;
use axum::{
    extract::{Query, State},
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone)]
pub struct Api {
    pub store: Store,
    pub token: Option<String>,
    pub depth: crate::depth::DepthCache,
}

async fn auth(State(api): State<Api>, request: Request<axum::body::Body>, next: Next) -> Response {
    if let Some(token) = &api.token {
        let provided = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if provided != Some(format!("Bearer {token}").as_str()) {
            return (StatusCode::UNAUTHORIZED, "invalid API token").into_response();
        }
    }
    next.run(request).await
}

pub fn router(api: Api) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/symbols", get(symbols))
        .route("/v1/history", get(history))
        .route("/v1/depth", get(depth))
        .route("/v1/hyperliquid", get(hyperliquid))
        .layer(middleware::from_fn_with_state(api.clone(), auth))
        .with_state(api)
}

#[derive(Deserialize)]
struct HyperliquidQuery {
    symbol: String,
    #[serde(default)]
    details: bool,
}
async fn hyperliquid(State(api): State<Api>, Query(query): Query<HyperliquidQuery>) -> Response {
    if !(5..=40).contains(&query.symbol.len())
        || !query.symbol.bytes().all(|b| b.is_ascii_alphanumeric())
        || !query.symbol.to_ascii_uppercase().ends_with("USDT")
    {
        return (
            StatusCode::BAD_REQUEST,
            "invalid Hyperliquid spot-symbol mapping",
        )
            .into_response();
    }
    match crate::hyperliquid::evidence_with_details(
        &api.store,
        &query.symbol.to_ascii_uppercase(),
        query.details,
    )
    .await
    {
        Ok(evidence) => Json(evidence).into_response(),
        Err(error) => {
            tracing::warn!(%error,"Hyperliquid evidence query failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Hyperliquid evidence unavailable",
            )
                .into_response()
        }
    }
}

async fn health(State(api): State<Api>) -> Response {
    match sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&api.store.pool).await {
        Ok(_) => Json(serde_json::json!({"status":"ok", "apiVersion":1, "market":"binance-spot", "realtime":"client-direct", "depthWindowSeconds":300, "depthWindowVersion":1})).into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response(),
    }
}

async fn symbols(State(api): State<Api>) -> Response {
    match api.store.enabled().await {
        Ok(symbols) => Json(symbols).into_response(),
        Err(error) => {
            tracing::error!(%error);
            (StatusCode::SERVICE_UNAVAILABLE, "database unavailable").into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub symbol: String,
    #[serde(default = "default_period")]
    pub period: String,
    pub before: Option<u64>,
    #[serde(default = "default_count")]
    pub count: usize,
    /// First buffered direct-WS ID. Wait for the server to persist through it.
    pub through_id: Option<u64>,
}
fn default_period() -> String {
    "15m".into()
}
fn default_count() -> usize {
    240
}

async fn history(State(api): State<Api>, Query(query): Query<HistoryQuery>) -> Response {
    if market_history::period_ms(&query.period).is_err()
        || query.count == 0
        || query.count > 300
        || query.before.is_some_and(|v| v > i64::MAX as u64)
        || query.through_id.is_some_and(|v| v > i64::MAX as u64)
    {
        return (
            StatusCode::BAD_REQUEST,
            "invalid period/count/time/id (count must be 1..300)",
        )
            .into_response();
    }
    let symbol = query.symbol.to_ascii_uppercase();
    if let Some(target) = query.through_id {
        let wait = async {
            loop {
                let progress = api.store.checkpoint(&symbol).await?;
                if progress.is_some_and(|id| id >= target) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(25), wait).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%symbol,%error);
                return (
                    StatusCode::NOT_FOUND,
                    "symbol not configured or unavailable",
                )
                    .into_response();
            }
            Err(_) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "collector has not reached the live handoff ID; retry after it catches up",
                )
                    .into_response()
            }
        }
    }
    match api
        .store
        .history(&symbol, &query.period, query.before, query.count)
        .await
    {
        Ok(history) => Json(history).into_response(),
        Err(error) => {
            tracing::warn!(%symbol,%error);
            (StatusCode::NOT_FOUND, "history unavailable for this symbol").into_response()
        }
    }
}

#[derive(Deserialize)]
struct DepthQuery {
    symbol: String,
    generation: Option<u64>,
    after: Option<u64>,
}
async fn depth(State(api): State<Api>, Query(query): Query<DepthQuery>) -> Response {
    if query.symbol.len() > 40
        || !query.symbol.bytes().all(|c| c.is_ascii_alphanumeric())
        || query.after.is_some_and(|a| a > i64::MAX as u64 / 1000)
    {
        return (StatusCode::BAD_REQUEST, "invalid depth query").into_response();
    }
    match api
        .depth
        .get(
            &query.symbol.to_ascii_uppercase(),
            query.generation,
            query.after,
        )
        .await
    {
        Some(window) => Json(window).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "depth symbol not enabled or initializing",
        )
            .into_response(),
    }
}
