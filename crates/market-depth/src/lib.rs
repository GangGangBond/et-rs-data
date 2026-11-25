//! Shared book reconstruction and sampling contract for server and clients.
use serde::{Deserialize, Serialize};
pub mod book;
pub mod window;
pub use window::{DepthSample, DepthWindow};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeatmapOrderRun {
    pub id: u64,
    pub start_time: u64,
    pub end_time: u64,
    pub price: f64,
    pub quantity: f64,
    pub is_bid: bool,
    pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeatmapRunClose {
    pub id: u64,
    pub end_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeatmapData {
    pub symbol: String,
    pub tick_size: f64,
    pub latest_time: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub runs: Vec<HeatmapOrderRun>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeatmapDepthUpdate {
    pub symbol: String,
    pub tick_size: f64,
    pub timestamp: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub started_runs: Vec<HeatmapOrderRun>,
    pub closed_runs: Vec<HeatmapRunClose>,
}

/// Exact exchange-tick levels, best price first on both sides. A complete bounded snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub timestamp: u64,
    pub tick_size: f64,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
}
