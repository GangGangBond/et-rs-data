//! Live-only Hyperliquid observations, separate from the authoritative spot feed.
mod collector;
mod model;
pub mod selection;
mod store;
pub use collector::{run, Config};
pub use model::{AccountState, Trade};
pub use store::{evidence, evidence_with_details, save_snapshot, save_trades};
