use anyhow::{ensure, Result};
use market_history::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;

pub fn address(value: &str) -> Result<String> {
    ensure!(
        value.len() == 42
            && value.starts_with("0x")
            && value[2..].bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid Hyperliquid address"
    );
    Ok(value.to_ascii_lowercase())
}
pub fn decimal(value: &str) -> Result<Decimal> {
    Ok(Decimal::from_str(value)?)
}
pub fn positive_optional(value: &Option<String>) -> Result<Option<Decimal>> {
    value
        .as_deref()
        .map(decimal)
        .transpose()
        .map(|p| p.filter(|p| *p > Decimal::ZERO))
}

#[derive(Clone, Debug, Deserialize)]
pub struct Trade {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub tid: u64,
    pub users: [String; 2],
}
impl Trade {
    pub fn validate(&self, now: u64) -> Result<()> {
        ensure!(
            !self.coin.is_empty() && self.coin.len() <= 80,
            "invalid trade coin"
        );
        ensure!(
            matches!(self.side.as_str(), "B" | "A"),
            "invalid trade side"
        );
        ensure!(
            self.time > 0 && self.time <= now + 5_000 && self.tid <= i64::MAX as u64,
            "invalid trade time/id"
        );
        ensure!(
            decimal(&self.px)? > Decimal::ZERO && decimal(&self.sz)? > Decimal::ZERO,
            "invalid trade price/size"
        );
        address(&self.users[0])?;
        address(&self.users[1])?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountState {
    pub time: u64,
    pub asset_positions: Vec<AssetPosition>,
}
#[derive(Debug, Deserialize)]
pub struct AssetPosition {
    pub position: Position,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub coin: String,
    pub szi: String,
    pub entry_px: Option<String>,
    pub liquidation_px: Option<String>,
    pub margin_used: String,
    pub leverage: Value,
}
impl AccountState {
    pub fn validate(&self, now: u64) -> Result<()> {
        ensure!(
            self.time > 0 && self.time <= now + 5_000 && now.saturating_sub(self.time) <= 60_000,
            "stale or future account state"
        );
        let mut coins = std::collections::HashSet::new();
        ensure!(
            self.asset_positions.len() <= 2_000,
            "oversized account state"
        );
        for item in &self.asset_positions {
            let p = &item.position;
            ensure!(
                !p.coin.is_empty() && p.coin.len() <= 80 && coins.insert(&p.coin),
                "invalid/duplicate position coin"
            );
            decimal(&p.szi)?;
            positive_optional(&p.entry_px)?;
            positive_optional(&p.liquidation_px)?;
            ensure!(
                decimal(&p.margin_used)? >= Decimal::ZERO,
                "negative margin used"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshot_preserves_unknown_liquidation_and_rejects_stale_or_invalid_state() {
        let mut state: AccountState = serde_json::from_value(serde_json::json!({"time":100_000,"assetPositions":[{"position":{"coin":"BTC","szi":"-2","entryPx":"50000","liquidationPx":null,"marginUsed":"1000","leverage":{"type":"cross","value":10}}}]})).unwrap();
        state.validate(100_000).unwrap();
        assert_eq!(
            positive_optional(&state.asset_positions[0].position.liquidation_px).unwrap(),
            None
        );
        assert!(state.validate(200_000).is_err());
        state.asset_positions[0].position.szi = "NaN".into();
        assert!(state.validate(100_000).is_err());
    }
    #[test]
    fn rejects_ambiguous_sides_and_bad_addresses() {
        let mut t: Trade = serde_json::from_value(serde_json::json!({"coin":"BTC","side":"B","px":"10","sz":"2","time":100,"tid":2,"users":["0x0000000000000000000000000000000000000001","0x0000000000000000000000000000000000000002"]})).unwrap();
        t.validate(100).unwrap();
        t.side = "Open Long".into();
        assert!(t.validate(100).is_err());
        assert!(address("0xINVALID").is_err());
    }
}
