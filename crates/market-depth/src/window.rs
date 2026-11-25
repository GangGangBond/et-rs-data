use serde::{Deserialize, Serialize};

pub const RETENTION_SECONDS: u64 = 300;
pub const VERSION: u32 = 1;

/// One observed second. Levels use the same aggregation/tolerance as the local heatmap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthSample {
    pub timestamp: u64,
    /// Exchange event time of the book used at this sampling instant.
    pub observed_ms: u64,
    pub price: f64,
    pub tick: f64,
    pub levels: Vec<(f64, f64, bool)>,
}
impl DepthSample {
    pub fn second(&self) -> u64 {
        self.timestamp / 1000
    }
    pub fn valid(&self) -> bool {
        self.timestamp <= i64::MAX as u64
            && self.observed_ms > 0
            && self.observed_ms <= i64::MAX as u64
            && self.timestamp.abs_diff(self.observed_ms) <= 5000
            && self.price.is_finite()
            && self.price > 0.0
            && self.tick.is_finite()
            && self.tick > 0.0
            && !self.levels.is_empty()
            && self.levels.len() <= 2000
            && self
                .levels
                .iter()
                .all(|(p, q, _)| p.is_finite() && *p > 0.0 && q.is_finite() && *q > 0.0)
    }
}

/// A full window or strictly newer seconds after a cursor. Generation changes invalidate history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthWindow {
    pub version: u32,
    pub symbol: String,
    pub generation: u64,
    pub reset: bool,
    pub server_time: u64,
    pub latest_ms: u64,
    pub samples: Vec<DepthSample>,
}
impl DepthWindow {
    pub fn validate(&self, symbol: &str, now: i64) -> Result<(), String> {
        if self.version != VERSION
            || self.symbol != symbol
            || self.generation == 0
            || self.server_time > i64::MAX as u64
            || self.latest_ms > self.server_time.saturating_add(5000)
            || now.abs_diff(self.server_time as i64) > 5000
            || self.samples.len() > RETENTION_SECONDS as usize + 1
        {
            return Err("invalid or stale server depth window".into());
        }
        if self.samples.iter().any(|s| {
            !s.valid()
                || s.observed_ms > self.latest_ms
                || s.timestamp > self.server_time.saturating_add(5000)
                || s.second().saturating_add(RETENTION_SECONDS) < self.server_time / 1000
        }) || self
            .samples
            .windows(2)
            .any(|s| s[0].second() >= s[1].second() || s[0].tick != s[1].tick)
        {
            return Err("invalid depth samples or order".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_stale_wrong_symbol_unsorted_future_and_nonfinite_windows() {
        let sample = DepthSample {
            timestamp: 1_000_000,
            observed_ms: 1_000_000,
            price: 100.0,
            tick: 1.0,
            levels: vec![(99.0, 1.0, true)],
        };
        let valid = DepthWindow {
            version: VERSION,
            symbol: "BTCUSDT".into(),
            generation: 1,
            reset: true,
            server_time: 1_000_000,
            latest_ms: 1_000_000,
            samples: vec![sample],
        };
        assert!(valid.validate("BTCUSDT", 1_000_000).is_ok());
        assert!(valid.validate("ETHUSDT", 1_000_000).is_err());
        assert!(valid.validate("BTCUSDT", 1_006_000).is_err());
        let mut bad = valid.clone();
        bad.samples.push(bad.samples[0].clone());
        assert!(bad.validate("BTCUSDT", 1_000_000).is_err());
        let mut bad = valid.clone();
        bad.samples[0].timestamp += 6000;
        assert!(bad.validate("BTCUSDT", 1_000_000).is_err());
        let mut bad = valid.clone();
        bad.samples[0].levels[0].1 = f64::NAN;
        assert!(bad.validate("BTCUSDT", 1_000_000).is_err());
        let mut bad = valid;
        bad.samples[0].timestamp -= 301000;
        assert!(bad.validate("BTCUSDT", 1_000_000).is_err());
    }
}
