CREATE TABLE hl_fills (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    coin TEXT NOT NULL,
    time_ms BIGINT NOT NULL,
    tid BIGINT NOT NULL,
    fill JSONB NOT NULL,
    PRIMARY KEY(address, coin, tid)
);
CREATE INDEX hl_fills_time ON hl_fills(coin, time_ms);
CREATE TABLE hl_fill_sync (
    address TEXT PRIMARY KEY REFERENCES hl_addresses(address) ON DELETE CASCADE,
    next_ms BIGINT NOT NULL,
    updated_ms BIGINT NOT NULL,
    error TEXT
);
