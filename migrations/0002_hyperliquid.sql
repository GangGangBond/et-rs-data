-- Independent perpetual evidence; never contributes to Binance spot candles.
CREATE TABLE hl_status (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    connected BOOLEAN NOT NULL DEFAULT FALSE,
    started_ms BIGINT,
    gap_count BIGINT NOT NULL DEFAULT 0,
    last_gap_ms BIGINT,
    error TEXT
);
INSERT INTO hl_status(id) VALUES(1);

CREATE TABLE hl_symbols (
    coin TEXT PRIMARY KEY,
    spot_symbol TEXT NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    context JSONB,
    context_ms BIGINT
);
CREATE TABLE hl_addresses (
    address TEXT PRIMARY KEY,
    first_seen_ms BIGINT NOT NULL,
    last_seen_ms BIGINT NOT NULL,
    attempted_ms BIGINT,
    snapshot_ms BIGINT,
    exchange_ms BIGINT,
    error TEXT
);
CREATE TABLE hl_address_coins (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    coin TEXT NOT NULL,
    PRIMARY KEY(address, coin)
);
CREATE INDEX hl_address_coins_coin ON hl_address_coins(coin);
CREATE INDEX hl_addresses_activity ON hl_addresses(last_seen_ms DESC);

CREATE TABLE hl_trades (
    coin TEXT NOT NULL,
    time_ms BIGINT NOT NULL,
    tid BIGINT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('B','A')),
    price NUMERIC NOT NULL CHECK(price > 0),
    quantity NUMERIC NOT NULL CHECK(quantity > 0),
    buyer TEXT NOT NULL,
    seller TEXT NOT NULL,
    PRIMARY KEY(coin, time_ms, tid)
);
CREATE TABLE hl_positions (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    coin TEXT NOT NULL,
    quantity NUMERIC NOT NULL CHECK(quantity <> 0),
    entry_price NUMERIC,
    liquidation_price NUMERIC,
    margin_used NUMERIC NOT NULL,
    leverage JSONB NOT NULL,
    exchange_ms BIGINT NOT NULL,
    observed_ms BIGINT NOT NULL,
    PRIMARY KEY(address, coin)
);
CREATE INDEX hl_trades_time ON hl_trades(time_ms);
CREATE INDEX hl_positions_coin ON hl_positions(coin, observed_ms);
CREATE TABLE hl_snapshots (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    observed_ms BIGINT NOT NULL,
    exchange_ms BIGINT NOT NULL,
    state JSONB NOT NULL,
    PRIMARY KEY(address, observed_ms)
);
CREATE INDEX hl_snapshots_time ON hl_snapshots(observed_ms);
