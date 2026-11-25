CREATE TABLE symbols (
    symbol TEXT PRIMARY KEY,
    metadata JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    market_cap_rank INTEGER,
    ranking_source TEXT,
    configured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE collector_progress (
    symbol TEXT PRIMARY KEY REFERENCES symbols(symbol),
    last_id BIGINT,
    last_trade_time BIGINT,
    coverage_start BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One exact-price footprint per minute; the primary key serves paginated reads.
CREATE TABLE candles_1m (
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    bucket BIGINT NOT NULL,
    data JSONB NOT NULL,
    PRIMARY KEY (symbol, bucket)
);
