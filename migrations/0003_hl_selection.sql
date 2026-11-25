-- Observed behavior, not asserted trader identity. These features never alter
-- public trade statistics; they only select the account-position sample.
ALTER TABLE hl_status ADD COLUMN min_position_usd DOUBLE PRECISION NOT NULL DEFAULT 10000;
ALTER TABLE hl_status ADD COLUMN sample_budget BIGINT NOT NULL DEFAULT 256;
CREATE TABLE hl_address_activity_1m (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    bucket_ms BIGINT NOT NULL,
    trades BIGINT NOT NULL,
    maker_trades BIGINT NOT NULL,
    buy_notional NUMERIC NOT NULL,
    sell_notional NUMERIC NOT NULL,
    active_seconds BIGINT NOT NULL,
    first_ms BIGINT NOT NULL,
    last_ms BIGINT NOT NULL,
    PRIMARY KEY(address, bucket_ms)
);
CREATE INDEX hl_address_activity_time ON hl_address_activity_1m(bucket_ms);

CREATE TABLE hl_address_selection (
    address TEXT PRIMARY KEY REFERENCES hl_addresses(address) ON DELETE CASCADE,
    selected BOOLEAN NOT NULL DEFAULT FALSE,
    excluded_reason TEXT,
    evidence JSONB NOT NULL DEFAULT '{}',
    evaluated_ms BIGINT NOT NULL
);
CREATE INDEX hl_address_selection_members ON hl_address_selection(selected);

-- Bootstrap behavior from trades we already collected, without remote history
-- downloads. Replay still uses the market-trade primary key for deduplication.
INSERT INTO hl_address_activity_1m(address,bucket_ms,trades,maker_trades,buy_notional,sell_notional,active_seconds,first_ms,last_ms)
SELECT u.address,t.time_ms / 60000 * 60000,count(*),
       count(*) FILTER(WHERE (u.is_buy AND t.side='A') OR (NOT u.is_buy AND t.side='B')),
       COALESCE(sum(t.price*t.quantity) FILTER(WHERE u.is_buy),0),
       COALESCE(sum(t.price*t.quantity) FILTER(WHERE NOT u.is_buy),0),
       bit_or(1::bigint << ((t.time_ms / 1000) % 60)::int),min(t.time_ms),max(t.time_ms)
FROM hl_trades t
CROSS JOIN LATERAL (VALUES(t.buyer,true),(t.seller,false)) u(address,is_buy)
WHERE t.time_ms >= (extract(epoch FROM now())*1000)::bigint - 3600000
GROUP BY u.address,t.time_ms / 60000 * 60000;
