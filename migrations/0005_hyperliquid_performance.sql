-- Derived complete position cycles; raw fills remain the source of truth.
CREATE TABLE hl_closed_cycles (
    address TEXT NOT NULL REFERENCES hl_addresses(address) ON DELETE CASCADE,
    cycle_index BIGINT NOT NULL,
    time_ms BIGINT NOT NULL,
    pnl DOUBLE PRECISION NOT NULL,
    PRIMARY KEY(address, cycle_index)
);
CREATE INDEX hl_closed_cycles_time ON hl_closed_cycles(time_ms);
CREATE INDEX hl_fills_address_time ON hl_fills(address,time_ms);
