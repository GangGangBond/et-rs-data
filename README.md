# et-data-service

English | [简体中文](README.zh-CN.md)

A small PostgreSQL-backed service that records **Binance Spot USDT footprint
history** for the et-rs desktop client. It stores 1-minute OHLCV plus exact
price-level bid/ask volume, serves rolled-up 1m/5m/15m/30m/1h bars over HTTP,
and keeps a 5-minute in-memory depth window for order-flow views.

> Personal-use project, not a general-purpose market-data platform. It does not
> forward realtime prices; et-rs keeps using Binance directly for live data.
> Hyperliquid collection is feature-gated and paused by default.

## What it does

- Collects Binance Spot `aggTrade` streams for a configurable set of USDT pairs.
- Stores 1m candles with per-price bid/ask volume (footprint) in PostgreSQL.
- Serves `1m`, `5m`, `15m`, `30m`, and `1h` bars, rolled up at query time.
- Returns coverage, checkpoint and pagination metadata; decimals are strings, not floats.
- Keeps a 5-minute, 1-second-resolution depth window in memory for `/v1/depth`.
- Commits candle writes and the aggTrade checkpoint in one transaction; replays are
  idempotent and gaps are recovered from Binance REST.
- Uses only PostgreSQL: no Redis, Kafka, or TimescaleDB. A PostgreSQL advisory lock
  prevents two collectors on the same database.
- Optional retention cleanup (default 30 days).
- Optional, feature-gated Hyperliquid observation (disabled by default).

## Repository layout

```text
src/                  HTTP API, collectors, depth cache
crates/market-history Decimal OHLCV/footprint aggregation and shared API types
crates/market-depth   order-book and depth-window types
migrations/           sqlx migrations (applied automatically by `serve`)
tests/                unit tests plus ignored PostgreSQL integration tests
scripts/              optional nginx reverse-proxy installer
```

`market-history` and `market-depth` are also used by et-rs, so the wire types and
aggregation rules stay shared.

## Quick start (Docker)

```bash
cp .env.example .env
docker compose -p et-history up -d postgres
docker compose -p et-history build data
docker compose -p et-history run --rm data init
docker compose -p et-history up -d data
docker compose -p et-history logs -f data
```

The API is at `http://127.0.0.1:8088`; PostgreSQL is published on
`127.0.0.1:5433`. `init` seeds the current top 10 eligible pairs only when the
database is empty; `serve` applies migrations but never changes the symbol list.

To stop the service without deleting history:

```bash
docker compose -p et-history down
```

Do not add `-v` unless you want to delete the database volume.

## Local build

Rust 1.97+ and PostgreSQL 17 (or any recent PostgreSQL).

```bash
export DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history'
cargo build --release --locked
./target/release/et-data-service init
./target/release/et-data-service serve
```

The binary does not read `.env`; export the variables in your shell. Docker
Compose reads `.env` automatically.

## CLI

```bash
et-data-service symbols list
et-data-service symbols add AVAXUSDT LTCUSDT
et-data-service symbols remove LTCUSDT
et-data-service symbols seed-top --count 10
```

`remove` disables collection but keeps history; `add` re-enables it and
backfills the gap. `seed-top` adds or re-enables the current ranked pairs
without deleting existing ones.

## HTTP API

```bash
curl http://127.0.0.1:8088/health
curl http://127.0.0.1:8088/v1/symbols
curl 'http://127.0.0.1:8088/v1/history?symbol=BTCUSDT&period=15m&count=240'
curl 'http://127.0.0.1:8088/v1/history?symbol=BTCUSDT&period=1m&before=1770000000000&count=240'
curl 'http://127.0.0.1:8088/v1/depth?symbol=BTCUSDT'
```

| Endpoint | Description |
|---|---|
| `GET /health` | Process/API health and capability flags. |
| `GET /v1/symbols` | Enabled symbols with metadata. |
| `GET /v1/history` | Rolled-up OHLCV + footprint bars. |
| `GET /v1/depth` | 5-minute depth window, or newer seconds after a cursor. |
| `GET /v1/hyperliquid` | Optional Hyperliquid observation (disabled by default). |

When `ET_DATA_TOKEN` is set, every request must send
`Authorization: Bearer <token>`.

`/v1/history` accepts `symbol`, `period` (`1m`, `5m`, `15m`, `30m`, `1h`),
`count` (1–300), `before` (exclusive UTC millisecond), and `through_id` (wait for
the collector to persist through this aggTrade ID, up to 25 seconds). Responses
include `bars`, `hasMore`, `coverageStart`, `lastAggregateId`, `lastTradeTime`,
and symbol metadata. Prices and quantities are decimal strings.

`/v1/depth` returns `version`, `symbol`, `generation`, `reset`, `server_time`,
`latest_ms`, and 1-second `samples`. `reset=true` means replace the local
window. Pass `generation` and `after` to receive only newer seconds.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string. |
| `ET_DATA_BIND` | `127.0.0.1:8088` | API listen address. |
| `ET_DATA_TOKEN` | empty | Bearer token; empty disables authentication. |
| `ET_DATA_BOOTSTRAP_MINUTES` | `60` | First backfill window for a new symbol (1–10080). |
| `ET_DATA_RETENTION_DAYS` | `30` | Delete candles older than N days; `0` disables cleanup. |
| `COINGECKO_API_KEY` | empty | Optional Demo API key for `init` / `seed-top`. |
| `COINGECKO_URL` | `https://api.coingecko.com/api/v3` | Override the ranking API. |
| `BINANCE_REST_URL` | `https://api.binance.com` | Override the Binance REST endpoint. |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/ws` | Override the Binance WebSocket endpoint. |

## How data is stored

- One row per symbol per minute in `candles_1m`, containing OHLCV and the exact
  price levels split into bid/ask volume.
- The aggTrade checkpoint and candle mutations commit in one transaction; a replay
  does not double-count.
- Missing aggTrade IDs trigger a bounded REST recovery. If Binance cannot provide
  the missing range, the checkpoint is preserved and the collector retries.
- Higher periods are rolled up at query time; partial bars at the coverage
  boundary are omitted.
- Retention cleanup runs every 5 minutes in small batches; disabled symbols keep
  their history.
- No tick archive, no historical depth archive, and no realtime WebSocket
  forwarding.
- `init` seeds the top 10 by CoinGecko market cap, excluding stablecoins and
  wrapped tokens, intersected with Binance Spot USDT pairs. It is not a plain
  global top-10 list.

## Optional Hyperliquid data (paused)

The `hyperliquid` feature is retained but disabled by default. The default build
does not start collection, and `serve` marks the stored Hyperliquid status
disabled. Re-enabling requires a build with `--features hyperliquid` and
`ET_HL_ENABLED=true`; changing the environment variable alone is not enough. See
`src/hyperliquid/` for the current implementation and selection rules.

## et-rs integration

In et-rs, open **Settings → Market data** and set the optional **Footprint
server** to `http://127.0.0.1:8088` (plus the token if configured). K-lines and
live prices still come from Binance; the server only supplies footprint history
and the depth window. If the server is unreachable, et-rs keeps working with its
normal Binance history.

## Tests

```bash
cargo test --workspace

# PostgreSQL integration tests use a dedicated database, never the collection DB.
ET_DATA_TEST_DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history_test' \
  cargo test --test postgres -- --ignored

ET_DATA_TEST_DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history_test' \
  cargo test --test hyperliquid_postgres -- --ignored
```

## Deployment notes

- Compose binds PostgreSQL to `127.0.0.1`; keep it that way unless you have a
  private network.
- Before exposing the API, set a non-empty `ET_DATA_TOKEN` and put it behind
  HTTPS.
- `scripts/install-nginx-proxy.sh` installs an optional Nginx reverse proxy with
  gzip for JSON responses.
- On a small machine, reduce build parallelism:
  `docker compose build --build-arg CARGO_BUILD_JOBS=1 data`.

## License

MIT. See [LICENSE](LICENSE).
