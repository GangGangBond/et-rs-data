# et-data-service

[English](README.md) | 简体中文

一个为 et-rs 桌面客户端提供 **Binance 现货 USDT 足迹历史** 的小型 PostgreSQL
服务。它保存 1 分钟 OHLCV 以及精确价位的主动买卖量，通过 HTTP 提供
1m/5m/15m/30m/1h 的合并 K 线和覆盖范围，并在内存中维护 5 分钟盘口窗口。

> 个人项目，不是通用行情平台。服务不转发实时价格；et-rs 的实时行情仍直连
> Binance。Hyperliquid 采集是 feature-gated 的，默认暂停。

## 功能

- 采集配置币种的 Binance 现货 `aggTrade`。
- PostgreSQL 保存 1 分钟 K 线，以及每个价位的 bid/ask 成交量（足迹）。
- 查询时合并 1m/5m/15m/30m/1h，返回覆盖范围、checkpoint 和分页信息。
- 价格和数量用十进制字符串传输，避免小价格币种丢精度。
- 内存维护 5 分钟、每秒采样的盘口窗口，供 `/v1/depth` 使用。
- K 线写入与 aggTrade checkpoint 同事务提交；重放幂等，缺号时从 Binance REST
  补齐。
- 只用 PostgreSQL，不需要 Redis/Kafka/TimescaleDB；advisory lock 防止同一
  数据库起两个采集器。
- 可选保留期清理（默认 30 天）。
- 可选、feature-gated 的 Hyperliquid 观测（默认关闭）。

## 目录

```text
src/                  HTTP API、采集器、盘口缓存
crates/market-history 十进制 OHLCV/足迹聚合与共享 API 类型
crates/market-depth   订单簿与盘口窗口类型
migrations/           sqlx 迁移（serve 自动执行）
tests/                单元测试 + 需要 PostgreSQL 的忽略测试
scripts/              可选的 Nginx 反向代理安装脚本
```

`market-history` 和 `market-depth` 也被 et-rs 使用，保证两边的聚合规则和传输
类型一致。

## Docker 快速启动

```bash
cp .env.example .env
docker compose -p et-history up -d postgres
docker compose -p et-history build data
docker compose -p et-history run --rm data init
docker compose -p et-history up -d data
docker compose -p et-history logs -f data
```

API 在 `http://127.0.0.1:8088`，PostgreSQL 映射到 `127.0.0.1:5433`。`init`
只在数据库为空时写入当前前十名币种；`serve` 会自动迁移，但不会改动币种配置。

停止服务但保留历史：

```bash
docker compose -p et-history down
```

不要加 `-v`，除非你想删除数据库卷。

## 本机构建

需要 Rust 1.97+ 和 PostgreSQL 17（或较新的 PostgreSQL）。

```bash
export DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history'
cargo build --release --locked
./target/release/et-data-service init
./target/release/et-data-service serve
```

程序本身不读取 `.env`，本机运行请在 shell 里 export；Docker Compose 会自动读取
`.env`。

## CLI

```bash
et-data-service symbols list
et-data-service symbols add AVAXUSDT LTCUSDT
et-data-service symbols remove LTCUSDT
et-data-service symbols seed-top --count 10
```

`remove` 停采但保留历史；`add` 重新启用并补齐停采期间的数据；`seed-top`
追加/启用当前排名币种，不删除原有配置。

## HTTP API

```bash
curl http://127.0.0.1:8088/health
curl http://127.0.0.1:8088/v1/symbols
curl 'http://127.0.0.1:8088/v1/history?symbol=BTCUSDT&period=15m&count=240'
curl 'http://127.0.0.1:8088/v1/history?symbol=BTCUSDT&period=1m&before=1770000000000&count=240'
curl 'http://127.0.0.1:8088/v1/depth?symbol=BTCUSDT'
```

| 接口 | 说明 |
|---|---|
| `GET /health` | 进程/API 健康与能力标记。 |
| `GET /v1/symbols` | 已启用币种及元数据。 |
| `GET /v1/history` | 合并后的 OHLCV + 足迹 K 线。 |
| `GET /v1/depth` | 5 分钟盘口窗口，或游标之后的新增秒。 |
| `GET /v1/hyperliquid` | 可选的 Hyperliquid 观测（默认关闭）。 |

设置 `ET_DATA_TOKEN` 后，所有请求都要带 `Authorization: Bearer <token>`。

`/v1/history` 参数：`symbol`、`period`（`1m`/`5m`/`15m`/`30m`/`1h`）、
`count`（1–300）、`before`（不包含的 UTC 毫秒边界）、`through_id`（等待采集器
提交到该 aggTrade ID，最多 25 秒）。响应包含 `bars`、`hasMore`、`coverageStart`、
`lastAggregateId`、`lastTradeTime` 和币种元数据；价格/数量是十进制字符串。

`/v1/depth` 返回 `version`、`symbol`、`generation`、`reset`、`server_time`、
`latest_ms` 和每秒的 `samples`。`reset=true` 表示要替换本地窗口；带上
`generation` 和 `after` 只取新增秒。

## 配置

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_URL` | 必填 | PostgreSQL 连接串。 |
| `ET_DATA_BIND` | `127.0.0.1:8088` | API 监听地址。 |
| `ET_DATA_TOKEN` | 空 | Bearer token；为空表示不鉴权。 |
| `ET_DATA_BOOTSTRAP_MINUTES` | `60` | 新币种首次回补窗口（1–10080）。 |
| `ET_DATA_RETENTION_DAYS` | `30` | 删除 N 天前的 K 线；`0` 表示不清理。 |
| `COINGECKO_API_KEY` | 空 | 可选的 Demo API key，用于 `init` / `seed-top`。 |
| `COINGECKO_URL` | `https://api.coingecko.com/api/v3` | 覆盖排名 API。 |
| `BINANCE_REST_URL` | `https://api.binance.com` | 覆盖 Binance REST 地址。 |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443/ws` | 覆盖 Binance WebSocket 地址。 |

## 数据行为

- `candles_1m` 每个币种每分钟一行，保存 OHLCV 和精确价位的 bid/ask 量。
- aggTrade checkpoint 与 K 线写入同事务提交；重复回放不会重复计数。
- aggTrade 缺号时触发有界 REST 补齐；如果 Binance 无法提供缺失区间，保留
  checkpoint 并重试。
- 大周期在查询时合并；覆盖边界处不完整的大周期不返回。
- 保留期清理每 5 分钟小批量执行；停采币种保留历史。
- 不保存逐笔成交归档、不保存历史盘口、不转发实时行情。
- `init` 的“前十”是 CoinGecko 市值排序、排除稳定币/包装币后，与 Binance 现货
  USDT 交易对取交集，不是简单的总榜前十。

## 可选 Hyperliquid（暂停）

`hyperliquid` feature 保留，但默认关闭。默认构建不启动采集，`serve` 会把数据库
中的 Hyperliquid 状态标记为关闭。恢复需要显式 `--features hyperliquid`
重新构建，并设置 `ET_HL_ENABLED=true`；只改环境变量不够。当前实现和筛选规则见
`src/hyperliquid/`。

## et-rs 接入

在 et-rs 的 **Settings → Market data** 中填写可选的 **Footprint server**：
`http://127.0.0.1:8088`，有 token 就一并填写。K 线和实时价格仍来自 Binance，本
服务只补足迹历史和盘口窗口；服务不可用时 et-rs 继续用 Binance 的正常历史。

## 测试

```bash
cargo test --workspace

# PostgreSQL 集成测试使用专用测试库，不要指向采集库。
ET_DATA_TEST_DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history_test' \
  cargo test --test postgres -- --ignored

ET_DATA_TEST_DATABASE_URL='postgres://et:et_local_dev@127.0.0.1:5433/et_history_test' \
  cargo test --test hyperliquid_postgres -- --ignored
```

## 部署提示

- Compose 默认只把 PostgreSQL 绑定到 `127.0.0.1`；没有私有网络时不要改成公网。
- 暴露 API 前设置非空 `ET_DATA_TOKEN`，并放在 HTTPS 反向代理后面。
- `scripts/install-nginx-proxy.sh` 可安装可选的 Nginx 反向代理，并对 JSON 开启
  gzip。
- 小内存机器可降低构建并行度：
  `docker compose build --build-arg CARGO_BUILD_JOBS=1 data`。

## 许可证

MIT，详见 [LICENSE](LICENSE)。
