# Crypto Stream Analytics

Real-time crypto market analytics pipeline. Live trades from the Coinbase public WebSocket are streamed through **Apache Kafka** and **Spark Structured Streaming** into **TimescaleDB** and **HDFS/Hive**, scored by a **Spark MLlib** anomaly-detection model, and shown on a **React** dashboard that updates every second.

> Status: in development. Live dashboard, API, stream processing and storage are working; HDFS/Hive and the ML model are next.

![Live dashboard](docs/dashboard.png)

---

## Architecture

```
Coinbase WebSocket ──► Ingestion (Python) ──► Kafka
                                               │
                                               ▼
                                     Spark Structured Streaming
                                      │                    │
                          upsert, 1 s │                    │ Parquet, 5 min
                                      ▼                    ▼
                                 TimescaleDB          HDFS + Hive
                                      │                    │ nightly
                                      ▼                    ▼
                               FastAPI + Redis      Spark SQL + MLlib
                                      │              (summaries, model)
                        WebSocket, 1 s│
                                      ▼
                               React dashboard
```

- **Hot path**: trade to screen in under 3 seconds.
- **Cold path**: full history in HDFS, nightly batch analytics and model training.

## Features

- Live price tickers, candlestick charts and volume for BTC, ETH, SOL, XRP and DOGE
- 1-minute and 5-minute OHLCV candles, VWAP, moving averages and rolling volatility
- Real-time anomaly alerts (volume spikes, sharp price moves) from a Spark MLlib model
- Batch analytics from Hive: daily volatility, hourly volume heatmap, pair correlation
- Pipeline health page: messages per second, Kafka consumer lag, end-to-end latency

## Tech stack

| Layer | Tools |
| --- | --- |
| Data source | Coinbase Advanced Trade public WebSocket |
| Streaming | Apache Kafka |
| Processing | Spark Structured Streaming, Spark SQL |
| Machine learning | Spark MLlib |
| Data lake | Hadoop HDFS, Parquet, Hive |
| Database | PostgreSQL + TimescaleDB, Redis |
| Backend | FastAPI (REST + WebSockets) |
| Frontend | React, TradingView lightweight-charts, served by nginx |
| Analysis | Pandas, Matplotlib, Seaborn |
| DevOps | Docker Compose, GitHub Actions |

## Getting started

Requirements: Docker Desktop, plus Python 3.12+ if you want to run services outside Docker.

```bash
git clone https://github.com/vishant0815/crypto-stream-analytics.git
cd crypto-stream-analytics
cp .env.example .env

# Start everything: Kafka, TimescaleDB, Redis, ingestion, db-writer, Spark, API and dashboard
docker compose --env-file .env -f docker/docker-compose.yml up -d --build

# Watch live stats
docker logs -f ingestion        # trades/s and tickers/s from Coinbase
docker logs -f db-writer        # rows/s written, duplicates skipped, latency
docker logs -f spark-streaming  # input rate, batch time, state size, watermark
```

- **Dashboard: http://localhost:3000**
- API docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8080 (messages arriving in `raw.trades`, `raw.ticker` and `agg.metrics`)
- Spark UI: http://localhost:4040 (Structured Streaming tab: query progress and batch durations)
- Database: `psql postgresql://crypto:crypto@localhost:5432/crypto`

Indicators need 15 minutes of continuous data before they appear, and the 60-minute average needs an hour.

Already ran an earlier version? The schema only runs automatically on an empty database, so apply any migrations you don't have yet, in order:

```bash
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/002_candles_and_indicators.sql
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/003_trade_replay_flag.sql
```

**Run the dashboard in development** (hot reload, talks to the API on port 8000):

```bash
cd dashboard
npm install
npm run dev     # http://localhost:5173
```

Try the analysis queries (candles, VWAP, top movers, latency percentiles, spreads):

```bash
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/queries/analysis.sql
```

**Run the ingestion service without Kafka** (prints live trades to the terminal):

```bash
cd ingestion
pip install -r requirements-dev.txt
python -m app.main --dry-run
```

**Run the tests**:

```bash
cd ingestion && pytest
cd ../db-writer && pytest
cd ../spark && pytest   # needs Java 17 or 21
```

The db-writer and API integration tests **delete data**, so they only run against a separate database whose name ends in `_test`, and refuse to start otherwise:

```bash
createdb -h localhost -U crypto crypto_test
for f in db/migrations/*.sql; do psql postgresql://crypto:crypto@localhost:5432/crypto_test -f "$f"; done

cd db-writer && TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto_test pytest
cd ../api && TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto_test \
             TEST_REDIS_URL=redis://localhost:6379/15 pytest
```

The API tests also need Kafka on `localhost:9092`; they create and use their own `test.*` topics, so they don't touch live data.

### Kafka topics

| Topic | Partitions | Retention | Contents |
| --- | --- | --- | --- |
| `raw.trades` | 6 | 7 days | Every trade, keyed by pair |
| `raw.ticker` | 6 | 2 days | Last price, 24h stats, best bid/ask |
| `agg.metrics` | 3 | 1 day | Candles and indicators (from Spark) |
| `alerts` | 1 | 30 days | Anomaly alerts (from Spark ML) |
| `dlq.trades` | 1 | 7 days | Messages that failed validation |

### Database design

| Table | Type | Written by | Retention |
| --- | --- | --- | --- |
| `pairs` | Reference table | Seed data, db-writer | Kept |
| `trades` | Hypertable, 1-day chunks, compressed after 7 days | db-writer | 30 days (full history in HDFS) |
| `ticker` | Hypertable, compressed after 2 days | db-writer | 7 days |
| `candles_1m` | Hypertable | Spark Streaming | Kept |
| `candles_5m` | Continuous aggregate over `candles_1m`, refreshed every minute | TimescaleDB | Kept |
| `candles_1h` | Continuous aggregate over `candles_1m`, refreshed every 5 min | TimescaleDB | Kept |
| `candle_indicators` | Hypertable: SMA 5/15/60, VWAP 15, volatility, RSI 14, volume ratio | Spark Streaming | Kept |
| `alerts` | Hypertable | Spark ML | Kept |
| `latest_ticker`, `latest_indicators` | Views (`DISTINCT ON`) | | |

The db-writer loads each batch with `COPY` into a staging table, then `INSERT ... ON CONFLICT DO NOTHING`. Kafka offsets are committed only after the database commit, so a crash replays the batch and duplicates are skipped: each trade is stored exactly once.

### Stream processing (Spark Structured Streaming)

One Spark job runs two queries on `raw.trades`, both with a 30-second watermark on exchange time and de-duplication by trade ID (Coinbase resends recent trades after every reconnect):

| Query | Trigger | Output mode | Writes |
| --- | --- | --- | --- |
| `live-candles` | 1 s | update | The current minute's candle, changing with every trade, to `candles_1m` and `agg.metrics` |
| `indicators` | 10 s | append | Each candle once its minute is final; loads the previous hour from TimescaleDB over JDBC and computes indicators with Spark window functions, to `candle_indicators` and `agg.metrics` |

- Moving windows are **time-based** (`rangeBetween` on the minute), so a minute with no trades doesn't stretch a 15-minute average over 16 minutes, and an indicator stays null until its window is full.
- Window state is kept in **RocksDB** and checkpointed, so after a restart the job continues from its last Kafka offsets with its open windows intact.
- Every write is an **upsert**, so a micro-batch that is re-run after a failure changes nothing.

### API and dashboard

The FastAPI service reads Kafka (`raw.ticker`, `raw.trades`, `agg.metrics`) and pushes **one batched update per second** to each browser over a WebSocket, so 50 trades in a second become one message. History comes from TimescaleDB over REST.

| Endpoint | Returns |
| --- | --- |
| `GET /api/candles?pair=BTC-USD&interval=1m\|5m\|1h&limit=300` | OHLCV candles, oldest first; for `1m`, the matching indicators |
| `GET /api/overview` | Every pair: price, 24 h change, 60-minute sparkline, latest indicators |
| `GET /api/trades/recent?pair=BTC-USD&limit=40` | Latest trades, newest first |
| `GET /api/pipeline` | Ingestion latency percentiles, trades per minute, data freshness, storage, live message rates |
| `GET /api/pairs`, `GET /api/health` | Pairs; status of database, Redis and Kafka |
| `WS /ws/live?pairs=BTC-USD,ETH-USD` | A snapshot, then about one update per second |

- **Redis** caches database responses for 2 seconds, so the database sees at most one query per endpoint every 2 seconds however many people are watching, and holds the latest live state so a new browser gets prices immediately. If Redis is down, requests go straight to the database.
- API database connections are **read-only** (`default_transaction_read_only`), with a 5-second statement timeout.
- A browser too slow to take its messages is dropped instead of holding back everyone else.
- The dashboard (React and TradingView's lightweight-charts) loads history once and then only applies pushed updates; 5m and 1h bars are rebuilt live from the 1-minute candles.
- In Docker, nginx serves the dashboard and forwards `/api` and `/ws` to the API, so the browser talks to one origin.
- Coinbase replays a batch of recent trades every time a client connects. They're flagged as `is_replay`, still counted in candles, and left out of the latency figures.

## Project structure

```
crypto-stream-analytics/
├── ingestion/          # WebSocket client and Kafka producer
├── db-writer/          # Kafka consumer writing trades to TimescaleDB
├── api/                # FastAPI: REST, WebSocket, Redis cache
├── dashboard/          # React dashboard, served by nginx in Docker
├── spark/
│   ├── streaming/      # candles and indicators job (Structured Streaming)
│   └── tests/          # unit tests for the candle and indicator logic
├── db/
│   ├── migrations/     # schema, hypertables, policies
│   └── queries/        # analysis queries
├── hive/               # external table definitions
├── notebooks/          # exploratory data analysis
├── docker/             # Dockerfiles and docker-compose.yml
└── .github/workflows/  # CI
```

## Roadmap

- [x] Ingestion service and Kafka setup
- [x] TimescaleDB schema and first queries
- [x] Spark streaming: candles and indicators
- [x] FastAPI and live dashboard
- [ ] HDFS archive, Hive tables, batch analytics
- [ ] ML anomaly detection and live scoring
- [ ] Pipeline monitoring, tests and CI

## Author

**Vishantkumar Bhavsar**
