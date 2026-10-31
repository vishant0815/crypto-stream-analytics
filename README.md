# Crypto Stream Analytics

Real-time crypto market analytics pipeline. Live trades from the Coinbase public WebSocket are streamed through **Apache Kafka** and **Spark Structured Streaming** into **TimescaleDB** and **HDFS/Hive**, scored by a **Spark MLlib** anomaly-detection model, and shown on a **React** dashboard that updates every second.

> Status: in development

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
| Frontend | React |
| Analysis | Pandas, Matplotlib, Seaborn |
| DevOps | Docker Compose, GitHub Actions |

## Getting started

Requirements: Docker Desktop, plus Python 3.12+ if you want to run services outside Docker.

```bash
git clone https://github.com/vishant0815/crypto-stream-analytics.git
cd crypto-stream-analytics
cp .env.example .env

# Start Kafka, TimescaleDB, ingestion and the db-writer
docker compose --env-file .env -f docker/docker-compose.yml up -d --build

# Watch live stats
docker logs -f ingestion    # trades/s and tickers/s from Coinbase
docker logs -f db-writer    # rows/s written, duplicates skipped, latency
```

- Kafka UI: http://localhost:8080 (messages arriving in `raw.trades` and `raw.ticker`)
- Database: `psql postgresql://crypto:crypto@localhost:5432/crypto`

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

# db-writer integration tests need a running database with the schema applied
TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto pytest
```

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
| `candles_1h` | Continuous aggregate over `candles_1m`, refreshed every 5 min | TimescaleDB | Kept |
| `alerts` | Hypertable | Spark ML | Kept |
| `latest_ticker` | View (`DISTINCT ON`) | | |

The db-writer loads each batch with `COPY` into a staging table, then `INSERT ... ON CONFLICT DO NOTHING`. Kafka offsets are committed only after the database commit, so a crash replays the batch and duplicates are skipped: each trade is stored exactly once.

## Project structure

```
crypto-stream-analytics/
├── ingestion/          # WebSocket client and Kafka producer
├── db-writer/          # Kafka consumer writing trades to TimescaleDB
├── spark/
│   ├── streaming/      # metrics, anomaly scoring, raw archive
│   └── batch/          # Hive summaries, ML training
├── db/
│   ├── migrations/     # schema, hypertables, policies
│   └── queries/        # analysis queries
├── hive/               # external table definitions
├── api/                # FastAPI app, WebSocket, Redis cache
├── dashboard/          # React frontend
├── notebooks/          # exploratory data analysis
├── docker/             # Dockerfiles and docker-compose.yml
└── .github/workflows/  # CI
```

## Roadmap

- [x] Ingestion service and Kafka setup
- [x] TimescaleDB schema and first queries
- [ ] Spark streaming: candles and indicators
- [ ] FastAPI and live dashboard
- [ ] HDFS archive, Hive tables, batch analytics
- [ ] ML anomaly detection and live scoring
- [ ] Pipeline monitoring, tests and CI

## Author

**Vishantkumar Bhavsar**
