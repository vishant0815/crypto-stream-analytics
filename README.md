# Crypto Stream Analytics

[![CI](https://github.com/vishant0815/crypto-stream-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/vishant0815/crypto-stream-analytics/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-231F20?logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-4.1-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop%20HDFS-3.4-66CCFF?logo=apachehadoop&logoColor=black)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-PostgreSQL%2016-FDB515?logo=postgresql&logoColor=black)
![Grafana](https://img.shields.io/badge/Grafana-Prometheus-F46800?logo=grafana&logoColor=white)

Real-time crypto market analytics pipeline. Live trades from the Coinbase public WebSocket are streamed through **Apache Kafka** and **Spark Structured Streaming** into **TimescaleDB** and **HDFS/Hive**, scored by a **Spark MLlib** anomaly-detection model, and shown on a **React** dashboard that updates every second. **Prometheus and Grafana** watch the pipeline itself.

![Live dashboard, switching between the light and dark themes](docs/demo.gif)

## Highlights

Measured with the project's own Prometheus metrics, with the whole stack running on a 2-core, 8 GB machine (Coinbase's volume varies through the day):

| | Result |
| --- | --- |
| Live data in | About 16 trades and 16 ticker updates per second across 5 coins, around 34 Kafka messages/s with candles and indicators |
| Coinbase → ingestion | 0.14 s median, 0.24 s at the 95th percentile |
| Trade → TimescaleDB | 0.7 s median, 1.4 s at p95 (the db-writer batches for up to 1 s on purpose) |
| Database batch write | 79 ms at p95 (`COPY` into staging + `INSERT ... ON CONFLICT`) |
| Spark live-candles micro-batch | 1.7 s median, 3.2 s at p95, with nothing left behind in Kafka |
| REST API | Under 160 ms at p95 for every route, behind a 2-second Redis cache |
| HDFS vs TimescaleDB | Trade counts match exactly in the nightly reconciliation |
| Anomaly model | 100% of injected anomalies caught, 0.07% false positives |

- **Exactly-once storage**: Kafka offsets are committed only after the database commit, and `ON CONFLICT DO NOTHING` skips redelivered trades.
- **Lambda architecture**: a hot path (Spark Streaming → TimescaleDB, seconds) and a cold path (HDFS/Hive archive → nightly Spark SQL batch), reconciled against each other every night.
- **ML in the stream**: a Spark MLlib K-Means model, retrained nightly from HDFS, scores every closed minute live; on injected anomalies it catches **100% with 0.07% false positives**.
- **Observable**: every service exports Prometheus metrics; a provisioned Grafana dashboard and 10 alert rules cover Kafka lag, Spark batch times, database writes and end-to-end latency.
- **Tested in CI**: 83 tests on every push, the integration tests against real TimescaleDB, Redis and Kafka, plus the dashboard build and every Docker image.

---

## Architecture

```mermaid
flowchart LR
    CB["Coinbase<br/>WebSocket"] --> ING["Ingestion<br/>(Python)"] --> K[("Kafka")]

    subgraph hot["Hot path: seconds"]
        direction TB
        DBW["db-writer<br/>exactly-once"]
        SS["Spark Structured Streaming<br/>candles · indicators · ML scoring"]
    end

    subgraph cold["Cold path: nightly"]
        direction TB
        ARC["Spark archive job"] -- "Parquet, 5 min" --> HDFS[("HDFS + Hive")]
        HDFS --> BATCH["Spark SQL batch<br/>+ MLlib training"]
    end

    K --> DBW & SS & ARC
    DBW & SS --> TS[("TimescaleDB")]
    BATCH -- "summaries, reconciliation" --> TS
    BATCH -. "new model" .-> SS
    TS & K --> API["FastAPI + Redis"] -- "WebSocket, 1/s" --> UI["React dashboard"]

    subgraph mon["Monitoring"]
        PROM["Prometheus<br/>10 alert rules"] --> GRAF["Grafana"]
    end
    ING & hot & cold & API -. "/metrics" .-> PROM
```

- **Hot path**: trade to screen in about 2 seconds.
- **Cold path**: full history in HDFS, nightly batch analytics and model training.
- **Monitoring**: every service exports metrics to Prometheus; Grafana charts the pipeline's health.

## Features

- Live price tickers, candlestick charts and volume for BTC, ETH, SOL, XRP and DOGE
- 1-minute and 5-minute OHLCV candles, VWAP, moving averages and rolling volatility
- Real-time anomaly alerts (volume spikes, sharp price moves) from a Spark MLlib model
- Batch analytics from Hive: daily volatility, hourly volume heatmap, pair correlation
- Pipeline health band on the dashboard, and a full Grafana dashboard for the pipeline's own health
- Light and dark glass themes, with an ink-spread animation between them

## Tech stack

| Layer | Tools |
| --- | --- |
| Data source | Coinbase Advanced Trade public WebSocket |
| Streaming | Apache Kafka |
| Processing | Spark Structured Streaming, Spark SQL |
| Machine learning | Spark MLlib |
| Data lake | Hadoop HDFS, Parquet, Hive metastore (on PostgreSQL), Spark SQL batch |
| Database | PostgreSQL + TimescaleDB, Redis |
| Backend | FastAPI (REST + WebSockets) |
| Frontend | React, TradingView lightweight-charts, served by nginx |
| Monitoring | Prometheus, Grafana, kafka-exporter |
| DevOps | Docker Compose, GitHub Actions |

## Getting started

Requirements: Docker Desktop, plus Python 3.12+ if you want to run services outside Docker.

```bash
git clone https://github.com/vishant0815/crypto-stream-analytics.git
cd crypto-stream-analytics
cp .env.example .env

# Start everything: Kafka, TimescaleDB, Redis, HDFS, ingestion, db-writer, Spark, API,
# dashboard, Prometheus and Grafana
docker compose --env-file .env -f docker/docker-compose.yml up -d --build

# Watch live stats
docker logs -f ingestion        # trades/s and tickers/s from Coinbase
docker logs -f db-writer        # rows/s written, duplicates skipped, latency
docker logs -f spark-streaming  # input rate, batch time, state size, watermark
```

- **Dashboard: http://localhost:3000**
- **Grafana: http://localhost:3001** (pipeline health; viewing needs no login, admin / `GRAFANA_ADMIN_PASSWORD` to edit)
- Prometheus: http://localhost:9090 (targets, queries, and alert rules at `/alerts`)
- API docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8080 (messages arriving in `raw.trades`, `raw.ticker` and `agg.metrics`)
- Spark UI: http://localhost:4040 (Structured Streaming tab: query progress and batch durations)
- HDFS UI: http://localhost:9870 (Utilities > Browse the file system > `/data/raw/trades`)
- Database: `psql postgresql://crypto:crypto@localhost:5432/crypto`

Indicators need 15 minutes of continuous data before they appear, and the 60-minute average needs an hour. The data-lake history fills in after the first nightly run (00:30 UTC) for a day the archive covered.

The whole stack runs 17 containers, three of them Spark, so give Docker Desktop at least 8 GB of memory.

Already ran an earlier version? The schema only runs automatically on an empty database, so apply any migrations you don't have yet, in order:

```bash
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/002_candles_and_indicators.sql
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/003_trade_replay_flag.sql
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/004_batch_analytics.sql
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/005_hive_metastore_schema.sql
psql postgresql://crypto:crypto@localhost:5432/crypto -f db/migrations/006_ml_anomaly.sql
PGHOST=localhost PGPASSWORD=crypto POSTGRES_USER=crypto POSTGRES_DB=crypto sh db/migrations/007_grafana_reader.sh
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

**Run the tests** (83 in all; GitHub Actions runs every one of them on each push, see `.github/workflows/ci.yml`):

```bash
cd ingestion && pytest
cd ../db-writer && pytest
cd ../spark && pytest   # needs Java 17 or 21
```

The db-writer and API integration tests **delete data**, so they only run against a separate database whose name ends in `_test`, and refuse to start otherwise:

```bash
createdb -h localhost -U crypto crypto_test
for f in db/migrations/*.sql; do psql postgresql://crypto:crypto@localhost:5432/crypto_test -f "$f"; done
PGHOST=localhost PGPASSWORD=crypto POSTGRES_USER=crypto POSTGRES_DB=crypto_test sh db/migrations/007_grafana_reader.sh

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

![Live dashboard](docs/dashboard.png)

The FastAPI service reads Kafka (`raw.ticker`, `raw.trades`, `agg.metrics`) and pushes **one batched update per second** to each browser over a WebSocket, so 50 trades in a second become one message. History comes from TimescaleDB over REST.

| Endpoint | Returns |
| --- | --- |
| `GET /api/candles?pair=BTC-USD&interval=1m\|5m\|1h&limit=300` | OHLCV candles, oldest first; for `1m`, the matching indicators |
| `GET /api/overview` | Every pair: price, 24 h change, 60-minute sparkline, latest indicators |
| `GET /api/trades/recent?pair=BTC-USD&limit=40` | Latest trades, newest first |
| `GET /api/pipeline` | Ingestion latency percentiles, trades per minute, data freshness, storage, live message rates |
| `GET /api/pairs`, `GET /api/health` | Pairs; status of database, Redis and Kafka |
| `GET /api/analytics?days=14` | Nightly batch results: per-pair daily summary, hourly activity, correlations, and the last runs with their reconciliation |
| `GET /api/alerts?pair=SOL-USD&limit=30` | Recent unusual minutes, newest first, with what drove each one |
| `GET /api/model` | The active anomaly model from the registry, and alerts in the last 24 hours |
| `WS /ws/live?pairs=BTC-USD,ETH-USD` | A snapshot, then about one update per second, including new alerts |

- **Redis** caches database responses for 2 seconds, so the database sees at most one query per endpoint every 2 seconds however many people are watching, and holds the latest live state so a new browser gets prices immediately. If Redis is down, requests go straight to the database.
- API database connections are **read-only** (`default_transaction_read_only`), with a 5-second statement timeout.
- A browser too slow to take its messages is dropped instead of holding back everyone else.
- The dashboard (React and TradingView's lightweight-charts) loads history once and then only applies pushed updates; 5m and 1h bars are rebuilt live from the 1-minute candles.
- In Docker, nginx serves the dashboard and forwards `/api` and `/ws` to the API, so the browser talks to one origin.
- Coinbase replays a batch of recent trades every time a client connects. They're flagged as `is_replay`, still counted in candles, and left out of the latency figures.

### Data lake: HDFS, Hive and the nightly batch

The streaming path keeps 30 days in TimescaleDB; the data lake keeps **every trade, forever**, on HDFS.

**Archive** (`spark-archive`, Spark Structured Streaming): every 5 minutes, all new trades from `raw.trades` are appended as Parquet under `/data/raw/trades/dt=YYYY-MM-DD/pair=XXX-USD/`, exactly as received, with their Kafka partition and offset for lineage. Each batch registers any new partitions in Hive straight away, so the data can be queried within minutes. A marker file per micro-batch stops a batch that is re-run after a crash from being written twice.

**Hive tables** (database `crypto`): `raw_trades` (partitioned by day and pair), and the batch outputs `daily_summary`, `hourly_profile` and `pair_correlation` (partitioned by day). All are external Parquet tables on HDFS. The metastore is Hive 2.3, matching Spark's built-in client, stored in the `hive_metastore` schema of the same PostgreSQL database. Its schema is created by Hive's official script (`db/migrations/005_hive_metastore_schema.sql`) rather than on the fly, because letting DataNucleus create it lazily deadlocks on PostgreSQL.

**Nightly batch** (`spark-batch`, Spark SQL), at 00:30 UTC for the day before:

1. **De-duplicate**: one row per trade, keeping the live copy over Coinbase's reconnect replays.
2. **Compact**: about 1,400 small files per day become one file per pair. The new files are written to a staging folder and swapped in with HDFS renames, and a file the archive writes into the old folder mid-swap is kept, not deleted.
3. **Compute**: daily summary (OHLC, return, range, realized volatility from 1-minute log returns, VWAP, traded value, buy share), activity by UTC hour, and Pearson correlation of 1-minute returns between every pair of coins.
4. **Store**: `INSERT OVERWRITE` of the day's partition in Hive, and the same rows into TimescaleDB for the dashboard, in one transaction.
5. **Reconcile**: count the day's trades in HDFS and in TimescaleDB, pair by pair, and log the run in `batch_runs` as `ok` or `mismatch`.

Every step can be repeated: running a day again gives the same result. On start, the scheduler catches up any of the last 7 days that has no finished run.

```bash
# Run the batch for one day by hand
docker compose -f docker/docker-compose.yml exec spark-batch /opt/app/run.sh lake.batch_job --date 2026-11-10

# Query the Hive tables with Spark SQL
docker compose -f docker/docker-compose.yml exec spark-batch /opt/app/run.sh lake.query \
  "SELECT pair, count(*) AS trades FROM crypto.raw_trades WHERE dt = '2026-11-10' GROUP BY pair"
```

### Anomaly detection (Spark MLlib)

Every closed minute of every pair is scored as it closes, and unusual ones become alerts on the dashboard, in the `alerts` table and on the `alerts` Kafka topic.

**Features**, one row per pair and minute (`spark/ml/features.py`). Each compares the minute with the same coin's previous hour, so the features are scale-free and one model serves every pair:

| Feature | Meaning |
| --- | --- |
| `abs_ret_z` | Size of the minute's move, in units of the past hour's typical move |
| `range_z` | High-low range vs the past hour's average |
| `log_volume_ratio`, `log_trades_ratio` | Volume and trade count vs the past hour's average (log) |
| `vwap_dev_z` | How far the close ended from the minute's average price: a late jump |
| `buy_imbalance` | 0 when buying and selling balance, 1 when it was all one side |

The nightly trainer and the live job call the **same function on the same 1-minute candles** (built by the same `build_candles`), so the model scores exactly what it was trained on.

**Model** (`spark/ml/model.py`): standardised features → **K-Means**, with k chosen by silhouette score. A minute's anomaly score is its distance to the nearest *normal* cluster centre (one holding at least 1% of training minutes): when several similar anomalies are in the training data, K-Means can give them a small cluster of their own, where they would look ordinary. The threshold is the 99.5th percentile of training scores, so about 1 minute in 200 is flagged. A minute is only alerted if its main driver is unusually *high*, so unusually quiet minutes don't count. Each alert names that driver, which sets its type (volume spike, price jump).

**Training** (`spark/ml/train_job.py`): nightly after the batch job, on the last 30 days of the HDFS archive. The model is saved to HDFS under `/models/anomaly/<version>`, recorded in `model_registry` with its training data, k, silhouette, threshold and alert rate, and activated if it passes a sanity check. The live job checks the registry every 10 minutes and switches to a new model without a restart. Until there are 2,000 pair-minutes of history, or if a model can't be loaded, fixed rules (`rules-v1`) do the job, so alerts never stop.

**Evaluation** (`spark/tests/test_ml.py`): three synthetic markets with different volatility, with price jumps and volume spikes injected into 0.3% of minutes and trained on without labels, as in production. The model catches **100% of injected anomalies with 0.07% false positives**, and labels every alert with the right type.

```bash
# Train now instead of waiting for the night
docker compose -f docker/docker-compose.yml exec spark-batch /opt/app/run.sh ml.train_job
```

### Monitoring: Prometheus and Grafana

Every service serves Prometheus metrics, Prometheus scrapes them every 5 seconds, and Grafana shows them on a **Pipeline health** dashboard that is provisioned from code (`monitoring/`), so it appears on first start with nothing to click.

![Grafana pipeline health dashboard](docs/grafana.png)

| Source | Metrics |
| --- | --- |
| Ingestion `:9101` | Trades and tickers received, dead-letter messages, Kafka delivery results, reconnects, sequence gaps, exchange → ingestion lag |
| db-writer `:9102` | Rows inserted per table, duplicates skipped, batch write time, messages per batch, **trade → database latency** |
| Spark `:9103` | Per query: micro-batch duration, input vs processed rows/s, state store rows and memory, **offsets behind Kafka**, watermark delay; anomaly alerts by type |
| API `/metrics` | Requests and latency per route, Redis cache hits, WebSocket clients and messages, Kafka messages read |
| kafka-exporter | Messages per topic and **consumer group lag** |
| TimescaleDB (SQL) | Trades per minute by pair, hypertable sizes, nightly batch runs and reconciliation, model registry |

Spark tracks its Kafka offsets in checkpoints rather than a consumer group, so its lag comes from a `StreamingQueryListener` that turns each progress report into metrics. The API's `/metrics` is not forwarded by nginx, so it isn't public. Grafana reads TimescaleDB as `grafana_reader`, a read-only login with a statement timeout.

**Alert rules** (`monitoring/prometheus/alerts.yml`): a service down, Coinbase silent for a minute, no trades stored for 5 minutes, db-writer over 5,000 messages behind, 95th-percentile trade → database latency over 5 s, slow or stalled Spark queries, Spark falling behind Kafka, a dead-letter spike, and API errors over 5%. Firing alerts appear at http://localhost:9090/alerts and as markers on the Grafana charts.

The monitoring has already paid off: the API latency panel showed `/api/pipeline` taking 1–2 seconds at the 95th percentile. The cause was a query joining each of 30 minutes to the trades table by range; counting with `time_bucket()` in one pass made it about 50 times faster.

### Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request, in four parallel jobs:

| Job | What it checks |
| --- | --- |
| Python services | Applies every migration to a real TimescaleDB, then runs the ingestion, db-writer and API tests against it, Redis and Kafka (service containers, same versions as Docker Compose) |
| Spark jobs | Streaming transforms, the data-lake batch and the ML model, on local Spark with Java 21 |
| Dashboard | `npm ci` and the production build |
| Docker and config | `docker compose config`, `promtool check config` (Prometheus config and alert rules), the Grafana dashboard JSON, and a build of every image |

## Project structure

```
crypto-stream-analytics/
├── ingestion/          # WebSocket client and Kafka producer
├── db-writer/          # Kafka consumer writing trades to TimescaleDB
├── api/                # FastAPI: REST, WebSocket, Redis cache
├── dashboard/          # React dashboard, served by nginx in Docker
├── spark/
│   ├── streaming/      # candles and indicators job (Structured Streaming)
│   ├── lake/           # HDFS archive job, nightly batch, scheduler, Hive tables
│   ├── ml/             # anomaly features, K-Means model, training job, model registry
│   └── tests/          # unit tests for streaming and batch logic
├── db/
│   ├── migrations/     # schema, hypertables, policies, roles
│   └── queries/        # analysis queries
├── monitoring/
│   ├── prometheus/     # scrape config and alert rules
│   └── grafana/        # provisioned data sources and the pipeline health dashboard
├── docker/             # docker-compose.yml (each service's Dockerfile is in its own folder)
├── docs/               # screenshots
└── .github/workflows/  # CI
```

## Roadmap

- [x] Ingestion service and Kafka setup
- [x] TimescaleDB schema and first queries
- [x] Spark streaming: candles and indicators
- [x] FastAPI and live dashboard
- [x] HDFS archive, Hive tables, batch analytics
- [x] ML anomaly detection and live scoring
- [x] Pipeline monitoring (Prometheus, Grafana), tests and CI

## Author

**Vishantkumar Bhavsar** · [LinkedIn](https://www.linkedin.com/in/vishant810)
