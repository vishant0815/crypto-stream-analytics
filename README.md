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

## Project structure

```
crypto-stream-analytics/
├── ingestion/          # WebSocket client and Kafka producer
├── spark/
│   ├── streaming/      # metrics, anomaly scoring, raw archive
│   └── batch/          # Hive summaries, ML training
├── db/                 # SQL migrations and TimescaleDB policies
├── hive/               # external table definitions
├── api/                # FastAPI app, WebSocket, Redis cache
├── dashboard/          # React frontend
├── notebooks/          # exploratory data analysis
├── docker/             # Dockerfiles and docker-compose.yml
└── .github/workflows/  # CI
```

## Roadmap

- [ ] Ingestion service and Kafka setup
- [ ] TimescaleDB schema and first queries
- [ ] Spark streaming: candles and indicators
- [ ] FastAPI and live dashboard
- [ ] HDFS archive, Hive tables, batch analytics
- [ ] ML anomaly detection and live scoring
- [ ] Pipeline monitoring, tests and CI

## Author

**Vishantkumar Bhavsar**
