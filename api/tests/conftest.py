"""Test setup: real TimescaleDB, Redis and Kafka, seeded with known data.

  TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto_test \\
  TEST_REDIS_URL=redis://localhost:6379/15 \\
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest

The database must be a separate one whose name ends in "_test": it is emptied
and re-seeded. The Redis database is flushed.
"""

import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pytest

DB_URL = os.getenv("TEST_DATABASE_URL")
REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")

if not DB_URL:
    pytest.skip("TEST_DATABASE_URL not set", allow_module_level=True)
if not urlparse(DB_URL).path.rstrip("/").endswith("_test"):
    raise RuntimeError("TEST_DATABASE_URL must name a database ending in '_test' (tests delete data)")

# The app reads settings at import time, so point it at the test services first
os.environ["DATABASE_URL"] = DB_URL
os.environ["REDIS_URL"] = REDIS_URL
os.environ["PUSH_INTERVAL_S"] = "0.5"
# Separate topics, so a pipeline running on the same Kafka can't leak live prices into the tests
TEST_TOPICS = {
    "TOPIC_TICKER": "test.raw.ticker",
    "TOPIC_TRADES": "test.raw.trades",
    "TOPIC_METRICS": "test.agg.metrics",
    "TOPIC_ALERTS": "test.alerts",
}
os.environ.update(TEST_TOPICS)

import psycopg  # noqa: E402
import redis  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

# Seeded data: BTC-USD has 120 one-minute candles ending one minute ago,
# closing at 100, 101, ... 219, so every expected value is easy to compute.
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)
FIRST = NOW - timedelta(minutes=120)
N_CANDLES = 120


def seed(conn: psycopg.Connection) -> None:
    conn.execute("TRUNCATE trades, ticker, candle_indicators")
    # DELETE, not TRUNCATE: continuous aggregates track deletes but not truncates,
    # and candles_5m / candles_1h must forget the previous run's candles
    conn.execute("DELETE FROM candles_1m")
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO candles_1m (bucket, pair_id, open, high, low, close, volume, vwap, trade_count)
               SELECT %s, pair_id, %s, %s, %s, %s, 2, %s, 10 FROM pairs WHERE symbol = 'BTC-USD'""",
            [
                (FIRST + timedelta(minutes=i), 100 + i - 0.5, 100 + i + 1, 100 + i - 1, 100 + i, 100 + i)
                for i in range(N_CANDLES)
            ],
        )
        cur.executemany(
            """INSERT INTO candle_indicators (bucket, pair_id, close, sma_5, rsi_14, candles_in_60)
               SELECT %s, pair_id, %s, %s, 70, 60 FROM pairs WHERE symbol = 'BTC-USD'""",
            [(FIRST + timedelta(minutes=i), 100 + i, 100 + i - 2) for i in range(N_CANDLES)],
        )
        cur.executemany(
            """INSERT INTO ticker (time, pair_id, price, price_pct_chg_24h, best_bid, best_ask, ingest_time)
               SELECT %s, pair_id, %s, 1.5, %s, %s, %s FROM pairs WHERE symbol = %s""",
            [
                (NOW - timedelta(seconds=30), 219.5, 219.4, 219.6, NOW, "BTC-USD"),
                (NOW - timedelta(seconds=30), 2500, 2499, 2501, NOW, "ETH-USD"),
            ],
        )
        cur.executemany(
            """INSERT INTO trades (time, pair_id, trade_id, price, size, side, ingest_time)
               SELECT %s, pair_id, %s, %s, 0.1, %s, %s FROM pairs WHERE symbol = 'BTC-USD'""",
            [
                (NOW - timedelta(seconds=60 - i), 1000 + i, 219 + i / 100, "BUY" if i % 2 else "SELL",
                 NOW - timedelta(seconds=60 - i) + timedelta(milliseconds=150))
                for i in range(50)
            ],
        )
    conn.commit()
    conn.autocommit = True  # refresh_continuous_aggregate can't run inside a transaction
    for view in ("candles_5m", "candles_1h"):
        conn.execute(f"CALL refresh_continuous_aggregate('{view}', NULL, NULL)")


def create_test_topics() -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    futures = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in TEST_TOPICS.values()])
    for topic, future in futures.items():
        try:
            future.result(15)
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


@pytest.fixture(scope="session")
def client():
    create_test_topics()
    with psycopg.connect(DB_URL) as conn:
        seed(conn)
    redis.Redis.from_url(REDIS_URL).flushdb()

    from app.main import app

    with TestClient(app) as c:
        yield c
