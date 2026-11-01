"""Writing results out of Spark: TimescaleDB upserts, Kafka messages, JDBC history reads.

Upserts make every write idempotent: if Spark re-runs a micro-batch after a
crash, the same rows are written again with the same values, never duplicated.
"""

import re
from collections.abc import Iterable

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from streaming.config import settings

ISO_UTC = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"
_SYMBOL = re.compile(r"^[A-Z0-9]{2,10}-[A-Z]{2,5}$")
_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$")

CANDLE_COLUMNS = [
    "open", "high", "low", "close", "volume", "buy_volume", "vwap",
    "trade_count", "first_trade_at", "last_trade_at",
]
INDICATOR_COLUMNS = [
    "close", "return_pct", "sma_5", "sma_15", "sma_60", "vwap_15",
    "volatility_15", "rsi_14", "volume_ratio_15", "candles_in_60",
]
TIMESTAMP_COLUMNS = {"bucket", "first_trade_at", "last_trade_at"}


def _upsert_sql(table: str, columns: list[str]) -> str:
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
    # pair_id comes from the symbol; the row is skipped if the pair is unknown
    return (
        f"INSERT INTO {table} (bucket, pair_id, {cols}, updated_at) "
        f"SELECT %s, p.pair_id, {placeholders}, now() FROM pairs p WHERE p.symbol = %s "
        f"ON CONFLICT (pair_id, bucket) DO UPDATE SET {updates}, updated_at = now()"
    )


CANDLE_UPSERT = _upsert_sql("candles_1m", CANDLE_COLUMNS)
INDICATOR_UPSERT = _upsert_sql("candle_indicators", INDICATOR_COLUMNS)
PAIR_INSERT = (
    "INSERT INTO pairs (symbol, base, quote) VALUES (%s, %s, %s) ON CONFLICT (symbol) DO NOTHING"
)


def _as_iso(df: DataFrame) -> DataFrame:
    """Timestamps -> ISO strings with an explicit UTC offset.

    Python turns Spark timestamps into naive datetimes in the local time zone,
    which is easy to get wrong. A string with '+00:00' means the same instant
    everywhere, and PostgreSQL parses it straight into TIMESTAMPTZ.
    """
    return df.select(
        *[
            F.date_format(c, ISO_UTC).alias(c) if c in TIMESTAMP_COLUMNS else F.col(c)
            for c in df.columns
        ]
    )


def _upsert_partition(sql: str, columns: list[str]):
    """Build a function that upserts one partition's rows in one transaction.

    The two streaming queries can write the same candle rows at the same time.
    Rows are always locked in the same order (pair, then time), which avoids
    most deadlocks; if PostgreSQL still detects one, it cancels one side and
    that transaction is simply retried.
    """

    def write(rows: Iterable[Row]) -> None:
        import time

        import psycopg  # imported on the executor, not the driver
        from psycopg import errors

        batch = sorted(
            ((r["bucket"], *[r[c] for c in columns], r["pair"]) for r in rows),
            key=lambda row: (row[-1], row[0]),
        )
        if not batch:
            return
        symbols = sorted({row[-1] for row in batch})

        for attempt in range(1, 6):
            try:
                with psycopg.connect(settings.database_url, application_name="spark-streaming") as conn:
                    with conn.cursor() as cur:
                        for symbol in symbols:
                            base, quote = symbol.split("-")
                            cur.execute(PAIR_INSERT, (symbol, base, quote))
                        cur.executemany(sql, batch)
                return
            except (errors.DeadlockDetected, errors.SerializationFailure):
                if attempt == 5:
                    raise
                time.sleep(0.1 * attempt)

    return write


def _upsert(df: DataFrame, sql: str, columns: list[str]) -> None:
    # A micro-batch is a few rows per pair: one partition = one connection, one transaction
    _as_iso(df.select("bucket", "pair", *columns)).coalesce(1).foreachPartition(_upsert_partition(sql, columns))


def upsert_candles(candles: DataFrame) -> None:
    _upsert(candles, CANDLE_UPSERT, CANDLE_COLUMNS)


def upsert_indicators(indicators: DataFrame) -> None:
    _upsert(indicators, INDICATOR_UPSERT, INDICATOR_COLUMNS)


def publish(df: DataFrame, message_type: str) -> None:
    """Send each row to agg.metrics as JSON, keyed by pair.

    Decimals become plain JSON numbers here, because these messages feed the
    dashboard, which only needs display precision.
    """
    as_numbers = df.select(
        *[F.col(c).cast("double").alias(c) if t.startswith("decimal") else F.col(c) for c, t in df.dtypes]
    )
    out = _as_iso(as_numbers)
    (
        out.select(
            F.col("pair").alias("key"),
            F.to_json(
                F.struct(F.lit(message_type).alias("type"), *[F.col(c) for c in out.columns]),
                # Keep null fields (e.g. sma_60 in the first hour) so every message has the same keys
                {"ignoreNullFields": "false"},
            ).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("topic", settings.topic_metrics)
        .save()
    )


def read_candle_history(spark: SparkSession, symbols: list[str], start_iso: str, end_iso: str) -> DataFrame:
    """Closed 1-minute candles for these pairs with start <= bucket <= end, via JDBC."""
    # Values are put into the SQL text, so only well-formed ones are allowed
    if not (_ISO.match(start_iso) and _ISO.match(end_iso)):
        raise ValueError(f"bad time range {start_iso!r} .. {end_iso!r}")
    safe = [s for s in symbols if _SYMBOL.match(s)]
    if not safe:
        return spark.createDataFrame([], "bucket timestamp, pair string, close decimal(20,10), volume decimal(28,12), vwap decimal(20,10)")
    in_list = ", ".join(f"'{s}'" for s in safe)
    query = f"""
        SELECT c.bucket, p.symbol AS pair, c.close, c.volume, c.vwap
        FROM candles_1m c
        JOIN pairs p USING (pair_id)
        WHERE p.symbol IN ({in_list})
          AND c.bucket BETWEEN '{start_iso}'::timestamptz AND '{end_iso}'::timestamptz
    """
    jdbc = settings.jdbc
    return (
        spark.read.format("jdbc")
        .option("url", jdbc["url"])
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", jdbc["driver"])
        .option("query", query)
        .load()
    )
