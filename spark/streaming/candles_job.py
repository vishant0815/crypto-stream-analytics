"""Spark Structured Streaming job: raw.trades -> candles and indicators.

Two queries read the same Kafka topic:

1. live-candles  (every 1 s, update mode)
   The candle for the current minute changes with every trade. Each micro-batch
   upserts the changed candles into candles_1m and sends them to agg.metrics,
   so the dashboard's last candle moves in real time.

2. indicators    (every 10 s, append mode)
   A candle is emitted once, after the watermark passes the end of its minute,
   meaning no more trades can change it. For those closed candles the job loads
   the previous hour of candles from TimescaleDB, computes moving averages,
   VWAP, volatility and RSI with Spark window functions, and writes them to
   candle_indicators and agg.metrics.

Both queries checkpoint their Kafka offsets and window state, so after a
restart they continue where they stopped. Writes are upserts, so a re-run
micro-batch is harmless.

Run locally:  python -m streaming.candles_job      (from the spark/ folder)
"""

import logging
import os
import signal
import threading
import time
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from streaming.config import settings
from streaming.sinks import (
    publish,
    read_candle_history,
    upsert_candles,
    upsert_indicators,
)
from streaming.transforms import build_candles, compute_indicators, parse_trades

log = logging.getLogger("candles-job")


# --------------------------------------------------------------------------
# Spark session and source
# --------------------------------------------------------------------------

def create_spark() -> SparkSession:
    # Every timestamp in the job is handled in UTC, in Java and in Python
    os.environ["TZ"] = "UTC"
    time.tzset()

    builder = (
        SparkSession.builder.appName("crypto-candles")
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        # Few pairs, small batches: a handful of partitions is faster than the default 200
        .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
        # RocksDB keeps window state off the JVM heap and checkpoints it incrementally
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
    )
    extra_jars = os.getenv("SPARK_EXTRA_JARS")  # local runs; the Docker image has them built in
    if extra_jars:
        builder = builder.config("spark.jars", extra_jars)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.log_level)
    return spark


def read_trades(spark: SparkSession) -> DataFrame:
    """Live trades from Kafka, typed, with late data and duplicates handled."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.topic_trades)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        # If Kafka deleted old data while the job was down, carry on from what exists
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        parse_trades(raw)
        .withWatermark("exchange_time", settings.watermark)
        # Coinbase resends recent trades after every reconnect; count each trade once
        .dropDuplicatesWithinWatermark(["pair", "trade_id"])
    )


# --------------------------------------------------------------------------
# Micro-batch handlers
# --------------------------------------------------------------------------

def write_live_candles(candles: DataFrame, batch_id: int) -> None:
    candles.persist()
    try:
        if candles.isEmpty():
            return
        upsert_candles(candles)
        publish(candles, "candle")
    finally:
        candles.unpersist()


def write_indicators(closed: DataFrame, batch_id: int) -> None:
    closed.persist()
    try:
        bounds = closed.agg(F.min("bucket").alias("lo"), F.max("bucket").alias("hi")).first()
        if bounds is None or bounds["lo"] is None:
            return
        spark = closed.sparkSession

        # The final values of closed candles; the live query may not have written them yet
        upsert_candles(closed)

        symbols = [r["pair"] for r in closed.select("pair").distinct().collect()]
        lo = bounds["lo"] - timedelta(minutes=settings.indicator_history_minutes)
        history = read_candle_history(
            spark,
            symbols,
            lo.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            bounds["hi"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        )

        cols = ["bucket", "pair", "close", "volume", "vwap"]
        keys = closed.select("pair", "bucket")
        # History from the database, minus the candles this batch is about to replace
        combined = history.join(keys, ["pair", "bucket"], "left_anti").select(cols).unionByName(closed.select(cols))

        indicators = compute_indicators(combined).join(keys, ["pair", "bucket"], "left_semi")
        indicators.persist()
        try:
            upsert_indicators(indicators)
            publish(indicators, "indicators")
            log.info("indicators: batch %d, %d closed candles", batch_id, indicators.count())
        finally:
            indicators.unpersist()
    finally:
        closed.unpersist()


# --------------------------------------------------------------------------
# Progress logging
# --------------------------------------------------------------------------

class ProgressLogger(StreamingQueryListener):
    """Logs input rate, processing rate and batch time, at most every 10 s per query."""

    def __init__(self, every_s: float = 10.0) -> None:
        self.every_s = every_s
        self._last: dict[str, float] = {}

    def onQueryStarted(self, event) -> None:
        log.info("query started: %s", event.name)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        now = time.monotonic()
        if now - self._last.get(p.name, 0) < self.every_s:
            return
        self._last[p.name] = now
        state_rows = sum(op.numRowsTotal for op in p.stateOperators) if p.stateOperators else 0
        log.info(
            "%s: batch=%d input/s=%.1f processed/s=%.1f batch_ms=%s state_rows=%d watermark=%s",
            p.name, p.batchId, p.inputRowsPerSecond or 0, p.processedRowsPerSecond or 0,
            p.durationMs.get("triggerExecution"), state_rows, p.eventTime.get("watermark"),
        )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("query %s failed: %s", event.id, event.exception)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def start(spark: SparkSession):
    live = (
        build_candles(read_trades(spark))
        .writeStream.queryName("live-candles")
        .outputMode("update")
        .trigger(processingTime=settings.candle_trigger)
        .option("checkpointLocation", f"{settings.checkpoint_dir}/live-candles")
        .foreachBatch(write_live_candles)
        .start()
    )
    closed = (
        build_candles(read_trades(spark))
        .writeStream.queryName("indicators")
        .outputMode("append")
        .trigger(processingTime=settings.indicator_trigger)
        .option("checkpointLocation", f"{settings.checkpoint_dir}/indicators")
        .foreachBatch(write_indicators)
        .start()
    )
    return [live, closed]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)  # very chatty at INFO
    spark = create_spark()
    spark.streams.addListener(ProgressLogger())
    queries = start(spark)

    # The signal handler only sets a flag. Calling into Spark from inside a
    # handler can collide with a Spark call already running on the main thread.
    stop_requested = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_requested.set())
    signal.signal(signal.SIGINT, lambda *_: stop_requested.set())

    try:
        # Wake up every second to check the flag. A failed query raises here,
        # so the process exits non-zero and Docker restarts it from the checkpoint.
        while not stop_requested.is_set():
            if spark.streams.awaitAnyTermination(timeout=1):
                break
    finally:
        log.info("stopping queries")
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        log.info("stopped")


if __name__ == "__main__":
    main()
