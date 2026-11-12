"""Streaming archive: raw.trades -> Parquet on HDFS, partitioned by day and pair.

Every trade is kept exactly as it arrived, including Coinbase's replays and the
Kafka partition and offset it came from, so any number in the system can be
traced back to the raw record. The nightly batch job de-duplicates and
compacts each finished day.

Each micro-batch:
1. skips itself if a marker file says it was already written (a re-run after a crash),
2. appends Parquet files under dt=YYYY-MM-DD/pair=XXX-USD/,
3. registers any new partitions in the Hive metastore, so the data can be queried at once,
4. writes the marker.

Run locally:  python -m lake.archive_job      (from the spark/ folder)
"""

import logging
import re
import signal
import threading

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StructField, StructType

from lake.config import settings
from lake.session import create_spark, hadoop_fs
from lake.tables import ensure_tables
from streaming import metrics
from streaming.transforms import PRICE, SIZE, TRADE_JSON_SCHEMA

log = logging.getLogger("archive-job")

ARCHIVE_JSON_SCHEMA = StructType([*TRADE_JSON_SCHEMA.fields, StructField("replay", BooleanType())])
_DT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PAIR = re.compile(r"^[A-Z0-9]{2,10}-[A-Z]{2,5}$")


def parse_for_archive(raw: DataFrame) -> DataFrame:
    """Kafka records -> archive rows. Unparseable records are dropped (they are in dlq.trades)."""
    t = F.from_json(F.col("value").cast("string"), ARCHIVE_JSON_SCHEMA)
    parsed = raw.select(t.alias("t"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
    return (
        parsed.select(
            F.col("t.trade_id").try_cast("long").alias("trade_id"),
            F.col("t.price").try_cast(PRICE).alias("price"),
            F.col("t.size").try_cast(SIZE).alias("size"),
            F.col("t.side").alias("side"),
            F.try_to_timestamp("t.exchange_time").alias("exchange_time"),
            F.try_to_timestamp("t.ingest_time").alias("ingest_time"),
            F.coalesce(F.col("t.replay"), F.lit(False)).alias("is_replay"),
            F.col("kafka_partition").cast("int"),
            "kafka_offset",
            F.col("t.pair").alias("pair"),
        )
        .where(
            F.col("pair").rlike(_PAIR.pattern)
            & F.col("trade_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("size") > 0)
            & F.col("exchange_time").isNotNull()
        )
        .withColumn("dt", F.date_format("exchange_time", "yyyy-MM-dd"))
    )


def write_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    fs, Path = hadoop_fs(spark, settings.archive_markers)
    marker = Path(f"{settings.archive_markers}/{batch_id}")
    if fs.exists(marker):
        log.info("batch %d already archived, skipping", batch_id)
        return

    batch.persist()
    try:
        partitions = [(r["dt"], r["pair"]) for r in batch.select("dt", "pair").distinct().collect()]
        if not partitions:
            return
        # One file per day and pair per batch, instead of one per Spark task
        (
            batch.repartition("dt", "pair")
            .write.mode("append")
            .partitionBy("dt", "pair")
            .parquet(settings.raw_trades_path)
        )
        register_partitions(spark, partitions)
        fs.create(marker, True).close()
        log.info("batch %d: archived %d trades into %d partitions", batch_id, batch.count(), len(partitions))
    finally:
        batch.unpersist()


def register_partitions(spark: SparkSession, partitions: list[tuple[str, str]]) -> None:
    """Add (dt, pair) partitions to the Hive table; ones that exist are left alone."""
    safe = [(dt, pair) for dt, pair in partitions if _DT.match(dt) and _PAIR.match(pair)]
    if safe:
        specs = " ".join(f"PARTITION (dt='{dt}', pair='{pair}')" for dt, pair in sorted(safe))
        spark.sql(f"ALTER TABLE {settings.hive_database}.raw_trades ADD IF NOT EXISTS {specs}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = create_spark("crypto-archive")
    ensure_tables(spark)
    spark.streams.addListener(metrics.MetricsListener())
    metrics.serve(settings.metrics_port)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.topic_trades)
        .option("startingOffsets", settings.archive_starting_offsets)
        .option("maxOffsetsPerTrigger", 500_000)
        .option("failOnDataLoss", "false")
        .load()
    )
    query = (
        parse_for_archive(raw)
        .writeStream.queryName("raw-archive")
        .trigger(processingTime=settings.archive_trigger)
        .option("checkpointLocation", settings.archive_checkpoint)
        .foreachBatch(write_batch)
        .start()
    )

    stop_requested = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_requested.set())
    signal.signal(signal.SIGINT, lambda *_: stop_requested.set())
    try:
        while not stop_requested.is_set():
            if spark.streams.awaitAnyTermination(timeout=1):
                break
    finally:
        log.info("stopping")
        if query.isActive:
            query.stop()
        spark.stop()
        log.info("stopped")


if __name__ == "__main__":
    main()
