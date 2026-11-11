"""Hive table definitions. Created if missing; safe to run every time a job starts."""

from pyspark.sql import SparkSession

from lake.config import settings


def ensure_tables(spark: SparkSession) -> None:
    db = settings.hive_database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION '{settings.warehouse_path}/{db}.db'")

    # Every trade as it arrived, partitioned by UTC day and pair.
    # External: dropping the table never deletes the archive.
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.raw_trades (
            trade_id        BIGINT,
            price           DECIMAL(20,10),
            size            DECIMAL(28,12),
            side            STRING,
            exchange_time   TIMESTAMP,
            ingest_time     TIMESTAMP,
            is_replay       BOOLEAN,
            kafka_partition INT,
            kafka_offset    BIGINT
        )
        PARTITIONED BY (dt STRING, pair STRING)
        STORED AS PARQUET
        LOCATION '{settings.raw_trades_path}'
    """)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.daily_summary (
            pair              STRING,
            open              DECIMAL(20,10),
            high              DECIMAL(20,10),
            low               DECIMAL(20,10),
            close             DECIMAL(20,10),
            return_pct        DOUBLE,
            range_pct         DOUBLE,
            realized_vol_pct  DOUBLE,
            volume            DECIMAL(28,12),
            notional_usd      DECIMAL(24,2),
            vwap              DECIMAL(20,10),
            trades            BIGINT,
            buy_volume_pct    DOUBLE,
            largest_trade_usd DECIMAL(24,2),
            active_minutes    INT
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
        LOCATION '{settings.curated_path("daily_summary")}'
    """)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.hourly_profile (
            pair           STRING,
            hour           INT,
            trades         BIGINT,
            volume         DECIMAL(28,12),
            notional_usd   DECIMAL(24,2),
            volatility_pct DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
        LOCATION '{settings.curated_path("hourly_profile")}'
    """)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {db}.pair_correlation (
            pair_a      STRING,
            pair_b      STRING,
            correlation DOUBLE,
            minutes     BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS PARQUET
        LOCATION '{settings.curated_path("pair_correlation")}'
    """)
