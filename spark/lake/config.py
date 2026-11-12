"""Settings for the data lake jobs (HDFS archive and nightly batch)."""

import os
from dataclasses import dataclass

from streaming.config import settings as stream_settings


@dataclass(frozen=True)
class LakeSettings:
    # Everything on HDFS lives under this root
    hdfs_root: str = os.getenv("HDFS_ROOT", "hdfs://namenode:8020").rstrip("/")
    hive_database: str = os.getenv("HIVE_DATABASE", "crypto")

    # How often the archive job writes a batch of Parquet files. Longer = fewer, bigger files.
    archive_trigger: str = os.getenv("ARCHIVE_TRIGGER", "5 minutes")
    # First start: "earliest" archives everything Kafka still holds (7 days of raw.trades)
    archive_starting_offsets: str = os.getenv("ARCHIVE_STARTING_OFFSETS", "earliest")

    # Nightly batch: run at this UTC time for the previous day; catch up this many missed days
    batch_time_utc: str = os.getenv("BATCH_TIME_UTC", "00:30")
    batch_catch_up_days: int = int(os.getenv("BATCH_CATCH_UP_DAYS", "7"))

    # Prometheus metrics endpoint of the archive job (0 turns it off)
    metrics_port: int = int(os.getenv("METRICS_PORT", "9103"))

    spark_master: str = os.getenv("SPARK_MASTER", "local[2]")
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")

    # ------------------------------------------------------------ paths

    @property
    def raw_trades_path(self) -> str:
        return f"{self.hdfs_root}/data/raw/trades"

    @property
    def compaction_path(self) -> str:
        return f"{self.hdfs_root}/data/raw/_compaction"

    def curated_path(self, table: str) -> str:
        return f"{self.hdfs_root}/data/curated/{table}"

    @property
    def warehouse_path(self) -> str:
        return f"{self.hdfs_root}/warehouse"

    @property
    def archive_checkpoint(self) -> str:
        return os.getenv("ARCHIVE_CHECKPOINT_DIR", f"{self.hdfs_root}/checkpoints/archive")

    @property
    def archive_markers(self) -> str:
        """One empty file per archived micro-batch, so a re-run batch is not written twice."""
        return f"{self.hdfs_root}/data/raw/_batches"

    # ------------------------------------------------------ connections

    @property
    def kafka_bootstrap_servers(self) -> str:
        return stream_settings.kafka_bootstrap_servers

    @property
    def topic_trades(self) -> str:
        return stream_settings.topic_trades

    @property
    def database_url(self) -> str:
        return stream_settings.database_url

    @property
    def metastore_jdbc(self) -> dict[str, str]:
        """The Hive metastore: the hive_metastore schema of the same PostgreSQL database."""
        jdbc = stream_settings.jdbc
        return {**jdbc, "url": f"{jdbc['url']}?currentSchema=hive_metastore"}


settings = LakeSettings()
