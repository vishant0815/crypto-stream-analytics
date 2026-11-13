"""Settings for the Spark streaming job, read from environment variables."""

import os
from dataclasses import dataclass
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_trades: str = os.getenv("TOPIC_TRADES", "raw.trades")
    topic_metrics: str = os.getenv("TOPIC_METRICS", "agg.metrics")
    topic_alerts: str = os.getenv("TOPIC_ALERTS", "alerts")
    # How often the live job checks the model registry for a newly trained model
    model_refresh_s: float = float(os.getenv("MODEL_REFRESH_S", "600"))

    database_url: str = os.getenv("DATABASE_URL", "postgresql://crypto:crypto@localhost:5432/crypto")

    # Where Spark keeps Kafka offsets and window state between restarts
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")

    # How late a trade may arrive (by exchange time) and still count
    watermark: str = os.getenv("WATERMARK", "30 seconds")
    candle_trigger: str = os.getenv("CANDLE_TRIGGER", "1 second")
    indicator_trigger: str = os.getenv("INDICATOR_TRIGGER", "10 seconds")
    # Cap on trades read per micro-batch, so a long backlog is worked through in steps
    max_offsets_per_trigger: int = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
    # First start with no checkpoint: "earliest" replays what Kafka still holds
    starting_offsets: str = os.getenv("STARTING_OFFSETS", "latest")

    # Minutes of candle history loaded to compute the longest indicator (SMA 60)
    indicator_history_minutes: int = int(os.getenv("INDICATOR_HISTORY_MINUTES", "65"))

    # Prometheus metrics endpoint (0 turns it off)
    metrics_port: int = int(os.getenv("METRICS_PORT", "9103"))

    spark_master: str = os.getenv("SPARK_MASTER", "local[2]")
    # Each partition is a separate state store per stateful step, with its own
    # load/commit cost every batch; 5 pairs need very few
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")

    @property
    def jdbc(self) -> dict[str, str]:
        """The same database as DATABASE_URL, in the form Spark's JDBC reader wants."""
        u = urlparse(self.database_url)
        return {
            "url": f"jdbc:postgresql://{u.hostname}:{u.port or 5432}{u.path}",
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "driver": "org.postgresql.Driver",
        }


settings = Settings()
