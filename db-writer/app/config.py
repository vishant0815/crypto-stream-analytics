"""Settings for the db-writer service, read from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://crypto:crypto@localhost:5432/crypto")

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id: str = os.getenv("DB_WRITER_GROUP_ID", "db-writer")
    topic_trades: str = os.getenv("TOPIC_TRADES", "raw.trades")
    topic_ticker: str = os.getenv("TOPIC_TICKER", "raw.ticker")

    # Write to the database every batch_size messages or every flush_interval_s,
    # whichever comes first. Bigger batches = fewer round trips.
    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    flush_interval_s: float = float(os.getenv("FLUSH_INTERVAL_S", "1"))

    retry_initial_s: float = float(os.getenv("RETRY_INITIAL_S", "1"))
    retry_max_s: float = float(os.getenv("RETRY_MAX_S", "30"))

    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    stats_interval_s: float = float(os.getenv("STATS_INTERVAL_S", "10"))


settings = Settings()
