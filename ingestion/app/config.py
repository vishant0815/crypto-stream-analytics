"""Settings for the ingestion service, read from environment variables."""

import os
from dataclasses import dataclass, field


def _list(name: str, default: str) -> list[str]:
    return [item.strip().upper() for item in os.getenv(name, default).split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # Coinbase Advanced Trade public market data feed (no API key needed)
    ws_url: str = os.getenv("COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com")
    product_ids: list[str] = field(
        default_factory=lambda: _list("PRODUCT_IDS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD")
    )

    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_trades: str = os.getenv("TOPIC_TRADES", "raw.trades")
    topic_ticker: str = os.getenv("TOPIC_TICKER", "raw.ticker")
    topic_dlq: str = os.getenv("TOPIC_DLQ", "dlq.trades")

    # Reconnect behaviour
    backoff_initial_s: float = float(os.getenv("BACKOFF_INITIAL_S", "1"))
    backoff_max_s: float = float(os.getenv("BACKOFF_MAX_S", "30"))
    # Reconnect if nothing (not even a heartbeat) arrives for this long
    idle_timeout_s: float = float(os.getenv("IDLE_TIMEOUT_S", "30"))

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    stats_interval_s: float = float(os.getenv("STATS_INTERVAL_S", "10"))


settings = Settings()
