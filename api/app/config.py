"""Settings for the API, read from environment variables."""

import os
from dataclasses import dataclass, field


def _list(name: str, default: str) -> list[str]:
    return [v.strip() for v in os.getenv(name, default).split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://crypto:crypto@localhost:5432/crypto")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    topic_trades: str = os.getenv("TOPIC_TRADES", "raw.trades")
    topic_ticker: str = os.getenv("TOPIC_TICKER", "raw.ticker")
    topic_metrics: str = os.getenv("TOPIC_METRICS", "agg.metrics")
    topic_alerts: str = os.getenv("TOPIC_ALERTS", "alerts")

    # How often live updates are pushed to browsers
    push_interval_s: float = float(os.getenv("PUSH_INTERVAL_S", "1"))
    # How long REST responses that hit the database are cached in Redis
    cache_ttl_s: float = float(os.getenv("CACHE_TTL_S", "2"))
    # A WebSocket client that can't take a message within this time is dropped
    ws_send_timeout_s: float = float(os.getenv("WS_SEND_TIMEOUT_S", "2"))
    max_ws_clients: int = int(os.getenv("MAX_WS_CLIENTS", "500"))

    cors_origins: list[str] = field(
        default_factory=lambda: _list("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
