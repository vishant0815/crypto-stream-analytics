"""Prometheus metrics for the API, served at /metrics on the API's own port.

The dashboard's nginx only forwards /api and /ws, so /metrics is reachable by
Prometheus on the internal network but not from the public site.
"""

import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

REQUESTS = Counter("api_requests_total", "HTTP requests, by route and status code", ["route", "status"])
LATENCY = Histogram(
    "api_request_seconds",
    "HTTP request time, by route",
    ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
CACHE = Counter("api_cache_total", "Redis cache lookups for REST responses", ["result"])
for _result in ("hit", "miss", "error"):
    CACHE.labels(_result)  # exported as 0 from the start
WS_CLIENTS = Gauge("api_ws_clients", "Browsers connected to the live WebSocket")
WS_SENT = Counter("api_ws_messages_sent_total", "Live updates sent to browsers")
WS_DROPPED = Counter("api_ws_clients_dropped_total", "WebSocket clients dropped because they were too slow or gone")
KAFKA_MESSAGES = Counter("api_kafka_messages_total", "Messages the live hub read from Kafka, by topic", ["topic"])
KAFKA_CONNECTED = Gauge("api_kafka_connected", "1 while the live hub's Kafka consumer is connected")

CONTENT_TYPE = CONTENT_TYPE_LATEST


def latest() -> bytes:
    return generate_latest()


class MetricsMiddleware:
    """Counts and times every HTTP request under its route template, e.g. /api/candles.

    Templates, not raw paths, keep the number of label values small.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] == "/metrics":
            await self.app(scope, receive, send)
            return
        start = time.perf_counter()
        status = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = scope.get("route")
            name = getattr(route, "path", None) or "unmatched"
            REQUESTS.labels(name, str(status)).inc()
            LATENCY.labels(name).observe(time.perf_counter() - start)
