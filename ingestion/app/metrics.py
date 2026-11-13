"""Prometheus metrics for the ingestion service, served at :METRICS_PORT/metrics."""

from collections.abc import Iterator
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, Histogram, start_http_server
from prometheus_client.core import CounterMetricFamily
from prometheus_client.registry import Collector

EVENTS = Counter(
    "ingestion_events_total",
    "Validated events received from Coinbase, by kind",
    ["kind"],  # trade | ticker
)
DLQ = Counter("ingestion_dlq_total", "Messages sent to the dead-letter topic because they failed validation")
EXCHANGE_LAG = Histogram(
    "ingestion_exchange_lag_seconds",
    "Time from a trade happening on Coinbase to this service receiving it",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
LAST_MESSAGE = Gauge(
    "ingestion_last_message_timestamp_seconds",
    "Unix time of the last message from Coinbase (heartbeats included)",
)


class _ClientCounters(Collector):
    """Exposes counters the Kafka sink and the WebSocket client already keep."""

    def __init__(self, sink: Any, stream: Any) -> None:
        self.sink, self.stream = sink, stream

    def collect(self) -> Iterator[CounterMetricFamily]:
        delivered = CounterMetricFamily(
            "ingestion_kafka_messages",
            "Messages handed to Kafka, by delivery result",
            labels=["result"],
        )
        delivered.add_metric(["delivered"], self.sink.sent)
        delivered.add_metric(["failed"], self.sink.failed)
        yield delivered
        yield CounterMetricFamily(
            "ingestion_reconnects", "WebSocket reconnections to Coinbase", value=self.stream.reconnects
        )
        yield CounterMetricFamily(
            "ingestion_sequence_gaps", "Gaps in Coinbase's message sequence numbers", value=self.stream.sequence_gaps
        )


def serve(port: int, sink: Any, stream: Any) -> None:
    """Start the /metrics endpoint. Port 0 turns metrics off."""
    if not port:
        return
    REGISTRY.register(_ClientCounters(sink, stream))
    start_http_server(port)
