"""Prometheus metrics for the Spark streaming jobs, served at :METRICS_PORT/metrics.

Spark reports each finished micro-batch to a StreamingQueryListener in the
driver. MetricsListener turns those reports into Prometheus metrics, labelled by
query name, so Grafana can chart batch times, throughput, state size and how far
each query is behind Kafka.
"""

import logging
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("spark-metrics")

BATCH_SECONDS = Histogram(
    "spark_streaming_batch_seconds",
    "Time to run one micro-batch, from trigger to commit",
    ["query"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 30, 60),
)
INPUT_ROWS = Counter("spark_streaming_input_rows_total", "Rows read from Kafka", ["query"])
INPUT_RATE = Gauge("spark_streaming_input_rows_per_second", "Rate rows arrived in the last batch", ["query"])
PROCESSED_RATE = Gauge(
    "spark_streaming_processed_rows_per_second", "Rate the last batch processed rows at", ["query"]
)
STATE_ROWS = Gauge("spark_streaming_state_rows", "Rows held in the state store (open windows, dedup keys)", ["query"])
STATE_BYTES = Gauge("spark_streaming_state_memory_bytes", "Memory used by the state store", ["query"])
OFFSETS_BEHIND = Gauge(
    "spark_streaming_kafka_offsets_behind",
    "Messages in Kafka the query has not read yet (max over partitions)",
    ["query"],
)
WATERMARK_DELAY = Gauge(
    "spark_streaming_watermark_delay_seconds",
    "How far the event-time watermark is behind the wall clock",
    ["query"],
)
LAST_PROGRESS = Gauge(
    "spark_streaming_last_progress_timestamp_seconds", "Unix time of the query's last progress report", ["query"]
)
BATCHES = Counter("spark_streaming_batches_total", "Micro-batches completed", ["query"])
FAILED = Counter("spark_streaming_query_failures_total", "Queries that stopped with an error", ["query"])

# Written by the candles job's micro-batch handlers
ALERTS = Counter("spark_anomaly_alerts_total", "Anomaly alerts raised", ["alert_type"])
MODEL = Gauge("spark_anomaly_model_info", "The model the live job scores with (value is always 1)", ["version"])


def _float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_time(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None


class MetricsListener(StreamingQueryListener):
    """Records every progress report of every query as Prometheus metrics."""

    def __init__(self) -> None:
        self._names: dict[str, str] = {}  # query id -> name, for the terminated event

    def onQueryStarted(self, event) -> None:
        self._names[str(event.id)] = event.name or str(event.id)

    def onQueryProgress(self, event) -> None:
        try:
            self._record(event.progress)
        except Exception as exc:  # metrics must never take down the stream
            log.warning("could not record progress metrics: %s", exc)

    def _record(self, p) -> None:
        q = p.name or str(p.id)
        BATCHES.labels(q).inc()
        LAST_PROGRESS.labels(q).set_to_current_time()
        INPUT_ROWS.labels(q).inc(p.numInputRows or 0)
        INPUT_RATE.labels(q).set(p.inputRowsPerSecond or 0)
        PROCESSED_RATE.labels(q).set(p.processedRowsPerSecond or 0)

        trigger_ms = (p.durationMs or {}).get("triggerExecution")
        if trigger_ms is not None:
            BATCH_SECONDS.labels(q).observe(trigger_ms / 1000)

        operators = p.stateOperators or []
        STATE_ROWS.labels(q).set(sum(op.numRowsTotal for op in operators))
        STATE_BYTES.labels(q).set(sum(op.memoryUsedBytes for op in operators))

        behind = [_float((s.metrics or {}).get("maxOffsetsBehindLatest")) for s in (p.sources or [])]
        behind = [b for b in behind if b is not None]
        if behind:
            OFFSETS_BEHIND.labels(q).set(max(behind))

        watermark = _parse_time((p.eventTime or {}).get("watermark"))
        if watermark is not None and watermark.year > 1970:  # epoch 0 = no watermark yet
            WATERMARK_DELAY.labels(q).set((datetime.now(timezone.utc) - watermark).total_seconds())

    def onQueryIdle(self, event) -> None:
        # No new data: the query is alive, just waiting
        LAST_PROGRESS.labels(self._names.get(str(event.id), str(event.id))).set_to_current_time()

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            FAILED.labels(self._names.get(str(event.id), str(event.id))).inc()


def serve(port: int) -> None:
    """Start the /metrics endpoint. Port 0 turns metrics off."""
    if port:
        start_http_server(port)
