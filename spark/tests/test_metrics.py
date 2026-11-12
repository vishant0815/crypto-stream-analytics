"""MetricsListener turns Spark progress reports into Prometheus metrics."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

from prometheus_client import REGISTRY

from streaming.metrics import MetricsListener


def value(name: str, **labels: str) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


def progress(name: str, rows: int, watermark: str) -> NS:
    return NS(
        name=name, id="q1", numInputRows=rows, inputRowsPerSecond=40.0, processedRowsPerSecond=250.0,
        durationMs={"triggerExecution": 1500},
        stateOperators=[NS(numRowsTotal=12, memoryUsedBytes=2048), NS(numRowsTotal=3, memoryUsedBytes=1024)],
        sources=[NS(metrics={"maxOffsetsBehindLatest": "7", "avgOffsetsBehindLatest": "3.5"})],
        eventTime={"watermark": watermark},
    )


def test_progress_becomes_metrics():
    listener = MetricsListener()
    wm = (datetime.now(timezone.utc) - timedelta(seconds=45)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    listener.onQueryProgress(NS(progress=progress("test-q", 80, wm)))
    listener.onQueryProgress(NS(progress=progress("test-q", 20, wm)))

    assert value("spark_streaming_input_rows_total", query="test-q") == 100
    assert value("spark_streaming_batches_total", query="test-q") == 2
    assert value("spark_streaming_batch_seconds_count", query="test-q") == 2
    assert value("spark_streaming_batch_seconds_sum", query="test-q") == 3.0
    assert value("spark_streaming_state_rows", query="test-q") == 15
    assert value("spark_streaming_state_memory_bytes", query="test-q") == 3072
    assert value("spark_streaming_kafka_offsets_behind", query="test-q") == 7
    assert 44 <= value("spark_streaming_watermark_delay_seconds", query="test-q") < 60


def test_missing_fields_do_not_break_the_stream():
    listener = MetricsListener()
    bare = NS(name="bare-q", id="q2", numInputRows=0, inputRowsPerSecond=None, processedRowsPerSecond=None,
              durationMs={}, stateOperators=[], sources=[NS(metrics={})],
              eventTime={"watermark": "1970-01-01T00:00:00.000Z"})
    listener.onQueryProgress(NS(progress=bare))
    assert value("spark_streaming_batches_total", query="bare-q") == 1
    assert value("spark_streaming_watermark_delay_seconds", query="bare-q") is None  # no watermark yet
    listener.onQueryProgress(NS(progress=None))  # garbage is logged, never raised
