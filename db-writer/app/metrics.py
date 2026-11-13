"""Prometheus metrics for the db-writer service, served at :METRICS_PORT/metrics."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

ROWS = Counter(
    "dbwriter_rows_total",
    "Rows handled by the database, by table and outcome (duplicates and rejects are counted across both tables)",
    ["table", "outcome"],  # outcome: inserted | duplicate | rejected
)
BAD_MESSAGES = Counter("dbwriter_bad_messages_total", "Kafka messages that could not be decoded and were skipped")
DB_ERRORS = Counter("dbwriter_db_errors_total", "Database writes that failed and were retried")
BATCH_SECONDS = Histogram(
    "dbwriter_batch_write_seconds",
    "Time to write one batch to TimescaleDB (COPY into staging + insert), including commit",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
BATCH_MESSAGES = Histogram(
    "dbwriter_batch_messages",
    "Kafka messages per batch",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)
TRADE_TO_DB = Histogram(
    "dbwriter_trade_to_db_seconds",
    "Time from a trade happening on Coinbase to it being stored in TimescaleDB",
    buckets=(0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30),
)
LAST_COMMIT = Gauge("dbwriter_last_commit_timestamp_seconds", "Unix time of the last committed batch")


def serve(port: int) -> None:
    """Start the /metrics endpoint. Port 0 turns metrics off."""
    if port:
        start_http_server(port)
