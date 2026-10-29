"""Entry point: Kafka (raw.trades, raw.ticker) -> TimescaleDB.

Delivery guarantee: offsets are committed to Kafka only after the database
transaction commits. If the service crashes in between, the batch is read
again on restart, and ON CONFLICT DO NOTHING skips the rows already stored.
So every trade lands in the database exactly once.
"""

import logging
import signal
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaError, Message

from app.config import settings
from app.db import Database, WriteResult
from app.mapping import BadMessage, TickerRow, TradeRow, decode, to_ticker_row, to_trade_row

log = logging.getLogger("db-writer")


class Stats:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.trades = self.tickers = self.duplicates = self.rejected = self.bad = 0
        self.latency_ms_total = 0.0
        self.latency_samples = 0
        self.window_start = time.monotonic()

    def add(self, result: WriteResult, trades: list[TradeRow]) -> None:
        self.trades += result.trades_inserted
        self.tickers += result.tickers_inserted
        self.duplicates += result.duplicates
        self.rejected += result.rejected
        # How long from the trade happening on the exchange to being stored here
        now = datetime.now(timezone.utc)
        for t in trades:
            self.latency_ms_total += (now - t.time).total_seconds() * 1000
            self.latency_samples += 1

    def report(self) -> None:
        elapsed = time.monotonic() - self.window_start
        avg_latency = self.latency_ms_total / self.latency_samples if self.latency_samples else 0
        log.info(
            "trades/s=%.1f tickers/s=%.1f duplicates=%d rejected=%d bad=%d avg_trade_to_db_ms=%.0f",
            self.trades / elapsed, self.tickers / elapsed,
            self.duplicates, self.rejected, self.bad, avg_latency,
        )
        self.reset()


class Writer:
    def __init__(self) -> None:
        self.running = True
        self.db = Database(settings.database_url)
        self.stats = Stats()
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.group_id,
                "client.id": "db-writer",
                # Offsets are committed by hand, after the database commit
                "enable.auto.commit": False,
                # First start: read everything Kafka still holds
                "auto.offset.reset": "earliest",
            }
        )

    def stop(self, *_: object) -> None:
        self.running = False

    def run(self) -> None:
        self.consumer.subscribe([settings.topic_trades, settings.topic_ticker])
        self._connect_db()
        last_report = time.monotonic()

        try:
            while self.running:
                messages = self.consumer.consume(settings.batch_size, timeout=settings.flush_interval_s)
                if messages:
                    self._handle_batch(messages)
                if time.monotonic() - last_report >= settings.stats_interval_s:
                    self.stats.report()
                    last_report = time.monotonic()
        finally:
            log.info("shutting down")
            self.consumer.close()  # leaves the group cleanly so partitions move at once
            self.db.close()

    def _handle_batch(self, messages: list[Message]) -> None:
        trades: list[TradeRow] = []
        tickers: list[TickerRow] = []

        for msg in messages:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue
            try:
                data = decode(msg.value())
                if msg.topic() == settings.topic_trades:
                    trades.append(to_trade_row(data))
                else:
                    tickers.append(to_ticker_row(data))
            except BadMessage as exc:
                self.stats.bad += 1
                log.warning("skipping %s offset %d: %s", msg.topic(), msg.offset(), exc)

        result = self._write_with_retry(trades, tickers)
        if result is None:  # shutting down before the write succeeded; Kafka will replay it
            return
        self.stats.add(result, trades)
        self.consumer.commit(asynchronous=False)

    def _write_with_retry(self, trades: list[TradeRow], tickers: list[TickerRow]) -> WriteResult | None:
        delay = settings.retry_initial_s
        paused = False
        while True:
            try:
                result = self.db.write(trades, tickers)
                if paused:
                    self.consumer.resume(self.consumer.assignment())
                return result
            except psycopg.OperationalError as exc:
                log.error("database unavailable (%s), retrying in %.0fs", exc, delay)

            # Stop fetching new messages while the database is down, but keep
            # calling poll() so Kafka doesn't think this consumer has died
            if not paused:
                self.consumer.pause(self.consumer.assignment())
                paused = True
            deadline = time.monotonic() + delay
            while time.monotonic() < deadline:
                if not self.running:
                    return None
                self.consumer.poll(0.5)
            delay = min(delay * 2, settings.retry_max_s)
            self._connect_db()

    def _connect_db(self) -> None:
        delay = settings.retry_initial_s
        while self.running:
            try:
                self.db.connect()
                return
            except psycopg.OperationalError as exc:
                log.error("cannot connect to database (%s), retrying in %.0fs", exc, delay)
                time.sleep(delay)
                delay = min(delay * 2, settings.retry_max_s)


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    writer = Writer()
    signal.signal(signal.SIGINT, writer.stop)
    signal.signal(signal.SIGTERM, writer.stop)
    writer.run()


if __name__ == "__main__":
    main()
