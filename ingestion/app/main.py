"""Entry point: Coinbase WebSocket -> validation -> Kafka.

Run with Kafka:      python -m app.main
Run without Kafka:   python -m app.main --dry-run
"""

import argparse
import asyncio
import logging
import signal
import time
from typing import Any

from app.client import CoinbaseStream
from app.config import settings
from app.models import ParseResult, parse_market_trades, parse_ticker, utc_now
from app.producer import ConsoleSink, KafkaSink, Sink

log = logging.getLogger("ingestion")


class Stats:
    def __init__(self) -> None:
        self.trades = 0
        self.tickers = 0
        self.dlq = 0
        self._window_start = time.monotonic()

    def report(self, sink: Sink, stream: CoinbaseStream) -> None:
        elapsed = time.monotonic() - self._window_start
        log.info(
            "trades/s=%.1f tickers/s=%.1f dlq=%d delivered=%d failed=%d reconnects=%d seq_gaps=%d",
            self.trades / elapsed,
            self.tickers / elapsed,
            self.dlq,
            sink.sent,
            sink.failed,
            stream.reconnects,
            stream.sequence_gaps,
        )
        self.trades = self.tickers = 0
        self._window_start = time.monotonic()


def route(message: dict[str, Any], sink: Sink, stats: Stats) -> None:
    channel = message.get("channel")

    if channel == "market_trades":
        result, topic = parse_market_trades(message), settings.topic_trades
    elif channel == "ticker":
        result, topic = parse_ticker(message), settings.topic_ticker
    elif channel in ("heartbeats", "subscriptions"):
        return
    elif channel == "_invalid" or message.get("type") == "error":
        sink.publish(settings.topic_dlq, None, {"error": "bad message", "raw": message, "ingest_time": utc_now()})
        stats.dlq += 1
        if message.get("type") == "error":
            log.error("coinbase error: %s", message.get("message"))
        return
    else:
        return

    _publish(result, topic, sink, stats)


def _publish(result: ParseResult, topic: str, sink: Sink, stats: Stats) -> None:
    for event in result.events:
        sink.publish(topic, event.pair, event.model_dump(mode="json"))
    for bad in result.errors:
        sink.publish(settings.topic_dlq, None, {**bad, "topic": topic, "ingest_time": utc_now()})

    stats.dlq += len(result.errors)
    if topic == settings.topic_trades:
        stats.trades += len(result.events)
    else:
        stats.tickers += len(result.events)


async def run(dry_run: bool) -> None:
    sink: Sink = ConsoleSink() if dry_run else KafkaSink(settings.kafka_bootstrap_servers)
    stream = CoinbaseStream(
        settings.ws_url,
        settings.product_ids,
        backoff_initial_s=settings.backoff_initial_s,
        backoff_max_s=settings.backoff_max_s,
        idle_timeout_s=settings.idle_timeout_s,
    )
    stats = Stats()
    last_report = time.monotonic()

    try:
        async for message in stream.messages():
            route(message, sink, stats)
            sink.poll()  # serve Kafka delivery callbacks
            if time.monotonic() - last_report >= settings.stats_interval_s:
                stats.report(sink, stream)
                last_report = time.monotonic()
    finally:
        log.info("shutting down, flushing producer")
        sink.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Coinbase market data into Kafka")
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending to Kafka")
    args = parser.parse_args()

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    loop = asyncio.new_event_loop()
    task = loop.create_task(run(args.dry_run))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
