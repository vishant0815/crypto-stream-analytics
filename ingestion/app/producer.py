"""Kafka publishing, plus a console sink for running without Kafka."""

import json
import logging
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message, Producer

log = logging.getLogger(__name__)


def to_json(value: Any) -> bytes:
    # default=str turns Decimal and datetime into strings
    return json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")


class Sink(Protocol):
    sent: int
    failed: int

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None: ...
    def poll(self) -> None: ...
    def close(self) -> None: ...


class KafkaSink:
    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "crypto-ingestion",
                # No duplicates or lost messages on producer retries
                "enable.idempotence": True,
                "acks": "all",
                # Small batching window: better throughput, still well under 1 s latency
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
        self.sent = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed topic=%s key=%s error=%s", msg.topic(), msg.key(), err)
        else:
            self.sent += 1

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        try:
            self._producer.produce(
                topic,
                key=key.encode("utf-8") if key else None,
                value=to_json(value),
                on_delivery=self._on_delivery,
            )
        except BufferError:
            # Local queue is full: serve delivery reports to make room, then retry once
            self._producer.poll(1)
            self._producer.produce(
                topic,
                key=key.encode("utf-8") if key else None,
                value=to_json(value),
                on_delivery=self._on_delivery,
            )

    def poll(self) -> None:
        self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(10)
        if remaining:
            log.warning("%d messages were not delivered before shutdown", remaining)


class ConsoleSink:
    """Prints events instead of sending them. Used with --dry-run."""

    def __init__(self) -> None:
        self.sent = 0
        self.failed = 0

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        self.sent += 1
        print(f"[{topic}] {key}: {to_json(value).decode()}", flush=True)

    def poll(self) -> None:
        pass

    def close(self) -> None:
        pass
