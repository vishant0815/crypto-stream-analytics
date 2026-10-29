"""Coinbase WebSocket client with automatic reconnect."""

import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator
from typing import Any

import websockets

log = logging.getLogger(__name__)

# Coinbase takes one channel per subscribe message.
# heartbeats keeps the connection open when a pair is quiet.
CHANNELS = ("market_trades", "ticker", "heartbeats")


class CoinbaseStream:
    def __init__(
        self,
        url: str,
        product_ids: list[str],
        backoff_initial_s: float = 1.0,
        backoff_max_s: float = 30.0,
        idle_timeout_s: float = 30.0,
    ) -> None:
        self.url = url
        self.product_ids = product_ids
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.idle_timeout_s = idle_timeout_s
        self.reconnects = 0
        self.sequence_gaps = 0
        self._last_sequence: int | None = None

    def _subscribe_messages(self) -> list[str]:
        return [
            json.dumps({"type": "subscribe", "product_ids": self.product_ids, "channel": channel})
            for channel in CHANNELS
        ]

    def _check_sequence(self, message: dict[str, Any]) -> None:
        seq = message.get("sequence_num")
        if not isinstance(seq, int):
            return
        if self._last_sequence is not None and seq != self._last_sequence + 1:
            self.sequence_gaps += 1
            log.warning("sequence gap: expected %d, got %d", self._last_sequence + 1, seq)
        self._last_sequence = seq

    async def messages(self) -> AsyncIterator[dict[str, Any]]:
        """Yield decoded messages forever, reconnecting with exponential backoff."""
        backoff = self.backoff_initial_s
        while True:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_size=10 * 1024 * 1024
                ) as ws:
                    for sub in self._subscribe_messages():
                        await ws.send(sub)
                    log.info("connected to %s, subscribed %s to %s", self.url, self.product_ids, CHANNELS)
                    self._last_sequence = None  # sequence restarts on a new connection
                    backoff = self.backoff_initial_s

                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
                        try:
                            message = json.loads(raw)
                        except json.JSONDecodeError:
                            yield {"channel": "_invalid", "raw": raw}
                            continue
                        self._check_sequence(message)
                        yield message

            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                log.warning("no data for %.0fs, reconnecting", self.idle_timeout_s)
            except (websockets.ConnectionClosed, OSError, websockets.InvalidHandshake) as exc:
                log.warning("connection lost: %s", exc)

            self.reconnects += 1
            # Jitter stops many clients reconnecting at exactly the same moment
            delay = backoff + random.uniform(0, backoff / 2)
            log.info("reconnecting in %.1fs (attempt %d)", delay, self.reconnects)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, self.backoff_max_s)
