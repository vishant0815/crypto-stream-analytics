"""Live data: Kafka -> in-memory state -> Redis + WebSocket clients.

One background task reads raw.ticker, raw.trades and agg.metrics from Kafka.
Every message updates the latest state per pair and is queued for the next push.
A second task pushes one batched update per second to every connected browser,
so 50 trades in a second become one message, not 50.

Each API instance reads Kafka without a consumer group (it gets every
partition), so any number of instances can run side by side and all of
them see all the data.
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import WebSocket
from redis.asyncio import Redis

from app.config import settings

log = logging.getLogger(__name__)

REDIS_LATEST = "live:latest"  # hash: pair -> JSON of the latest ticker/candle/indicators
TRADES_PER_PUSH = 50          # most recent trades per pair kept for one push
FEATURE_KEYS = ("abs_ret_z", "range_z", "log_volume_ratio", "log_trades_ratio", "vwap_dev_z", "buy_imbalance")


def _num(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _epoch(iso: str | None) -> float | None:
    """'2026-10-31T21:44:00.000000Z' -> epoch seconds."""
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


@dataclass(eq=False)  # compared by identity, so clients can live in a set
class Client:
    ws: WebSocket
    pairs: set[str]


@dataclass
class LiveState:
    latest: dict[str, dict[str, Any]] = field(default_factory=lambda: defaultdict(dict))
    # Changes since the last push
    pending_tickers: dict[str, dict] = field(default_factory=dict)
    pending_candles: dict[tuple[str, int], dict] = field(default_factory=dict)
    pending_indicators: dict[tuple[str, int], dict] = field(default_factory=dict)
    pending_trades: dict[str, deque] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=TRADES_PER_PUSH)))
    pending_alerts: dict[tuple[str, int], dict] = field(default_factory=dict)
    # Health counters
    received: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    last_message_at: float | None = None
    # (time, totals) once per push tick, for rates over a rolling window
    _samples: deque = field(default_factory=lambda: deque(maxlen=64))

    # ------------------------------------------------------------ ingest

    def apply(self, topic: str, value: dict[str, Any]) -> None:
        pair = value.get("pair")
        if not isinstance(pair, str):
            return
        now = time.time()
        self.last_message_at = now

        if topic == settings.topic_ticker:
            self.received["ticker"] += 1
            ticker = {
                "price": _num(value.get("price")),
                "change_24h_pct": _num(value.get("price_pct_chg_24h")),
                "high_24h": _num(value.get("high_24h")),
                "low_24h": _num(value.get("low_24h")),
                "volume_24h": _num(value.get("volume_24h")),
                "best_bid": _num(value.get("best_bid")),
                "best_ask": _num(value.get("best_ask")),
                "time": _epoch(value.get("exchange_time")),
            }
            self.latest[pair]["ticker"] = ticker
            self.pending_tickers[pair] = ticker

        elif topic == settings.topic_trades:
            self.received["trade"] += 1
            t = _epoch(value.get("exchange_time"))
            # Coinbase replays recent trades after a reconnect; they are not live
            if t is None or value.get("replay") is True or now - t > 60:
                return
            self.pending_trades[pair].append(
                {
                    "time_ms": int(t * 1000),
                    "trade_id": value.get("trade_id"),
                    "price": _num(value.get("price")),
                    "size": _num(value.get("size")),
                    "side": value.get("side"),
                }
            )

        elif topic == settings.topic_alerts:
            bucket = _epoch(value.get("bucket"))
            if bucket is None:
                return
            self.received["alert"] += 1
            alert = {
                "time": int(bucket),
                "symbol": pair,
                "alert_type": value.get("alert_type"),
                "score": value.get("score"),
                "model_ver": value.get("model_ver"),
                "driver": value.get("driver"),
                "features": {k: value.get(k) for k in FEATURE_KEYS if k in value},
            }
            self.pending_alerts[(pair, alert["time"])] = alert

        elif topic == settings.topic_metrics:
            kind = value.get("type")
            bucket = _epoch(value.get("bucket"))
            if bucket is None:
                return
            body = {k: v for k, v in value.items() if k not in ("type", "pair", "bucket")}
            body["time"] = int(bucket)
            if kind == "candle":
                self.received["candle"] += 1
                candle = {
                    "time": body["time"],
                    "open": body.get("open"),
                    "high": body.get("high"),
                    "low": body.get("low"),
                    "close": body.get("close"),
                    "volume": body.get("volume"),
                    "vwap": body.get("vwap"),
                    "trades": body.get("trade_count"),
                }
                # Spark sends a candle again whenever it changes; keep only the newest per minute
                if candle["time"] >= self.latest[pair].get("candle", {}).get("time", 0):
                    self.latest[pair]["candle"] = candle
                self.pending_candles[(pair, candle["time"])] = candle
            elif kind == "indicators":
                self.received["indicators"] += 1
                self.latest[pair]["indicators"] = body
                self.pending_indicators[(pair, body["time"])] = body

    # -------------------------------------------------------------- push

    def take_update(self) -> dict[str, Any] | None:
        """Everything that changed since the last push, grouped by pair; clears the queue."""
        if not (self.pending_tickers or self.pending_candles or self.pending_indicators or self.pending_trades
                or self.pending_alerts):
            return None
        by_pair: dict[str, dict[str, Any]] = defaultdict(dict)
        for pair, ticker in self.pending_tickers.items():
            by_pair[pair]["ticker"] = ticker
        for (pair, _), candle in sorted(self.pending_candles.items(), key=lambda kv: kv[0][1]):
            by_pair[pair].setdefault("candles", []).append(candle)
        for (pair, _), ind in sorted(self.pending_indicators.items(), key=lambda kv: kv[0][1]):
            by_pair[pair].setdefault("indicators", []).append(ind)
        for pair, trades in self.pending_trades.items():
            if trades:
                by_pair[pair]["trades"] = list(trades)[::-1]  # newest first
        for (pair, _), alert in sorted(self.pending_alerts.items(), key=lambda kv: kv[0][1]):
            by_pair[pair].setdefault("alerts", []).append(alert)
        self.pending_tickers.clear()
        self.pending_candles.clear()
        self.pending_indicators.clear()
        self.pending_trades.clear()
        self.pending_alerts.clear()
        return dict(by_pair)

    def snapshot(self) -> dict[str, Any]:
        return {pair: dict(v) for pair, v in self.latest.items()}

    def sample(self) -> None:
        self._samples.append((time.monotonic(), dict(self.received)))

    def rates(self, window_s: float = 10.0) -> dict[str, float]:
        """Messages per second by kind over roughly the last window_s seconds."""
        if len(self._samples) < 2:
            return {}
        now_t, now_counts = self._samples[-1]
        old_t, old_counts = next(
            ((t, c) for t, c in self._samples if now_t - t <= window_s), self._samples[0]
        )
        span = now_t - old_t
        if span <= 0:
            return {}
        return {k: round((v - old_counts.get(k, 0)) / span, 2) for k, v in now_counts.items()}

    def health(self) -> dict[str, Any]:
        return {
            "messages": dict(self.received),
            "messages_per_s": self.rates(),
            "last_message_age_s": None if self.last_message_at is None else round(time.time() - self.last_message_at, 2),
        }


class LiveHub:
    """Owns the Kafka consumer, the shared state and the connected WebSocket clients."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.state = LiveState()
        self.clients: set[Client] = set()
        self.kafka_connected = False
        self._tasks: list[asyncio.Task] = []
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._consume(), name="kafka-consumer"),
            asyncio.create_task(self._push_loop(), name="ws-push"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for client in list(self.clients):
            await client.ws.close(code=1001)

    # ------------------------------------------------------------ Kafka

    async def _consume(self) -> None:
        backoff = 1.0
        while True:
            consumer = AIOKafkaConsumer(
                settings.topic_ticker,
                settings.topic_trades,
                settings.topic_metrics,
                settings.topic_alerts,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=None,               # every instance reads every partition
                auto_offset_reset="latest",  # only live data; history comes from the database
                enable_auto_commit=False,
                client_id="api",
            )
            try:
                await consumer.start()
                self._consumer = consumer
                self.kafka_connected = True
                backoff = 1.0
                log.info("kafka consumer started")
                async for msg in consumer:
                    try:
                        self.state.apply(msg.topic, json.loads(msg.value))
                    except (ValueError, TypeError):
                        log.warning("skipping undecodable message on %s", msg.topic)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # broker down, network error, ...
                log.error("kafka consumer error: %s; retrying in %.0fs", exc, backoff)
            finally:
                self.kafka_connected = False
                self._consumer = None
                await consumer.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    # -------------------------------------------------------- WebSockets

    async def _push_loop(self) -> None:
        while True:
            await asyncio.sleep(settings.push_interval_s)
            self.state.sample()
            update = self.state.take_update()
            if update is None:
                continue
            await self._save_latest(update.keys())
            if self.clients:
                await self._broadcast(update)

    async def _save_latest(self, pairs) -> None:
        """Latest state per pair to Redis, for REST reads and for other API instances."""
        try:
            mapping = {p: json.dumps(self.state.latest[p]) for p in pairs if p in self.state.latest}
            if mapping:
                await self.redis.hset(REDIS_LATEST, mapping=mapping)
        except Exception as exc:
            log.warning("redis write failed: %s", exc)

    async def _broadcast(self, update: dict[str, Any]) -> None:
        ts = time.time()
        sends = []
        clients = list(self.clients)
        for client in clients:
            data = {p: v for p, v in update.items() if not client.pairs or p in client.pairs}
            if data:
                sends.append(self._send(client, {"type": "update", "server_time": ts, "pairs": data}))
        await asyncio.gather(*sends)

    async def _send(self, client: Client, message: dict[str, Any]) -> None:
        try:
            await asyncio.wait_for(client.ws.send_text(json.dumps(message)), settings.ws_send_timeout_s)
        except Exception:
            # Too slow or gone: drop it so it can't hold back everyone else
            self.clients.discard(client)
            try:
                await client.ws.close(code=1011)
            except Exception:
                pass

    async def add_client(self, ws: WebSocket, pairs: set[str]) -> Client | None:
        if len(self.clients) >= settings.max_ws_clients:
            await ws.close(code=1013)  # try again later
            return None
        client = Client(ws, pairs)
        self.clients.add(client)
        snapshot = await self.latest()
        if pairs:
            snapshot = {p: v for p, v in snapshot.items() if p in pairs}
        await ws.send_text(json.dumps({"type": "snapshot", "server_time": time.time(), "pairs": snapshot}))
        return client

    async def latest(self) -> dict[str, dict[str, Any]]:
        """Latest state per pair: Redis (survives restarts, shared by instances) overlaid with memory."""
        merged: dict[str, dict[str, Any]] = {}
        try:
            for pair, raw in (await self.redis.hgetall(REDIS_LATEST)).items():
                merged[pair.decode() if isinstance(pair, bytes) else pair] = json.loads(raw)
        except Exception as exc:
            log.warning("redis read failed: %s", exc)
        for pair, value in self.state.snapshot().items():
            merged.setdefault(pair, {}).update(value)
        return merged

    def remove_client(self, client: Client) -> None:
        self.clients.discard(client)
