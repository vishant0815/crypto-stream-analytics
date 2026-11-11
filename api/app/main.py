"""FastAPI app: REST endpoints for history, a WebSocket for live updates.

Run locally:  uvicorn app.main:app --reload --port 8000   (from the api/ folder)
Docs:         http://localhost:8000/docs
"""

import asyncio
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from redis.asyncio import Redis

from app.config import settings
from app.db import Database
from app.live import LiveHub

log = logging.getLogger("api")

PAIR = re.compile(r"^[A-Z0-9]{2,10}-[A-Z]{2,5}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    db = Database()
    redis = Redis.from_url(settings.redis_url, socket_timeout=2, socket_connect_timeout=2)
    await db.open()
    hub = LiveHub(redis)
    await hub.start()
    app.state.db, app.state.redis, app.state.hub = db, redis, hub
    log.info("api started")
    try:
        yield
    finally:
        await hub.stop()
        await redis.aclose()
        await db.close()


app = FastAPI(
    title="Crypto Stream Analytics API",
    description="Live crypto market data: candles, indicators, trades and pipeline health.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["GET"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=1000)


# --------------------------------------------------------------------------- helpers

def check_pair(pair: str) -> str:
    if not PAIR.match(pair):
        raise HTTPException(status_code=422, detail="pair must look like BTC-USD")
    return pair


async def cached(request: Request, key: str, load: Callable[[], Awaitable[Any]]) -> Any:
    """Serve from Redis if fresh, else load from the database and store for cache_ttl_s.

    With many viewers, the database sees at most one query per key per TTL.
    If Redis is down, every request simply goes to the database.
    """
    redis: Redis = request.app.state.redis
    try:
        hit = await redis.get(key)
        if hit is not None:
            return json.loads(hit)
    except Exception as exc:
        log.warning("redis get failed: %s", exc)
    value = await load()
    try:
        await redis.set(key, json.dumps(value, default=float), px=int(settings.cache_ttl_s * 1000))
    except Exception as exc:
        log.warning("redis set failed: %s", exc)
    return value


# ---------------------------------------------------------------------------- routes

@app.get("/api/health", tags=["system"])
async def health(request: Request) -> dict[str, Any]:
    """Status of the database, Redis and the Kafka consumer."""
    checks: dict[str, bool] = {}
    try:
        checks["database"] = await asyncio.wait_for(request.app.state.db.ping(), 3)
    except Exception:
        checks["database"] = False
    try:
        checks["redis"] = bool(await request.app.state.redis.ping())
    except Exception:
        checks["redis"] = False
    checks["kafka"] = request.app.state.hub.kafka_connected
    return {"status": "ok" if all(checks.values()) else "degraded", **checks}


@app.get("/api/pairs", tags=["market"])
async def pairs(request: Request) -> list[dict[str, Any]]:
    return await cached(request, "cache:pairs", request.app.state.db.pairs)


@app.get("/api/candles", tags=["market"])
async def candles(
    request: Request,
    pair: str = "BTC-USD",
    interval: Annotated[str, Query(pattern="^(1m|5m|1h)$")] = "1m",
    limit: Annotated[int, Query(ge=1, le=1000)] = 300,
) -> dict[str, Any]:
    """OHLCV candles, oldest first. For 1m, the indicators for the same minutes are included."""
    check_pair(pair)
    db: Database = request.app.state.db

    async def load() -> dict[str, Any]:
        rows = await db.candles(pair, interval, limit)
        indicators = []
        if interval == "1m" and rows:
            indicators = await db.indicators(pair, rows[0]["time"])
        return {"pair": pair, "interval": interval, "candles": rows, "indicators": indicators}

    return await cached(request, f"cache:candles:{pair}:{interval}:{limit}", load)


@app.get("/api/overview", tags=["market"])
async def overview(request: Request) -> list[dict[str, Any]]:
    """Every pair: price, 24 h and 1 h change, 60-minute sparkline, latest indicators.

    Ticker fields are taken from the live stream when it's fresher than the database.
    """
    rows = await cached(request, "cache:overview", request.app.state.db.overview)
    live = await request.app.state.hub.latest()
    for row in rows:
        ticker = live.get(row["symbol"], {}).get("ticker")
        if ticker and (ticker.get("time") or 0) > (row.get("ticker_time") or 0):
            row.update({k: v for k, v in ticker.items() if k != "time" and v is not None})
            row["ticker_time"] = ticker["time"]
    return rows


@app.get("/api/trades/recent", tags=["market"])
async def recent_trades(
    request: Request,
    pair: str = "BTC-USD",
    limit: Annotated[int, Query(ge=1, le=200)] = 40,
) -> list[dict[str, Any]]:
    """The most recent trades for a pair, newest first."""
    check_pair(pair)
    return await cached(
        request, f"cache:trades:{pair}:{limit}", lambda: request.app.state.db.recent_trades(pair, limit)
    )


@app.get("/api/pipeline", tags=["system"])
async def pipeline(request: Request) -> dict[str, Any]:
    """Pipeline health: ingestion latency, trades per minute, data freshness, storage, live stream rates."""
    stats = await cached(request, "cache:pipeline", request.app.state.db.pipeline_stats)
    hub: LiveHub = request.app.state.hub
    return {**stats, "live": {**hub.state.health(), "kafka_connected": hub.kafka_connected, "ws_clients": len(hub.clients)}}


@app.get("/api/analytics", tags=["batch"])
async def analytics(
    request: Request,
    days: Annotated[int, Query(ge=1, le=90)] = 14,
) -> dict[str, Any]:
    """Nightly batch results from the HDFS archive: daily summary per pair, hourly activity,
    pair correlations for the latest day, and the last batch runs with their HDFS vs
    TimescaleDB reconciliation."""
    return await cached(request, f"cache:analytics:{days}", lambda: request.app.state.db.analytics(days))


# ------------------------------------------------------------------------- websocket

@app.websocket("/ws/live")
async def live(ws: WebSocket, pairs: str = "") -> None:
    """Live updates. Optional ?pairs=BTC-USD,ETH-USD to receive only those pairs.

    First message: {"type": "snapshot", "pairs": {...latest per pair}}
    Then about once a second: {"type": "update", "pairs": {pair: {ticker, candles, indicators, trades}}}
    """
    wanted = {p for p in pairs.split(",") if PAIR.match(p)}
    await ws.accept()
    hub: LiveHub = ws.app.state.hub
    client = await hub.add_client(ws, wanted)
    if client is None:
        return
    try:
        # The browser may send {"type":"subscribe","pairs":[...]} to change pairs, or a ping
        while True:
            message = await ws.receive_text()
            try:
                data = json.loads(message)
            except ValueError:
                continue
            if data.get("type") == "subscribe" and isinstance(data.get("pairs"), list):
                client.pairs = {p for p in data["pairs"] if isinstance(p, str) and PAIR.match(p)}
            elif data.get("type") == "ping":
                await ws.send_text(json.dumps({"type": "pong", "server_time": time.time()}))
    except WebSocketDisconnect:
        pass
    finally:
        hub.remove_client(client)
