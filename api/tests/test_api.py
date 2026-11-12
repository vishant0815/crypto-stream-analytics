import json
import time

import psycopg
import pytest
import redis
from confluent_kafka import Producer

from app.config import settings
from tests.conftest import DB_URL, FIRST, N_CANDLES, REDIS_URL

# ------------------------------------------------------------------- REST


def test_health(client):
    body = client.get("/api/health").json()
    assert body["database"] is True and body["redis"] is True


def test_pairs(client):
    symbols = [p["symbol"] for p in client.get("/api/pairs").json()]
    assert symbols[:5] == ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]


def test_candles_are_oldest_first_and_limited(client):
    body = client.get("/api/candles", params={"pair": "BTC-USD", "interval": "1m", "limit": 30}).json()
    candles = body["candles"]
    assert len(candles) == 30
    times = [c["time"] for c in candles]
    assert times == sorted(times)
    # The newest 30 of 120: closes 190..219
    assert candles[0]["close"] == 190 and candles[-1]["close"] == 219
    assert candles[-1]["time"] == int((FIRST.timestamp())) + (N_CANDLES - 1) * 60


def test_candles_include_matching_indicators(client):
    body = client.get("/api/candles", params={"pair": "BTC-USD", "limit": 10}).json()
    assert len(body["indicators"]) == 10
    assert [i["time"] for i in body["indicators"]] == [c["time"] for c in body["candles"]]
    assert body["indicators"][-1]["sma_5"] == 217 and body["indicators"][-1]["rsi_14"] == 70


@pytest.mark.parametrize("interval,minutes", [("5m", 5), ("1h", 60)])
def test_rollups_from_continuous_aggregates(client, interval, minutes):
    candles = client.get("/api/candles", params={"pair": "BTC-USD", "interval": interval, "limit": 1000}).json()["candles"]
    assert sum(c["trades"] for c in candles) == 10 * N_CANDLES  # every 1-minute candle counted once
    full = [c for c in candles if c["trades"] == 10 * minutes]
    assert full, "expected at least one complete bucket"
    c = full[0]
    first_close = c["close"] - (minutes - 1)
    assert c["open"] == first_close - 0.5 and c["high"] == c["close"] + 1 and c["low"] == first_close - 1


def test_bad_input_is_rejected(client):
    assert client.get("/api/candles", params={"pair": "btc'; drop table trades;--"}).status_code == 422
    assert client.get("/api/candles", params={"interval": "7m"}).status_code == 422
    assert client.get("/api/candles", params={"limit": 5000}).status_code == 422


def test_overview(client):
    rows = {r["symbol"]: r for r in client.get("/api/overview").json()}
    btc = rows["BTC-USD"]
    assert btc["price"] == 219.5 and btc["change_24h_pct"] == 1.5
    # One close per minute of the last hour (59 or 60, depending on the second the test runs)
    assert len(btc["spark"]) in (59, 60)
    assert btc["spark"][-1] == 219
    assert btc["rsi_14"] == 70
    assert rows["SOL-USD"]["price"] is None  # no data for SOL: present, but empty


def test_recent_trades_newest_first(client):
    trades = client.get("/api/trades/recent", params={"pair": "BTC-USD", "limit": 5}).json()
    assert [t["trade_id"] for t in trades] == [1049, 1048, 1047, 1046, 1045]


def test_pipeline_stats(client):
    body = client.get("/api/pipeline").json()
    assert body["latency"]["trades"] == 50
    assert body["latency"]["p50_ms"] == pytest.approx(150, abs=1)
    assert len(body["trades_per_minute"]) == 30
    assert {s["name"] for s in body["storage"]} >= {"trades", "candles_1m"}
    assert "live" in body


def test_responses_are_cached_in_redis(client):
    client.get("/api/candles", params={"pair": "ETH-USD", "limit": 7})
    r = redis.Redis.from_url(REDIS_URL)
    key = "cache:candles:ETH-USD:1m:7"
    assert r.exists(key) and 0 < r.pttl(key) <= settings.cache_ttl_s * 1000


def test_api_database_connections_are_read_only(client):
    async def try_write():
        return await client.app.state.db.fetch("INSERT INTO pairs (symbol, base, quote) VALUES ('HACK-USD','HACK','USD')")

    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        client.portal.call(try_write)
    with psycopg.connect(DB_URL) as conn:
        assert conn.execute("SELECT count(*) FROM pairs WHERE symbol = 'HACK-USD'").fetchone()[0] == 0


# -------------------------------------------------------------- WebSocket


def wait_for_kafka(client, timeout=30):
    deadline = time.time() + timeout
    while not client.app.state.hub.kafka_connected:
        assert time.time() < deadline, "API never connected to Kafka"
        time.sleep(0.2)
    time.sleep(2)  # let the consumer finish joining its partitions


def produce_live_data(pair: str) -> None:
    now = time.time()
    iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + ".000000Z"
    minute = time.strftime("%Y-%m-%dT%H:%M:00.000000Z", time.gmtime(now))
    # Same compression as the real ingestion service: the API must be able to decode it
    p = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "compression.type": "lz4"})
    p.produce(settings.topic_ticker, key=pair, value=json.dumps(
        {"pair": pair, "price": "123.45", "price_pct_chg_24h": "-2.5", "exchange_time": iso, "ingest_time": iso}))
    p.produce(settings.topic_trades, key=pair, value=json.dumps(
        {"pair": pair, "trade_id": "77", "price": "123.45", "size": "0.5", "side": "BUY", "exchange_time": iso, "ingest_time": iso}))
    p.produce(settings.topic_metrics, key=pair, value=json.dumps(
        {"type": "candle", "pair": pair, "bucket": minute, "open": 120.0, "high": 124.0, "low": 119.0,
         "close": 123.45, "volume": 3.0, "vwap": 122.0, "trade_count": 12}))
    p.flush(10)


def receive_until(ws, predicate, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = json.loads(ws.receive_text())
        if predicate(msg):
            return msg
    raise AssertionError("expected message not received")


def test_websocket_snapshot_then_live_update(client):
    wait_for_kafka(client)
    with client.websocket_connect("/ws/live") as ws:
        assert json.loads(ws.receive_text())["type"] == "snapshot"
        produce_live_data("TST-USD")
        msg = receive_until(ws, lambda m: m["type"] == "update" and "TST-USD" in m["pairs"])
        data = msg["pairs"]["TST-USD"]
    # Messages for one pair can arrive in the same or consecutive pushes
    assert data.get("ticker", {}).get("price") == 123.45 or data.get("candles") or data.get("trades")


def test_websocket_combines_ticker_candle_and_trade(client):
    wait_for_kafka(client)
    seen: dict = {}
    with client.websocket_connect("/ws/live?pairs=TS2-USD") as ws:
        ws.receive_text()  # snapshot
        produce_live_data("TS2-USD")
        deadline = time.time() + 15
        while time.time() < deadline and not {"ticker", "candles", "trades"} <= seen.keys():
            msg = json.loads(ws.receive_text())
            if msg["type"] == "update":
                seen.update(msg["pairs"].get("TS2-USD", {}))
    assert seen["ticker"]["price"] == 123.45 and seen["ticker"]["change_24h_pct"] == -2.5
    assert seen["candles"][-1]["close"] == 123.45 and seen["candles"][-1]["trades"] == 12
    assert seen["trades"][0]["side"] == "BUY" and seen["trades"][0]["size"] == 0.5


def test_websocket_only_sends_subscribed_pairs(client):
    wait_for_kafka(client)
    with client.websocket_connect("/ws/live?pairs=TS3-USD") as ws:
        ws.receive_text()
        produce_live_data("TS4-USD")  # not subscribed
        produce_live_data("TS3-USD")
        msg = receive_until(ws, lambda m: m["type"] == "update" and "TS3-USD" in m["pairs"])
        assert "TS4-USD" not in msg["pairs"]


def test_latest_state_is_saved_to_redis_and_used_for_new_clients(client):
    wait_for_kafka(client)
    with client.websocket_connect("/ws/live") as ws:
        ws.receive_text()
        produce_live_data("TS5-USD")
        receive_until(ws, lambda m: m["type"] == "update" and "TS5-USD" in m["pairs"])
    saved = redis.Redis.from_url(REDIS_URL).hget("live:latest", "TS5-USD")
    assert json.loads(saved)["ticker"]["price"] == 123.45
    # A browser that connects later gets it straight away in the snapshot
    with client.websocket_connect("/ws/live?pairs=TS5-USD") as ws:
        snap = json.loads(ws.receive_text())
    assert snap["pairs"]["TS5-USD"]["ticker"]["price"] == 123.45


def test_ping_pong(client):
    with client.websocket_connect("/ws/live") as ws:
        ws.receive_text()
        ws.send_text(json.dumps({"type": "ping"}))
        assert receive_until(ws, lambda m: m["type"] == "pong")



def test_message_rates_use_a_rolling_window():
    from app.live import LiveState

    state = LiveState()
    state.received["trade"] = 0
    state._samples.append((100.0, {"trade": 0}))
    state._samples.append((105.0, {"trade": 50}))
    state._samples.append((110.0, {"trade": 150}))
    assert state.rates(window_s=10)["trade"] == 15.0   # 150 trades over 10 s
    assert state.rates(window_s=5)["trade"] == 20.0    # last 5 s only: 100 trades


def test_numbers_are_json_numbers_not_strings(client):
    """Pydantic v2 sends PostgreSQL numeric as strings; every query casts to float8 instead."""
    def check(value, path="$"):
        if isinstance(value, dict):
            for k, v in value.items():
                check(v, f"{path}.{k}")
        elif isinstance(value, list):
            for i, v in enumerate(value):
                check(v, f"{path}[{i}]")
        elif isinstance(value, str):
            try:
                float(value)
            except ValueError:
                return
            raise AssertionError(f"{path} is a number sent as a string: {value!r}")

    for path in ["/api/overview", "/api/pipeline", "/api/candles", "/api/trades/recent", "/api/pairs", "/api/analytics",
                 "/api/alerts", "/api/model"]:
        redis.Redis.from_url(REDIS_URL).flushdb()
        check(client.get(path).json(), path)  # straight from the database
        check(client.get(path).json(), path)  # from the Redis cache


def test_replayed_trades_are_left_out_of_latency(client):
    with psycopg.connect(DB_URL) as conn:
        conn.execute(
            """INSERT INTO trades (time, pair_id, trade_id, price, size, side, ingest_time, is_replay)
               SELECT now() - INTERVAL '50 seconds', pair_id, 5000, 1, 1, 'BUY', now(), true
               FROM pairs WHERE symbol = 'ETH-USD'"""
        )
    redis.Redis.from_url(REDIS_URL).delete("cache:pipeline")
    latency = client.get("/api/pipeline").json()["latency"]
    assert latency["p99_ms"] < 1000  # the 50-second-old replayed trade is not counted


def test_analytics_from_the_nightly_batch(client):
    with psycopg.connect(DB_URL) as conn:
        conn.execute("DELETE FROM analytics_daily; DELETE FROM analytics_hourly; DELETE FROM analytics_correlation; DELETE FROM batch_runs")
        conn.execute(
            """INSERT INTO analytics_daily (dt, pair_id, open, high, low, close, return_pct, range_pct, realized_vol_pct,
                   volume, notional_usd, vwap, trades, buy_volume_pct, largest_trade_usd, active_minutes)
               SELECT d::date, pair_id, 100, 110, 90, 105, 5, 20, 1.5, 10, 1000, 101, 50, 55, 300, 1200
               FROM pairs, (VALUES ('2026-11-09'), ('2026-11-10')) v(d) WHERE symbol IN ('BTC-USD', 'ETH-USD')"""
        )
        conn.execute(
            """INSERT INTO analytics_hourly (dt, hour, pair_id, trades, volume, notional_usd, volatility_pct)
               SELECT '2026-11-10', h, pair_id, 10, 1, CASE WHEN h = 14 THEN 300 ELSE 100 END, 0.1
               FROM pairs, generate_series(0, 1) g(x), (VALUES (14), (15)) v(h) WHERE symbol = 'BTC-USD' AND x = 0"""
        )
        conn.execute(
            """INSERT INTO analytics_correlation (dt, pair_a, pair_b, correlation, minutes)
               SELECT '2026-11-10', a.pair_id, b.pair_id, 0.8, 1300 FROM pairs a, pairs b
               WHERE a.symbol = 'BTC-USD' AND b.symbol = 'ETH-USD'"""
        )
        conn.execute(
            """INSERT INTO batch_runs (job, dt, started_at, finished_at, status, rows_hdfs, rows_db, files_before, files_after, details)
               VALUES ('daily', '2026-11-10', now(), now(), 'ok', 3513, 3513, 65, 5, '{"seconds": 13.4}')"""
        )
    body = client.get("/api/analytics", params={"days": 1}).json()
    assert body["latest_dt"] == "2026-11-10"
    assert {(d["dt"], d["symbol"]) for d in body["daily"]} == {("2026-11-10", "BTC-USD"), ("2026-11-10", "ETH-USD")}
    shares = {h["hour"]: h["share_pct"] for h in body["hourly"]}
    assert shares == {14: 75.0, 15: 25.0}  # share of the pair's traded value in each hour
    assert body["correlation"] == [{"pair_a": "BTC-USD", "pair_b": "ETH-USD", "correlation": 0.8, "minutes": 1300}]
    run = body["runs"][0]
    assert (run["status"], run["rows_hdfs"], run["rows_db"], run["files_before"], run["files_after"]) == ("ok", 3513, 3513, 65, 5)
    assert run["seconds"] == 13.4
    assert client.get("/api/analytics", params={"days": 2}).json()["daily"].__len__() == 4


def test_alerts_and_model_endpoints(client):
    with psycopg.connect(DB_URL) as conn:
        conn.execute("DELETE FROM alerts; DELETE FROM model_registry")
        conn.execute(
            """INSERT INTO alerts (time, pair_id, alert_type, score, details, model_ver)
               SELECT now() - make_interval(mins => m), pair_id, t, s,
                      jsonb_build_object('driver', d, 'features', jsonb_build_object('log_volume_ratio', 3.4)), 'rules-v1'
               FROM pairs, (VALUES (2, 'VOLUME_SPIKE', 2.5, 'log_volume_ratio'), (9, 'PRICE_JUMP', 1.4, 'abs_ret_z')) v(m, t, s, d)
               WHERE symbol = 'SOL-USD'"""
        )
    redis.Redis.from_url(REDIS_URL).flushdb()
    got = client.get("/api/alerts").json()
    assert [a["alert_type"] for a in got] == ["VOLUME_SPIKE", "PRICE_JUMP"]  # newest first
    assert got[0]["symbol"] == "SOL-USD" and got[0]["driver"] == "log_volume_ratio"
    assert got[0]["features"]["log_volume_ratio"] == 3.4
    assert client.get("/api/alerts", params={"pair": "BTC-USD"}).json() == []
    assert client.get("/api/alerts", params={"pair": "bad"}).status_code == 422

    body = client.get("/api/model").json()
    assert body["mode"] == "rules" and body["model"] is None
    assert (body["alerts_24h"], body["volume_spikes_24h"], body["price_jumps_24h"]) == (2, 1, 1)


def test_alerts_are_pushed_over_the_websocket(client):
    wait_for_kafka(client)
    with client.websocket_connect("/ws/live?pairs=TS6-USD") as ws:
        ws.receive_text()
        minute = time.strftime("%Y-%m-%dT%H:%M:00.000000Z", time.gmtime())
        p = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "compression.type": "lz4"})
        p.produce(settings.topic_alerts, key="TS6-USD", value=json.dumps(
            {"type": "alert", "pair": "TS6-USD", "bucket": minute, "alert_type": "VOLUME_SPIKE", "score": 4.2,
             "driver": "log_volume_ratio", "model_ver": "kmeans-test", "log_volume_ratio": 3.1, "abs_ret_z": 0.4}))
        p.flush(10)
        msg = receive_until(ws, lambda m: m["type"] == "update" and "alerts" in m["pairs"].get("TS6-USD", {}))
    alert = msg["pairs"]["TS6-USD"]["alerts"][0]
    assert (alert["alert_type"], alert["score"], alert["model_ver"]) == ("VOLUME_SPIKE", 4.2, "kmeans-test")
    assert alert["features"] == {"log_volume_ratio": 3.1, "abs_ret_z": 0.4}
