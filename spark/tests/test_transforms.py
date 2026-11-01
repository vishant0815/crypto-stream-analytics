"""Unit tests for candle and indicator logic, on small static DataFrames.

The indicator test checks Spark's results against a plain-Python reference
implementation on a random price series with a missing minute in it.
"""

import json
import math
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from streaming.transforms import build_candles, compute_indicators, parse_trades

T0 = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)


def kafka_rows(spark, trades):
    """Fake Kafka records: a binary 'value' column holding JSON."""
    rows = [(json.dumps(t).encode() if isinstance(t, dict) else t,) for t in trades]
    return spark.createDataFrame(rows, "value binary")


def trade(trade_id, price, size="1", side="BUY", seconds=0, pair="BTC-USD"):
    t = T0 + timedelta(seconds=seconds)
    return {
        "pair": pair,
        "trade_id": str(trade_id),
        "price": str(price),
        "size": str(size),
        "side": side,
        "exchange_time": t.isoformat().replace("+00:00", "Z"),
        "ingest_time": t.isoformat().replace("+00:00", "Z"),
    }


# ---------------------------------------------------------------- parsing

def test_parse_keeps_valid_trades_and_drops_bad_ones(spark):
    raw = kafka_rows(
        spark,
        [
            trade(1, "67000.12345"),
            trade(2, "-5"),                       # negative price
            trade(3, "100", side="HOLD"),         # unknown side
            {**trade(4, "100"), "trade_id": "x"},  # id not a number
            {**trade(5, "100"), "price": "NaN"},   # not a usable price
            {**trade(6, "100"), "exchange_time": "yesterday"},
            b"not json",
        ],
    )
    rows = parse_trades(raw).collect()
    assert len(rows) == 1
    assert rows[0]["price"] == Decimal("67000.1234500000")
    assert rows[0]["exchange_time"].year == 2026


# ---------------------------------------------------------------- candles

def candles_for(spark, trades):
    out = build_candles(parse_trades(kafka_rows(spark, trades))).orderBy("pair", "bucket").collect()
    return {(r["pair"], r["bucket"].minute): r for r in out}


def test_ohlcv_and_vwap(spark):
    c = candles_for(
        spark,
        [
            trade(1, "100", size="2", side="BUY", seconds=5),
            trade(2, "110", size="1", side="SELL", seconds=20),
            trade(3, "90", size="1", side="BUY", seconds=40),
            trade(4, "105", size="4", side="SELL", seconds=59),
        ],
    )[("BTC-USD", 0)]
    assert (c["open"], c["high"], c["low"], c["close"]) == (100, 110, 90, 105)
    assert c["volume"] == 8 and c["buy_volume"] == 3 and c["trade_count"] == 4
    # VWAP = (100*2 + 110*1 + 90*1 + 105*4) / 8 = 102.5
    assert c["vwap"] == Decimal("102.5")


def test_vwap_keeps_full_precision_for_small_prices(spark):
    # Spark's default decimal division keeps only 6 decimal places, which
    # visibly distorts the VWAP of a coin priced around $0.07
    c = candles_for(
        spark,
        [
            trade(1, "0.07566", size="14.6", seconds=1),
            trade(2, "0.07567", size="639.6", seconds=2),
            trade(3, "0.07571", size="551.4", seconds=3),
        ],
    )[("BTC-USD", 0)]
    notional = Decimal("0.07566") * Decimal("14.6") + Decimal("0.07567") * Decimal("639.6") + Decimal("0.07571") * Decimal("551.4")
    expected = (notional / Decimal("1205.6")).quantize(Decimal("1e-10"))
    assert c["vwap"] == expected


def test_open_and_close_use_trade_id_when_times_tie(spark):
    c = candles_for(
        spark,
        [trade(11, "101", seconds=10), trade(10, "100", seconds=10), trade(12, "102", seconds=10)],
    )[("BTC-USD", 0)]
    assert c["open"] == 100 and c["close"] == 102


def test_trades_split_by_minute_and_pair(spark):
    c = candles_for(
        spark,
        [
            trade(1, "100", seconds=59),
            trade(2, "200", seconds=60),                  # next minute
            trade(3, "5", seconds=30, pair="SOL-USD"),
        ],
    )
    assert set(c) == {("BTC-USD", 0), ("BTC-USD", 1), ("SOL-USD", 0)}
    assert c[("BTC-USD", 0)]["close"] == 100 and c[("BTC-USD", 1)]["open"] == 200


# ---------------------------------------------------------------- indicators

def candle_history(spark, closes, missing_minutes=(), pair="ETH-USD"):
    rows = [
        (T0 + timedelta(minutes=i), pair, Decimal(str(c)), Decimal("2"), Decimal(str(c)))
        for i, c in enumerate(closes)
        if i not in missing_minutes
    ]
    return spark.createDataFrame(
        rows, "bucket timestamp, pair string, close decimal(20,10), volume decimal(28,12), vwap decimal(20,10)"
    )


def indicators_by_minute(spark, closes, missing=()):
    out = compute_indicators(candle_history(spark, closes, missing)).collect()
    return {int((r["bucket"] - T0.replace(tzinfo=None)).total_seconds() // 60): r for r in out}


def reference(closes, missing, minute):
    """Plain-Python version of the same indicators, for one minute."""
    present = {i: c for i, c in enumerate(closes) if i not in missing}

    def window(n):
        return [present[m] for m in range(minute - n + 1, minute + 1) if m in present]

    def ret(m):  # % return, only between consecutive minutes
        return (present[m] / present[m - 1] - 1) * 100 if m in present and m - 1 in present else None

    def log_ret(m):
        return math.log(present[m] / present[m - 1]) if m in present and m - 1 in present else None

    sma = {n: (sum(window(n)) / n if len(window(n)) == n else None) for n in (5, 15, 60)}

    rets14 = [ret(m) for m in range(minute - 13, minute + 1) if m in present and ret(m) is not None]
    rsi = None
    if len(rets14) == 14:
        gain = sum(max(r, 0) for r in rets14) / 14
        loss = sum(max(-r, 0) for r in rets14) / 14
        rsi = 50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)

    logs15 = [log_ret(m) for m in range(minute - 14, minute + 1) if m in present and log_ret(m) is not None]
    vol = None
    if len(logs15) >= 14:
        mean = sum(logs15) / len(logs15)
        vol = math.sqrt(sum((x - mean) ** 2 for x in logs15) / (len(logs15) - 1)) * 100

    return sma, rsi, vol, ret(minute)


def test_indicators_match_reference_on_random_walk_with_a_gap(spark):
    rng = random.Random(42)
    closes, price = [], 2500.0
    for _ in range(90):
        price *= 1 + rng.gauss(0, 0.002)
        closes.append(round(price, 4))
    missing = {70}  # a minute with no trades

    got = indicators_by_minute(spark, closes, missing)

    for minute in (4, 14, 59, 65, 69, 71, 75, 84, 89):
        sma, rsi, vol, ret = reference(closes, missing, minute)
        row = got[minute]
        for n in (5, 15, 60):
            expected, actual = sma[n], row[f"sma_{n}"]
            if expected is None:
                assert actual is None, (minute, n)
            else:
                assert float(actual) == pytest.approx(expected, abs=1e-6), (minute, n)
        assert (row["rsi_14"] is None) == (rsi is None), minute
        if rsi is not None:
            assert row["rsi_14"] == pytest.approx(rsi, rel=1e-9), minute
        assert (row["volatility_15"] is None) == (vol is None), minute
        if vol is not None:
            assert row["volatility_15"] == pytest.approx(vol, rel=1e-9), minute
        assert (row["return_pct"] is None) == (ret is None), minute


def test_gap_resets_return_and_short_windows(spark):
    closes = [100 + i for i in range(20)]
    got = indicators_by_minute(spark, closes, missing={10})
    assert got[11]["return_pct"] is None      # previous minute missing
    assert got[12]["return_pct"] is not None
    assert got[11]["sma_5"] is None           # window 7..11 holds only 4 candles
    assert got[15]["sma_5"] is not None       # window 11..15 is complete again
    assert got[19]["candles_in_60"] == 19


def test_rising_and_flat_prices(spark):
    rising = indicators_by_minute(spark, [100 + i for i in range(30)])
    assert rising[29]["rsi_14"] == 100.0
    flat = indicators_by_minute(spark, [100] * 30)
    assert flat[29]["rsi_14"] == 50.0
    assert flat[29]["volatility_15"] == 0.0
    assert flat[29]["volume_ratio_15"] == 1.0
