"""Tests for the archive and nightly batch logic.

Analytics are checked against plain-Python reference calculations on random
trades; compaction runs against the local filesystem through the same Hadoop
FileSystem API the job uses on HDFS.
"""

import json
import math
import random
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

import lake.batch_job as batch_job
from lake.analytics import daily_summary, dedupe_trades, hourly_profile, minute_returns, pair_correlation
from lake.archive_job import parse_for_archive
from lake.config import LakeSettings
from lake.scheduler import days_to_catch_up, next_run_after

DAY = date(2026, 11, 10)
T0 = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)
SCHEMA = (
    "pair string, trade_id long, price decimal(20,10), size decimal(28,12), side string, "
    "exchange_time timestamp, ingest_time timestamp, is_replay boolean, kafka_partition int, kafka_offset long"
)


def naive(t: datetime) -> datetime:
    """Spark hands back naive UTC datetimes (the tests run with TZ=UTC)."""
    return t.astimezone(timezone.utc).replace(tzinfo=None)


def random_day(seed=7, minutes=180, pairs=("BTC-USD", "ETH-USD", "SOL-USD")):
    """Trades over `minutes` minutes with a missing minute per pair; prices follow correlated random walks."""
    rng = random.Random(seed)
    price = {"BTC-USD": 77000.0, "ETH-USD": 2260.0, "SOL-USD": 95.0}
    rows, tid = [], 1
    for m in range(minutes):
        market = rng.gauss(0, 0.001)
        for p in pairs:
            if (m == 50 and p == "ETH-USD") or (m == 90 and p == "SOL-USD"):
                continue  # a quiet minute: no trades
            for _ in range(rng.randint(1, 4)):
                price[p] *= 1 + market + rng.gauss(0, 0.0005)
                t = T0 + timedelta(minutes=m, seconds=rng.uniform(0, 59.9))
                rows.append((p, tid, Decimal(f"{price[p]:.6f}"), Decimal(f"{rng.uniform(0.001, 2):.6f}"),
                             rng.choice(["BUY", "SELL"]), naive(t), naive(t + timedelta(milliseconds=80)), False, 0, tid))
                tid += 1
    return rows


# ------------------------------------------------------------------ parsing for the archive

def test_archive_parsing_keeps_lineage_and_drops_bad_records(spark):
    good = {"pair": "BTC-USD", "trade_id": "9", "price": "77000.5", "size": "0.01", "side": "BUY",
            "exchange_time": "2026-11-10T23:59:59.900Z", "ingest_time": "2026-11-11T00:00:00.050Z", "replay": True}
    old_format = {k: v for k, v in good.items() if k != "replay"} | {"trade_id": "10"}
    bad = [{**good, "price": "-1"}, {**good, "pair": "btc"}, {**good, "trade_id": "x"}, {**good, "exchange_time": "soon"}]
    raw = spark.createDataFrame(
        [(json.dumps(v).encode(), 2, 100 + i) for i, v in enumerate([good, old_format, *bad])] + [(b"junk", 0, 1)],
        "value binary, partition int, offset long",
    )
    rows = {r["trade_id"]: r for r in parse_for_archive(raw).collect()}
    assert set(rows) == {9, 10}
    assert rows[9]["dt"] == "2026-11-10"            # by exchange time, though received after midnight
    assert rows[9]["is_replay"] is True and rows[10]["is_replay"] is False
    assert (rows[9]["kafka_partition"], rows[9]["kafka_offset"]) == (2, 100)


# ------------------------------------------------------------------------- de-duplication

def test_dedupe_keeps_the_live_copy_of_each_trade(spark):
    t = naive(T0)
    rows = [
        ("BTC-USD", 1, Decimal("1"), Decimal("1"), "BUY", t, naive(T0 + timedelta(seconds=5)), True, 0, 10),   # replay
        ("BTC-USD", 1, Decimal("1"), Decimal("1"), "BUY", t, naive(T0 + timedelta(seconds=1)), False, 0, 4),   # live
        ("BTC-USD", 1, Decimal("1"), Decimal("1"), "BUY", t, naive(T0 + timedelta(seconds=1)), False, 0, 4),   # written twice
        ("ETH-USD", 1, Decimal("2"), Decimal("1"), "SELL", t, naive(T0), False, 1, 3),                        # same id, other pair
    ]
    out = dedupe_trades(spark.createDataFrame(rows, SCHEMA)).collect()
    assert len(out) == 2
    btc = next(r for r in out if r["pair"] == "BTC-USD")
    assert btc["is_replay"] is False and btc["kafka_offset"] == 4


# ------------------------------------------------------------------------------ analytics

@pytest.fixture(scope="module")
def day(spark):
    rows = random_day()
    trades = spark.createDataFrame(rows, SCHEMA)
    returns = minute_returns(trades)
    return rows, trades, returns


def reference_minutes(rows, pair):
    """{minute index: close} for one pair, close = last trade by (time, id)."""
    last = {}
    for p, tid, price, size, side, t, *_ in sorted(rows, key=lambda r: (r[5], r[1])):
        if p == pair:
            last[int((t - naive(T0)).total_seconds() // 60)] = float(price)
    return last


def reference_returns(rows, pair):
    closes = reference_minutes(rows, pair)
    return {m: math.log(c / closes[m - 1]) for m, c in closes.items() if m - 1 in closes}


def test_daily_summary_matches_reference(day):
    rows, trades, returns = day
    got = {r["pair"]: r for r in daily_summary(trades, returns).collect()}
    for pair in ("BTC-USD", "ETH-USD", "SOL-USD"):
        mine = sorted((r for r in rows if r[0] == pair), key=lambda r: (r[5], r[1]))
        prices = [r[2] for r in mine]
        volume = sum(r[3] for r in mine)
        notional = sum(r[2] * r[3] for r in mine)
        rets = reference_returns(rows, pair).values()
        g = got[pair]
        assert (g["open"], g["close"]) == (prices[0], prices[-1])
        assert (g["high"], g["low"]) == (max(prices), min(prices))
        assert g["trades"] == len(mine) and g["volume"] == volume
        assert g["notional_usd"] == notional.quantize(Decimal("0.01"))
        assert g["vwap"] == (notional / volume).quantize(Decimal("1e-10"))
        assert g["return_pct"] == pytest.approx(float((prices[-1] / prices[0] - 1) * 100), rel=1e-9)
        assert g["realized_vol_pct"] == pytest.approx(math.sqrt(sum(r * r for r in rets)) * 100, rel=1e-9)
        buy = sum(r[3] for r in mine if r[4] == "BUY")
        assert g["buy_volume_pct"] == pytest.approx(float(buy / volume * 100), rel=1e-9)
        assert g["largest_trade_usd"] == max(r[2] * r[3] for r in mine).quantize(Decimal("0.01"))
        assert g["active_minutes"] == len(reference_minutes(rows, pair))
    assert got["ETH-USD"]["active_minutes"] == 179  # one quiet minute


def test_returns_skip_gaps(day):
    rows, trades, returns = day
    eth = {int((r["minute"] - naive(T0)).total_seconds() // 60): r["log_ret"] for r in returns.where("pair = 'ETH-USD'").collect()}
    assert 50 not in eth and eth[51] is None and eth[52] is not None


def test_hourly_profile(day):
    rows, trades, returns = day
    got = {(r["pair"], r["hour"]): r for r in hourly_profile(trades, returns).collect()}
    assert {h for _, h in got} == {0, 1, 2}
    for pair in ("BTC-USD", "SOL-USD"):
        for hour in range(3):
            mine = [r for r in rows if r[0] == pair and r[5].hour == hour]
            assert got[(pair, hour)]["trades"] == len(mine)
            rets = [v for m, v in reference_returns(rows, pair).items() if m // 60 == hour]
            mean = sum(rets) / len(rets)
            sd = math.sqrt(sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)) * 100
            assert got[(pair, hour)]["volatility_pct"] == pytest.approx(sd, rel=1e-9)


def test_pair_correlation_matches_pearson(day):
    rows, trades, returns = day
    got = {(r["pair_a"], r["pair_b"]): r for r in pair_correlation(returns).collect()}
    assert set(got) == {("BTC-USD", "ETH-USD"), ("BTC-USD", "SOL-USD"), ("ETH-USD", "SOL-USD")}
    for a, b in got:
        ra, rb = reference_returns(rows, a), reference_returns(rows, b)
        common = sorted(set(ra) & set(rb))
        xs, ys = [ra[m] for m in common], [rb[m] for m in common]
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        r = cov / math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
        assert got[(a, b)]["minutes"] == len(common)
        assert got[(a, b)]["correlation"] == pytest.approx(r, rel=1e-9)
        assert 0.5 < got[(a, b)]["correlation"] < 1  # the walks share a market factor


# ----------------------------------------------------------------------------- compaction

def test_compaction_one_file_per_pair_no_rows_lost_late_file_kept(spark, tmp_path, monkeypatch):
    root = f"file://{tmp_path}"
    lake_settings = LakeSettings(hdfs_root=root)
    monkeypatch.setattr(batch_job, "settings", lake_settings)
    rows = random_day(minutes=30)
    df = spark.createDataFrame(rows, SCHEMA).withColumn("dt", F.lit(DAY.isoformat()))
    # Like the archive: many small appends, plus a duplicate batch (a re-run after a crash)
    chunks = [df.where(f"trade_id % 6 = {k}") for k in range(6)] + [df.where("trade_id % 6 = 0")]
    for c in chunks:
        c.repartition(1).write.mode("append").partitionBy("dt", "pair").parquet(lake_settings.raw_trades_path)
    day_path = f"{lake_settings.raw_trades_path}/dt={DAY.isoformat()}"
    before = len(batch_job.list_parquet_files(spark, day_path))
    assert before == 7 * 3

    started_ms = int(time.time() * 1000)
    trades = dedupe_trades(batch_job.read_day(spark, DAY)).persist()
    expected = trades.count()
    assert expected == len(rows)

    # A late trade lands in the old folder after the run started
    time.sleep(0.05)
    late = [("BTC-USD", 999_999, Decimal("1"), Decimal("1"), "BUY", naive(T0 + timedelta(hours=23)), naive(T0), True, 0, 1)]
    spark.createDataFrame(late, SCHEMA).withColumn("dt", F.lit(DAY.isoformat())) \
        .repartition(1).write.mode("append").partitionBy("dt", "pair").parquet(lake_settings.raw_trades_path)

    batch_job.compact_day(spark, DAY, trades, started_ms)
    trades.unpersist()

    files = batch_job.list_parquet_files(spark, day_path)
    per_pair = {}
    for f in files:
        per_pair.setdefault(f.getPath().getParent().getName(), []).append(f)
    assert {k: len(v) for k, v in per_pair.items()} == {"pair=BTC-USD": 2, "pair=ETH-USD": 1, "pair=SOL-USD": 1}
    after = dedupe_trades(batch_job.read_day(spark, DAY))
    assert after.count() == expected + 1                      # nothing lost, late trade kept
    assert after.where("trade_id = 999999").count() == 1
    assert not batch_job.list_parquet_files(spark, lake_settings.compaction_path)  # staging cleaned up


# ------------------------------------------------------------------------------ scheduler

def test_next_run_is_today_or_tomorrow():
    at = lambda h, m: datetime(2026, 11, 11, h, m, tzinfo=timezone.utc)  # noqa: E731
    assert next_run_after(at(0, 10), "00:30") == at(0, 30)
    assert next_run_after(at(0, 30), "00:30") == at(0, 30) + timedelta(days=1)
    assert next_run_after(at(16, 0), "00:30") == at(0, 30) + timedelta(days=1)


def test_catch_up_only_finished_days_in_the_window():
    today = date(2026, 11, 11)
    archived = [date(2026, 11, d) for d in (1, 5, 8, 9, 10, 11)]
    done = {date(2026, 11, 9)}
    assert days_to_catch_up(archived, done, today, 7) == [date(2026, 11, 5), date(2026, 11, 8), date(2026, 11, 10)]
