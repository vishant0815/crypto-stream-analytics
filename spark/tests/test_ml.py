"""Tests for anomaly features and models.

The model test builds three synthetic markets with different volatility, injects
known anomalies (price jumps and volume spikes) into about 0.3% of minutes, trains
on everything (unlabelled, as in production) and measures how many injected
anomalies are caught, how many ordinary minutes are flagged, and whether each
alert gets the right type.
"""

import math
import random
from datetime import datetime, timedelta

import pytest
from pyspark.ml import PipelineModel

from ml.features import FEATURES, HISTORY_MINUTES, MIN_HISTORY, minute_features
from ml.model import RULES_VERSION, score_with_model, score_with_rules, train, with_scores

T0 = datetime(2026, 11, 1)
CANDLE_SCHEMA = (
    "pair string, bucket timestamp, open double, high double, low double, close double, "
    "volume double, vwap double, trade_count int, buy_volume double"
)
MARKETS = {"BTC-USD": (77000.0, 0.0004, 3.0), "SOL-USD": (95.0, 0.0009, 400.0), "DOGE-USD": (0.068, 0.0015, 90000.0)}


def synthetic_market(minutes=2000, seed=11, inject=True):
    """Returns (rows, injected) where injected maps (pair, minute) -> 'PRICE_JUMP' | 'VOLUME_SPIKE'."""
    rng = random.Random(seed)
    rows, injected = [], {}
    for p_i, (pair, (price, sigma, vol)) in enumerate(MARKETS.items()):
        events = {}
        if inject:
            for j in range(3):
                events[150 + 600 * j + 37 * p_i] = "PRICE_JUMP"
                events[450 + 600 * j + 41 * p_i] = "VOLUME_SPIKE"
        for m in range(minutes):
            kind = events.get(m)
            ret = rng.gauss(0, sigma)
            volume = vol * math.exp(rng.gauss(0, 0.45))
            trades = max(1, int(rng.gauss(20, 4)))
            buy_share = rng.uniform(0.3, 0.7)
            if kind == "PRICE_JUMP":
                ret = 14 * sigma * rng.choice([-1, 1])
            elif kind == "VOLUME_SPIKE":
                volume *= 30
                trades *= 12
            if kind:
                injected[(pair, m)] = kind
            open_ = price
            close = price * math.exp(ret)
            wiggle = abs(rng.gauss(0, sigma / 3))
            high, low = max(open_, close) * math.exp(wiggle), min(open_, close) * math.exp(-wiggle)
            vwap = (open_ + close) / 2
            rows.append((pair, T0 + timedelta(minutes=m), open_, high, low, close, volume, vwap, trades, volume * buy_share))
            price = close
    return rows, injected


def features_of(spark, rows):
    return minute_features(spark.createDataFrame(rows, CANDLE_SCHEMA))


def minute_index(ts):
    return int((ts - T0).total_seconds() // 60)


# ------------------------------------------------------------------------------ features

def test_features_match_reference(spark):
    rows, _ = synthetic_market(minutes=120, inject=False)
    got = {(r["pair"], minute_index(r["bucket"])): r for r in features_of(spark, rows).collect()}
    btc = [r for r in rows if r[0] == "BTC-USD"]
    m = 100
    past = btc[m - HISTORY_MINUTES:m]
    rets = [math.log(r[5] / r[2]) for r in past]
    mean = sum(rets) / len(rets)
    sd = math.sqrt(sum((x - mean) ** 2 for x in rets) / len(rets))
    cur = btc[m]
    g = got[("BTC-USD", m)]
    assert g["history_minutes"] == HISTORY_MINUTES
    assert g["abs_ret_z"] == pytest.approx(abs(math.log(cur[5] / cur[2])) / sd, rel=1e-9)
    avg_vol = sum(r[6] for r in past) / len(past)
    assert g["log_volume_ratio"] == pytest.approx(math.log(cur[6] / avg_vol), rel=1e-6)
    avg_range = sum(math.log(r[3] / r[4]) for r in past) / len(past)
    assert g["range_z"] == pytest.approx(math.log(cur[3] / cur[4]) / avg_range, rel=1e-9)
    assert g["buy_imbalance"] == pytest.approx(abs(cur[9] / cur[6] - 0.5) * 2, rel=1e-9)


def test_minutes_without_enough_history_are_not_scored(spark):
    rows, _ = synthetic_market(minutes=60, inject=False)
    first = min(minute_index(r["bucket"]) for r in features_of(spark, rows).collect())
    assert first == MIN_HISTORY


def test_features_are_scale_free(spark):
    """The same market at 1000x the price and volume gives the same features."""
    rows, _ = synthetic_market(minutes=90, inject=False)
    scaled = [(p, t, o * 1000, h * 1000, l * 1000, c * 1000, v * 1000, w * 1000, n, b * 1000) for p, t, o, h, l, c, v, w, n, b in rows]
    a = {(r["pair"], r["bucket"]): r for r in features_of(spark, rows).collect()}
    b = {(r["pair"], r["bucket"]): r for r in features_of(spark, scaled).collect()}
    for key in a:
        for f in FEATURES:
            assert a[key][f] == pytest.approx(b[key][f], rel=1e-6, abs=1e-9), f


# --------------------------------------------------------------------------------- rules

def test_rules_flag_obvious_spikes_only(spark):
    rows, injected = synthetic_market(minutes=700)
    alerts = {(r["pair"], minute_index(r["bucket"])): r for r in score_with_rules(features_of(spark, rows)).collect()}
    caught = [k for k in injected if k in alerts]
    assert len(caught) == len([k for k in injected if k[1] >= MIN_HISTORY])
    assert all(alerts[k]["model_ver"] == RULES_VERSION for k in caught)
    assert all(alerts[k]["alert_type"] == injected[k] for k in caught)
    false_alarms = [k for k in alerts if k not in injected]
    assert len(false_alarms) <= 2


# --------------------------------------------------------------------------------- model

@pytest.fixture(scope="module")
def trained(spark):
    rows, injected = synthetic_market(minutes=2000)
    feats = features_of(spark, rows).persist()
    model = train(feats, k_candidates=(3, 4, 5, 6))
    return feats, injected, model


def test_model_catches_injected_anomalies_with_few_false_alarms(trained):
    feats, injected, tm = trained
    alerts = {(r["pair"], minute_index(r["bucket"])): r for r in score_with_model(tm.model, tm.threshold, "test", feats, tm.normal_clusters).collect()}
    scored = feats.count()
    normal = scored - len(injected)

    caught = [k for k in injected if k in alerts]
    false_alarms = [k for k in alerts if k not in injected]
    recall = len(caught) / len(injected)
    false_positive_rate = len(false_alarms) / normal
    type_accuracy = sum(alerts[k]["alert_type"] == injected[k] for k in caught) / len(caught)

    print(f"\nk={tm.k} sizes={tm.cluster_sizes} normal={tm.normal_clusters} silhouette={tm.silhouette:.3f} threshold={tm.threshold:.2f} "
          f"recall={recall:.2f} false_positive_rate={false_positive_rate:.4f} type_accuracy={type_accuracy:.2f}")
    assert recall >= 0.9
    assert false_positive_rate <= 0.005
    assert type_accuracy >= 0.9
    assert 0 < tm.alert_rate <= 0.006


def test_unusually_quiet_minutes_are_not_alerts(spark, trained):
    feats, _, tm = trained
    quiet = feats.limit(20).selectExpr(
        "pair", "bucket", "close", "volume", "trade_count", "history_minutes",
        "0.0 AS abs_ret_z", "0.05 AS range_z", "-6.0 AS log_volume_ratio", "-3.0 AS log_trades_ratio",
        "0.0 AS vwap_dev_z", "0.0 AS buy_imbalance",
    )
    scored = with_scores(tm.model, quiet, tm.normal_clusters).collect()
    assert all(r["score"] > tm.threshold for r in scored)  # far from every cluster...
    assert score_with_model(tm.model, tm.threshold, "test", quiet, tm.normal_clusters).count() == 0  # ...but not an alert


def test_anomalies_with_a_cluster_of_their_own_are_still_caught(trained):
    """Without the normal-cluster rule, the injected price jumps form their own small cluster and score low."""
    feats, injected, tm = trained
    tiny = [c for c, n in tm.cluster_sizes.items() if c not in tm.normal_clusters]
    jumps = {k for k, v in injected.items() if v == "PRICE_JUMP"}
    in_tiny = [r for r in with_scores(tm.model, feats, tm.normal_clusters).collect()
               if (r["pair"], minute_index(r["bucket"])) in jumps and r["cluster"] in tiny]
    assert all(r["score"] > tm.threshold for r in in_tiny)


def test_saved_model_scores_the_same(spark, trained, tmp_path):
    feats, _, tm = trained
    path = f"file://{tmp_path}/model"
    tm.model.write().overwrite().save(path)
    loaded = PipelineModel.load(path)
    before = {(r["pair"], r["bucket"]): r["score"] for r in with_scores(tm.model, feats).collect()}
    after = {(r["pair"], r["bucket"]): r["score"] for r in with_scores(loaded, feats).collect()}
    assert before.keys() == after.keys()
    assert all(after[k] == pytest.approx(before[k], rel=1e-12) for k in before)
