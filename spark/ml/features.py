"""Features for anomaly detection, one row per pair and minute.

The nightly trainer and the live Spark job both call minute_features() on the
same 1-minute candles (built by streaming.transforms.build_candles), so the
model always scores exactly what it was trained on.

Every feature compares a minute with the same coin's previous hour, so they
are scale-free: a $50 move means a lot for SOL and nothing for BTC, but "eight
times the usual move" means the same for both. One model serves every pair.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

HISTORY_MINUTES = 60   # each minute is compared with this many minutes before it
MIN_HISTORY = 30       # minutes of history needed before a minute can be scored
FEATURE_CAP = 50.0     # a ratio of 10,000x is no more anomalous than 50x for clustering

# Order matters: it is the order of the model's feature vector
FEATURES = [
    "abs_ret_z",         # |close/open move| in units of the past hour's typical move
    "range_z",           # high-low range vs the past hour's average range
    "log_volume_ratio",  # log of volume vs the past hour's average volume
    "log_trades_ratio",  # log of trade count vs the past hour's average
    "vwap_dev_z",        # |close vs VWAP| in units of the typical move: a late jump within the minute
    "buy_imbalance",     # 0 = balanced buying and selling, 1 = all one side
]

# Floors stop a perfectly flat hour (typical move of zero) dividing by zero
_MOVE_FLOOR = 1e-5   # 0.001%
_RANGE_FLOOR = 1e-5


def _past_hour() -> Window:
    """This pair's previous HISTORY_MINUTES minutes, not including the current one."""
    return (
        Window.partitionBy("pair")
        .orderBy(F.col("bucket").cast("long"))
        .rangeBetween(-HISTORY_MINUTES * 60, -60)
    )


def _cap(c: Column) -> Column:
    return F.least(c, F.lit(FEATURE_CAP))


def minute_features(candles: DataFrame) -> DataFrame:
    """1-minute candles -> one feature row per pair-minute that has enough history.

    Input columns: pair, bucket, open, high, low, close, volume, vwap, trade_count, buy_volume.
    Output: pair, bucket, close, volume, trade_count, history_minutes and the FEATURES columns.
    Minutes with fewer than MIN_HISTORY minutes of history are dropped.
    """
    d = lambda c: F.col(c).cast("double")  # noqa: E731
    base = candles.select(
        "pair",
        "bucket",
        d("close").alias("close"),
        d("volume").alias("volume"),
        F.col("trade_count").cast("double").alias("trades"),
        F.log(d("close") / d("open")).alias("ret"),
        F.log(d("high") / d("low")).alias("range"),
        F.log(d("close") / d("vwap")).alias("vwap_dev"),
        F.when(d("volume") > 0, d("buy_volume") / d("volume")).alias("buy_share"),
    )

    w = _past_hour()
    hist = base.select(
        "*",
        F.count("ret").over(w).alias("history_minutes"),
        F.stddev_pop("ret").over(w).alias("hist_ret_sd"),
        F.avg("range").over(w).alias("hist_range"),
        F.avg("volume").over(w).alias("hist_volume"),
        F.avg("trades").over(w).alias("hist_trades"),
    )

    move = F.greatest(F.col("hist_ret_sd"), F.lit(_MOVE_FLOOR))
    return hist.where(F.col("history_minutes") >= MIN_HISTORY).select(
        "pair",
        "bucket",
        "close",
        "volume",
        F.col("trades").cast("int").alias("trade_count"),
        F.col("history_minutes").cast("int").alias("history_minutes"),
        _cap(F.abs("ret") / move).alias("abs_ret_z"),
        _cap(F.col("range") / F.greatest(F.col("hist_range"), F.lit(_RANGE_FLOOR))).alias("range_z"),
        _cap(F.log((F.col("volume") + 1e-12) / (F.col("hist_volume") + 1e-12))).alias("log_volume_ratio"),
        _cap(F.log((F.col("trades") + 1) / (F.col("hist_trades") + 1))).alias("log_trades_ratio"),
        _cap(F.abs("vwap_dev") / move).alias("vwap_dev_z"),
        (F.abs(F.coalesce(F.col("buy_share"), F.lit(0.5)) - 0.5) * 2).alias("buy_imbalance"),
    )
