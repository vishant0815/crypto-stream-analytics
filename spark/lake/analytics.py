"""Daily batch analytics as pure DataFrame functions (no I/O), so they are unit tested.

Input everywhere: one day of archived trades with columns
pair, trade_id, price, size, side, exchange_time, ingest_time, is_replay.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from streaming.transforms import PRICE, SIZE, precise_divide

USD = DecimalType(24, 2)


def dedupe_trades(raw: DataFrame) -> DataFrame:
    """One row per (pair, trade_id).

    The archive keeps every copy it received: Coinbase replays recent trades
    after each reconnect, and a crashed micro-batch can be written twice.
    The copy kept is the live one (not a replay), earliest received.
    """
    first_copy = Window.partitionBy("pair", "trade_id").orderBy(
        F.col("is_replay").asc_nulls_last(), F.col("ingest_time").asc_nulls_last(), F.col("kafka_offset").asc_nulls_last()
    )
    return raw.withColumn("_n", F.row_number().over(first_copy)).where("_n = 1").drop("_n")


def _in_order() -> Column:
    return F.struct("exchange_time", "trade_id")


def minute_returns(trades: DataFrame) -> DataFrame:
    """Close of every minute with trades, and the log return from the previous minute.

    A return is only computed between consecutive minutes; after a minute with
    no trades it is null, so a gap never shows up as a one-minute move.
    """
    closes = trades.groupBy("pair", F.date_trunc("minute", "exchange_time").alias("minute")).agg(
        F.max_by("price", _in_order()).alias("close")
    )
    by_time = Window.partitionBy("pair").orderBy("minute")
    prev_close = F.lag("close").over(by_time)
    prev_minute = F.lag("minute").over(by_time)
    contiguous = (F.col("minute").cast("long") - prev_minute.cast("long")) == 60
    return closes.select(
        "pair",
        "minute",
        "close",
        F.when(contiguous, F.log(F.col("close").cast("double") / prev_close.cast("double"))).alias("log_ret"),
    )


def daily_summary(trades: DataFrame, returns: DataFrame) -> DataFrame:
    notional = F.col("price") * F.col("size")
    day = trades.groupBy("pair").agg(
        F.min_by("price", _in_order()).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", _in_order()).alias("close"),
        F.sum("size").alias("volume"),
        F.sum(notional).alias("notional"),
        F.count(F.lit(1)).alias("trades"),
        F.sum(F.when(F.col("side") == "BUY", F.col("size")).otherwise(F.lit(0))).alias("buy_volume"),
        F.max(notional).alias("largest_trade"),
        F.countDistinct(F.date_trunc("minute", "exchange_time")).alias("active_minutes"),
    )
    vol = returns.groupBy("pair").agg(
        # Realized volatility for the day: square root of the summed squared 1-minute log returns
        (F.sqrt(F.sum(F.col("log_ret") * F.col("log_ret"))) * 100).alias("realized_vol_pct"),
        F.count("log_ret").alias("_n_returns"),
    )
    return (
        day.join(vol, "pair", "left")
        .select(
            "pair",
            F.col("open").cast(PRICE).alias("open"),
            F.col("high").cast(PRICE).alias("high"),
            F.col("low").cast(PRICE).alias("low"),
            F.col("close").cast(PRICE).alias("close"),
            ((F.col("close") / F.col("open") - 1) * 100).cast("double").alias("return_pct"),
            ((F.col("high") - F.col("low")) / F.col("open") * 100).cast("double").alias("range_pct"),
            F.when(F.col("_n_returns") > 0, F.col("realized_vol_pct")).alias("realized_vol_pct"),
            F.col("volume").cast(SIZE).alias("volume"),
            F.col("notional").cast(USD).alias("notional_usd"),
            precise_divide(F.col("notional"), F.col("volume")).alias("vwap"),
            F.col("trades").cast("long").alias("trades"),
            # As doubles: dividing two wide decimals in Spark keeps only 6 decimal places
            (F.col("buy_volume").cast("double") / F.col("volume").cast("double") * 100).alias("buy_volume_pct"),
            F.col("largest_trade").cast(USD).alias("largest_trade_usd"),
            F.col("active_minutes").cast("int").alias("active_minutes"),
        )
    )


def hourly_profile(trades: DataFrame, returns: DataFrame) -> DataFrame:
    """Activity per pair and UTC hour: when in the day does each coin trade, and how jumpy is it."""
    activity = trades.groupBy("pair", F.hour("exchange_time").alias("hour")).agg(
        F.count(F.lit(1)).cast("long").alias("trades"),
        F.sum("size").cast(SIZE).alias("volume"),
        F.sum(F.col("price") * F.col("size")).cast(USD).alias("notional_usd"),
    )
    vol = returns.groupBy("pair", F.hour("minute").alias("hour")).agg(
        (F.stddev_samp("log_ret") * 100).alias("volatility_pct")
    )
    return activity.join(vol, ["pair", "hour"], "left").select(
        "pair", F.col("hour").cast("int"), "trades", "volume", "notional_usd", "volatility_pct"
    )


def pair_correlation(returns: DataFrame) -> DataFrame:
    """Pearson correlation of 1-minute log returns for every pair of coins.

    Only minutes where both coins traded (and so both have a return) count.
    Each pair of coins appears once, with pair_a < pair_b alphabetically.
    """
    r = returns.where(F.col("log_ret").isNotNull())
    a = r.select(F.col("pair").alias("pair_a"), "minute", F.col("log_ret").alias("ret_a"))
    b = r.select(F.col("pair").alias("pair_b"), "minute", F.col("log_ret").alias("ret_b"))
    return (
        a.join(b, "minute")
        .where(F.col("pair_a") < F.col("pair_b"))
        .groupBy("pair_a", "pair_b")
        .agg(F.corr("ret_a", "ret_b").alias("correlation"), F.count(F.lit(1)).cast("long").alias("minutes"))
    )
