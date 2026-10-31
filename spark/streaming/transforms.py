"""DataFrame logic for candles and indicators.

Every function here takes and returns DataFrames and does no I/O, so the same
code runs on a live stream and in unit tests on small static DataFrames.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

PRICE = DecimalType(20, 10)
SIZE = DecimalType(28, 12)


def precise_divide(total: Column, volume: Column) -> Column:
    """total / volume with 10 decimal places, cast to a price.

    Spark sizes a decimal division from the operand types; with two wide
    decimals the result is cut to 6 decimal places, which visibly distorts the
    VWAP of a coin priced around $0.07. Narrowing both sides to realistic sizes
    first (notional < 10^16 USD, volume < 10^12 coins per window) lets the
    result keep 10 decimal places.
    """
    return (total.cast(DecimalType(30, 14)) / volume.cast(DecimalType(24, 12))).cast(PRICE)

# Shape of the JSON the ingestion service writes to raw.trades.
# Numbers arrive as strings so no precision is lost on the way.
TRADE_JSON_SCHEMA = StructType(
    [
        StructField("pair", StringType()),
        StructField("trade_id", StringType()),
        StructField("price", StringType()),
        StructField("size", StringType()),
        StructField("side", StringType()),
        StructField("exchange_time", StringType()),
        StructField("ingest_time", StringType()),
    ]
)


def parse_trades(raw: DataFrame) -> DataFrame:
    """Kafka records (binary value) -> typed trades. Unparseable rows are dropped.

    Spark 4 runs in ANSI mode, where a plain cast of a bad value ("x" to a
    number) throws and would stop the whole stream. try_cast and
    try_to_timestamp return null instead, and the filter below drops the row.
    """
    parsed = raw.select(F.from_json(F.col("value").cast("string"), TRADE_JSON_SCHEMA).alias("t")).select("t.*")
    return (
        parsed.select(
            F.col("pair"),
            F.col("trade_id").try_cast("long").alias("trade_id"),
            F.col("price").try_cast(PRICE).alias("price"),
            F.col("size").try_cast(SIZE).alias("size"),
            F.col("side"),
            F.try_to_timestamp("exchange_time").alias("exchange_time"),
        )
        .where(
            F.col("pair").isNotNull()
            & F.col("trade_id").isNotNull()
            & (F.col("price") > 0)
            & (F.col("size") > 0)
            & F.col("side").isin("BUY", "SELL")
            & F.col("exchange_time").isNotNull()
        )
    )


def _trade_order() -> Column:
    """Sort key for 'first'/'last' trade: exchange time, then trade id to break ties."""
    return F.struct(F.col("exchange_time"), F.col("trade_id"))


def build_candles(trades: DataFrame, window: str = "1 minute") -> DataFrame:
    """Trades -> OHLCV candles per pair and time window."""
    notional = F.col("price") * F.col("size")
    candles = trades.groupBy(F.window("exchange_time", window).alias("w"), F.col("pair")).agg(
        F.min_by("price", _trade_order()).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", _trade_order()).alias("close"),
        F.sum("size").alias("volume"),
        F.sum(F.when(F.col("side") == "BUY", F.col("size")).otherwise(F.lit(0))).alias("buy_volume"),
        F.sum(notional).alias("notional"),
        F.count(F.lit(1)).alias("trade_count"),
        F.min("exchange_time").alias("first_trade_at"),
        F.max("exchange_time").alias("last_trade_at"),
    )
    return candles.select(
        F.col("w.start").alias("bucket"),
        F.col("pair"),
        "open",
        "high",
        "low",
        "close",
        F.col("volume").cast(SIZE).alias("volume"),
        F.col("buy_volume").cast(SIZE).alias("buy_volume"),
        precise_divide(F.col("notional"), F.col("volume")).alias("vwap"),
        F.col("trade_count").cast("int").alias("trade_count"),
        "first_trade_at",
        "last_trade_at",
    )


def _minutes_back(n: int) -> Window:
    """Window over this pair's candles from n-1 minutes ago up to now, by time not row count."""
    return Window.partitionBy("pair").orderBy(F.col("bucket").cast("long")).rangeBetween(-(n - 1) * 60, 0)


def _sma(n: int) -> Column:
    """Average close over n minutes; null until the window holds n candles."""
    w = _minutes_back(n)
    return F.when(F.count("close").over(w) == n, F.avg("close").over(w)).cast(PRICE)


def compute_indicators(candles: DataFrame) -> DataFrame:
    """1-minute candle history -> technical indicators for every candle.

    Needs at least 60 minutes of history before the candles you care about,
    otherwise the longer indicators are left null (never computed from too
    little data).
    """
    by_time = Window.partitionBy("pair").orderBy("bucket")
    w15 = _minutes_back(15)
    w14 = _minutes_back(14)
    w60 = _minutes_back(60)

    prev_close = F.lag("close").over(by_time)
    prev_bucket = F.lag("bucket").over(by_time)
    # A return only counts if the previous candle is exactly one minute earlier
    contiguous = (F.col("bucket").cast("long") - prev_bucket.cast("long")) == 60

    with_returns = candles.select(
        "*",
        F.when(contiguous, (F.col("close") / prev_close - 1) * 100).cast("double").alias("return_pct"),
        F.when(contiguous, F.log(F.col("close").cast("double") / prev_close.cast("double"))).alias("log_ret"),
    ).select(
        "*",
        F.greatest(F.col("return_pct"), F.lit(0.0)).alias("gain"),
        F.greatest(-F.col("return_pct"), F.lit(0.0)).alias("loss"),
    )

    avg_gain = F.avg("gain").over(w14)
    avg_loss = F.avg("loss").over(w14)
    rsi = (
        F.when(F.count("return_pct").over(w14) < 14, F.lit(None))
        .when((avg_gain == 0) & (avg_loss == 0), F.lit(50.0))  # flat price: neutral
        .when(avg_loss == 0, F.lit(100.0))
        .otherwise(100 - 100 / (1 + avg_gain / avg_loss))
    )

    return with_returns.select(
        "bucket",
        "pair",
        "close",
        "return_pct",
        _sma(5).alias("sma_5"),
        _sma(15).alias("sma_15"),
        _sma(60).alias("sma_60"),
        F.when(
            F.count("close").over(w15) == 15,
            precise_divide(F.sum(F.col("vwap") * F.col("volume")).over(w15), F.sum("volume").over(w15)),
        ).alias("vwap_15"),
        F.when(F.count("log_ret").over(w15) >= 14, F.stddev_samp("log_ret").over(w15) * 100).alias("volatility_15"),
        rsi.cast("double").alias("rsi_14"),
        F.when(
            F.count("volume").over(w15) == 15,
            F.col("volume") / F.avg("volume").over(w15),
        )
        .cast("double")
        .alias("volume_ratio_15"),
        F.count("close").over(w60).cast("short").alias("candles_in_60"),
    )
