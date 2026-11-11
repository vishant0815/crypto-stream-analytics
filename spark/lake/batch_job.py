"""Nightly batch job for one UTC day of archived trades.

1. Read the day from the HDFS archive and de-duplicate it.
2. Compact it: the archive writes a small file per pair every few minutes;
   they are rewritten as one file per pair.
3. Compute the daily summary, hourly profile and pair correlations with Spark SQL.
4. Write them to Hive (INSERT OVERWRITE of the day's partition) and to TimescaleDB
   (delete and insert the day, in one transaction) for the dashboard.
5. Reconcile: count the day's trades in HDFS and in TimescaleDB, pair by pair.
6. Log the run in batch_runs.

Every step can be repeated: running the same day twice gives the same result.

Run locally:  python -m lake.batch_job --date 2026-11-10     (from the spark/ folder)
              python -m lake.batch_job --date today --allow-partial   (today so far, no compaction)
"""

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lake.analytics import daily_summary, dedupe_trades, hourly_profile, minute_returns, pair_correlation
from lake.config import settings
from lake.session import create_spark, hadoop_fs
from lake.tables import ensure_tables

log = logging.getLogger("batch-job")

ARCHIVE_COLUMNS = [
    "trade_id", "price", "size", "side", "exchange_time", "ingest_time", "is_replay", "kafka_partition", "kafka_offset",
]


# --------------------------------------------------------------------------- HDFS helpers

def list_parquet_files(spark: SparkSession, path: str) -> list:
    fs, Path = hadoop_fs(spark, path)
    p = Path(path)
    if not fs.exists(p):
        return []
    files, it = [], fs.listFiles(p, True)
    while it.hasNext():
        status = it.next()
        if status.getPath().getName().endswith(".parquet"):
            files.append(status)
    return files


def archived_days(spark: SparkSession) -> list[date]:
    fs, Path = hadoop_fs(spark, settings.raw_trades_path)
    root = Path(settings.raw_trades_path)
    if not fs.exists(root):
        return []
    days = []
    for status in fs.listStatus(root):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith("dt="):
            try:
                days.append(date.fromisoformat(name[3:]))
            except ValueError:
                pass
    return sorted(days)


def read_day(spark: SparkSession, dt: date) -> DataFrame:
    path = f"{settings.raw_trades_path}/dt={dt.isoformat()}"
    spark.catalog.refreshByPath(path)  # the file listing may have changed since the last read
    return spark.read.option("basePath", settings.raw_trades_path).parquet(path).select("pair", *ARCHIVE_COLUMNS)


def compact_day(spark: SparkSession, dt: date, trades: DataFrame, started_ms: int) -> None:
    """Rewrite a day as one Parquet file per pair, swapping the folders in place.

    The new files are written to a staging folder first. Then, per pair, the old
    folder is moved aside and the new one moved in: two renames, each atomic on
    HDFS. If the archive job wrote a late file into the old folder after this run
    started (Coinbase can replay a trade from just before midnight), that file is
    moved into the new folder instead of being deleted.
    """
    day = f"{settings.raw_trades_path}/dt={dt.isoformat()}"
    staging = f"{settings.compaction_path}/dt={dt.isoformat()}"
    trash = f"{settings.compaction_path}/_replaced/dt={dt.isoformat()}-{started_ms}"
    fs, Path = hadoop_fs(spark, day)

    trades.repartition("pair").write.mode("overwrite").partitionBy("pair").parquet(staging)

    for status in fs.listStatus(Path(staging)):
        name = status.getPath().getName()
        if not (status.isDirectory() and name.startswith("pair=")):
            continue
        target, old = Path(f"{day}/{name}"), Path(f"{trash}/{name}")
        fs.mkdirs(Path(trash))
        if fs.exists(target) and not fs.rename(target, old):
            raise RuntimeError(f"could not move {target} aside")
        if not fs.rename(status.getPath(), target):
            raise RuntimeError(f"could not move compacted {name} into place")
        if fs.exists(old):
            for f in fs.listStatus(old):
                if f.getPath().getName().endswith(".parquet") and f.getModificationTime() > started_ms:
                    fs.rename(f.getPath(), Path(f"{day}/{name}/{f.getPath().getName()}"))
                    log.info("kept late file %s", f.getPath().getName())
    fs.delete(Path(trash), True)
    fs.delete(Path(staging), True)


# ----------------------------------------------------------------------- database helpers

def _rows(df: DataFrame) -> list[dict]:
    return [r.asDict() for r in df.collect()]


def export_to_database(dt: date, daily: list[dict], hourly: list[dict], corr: list[dict]) -> None:
    """Replace the day's rows in TimescaleDB, in one transaction."""
    with psycopg.connect(settings.database_url, application_name="spark-batch") as conn, conn.cursor() as cur:
        symbols = {r["pair"] for r in daily} | {r["pair"] for r in hourly}
        symbols |= {r["pair_a"] for r in corr} | {r["pair_b"] for r in corr}
        for s in sorted(symbols):
            base, quote = s.split("-")
            cur.execute("INSERT INTO pairs (symbol, base, quote) VALUES (%s, %s, %s) ON CONFLICT (symbol) DO NOTHING", (s, base, quote))
        cur.execute("SELECT symbol, pair_id FROM pairs")
        ids = dict(cur.fetchall())

        for table in ("analytics_daily", "analytics_hourly", "analytics_correlation"):
            cur.execute(f"DELETE FROM {table} WHERE dt = %s", (dt,))
        cur.executemany(
            """INSERT INTO analytics_daily (dt, pair_id, open, high, low, close, return_pct, range_pct, realized_vol_pct,
                   volume, notional_usd, vwap, trades, buy_volume_pct, largest_trade_usd, active_minutes)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            [
                (dt, ids[r["pair"]], r["open"], r["high"], r["low"], r["close"], r["return_pct"], r["range_pct"],
                 r["realized_vol_pct"], r["volume"], r["notional_usd"], r["vwap"], r["trades"], r["buy_volume_pct"],
                 r["largest_trade_usd"], r["active_minutes"])
                for r in daily
            ],
        )
        cur.executemany(
            """INSERT INTO analytics_hourly (dt, hour, pair_id, trades, volume, notional_usd, volatility_pct)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            [(dt, r["hour"], ids[r["pair"]], r["trades"], r["volume"], r["notional_usd"], r["volatility_pct"]) for r in hourly],
        )
        cur.executemany(
            """INSERT INTO analytics_correlation (dt, pair_a, pair_b, correlation, minutes) VALUES (%s, %s, %s, %s, %s)""",
            [
                (dt, *sorted((ids[r["pair_a"]], ids[r["pair_b"]])), r["correlation"], r["minutes"])
                for r in corr
            ],
        )


def database_counts(dt: date) -> dict[str, int]:
    start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            """SELECT p.symbol, count(*) FROM trades t JOIN pairs p USING (pair_id)
               WHERE t.time >= %s AND t.time < %s GROUP BY 1""",
            (start, start + timedelta(days=1)),
        ).fetchall()
    return dict(rows)


def start_run(dt: date) -> int:
    with psycopg.connect(settings.database_url) as conn:
        return conn.execute(
            "INSERT INTO batch_runs (job, dt, started_at, status) VALUES ('daily', %s, now(), 'running') RETURNING run_id",
            (dt,),
        ).fetchone()[0]


def finish_run(run_id: int, status: str, **fields) -> None:
    details = json.dumps(fields.pop("details", {}), default=str)
    with psycopg.connect(settings.database_url) as conn:
        conn.execute(
            """UPDATE batch_runs SET finished_at = now(), status = %s, rows_hdfs = %s, rows_db = %s,
                   files_before = %s, files_after = %s, details = %s WHERE run_id = %s""",
            (status, fields.get("rows_hdfs"), fields.get("rows_db"), fields.get("files_before"),
             fields.get("files_after"), details, run_id),
        )


# ---------------------------------------------------------------------------------- run

def run_day(spark: SparkSession, dt: date, compact: bool = True, allow_partial: bool = False) -> str:
    today = datetime.now(timezone.utc).date()
    if dt >= today and not allow_partial:
        raise ValueError(f"{dt} is not over yet (UTC); use --allow-partial to analyse it so far")
    compact = compact and dt < today  # never compact a day the archive is still writing

    ensure_tables(spark)
    run_id = start_run(dt)
    started = time.time()
    log.info("run %d: %s%s", run_id, dt, " (partial day)" if dt >= today else "")
    try:
        day_path = f"{settings.raw_trades_path}/dt={dt.isoformat()}"
        files_before = len(list_parquet_files(spark, day_path))
        if files_before == 0:
            finish_run(run_id, "failed", details={"error": "no archived trades for this day"})
            return "failed"

        trades = dedupe_trades(read_day(spark, dt)).persist()
        rows_hdfs = trades.count()

        if compact:
            compact_day(spark, dt, trades, int(started * 1000))
            trades.unpersist()
            # From here on, read the compacted files: the same rows, from one file per pair
            trades = dedupe_trades(read_day(spark, dt)).persist()
            if trades.count() != rows_hdfs:
                raise RuntimeError("row count changed during compaction")
        files_after = len(list_parquet_files(spark, day_path))

        returns = minute_returns(trades).persist()
        daily = daily_summary(trades, returns)
        hourly = hourly_profile(trades, returns)
        corr = pair_correlation(returns)

        db = settings.hive_database
        part = F.lit(dt.isoformat()).alias("dt")
        daily.select("*", part).write.mode("overwrite").insertInto(f"{db}.daily_summary")
        hourly.select("*", part).write.mode("overwrite").insertInto(f"{db}.hourly_profile")
        corr.select("*", part).write.mode("overwrite").insertInto(f"{db}.pair_correlation")

        # Read back from Hive: what the dashboard gets is exactly what Hive holds
        where = f"dt = '{dt.isoformat()}'"
        daily_rows = _rows(spark.table(f"{db}.daily_summary").where(where).drop("dt"))
        hourly_rows = _rows(spark.table(f"{db}.hourly_profile").where(where).drop("dt"))
        corr_rows = _rows(spark.table(f"{db}.pair_correlation").where(where).drop("dt"))
        export_to_database(dt, daily_rows, hourly_rows, corr_rows)

        hdfs_by_pair = {r["pair"]: r["count"] for r in trades.groupBy("pair").count().collect()}
        db_by_pair = database_counts(dt)
        rows_db = sum(db_by_pair.values())
        status = "ok" if hdfs_by_pair == db_by_pair else "mismatch"
        finish_run(
            run_id, status, rows_hdfs=rows_hdfs, rows_db=rows_db, files_before=files_before, files_after=files_after,
            details={
                "partial_day": dt >= today,
                "compacted": compact,
                "seconds": round(time.time() - started, 1),
                "hdfs_by_pair": hdfs_by_pair,
                "db_by_pair": db_by_pair,
                "pairs": len(daily_rows),
                "hourly_rows": len(hourly_rows),
                "correlations": len(corr_rows),
            },
        )
        log.info("run %d: %s, %d trades in HDFS, %d in TimescaleDB, files %d -> %d, %.1f s",
                 run_id, status, rows_hdfs, rows_db, files_before, files_after, time.time() - started)
        returns.unpersist()
        trades.unpersist()
        return status
    except Exception as exc:
        finish_run(run_id, "failed", details={"error": f"{type(exc).__name__}: {exc}"[:2000]})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Nightly batch analytics for one UTC day")
    parser.add_argument("--date", default="yesterday", help="YYYY-MM-DD, 'yesterday' (default) or 'today'")
    parser.add_argument("--allow-partial", action="store_true", help="allow a day that is not over yet")
    parser.add_argument("--no-compact", action="store_true", help="skip compaction")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    dt = {"today": today, "yesterday": today - timedelta(days=1)}.get(args.date) or date.fromisoformat(args.date)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = create_spark("crypto-batch")
    try:
        status = run_day(spark, dt, compact=not args.no_compact, allow_partial=args.allow_partial)
    finally:
        spark.stop()
    raise SystemExit(0 if status in ("ok", "mismatch") else 1)


if __name__ == "__main__":
    main()
