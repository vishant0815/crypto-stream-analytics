"""Runs the batch job every night, and catches up on days it missed.

On start, any archived day in the last BATCH_CATCH_UP_DAYS days without a
finished run ('ok' or 'mismatch') is processed, oldest first. Then it sleeps
until BATCH_TIME_UTC (default 00:30) and runs the day that just ended.
One SparkSession is kept for the life of the process.

Run locally:  python -m lake.scheduler      (from the spark/ folder)
"""

import logging
import signal
import threading
from datetime import date, datetime, timedelta, timezone

import psycopg

from lake.batch_job import archived_days, run_day
from lake.config import settings
from lake.session import create_spark

log = logging.getLogger("batch-scheduler")


def next_run_after(now: datetime, at: str) -> datetime:
    hour, minute = (int(x) for x in at.split(":"))
    run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return run if run > now else run + timedelta(days=1)


def days_done() -> set[date]:
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute("SELECT DISTINCT dt FROM batch_runs WHERE job = 'daily' AND status IN ('ok', 'mismatch')").fetchall()
    return {r[0] for r in rows}


def days_to_catch_up(archived: list[date], done: set[date], today: date, window: int) -> list[date]:
    first = today - timedelta(days=window)
    return [d for d in archived if first <= d < today and d not in done]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    spark = create_spark("crypto-batch-scheduler")
    try:
        today = datetime.now(timezone.utc).date()
        for dt in days_to_catch_up(archived_days(spark), days_done(), today, settings.batch_catch_up_days):
            if stop.is_set():
                break
            log.info("catching up %s", dt)
            try:
                run_day(spark, dt)
            except Exception:
                log.exception("run for %s failed; it will be retried on the next start", dt)

        while not stop.is_set():
            now = datetime.now(timezone.utc)
            run_at = next_run_after(now, settings.batch_time_utc)
            log.info("next run at %s", run_at.isoformat())
            if stop.wait((run_at - now).total_seconds()):
                break
            dt = run_at.date() - timedelta(days=1)
            try:
                run_day(spark, dt)
            except Exception:
                log.exception("run for %s failed; it will be retried on the next start", dt)
    finally:
        spark.stop()
        log.info("stopped")


if __name__ == "__main__":
    main()
