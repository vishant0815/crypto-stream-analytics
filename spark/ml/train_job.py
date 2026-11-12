"""Train the anomaly model on the HDFS archive and register it.

1. Read the last TRAIN_DAYS days of archived trades from HDFS and de-duplicate them.
2. Rebuild 1-minute candles with the same code as the live job (build_candles).
3. Compute features with the same code the live job scores with (minute_features).
4. Fit K-Means for several k, keep the best by silhouette, set the alert threshold.
5. Save the model to HDFS under /models/anomaly/<version> and record it in model_registry.
   It becomes the active model if it passes a sanity check; the live job picks it
   up within ten minutes, without a restart.

With too little history (under MIN_TRAINING_ROWS pair-minutes), nothing is trained
and the live job keeps using the rule baseline.

Run locally:  python -m ml.train_job [--days 30]      (from the spark/ folder)
Runs nightly after the batch job (see lake/scheduler.py).
"""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lake.analytics import dedupe_trades
from lake.batch_job import archived_days, read_day
from lake.config import settings
from lake.session import create_spark
from ml.features import FEATURES, minute_features
from ml.model import THRESHOLD_QUANTILE, train
from ml.registry import register
from streaming.transforms import build_candles

log = logging.getLogger("train-job")

TRAIN_DAYS = int(os.getenv("TRAIN_DAYS", "30"))
MIN_TRAINING_ROWS = int(os.getenv("MIN_TRAINING_ROWS", "2000"))
# A model that would flag more than this share of ordinary minutes is not activated
MAX_ALERT_RATE = 0.02


def train_and_register(spark: SparkSession, days: int = TRAIN_DAYS, min_rows: int = MIN_TRAINING_ROWS) -> str | None:
    """Returns the new model's version, or None if there was not enough data."""
    today = datetime.now(timezone.utc).date()
    window = [d for d in archived_days(spark) if today - timedelta(days=days) <= d <= today]
    if not window:
        log.info("no archived days in the last %d days; keeping the current model or rules", days)
        return None

    trades = None
    for d in window:
        day = read_day(spark, d)
        trades = day if trades is None else trades.unionByName(day)
    trades = dedupe_trades(trades)

    candles = build_candles(trades.select("pair", "trade_id", "price", "size", "side", "exchange_time"))
    features = minute_features(candles).persist()
    rows = features.count()
    if rows < min_rows:
        log.info("only %d pair-minutes of history (need %d); keeping the current model or rules", rows, min_rows)
        features.unpersist()
        return None

    span = features.agg(F.min("bucket").alias("lo"), F.max("bucket").alias("hi")).first()
    per_pair = {r["pair"]: r["count"] for r in features.groupBy("pair").count().collect()}
    trained = train(features)
    features.unpersist()

    version = "kmeans-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    path = f"{settings.hdfs_root}/models/anomaly/{version}"
    trained.model.write().overwrite().save(path)

    activate = 0 < trained.alert_rate <= MAX_ALERT_RATE and len(trained.normal_clusters) >= 2
    register(
        settings.database_url,
        version=version,
        path=path,
        data_from=span["lo"].replace(tzinfo=timezone.utc),
        data_to=span["hi"].replace(tzinfo=timezone.utc),
        training_rows=trained.rows,
        k=trained.k,
        silhouette=trained.silhouette,
        threshold=trained.threshold,
        alert_rate=trained.alert_rate,
        features=FEATURES,
        details={
            "normal_clusters": trained.normal_clusters,
            "cluster_sizes": {str(c): n for c, n in sorted(trained.cluster_sizes.items())},
            "silhouette_by_k": {str(k): round(s, 4) for k, s in trained.silhouettes.items()},
            "threshold_quantile": THRESHOLD_QUANTILE,
            "days": [d.isoformat() for d in window],
            "rows_by_pair": per_pair,
        },
        activate=activate,
    )
    log.info(
        "model %s: %d pair-minutes, k=%d, silhouette %.3f, threshold %.2f, alert rate %.3f%%, %s",
        version, trained.rows, trained.k, trained.silhouette, trained.threshold, 100 * trained.alert_rate,
        "active" if activate else "NOT activated (failed sanity check)",
    )
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and register the anomaly model")
    parser.add_argument("--days", type=int, default=TRAIN_DAYS, help="days of history to train on")
    parser.add_argument("--min-rows", type=int, default=MIN_TRAINING_ROWS, help="minimum pair-minutes needed")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = create_spark("crypto-train")
    try:
        train_and_register(spark, args.days, args.min_rows)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
