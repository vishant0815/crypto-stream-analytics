"""Anomaly detection: a rule baseline and a K-Means model (Spark MLlib).

K-Means groups ordinary minutes into a few typical patterns (quiet, busy,
trending...). A minute far from every pattern is unusual. Its anomaly score is
the distance to the nearest NORMAL cluster centre in standardised feature space,
and the alert threshold is the 99.5th percentile of the training scores: about
1 minute in 200 stands out.

Why "normal" clusters: if several similar anomalies are in the training data,
K-Means can give them a small cluster of their own, and each would then sit
right next to its own centre and look ordinary. So only clusters that hold at
least MIN_CLUSTER_SHARE of the training minutes count as patterns; anything
else is measured against the nearest real pattern.

Only minutes that are unusually HIGH on their main driver are alerted: a minute
far from the centres because it was unusually quiet is not interesting.
Each alert names the feature that drove it, which sets its type.
"""

from dataclasses import dataclass, field

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ml.features import FEATURES

THRESHOLD_QUANTILE = 0.995
MIN_CLUSTER_SHARE = 0.01
K_CANDIDATES = (3, 4, 5, 6, 7, 8)
SEED = 42

# Which alert type each driving feature produces
ALERT_TYPE = {
    "abs_ret_z": "PRICE_JUMP",
    "range_z": "PRICE_JUMP",
    "vwap_dev_z": "PRICE_JUMP",
    "log_volume_ratio": "VOLUME_SPIKE",
    "log_trades_ratio": "VOLUME_SPIKE",
    "buy_imbalance": "OTHER",
}

# Rule baseline, used until there is enough history to train a model
RULES_VERSION = "rules-v1"
RULES = {
    "abs_ret_z": 8.0,                 # a move 8x the past hour's typical move
    "range_z": 8.0,                   # a range 8x the past hour's average
    "log_volume_ratio": 2.302585,     # ln(10): 10x the past hour's average volume
}

ALERT_COLUMNS = ["pair", "bucket", "alert_type", "score", "driver", "model_ver", *FEATURES]


@dataclass
class TrainedModel:
    model: PipelineModel
    threshold: float
    k: int
    silhouette: float
    alert_rate: float
    rows: int
    normal_clusters: list[int]
    cluster_sizes: dict[int, int] = field(default_factory=dict)
    silhouettes: dict[int, float] = field(default_factory=dict)


# ----------------------------------------------------------------------------- rules

def score_with_rules(features: DataFrame) -> DataFrame:
    """Alerts from fixed thresholds. Score = how many times over its limit the worst feature is."""
    ratios = [F.col(f) / F.lit(limit) for f, limit in RULES.items()]
    names = F.array(*[F.lit(f) for f in RULES])
    vals = F.array(*ratios)
    top = F.array_max(vals)
    driver = F.element_at(names, F.array_position(vals, top).cast("int"))
    return (
        features.select("*", top.alias("score"), driver.alias("driver"))
        .where(F.col("score") >= 1.0)
        .select(
            *[c for c in ALERT_COLUMNS if c not in ("alert_type", "model_ver")],
            _alert_type(F.col("driver")).alias("alert_type"),
            F.lit(RULES_VERSION).alias("model_ver"),
        )
        .select(*ALERT_COLUMNS)
    )


def _alert_type(driver: Column) -> Column:
    expr = F.lit("OTHER")
    for feature, kind in ALERT_TYPE.items():
        expr = F.when(driver == feature, F.lit(kind)).otherwise(expr)
    return expr


# ----------------------------------------------------------------------------- model

def _pipeline(k: int) -> Pipeline:
    return Pipeline(
        stages=[
            VectorAssembler(inputCols=FEATURES, outputCol="raw_features", handleInvalid="skip"),
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
            KMeans(k=k, seed=SEED, featuresCol="features", predictionCol="cluster", maxIter=50),
        ]
    )


def train(features: DataFrame, k_candidates=K_CANDIDATES) -> TrainedModel:
    """Fit K-Means for each candidate k, keep the best by silhouette, set the threshold."""
    data = features.select(*FEATURES).na.drop().persist()
    rows = data.count()
    if rows < 50:
        data.unpersist()
        raise ValueError(f"only {rows} pair-minutes to train on")

    evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster", metricName="silhouette")
    best: tuple[float, int, PipelineModel] | None = None
    silhouettes: dict[int, float] = {}
    for k in k_candidates:
        model = _pipeline(k).fit(data)
        s = evaluator.evaluate(model.transform(data))
        silhouettes[k] = s
        if best is None or s > best[0]:
            best = (s, k, model)

    silhouette, k, model = best
    sizes = {r["cluster"]: r["count"] for r in model.transform(data).groupBy("cluster").count().collect()}
    normal = sorted(c for c, n in sizes.items() if n >= MIN_CLUSTER_SHARE * rows)
    scores = with_scores(model, data, normal)
    threshold = scores.approxQuantile("score", [THRESHOLD_QUANTILE], 0.0001)[0]
    alert_rate = scores.where(F.col("score") > threshold).count() / rows
    data.unpersist()
    return TrainedModel(model, threshold, k, silhouette, alert_rate, rows, normal, sizes, silhouettes)


def with_scores(model: PipelineModel, features: DataFrame, normal_clusters: list[int] | None = None) -> DataFrame:
    """Adds score (distance to the nearest normal centre), driver (the feature furthest
    from that centre) and driver_high (whether it is above the centre, not below).
    normal_clusters: which clusters count as patterns; all of them if not given."""
    all_centers = model.stages[-1].clusterCenters()
    keep = range(len(all_centers)) if normal_clusters is None else normal_clusters
    centers = F.array(*[F.array(*[F.lit(float(v)) for v in all_centers[i]]) for i in keep])
    names = F.array(*[F.lit(f) for f in FEATURES])

    x = vector_to_array("features")
    dist = lambda c: F.sqrt(F.aggregate(F.zip_with(x, c, lambda a, b: (a - b) * (a - b)), F.lit(0.0), lambda acc, v: acc + v))  # noqa: E731
    dists = F.transform(centers, dist)
    nearest = F.element_at(centers, F.array_position(dists, F.array_min(dists)).cast("int"))
    diffs = F.zip_with(x, nearest, lambda a, b: a - b)
    abs_diffs = F.transform(diffs, lambda v: F.abs(v))
    idx = F.array_position(abs_diffs, F.array_max(abs_diffs)).cast("int")

    out = model.transform(features).withColumn("_diffs", diffs)
    return out.select(
        *features.columns,
        "cluster",
        F.array_min(dists).alias("score"),
        F.element_at(names, idx).alias("driver"),
        (F.element_at("_diffs", idx) > 0).alias("driver_high"),
    )


def score_with_model(
    model: PipelineModel, threshold: float, version: str, features: DataFrame, normal_clusters: list[int] | None = None
) -> DataFrame:
    """Alerts: minutes above the threshold whose main driver is unusually high."""
    return (
        with_scores(model, features, normal_clusters)
        .where((F.col("score") > F.lit(threshold)) & F.col("driver_high"))
        .select(
            *[c for c in ALERT_COLUMNS if c not in ("alert_type", "model_ver")],
            _alert_type(F.col("driver")).alias("alert_type"),
            F.lit(version).alias("model_ver"),
        )
        .select(*ALERT_COLUMNS)
    )
