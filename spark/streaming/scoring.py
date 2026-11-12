"""Live anomaly scoring for the streaming job.

Keeps the active model loaded in the driver and checks the model registry every
MODEL_REFRESH_S seconds, so a model trained overnight is picked up without a
restart. With no active model, or if loading one fails (HDFS down, say), it
scores with the rule baseline instead, so alerts never stop.
"""

import logging
import time

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame

from ml.model import RULES_VERSION, score_with_model, score_with_rules
from ml.registry import ActiveModel, active_model
from streaming.config import settings

log = logging.getLogger("scoring")


class LiveScorer:
    def __init__(self, refresh_s: float = settings.model_refresh_s) -> None:
        self.refresh_s = refresh_s
        self.active: ActiveModel | None = None
        self.model: PipelineModel | None = None
        self._checked_at = 0.0

    @property
    def version(self) -> str:
        return self.active.version if self.model is not None else RULES_VERSION

    def refresh(self, force: bool = False) -> None:
        if not force and time.monotonic() - self._checked_at < self.refresh_s:
            return
        self._checked_at = time.monotonic()
        try:
            latest = active_model(settings.database_url)
        except Exception as exc:
            log.warning("could not read the model registry (%s); keeping %s", exc, self.version)
            return
        if latest is None:
            if self.model is not None:
                log.info("no active model any more; scoring with %s", RULES_VERSION)
            self.active, self.model = None, None
            return
        if self.active is not None and latest.version == self.active.version and self.model is not None:
            return
        try:
            model = PipelineModel.load(latest.path)
        except Exception as exc:
            log.warning("could not load model %s from %s (%s); keeping %s", latest.version, latest.path, exc, self.version)
            return
        self.active, self.model = latest, model
        log.info("scoring with model %s (threshold %.2f)", latest.version, latest.threshold)

    def score(self, features: DataFrame) -> DataFrame:
        self.refresh()
        if self.model is None:
            return score_with_rules(features)
        return score_with_model(
            self.model, self.active.threshold, self.active.version, features, self.active.normal_clusters
        )
