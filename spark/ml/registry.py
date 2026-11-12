"""The model registry table: which models exist and which one is live."""

import json
from dataclasses import dataclass
from datetime import datetime

import psycopg


@dataclass(frozen=True)
class ActiveModel:
    version: str
    path: str
    threshold: float
    normal_clusters: list[int]


def active_model(database_url: str) -> ActiveModel | None:
    with psycopg.connect(database_url) as conn:
        row = conn.execute(
            "SELECT version, path, threshold, details FROM model_registry WHERE is_active"
        ).fetchone()
    if row is None:
        return None
    version, path, threshold, details = row
    return ActiveModel(version, path, threshold, list(details.get("normal_clusters", [])))


def register(
    database_url: str,
    *,
    version: str,
    path: str,
    data_from: datetime,
    data_to: datetime,
    training_rows: int,
    k: int,
    silhouette: float,
    threshold: float,
    alert_rate: float,
    features: list[str],
    details: dict,
    activate: bool,
) -> None:
    """Record a model; if activate, it replaces the active one in the same transaction."""
    with psycopg.connect(database_url) as conn, conn.transaction():
        if activate:
            conn.execute("UPDATE model_registry SET is_active = false WHERE is_active")
        conn.execute(
            """INSERT INTO model_registry (version, path, data_from, data_to, training_rows, k, silhouette,
                   threshold, alert_rate, features, details, is_active)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (version, path, data_from, data_to, training_rows, k, silhouette, threshold, alert_rate,
             json.dumps(features), json.dumps(details, default=str), activate),
        )
