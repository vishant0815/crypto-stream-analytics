-- =====================================================================
-- Phase 6: anomaly detection
-- New databases run this automatically after 005.
-- Existing databases: psql "$DATABASE_URL" -f db/migrations/006_ml_anomaly.sql
-- =====================================================================

-- Every trained model: where it is stored on HDFS, what it was trained on,
-- and how well it separated the data. The live job scores with the active one.
CREATE TABLE IF NOT EXISTS model_registry (
    version       TEXT        PRIMARY KEY,          -- e.g. kmeans-20261112-003512
    path          TEXT        NOT NULL,             -- HDFS folder of the saved Spark PipelineModel
    trained_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    data_from     TIMESTAMPTZ NOT NULL,             -- first and last minute of the training data
    data_to       TIMESTAMPTZ NOT NULL,
    training_rows INTEGER     NOT NULL,             -- pair-minutes used for training
    k             SMALLINT    NOT NULL,             -- number of clusters chosen
    silhouette    DOUBLE PRECISION,                 -- cluster separation, -1 to 1
    threshold     DOUBLE PRECISION NOT NULL,        -- anomaly score above which a minute is flagged
    alert_rate    DOUBLE PRECISION NOT NULL,        -- share of training minutes above the threshold
    features      JSONB       NOT NULL,             -- feature names, in model order
    details       JSONB       NOT NULL DEFAULT '{}',
    is_active     BOOLEAN     NOT NULL DEFAULT false
);
-- At most one active model
CREATE UNIQUE INDEX IF NOT EXISTS model_registry_one_active ON model_registry (is_active) WHERE is_active;

-- One alert per pair and minute: a re-scored minute updates its alert instead of adding another.
-- (The time column must be part of any unique index on a hypertable.)
CREATE UNIQUE INDEX IF NOT EXISTS alerts_pair_time_key ON alerts (pair_id, time);

GRANT SELECT ON model_registry TO dashboard_reader;
