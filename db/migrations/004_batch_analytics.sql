-- =====================================================================
-- Phase 5: Hive metastore schema and nightly batch analytics results
-- New databases run this automatically after 003.
-- Existing databases: psql "$DATABASE_URL" -f db/migrations/004_batch_analytics.sql
-- =====================================================================

-- The Hive metastore (table and partition definitions for the HDFS data)
-- lives in its own schema. Spark creates its tables on first start.
CREATE SCHEMA IF NOT EXISTS hive_metastore;

-- ---------------------------------------------------------------------
-- Results of the nightly Spark batch job, one day at a time.
-- The full results also live in Hive; these copies feed the dashboard.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics_daily (
    dt               DATE        NOT NULL,
    pair_id          SMALLINT    NOT NULL REFERENCES pairs,
    open             NUMERIC(20,10) NOT NULL,
    high             NUMERIC(20,10) NOT NULL,
    low              NUMERIC(20,10) NOT NULL,
    close            NUMERIC(20,10) NOT NULL,
    return_pct       DOUBLE PRECISION NOT NULL,  -- close vs open, %
    range_pct        DOUBLE PRECISION NOT NULL,  -- (high - low) / open, %
    realized_vol_pct DOUBLE PRECISION,           -- sqrt(sum of squared 1-min log returns), %
    volume           NUMERIC(28,12) NOT NULL,
    notional_usd     NUMERIC(24,2)  NOT NULL,
    vwap             NUMERIC(20,10) NOT NULL,
    trades           INTEGER     NOT NULL,
    buy_volume_pct   DOUBLE PRECISION NOT NULL,
    largest_trade_usd NUMERIC(24,2) NOT NULL,
    active_minutes   SMALLINT    NOT NULL,       -- minutes with at least one trade (of 1440)
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, dt)
);

CREATE TABLE IF NOT EXISTS analytics_hourly (
    dt             DATE        NOT NULL,
    hour           SMALLINT    NOT NULL CHECK (hour BETWEEN 0 AND 23),  -- UTC
    pair_id        SMALLINT    NOT NULL REFERENCES pairs,
    trades         INTEGER     NOT NULL,
    volume         NUMERIC(28,12) NOT NULL,
    notional_usd   NUMERIC(24,2)  NOT NULL,
    volatility_pct DOUBLE PRECISION,             -- std dev of 1-min log returns in the hour, %
    PRIMARY KEY (pair_id, dt, hour)
);

CREATE TABLE IF NOT EXISTS analytics_correlation (
    dt          DATE        NOT NULL,
    pair_a      SMALLINT    NOT NULL REFERENCES pairs,
    pair_b      SMALLINT    NOT NULL REFERENCES pairs,
    correlation DOUBLE PRECISION,                -- Pearson, of 1-minute log returns
    minutes     INTEGER     NOT NULL,            -- minutes where both pairs traded
    PRIMARY KEY (dt, pair_a, pair_b),
    CHECK (pair_a < pair_b)
);

-- One row per batch run: what it did and whether HDFS and TimescaleDB agree
CREATE TABLE IF NOT EXISTS batch_runs (
    run_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job         TEXT        NOT NULL,
    dt          DATE        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status      TEXT        NOT NULL CHECK (status IN ('running', 'ok', 'mismatch', 'failed')),
    rows_hdfs   BIGINT,     -- distinct trades for the day in the HDFS archive
    rows_db     BIGINT,     -- trades for the day in TimescaleDB
    files_before INTEGER,   -- Parquet files for the day before compaction
    files_after  INTEGER,
    details     JSONB       NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS batch_runs_dt_idx ON batch_runs (dt DESC, started_at DESC);

GRANT SELECT ON analytics_daily, analytics_hourly, analytics_correlation, batch_runs TO dashboard_reader;
