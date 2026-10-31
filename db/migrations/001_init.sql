-- =====================================================================
-- Crypto Stream Analytics: hot store schema (PostgreSQL + TimescaleDB)
-- Runs once, on the first start of an empty database.
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- Reference data
-- ---------------------------------------------------------------------
CREATE TABLE pairs (
    pair_id    SMALLSERIAL PRIMARY KEY,
    symbol     TEXT        NOT NULL UNIQUE CHECK (symbol ~ '^[A-Z0-9]{2,10}-[A-Z]{2,5}$'),
    base       TEXT        NOT NULL,
    quote      TEXT        NOT NULL,
    is_active  BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO pairs (symbol, base, quote) VALUES
    ('BTC-USD',  'BTC',  'USD'),
    ('ETH-USD',  'ETH',  'USD'),
    ('SOL-USD',  'SOL',  'USD'),
    ('XRP-USD',  'XRP',  'USD'),
    ('DOGE-USD', 'DOGE', 'USD');

-- ---------------------------------------------------------------------
-- Raw trades: one row per exchange trade
-- ---------------------------------------------------------------------
CREATE TABLE trades (
    time        TIMESTAMPTZ    NOT NULL,              -- exchange time (event time)
    pair_id     SMALLINT       NOT NULL REFERENCES pairs,
    trade_id    BIGINT         NOT NULL,
    price       NUMERIC(20,10) NOT NULL CHECK (price > 0),
    size        NUMERIC(28,12) NOT NULL CHECK (size > 0),
    side        TEXT           NOT NULL CHECK (side IN ('BUY', 'SELL')),
    ingest_time TIMESTAMPTZ    NOT NULL,              -- when our pipeline received it
    -- The time column must be part of any unique key on a hypertable.
    -- (pair_id, trade_id) makes a resent trade a duplicate, not a new row.
    PRIMARY KEY (pair_id, trade_id, time)
);

SELECT create_hypertable('trades', by_range('time', INTERVAL '1 day'));

-- Dashboard queries are "one pair, recent time range"
CREATE INDEX trades_pair_time_idx ON trades (pair_id, time DESC);

-- Columnar compression for chunks older than 7 days, grouped per pair.
-- trade_id is in the order-by so the primary key stays fast on compressed data.
ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pair_id',
    timescaledb.compress_orderby   = 'time DESC, trade_id'
);
SELECT add_compression_policy('trades', INTERVAL '7 days');
SELECT add_retention_policy('trades', INTERVAL '30 days');   -- full history lives in HDFS

-- ---------------------------------------------------------------------
-- Ticker snapshots: last price, 24h stats, best bid/ask
-- ---------------------------------------------------------------------
CREATE TABLE ticker (
    time              TIMESTAMPTZ    NOT NULL,
    pair_id           SMALLINT       NOT NULL REFERENCES pairs,
    price             NUMERIC(20,10) NOT NULL,
    volume_24h        NUMERIC(28,8),
    high_24h          NUMERIC(20,10),
    low_24h           NUMERIC(20,10),
    price_pct_chg_24h NUMERIC(12,6),
    best_bid          NUMERIC(20,10),
    best_ask          NUMERIC(20,10),
    ingest_time       TIMESTAMPTZ    NOT NULL,
    PRIMARY KEY (pair_id, time)
);

SELECT create_hypertable('ticker', by_range('time', INTERVAL '1 day'));

ALTER TABLE ticker SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pair_id',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('ticker', INTERVAL '2 days');
SELECT add_retention_policy('ticker', INTERVAL '7 days');

-- Latest ticker per pair, for the dashboard header
CREATE VIEW latest_ticker AS
SELECT DISTINCT ON (t.pair_id)
       p.symbol, t.time, t.price, t.price_pct_chg_24h, t.volume_24h,
       t.high_24h, t.low_24h, t.best_bid, t.best_ask,
       t.best_ask - t.best_bid AS spread
FROM ticker t
JOIN pairs p USING (pair_id)
WHERE t.time > now() - INTERVAL '1 hour'
ORDER BY t.pair_id, t.time DESC;

-- ---------------------------------------------------------------------
-- 1-minute candles: written by Spark Structured Streaming (Phase 3)
-- ---------------------------------------------------------------------
CREATE TABLE candles_1m (
    bucket      TIMESTAMPTZ NOT NULL,
    pair_id     SMALLINT    NOT NULL REFERENCES pairs,
    open        NUMERIC(20,10) NOT NULL,
    high        NUMERIC(20,10) NOT NULL,
    low         NUMERIC(20,10) NOT NULL,
    close       NUMERIC(20,10) NOT NULL,
    volume      NUMERIC(28,12) NOT NULL,
    vwap        NUMERIC(20,10),
    trade_count INTEGER     NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, bucket),
    CHECK (low <= high)
);

SELECT create_hypertable('candles_1m', by_range('bucket', INTERVAL '7 days'));

-- 1-hour candles rolled up from 1-minute candles, kept up to date automatically.
-- materialized_only = false also shows the current, unfinished hour.
CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '1 hour', bucket) AS bucket,
       pair_id,
       first(open, bucket) AS open,
       max(high)           AS high,
       min(low)            AS low,
       last(close, bucket) AS close,
       sum(volume)         AS volume,
       sum(vwap * volume) / NULLIF(sum(volume), 0) AS vwap,
       sum(trade_count)    AS trade_count
FROM candles_1m
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy('candles_1h',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes');

-- ---------------------------------------------------------------------
-- Anomaly alerts: written by the Spark ML scoring job (Phase 6)
-- ---------------------------------------------------------------------
CREATE TABLE alerts (
    id         BIGINT GENERATED ALWAYS AS IDENTITY,
    time       TIMESTAMPTZ      NOT NULL,
    pair_id    SMALLINT         NOT NULL REFERENCES pairs,
    alert_type TEXT             NOT NULL CHECK (alert_type IN ('VOLUME_SPIKE', 'PRICE_JUMP', 'OTHER')),
    score      DOUBLE PRECISION NOT NULL,
    details    JSONB            NOT NULL DEFAULT '{}',
    model_ver  TEXT,
    PRIMARY KEY (id, time)
);

SELECT create_hypertable('alerts', by_range('time', INTERVAL '7 days'));
CREATE INDEX alerts_pair_time_idx ON alerts (pair_id, time DESC);

-- ---------------------------------------------------------------------
-- Read-only role for the API (Phase 4): it can query, never write
-- ---------------------------------------------------------------------
CREATE ROLE dashboard_reader NOLOGIN;
GRANT USAGE ON SCHEMA public TO dashboard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dashboard_reader;
