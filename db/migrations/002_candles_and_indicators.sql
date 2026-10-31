-- =====================================================================
-- Phase 3: Spark streaming outputs
-- New databases run this automatically after 001_init.sql.
-- Existing databases: psql "$DATABASE_URL" -f db/migrations/002_candles_and_indicators.sql
-- =====================================================================

-- Extra fields Spark computes for every 1-minute candle
ALTER TABLE candles_1m
    ADD COLUMN IF NOT EXISTS buy_volume    NUMERIC(28,12),
    ADD COLUMN IF NOT EXISTS first_trade_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_trade_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS candles_1m_bucket_idx ON candles_1m (bucket DESC);

-- 5-minute candles rolled up from 1-minute candles, like candles_1h
CREATE MATERIALIZED VIEW IF NOT EXISTS candles_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket(INTERVAL '5 minutes', bucket) AS bucket,
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

SELECT add_continuous_aggregate_policy('candles_5m',
    start_offset      => INTERVAL '30 minutes',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => true);

-- ---------------------------------------------------------------------
-- Technical indicators per closed 1-minute candle (written by Spark)
-- Moving windows are time-based, so a minute with no trades does not
-- stretch a "15-minute" average over 16 minutes.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candle_indicators (
    bucket          TIMESTAMPTZ NOT NULL,
    pair_id         SMALLINT    NOT NULL REFERENCES pairs,
    close           NUMERIC(20,10) NOT NULL,
    return_pct      DOUBLE PRECISION,   -- change vs. previous candle's close, %
    sma_5           NUMERIC(20,10),     -- simple moving average of close, 5 min
    sma_15          NUMERIC(20,10),
    sma_60          NUMERIC(20,10),
    vwap_15         NUMERIC(20,10),     -- volume-weighted average price, 15 min
    volatility_15   DOUBLE PRECISION,   -- std dev of 1-min log returns over 15 min, %
    rsi_14          DOUBLE PRECISION,   -- relative strength index, 14 min (0-100)
    volume_ratio_15 DOUBLE PRECISION,   -- this minute's volume / 15-min average (incl. this one)
    candles_in_60   SMALLINT    NOT NULL, -- how many candles the 60-min window held
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair_id, bucket)
);

SELECT create_hypertable('candle_indicators', by_range('bucket', INTERVAL '7 days'),
                         if_not_exists => true);

-- Latest closed candle + indicators per pair, for the dashboard header
CREATE OR REPLACE VIEW latest_indicators AS
SELECT DISTINCT ON (i.pair_id)
       p.symbol, i.*
FROM candle_indicators i
JOIN pairs p USING (pair_id)
WHERE i.bucket > now() - INTERVAL '1 hour'
ORDER BY i.pair_id, i.bucket DESC;

GRANT SELECT ON candles_5m, candle_indicators, latest_indicators TO dashboard_reader;
