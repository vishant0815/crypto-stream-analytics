-- =====================================================================
-- Analysis queries on the hot store.
-- Run with: psql "$DATABASE_URL" -f db/queries/analysis.sql
-- =====================================================================

-- 1. One-minute OHLCV candles for BTC built straight from raw trades
SELECT time_bucket(INTERVAL '1 minute', t.time) AS minute,
       first(t.price, t.time)                   AS open,
       max(t.price)                             AS high,
       min(t.price)                             AS low,
       last(t.price, t.time)                    AS close,
       round(sum(t.size), 6)                    AS volume,
       count(*)                                 AS trades
FROM trades t
JOIN pairs p USING (pair_id)
WHERE p.symbol = 'BTC-USD'
  AND t.time > now() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute DESC
LIMIT 15;

-- 2. VWAP, notional volume and buy pressure per pair, last hour
SELECT p.symbol,
       round(sum(t.price * t.size) / sum(t.size), 6)                           AS vwap,
       round(sum(t.price * t.size), 2)                                         AS notional_usd,
       round(100.0 * sum(t.size) FILTER (WHERE t.side = 'BUY') / sum(t.size), 1) AS buy_volume_pct,
       count(*)                                                                AS trades
FROM trades t
JOIN pairs p USING (pair_id)
WHERE t.time > now() - INTERVAL '1 hour'
GROUP BY p.symbol
ORDER BY notional_usd DESC;

-- 3. Top movers: price change % over the last hour, ranked
WITH bounds AS (
    SELECT pair_id,
           first(price, time) AS first_price,
           last(price, time)  AS last_price
    FROM trades
    WHERE time > now() - INTERVAL '1 hour'
    GROUP BY pair_id
)
SELECT p.symbol,
       b.first_price,
       b.last_price,
       round(100 * (b.last_price - b.first_price) / b.first_price, 3) AS change_pct,
       rank() OVER (ORDER BY abs(b.last_price - b.first_price) / b.first_price DESC) AS move_rank
FROM bounds b
JOIN pairs p USING (pair_id)
ORDER BY move_rank;

-- 4. The 3 largest trades per pair by USD value, last 24 hours
SELECT symbol, time, side, price, size, notional_usd
FROM (
    SELECT p.symbol, t.time, t.side, t.price, t.size,
           round(t.price * t.size, 2) AS notional_usd,
           row_number() OVER (PARTITION BY t.pair_id ORDER BY t.price * t.size DESC) AS rn
    FROM trades t
    JOIN pairs p USING (pair_id)
    WHERE t.time > now() - INTERVAL '24 hours'
) ranked
WHERE rn <= 3
ORDER BY symbol, notional_usd DESC;

-- 5. Pipeline latency: exchange trade -> our ingestion, p50 / p95 / p99 in ms.
--    On every (re)connect Coinbase first sends a batch of recent, older trades,
--    which inflates p95/p99 over short windows; over hours it evens out.
SELECT p.symbol,
       count(*) AS trades,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms)::numeric, 0) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)::numeric, 0) AS p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)::numeric, 0) AS p99_ms
FROM (
    SELECT pair_id, extract(epoch FROM ingest_time - time) * 1000 AS ms
    FROM trades
    WHERE time > now() - INTERVAL '1 hour'
) l
JOIN pairs p USING (pair_id)
GROUP BY p.symbol
ORDER BY p.symbol;

-- 6. Quiet minutes: minutes in the last hour where a pair had no trades at all
SELECT p.symbol, count(*) AS empty_minutes
FROM pairs p
CROSS JOIN generate_series(
    date_trunc('minute', now() - INTERVAL '1 hour'),
    date_trunc('minute', now()) - INTERVAL '1 minute',
    INTERVAL '1 minute'
) AS m(minute)
LEFT JOIN trades t
       ON t.pair_id = p.pair_id
      AND t.time >= m.minute
      AND t.time <  m.minute + INTERVAL '1 minute'
WHERE p.is_active
  AND t.pair_id IS NULL
GROUP BY p.symbol
ORDER BY empty_minutes DESC;

-- 7. Spread from the latest ticker, in basis points
SELECT symbol, price, best_bid, best_ask,
       round(10000 * spread / price, 3) AS spread_bps,
       time
FROM latest_ticker
ORDER BY spread_bps DESC;

-- 8. Storage: size of each hypertable and how much compression saves
SELECT h.hypertable_name,
       pg_size_pretty(hypertable_size(format('%I', h.hypertable_name)::regclass)) AS total_size,
       (SELECT count(*) FROM timescaledb_information.chunks c
         WHERE c.hypertable_name = h.hypertable_name)                        AS chunks,
       pg_size_pretty(s.before_compression_total_bytes)                       AS before_compression,
       pg_size_pretty(s.after_compression_total_bytes)                        AS after_compression
FROM timescaledb_information.hypertables h
LEFT JOIN LATERAL hypertable_compression_stats(format('%I', h.hypertable_name)::regclass) s ON true
ORDER BY h.hypertable_name;
