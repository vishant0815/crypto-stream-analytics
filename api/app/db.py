"""Read-only queries against TimescaleDB.

Every connection runs with default_transaction_read_only = on, so the API
cannot change data even if a query were wrong. Numbers are cast to float8
and times to epoch seconds in SQL, which is what the charts in the browser use.
"""

from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import settings

# Interval name -> table. Only these names reach SQL; anything else is rejected.
CANDLE_TABLES = {"1m": "candles_1m", "5m": "candles_5m", "1h": "candles_1h"}


class Database:
    def __init__(self, url: str = settings.database_url) -> None:
        self.pool = AsyncConnectionPool(
            url,
            min_size=1,
            max_size=10,
            open=False,
            kwargs={
                "row_factory": dict_row,
                "autocommit": True,
                "options": "-c default_transaction_read_only=on -c statement_timeout=5000",
                "application_name": "api",
            },
        )

    async def open(self) -> None:
        await self.pool.open(wait=True, timeout=30)

    async def close(self) -> None:
        await self.pool.close()

    async def fetch(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, params or {})
            return await cur.fetchall()

    async def ping(self) -> bool:
        rows = await self.fetch("SELECT 1 AS ok")
        return rows[0]["ok"] == 1

    # ------------------------------------------------------------------ pairs

    async def pairs(self) -> list[dict[str, Any]]:
        return await self.fetch(
            "SELECT symbol, base, quote FROM pairs WHERE is_active ORDER BY pair_id"
        )

    # ---------------------------------------------------------------- candles

    async def candles(self, pair: str, interval: str, limit: int) -> list[dict[str, Any]]:
        table = CANDLE_TABLES[interval]  # KeyError for unknown intervals, checked by the route
        return await self.fetch(
            f"""
            SELECT * FROM (
                SELECT extract(epoch FROM c.bucket)::bigint AS time,
                       c.open::float8, c.high::float8, c.low::float8, c.close::float8,
                       c.volume::float8, c.vwap::float8, c.trade_count::int AS trades
                FROM {table} c
                JOIN pairs p USING (pair_id)
                WHERE p.symbol = %(pair)s
                ORDER BY c.bucket DESC
                LIMIT %(limit)s
            ) newest
            ORDER BY time
            """,
            {"pair": pair, "limit": limit},
        )

    async def indicators(self, pair: str, since_epoch: int) -> list[dict[str, Any]]:
        return await self.fetch(
            """
            SELECT extract(epoch FROM i.bucket)::bigint AS time,
                   i.sma_5::float8, i.sma_15::float8, i.sma_60::float8, i.vwap_15::float8,
                   i.rsi_14, i.volatility_15, i.volume_ratio_15, i.return_pct
            FROM candle_indicators i
            JOIN pairs p USING (pair_id)
            WHERE p.symbol = %(pair)s AND i.bucket >= to_timestamp(%(since)s)
            ORDER BY i.bucket
            """,
            {"pair": pair, "since": since_epoch},
        )

    # --------------------------------------------------------------- overview

    async def overview(self) -> list[dict[str, Any]]:
        """One row per pair: latest ticker, 1-hour change, a 60-minute sparkline, latest indicators."""
        return await self.fetch(
            """
            SELECT p.symbol,
                   t.price::float8, t.price_pct_chg_24h::float8 AS change_24h_pct,
                   t.high_24h::float8, t.low_24h::float8, t.volume_24h::float8,
                   t.best_bid::float8, t.best_ask::float8,
                   extract(epoch FROM t.time)::float8 AS ticker_time,
                   h.change_1h_pct, h.volume_1h, h.spark,
                   i.rsi_14, i.volatility_15, i.sma_15::float8, i.volume_ratio_15
            FROM pairs p
            LEFT JOIN latest_ticker t ON t.symbol = p.symbol
            LEFT JOIN LATERAL (
                SELECT (100 * (last(c.close, c.bucket) / first(c.open, c.bucket) - 1))::float8 AS change_1h_pct,
                       sum(c.volume)::float8 AS volume_1h,
                       array_agg(c.close::float8 ORDER BY c.bucket) AS spark
                FROM candles_1m c
                WHERE c.pair_id = p.pair_id AND c.bucket > now() - INTERVAL '60 minutes'
            ) h ON true
            LEFT JOIN latest_indicators i ON i.symbol = p.symbol
            WHERE p.is_active
            ORDER BY p.pair_id
            """
        )

    # ----------------------------------------------------------------- trades

    async def recent_trades(self, pair: str, limit: int) -> list[dict[str, Any]]:
        return await self.fetch(
            """
            SELECT (extract(epoch FROM t.time) * 1000)::bigint AS time_ms,
                   t.trade_id, t.price::float8, t.size::float8, t.side
            FROM trades t
            JOIN pairs p USING (pair_id)
            WHERE p.symbol = %(pair)s AND t.time > now() - INTERVAL '1 day'
            ORDER BY t.time DESC, t.trade_id DESC
            LIMIT %(limit)s
            """,
            {"pair": pair, "limit": limit},
        )

    # --------------------------------------------------------------- pipeline

    async def pipeline_stats(self) -> dict[str, Any]:
        latency = await self.fetch(
            """
            -- Exchange trade -> our ingestion, last 5 minutes. Trades Coinbase
            -- replays right after a (re)connect are flagged and left out.
            SELECT count(*)::int AS trades,
                   percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) AS p50_ms,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95_ms,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99_ms
            FROM (
                SELECT extract(epoch FROM ingest_time - time) * 1000 AS ms
                FROM trades
                WHERE ingest_time > now() - INTERVAL '5 minutes'
                  AND NOT coalesce(is_replay, false)
            ) l
            """
        )
        per_minute = await self.fetch(
            """
            SELECT extract(epoch FROM m)::bigint AS time, count(t.time)::int AS trades
            FROM generate_series(date_trunc('minute', now()) - INTERVAL '29 minutes',
                                 date_trunc('minute', now()), INTERVAL '1 minute') AS m
            LEFT JOIN trades t ON t.time >= m AND t.time < m + INTERVAL '1 minute'
            GROUP BY m ORDER BY m
            """
        )
        freshness = await self.fetch(
            """
            SELECT extract(epoch FROM now() - (SELECT max(ingest_time) FROM trades))::float8      AS trades_age_s,
                   extract(epoch FROM now() - (SELECT max(updated_at) FROM candles_1m))::float8   AS candles_age_s,
                   extract(epoch FROM now() - (SELECT max(updated_at) FROM candle_indicators))::float8 AS indicators_age_s
            """
        )
        storage = await self.fetch(
            """
            SELECT h.hypertable_name AS name,
                   hypertable_size(format('%%I', h.hypertable_name)::regclass) AS bytes,
                   (SELECT count(*) FROM timescaledb_information.chunks c
                     WHERE c.hypertable_name = h.hypertable_name)::int AS chunks
            FROM timescaledb_information.hypertables h
            ORDER BY bytes DESC
            """
        )
        return {
            "latency": latency[0],
            "trades_per_minute": per_minute,
            "freshness": freshness[0],
            "storage": storage,
        }

    # -------------------------------------------------------------- analytics

    async def analytics(self, days: int) -> dict[str, Any]:
        """Results of the nightly Spark batch job (computed from the HDFS archive)."""
        latest = await self.fetch("SELECT max(dt) AS dt FROM analytics_daily")
        latest_dt = latest[0]["dt"]
        if latest_dt is None:
            return {"latest_dt": None, "days": days, "daily": [], "hourly": [], "correlation": [], "runs": await self.batch_runs()}
        params = {"latest": latest_dt, "days": days}
        daily = await self.fetch(
            """
            SELECT a.dt::text AS dt, p.symbol, a.close::float8, a.return_pct, a.range_pct, a.realized_vol_pct,
                   a.notional_usd::float8, a.trades, a.buy_volume_pct, a.largest_trade_usd::float8, a.active_minutes
            FROM analytics_daily a JOIN pairs p USING (pair_id)
            WHERE a.dt > %(latest)s::date - %(days)s
            ORDER BY a.dt, p.pair_id
            """,
            params,
        )
        hourly = await self.fetch(
            """
            -- Share of each pair's traded value that falls in each UTC hour, over the window
            SELECT p.symbol, h.hour,
                   (100 * sum(h.notional_usd) / NULLIF(sum(sum(h.notional_usd)) OVER (PARTITION BY h.pair_id), 0))::float8 AS share_pct,
                   sum(h.trades)::int AS trades,
                   avg(h.volatility_pct) AS volatility_pct
            FROM analytics_hourly h JOIN pairs p USING (pair_id)
            WHERE h.dt > %(latest)s::date - %(days)s
            GROUP BY p.symbol, p.pair_id, h.pair_id, h.hour
            ORDER BY p.pair_id, h.hour
            """,
            params,
        )
        correlation = await self.fetch(
            """
            SELECT pa.symbol AS pair_a, pb.symbol AS pair_b, c.correlation, c.minutes
            FROM analytics_correlation c
            JOIN pairs pa ON pa.pair_id = c.pair_a
            JOIN pairs pb ON pb.pair_id = c.pair_b
            WHERE c.dt = %(latest)s
            ORDER BY pa.pair_id, pb.pair_id
            """,
            params,
        )
        return {
            "latest_dt": latest_dt.isoformat(),
            "days": days,
            "daily": daily,
            "hourly": hourly,
            "correlation": correlation,
            "runs": await self.batch_runs(),
        }

    async def batch_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        return await self.fetch(
            """
            SELECT run_id, dt::text AS dt, status, rows_hdfs, rows_db, files_before, files_after,
                   extract(epoch FROM started_at)::float8 AS started_at,
                   extract(epoch FROM finished_at)::float8 AS finished_at,
                   (details->>'seconds')::float8 AS seconds,
                   coalesce((details->>'partial_day')::boolean, false) AS partial_day,
                   details->>'error' AS error
            FROM batch_runs
            ORDER BY started_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
