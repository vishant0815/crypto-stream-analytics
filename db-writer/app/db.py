"""Database access: fast batch inserts into TimescaleDB.

Each batch is COPYed into a temporary staging table, then moved into the
real table with INSERT ... ON CONFLICT DO NOTHING. COPY is far faster than
row-by-row INSERTs, and ON CONFLICT makes replays from Kafka harmless:
a trade that is already stored is simply skipped.
"""

import logging
from dataclasses import dataclass

import psycopg
from psycopg import errors

from app.mapping import TickerRow, TradeRow

log = logging.getLogger(__name__)

TRADE_COLUMNS = "time, pair_id, trade_id, price, size, side, ingest_time"
TICKER_COLUMNS = (
    "time, pair_id, price, volume_24h, high_24h, low_24h, "
    "price_pct_chg_24h, best_bid, best_ask, ingest_time"
)

# Errors caused by the data itself. Retrying the same rows would fail forever,
# so these rows are written one at a time and the bad ones are skipped.
DATA_ERRORS = (errors.DataError, errors.IntegrityError)


@dataclass
class WriteResult:
    trades_inserted: int = 0
    tickers_inserted: int = 0
    duplicates: int = 0
    rejected: int = 0


class Database:
    def __init__(self, url: str) -> None:
        self.url = url
        self.conn: psycopg.Connection | None = None
        self._pair_ids: dict[str, int] = {}

    # ---------- connection ----------

    def connect(self) -> None:
        self.close()
        self.conn = psycopg.connect(self.url, application_name="db-writer")
        with self.conn.cursor() as cur:
            # Session-only staging tables, emptied after every commit
            cur.execute(
                "CREATE TEMP TABLE trades_stage "
                "(LIKE trades INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
            )
            cur.execute(
                "CREATE TEMP TABLE ticker_stage "
                "(LIKE ticker INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
            )
            cur.execute("SELECT symbol, pair_id FROM pairs")
            self._pair_ids = dict(cur.fetchall())
        self.conn.commit()
        log.info("connected to database, %d pairs loaded", len(self._pair_ids))

    def close(self) -> None:
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
        self.conn = None

    # ---------- pairs ----------

    def _pair_id(self, cur: psycopg.Cursor, symbol: str) -> int:
        """Look up a pair, creating it the first time a new symbol appears."""
        if symbol not in self._pair_ids:
            base, quote = symbol.split("-")
            cur.execute(
                "INSERT INTO pairs (symbol, base, quote) VALUES (%s, %s, %s) "
                "ON CONFLICT (symbol) DO NOTHING",
                (symbol, base, quote),
            )
            cur.execute("SELECT pair_id FROM pairs WHERE symbol = %s", (symbol,))
            self._pair_ids[symbol] = cur.fetchone()[0]
            log.info("new pair %s registered with id %d", symbol, self._pair_ids[symbol])
        return self._pair_ids[symbol]

    # ---------- writes ----------

    def write(self, trades: list[TradeRow], tickers: list[TickerRow]) -> WriteResult:
        """Write one batch in a single transaction. Raises on connection problems."""
        assert self.conn is not None, "call connect() first"
        result = WriteResult()
        try:
            with self.conn.transaction(), self.conn.cursor() as cur:
                result.trades_inserted = self._copy_trades(cur, trades)
                result.tickers_inserted = self._copy_tickers(cur, tickers)
        except DATA_ERRORS as exc:
            log.warning("batch rejected (%s), retrying row by row", exc.diag.message_primary)
            self._forget_new_pairs()
            return self._write_one_by_one(trades, tickers)
        except Exception:
            self._forget_new_pairs()
            raise

        result.duplicates = (len(trades) - result.trades_inserted) + (len(tickers) - result.tickers_inserted)
        return result

    def _copy_trades(self, cur: psycopg.Cursor, rows: list[TradeRow]) -> int:
        if not rows:
            return 0
        # Resolve pair ids first: no other query can run while COPY is open
        ids = {s: self._pair_id(cur, s) for s in {r.symbol for r in rows}}
        with cur.copy(f"COPY trades_stage ({TRADE_COLUMNS}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row((r.time, ids[r.symbol], r.trade_id, r.price, r.size, r.side, r.ingest_time))
        cur.execute(
            f"INSERT INTO trades ({TRADE_COLUMNS}) SELECT {TRADE_COLUMNS} FROM trades_stage "
            "ON CONFLICT DO NOTHING"
        )
        return cur.rowcount

    def _copy_tickers(self, cur: psycopg.Cursor, rows: list[TickerRow]) -> int:
        if not rows:
            return 0
        ids = {s: self._pair_id(cur, s) for s in {r.symbol for r in rows}}
        with cur.copy(f"COPY ticker_stage ({TICKER_COLUMNS}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row(
                    (
                        r.time, ids[r.symbol], r.price, r.volume_24h, r.high_24h,
                        r.low_24h, r.price_pct_chg_24h, r.best_bid, r.best_ask, r.ingest_time,
                    )
                )
        cur.execute(
            f"INSERT INTO ticker ({TICKER_COLUMNS}) SELECT {TICKER_COLUMNS} FROM ticker_stage "
            "ON CONFLICT DO NOTHING"
        )
        return cur.rowcount

    def _write_one_by_one(self, trades: list[TradeRow], tickers: list[TickerRow]) -> WriteResult:
        """Slow path: each row in its own savepoint, so one bad row can't sink the batch."""
        assert self.conn is not None
        result = WriteResult()
        with self.conn.transaction(), self.conn.cursor() as cur:
            for row in [*trades, *tickers]:
                try:
                    with self.conn.transaction():  # savepoint
                        if isinstance(row, TradeRow):
                            inserted = self._copy_trades(cur, [row])
                            result.trades_inserted += inserted
                        else:
                            inserted = self._copy_tickers(cur, [row])
                            result.tickers_inserted += inserted
                        result.duplicates += 1 - inserted
                        # Staging rows only clear on commit, so clear them by hand here
                        cur.execute("TRUNCATE trades_stage, ticker_stage")
                except DATA_ERRORS as exc:
                    result.rejected += 1
                    self._forget_new_pairs()
                    log.error("row rejected: %s | %r", exc.diag.message_primary, row)
        return result

    def _forget_new_pairs(self) -> None:
        """A rolled-back transaction may have undone new pairs; reload from the database next time."""
        self._pair_ids = {}
        if self.conn is not None and not self.conn.closed:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT symbol, pair_id FROM pairs")
                    self._pair_ids = dict(cur.fetchall())
                self.conn.commit()
            except psycopg.Error:
                pass
