"""Integration tests against a real TimescaleDB.

Skipped unless TEST_DATABASE_URL points at a database with db/migrations applied, e.g.
  TEST_DATABASE_URL=postgresql://crypto:crypto@localhost:5432/crypto pytest
"""

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg
import pytest

from app.db import Database
from app.mapping import TickerRow, TradeRow

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL not set")

NOW = datetime.now(timezone.utc).replace(microsecond=0)


def trade(trade_id: int, symbol: str = "BTC-USD", seconds: int = 0, price: str = "67000") -> TradeRow:
    t = NOW + timedelta(seconds=seconds)
    return TradeRow(t, symbol, trade_id, Decimal(price), Decimal("0.5"), "BUY", t + timedelta(milliseconds=200))


def ticker(symbol: str = "ETH-USD", seconds: int = 0) -> TickerRow:
    t = NOW + timedelta(seconds=seconds)
    return TickerRow(t, symbol, Decimal("2500"), None, None, None, None, Decimal("2499.9"), Decimal("2500.1"), t)


@pytest.fixture
def db():
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute("TRUNCATE trades, ticker")
        conn.execute("DELETE FROM pairs WHERE symbol NOT IN ('BTC-USD','ETH-USD','SOL-USD','XRP-USD','DOGE-USD')")
    database = Database(URL)
    database.connect()
    yield database
    database.close()


def count(table: str) -> int:
    with psycopg.connect(URL) as conn:
        return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_batch_is_written(db):
    result = db.write([trade(1), trade(2, seconds=1)], [ticker()])
    assert (result.trades_inserted, result.tickers_inserted, result.duplicates) == (2, 1, 0)
    assert count("trades") == 2 and count("ticker") == 1


def test_replayed_batch_is_skipped_not_duplicated(db):
    batch = [trade(1), trade(2, seconds=1)]
    db.write(batch, [])
    result = db.write(batch, [])  # same messages read again after a crash
    assert result.trades_inserted == 0 and result.duplicates == 2
    assert count("trades") == 2


def test_duplicates_inside_one_batch(db):
    result = db.write([trade(1), trade(1)], [])
    assert result.trades_inserted == 1 and result.duplicates == 1


def test_new_pair_is_registered(db):
    db.write([trade(1, symbol="ADA-USD")], [])
    with psycopg.connect(URL) as conn:
        row = conn.execute("SELECT base, quote FROM pairs WHERE symbol = 'ADA-USD'").fetchone()
    assert row == ("ADA", "USD")


def test_one_bad_row_does_not_lose_the_batch(db):
    # 10^11 is bigger than NUMERIC(20,10) allows, so the database rejects this row
    bad = trade(3, seconds=2, price="100000000000")
    result = db.write([trade(1), trade(2, seconds=1), bad], [ticker()])
    assert result.rejected == 1
    assert result.trades_inserted == 2 and result.tickers_inserted == 1
    assert count("trades") == 2


def test_writes_work_again_after_reconnect(db):
    db.write([trade(1)], [])
    db.connect()  # e.g. after the database restarted
    result = db.write([trade(2, seconds=1)], [])
    assert result.trades_inserted == 1
