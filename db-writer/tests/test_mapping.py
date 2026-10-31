import json
from decimal import Decimal

import pytest

from app.mapping import BadMessage, decode, to_ticker_row, to_trade_row

TRADE = {
    "pair": "BTC-USD",
    "trade_id": "987654321",
    "price": "67000.12",
    "size": "0.0051",
    "side": "BUY",
    "exchange_time": "2026-10-29T12:00:00.111222Z",
    "ingest_time": "2026-10-29T12:00:00.300000Z",
}

TICKER = {
    "pair": "ETH-USD",
    "price": "2500.5",
    "volume_24h": "12345.6",
    "high_24h": "2600",
    "low_24h": "2400",
    "price_pct_chg_24h": "-1.25",
    "best_bid": "2500.4",
    "best_ask": None,
    "exchange_time": "2026-10-29T12:00:01Z",
    "ingest_time": "2026-10-29T12:00:01.05Z",
}


def test_trade_row_types():
    row = to_trade_row(decode(json.dumps(TRADE).encode()))
    assert row.trade_id == 987654321
    assert row.price == Decimal("67000.12")
    assert row.time.tzinfo is not None
    assert row.time.microsecond == 111222


def test_ticker_row_allows_missing_optional_fields():
    row = to_ticker_row(TICKER)
    assert row.best_bid == Decimal("2500.4")
    assert row.best_ask is None


@pytest.mark.parametrize(
    "change",
    [
        {"price": "0"},
        {"price": "NaN"},
        {"size": "-2"},
        {"side": "HOLD"},
        {"pair": "btc/usd"},
        {"trade_id": "abc"},
        {"trade_id": None},
        {"exchange_time": "2026-10-29T12:00:00"},  # no time zone
        {"exchange_time": "yesterday"},
    ],
)
def test_bad_trades_are_rejected(change):
    with pytest.raises(BadMessage):
        to_trade_row({**TRADE, **change})


def test_missing_field_is_rejected():
    data = dict(TRADE)
    del data["price"]
    with pytest.raises(BadMessage):
        to_trade_row(data)


@pytest.mark.parametrize("raw", [None, b"", b"not json", b"[1, 2]"])
def test_decode_rejects_non_objects(raw):
    with pytest.raises(BadMessage):
        decode(raw)
