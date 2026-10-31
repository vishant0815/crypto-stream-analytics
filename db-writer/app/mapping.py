"""Turns Kafka message values (JSON from the ingestion service) into table rows.

Pure functions, no I/O, so they are easy to unit test.
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class TradeRow:
    time: datetime
    symbol: str
    trade_id: int
    price: Decimal
    size: Decimal
    side: str
    ingest_time: datetime


@dataclass(frozen=True)
class TickerRow:
    time: datetime
    symbol: str
    price: Decimal
    volume_24h: Decimal | None
    high_24h: Decimal | None
    low_24h: Decimal | None
    price_pct_chg_24h: Decimal | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    ingest_time: datetime


class BadMessage(ValueError):
    """The message can't be turned into a row; it is logged and skipped."""


# Same rule as the CHECK constraint on pairs.symbol
_SYMBOL = re.compile(r"^[A-Z0-9]{2,10}-[A-Z]{2,5}$")


def _symbol(value: Any) -> str:
    if not isinstance(value, str) or not _SYMBOL.match(value):
        raise ValueError(f"invalid pair {value!r}")
    return value


def _positive(value: Any) -> Decimal:
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError(f"must be a positive number, got {value!r}")
    return number


def _decimal(value: Any) -> Decimal | None:
    return None if value in (None, "") else Decimal(str(value))


def _side(value: Any) -> str:
    if value not in ("BUY", "SELL"):
        raise ValueError(f"invalid side {value!r}")
    return value


def _time(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no time zone: {value!r}")
    return parsed


def decode(value: bytes | None) -> dict[str, Any]:
    if not value:
        raise BadMessage("empty message")
    try:
        data = json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise BadMessage(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise BadMessage("message is not a JSON object")
    return data


def to_trade_row(data: dict[str, Any]) -> TradeRow:
    try:
        return TradeRow(
            time=_time(data["exchange_time"]),
            symbol=_symbol(data["pair"]),
            trade_id=int(data["trade_id"]),
            price=_positive(data["price"]),
            size=_positive(data["size"]),
            side=_side(data["side"]),
            ingest_time=_time(data["ingest_time"]),
        )
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        raise BadMessage(f"bad trade: {exc!r}") from exc


def to_ticker_row(data: dict[str, Any]) -> TickerRow:
    try:
        return TickerRow(
            time=_time(data["exchange_time"]),
            symbol=_symbol(data["pair"]),
            price=_positive(data["price"]),
            volume_24h=_decimal(data.get("volume_24h")),
            high_24h=_decimal(data.get("high_24h")),
            low_24h=_decimal(data.get("low_24h")),
            price_pct_chg_24h=_decimal(data.get("price_pct_chg_24h")),
            best_bid=_decimal(data.get("best_bid")),
            best_ask=_decimal(data.get("best_ask")),
            ingest_time=_time(data["ingest_time"]),
        )
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        raise BadMessage(f"bad ticker: {exc!r}") from exc
