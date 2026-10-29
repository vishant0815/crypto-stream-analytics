"""Validated event models and parsers for Coinbase WebSocket messages.

Prices and sizes are kept as Decimal and written to Kafka as strings,
so no precision is lost before Spark reads them.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ValidationError, field_validator

PAIR_PATTERN = r"^[A-Z0-9]{2,10}-[A-Z]{2,5}$"
_FRACTION = re.compile(r"\.(\d{6})\d+")


def _to_datetime(value: Any) -> Any:
    """Coinbase sends nanosecond timestamps; Python datetimes hold microseconds."""
    if isinstance(value, str):
        return _FRACTION.sub(r".\1", value)
    return value


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Trade(BaseModel):
    pair: str
    trade_id: str
    price: Decimal
    size: Decimal
    side: Literal["BUY", "SELL"]
    exchange_time: datetime
    ingest_time: datetime

    _parse_time = field_validator("exchange_time", "ingest_time", mode="before")(_to_datetime)

    @field_validator("pair")
    @classmethod
    def _check_pair(cls, v: str) -> str:
        if not re.match(PAIR_PATTERN, v):
            raise ValueError(f"invalid pair {v!r}")
        return v

    @field_validator("price", "size")
    @classmethod
    def _positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("must be greater than zero")
        return v


class Ticker(BaseModel):
    pair: str
    price: Decimal
    volume_24h: Decimal | None = None
    high_24h: Decimal | None = None
    low_24h: Decimal | None = None
    price_pct_chg_24h: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    exchange_time: datetime
    ingest_time: datetime

    _parse_time = field_validator("exchange_time", "ingest_time", mode="before")(_to_datetime)


class ParseResult(BaseModel):
    """Valid events plus the raw items that failed validation (for the DLQ)."""

    events: list[Trade | Ticker] = []
    errors: list[dict[str, Any]] = []


def parse_market_trades(message: dict[str, Any]) -> ParseResult:
    result = ParseResult()
    ingest_time = utc_now()
    for event in message.get("events", []):
        for raw in event.get("trades", []):
            try:
                result.events.append(
                    Trade(
                        pair=raw.get("product_id", ""),
                        trade_id=str(raw.get("trade_id", "")),
                        price=raw.get("price"),
                        size=raw.get("size"),
                        side=raw.get("side"),
                        exchange_time=raw.get("time"),
                        ingest_time=ingest_time,
                    )
                )
            except ValidationError as exc:
                result.errors.append({"error": exc.errors(include_url=False), "raw": raw})
    return result


def parse_ticker(message: dict[str, Any]) -> ParseResult:
    result = ParseResult()
    ingest_time = utc_now()
    exchange_time = message.get("timestamp")
    for event in message.get("events", []):
        for raw in event.get("tickers", []):
            try:
                result.events.append(
                    Ticker(
                        pair=raw.get("product_id", ""),
                        price=raw.get("price"),
                        volume_24h=raw.get("volume_24_h"),
                        high_24h=raw.get("high_24_h"),
                        low_24h=raw.get("low_24_h"),
                        price_pct_chg_24h=raw.get("price_percent_chg_24_h"),
                        best_bid=raw.get("best_bid") or None,
                        best_ask=raw.get("best_ask") or None,
                        exchange_time=exchange_time,
                        ingest_time=ingest_time,
                    )
                )
            except ValidationError as exc:
                result.errors.append({"error": exc.errors(include_url=False), "raw": raw})
    return result
