from decimal import Decimal

from app.main import Stats, route
from app.models import parse_market_trades, parse_ticker

TRADES_MESSAGE = {
    "channel": "market_trades",
    "timestamp": "2026-10-29T12:00:00.123456789Z",
    "sequence_num": 5,
    "events": [
        {
            "type": "update",
            "trades": [
                {
                    "trade_id": "123456",
                    "product_id": "BTC-USD",
                    "price": "67000.12",
                    "size": "0.00510000",
                    "side": "BUY",
                    "time": "2026-10-29T12:00:00.111222333Z",
                },
                {
                    "trade_id": "123457",
                    "product_id": "BTC-USD",
                    "price": "-1",
                    "size": "0.1",
                    "side": "SELL",
                    "time": "2026-10-29T12:00:00.2Z",
                },
            ],
        }
    ],
}

TICKER_MESSAGE = {
    "channel": "ticker",
    "timestamp": "2026-10-29T12:00:01.987654321Z",
    "events": [
        {
            "type": "update",
            "tickers": [
                {
                    "type": "ticker",
                    "product_id": "ETH-USD",
                    "price": "2500.5",
                    "volume_24_h": "12345.6",
                    "low_24_h": "2400",
                    "high_24_h": "2600",
                    "price_percent_chg_24_h": "1.25",
                    "best_bid": "2500.4",
                    "best_ask": "2500.6",
                }
            ],
        }
    ],
}


class FakeSink:
    def __init__(self) -> None:
        self.sent = 0
        self.failed = 0
        self.messages: list[tuple[str, str | None, dict]] = []

    def publish(self, topic, key, value):
        self.messages.append((topic, key, value))
        self.sent += 1

    def poll(self):
        pass

    def close(self):
        pass


def test_valid_trade_is_parsed_with_full_precision():
    result = parse_market_trades(TRADES_MESSAGE)
    trade = result.events[0]
    assert trade.pair == "BTC-USD"
    assert trade.price == Decimal("67000.12")
    assert trade.size == Decimal("0.00510000")
    assert trade.exchange_time.microsecond == 111222  # nanoseconds trimmed, not rejected


def test_invalid_trade_goes_to_errors_not_events():
    result = parse_market_trades(TRADES_MESSAGE)
    assert len(result.events) == 1
    assert len(result.errors) == 1
    assert result.errors[0]["raw"]["trade_id"] == "123457"


def test_ticker_is_parsed():
    result = parse_ticker(TICKER_MESSAGE)
    ticker = result.events[0]
    assert ticker.pair == "ETH-USD"
    assert ticker.best_ask == Decimal("2500.6")
    assert result.errors == []


def test_route_sends_trades_keyed_by_pair_and_bad_ones_to_dlq():
    sink, stats = FakeSink(), Stats()
    route(TRADES_MESSAGE, sink, stats)

    topics = [m[0] for m in sink.messages]
    assert topics == ["raw.trades", "dlq.trades"]
    assert sink.messages[0][1] == "BTC-USD"
    assert sink.messages[0][2]["price"] == "67000.12"  # Decimal kept as string in JSON
    assert stats.trades == 1 and stats.dlq == 1


def test_heartbeats_are_ignored():
    sink, stats = FakeSink(), Stats()
    route({"channel": "heartbeats", "events": []}, sink, stats)
    assert sink.messages == []
