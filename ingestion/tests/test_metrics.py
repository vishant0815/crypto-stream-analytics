"""The Prometheus counters move with what route() does."""

from prometheus_client import REGISTRY

from app.main import Stats, route
from app.producer import ConsoleSink


def value(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def trades_message(*prices: str) -> dict:
    return {
        "channel": "market_trades",
        "timestamp": "2026-10-29T12:00:00Z",
        "sequence_num": 1,
        "events": [{
            "type": "update",
            "trades": [
                {"trade_id": str(i), "product_id": "BTC-USD", "price": p, "size": "0.1",
                 "side": "BUY", "time": "2026-10-29T12:00:00Z"}
                for i, p in enumerate(prices)
            ],
        }],
    }


def test_valid_and_invalid_trades_are_counted(capsys):
    trades, dlq = value("ingestion_events_total", kind="trade"), value("ingestion_dlq_total")
    lag = value("ingestion_exchange_lag_seconds_count")

    route(trades_message("67000", "-1", "67001"), ConsoleSink(), Stats())

    assert value("ingestion_events_total", kind="trade") == trades + 2
    assert value("ingestion_dlq_total") == dlq + 1
    assert value("ingestion_exchange_lag_seconds_count") == lag + 2
    assert value("ingestion_last_message_timestamp_seconds") > 0


def test_replayed_trades_are_left_out_of_the_lag(capsys):
    message = trades_message("67000")
    message["events"][0]["type"] = "snapshot"  # Coinbase's resend after (re)connecting
    lag = value("ingestion_exchange_lag_seconds_count")
    route(message, ConsoleSink(), Stats())
    assert value("ingestion_exchange_lag_seconds_count") == lag
