import pytest
from pydantic import ValidationError

from schemas import StockPriceEvent


def test_valid_record_passes():
    event = StockPriceEvent(
        ticker="AAPL",
        price=190.5,
        fetched_at="2026-07-09T12:00:00",
    )
    assert event.ticker == "AAPL"
    assert event.price == 190.5


def test_negative_price_is_rejected():
    with pytest.raises(ValidationError):
        StockPriceEvent(
            ticker="AAPL",
            price=-10,
            fetched_at="2026-07-09T12:00:00",
        )


def test_zero_price_is_rejected():
    with pytest.raises(ValidationError):
        StockPriceEvent(
            ticker="AAPL",
            price=0,
            fetched_at="2026-07-09T12:00:00",
        )


def test_lowercase_ticker_is_rejected():
    with pytest.raises(ValidationError):
        StockPriceEvent(
            ticker="aapl",
            price=190.5,
            fetched_at="2026-07-09T12:00:00",
        )


def test_ticker_with_numbers_is_rejected():
    with pytest.raises(ValidationError):
        StockPriceEvent(
            ticker="AAPL1",
            price=190.5,
            fetched_at="2026-07-09T12:00:00",
        )


def test_missing_field_is_rejected():
    with pytest.raises(ValidationError):
        StockPriceEvent(ticker="AAPL", price=190.5)  # missing fetched_at