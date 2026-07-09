from datetime import datetime
from pydantic import BaseModel, field_validator

class StockPriceEvent(BaseModel):
    ticker: str
    price: float
    fetched_at: datetime

    @field_validator("ticker")
    @classmethod
    def ticker_must_be_known_format(cls, v: str) -> str:
        if not v.isupper() or not v.isalpha():
            raise ValueError(f"'{v}' doesn't look like a valid ticker symbol")
        return v

    @field_validator("price")
    @classmethod
    def price_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"price must be positive, got {v}")
        return v