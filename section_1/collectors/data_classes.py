from dataclasses import dataclass
from datetime import datetime


@dataclass
class StockRecord:
    id: str
    source_id: int
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    stock_ticker: str
    base_currency: str


@dataclass
class ExchangeRateRecord:
    id: str
    source_id: int
    date: datetime
    base_currency: str
    target_currency: str
    rate: float
