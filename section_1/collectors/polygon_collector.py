import pandas as pd
from datetime import datetime
from typing import List, Dict

from DE_Assignment.section_1.collectors.base_data_collector import BaseDataCollector
from DE_Assignment.section_1.collectors.data_classes import StockRecord
from DE_Assignment.section_1.models import SourceConfig


class PolygonCollector(BaseDataCollector):
    def __init__(self, source_config: SourceConfig):
        super().__init__(source_config)
        self.base_url = "https://api.polygon.io"

    def get_raw_data(self) -> List[Dict]:
        ticker = self.config.url_additional
        date_start = self.start_since
        date_end = datetime.now()
        date_start_str = date_start.strftime("%Y-%m-%d")
        date_end_str = date_end.strftime("%Y-%m-%d")
        url = (f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/day/"
               f"{date_start_str}/{date_end_str}?adjusted=true&sort=asc&limit=120&apiKey={self.config.token}")
        data = self._make_api_request(url)
        if "results" not in data:
            raise ValueError(f"Polygon response missing 'results': {data}")
        df = pd.DataFrame(data["results"])
        # Note: Normalize timestamp to ensure only date is stored.
        df["date"] = pd.to_datetime(df["t"], unit="ms").dt.normalize()
        df = df[["date", "o", "h", "l", "c", "v"]]
        df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
        df["stock_ticker"] = ticker.upper()
        return df.to_dict(orient="records")

    def process_row(self, row: Dict) -> StockRecord:
        return StockRecord(
            id="",
            source_id=self.config.source_id,
            date=row.get("date"),
            open=row.get("open"),
            high=row.get("high"),
            low=row.get("low"),
            close=row.get("close"),
            volume=row.get("volume"),
            stock_ticker=row.get("stock_ticker"),
            base_currency="USD",
        )
