from datetime import datetime
from typing import List, Dict, Optional
from section_1.collectors.base_data_collector import BaseDataCollector
from section_1.collectors.data_classes import ExchangeRateRecord
from section_1.models import SourceConfig


class FrankfurterExchangeRateCollector(BaseDataCollector):
    def __init__(self, source_config: SourceConfig):
        super().__init__(source_config)
        self.base_url = "https://api.frankfurter.dev/v1/"

    def collect(self):
        result_rates = self.get_raw_data()
        results = []
        for date_rate in result_rates:
            for target_currency, rate in date_rate["rates"].items():
                record = self.process_row({"date": date_rate["date"],
                                           "base_currency": self.config.url_additional,
                                           "target_currency": target_currency,
                                           "rate": rate
                                           })
                if record:
                    results.append(record)
        stats = self.db_data.write_data(results, self.config.end_table)
        self.logger.info(f"stats: {stats}")
        return stats

    def get_raw_data(self) -> List[Dict]:
        url = f"{self.base_url}{self.start_since.strftime('%Y-%m-%d')}..?base={self.config.url_additional}"
        # Pagination is unnecessary
        # the APIs support data requests spanning over one year.
        data = self._make_api_request(url)
        result_rates = [
            {"rates": rates, "date": date, "base_currency": self.config.url_additional}
            for date, rates in data.get("rates", {}).items()
        ]
        return result_rates

    def process_row(self, row) -> Optional[ExchangeRateRecord]:
        try:
            dt = datetime.fromisoformat(row["date"])
            return ExchangeRateRecord(
                id="",
                source_id=self.config.source_id,
                date=dt,
                base_currency=row['base_currency'],
                target_currency=row['target_currency'],
                rate=row["rate"]
            )
        except Exception as e:
            # Note: Could log error details here if needed.
            self.logger.info(f"Failed to process row: {e}")
            return None
