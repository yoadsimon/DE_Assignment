import logging
from abc import ABC, abstractmethod

import requests
from tqdm import tqdm
from typing import List, Dict, Optional
import time

from requests.exceptions import HTTPError, RequestException

from section_1.collectors.data_classes import StockRecord, ExchangeRateRecord
from section_1.database import DbData
from section_1.models import SourceConfig


class BaseDataCollector(ABC):
    def __init__(self, source_config: SourceConfig):
        self.config = source_config
        self.db_data = DbData()
        self.start_since = self.get_start_since()
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_start_since(self):
        # Chooses the later of stored data or configured scrape_since.
        data_since = self.db_data.get_data_since(self.config.source_id, self.config.end_table)
        config_since = self.config.scrape_since
        if not data_since:
            return config_since
        return max(data_since, config_since)

    def collect(self):
        raw_data = self.get_raw_data()
        filtered_data = self.filter_data(raw_data)
        results: List[StockRecord] = []
        if filtered_data:
            for row in tqdm(filtered_data):
                record = self.process_row(row)
                if record is not None:
                    results.append(record)
        stats = self.db_data.write_data(results)
        self.logger.info(f"stats: {stats}")
        return stats

    def _make_api_request(self, url: str, retry=0) -> dict:
        self.logger.info(f"Making API request to {url}")
        try:
            response = requests.get(url, timeout=20)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", 60)
                error_msg = f"Rate limit exceeded. Retry after {retry_after} seconds."
                self.logger.error(error_msg)
                if retry >= 3:
                    raise Exception("Rate limit exceeded")
                time.sleep(int(retry_after))
                return self._make_api_request(url)
            response.raise_for_status()
            data = response.json()
            return data
        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err}")
            raise
        except RequestException as req_err:
            self.logger.error(f"Request exception: {req_err}")
            raise
        except Exception as err:
            self.logger.error(f"Unexpected error: {err}")
            raise

    def filter_data(self, data: List[Dict]) -> List[Dict]:
        # An example method that could add to the base class.
        return data

    @abstractmethod
    def get_raw_data(self) -> List[Dict]:
        pass

    @abstractmethod
    def process_row(self, row: Dict) -> Optional[StockRecord | ExchangeRateRecord]:
        pass
