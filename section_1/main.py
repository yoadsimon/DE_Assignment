from datetime import datetime

from DE_Assignment.section_1.collectors.base_data_collector import BaseDataCollector
from DE_Assignment.section_1.collectors.collectors import collectors
from DE_Assignment.section_1.database import DbSettings, DbData
from DE_Assignment.section_1.models import SourceConfig


def create_source(source):
    # Instantiate a SourceConfig and add it to the database.
    source_config = SourceConfig(**source)
    db_settings = DbSettings()
    db_settings.add_source(source_config)


def preform_single_scrape(source_id: int):
    # Retrieve source configuration and invoke the appropriate data collector.
    db_settings = DbSettings()
    source_config: SourceConfig = db_settings.get_source_config(source_id)
    collector: BaseDataCollector = collectors[source_config.source_type](source_config)
    return collector.collect()


if __name__ == "__main__":
    # Sample test for polygon source.
    source1 = {
        "source_id": 1,
        "source_type": "polygon",
        "url_additional": "NVDA",
        "scrape_since": datetime(2025, 2, 1),
        "token": "TbZ3iHiZyQtooWrx9kzwfL73hpdjeE0Y",
        "end_table": ""
    }
    create_source(source=source1)
    stats = preform_single_scrape(1)
    print(stats)

    # Sample test for frankfurter source.
    source2 = {
        "source_id": 2,
        "source_type": "frankfurter",
        "url_additional": "USD",
        "scrape_since": datetime(2025, 2, 1),
        "end_table": ""  # will be looked up (should resolve to "exchange_rate_records")
    }
    create_source(source=source2)
    stats = preform_single_scrape(2)
    print(stats)

    # Test for stock price conversion.
    date = datetime(2025, 3, 3)
    stock_name = "NVDA"
    currency = "ILS"
    db_data = DbData()
    rate = db_data.get_stock_price(stock_name=stock_name,
                                   date=date,
                                   out_put_currency=currency)
    print(f"Stock price for {stock_name} on {date} is {rate} in {currency}")