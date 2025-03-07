import abc
import os
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Float, Date, ForeignKey
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.future import select
from dotenv import load_dotenv
from typing import List

from DE_Assignment.section_1.collectors.data_classes import StockRecord, ExchangeRateRecord
from DE_Assignment.section_1.models import SourceConfig

load_dotenv()
Base = declarative_base()


class SourceConfigModel(Base):
    __tablename__ = "sources"
    source_id = Column(Integer, primary_key=True)
    source_type = Column(String)
    url_additional = Column(String)
    scrape_since = Column(Date)
    token = Column(String)
    end_table = Column(String)  # now provided explicitly


class SourceTypeEndTable(Base):
    __tablename__ = "source_type_end_table"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_type = Column(String, unique=True, nullable=False)
    end_table = Column(String, nullable=False)


class StockRecordModel(Base):
    __tablename__ = "stock_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.source_id"))
    date = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    stock_ticker = Column(String, nullable=False)
    base_currency = Column(String, nullable=False)


class ExchangeRateRecordModel(Base):
    __tablename__ = "exchange_rate_records"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.source_id"))
    date = Column(DateTime, nullable=False)
    base_currency = Column(String, nullable=False)
    target_currency = Column(String, nullable=False)
    rate = Column(Float, nullable=False)


class DbBase(abc.ABC):
    def __init__(self):
        db_url = os.getenv("DB_URL")
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        # Note: Initialize the mapping between source types and target tables.
        self.__init_source_type_end_table()

    def get_db_session(self) -> Session:
        return Session(self.engine)

    def __init_source_type_end_table(self):
        session = self.get_db_session()
        table_mapping = {
            "polygon": "stock_records",
            "frankfurter": "exchange_rate_records"
        }
        # Create mapping entries only if not already present.
        for src_type, end_table in table_mapping.items():
            stmt = select(SourceTypeEndTable).where(SourceTypeEndTable.source_type == src_type)
            result = session.execute(stmt).scalars().first()
            if not result:
                new_mapping = SourceTypeEndTable(source_type=src_type, end_table=end_table)
                session.add(new_mapping)
        session.commit()


class DbSettings(DbBase):
    def get_source_config(self, source_id: int) -> SourceConfig:
        with self.get_db_session() as session:
            stmt = select(SourceConfigModel).where(
                SourceConfigModel.source_id == source_id,
            )
            result = session.execute(stmt).scalars().first()
            if not result:
                raise ValueError(f"Configuration not found for source_id: {source_id}")
            config_dict = {
                "source_id": result.source_id,
                "source_type": result.source_type,
                "url_additional": result.url_additional,
                "scrape_since": result.scrape_since,
                "token": result.token,
                "end_table": result.end_table
            }
            return SourceConfig(**config_dict)

    def add_source(self, source_config: SourceConfig) -> None:
        with self.get_db_session() as session:
            stmt = select(SourceConfigModel).where(
                SourceConfigModel.source_id == source_config.source_id
            )
            existing = session.execute(stmt).scalars().first()
            if existing:
                raise ValueError(f"Source config with source_id {source_config.source_id} already exists.")

            end_table_value = source_config.end_table
            if not end_table_value:
                # Note: Lookup default target table based on source type.
                lookup_stmt = select(SourceTypeEndTable).where(
                    SourceTypeEndTable.source_type == source_config.source_type
                )
                lookup = session.execute(lookup_stmt).scalars().first()
                if not lookup:
                    raise ValueError(f"No default end_table found for source type: {source_config.source_type}")
                end_table_value = lookup.end_table

            new_config = SourceConfigModel(
                source_id=source_config.source_id,
                source_type=source_config.source_type,
                url_additional=source_config.url_additional,
                scrape_since=source_config.scrape_since,
                token=source_config.token,
                end_table=end_table_value
            )
            session.add(new_config)
            session.commit()
            print("Source config inserted successfully.")


class DbData(DbBase):
    def write_data(self, data: List[StockRecord] | List[ExchangeRateRecord], table_end="stock_records") -> dict:
        with self.get_db_session() as session:
            inserted = 0
            modified = 0
            failed = 0
            # Iterate records and update or insert based on existence.
            for record in data:
                try:
                    if table_end == "stock_records" and isinstance(record, StockRecord):
                        existing = session.query(StockRecordModel).filter_by(
                            source_id=record.source_id,
                            date=record.date,
                            stock_ticker=record.stock_ticker
                        ).first()
                        if existing:
                            existing.open = record.open
                            existing.high = record.high
                            existing.low = record.low
                            existing.close = record.close
                            existing.volume = record.volume
                            modified += 1
                        else:
                            # Remove the id field to let the database autogenerate it.
                            data_dict = record.__dict__
                            data_dict.pop("id", None)
                            new_fact = StockRecordModel(**data_dict)
                            session.add(new_fact)
                            inserted += 1

                    elif table_end == "exchange_rate_records" and isinstance(record, ExchangeRateRecord):
                        existing = session.query(ExchangeRateRecordModel).filter_by(
                            source_id=record.source_id,
                            date=record.date,
                            base_currency=record.base_currency,
                            target_currency=record.target_currency
                        ).first()
                        if existing:
                            existing.rate = record.rate
                            modified += 1
                        else:
                            data_dict = record.__dict__
                            data_dict.pop("id", None)
                            new_rate = ExchangeRateRecordModel(**data_dict)
                            session.add(new_rate)
                            inserted += 1
                    else:
                        raise ValueError(f"Unknown table_end: {table_end}")
                except Exception as e:
                    failed += 1
            session.commit()
        return {"inserted": inserted, "modified": modified, "failed": failed}

    def get_data_since(self, source_id: int, end_table: str):
        with self.get_db_session() as session:
            table_mapping = {
                "stock_records": StockRecordModel,
                "exchange_rate_records": ExchangeRateRecordModel,
            }
            model = table_mapping.get(end_table)
            if model is None:
                raise ValueError(f"Unknown end_table: {end_table}")

            stmt = (
                select(model)
                .where(model.source_id == source_id)
                .order_by(model.date.desc())
                .limit(1)
            )
            result = session.execute(stmt).scalars().first()
            if result:
                return result.date
        return None

    def get_stock_price(self, stock_name, date, out_put_currency):
        """
        Returns the stock price for a given stock and date in the desired currency.
        First, looks for a stock record in the required currency.
        If not found, performs a join with the exchange rate table to convert the price.
        """
        with self.get_db_session() as session:
            # Try same currency first.
            stmt = (
                select(StockRecordModel)
                .where(StockRecordModel.stock_ticker == stock_name)
                .where(StockRecordModel.date == date)
                .where(StockRecordModel.base_currency == out_put_currency)
                .limit(1)
            )
            stock_record = session.execute(stmt).scalar_one_or_none()
            if stock_record:
                return stock_record.close

            # Otherwise, perform a join to attempt conversion.
            stmt = (
                select(StockRecordModel, ExchangeRateRecordModel)
                .join(
                    ExchangeRateRecordModel,
                    (ExchangeRateRecordModel.base_currency == StockRecordModel.base_currency)
                    & (ExchangeRateRecordModel.target_currency == out_put_currency)
                    & (ExchangeRateRecordModel.date == date)
                )
                .where(StockRecordModel.stock_ticker == stock_name)
                .where(StockRecordModel.date == date)
                .limit(1)
            )
            row = session.execute(stmt).first()
            if row:
                stock_record, exchange_rate = row
                return stock_record.close * exchange_rate.rate

            # None found.
            return None
