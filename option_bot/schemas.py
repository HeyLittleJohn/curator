import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DECIMAL,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import expression, func
from sqlalchemy.types import Date, DateTime


Base = declarative_base()


class ContractType(enum.Enum):
    call = "call"
    put = "put"


class UTCNow(expression.FunctionElement):  # type: ignore[name-defined]
    type = DateTime()


class StockTickers(Base):
    __tablename__ = "stock_tickers"
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    ticker = Column(String, unique=True, nullable=False)
    imported = Column(Boolean, nullable=False, server_default=expression.false())
    options_imported = Column(Boolean, nullable=False, server_default=expression.false())
    name = Column(String, nullable=False)
    active = Column(Boolean, nullable=False)
    type = Column(String)
    market = Column(String)
    locale = Column(String)
    primary_exchange = Column(String)
    currency_name = Column(String)
    cik = Column(String)
    created_at = Column(DateTime, server_default=func.now())  # make sure this is UTCNow
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.utcnow)


class TickerModel(BaseModel):
    class Config:
        orm_mode = True

    ticker: str
    imported: bool
    name: str
    type: str
    active: bool
    market: Optional[str]
    locale: Optional[str]
    primary_exchange: Optional[str]
    currency_name: Optional[str]
    cik: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    id: Optional[int]


class StockPricesRaw(Base):
    __tablename__ = "stock_prices"
    __table_args__ = (UniqueConstraint("ticker_id", "as_of_date", name="uq_stock_price"),)
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    ticker_id = Column(BigInteger, ForeignKey("stock_tickers.id", ondelete="CASCADE"), nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    close_price = Column(DECIMAL(19, 4), nullable=False)
    open_price = Column(DECIMAL(19, 4), nullable=False)
    high_price = Column(DECIMAL(19, 4), nullable=False)
    low_price = Column(DECIMAL(19, 4), nullable=False)
    volume_weight_price = Column(DECIMAL(19, 4), nullable=False)
    volume = Column(DECIMAL(19, 4), nullable=False)
    number_of_transactions = Column(Integer)
    otc = Column(Boolean)
    created_at = Column(DateTime, server_default=func.now())  # make sure this is UTCNow
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.utcnow)
    is_overwritten = Column(Boolean, server_default=expression.false())


class OptionsTickers(Base):
    __tablename__ = "options_tickers"
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    underlying_ticker_id = Column(Integer, ForeignKey("stock_tickers.id", ondelete="CASCADE"), nullable=False)
    options_ticker = Column(String, nullable=False, unique=True)
    expiration_date = Column(Date, nullable=False)
    strike_price = Column(DECIMAL(19, 4), nullable=False)
    contract_type = Column(Enum(ContractType), nullable=False)
    shares_per_contract = Column(Integer)
    cfi = Column(String)
    exercise_style = Column(String)
    primary_exchange = Column(String)


class OptionsPricesRaw(Base):
    __tablename__ = "option_prices"
    __table_args__ = (UniqueConstraint("options_ticker_id", "price_date", "as_of_date", name="uq_options_price"),)
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(BigInteger, ForeignKey("options_tickers.id", ondelete="CASCADE"), nullable=False)
    price_date = Column(Date, nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    close_price = Column(DECIMAL(19, 4), nullable=False)
    open_price = Column(DECIMAL(19, 4), nullable=False)
    high_price = Column(DECIMAL(19, 4), nullable=False)
    low_price = Column(DECIMAL(19, 4), nullable=False)
    volume_weight_price = Column(DECIMAL(19, 4), nullable=False)
    volume = Column(DECIMAL(19, 4), nullable=False)
    number_of_transactions = Column(Integer)
    otc = Column(Boolean)
    created_at = Column(DateTime, server_default=func.now())  # make sure this is UTCNow
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.utcnow)
    is_overwritten = Column(Boolean, server_default=expression.false())


# View: OptionsPricesRich where you calculate volatility, greeks, implied volatility, daily return?
