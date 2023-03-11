import enum
from datetime import date, datetime
from decimal import Decimal
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


class OptionsTickerModel(BaseModel):
    class Config:
        orm_mode = True

    options_ticker: str
    underlying_ticker_id: int
    expiration_date: date
    strike_price: Decimal
    contract_type: ContractType
    shares_per_contract: Optional[int]
    cfi: Optional[str]
    exercise_style: Optional[str]
    primary_exchange: Optional[str]
    id: Optional[int]


class OptionsPricesRaw(Base):
    __tablename__ = "option_prices"
    __table_args__ = (UniqueConstraint("options_ticker_id", "as_of_date", name="uq_options_price"),)
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(BigInteger, ForeignKey("options_tickers.id", ondelete="CASCADE"), nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    close_price = Column(DECIMAL(19, 4), nullable=False)
    open_price = Column(DECIMAL(19, 4), nullable=False)
    high_price = Column(DECIMAL(19, 4), nullable=False)
    low_price = Column(DECIMAL(19, 4), nullable=False)
    volume_weight_price = Column(DECIMAL(19, 4), nullable=False)
    volume = Column(DECIMAL(19, 4), nullable=False)
    number_of_transactions = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())  # make sure this is UTCNow
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.utcnow)
    is_overwritten = Column(Boolean, server_default=expression.false())


class PriceModel(BaseModel):
    class Config:
        orm_mode = True

    options_ticker_id: int
    as_of_date: datetime
    close_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume_weight_price: Decimal
    volume: Decimal
    number_of_transactions: int
    id: Optional[int]


# View: OptionsPricesRich where you calculate volatility, greeks, implied volatility, daily return?
