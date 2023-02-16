import enum
from datetime import datetime

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


class OptionType(enum.Enum):
    call = "call"
    put = "put"


class UTCNow(expression.FunctionElement):  # type: ignore[name-defined]
    type = DateTime()


class StockTickers(Base):
    __tablename__ = "stock_tickers"
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    ticker_symbol = Column(String, nullable=False)
    name = Column(String)


class OptionsTickers(Base):
    __tablename__ = "options_tickers"
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    underlying_ticker_id = Column(Integer, ForeignKey("stock_tickers.id", ondelete="CASCADE"), nullable=False)
    option_ticker = Column(String, nullable=False)
    exp_date = Column(Date, nullable=False)
    strike_price = Column(DECIMAL(19, 4), nullable=False)
    option_type = Column(Enum(OptionType), nullable=False)


class OptionsPricesRaw(Base):
    __tablename__ = "option_prices"
    __table_args__ = (UniqueConstraint("options_ticker_id", "price_date", "as_of_date", name="uq_current_price"),)
    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(BigInteger, ForeignKey("options_tickers.id", ondelete="CASCADE"), nullable=False)
    price_date = Column(Date, nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    close_price = Column(DECIMAL(19, 4), nullable=False)
    open_price = Column(DECIMAL(19, 4), nullable=False)
    high_price = Column(DECIMAL(19, 4), nullable=False)
    low_price = Column(DECIMAL(19, 4), nullable=False)
    volume_weight_price = Column(DECIMAL(19, 4), nullable=False)
    volumn = Column(Integer, nullable=False)
    number_of_trades = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())  # make sure this is UTCNow
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.utcnow)
    is_overwritten = Column(Boolean, server_default=expression.false())


# View: OptionsPricesRich where you calculate volatility, greeks, implied volatility, daily return?
