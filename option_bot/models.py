import enum
from datetime import datetime

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import expression
from sqlalchemy.types import Date, DateTime

BaseSchema = declarative_base()


class OptionType(enum.Enum):
    call = "call"
    put = "put"


class UTCNow(expression.FunctionElement):  # type: ignore[name-defined]
    type = DateTime()


class StockTickers(BaseSchema):
    __tablename__ = "stock_tickers"
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    ticker_symbol = Column(String, nullable=False)
    name = Column(String)


class OptionsTickers(BaseSchema):
    __tablename__ = "options_tickers"
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    underlying_ticker_id = Column(Integer, ForeignKey("stock_tickers.id", ondelete="CASCADE"), nullable=False)
    option_ticker = Column(String, nullable=False)
    exp_date = Column()
    strike_price = Column()
    option_type = Column()


class OptionsPrices(BaseSchema):
    __tablename__ = "option_prices"
    __table_args__ = UniqueConstraint("options_ticker_id", "price_date", "as_of_date", name="uq_current_price")
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(Integer, ForeignKey("options_tickers.id", ondelete=True), nullable=False)
    price_date = Column()
    as_of_date = Column()
    created_at = Column()
    updated_at = Column(DateTime, server_default=UTCNow(), onupdate=datetime.datetime.utcnow)
    is_overwritten = Column()
