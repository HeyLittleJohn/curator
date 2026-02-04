import datetime as dt
import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    DECIMAL,
    BigInteger,
    Boolean,
    Column,
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
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.now(dt.UTC))


class TickerModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    volume_weight_price = Column(DECIMAL(19, 4))
    volume = Column(DECIMAL(19, 4), nullable=False)
    number_of_transactions = Column(Integer)
    otc = Column(Boolean)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.now(dt.UTC))
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
    model_config = ConfigDict(from_attributes=True)

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
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.now(dt.UTC))
    is_overwritten = Column(Boolean, server_default=expression.false())


class PriceModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    as_of_date: datetime
    close_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume_weight_price: Decimal
    volume: Decimal
    number_of_transactions: int
    id: Optional[int]


class StockPriceModel(PriceModel):
    model_config = ConfigDict(from_attributes=True)

    ticker_id: int


class OptionPriceModel(PriceModel):
    model_config = ConfigDict(from_attributes=True)

    option_ticker_id: int


class OptionsSnapshotModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    options_ticker_id: int
    as_of_date: datetime
    implied_volatility: Decimal
    delta: Decimal
    gamma: Decimal
    theta: Decimal
    vega: Decimal
    open_interest: int
    id: Optional[int]


class OptionsSnapshot(Base):
    __tablename__ = "options_snapshots"
    __table_args__ = (UniqueConstraint("options_ticker_id", "as_of_date", name="uq_options_snapshot"),)

    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(BigInteger, ForeignKey("options_tickers.id", ondelete="CASCADE"), nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    implied_volatility = Column(DECIMAL(24, 20), nullable=False)
    delta = Column(DECIMAL(24, 20), nullable=False)
    gamma = Column(DECIMAL(24, 20), nullable=False)
    theta = Column(DECIMAL(24, 20), nullable=False)
    vega = Column(DECIMAL(24, 20), nullable=False)
    open_interest = Column(BigInteger, nullable=False)
    is_overwritten = Column(Boolean, server_default=expression.false())


class QuoteModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ask_exchange: Optional[int]
    ask_price: Decimal
    ask_size: int
    bid_exchange: Optional[int]
    bid_price: Decimal
    bid_size: int
    sequence_number: Optional[int]
    sip_timestamp: int


class OptionsQuotesRaw(Base):
    __tablename__ = "options_quotes"
    # NOTE: no unique constraint on this table
    # But there will be a transformation to connect this and the OptionsRaw and that will have a constraint

    id = Column(BigInteger, primary_key=True, unique=True, autoincrement=True)
    options_ticker_id = Column(BigInteger, ForeignKey("options_tickers.id", ondelete="CASCADE"), nullable=False)
    as_of_date = Column(DateTime, nullable=False)
    ask_exchange = Column(Integer)
    ask_price = Column(DECIMAL(19, 4), nullable=False)
    ask_size = Column(Integer, nullable=False)
    bid_exchange = Column(Integer)
    bid_price = Column(DECIMAL(19, 4), nullable=False)
    bid_size = Column(Integer, nullable=False)
    sequence_number = Column(BigInteger)
    sip_timestamp = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=datetime.now(dt.UTC))
    is_overwritten = Column(Boolean, server_default=expression.false())


class SilverFuturesMBO(Base):
    """Databento Level 3 MBO (Market By Order) data for silver futures.

    Stores order book events including trades, order additions, cancellations,
    modifications, and book clear events from Databento's GLBX MBO feed.

    Attributes:
        ts_recv: Timestamp when data was received (ISO 8601).
        ts_event: Timestamp when event occurred at matching engine (ISO 8601).
        rtype: Record type (160 for MBO schema).
        publisher_id: Databento publisher ID.
        instrument_id: Numeric instrument identifier.
        action: Event type - A(dd), C(ancel), M(odify), R(eset), T(rade), F(ill), N(one).
        side: A(sk), B(id), or N(one).
        price: Order price in fixed-point format (1e-9 scale).
        size: Order quantity.
        channel_id: Databento channel ID.
        order_id: Venue-assigned order ID.
        flags: Bit field for event characteristics.
        ts_in_delta: Nanoseconds before ts_recv when matching engine sent data.
        sequence: Original message sequence number from venue.
        symbol: Futures contract symbol (e.g., SILG4, SILH4).
    """

    __tablename__ = "silver_futures_mbo"
    __table_args__ = (UniqueConstraint("ts_event", "order_id", "sequence", name="uq_silver_mbo_event"),)

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_recv = Column(DateTime, nullable=False)
    ts_event = Column(DateTime, nullable=False)
    rtype = Column(Integer, nullable=False)
    publisher_id = Column(Integer, nullable=False)
    instrument_id = Column(Integer, nullable=False)
    action = Column(String(1), nullable=False)
    side = Column(String(1))
    price = Column(DECIMAL(5, 9))
    size = Column(Integer, nullable=False)
    channel_id = Column(Integer)
    order_id = Column(BigInteger, nullable=False)
    flags = Column(Integer)
    ts_in_delta = Column(Integer)
    sequence = Column(Integer)
    symbol = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class SilverFuturesMBOModel(BaseModel):
    """Pydantic model for SilverFuturesMBO data validation.

    Args:
        ts_recv: Receive timestamp as datetime.
        ts_event: Event timestamp as datetime.
        rtype: Record type integer.
        publisher_id: Publisher identifier.
        instrument_id: Instrument identifier.
        action: Single character action type.
        side: Single character side indicator (optional).
        price: Price in fixed-point format (optional).
        size: Order size.
        channel_id: Channel identifier (optional).
        order_id: Order identifier.
        flags: Event flags (optional).
        ts_in_delta: Timestamp delta (optional).
        sequence: Sequence number (optional).
        symbol: Contract symbol string.

    Returns:
        Validated SilverFuturesMBOModel instance.
    """

    model_config = ConfigDict(from_attributes=True)

    ts_recv: datetime
    ts_event: datetime
    rtype: int
    publisher_id: int
    instrument_id: int
    action: str
    side: Optional[str]
    price: Optional[Decimal]
    size: int
    channel_id: Optional[int]
    order_id: int
    flags: Optional[int]
    ts_in_delta: Optional[int]
    sequence: Optional[int]
    symbol: str
    id: Optional[int] = None
