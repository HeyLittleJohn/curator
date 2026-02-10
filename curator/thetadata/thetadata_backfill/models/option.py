"""SQLAlchemy models for options data with full Greeks support."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Date, DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from thetadata_backfill.models.base import Base, TimestampMixin


class OptionOHLC(Base, TimestampMixin):
    """Option OHLC (Open/High/Low/Close) data."""
    
    __tablename__ = "option_ohlc"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)  # CALL or PUT
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    
    __table_args__ = (
        Index(
            "ix_option_ohlc_contract_timestamp",
            "symbol", "expiration", "strike", "right", "timestamp",
            unique=True,
        ),
        Index("ix_option_ohlc_symbol", "symbol"),
        Index("ix_option_ohlc_expiration", "expiration"),
    )


class OptionTrade(Base, TimestampMixin):
    """Option trade data."""
    
    __tablename__ = "option_trade"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    condition: Mapped[int] = mapped_column(nullable=False)
    exchange: Mapped[Optional[int]] = mapped_column(nullable=True)
    ext_condition1: Mapped[Optional[int]] = mapped_column(nullable=True)
    ext_condition2: Mapped[Optional[int]] = mapped_column(nullable=True)
    ext_condition3: Mapped[Optional[int]] = mapped_column(nullable=True)
    ext_condition4: Mapped[Optional[int]] = mapped_column(nullable=True)
    
    __table_args__ = (
        Index("ix_option_trade_contract_timestamp", "symbol", "expiration", "strike", "right", "timestamp"),
        Index("ix_option_trade_contract_sequence", "symbol", "expiration", "strike", "right", "sequence", unique=True),
    )


class OptionQuote(Base, TimestampMixin):
    """Option quote (NBBO) data."""
    
    __tablename__ = "option_quote"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bid: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    bid_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bid_exchange: Mapped[Optional[int]] = mapped_column(nullable=True)
    bid_condition: Mapped[Optional[int]] = mapped_column(nullable=True)
    ask: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    ask_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ask_exchange: Mapped[Optional[int]] = mapped_column(nullable=True)
    ask_condition: Mapped[Optional[int]] = mapped_column(nullable=True)
    
    __table_args__ = (
        Index("ix_option_quote_contract_timestamp", "symbol", "expiration", "strike", "right", "timestamp"),
    )


class OptionGreeks(Base, TimestampMixin):
    """Option Greeks data - includes all first and second order Greeks."""
    
    __tablename__ = "option_greeks"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    
    # Contract info
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    
    # Timestamps
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    underlying_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Prices
    bid: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    ask: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    underlying_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # Implied volatility
    implied_vol: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    iv_error: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # First-order Greeks
    delta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    gamma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    theta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    vega: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    rho: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    epsilon: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    lambda_val: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # Second-order Greeks
    vanna: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    charm: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    vomma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    veta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    color: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    zomma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    speed: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    ultima: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    __table_args__ = (
        Index("ix_option_greeks_contract_timestamp", "symbol", "expiration", "strike", "right", "timestamp"),
        Index("ix_option_greeks_symbol", "symbol"),
        Index("ix_option_greeks_expiration", "expiration"),
    )


class OptionTradeGreeks(Base, TimestampMixin):
    """Option trade with Greeks snapshot."""
    
    __tablename__ = "option_trade_greeks"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    
    # Contract info
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    
    # Trade data
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    condition: Mapped[int] = mapped_column(nullable=False)
    exchange: Mapped[Optional[int]] = mapped_column(nullable=True)
    
    # Underlying info
    underlying_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    underlying_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # Implied volatility
    implied_vol: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    iv_error: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # First-order Greeks
    delta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    gamma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    theta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    vega: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    rho: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    epsilon: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    lambda_val: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    # Second-order Greeks
    vanna: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    charm: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    vomma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    veta: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    color: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    zomma: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    speed: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    ultima: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    __table_args__ = (
        Index("ix_option_trade_greeks_contract_seq", "symbol", "expiration", "strike", "right", "sequence", unique=True),
        Index("ix_option_trade_greeks_symbol", "symbol"),
    )


class OptionOpenInterest(Base, TimestampMixin):
    """Option open interest data (EOD)."""
    
    __tablename__ = "option_open_interest"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_interest: Mapped[int] = mapped_column(BigInteger, nullable=False)
    
    __table_args__ = (
        Index(
            "ix_option_oi_contract_date",
            "symbol", "expiration", "strike", "right", "trade_date",
            unique=True,
        ),
    )


class OptionEOD(Base, TimestampMixin):
    """Option end-of-day report."""
    
    __tablename__ = "option_eod"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    expiration: Mapped[date] = mapped_column(Date, nullable=False)
    strike: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    right: Mapped[str] = mapped_column(String(4), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    report_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_trade: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    open_interest: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    bid: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    ask: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    
    __table_args__ = (
        Index(
            "ix_option_eod_contract_date",
            "symbol", "expiration", "strike", "right", "trade_date",
            unique=True,
        ),
    )
