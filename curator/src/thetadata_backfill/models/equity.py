"""SQLAlchemy models for equity data."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from thetadata_backfill.models.base import Base, TimestampMixin


class EquityOHLC(Base, TimestampMixin):
    """Equity OHLC (Open/High/Low/Close) data."""
    
    __tablename__ = "equity_ohlc"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    
    __table_args__ = (
        Index("ix_equity_ohlc_symbol_timestamp", "symbol", "timestamp", unique=True),
    )


class EquityTrade(Base, TimestampMixin):
    """Equity trade data."""
    
    __tablename__ = "equity_trade"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
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
        Index("ix_equity_trade_symbol_timestamp", "symbol", "timestamp"),
        Index("ix_equity_trade_symbol_sequence", "symbol", "sequence", unique=True),
    )


class EquityQuote(Base, TimestampMixin):
    """Equity quote (NBBO) data."""
    
    __tablename__ = "equity_quote"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
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
        Index("ix_equity_quote_symbol_timestamp", "symbol", "timestamp"),
    )


class EquityEOD(Base, TimestampMixin):
    """Equity end-of-day report."""
    
    __tablename__ = "equity_eod"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    trade_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    report_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_trade: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bid: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    bid_size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    ask: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    ask_size: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    
    __table_args__ = (
        Index("ix_equity_eod_symbol_date", "symbol", "trade_date", unique=True),
    )
