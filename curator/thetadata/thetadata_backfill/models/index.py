"""SQLAlchemy models for index data."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import BigInteger, Date, DateTime, Index, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from thetadata_backfill.models.base import Base, TimestampMixin


class IndexOHLC(Base, TimestampMixin):
    """Index OHLC (Open/High/Low/Close) data."""
    
    __tablename__ = "index_ohlc"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    
    __table_args__ = (
        Index("ix_index_ohlc_symbol_timestamp", "symbol", "timestamp", unique=True),
    )


class IndexPrice(Base, TimestampMixin):
    """Index price levels (tick data)."""
    
    __tablename__ = "index_price"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    
    __table_args__ = (
        Index("ix_index_price_symbol_timestamp", "symbol", "timestamp"),
    )


class IndexEOD(Base, TimestampMixin):
    """Index end-of-day report."""
    
    __tablename__ = "index_eod"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    report_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_trade: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    
    __table_args__ = (
        Index("ix_index_eod_symbol_date", "symbol", "trade_date", unique=True),
    )
