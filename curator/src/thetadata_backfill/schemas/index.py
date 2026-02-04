"""Pydantic schemas for index data from ThetaData API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel


class IndexSymbol(BaseModel):
    """Symbol from index symbols list endpoint."""
    
    symbol: str


class IndexDate(BaseModel):
    """Date from index dates list endpoint."""
    
    date: date


class IndexOHLCResponse(BaseModel):
    """OHLC data response for indices."""
    
    timestamp: datetime
    symbol: Optional[str] = None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[int] = None
    count: Optional[int] = None


class IndexPriceResponse(BaseModel):
    """Price report for indices."""
    
    timestamp: datetime
    symbol: Optional[str] = None
    price: Decimal


class IndexEODResponse(BaseModel):
    """End-of-day report for indices."""
    
    created: datetime
    last_trade: Optional[datetime] = None
    symbol: Optional[str] = None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = 0
    count: int = 0
    bid_size: Optional[int] = None
    bid_exchange: Optional[int] = None
    bid: Optional[Decimal] = None
    bid_condition: Optional[int] = None
    ask_size: Optional[int] = None
    ask_exchange: Optional[int] = None
    ask: Optional[Decimal] = None
    ask_condition: Optional[int] = None
