"""Pydantic schemas for equity data from ThetaData API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel


class EquitySymbol(BaseModel):
    """Symbol from symbols list endpoint."""
    
    symbol: str


class EquityDate(BaseModel):
    """Date from dates list endpoint."""
    
    date: date


class EquityOHLCResponse(BaseModel):
    """OHLC data response from ThetaData API."""
    
    timestamp: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    count: Optional[int] = None


class EquityTradeResponse(BaseModel):
    """Trade data response from ThetaData API."""
    
    timestamp: datetime
    symbol: Optional[str] = None
    sequence: int
    size: int
    condition: int
    price: Decimal
    exchange: Optional[int] = None
    ext_condition1: Optional[int] = None
    ext_condition2: Optional[int] = None
    ext_condition3: Optional[int] = None
    ext_condition4: Optional[int] = None


class EquityQuoteResponse(BaseModel):
    """Quote data response from ThetaData API."""
    
    timestamp: datetime
    symbol: Optional[str] = None
    bid_size: int
    bid_exchange: Optional[int] = None
    bid: Decimal
    bid_condition: Optional[int] = None
    ask_size: int
    ask_exchange: Optional[int] = None
    ask: Decimal
    ask_condition: Optional[int] = None


class EquityEODResponse(BaseModel):
    """End-of-day report response from ThetaData API."""
    
    created: datetime
    last_trade: Optional[datetime] = None
    symbol: Optional[str] = None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    count: int = 0
    bid_size: Optional[int] = None
    bid_exchange: Optional[int] = None
    bid: Optional[Decimal] = None
    bid_condition: Optional[int] = None
    ask_size: Optional[int] = None
    ask_exchange: Optional[int] = None
    ask: Optional[Decimal] = None
    ask_condition: Optional[int] = None
