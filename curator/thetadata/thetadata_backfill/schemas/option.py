"""Pydantic schemas for options data from ThetaData API.

Includes all first-order and second-order Greeks.
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class OptionRight(str, Enum):
    """Option right (call or put)."""
    
    CALL = "CALL"
    PUT = "PUT"
    
    @classmethod
    def from_str(cls, value: str) -> "OptionRight":
        """Parse from string, case-insensitive."""
        return cls(value.upper())


class OptionContract(BaseModel):
    """Option contract identifier."""
    
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight


class OptionExpiration(BaseModel):
    """Expiration date from expirations list endpoint."""
    
    expiration: date


class OptionStrike(BaseModel):
    """Strike from strikes list endpoint."""
    
    strike: Decimal


class OptionOHLCResponse(BaseModel):
    """OHLC data response for options."""
    
    timestamp: datetime
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    count: Optional[int] = None


class OptionTradeResponse(BaseModel):
    """Trade data response for options."""
    
    timestamp: datetime
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    sequence: int
    size: int
    condition: int
    price: Decimal
    exchange: Optional[int] = None
    ext_condition1: Optional[int] = None
    ext_condition2: Optional[int] = None
    ext_condition3: Optional[int] = None
    ext_condition4: Optional[int] = None


class OptionQuoteResponse(BaseModel):
    """Quote data response for options."""
    
    timestamp: datetime
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    bid_size: int
    bid_exchange: Optional[int] = None
    bid: Decimal
    bid_condition: Optional[int] = None
    ask_size: int
    ask_exchange: Optional[int] = None
    ask: Decimal
    ask_condition: Optional[int] = None


class OptionGreeksResponse(BaseModel):
    """Full Greeks data response including first and second order Greeks."""
    
    # Contract info
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    
    # Timestamps
    timestamp: datetime
    underlying_timestamp: Optional[datetime] = None
    
    # Prices
    bid: Decimal
    ask: Decimal
    underlying_price: Optional[Decimal] = None
    
    # Implied volatility
    implied_vol: Optional[Decimal] = None
    iv_error: Optional[Decimal] = None
    
    # First-order Greeks
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    rho: Optional[Decimal] = None
    epsilon: Optional[Decimal] = None
    lambda_: Optional[Decimal] = Field(default=None, alias="lambda")
    
    # Second-order Greeks
    vanna: Optional[Decimal] = None
    charm: Optional[Decimal] = None
    vomma: Optional[Decimal] = None
    veta: Optional[Decimal] = None
    color: Optional[Decimal] = None
    zomma: Optional[Decimal] = None
    speed: Optional[Decimal] = None
    ultima: Optional[Decimal] = None
    
    class Config:
        populate_by_name = True


class OptionTradeGreeksResponse(BaseModel):
    """Trade with Greeks data response."""
    
    # Contract info
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    
    # Trade data
    timestamp: datetime
    sequence: int
    size: int
    condition: int
    price: Decimal
    exchange: Optional[int] = None
    ext_condition1: Optional[int] = None
    ext_condition2: Optional[int] = None
    ext_condition3: Optional[int] = None
    ext_condition4: Optional[int] = None
    
    # Underlying info
    underlying_timestamp: Optional[datetime] = None
    underlying_price: Optional[Decimal] = None
    
    # Implied volatility
    implied_vol: Optional[Decimal] = None
    iv_error: Optional[Decimal] = None
    
    # First-order Greeks
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    rho: Optional[Decimal] = None
    epsilon: Optional[Decimal] = None
    lambda_: Optional[Decimal] = Field(default=None, alias="lambda")
    
    # Second-order Greeks
    vanna: Optional[Decimal] = None
    charm: Optional[Decimal] = None
    vomma: Optional[Decimal] = None
    veta: Optional[Decimal] = None
    color: Optional[Decimal] = None
    zomma: Optional[Decimal] = None
    speed: Optional[Decimal] = None
    ultima: Optional[Decimal] = None
    
    class Config:
        populate_by_name = True


class OptionOpenInterestResponse(BaseModel):
    """Open interest data response."""
    
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    date: date
    open_interest: int


class OptionEODResponse(BaseModel):
    """End-of-day report for options."""
    
    symbol: str
    expiration: date
    strike: Decimal
    right: OptionRight
    created: datetime
    last_trade: Optional[datetime] = None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    count: int = 0
    open_interest: Optional[int] = None
    bid: Optional[Decimal] = None
    ask: Optional[Decimal] = None
