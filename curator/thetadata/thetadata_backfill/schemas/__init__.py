"""Pydantic schemas for ThetaData API responses."""

from thetadata_backfill.schemas.equity import (
    EquityDate,
    EquityEODResponse,
    EquityOHLCResponse,
    EquityQuoteResponse,
    EquitySymbol,
    EquityTradeResponse,
)
from thetadata_backfill.schemas.index import (
    IndexDate,
    IndexEODResponse,
    IndexOHLCResponse,
    IndexPriceResponse,
    IndexSymbol,
)
from thetadata_backfill.schemas.option import (
    OptionContract,
    OptionEODResponse,
    OptionExpiration,
    OptionGreeksResponse,
    OptionOHLCResponse,
    OptionOpenInterestResponse,
    OptionQuoteResponse,
    OptionRight,
    OptionStrike,
    OptionTradeGreeksResponse,
    OptionTradeResponse,
)

__all__ = [
    # Equity
    "EquitySymbol",
    "EquityDate",
    "EquityOHLCResponse",
    "EquityTradeResponse",
    "EquityQuoteResponse",
    "EquityEODResponse",
    # Option
    "OptionContract",
    "OptionRight",
    "OptionExpiration",
    "OptionStrike",
    "OptionOHLCResponse",
    "OptionTradeResponse",
    "OptionQuoteResponse",
    "OptionGreeksResponse",
    "OptionTradeGreeksResponse",
    "OptionOpenInterestResponse",
    "OptionEODResponse",
    # Index
    "IndexSymbol",
    "IndexDate",
    "IndexOHLCResponse",
    "IndexPriceResponse",
    "IndexEODResponse",
]
