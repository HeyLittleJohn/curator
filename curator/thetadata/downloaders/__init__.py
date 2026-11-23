"""Data downloaders for ThetaData."""

from curator.thetadata.downloaders.base import (
    BackfillResult,
    BaseDownloader,
    DownloadTask,
)
from curator.thetadata.downloaders.equity import (
    EquityEODDownloader,
    EquityOHLCDownloader,
    EquityQuoteDownloader,
    EquityTradeDownloader,
)
from curator.thetadata.downloaders.index import (
    IndexEODDownloader,
    IndexOHLCDownloader,
    IndexPriceDownloader,
)
from curator.thetadata.downloaders.option import (
    OptionGreeksDownloader,
    OptionOHLCDownloader,
    OptionOpenInterestDownloader,
    OptionTradeGreeksDownloader,
)

__all__ = [
    # Base
    "BackfillResult",
    "BaseDownloader",
    "DownloadTask",
    # Equity
    "EquityOHLCDownloader",
    "EquityTradeDownloader",
    "EquityQuoteDownloader",
    "EquityEODDownloader",
    # Option
    "OptionOHLCDownloader",
    "OptionTradeGreeksDownloader",
    "OptionGreeksDownloader",
    "OptionOpenInterestDownloader",
    # Index
    "IndexOHLCDownloader",
    "IndexPriceDownloader",
    "IndexEODDownloader",
]
