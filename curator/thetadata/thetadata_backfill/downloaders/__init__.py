"""Data downloaders for ThetaData."""

from thetadata_backfill.downloaders.base import (
    BackfillResult,
    BaseDownloader,
    DownloadTask,
)
from thetadata_backfill.downloaders.equity import (
    EquityEODDownloader,
    EquityOHLCDownloader,
    EquityQuoteDownloader,
    EquityTradeDownloader,
)
from thetadata_backfill.downloaders.index import (
    IndexEODDownloader,
    IndexOHLCDownloader,
    IndexPriceDownloader,
)
from thetadata_backfill.downloaders.option import (
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
