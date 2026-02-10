"""Data downloaders for ThetaData."""

from thetadata_backfill.downloaders.base import (
    BackfillResult,
    BaseDownloader,
    DownloadTask,
)
from thetadata_backfill.downloaders.equity import (
    EquityOHLCDownloader,
    EquityTradeDownloader,
    EquityQuoteDownloader,
    EquityEODDownloader,
)
from thetadata_backfill.downloaders.option import (
    OptionOHLCDownloader,
    OptionTradeGreeksDownloader,
    OptionGreeksDownloader,
    OptionOpenInterestDownloader,
)
from thetadata_backfill.downloaders.index import (
    IndexOHLCDownloader,
    IndexPriceDownloader,
    IndexEODDownloader,
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
