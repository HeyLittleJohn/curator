from datetime import datetime
from decimal import Decimal

import requests

MAX_QUERY_PER_MINUTE = 5
polygon_api = "https://api.polygon.io"


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API"""


# NOTE: make sure you integrate the "limit" for each API endpoint into the individual call classes


class HistoricalOptionsPrices(object):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker

    Attributes:
        ticker: str
            the underlying stock ticker
        current_price: decimal
            The current price of the underlying ticker
        exp_date: [datetime, datetime]
            the range of option expiration dates to be queried
        base_date: [datetime]
            the date that is the basis for current observations. \
            In other words: the date at which you are looking at the chain of options data
        strike_price: decimal
            the strike price range want to include in our queries

    Note:
        exp_date and strike_price are inclusive ranges
    """

    def __init__(
        self,
        ticker: str,
        exp_date: tuple[datetime, datetime],
        base_date: datetime,
        strike_price: tuple[int, int],
        current_price: Decimal,
    ):
        self.ticker = ticker
        self.current_price = current_price
        self.exp_date = exp_date
        self.base_date = base_date
        self.strike_price = strike_price

    def options_tickers_constructor(self) -> list[str]:
        """Function to generate the master list of options contract tickers for the historical query"""
        # NOTE: should check that strike prices are whole numbers or maybe 0.5 and nothing else.
        return

    def window_of_focus_dates(self):
        """"""
        return

    def historical_aggs(self):
        return

    def time_conversion(self):
        return
