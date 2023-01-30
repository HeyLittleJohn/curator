from datetime import datetime
from typing import Tuple

import requests

MAX_QUERY_PER_MINUTE = 5


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API"""


class HistoricalOptionsPrices(object):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker

    Args:
        ticker: the underlying stock ticker
        exp_date: the expiration date of the option
        base_date: the date that is the basis for current observations. \
            In other words: the date at which you are looking at the chain of options data
        strike_price: the strike price range want to include in our queries

    Note:
        exp_date and strike_price are inclusive ranges
    """

    def __init__(
        self, ticker: str, exp_date: Tuple[datetime, datetime], base_date: datetime, strike_price: Tuple[int, int]
    ):
        self.ticker = ticker
        self.exp_date = exp_date
        self.base_date = base_date
        self.strike_price = strike_price

    def options_tickers_constructor(self):
        """Function to generate the master list of options contract tickers"""

        return

    def window_of_focus_dates(self):
        return

    def historical_aggs(self):
        return

    def time_conversion(self):
        return
