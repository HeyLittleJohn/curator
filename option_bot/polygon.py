from datetime import datetime
from typing import Tuple

import requests


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API"""


class HistoricalOptionsPrices(object):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker

    Args:
        ticker: the underlying stock ticker
        exp_date: the expiration date of the option
        strike_price:

    Note:
        exp_date and strike_price are inclusive ranges
    """

    def __init__(self, ticker: str, exp_date: Tuple[datetime, datetime], strike_price: Tuple[int, int]):
        self.ticker = ticker
        self.exp_date = exp_date
        self.strike_price = strike_price

    def options_ticker_constructor(self):
        return

    def historical_aggs(self):
        return

    def time_conversion(self):
        return
