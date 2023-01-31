import os
from datetime import datetime
from decimal import Decimal

import requests

MAX_QUERY_PER_MINUTE = 5
polygon_api = "https://api.polygon.io"
api_key = os.getenv("POLYGON_API_KEY")


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API"""

    # NOTE: make sure you integrate the "limit" for each API endpoint into the individual call classes
    def query_all(url: str, payload: dict = None):
        results = requests.get(url, params=payload)
        # TODO: add pagination functionality, add the apikey to next_url query
        # TODO: add sleep functionality based on number of queries made
        return results


class HistoricalOptionsPrices(PolygonPaginator):
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
        self.ticker_list = self.options_tickers_constructor()

    def options_tickers_constructor(self) -> list[str]:
        """Function to return the master list of options contract tickers for the historical \
             query based on class attributes"""
        # NOTE: should check that strike prices are whole numbers or maybe 0.5 and nothing else.
        return

    def window_of_focus_dates(self):
        """"""
        return

    def historical_aggs(self, start_date: datetime, end_date: datetime, timespan: str = "day", multiplier: int = 1):
        """api call to the aggs endpoint

        Parameters:
            start_date (datetime): beginning of date range for historical query (date inclusive)
            end_date (datetime): ending of date range for historical query (date inclusive)
            timespan (str) : the default value is set to "day". \
                Options are ["minute", "hour", "day", "week", "month", "quarter", "year"]
            multiplier (int) : multiples of the timespan that should be included in the call. Defaults to 1

        """
        # TODO: make iterator for all options contracts
        agg_prices = []
        for ticker in self.ticker_list:
            url = polygon_api + f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
            ticker_results = self.query_all(url)
            agg_prices.append(ticker_results)

        return agg_prices

    def time_conversion(self):
        return
