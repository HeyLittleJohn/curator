import math
import os
import time
from datetime import datetime
from decimal import Decimal

import requests
from utils import timestamp_to_datetime  # ,first_weekday_of_month

api_key = os.getenv("POLYGON_API_KEY")


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API. \
        It tracks queries made to the polygon API and calcs potential need for sleep"""

    MAX_QUERY_PER_MINUTE = 4  # free api limits to 5 / min which is 4 when indexed at 0
    polygon_api = "https://api.polygon.io"

    def __init__(self, query_count: int = 0):
        self.query_count = query_count
        self.results = []
        self.query_time_log = []

    def _api_sleep_time(self) -> int:
        sleep_time = 60
        if len(self.query_time_log) > 2:
            a = timestamp_to_datetime(self.query_time_log[0]["query_timestamp"])
            b = timestamp_to_datetime(self.query_time_log[-1]["query_timestamp"])
            sleep_time = math.ceil((b - a).total_seconds())
        return sleep_time

    def query_all(self, url: str, payload: dict = {}):
        payload["apiKey"] = api_key
        if self.query_count >= self.MAX_QUERY_PER_MINUTE:
            time.sleep(self.api_sleep_time())
            self.query_count = 0
        response = requests.get(url, params=payload)
        self.query_time_log.append({"request_id": response.get("request_id"), "query_timestamp": time.time()})
        self.query_count += 1
        response.raise_for_status()
        if response.status_code == 200:
            self.results.append(response.json())
            next_url = response.get("next_url")
            if next_url:
                self.query_all(next_url)


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
        self.underlying_ticker = ticker
        self.current_price = current_price
        self.exp_date = exp_date
        self.base_date = base_date  # as_of date
        self.strike_price = strike_price
        self.ticker_list = self._options_tickers_constructor()

    def _options_tickers_constructor(self) -> list[str]:
        """Function to return the master list of options contract tickers for the historical \
             query based on class attributes"""
        # NOTE: should check that strike prices are whole numbers or maybe 0.5 and nothing else.
        return ["test_ticker"]

    def _window_of_focus_dates(self):
        """"""
        return

    def _time_conversion(self):
        return

    def _clean_api_results(self, ticker: str) -> list[dict]:
        clean_results = []
        return clean_results

    def get_historical_prices(
        self, start_date: datetime, end_date: datetime, timespan: str = "day", multiplier: int = 1
    ):
        """api call to the aggs endpoint

        Parameters:
            start_date (datetime): beginning of date range for historical query (date inclusive)
            end_date (datetime): ending of date range for historical query (date inclusive)
            timespan (str) : the default value is set to "day". \
                Options are ["minute", "hour", "day", "week", "month", "quarter", "year"]
            multiplier (int) : multiples of the timespan that should be included in the call. Defaults to 1

        """
        # TODO: implement async/await so it pulls and processes more quickly
        # TODO: pull the function inputs from self, not as inputs
        self.hist_prices = []
        for ticker in self.ticker_list:
            url = self.polygon_api + f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
            self.query_all(url)
            ticker_results = self._clean_api_results(ticker)
            self.hist_prices.append(ticker_results)
