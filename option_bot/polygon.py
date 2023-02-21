import asyncio
import math
import time
from datetime import datetime
from decimal import Decimal

import requests
from proj_constants import log, POLYGON_API_KEY
from utils import timestamp_to_datetime  # ,first_weekday_of_month


# from multiprocessing import Lock, Pool


# NOTE: perhaps rather than inherit, make these subclasses with the overall paginator \
# keeping track of all your queries/logs so that it sleeps appropriately


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API. \
        It tracks queries made to the polygon API and calcs potential need for sleep"""

    MAX_QUERY_PER_MINUTE = 4  # free api limits to 5 / min which is 4 when indexed at 0
    polygon_api = "https://api.polygon.io"

    def __init__(self):  # , query_count: int = 0):
        self.query_count = 0  # = query_count
        self.query_time_log = []
        self.results = []
        self.clean_results = []

    def _api_sleep_time(self) -> int:
        sleep_time = 60
        if len(self.query_time_log) > 2:
            a = timestamp_to_datetime(self.query_time_log[0]["query_timestamp"])
            b = timestamp_to_datetime(self.query_time_log[-1]["query_timestamp"])
            diff = math.ceil((b - a).total_seconds())
            sleep_time = diff if diff < sleep_time else sleep_time
        return sleep_time

    async def query_all(self, url: str, payload: dict = {}, overload=False):
        payload["apiKey"] = POLYGON_API_KEY
        if (self.query_count >= self.MAX_QUERY_PER_MINUTE) or overload:
            await asyncio.sleep(self._api_sleep_time())
            self.query_count = 0
            self.query_time_log = []

        log.info(f"{url} {payload} overload:{overload}")
        response = requests.get(url, params=payload)
        log.info(f"status code: {response.status_code}")

        self.query_count += 1

        if response.status_code == 200:
            results = response.json()
            self.query_time_log.append({"request_id": results.get("request_id"), "query_timestamp": time.time()})
            self.results.append(results)
            next_url = results.get("next_url")
            if next_url:
                await self.query_all(next_url)
        elif response.status_code == 429:
            await self.query_all(url, payload, overload=True)
        else:
            response.raise_for_status()


class HistoricalStockPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker"""

    def __init__(
        self,
        ticker: str,
    ):
        pass


class StockMetaData(PolygonPaginator):
    """Object to query the Polygon API and retrieve information about listed stocks. \
        It can be used to query for a single individual ticker or to pull the entire corpus"""

    def __init__(self, ticker: str, all_: bool):
        self.ticker = ticker
        self.all_ = all_
        self.payload = {"active": True, "market": "stocks", "limit": 1000}
        super().__init__()

    async def query_data(self):
        url = self.polygon_api + "/v3/reference/tickers"
        if not self.all_:
            self.payload["ticker"] = self.ticker
        await self.query_all(url=url, payload=self.payload)

    def clean_metadata(self):
        selected_keys = [
            "ticker",
            "name",
            "type",
            "active",
            "market",
            "locale",
            "primary_exchange",
            "currency_name",
            "cik",
        ]
        for result_list in self.results:
            for ticker in result_list["results"]:
                t = {x: ticker.get(x) for x in selected_keys}
                self.clean_results.append(t)

    def clean_data_generator(self):
        batch_size = 5000
        for i in range(0, len(self.clean_results), batch_size):
            clean_results_batch = self.clean_results[i : i + batch_size]
            yield clean_results_batch


class OptionsContracts(PolygonPaginator):
    """Object to query options contract tickers for a given underlying ticker based on given dates.

    Attributes:
        ticker: str
            the underlying stock ticker
        base_date: [datetime]
            the date that is the basis for current observations. \
            In other words: the date at which you are looking at the chain of options data
        current_price: decimal
            The current price of the underlying ticker
    """

    def __init__(self, ticker: str, base_date: datetime, current_price: Decimal, exp_date: tuple[datetime, datetime]):
        self.ticker = ticker
        self.base_date = base_date
        self.current_price = current_price
        self.strike_range = self._determine_strike_range()

    def _determine_strike_range(self) -> tuple[int, int]:
        """function to determine strike range based on stock snapshot

        Returns:
            strike_range: tuple of ints with the max and min strike prices of interest
        """
        strike_range = ()
        return strike_range


class HistoricalOptionsPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker

    Attributes:
        options_tickers: List[str]
            the options contract tickers

        exp_date: [datetime, datetime]
            the range of option expiration dates to be queried

        strike_price: decimal
            the strike price range want to include in our queries

    Note:
        exp_date and strike_price are inclusive ranges
    """

    def __init__(
        self,
        tickers: list[str],
        base_date: datetime,
    ):
        self.o_tickers = tickers
        self.base_date = base_date  # as_of date
        self.ticker_list = self._options_tickers_constructor()

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


# TODO: figure out how you are going to handle data refreshing. Simply update the whole history?
# Or append and find a way to adjust for splits?

# TODO: Make all functions with these classes async ready, and make the classes ready to manage multiple workers
