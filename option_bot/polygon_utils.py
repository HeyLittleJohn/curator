import asyncio
import math
import time
from datetime import date, datetime
from enum import Enum

import numpy as np
from aiohttp import request
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from dateutil.relativedelta import relativedelta

from option_bot.exceptions import (
    ProfClientConnectionError,
    ProjBaseException,
    ProjClientResponseError,
    ProjIndexError,
    ProjTimeoutError,
)
from option_bot.proj_constants import log, POLYGON_API_KEY
from option_bot.utils import first_weekday_of_month, timestamp_to_datetime


class Timespans(Enum):
    minute = "minute"
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"


class PolygonPaginator(object):
    """API paginator interface for calls to the Polygon API. \
        It tracks queries made to the polygon API and calcs potential need for sleep

        self.query_all() is the universal function to query the api until all results have been returned.

        self.fetch() will ultimately result in the population of self.clean_results (a list of dicts with polygon data)
        self.clean_results will be converted into a generator that can be batch input into the db"""

    paginator_type = "Generic"

    MAX_QUERY_PER_SECOND = 99
    MAX_QUERY_PER_MINUTE = 4  # free api limits to 5 / min which is 4 when indexed at 0
    polygon_api = "https://api.polygon.io"

    def __init__(self):  # , query_count: int = 0):
        self.query_count = 0  # = query_count
        self.query_time_log = []
        self.results = []
        self.clean_results = []
        self.clean_data_generator = iter(())

    def _api_sleep_time(self) -> int:
        sleep_time = 60
        if len(self.query_time_log) > 2:
            a = timestamp_to_datetime(self.query_time_log[0]["query_timestamp"])
            b = timestamp_to_datetime(self.query_time_log[-1]["query_timestamp"])
            diff = math.ceil((b - a).total_seconds())
            sleep_time = diff if diff < sleep_time else sleep_time
        return sleep_time

    async def query_all(self, url: str, payload: dict = {}, overload=False, retry=False):
        payload["apiKey"] = POLYGON_API_KEY
        if (self.query_count >= self.MAX_QUERY_PER_MINUTE) or overload:
            await asyncio.sleep(self._api_sleep_time())
            self.query_count = 0
            self.query_time_log = []
        # elif self.query_count >= self.MAX_QUERY_PER_SECOND / 30:
        #     time.sleep(1)
        #     self.query_count = 0
        #     self.query_time_log = []
        # else:
        #     time.sleep(0.01)  # trying to keep things under 100 requests per second

        log.info(f"{url} {payload} overload:{overload}, retry attempt: {retry}")

        try:
            async with request(method="GET", url=url, params=payload) as response:
                log.info(f"status code: {response.status}")

                self.query_count += 1
                results = {"temp": "dict"}
                if response.status == 200:
                    results = await response.json()
                    self.query_time_log.append(
                        {"request_id": results.get("request_id"), "query_timestamp": time.time()}
                    )
                    self.results.append(results)  # convert this to Yield
                    next_url = results.get("next_url")
                    if next_url:
                        await self.query_all(next_url)
                elif response.status == 429:
                    await self.query_all(url, payload, overload=True)
                else:
                    response.raise_for_status()

        except (ClientResponseError, ProjClientResponseError) as e:
            log.error(e, exc_info=True)
            if not retry:
                log.error(f"ClientResponseError: args:{url}, {payload}, {retry}. Will retry")
                await self.query_all(url, payload, retry=True)
            else:
                raise ProjClientResponseError(f"failed retry, args:{url}, {payload}, {retry}")

        except (ClientConnectionError, ProfClientConnectionError) as e:
            log.error(e, exc_info=True)
            if not retry:
                log.error(f"ProjClientConnectionError. args: {url}, {payload}, {retry}")
                log.info("sleeping for one minute")
                await asyncio.sleep(60)
                log.info("retrying connection and query")
                await self.query_all(url, payload, retry=True)
            else:
                raise ProfClientConnectionError(f"failed to reconnect on retry. args: {url}, {payload}, {retry}")

        except (TimeoutError, ProjTimeoutError) as e:
            log.error(e, exc_info=True)
            if not retry:
                log.error(f"ProjTimeoutError. args: {url}, {payload}, {retry}")
                log.info("sleeping for one minute")
                await asyncio.sleep(60)
                log.info("retrying connection and query")
                await self.query_all(url, payload, retry=True)
            else:
                raise ProjTimeoutError(f"failed to reconnect on retry. args: {url}, {payload}, {retry}")

    def make_clean_generator(self):
        try:
            record_size = len(self.clean_results[0])
            batch_size = round(60000 / record_size)  # postgres input limit is ~65000
            for i in range(0, len(self.clean_results), batch_size):
                yield self.clean_results[i : i + batch_size]
        except IndexError as e:
            log.error(e, exc_info=True)
            if hasattr(self, "ticker"):
                t = self.ticker
            elif hasattr(self, "o_ticker"):
                t = self.o_ticker
            raise ProjIndexError(f"No results for ticker: {t}, using object: {self.paginator_type} ")

    async def query_data(self):
        """shell function to be overwritten by every inheriting class"""
        raise ProjBaseException("Function undefined in inherited class")

    def clean_data(self):
        """shell function to be overwritten by every inheriting class"""
        raise ProjBaseException("Function undefined in inherited class")

    async def fetch(self):
        await self.query_data()
        self.clean_data()
        self.clean_data_generator = self.make_clean_generator()


class StockMetaData(PolygonPaginator):
    """Object to query the Polygon API and retrieve information about listed stocks. \
        It can be used to query for a single individual ticker or to pull the entire corpus"""

    paginator_type = "StockMetaData"

    def __init__(self, ticker: str, all_: bool):
        self.ticker = ticker
        self.all_ = all_
        self.payload = {"active": "true", "market": "stocks", "limit": 1000}
        super().__init__()

    async def query_data(self):
        """"""
        url = self.polygon_api + "/v3/reference/tickers"
        if not self.all_:
            self.payload["ticker"] = self.ticker
        await self.query_all(url=url, payload=self.payload)

    def clean_data(self):
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


class HistoricalStockPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the underlying stock"""

    paginator_type = "StockPrices"

    def __init__(
        self,
        ticker: str,
        ticker_id: int,
        start_date: datetime,
        end_date: datetime,
        multiplier: int = 1,
        timespan: Timespans = Timespans.day,
        adjusted: bool = True,
    ):
        self.ticker = ticker
        self.ticker_id = ticker_id
        self.multiplier = multiplier
        self.timespan = timespan.value
        self.start_date = start_date.date()
        self.end_date = end_date.date()
        self.adjusted = "true" if adjusted else "false"
        super().__init__()

    async def query_data(self):
        url = (
            self.polygon_api
            + f"/v2/aggs/ticker/{self.ticker}/range/{self.multiplier}/{self.timespan}/{self.start_date}/{self.end_date}"
        )
        payload = {"adjusted": self.adjusted, "sort": "desc", "limit": 50000}
        await self.query_all(url, payload)

    def clean_data(self):
        key_mapping = {
            "v": "volume",
            "vw": "volume_weight_price",
            "c": "close_price",
            "o": "open_price",
            "h": "high_price",
            "l": "low_price",
            "t": "as_of_date",
            "n": "number_of_transactions",
            "otc": "otc",
        }
        for page in self.results:
            for record in page.get("results"):
                t = {key_mapping[key]: record.get(key) for key in key_mapping}
                t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                t["ticker_id"] = self.ticker_id
                self.clean_results.append(t)


class OptionsContracts(PolygonPaginator):
    """Object to query options contract tickers for a given underlying ticker based on given dates.
    It will pull all options contracts that exist for each underlying ticker as of the first business day of each month.
    Number of months are determined by `month_hist`"""

    paginator_type = "OptionsContracts"

    def __init__(
        self,
        ticker: str,
        ticker_id: int,
        months_hist: int = 24,
        all_: bool = False,
        ticker_id_lookup: dict | None = None,
    ):
        super().__init__()
        self.ticker = ticker
        self.ticker_id = ticker_id
        self.months_hist = months_hist
        self.base_dates = self._determine_base_dates()
        self.all_ = all_
        self.ticker_id_lookup = ticker_id_lookup

    def _determine_base_dates(self) -> list[datetime]:
        year_month_array = []
        counter = 0
        year = datetime.now().year
        month = datetime.now().month
        while counter <= self.months_hist:
            year_month_array.append(
                str(year) + "-" + str(month) if len(str(month)) > 1 else str(year) + "-0" + str(month)
            )
            if month == 1:
                month = 12
                year -= 1
            else:
                month -= 1
            counter += 1
        return [str(x) for x in first_weekday_of_month(np.array(year_month_array)).tolist()]

    async def query_data(self):
        url = self.polygon_api + "/v3/reference/options/contracts"
        payload = {"limit": 1000}
        if not self.all_:
            payload["underlying_ticker"] = self.ticker
        args_list = [[url, dict(payload, **{"as_of": date})] for date in self.base_dates]
        for args in args_list:
            await self.query_all(*args)

    def clean_data(self):
        key_mapping = {
            "ticker": "options_ticker",
            "expiration_date": "expiration_date",
            "strike_price": "strike_price",
            "contract_type": "contract_type",
            "shares_per_contract": "shares_per_contract",
            "primary_exchange": "primary_exchange",
            "exercise_style": "exercise_style",
            "cfi": "cfi",
        }
        for page in self.results:
            for record in page.get("results"):
                t = {key_mapping[key]: record.get(key) for key in key_mapping}
                t["underlying_ticker_id"] = (
                    self.ticker_id_lookup[record.get("underlying_ticker")] if self.all_ else self.ticker_id
                )
                self.clean_results.append(t)
        self.clean_results = list({v["options_ticker"]: v for v in self.clean_results}.values())
        # NOTE: the list(comprehension) above ascertains that all options_tickers are unique


class HistoricalOptionsPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker"""

    paginator_type = "OptionsPrices"

    def __init__(
        self,
        o_ticker: str,
        o_ticker_id: int,
        expiration_date: datetime,
        month_hist: int = 24,
        multiplier: int = 1,
        timespan: Timespans = Timespans.day,
        adjusted: bool = True,
    ):
        super().__init__()
        self.o_ticker = o_ticker
        self.o_ticker_id = o_ticker_id
        self.expiration_date = expiration_date.date()
        self.timespan = timespan.value
        self.multiplier = multiplier
        self.adjusted = "true" if adjusted else "false"
        self.month_hist = month_hist

    def _determine_start_end_dates(self, exp_date: date):
        end_date = datetime.now().date() if exp_date > datetime.now().date() else exp_date
        start_date = end_date - relativedelta(months=self.month_hist)
        return start_date, end_date

    async def query_data(self):
        """api call to the aggs endpoint

        Parameters:
            start_date (datetime): beginning of date range for historical query (date inclusive)
            end_date (datetime): ending of date range for historical query (date inclusive)
            timespan (str) : the default value is set to "day". \
                Options are ["minute", "hour", "day", "week", "month", "quarter", "year"]
            multiplier (int) : multiples of the timespan that should be included in the call. Defaults to 1

        """
        payload = {"adjusted": self.adjusted, "sort": "desc", "limit": 50000}
        url = (
            self.polygon_api
            + f"/v2/aggs/ticker/{self.o_ticker}/range/{self.multiplier}/{self.timespan}/"
            + f"{self._determine_start_end_dates(self.expiration_date)[0]}/"
            + f"{self._determine_start_end_dates(self.expiration_date)[1]}"
        )
        await self.query_all(url, payload)

    def clean_data(self):
        results_hash = {}
        key_mapping = {
            "v": "volume",
            "vw": "volume_weight_price",
            "c": "close_price",
            "o": "open_price",
            "h": "high_price",
            "l": "low_price",
            "t": "as_of_date",
            "n": "number_of_transactions",
        }
        for page in self.results:
            if page.get("results"):
                for record in page.get("results"):
                    t = {key_mapping[key]: record.get(key) for key in key_mapping}
                    t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                    if not results_hash.get(page["ticker"]):
                        results_hash[page["ticker"]] = []
                    results_hash[page["ticker"]].append(t)

        self._clean_record_hash(results_hash)

    def _clean_record_hash(self, results_hash: dict):
        for ticker in results_hash:
            self.clean_results.extend(
                [dict(x, **{"options_ticker_id": self.o_ticker_id}) for x in results_hash[ticker]]
            )
