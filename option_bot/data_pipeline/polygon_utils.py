import asyncio
from abc import ABC, abstractmethod
from datetime import date, datetime
from enum import Enum

import numpy as np
import pandas as pd
from aiohttp import ClientSession
from aiohttp.client_exceptions import (
    ClientConnectionError,
    ClientConnectorError,
    ClientResponseError,
)
from data_pipeline.exceptions import ProjAPIError, ProjAPIOverload
from dateutil.relativedelta import relativedelta
from db_tools.utils import OptionTicker

from option_bot.proj_constants import BASE_DOWNLOAD_PATH, POLYGON_API_KEY, POLYGON_BASE_URL, log
from option_bot.utils import (
    first_weekday_of_month,
    string_to_date,
    timestamp_now,
    trading_days_in_range,
    write_api_data_to_file,
)


class Timespans(Enum):
    second = "second"
    minute = "minute"
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"


class PolygonPaginator(ABC):
    """API paginator interface for calls to the Polygon API. \
        It tracks queries made to the polygon API and calcs potential need for sleep

        self.query_all() is the universal function to query the api until all results have been returned.

        self.fetch() will ultimately result in the population of self.clean_results (a list of dicts with polygon data)
        self.clean_results will be converted into a generator that can be batch input into the db"""

    paginator_type = "Generic"

    MAX_QUERY_PER_SECOND = 99
    # MAX_QUERY_PER_MINUTE = 4  # free api limits to 5 / min which is 4 when indexed at 0

    def __init__(self):
        self.clean_results = []
        self.clean_data_generator = iter(())

    def _api_sleep_time(self) -> int:
        """Returns the time to sleep based if the endpoint returns a 429 error"""
        return 60

    def _clean_url(self, url: str) -> str:
        """Clean the url to remove the base url"""
        return url.replace(POLYGON_BASE_URL, "")

    def _clean_o_ticker(self, o_ticker: str) -> str:
        """Clean the options ticker to remove the prefix to make it compatible as a file name"""
        return o_ticker.split(":")[1]

    def _download_path(self, path: str, file_name: str) -> str:
        """Returns the path and filename where API data will be downloaded
        Args:
            path: str of the path where file should be downloaded.
                Ought to include the ticker in the path
            file_name: identifying name including timestamp, option_ticker, etc
        Returns:
            str of the download path,
            str of the file name"""
        return f"{BASE_DOWNLOAD_PATH}/{self.paginator_type}/{path}/", f"{file_name}.json"

    async def _execute_request(self, session: ClientSession, url: str, payload: dict = {}) -> tuple[int, dict]:
        """Execute the request and return the response status code and json response
        Args:
            session: aiohttp ClientSession
            url: url to query
            payload: dict of query params
        Returns:
            (status_code, json_response)
                status_code: int of the response status code
                json_response: dict of the json response
        """
        payload["apiKey"] = POLYGON_API_KEY
        async with session.request(method="GET", url=url, params=payload) as response:
            status_code = response.status
            if status_code == 429:
                raise ProjAPIOverload(f"API Overload, 429 error code: {url} {payload}")
            elif status_code >= 400:
                raise ProjAPIError(f"API Error, status code {status_code}: {url} {payload}")
            json_response = await response.json() if status_code == 200 else {}
            return (status_code, json_response)

    async def _query_all(
        self, session: ClientSession, url: str, payload: dict = {}, limit: bool = False
    ) -> list[dict]:
        """Query the API until all results have been returned
        Args:
            session: aiohttp ClientSession
            url: url to query
            payload: dict of query params
            limit: bool to determine if query should be limited to the first page of results. Default set to false

        Returns:
            list of dicts of the json response
        """
        results = []
        status = 0
        retry = False

        while True:
            try:
                status, response = await self._execute_request(session, url, payload)

            except ProjAPIOverload as e:
                log.exception(e, extra={"context": "Going to sleep for 60 seconds..."} if not retry else {})
                status = 1

            except ProjAPIError as e:
                log.exception(e)
                status = 2

            except (ClientConnectionError, ClientConnectorError, ClientResponseError) as e:
                log.exception(e, extra={"context": "Connection Lost! Going to sleep for 45 seconds..."})
                status = 3

            except asyncio.TimeoutError as e:
                log.exception(
                    e,
                    extra={"context": "Event loop request timed out! Consider decreasing concurrency"},
                    exc_info=False,
                )
                status = 4

            except Exception as e:
                log.exception(e, extra={"context": "Unexpected Error while querying the API"})
                status = 5

            finally:
                if status == 200:
                    results.append(response)
                    if response.get("next_url") and not limit:
                        url = self._clean_url(response["next_url"])
                        payload = {}
                        status = 0
                        retry = False
                    else:
                        break

                elif status == 1 and retry is False:
                    await asyncio.sleep(self._api_sleep_time())
                    retry = True
                    status = 0

                elif status in (2, 4) and retry is False:
                    retry = True
                    status = 0

                elif status == 3 and retry is False:
                    await asyncio.sleep(45)
                    retry = True
                    status = 0

                else:
                    break

        return results

    async def download_data(self, url: str, payload: dict, ticker: str, session: ClientSession = None):
        """query_data() is an api to call the _query_all() function.
        Downloaded data is then saved to json.

        Overwrite this to customize the way to insert the ticker_id into the query results
        """
        log.info(f"Downloading data for {ticker}")
        log.debug(f"Downloading data for {ticker} with url: {url} and payload: {payload}")
        results = await self._query_all(session, url, payload)
        log.info(f"Writing data for {ticker} to file")
        write_api_data_to_file(results, *self._download_path(ticker, str(timestamp_now())))

    @abstractmethod
    def generate_request_args(self, args_data):
        """Requiring a generate_request_args() function to be overwritten by every inheriting class

        This function builds the list of args that get passed to the `download_data()` function in the process pool.

        Look at the download_data inputs to see the reqs for this function's outputs

        Args:
            args_data: this function requires an iterable passed with the inputs used to generate url_args

        Returns:
            url_args: list(tuple) of the (url, payload, and ticker_id) for each request \
                defined by each classes's download_data() function"""


class StockMetaData(PolygonPaginator):
    """Object to query the Polygon API and retrieve information about listed stocks. \
        It can be used to query for a single individual ticker or to pull the entire corpus"""

    paginator_type = "StockMetaData"

    def __init__(self, tickers: list[str], all_: bool):
        self.tickers = tickers
        self.all_ = all_
        self.url_base = "/v3/reference/tickers"
        self.payload = {"active": "true", "market": "stocks", "limit": 1000}
        super().__init__()

    def generate_request_args(self, args_data: list[str] = []):
        """Generate the urls to query the Polygon API.

        Returns:
        urls: list(tuple), each tuple contains the url, the payload for the request and an empty string"""
        if not self.all_:
            urls = [
                (self.url_base, dict(self.payload, **{"ticker": ticker}), ticker) for ticker in self.tickers
            ]
        else:
            urls = [(self.url_base, self.payload, "")]
        return urls


# class StockDetails(StockMetaData):
# NOTE: this class will hit the same endpoint but will add `/{ticker}?{date}` for historical data
# add if then logic to exception handling to check the ticker events endpoint if ticker not found


class HistoricalStockPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the underlying stock"""

    paginator_type = "StockPrices"

    def __init__(
        self,
        start_date: datetime,
        end_date: datetime,
        multiplier: int = 1,
        timespan: Timespans = Timespans.hour,
        adjusted: bool = True,
    ):
        self.multiplier = multiplier
        self.timespan = timespan.value
        self.start_date = start_date.date()
        self.end_date = end_date.date()
        self.adjusted = "true" if adjusted else "false"
        self.payload = {"adjusted": self.adjusted, "sort": "desc", "limit": 50000}
        super().__init__()

    def generate_request_args(self, args_data: list[str]) -> list[tuple[str, dict, str]]:
        """Generate the urls to query the stock prices endpoint.

        Args:
            args_data: list of tickers(str)

        Returns:
            url_args: list(tuple) of the (url, payload, and ticker) for each request
        """
        return [
            (
                f"/v2/aggs/ticker/{ticker}/range/{self.multiplier}/{self.timespan}/{self.start_date}/{self.end_date}",
                self.payload,
                ticker,
            )
            for ticker in args_data
        ]


class OptionsContracts(PolygonPaginator):
    """Object to query options contract tickers for a given underlying ticker based on given dates.
    It will pull all options contracts that exist for each underlying ticker as of the first business day of each month.
    Number of months are determined by `month_hist`"""

    paginator_type = "OptionsContracts"

    def __init__(
        self,
        tickers: list[str],
        ticker_id_lookup: dict[str, int],  # ticker str is key, id is value
        months_hist: int = 24,
    ):
        super().__init__()
        self.tickers = tickers
        self.ticker_id_lookup = ticker_id_lookup
        self.months_hist = months_hist
        self.base_dates = self._determine_base_dates()

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

    def generate_request_args(self, args_data: list[str]) -> list[tuple[str, dict, str]]:
        """Generate the urls to query the options contracts endpoint.

        Args:
            args_data: list of tickers

        Returns:
            url_args: list(tuple) of the (url, payload, and ticker_id) for each request"""
        url_base = "/v3/reference/options/contracts"
        payload = {"limit": 1000}
        return [
            (url_base, dict(payload, **{"underlying_ticker": ticker, "as_of": date}), ticker)
            for ticker in args_data
            for date in self.base_dates
        ]

    async def download_data(self, url: str, payload: dict, ticker: str, session: ClientSession = None):
        """Overwriting inherited download_data().
        This special case will add a specific identified to json filename from the payload dict.

        NOTE: session = None prevents the function from crashing without a session input initially.
        This lets us wait for the process pool to insert the session into the args.
        """
        log.info(f"Downloading options contract data for {ticker}")
        log.debug(f"Downloading data for {ticker} with url: {url} and payload: {payload}")
        results = await self._query_all(session, url, payload)
        log.info(f"Writing options contract data for {ticker} to file")
        write_api_data_to_file(
            results, *self._download_path(ticker + "/" + str(payload["as_of"]), str(timestamp_now()))
        )  # NOTE: this creates a folder for each "as_of" date


class HistoricalOptionsPrices(PolygonPaginator):
    """Object to query Polygon API and retrieve historical prices for the options chain for a given ticker"""

    paginator_type = "OptionsPrices"

    def __init__(
        self,
        months_hist: int = 24,
        multiplier: int = 1,
        timespan: Timespans = Timespans.day,
        adjusted: bool = True,
    ):
        super().__init__()
        self.timespan = timespan.value
        self.multiplier = multiplier
        self.adjusted = "true" if adjusted else "false"
        self.months_hist = months_hist

    def _determine_start_end_dates(self, exp_date: date):
        end_date = datetime.now().date() if exp_date > datetime.now().date() else exp_date
        start_date = end_date - relativedelta(months=self.months_hist)
        return start_date, end_date

    def _construct_url(self, o_ticker: str, expiration_date: datetime) -> str:
        """function to construct the url for the options prices endpoint"""
        return f"/v2/aggs/ticker/{o_ticker}/range/{self.multiplier}/{self.timespan}/" + "{0}/{1}".format(
            *self._determine_start_end_dates(expiration_date)
        )

    def generate_request_args(self, args_data: list[OptionTicker]) -> list[tuple[str, dict, str, str, str]]:
        """Generate the urls to query the options prices endpoint.

        Args:
            args_data: list of named tuples. OptionTicker(options_ticker, id, expiration_date, underlying_ticker)

        Returns:
            url_args: list(tuple) of the (url, payload, and ticker, underlying ticker, clean ticker) for each request"""
        payload = {"adjusted": self.adjusted, "sort": "desc", "limit": 50000}
        return [
            (
                self._construct_url(o_ticker.o_ticker, o_ticker.expiration_date),
                payload,
                o_ticker.o_ticker,
                o_ticker.underlying_ticker,
                self._clean_o_ticker(o_ticker.o_ticker),
            )
            for o_ticker in args_data
        ]

    async def download_data(
        self,
        url: str,
        payload: dict,
        o_ticker: str,
        under_ticker: str,
        clean_ticker: str,
        session: ClientSession = None,
    ):
        """Overwriting inherited download_data().
        This special case will add a specific identified to json filename from the payload dict.

        NOTE: session = None prevents the function from crashing without a session input initially.
        This lets us wait for the process pool to insert the session into the args.
        """
        log.info(f"Downloading price data for {o_ticker}")
        log.debug(f"Downloading data for {o_ticker} with url: {url} and payload: {payload}")
        results = await self._query_all(session, url, payload)
        results = [x.get("results") for x in results if x.get("results")]

        if results:
            log.info(f"Writing price data for {o_ticker} to file")
            write_api_data_to_file(
                results,
                *self._download_path(
                    under_ticker + "/" + clean_ticker,
                    str(timestamp_now()),
                ),
            )
        else:
            log.info(f"No price data for {o_ticker} in the time range")


class CurrentContractSnapshot(PolygonPaginator):
    """Object to query Polygon API and retrieve current snapshot with greeks and IV. Not historical data"""

    paginator_type = "ContractSnapshot"

    def __init__(self):
        super().__init__()

    def _construct_url(self, under_ticker: str, o_ticker: str) -> str:
        """function to construct the url for the snapshot endpoint"""
        return f"/v3/snapshot/options/{under_ticker}/{o_ticker}"

    def generate_request_args(self, args_data: list[OptionTicker]):
        """Generate the urls to query the options prices endpoint.
        Inputs should be OptionTickers for unexpired contracts.

        Args:
            args_data: list of named tuples. OptionTicker(options_ticker, id, expiration_date, underlying_ticker)

        Returns:
            url_args: list(tuple) of the (url, underlying ticker, clean ticker) for each request"""
        return [
            (
                self._construct_url(o_ticker.underlying_ticker, o_ticker.o_ticker),
                o_ticker.o_ticker,
                o_ticker.underlying_ticker,
                self._clean_o_ticker(o_ticker.o_ticker),
            )
            for o_ticker in args_data
            if o_ticker.expiration_date >= datetime.now().date()
            # NOTE: this filters is redundant since o_tickers passed in should be unexpired
        ]

    async def download_data(
        self,
        url: str,
        o_ticker: str,
        under_ticker: str,
        clean_ticker: str,
        session: ClientSession = None,
    ):
        """Overwriting inherited download_data().

        NOTE: session = None prevents the function from crashing without a session input initially.
        This lets us wait for the process pool to insert the session into the args.
        """
        log.info(f"Downloading snapshot/greek data for {o_ticker}")
        log.debug(f"Downloading data for {o_ticker} with url: {url} and no payload")
        results = await self._query_all(session, url)
        log.info(f"Writing snapshot/greek data for {o_ticker} to file")
        write_api_data_to_file(
            results,
            *self._download_path(
                under_ticker + "/" + clean_ticker,
                str(timestamp_now()),
            ),
        )


class HistoricalQuotes(HistoricalOptionsPrices):
    """Object to query Polygon API and retrieve historical quotes for the options chain for a given ticker"""

    paginator_type = "OptionsQuotes"

    def __init__(self, months_hist: int = 24):
        super().__init__(months_hist=months_hist, timespan=Timespans.hour)
        self.start_date, self.close_date = self._determine_start_end_dates(
            string_to_date("2040-01-01")
        )  # NOTE: magic number meant to always trigger the newest date (today)
        self.dates = trading_days_in_range(str(self.start_date), str(self.close_date), count=False)
        self.dates = self._prepare_timestamps(self.dates)

    def _construct_url(self, o_ticker: str) -> str:
        return f"/v3/quotes/{o_ticker}"

    def generate_request_args(
        self, args_data: list[OptionTicker]
    ) -> list[str, tuple[list[tuple[str, dict]], str, str]]:
        """Generate the urls to query the options quotes endpoint.
        Inputs should be OptionTickers. We then generate the date ranges.
        To prepare the args, we make the timestamp pairs (1 hour wide) and query for the oldest quote in each window.
        Except for the final pair, we get the newest as "closing" quote.
        Only create args if the option has not yet expired during the dates in the time range.

        Outputs:
            output_args: list of (o_ticker: str, tuple(
                list(tuple(url, payload)), underlying_ticker, clean_ticker)
            }
        """
        output_args = []
        log.info(f"Generating request args for {len(args_data)} option tickers")
        count = 0
        for o_ticker in args_data:
            if count % 500 == 0:
                log.info(f"Generating request args for {count}/{len(args_data)} option tickers")
            count += 1
            payloads = self.dates.loc[self.dates["timestamp.gte"] <= str(o_ticker.expiration_date)].to_dict(
                "records"
            )
            if payloads:
                url = self._construct_url(o_ticker.o_ticker)
                args = [(url, payload, o_ticker.underlying_ticker) for payload in payloads]
                output_args.extend(args)

        return output_args

    @staticmethod
    def _prepare_timestamps(dates: pd.DataFrame) -> list[int]:
        """converts market dates to timestamps occurring every hour from 9:30am to 5:30pm based on market tz"""
        dates = dates.tz_localize("US/Eastern")
        for i in range(9):
            if i >= 7:
                dates[f"{i+9}_oclock"] = dates.index + pd.Timedelta(hours=i + 9)
            else:
                dates[f"{i+9}_oclock"] = dates.index + pd.Timedelta(hours=i + 9, minutes=30)
            dates[f"{i+9}_oclock"] = dates[f"{i+9}_oclock"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            dates[f"{i+9}_oclock"] = (
                dates[f"{i+9}_oclock"].astype(str).str.slice(stop=-2)
                + ":"
                + dates[f"{i+9}_oclock"].astype(str).str.slice(start=-2)
            )
        dates.drop(columns=["market_open", "market_close"], inplace=True)
        dates = pd.DataFrame({"timestamp.gte": dates.values.flatten()})
        dates["timestamp.lte"] = dates["timestamp.gte"].shift(-1)
        dates = dates[~dates["timestamp.gte"].str.contains("T17")].reset_index(drop=True)
        dates["limit"] = 1
        dates["sort"] = "timestamp"
        dates["order"] = np.tile(["asc"] * 7 + ["desc"], int(np.ceil(len(dates) / 8)))[: len(dates)]

        return dates.sort_index(ascending=False).reset_index(drop=True)

    async def download_data(
        self,
        url: str,
        payload: dict,
        under_ticker: str,
        session: ClientSession = None,
    ):
        """Overwriting inherited download_data().
        Creates a file per options ticker with all available quote date in the time range

        args:
            o_ticker: str,
            request_args: tuple(list(tuple(url, payload)),
            under_ticker: str,
            clean_ticker: str,

        NOTE: session = None prevents the function from crashing without a session input initially.
        This lets us wait for the process pool to insert the session into the args.
        """
        o_ticker = url.split(":")[1]
        log.info(f"Downloading quote data for {o_ticker}")

        return await self._query_all(session, url, payload, limit=True)

        # if results:
        #     log.info(f"Writing quote data for {o_ticker} to file")

        #     results = [x[0].get("results")[0] for x in results if x[0].get("results")]

        #     write_api_data_to_file(
        #         results,
        #         *self._download_path(
        #             under_ticker + "/" + o_ticker,
        #             str(timestamp_now()),
        #         ),
        #     )
        # else:
        #     log.info(f"No quote data for {o_ticker} in the time range")
