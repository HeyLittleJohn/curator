import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generator

from db_tools.queries import (
    update_options_prices,
    update_options_snapshot,
    update_options_tickers,
    update_stock_metadata,
    update_stock_prices,
)
from db_tools.utils import OptionTicker

from option_bot.proj_constants import BASE_DOWNLOAD_PATH, POSTGRES_BATCH_MAX, log
from option_bot.utils import read_data_from_file, timestamp_to_datetime, two_years_ago


class PathRunner(ABC):
    """Base class for runner that will traverse the directory structure and retrieve/clean raw data.
    This class will be inherited by the specific runners for each data type"""

    runner_type = "Generic"
    base_directory = BASE_DOWNLOAD_PATH

    def __init__(self):
        self.record_size = 0
        self.batch_size = 0

    def _determine_most_recent_file(self, directory_path: str) -> str:
        """This function will determine the most recent file in a directory based on the file name.
        It will return the name of the most recent file"""
        f = os.listdir(directory_path)
        f.sort(reverse=True, key=lambda x: int(x.split(".")[0]))
        # NOTE: this lambda may be unnecessary. Does it slow it down?
        return directory_path + "/" + f[0]

    def _make_batch_generator(self, clean_data: list[dict]) -> Generator[dict]:
        if self.batch_size == 0:
            self.record_size = len(clean_data[0].keys())
            self.batch_size = POSTGRES_BATCH_MAX // self.record_size
        for i in range(0, len(clean_data), self.batch_size):
            yield clean_data[i : i + self.batch_size]

    @abstractmethod
    async def upload_func(self, data: list[dict]):
        """Requiring a upload_func() function to be overwritten by every inheriting class.
        This function will upload the data to the database

        Args:
            data (list[dict]): data to be uploaded to the database"""
        pass

    @abstractmethod
    def generate_path_args(self) -> list[str]:
        """Requiring a generate_path_args() function to be overwritten by every inheriting class.
        This function will generate the arguments to be passed to the pool for each file in the directory

        Optional: can accept path_input_args as defined by the inheriting class"""
        pass

    @abstractmethod
    def clean_data(self, results: list[dict], ticker_data: Any) -> list[dict]:
        """Requiring a clean_data() function to be overwritten by every inheriting class.

        All classes should maintain this arg/return schema.

        Args:
            results (list[dict]): raw data from the file
            ticker_data (Any): ticker data to be passed to the clean function. Optional

        Returns:
            list[dict]: cleaned data"""
        pass

    async def upload(self, file_path: str, ticker_data: tuple = ()):
        """This function will read the file, clean the data, and upload to the database

        Every self.upload_func needs to require args in this format:
            upload_func(data: list[dict])

        Args:
            file_path (str): path to the file to be uploaded
        """
        log.info(f"uploading data from {file_path}")
        raw_data = read_data_from_file(file_path)
        clean_data = self.clean_data(raw_data, ticker_data)
        if len(clean_data) > 0:
            for batch in self._make_batch_generator(clean_data):
                await self.upload_func(batch)


class MetaDataRunner(PathRunner):
    """Runner for stock metadata local data"""

    runner_type = "StockMetaData"
    base_directory = f"{BASE_DOWNLOAD_PATH}/StockMetaData"

    def __init__(self, tickers: list[str] = [], all_: bool = False):
        self.all_ = all_
        self.tickers = tickers
        super().__init__()

    async def upload_func(self, data: list[dict]):
        return await update_stock_metadata(data)

    def generate_path_args(self) -> list[tuple[str]]:
        """This function will generate the arguments to be passed to the pool for each file in the directory

        Returns:
            list of tuples with file paths and nothing to be passed to the pool
            e.g. ("~/.polygon_data/StockMetaData/1688127754374.json",)
        """
        if not os.path.exists(self.base_directory):
            log.warning("no metadata found. Download metadata first!")
            raise FileNotFoundError

        if self.all_:
            return [(self._determine_most_recent_file(self.base_directory),)]
        else:
            return [
                (self._determine_most_recent_file(f"{self.base_directory}/{ticker}"),)
                for ticker in self.tickers
            ]

    def clean_data(self, results: list[dict], ticker_data: tuple = ()):
        clean_results = []
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
        for result_list in results:
            for ticker in result_list["results"]:
                t = {x: ticker.get(x) for x in selected_keys}
                clean_results.append(t)
        return clean_results


class StockPricesRunner(PathRunner):
    """Runner for stock prices local data"""

    runner_type = "StockPrices"
    base_directory = f"{BASE_DOWNLOAD_PATH}/StockPrices"

    def __init__(self):
        super().__init__()

    def generate_path_args(self, ticker_id_lookup: dict) -> list[tuple[str, tuple[int]]]:
        if not os.path.exists(self.base_directory):
            log.warning("no options contracts found. Download options contracts first!")
            raise FileNotFoundError

        tickers = list(ticker_id_lookup.keys())
        path_args = []
        for ticker in tickers:
            temp_path = self.base_directory + "/" + ticker
            path_args.append(
                (
                    self._determine_most_recent_file(temp_path),
                    (ticker_id_lookup[ticker],),  # a tuple
                )
            )
        return path_args

    async def upload_func(self, data: list[dict]):
        return await update_stock_prices(data)

    def clean_data(self, results: list[dict], ticker_id: tuple[int]) -> list[dict]:
        clean_results = []
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
        for page in results:
            if page.get("results"):
                for record in page.get("results"):
                    t = {key_mapping[key]: record.get(key) for key in key_mapping}
                    t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                    t["ticker_id"] = ticker_id[0]
                    clean_results.append(t)
        return clean_results


class OptionsContractsRunner(PathRunner):
    """Runner for options contracts local data"""

    runner_type = "OptionsContracts"
    base_directory = f"{BASE_DOWNLOAD_PATH}/OptionsContracts"

    def __init__(self, months_hist: int, hist_limit_date: str = ""):
        self.months_hist = months_hist
        self.hist_limit_date = self._configure_hist_limit_date(hist_limit_date)
        super().__init__()

    def _configure_hist_limit_date(self, date_str: str = "") -> str:
        """This function will return a string of the first day of the month going back up to 2 year.

        Args:
            date_str (str): date in format "YYYY-MM-DD" or an empty string

        Returns:
            str: date in format "YYYY-MM-DD"
        """
        if date_str:
            date = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            date = two_years_ago()
        return date.strftime("%Y-%m-01")

    async def upload_func(self, data: list[dict]):
        return await update_options_tickers(data)

    def generate_path_args(self, ticker_id_lookup: dict) -> list[tuple[str, tuple[int]]]:
        """This function will generate the arguments to be passed to the pool,
        It will traverse the directories for each ticker and date to find the most recent, relevant file.

        Returns:
            path_args: list of tuples containing the file path and the ticker idto be passed to the pool.
            e.g. "(~/.polygon_data/OptionsContracts/SPY/2022-06-01/1688127754374.json, (9912,))"
        """
        # NOTE: may need try/except in case specific ticker files didn't successfully download
        if not os.path.exists(self.base_directory):
            log.warning("no options contracts found. Download options contracts first!")
            raise FileNotFoundError

        tickers = list(ticker_id_lookup.keys())
        path_args = []
        for ticker in tickers:
            temp_dir_list = os.listdir(self.base_directory + "/" + ticker)
            temp_dir_list.append(self.hist_limit_date)
            temp_dir_list.sort(reverse=True)
            i = temp_dir_list.index(self.hist_limit_date)
            for date in temp_dir_list[:i]:
                temp_path = f"{self.base_directory}/{ticker}/{date}"
                path_args.append(
                    (
                        self._determine_most_recent_file(temp_path),
                        (ticker_id_lookup[ticker],),  # a tuple
                    )
                )
        return path_args

    def clean_data(self, results: list[dict], ticker_id: tuple[int]) -> list[dict]:
        # TODO: make sure this fits the json data schema
        clean_results = []
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
        for page in results:
            for record in page.get("results"):
                t = {key_mapping[key]: record.get(key) for key in key_mapping}
                t["underlying_ticker_id"] = ticker_id[0]
                clean_results.append(t)
        clean_results = list({v["options_ticker"]: v for v in clean_results}.values())
        # NOTE: the list(comprehension) above ascertains that all options_tickers are unique
        return clean_results


class OptionsPricesRunner(PathRunner):
    """Runner for options prices local data"""

    runner_type = "OptionsPrices"
    base_directory = f"{BASE_DOWNLOAD_PATH}/OptionsPrices"

    def __init__(self):
        super().__init__()

    def _clean_o_ticker(self, o_ticker: str) -> str:
        """Clean the options ticker to remove the prefix to make it compatible as a file name"""
        return o_ticker.split(":")[1]

    def _make_o_ticker(self, clean_o_ticker: str) -> str:
        """re-adds the option prefix to the clean options ticker"""
        return f"O:{clean_o_ticker}"

    async def upload_func(self, data: list[dict]):
        return await update_options_prices(data)

    def generate_path_args(self, o_tickers_lookup: dict) -> list[tuple[str, tuple[str, int, str, str]]]:
        """This function will generate the arguments to be passed to the pool. Requires the o_tickers_lookup.

        Args:
            o_tickers_lookup (dict): dict(o_ticker: OptionsTicker namedtuple)
           (OptionTicker namedtuple containing o_ticker, id, expiration_date, underlying_ticker).

        Returns:
            path_args
        """
        if not os.path.exists(self.base_directory):
            log.warning("no options contracts found. Download options contracts first!")
            raise FileNotFoundError

        o_tickers = o_tickers_lookup.keys()
        path_args = []
        for o_ticker in o_tickers:
            temp_path = (
                self.base_directory
                + "/"
                + o_tickers_lookup[o_ticker].underlying_ticker
                + "/"
                + self._clean_o_ticker(o_ticker)
            )
            path_args.append((self._determine_most_recent_file(temp_path), o_tickers_lookup[o_ticker]))
        return path_args

    def clean_data(self, results: list[dict], o_ticker: OptionTicker) -> list[dict]:
        """This function will clean the data and return a list of dicts to be uploaded to the db

        Args:
            results (list[dict]): raw data from the file
            o_ticker (tuple[str, int, str, str]):
            OptionTicker named tuple containing o_ticker, id, expiration_date, underlying_ticker.
        """
        clean_results = []
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
        if isinstance(results, list):
            for page in results:
                if page.get("results"):
                    for record in page.get("results"):
                        t = {key_mapping[key]: record.get(key) for key in key_mapping}
                        t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                        t["options_ticker_id"] = o_ticker.id
                        clean_results.append(t)
        return clean_results


class OptionsSnapshotRunner(OptionsPricesRunner):
    """Runner for options snapshot data"""

    runner_type = "OptionsSnapshots"
    base_directory = f"{BASE_DOWNLOAD_PATH}/OptionsSnapshots"

    def __init__(self):
        super().__init__()

    async def upload_func(self, data: list[dict]):
        return await update_options_snapshot(data)


class OptionsQuoteRunner(PathRunner):
    pass
