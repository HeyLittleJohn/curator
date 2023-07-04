import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Awaitable

from db_tools.queries import update_options_prices, update_options_tickers, update_stock_metadata

from option_bot.proj_constants import log, POSTGRES_BATCH_MAX
from option_bot.utils import read_data_from_file, timestamp_to_datetime, two_years_ago


class PathRunner(ABC):
    """Base class for runner that will traverse the directory structure and retrieve/clean raw data.
    This class will be inherited by the specific runners for each data type"""

    runner_type = "Generic"
    upload_func: Awaitable = None
    base_directory = "/.polygon_data"

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

    def _make_batch_generator(self, clean_data: list[dict]) -> list[dict]:
        if self.batch_size == 0:
            self.record_size = len(clean_data[0].keys())
            self.batch_size = POSTGRES_BATCH_MAX // self.record_size
        for i in range(0, len(clean_data), self.batch_size):
            yield clean_data[i : i + self.batch_size]

    @abstractmethod
    def generate_path_args(self) -> list[str]:
        """Requiring a generate_path_args() function to be overwritten by every inheriting class.
        This function will generate the arguments to be passed to the pool for each file in the directory"""
        pass

    @abstractmethod
    def clean_data(self, results: list[dict], ticker_data: Any) -> list[dict]:
        """Requiring a clean_data() function to be overwritten by every inheriting class.

        All classes should maintain this arg/return schema.

        Args:
            results (list[dict]): raw data from the file
            ticker_data (dict): ticker data to be passed to the clean function. Optional

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
        for batch in self._make_batch_generator(clean_data):
            await self.upload_func(batch)


class MetaDataRunner(PathRunner):
    """Runner for stock metadata local data"""

    runner_type = "StockMetaData"
    base_directory = "/.polygon_data/StockMetaData"
    upload_func = update_stock_metadata

    def __init__(self, tickers: list[str] = [], all_: bool = False):
        self.all_ = all_
        self.tickers = tickers

    def generate_path_args(self) -> list[str]:
        """This function will generate the arguments to be passed to the pool for each file in the directory

        Returns:
            list of file paths to be passed to the pool
            e.g. "/.polygon_data/StockMetaData/1688127754374.json"
        """
        if not os.path.exists(self.base_directory):
            log.warning("no metadata found. Download metadata first!")
            raise FileNotFoundError

        if self.all_:
            return [self._determine_most_recent_file(self.base_directory)]
        else:
            return [self._determine_most_recent_file(f"{self.base_directory}/{ticker}") for ticker in self.tickers]

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


class OptionsContractsRunner(PathRunner):
    """Runner for options contracts local data"""

    runner_type = "OptionsContracts"
    base_directory = "/.polygon_data/OptionsContracts"
    upload_func = update_options_tickers

    def __init__(self, months_hist: int, hist_limit_date: str = ""):
        self.months_hist = months_hist
        self.hist_limit_date = self._configure_hist_limit_date(hist_limit_date)

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

    def generate_path_args(self, ticker_id_lookup: dict) -> list[str]:
        """This function will generate the arguments to be passed to the pool,
        It will traverse the directories for each ticker and date to find the most recent, relevant file.

        Returns:
            list of file paths to be passed to the pool
            e.g. "/.polygon_data/OptionsContracts/SPY/2022-06-01/1688127754374.json"
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
                path_args.append(self._determine_most_recent_file(temp_path), ticker_id_lookup[ticker])
        return path_args

    def clean_data(self, results: list[dict], ticker_id: int) -> list[dict]:
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
                t["underlying_ticker_id"] = ticker_id
                clean_results.append(t)
        clean_results = list({v["options_ticker"]: v for v in clean_results}.values())
        # NOTE: the list(comprehension) above ascertains that all options_tickers are unique
        return clean_results


class OptionsPricesRunner(PathRunner):
    """Runner for options prices local data"""

    runner_type = "OptionsPrices"
    base_directory = "/.polygon_data/OptionsPrices"
    upload_func = update_options_prices

    def __init__(self, months_hist: int, hist_limit_date: str = ""):
        self.months_hist = months_hist
        self.hist_limit_date = self._configure_hist_limit_date(hist_limit_date)

    def generate_path_args(self, o_tickers_lookup: dict) -> list[str]:
        """This function will generate the arguments to be passed to the pool. Requires the o_tickers_lookup"""
        pass

    def clean_data(self, results: list[dict], o_ticker: tuple[str, int, str, str]) -> list[dict]:
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
        for page in results:
            if page.get("results"):
                for record in page.get("results"):
                    t = {key_mapping[key]: record.get(key) for key in key_mapping}
                    t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                    t["options_ticker_id"] = o_ticker[1]
                    clean_results.append(t)
