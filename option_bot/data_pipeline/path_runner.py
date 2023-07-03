import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Awaitable

from db_tools.queries import update_options_tickers, update_stock_metadata

from option_bot.proj_constants import log, POSTGRES_BATCH_MAX
from option_bot.utils import read_data_from_file, two_years_ago


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
    def clean_data(self) -> list[dict]:
        """Requiring a clean_data() function to be overwritten by every inheriting class"""
        pass

    async def upload(self, file_path: str):
        """This function will read the file, clean the data, and upload to the database

        Every self.upload_func needs to require args in this format:
            upload_func(data: list[dict])

        Args:
            file_path (str): path to the file to be uploaded
        """
        log.info(f"uploading data from {file_path}")
        raw_data = read_data_from_file(file_path)
        clean_data = self.clean_data(raw_data)
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

    def clean_data(self, results: list[dict]):
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

    def generate_path_args(self, tickers: list[str]) -> list[str]:
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

        path_args = []
        for ticker in tickers:
            temp_dir_list = os.listdir(self.base_directory + "/" + ticker)
            temp_dir_list.append(self.hist_limit_date)
            temp_dir_list.sort(reverse=True)
            i = temp_dir_list.index(self.hist_limit_date)
            for date in temp_dir_list[:i]:
                temp_path = f"{self.base_directory}/{ticker}/{date}"
                path_args.append(self._determine_most_recent_file(temp_path))
        return path_args

    def clean_data(self, results: list[dict], ticker_id_lookup: dict):
        # TODO: make sure this fits the json data schema. And add ticker_id_lookup
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
                t["underlying_ticker_id"] = ticker_id_lookup[record.get("underlying_ticker")]
                clean_results.append(t)
        clean_results = list({v["options_ticker"]: v for v in clean_results}.values())
        # NOTE: the list(comprehension) above ascertains that all options_tickers are unique
        return clean_results


class Uploader:
    def __init__(
        self, upload_func: Awaitable, cleaning_func: function, expected_args: int, record_size: int
    ):  # , upload_pk_name: str):
        self.clean_data = []
        # self.upload_pk_name = upload_pk_name # ticker, whatever field is constrained of duplicates
        self.batch_max = 62000
        self.record_size = record_size  # number of fields per record
        self.upload_func = upload_func
        self.expected_args = expected_args
        self.arg_counter = 0
        self.batch_counter = 1
        self.batch_size = self.batch_max // self.record_size

    async def process_clean_data(self, clean_data: list[dict]):
        """adds the clean data to the uploader object which uploads in batches based on the amount of data present"""
        log.info(f"received clean results for arg number {self.arg_counter}")
        self.clean_data.extend(clean_data)
        if self.arg_counter == 0:
            self._update_record_size(clean_data)
        self.arg_counter += 1  # index of 1 since matching len of url_args
        if len(self.clean_data) >= self.batch_size or self.arg_counter == self.expected_args:
            for batch in self._make_batch_generator():
                log.info(f"uploading batch {self.batch_counter} with {self.upload_func.__qualname__}")
                await self.upload_func(batch)
                self.batch_counter += 1

    def update_expected_args(self):
        """decrease the expected number of pool args if the query returns no data"""
        self.expected_args -= 1

    # NOTE: expected_records is originally set to the number of args put into the pool

    def _update_record_size(self, clean_data: list[dict]):
        """update the record size if the first arg returns data"""
        self.record_size = len(clean_data[0].keys())
        self.batch_size = self.batch_max // self.record_size

    def _make_batch_generator(self):
        """generator that batches self.clean_data into batches of size self.batch_size"""
        try:
            for i in range(0, len(self.clean_data), self.batch_size):
                yield self.clean_data[i : i + self.batch_size]

        except Exception as e:
            log.exception(e)
            raise e

        finally:
            self.clean_data = []  # return to empty list after all data has been uploaded
            self.batch_counter = 1

    def _ensure_no_duplicates(self):
        """ensures there are no duplicate constrained values in upload data"""
        pass
        # Note: you may not need this. Cleaning could remain polygon_utils file
        # or this could be abstracted to dedupe all the data types
