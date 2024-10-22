import os
from abc import ABC, abstractmethod
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Generator

from db_tools.queries import (
    update_options_prices,
    update_options_quotes,
    update_options_snapshot,
    update_options_tickers,
    update_stock_metadata,
    update_stock_prices,
)
from db_tools.utils import OptionTicker

from curator.proj_constants import BASE_DOWNLOAD_PATH, POSTGRES_BATCH_MAX, log
from curator.utils import (
    clean_o_ticker,
    months_ago,
    read_data_from_file,
    timestamp_now,
    timestamp_to_datetime,
)


class PathRunner(ABC):
    """Base class for runner that will traverse the directory structure and retrieve/clean raw data.
    This class will be inherited by the specific runners for each data type"""

    runner_type = "Generic"

    def __init__(self):
        self.record_size = 0
        self.batch_size = 0
        self.base_directory = f"{BASE_DOWNLOAD_PATH}/{self.runner_type}"

    def _determine_most_recent_file(self, directory_path: str) -> str:
        """This function will determine the most recent file in a directory based on the file name.
        It will return the name of the most recent file"""
        f = os.listdir(directory_path)
        f.sort(reverse=True, key=lambda x: int(x.split(".")[0]))
        # NOTE: this lambda may be unnecessary. Does it slow it down?
        return directory_path + "/" + f[0]

    def _make_batch_generator(self, clean_data: list[dict]) -> Generator[list[dict], None, None]:
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

    async def upload(self, file_path: str, ticker_data: tuple = ()) -> str | None:
        """This function will read the file, clean the data, and upload to the database

        Every self.upload_func requires args in this format:
            upload_func(data: list[dict])

        Args:
            file_path (str): path to the file to be uploaded
        """
        log.info(f"uploading data from {file_path}")
        close_file = True if self.runner_type == "OptionsQuotes" else False
        try:
            raw_data = read_data_from_file(file_path, close_file)
        except FileNotFoundError:
            log.warning(f"file not found at: {file_path} for {self.runner_type}, with ticker: {ticker_data}")
            raw_data = None
        except JSONDecodeError:
            log.warning(f"failed to parse {file_path}. Make sure it is correct JSON")
            raw_data = None
            return file_path

        if raw_data:
            clean_data = self.clean_data(raw_data, ticker_data)
            if len(clean_data) > 0:
                for batch in self._make_batch_generator(clean_data):
                    await self.upload_func(batch)


class MetaDataRunner(PathRunner):
    """Runner for stock metadata local data"""

    runner_type = "StockMetaData"

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
            date = months_ago()
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

    def __init__(self):
        super().__init__()

    def _make_o_ticker(self, clean_o_ticker: str) -> str:
        """re-adds the option prefix to the clean options ticker"""
        return f"O:{clean_o_ticker}"

    async def upload_func(self, data: list[dict]):
        return await update_options_prices(data)

    def generate_path_args(self, o_tickers_lookup: dict[str, OptionTicker]) -> list[tuple[str, OptionTicker]]:
        """This function will generate the arguments to be passed to the pool. Requires the o_tickers_lookup.

        Args:
            o_tickers_lookup (dict): dict(o_ticker: OptionTicker namedtuple)
           (OptionTicker namedtuple containing o_ticker, id, expiration_date, underlying_ticker).

        Returns:
            path_args: list of tuples containing the file path and the OptionTicker namedtuple to be passed to the pool.
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
                + clean_o_ticker(o_ticker)
            )

            try:
                file = self._determine_most_recent_file(temp_path)
                path_args.append((file, o_tickers_lookup[o_ticker]))
            except FileNotFoundError:
                log.debug(f"file not found at: {temp_path} for {self.runner_type}, with ticker: {o_ticker}")
                continue

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
                for record in page:
                    t = {key_mapping[key]: record.get(key) for key in key_mapping}
                    t["as_of_date"] = timestamp_to_datetime(t["as_of_date"], msec_units=True)
                    t["options_ticker_id"] = o_ticker.id
                    clean_results.append(t)
        return clean_results


class OptionsSnapshotRunner(OptionsPricesRunner):
    """Runner for options snapshot data"""

    runner_type = "ContractSnapshot"

    def __init__(self):
        super().__init__()

    def clean_data(self, results: list[dict], o_ticker: OptionTicker) -> list[dict]:
        """This function will clean the data and return a list of dicts to be uploaded to the db

        Args:
            results list[dict]: raw snapshot data from the file
            o_ticker (tuple[str, int, str, str]):
            OptionTicker named tuple containing o_ticker, id, expiration_date, underlying_ticker.
        """
        results = results[0]
        clean_results = {}
        if isinstance(results, dict) and results.get("results"):
            results = results.get("results")
            clean_results["options_ticker_id"] = o_ticker.id
            clean_results["as_of_date"] = timestamp_to_datetime(
                results.get("last_quote", {}).get("last_updated", timestamp_now() * 1000000) / 1000000,
            ).date()
            clean_results["implied_volatility"] = results.get("implied_volatility", 0.0)
            clean_results["delta"] = results.get("greeks", {}).get("delta", 0.0)
            clean_results["gamma"] = results.get("greeks", {}).get("gamma", 0.0)
            clean_results["theta"] = results.get("greeks", {}).get("theta", 0.0)
            clean_results["vega"] = results.get("greeks", {}).get("vega", 0.0)
            clean_results["open_interest"] = results.get("open_interest", 0)
        return [clean_results]

    async def upload_func(self, data: list[dict]):
        return await update_options_snapshot(data)


class OptionsQuoteRunner(OptionsPricesRunner):
    """Runner for options quote data"""

    runner_type = "OptionsQuotes"

    def __init__(self):
        super().__init__()

    async def upload_func(self, data: list[dict]):
        return await update_options_quotes(data)

    def generate_path_args(self, ticker: str) -> list[str]:
        """returns the paths to each of the json file for each process in each ticker subdirectory"""
        if not os.path.exists(self.base_directory):
            log.warning("no options contracts found. Download options contracts first!")
            raise FileNotFoundError
        ticker_path = self.base_directory + "/" + ticker
        dirs = os.listdir(ticker_path)
        return [(self._determine_most_recent_file(ticker_path + "/" + dir), (ticker)) for dir in dirs]

    def clean_data(self, results: list[dict], o_ticker: OptionTicker) -> list[dict]:
        """This function will clean the data and return a list of dicts to be uploaded to the db

        Args:
            results list[dict]: raw quote data from the file
            o_ticker (tuple[str, int, str, str]):
            OptionTicker named tuple containing o_ticker, id, expiration_date, underlying_ticker.
        """
        if isinstance(results, list):
            if isinstance(results[0], list):
                clean_results = [self._convert_timestamps(record) for batch in results for record in batch]

            else:
                clean_results = [self._convert_timestamps(record) for record in results]
        else:
            clean_results = []

        return clean_results

    @staticmethod
    def _convert_timestamps(record: dict) -> dict:
        record["as_of_date"] = timestamp_to_datetime(
            record.get("sip_timestamp", timestamp_now() * 1000000), nano_sec=True, msec_units=False
        )
        return record
