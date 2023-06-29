from typing import Awaitable

# from aiomultiprocess import Pool
from data_pipeline.polygon_utils import PolygonPaginator

from option_bot.proj_constants import log


class Uploader:
    def __init__(self, upload_func: Awaitable, expected_args: int, record_size: int):  # , upload_pk_name: str):
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


async def etl_pool_uploader(
    paginator: PolygonPaginator, upload_func: Awaitable, record_size: int, expected_args: int, pool_kwargs: dict = {}
):
    """This function will create a process pool to concurrently upload the downloaded json to the db

    Args:
        upload_func: function to upload the data to the db, matching the endpoint/data type being queried
        record_size: number of fields per record, used to estimate the batch size
        expected_args: number of arguments expected to be passed to the pool. Based on files in directory

    """
    # NOTE: uploader was originally a linear class. It can't handle concurrency. Need to change
    # uploader = Uploader(upload_func, expected_args, record_size)
    pass


async def upload_stock_metadata(tickers, all_):
    """This function uploads stock metadata to the database"""
    pass


async def upload_options_contracts(tickers, all_):
    """This function uploads options contract data to the database"""
    pass


async def upload_options_prices(tickers, all_):
    """This function uploads options prices data to the database"""
    pass


def generate_directory_args(directory: str, file_type: str):
    pass
