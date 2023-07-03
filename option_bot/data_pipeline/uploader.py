from typing import Awaitable

from aiomultiprocess import Pool
from data_pipeline.path_runner import PathRunner

from option_bot.proj_constants import log


async def etl_pool_uploader(
    paginator: PathRunner, upload_func: Awaitable, record_size: int, expected_args: int, pool_kwargs: dict = {}
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


async def upload_stock_prices(tickers, all_):
    """This function uploads stock prices to the database"""
    pass


async def upload_options_contracts(ticker_lookups):
    """This function uploads options contract data to the database"""
    pass


async def upload_options_prices(tickers, all_):
    """This function uploads options prices data to the database"""
    pass


def generate_directory_args(directory: str, file_type: str):
    pass
