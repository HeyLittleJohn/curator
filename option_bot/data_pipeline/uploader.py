from typing import Awaitable

from aiomultiprocess import Pool
from data_pipeline.path_runner import MetaDataRunner, PathRunner

from option_bot.proj_constants import log
from option_bot.utils import pool_kwarg_config


async def etl_pool_uploader(runner: PathRunner, pool_kwargs: dict = {}):
    """This function will create a process pool to concurrently upload the downloaded json to the db

    Args:
        runner: PathRunner object, specific to the data type being uploaded,
        pool_kwargs: kwargs to be passed to the process pool

    """
    # NOTE: uploader was originally a linear class. It can't handle concurrency. Need to change
    # uploader = Uploader(upload_func, expected_args, record_size)
    log.info(f"generating the path args to be uploaded -- {runner.runner_type}")
    path_args = runner.generate_path_args()

    log.info("uploading data to the database -- Starting Process Pool")
    pool_kwargs = pool_kwarg_config(pool_kwargs)
    log.debug(f"process pool kwargs: {pool_kwargs}")
    async with Pool(**pool_kwargs) as pool:
        await pool.starmap(runner.upload, path_args)


async def upload_stock_metadata(tickers: list[str], all_: bool):
    """This function uploads stock metadata to the database"""
    if all_:
        log.info("uploading all stock metadata")
    else:
        log.info(f"uploading stock metadata for {tickers}")
    meta = MetaDataRunner(tickers, all_)
    pool_kwargs = {"processes": 1, "childconcurrency": 1, "queuecount": 1}
    await etl_pool_uploader(meta, pool_kwargs=pool_kwargs)


async def upload_stock_prices(tickers, all_):
    """This function uploads stock prices to the database"""
    pass


async def upload_options_contracts(ticker_id_lookup: dict):
    """This function uploads options contract data to the database"""
    tickers = list(ticker_id_lookup.keys())


async def upload_options_prices(tickers, all_):
    """This function uploads options prices data to the database"""
    pass


def generate_directory_args(directory: str, file_type: str):
    pass
