from aiomultiprocess import Pool
from data_pipeline.path_runner import (
    MetaDataRunner,
    OptionsContractsRunner,
    OptionsPricesRunner,
    PathRunner,
    StockPricesRunner,
)
from db_tools.utils import OptionTicker

from option_bot.proj_constants import log
from option_bot.utils import pool_kwarg_config


async def etl_pool_uploader(runner: PathRunner, pool_kwargs: dict = {}, path_input_args: list[str] = []):
    """This function will create a process pool to concurrently upload the downloaded json to the db

    Args:
        runner: PathRunner object, specific to the data type being uploaded,
        pool_kwargs: kwargs to be passed to the process poolm
        path_input_args: list of args to be passed to the runner's generate_path_args() function.
        These input args will likely be a list of tickers or something similar.

    """
    # NOTE: uploader was originally a linear class. It can't handle concurrency. Need to change
    # uploader = Uploader(upload_func, expected_args, record_size)
    log.info(f"generating the path args to be uploaded -- {runner.runner_type}")
    path_args = (
        runner.generate_path_args() if not path_input_args else runner.generate_path_args(path_input_args)
    )

    log.info(
        f"uploading data to the database -- Starting Process Pool -- Upload Function: {runner.upload_func.__qualname__}"
    )
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


async def upload_stock_prices(ticker_id_lookup: dict):
    """This function uploads stock prices to the database"""
    price_runner = StockPricesRunner()
    pool_kwargs = {"childconcurrency": 3}
    await etl_pool_uploader(price_runner, path_input_args=ticker_id_lookup, pool_kwargs=pool_kwargs)


async def upload_options_contracts(ticker_id_lookup: dict, months_hist: int, hist_limit_date: str = ""):
    """This function uploads options contract data to the database"""
    opt_runner = OptionsContractsRunner(months_hist, hist_limit_date)
    pool_kwargs = {"childconcurrency": 3}
    await etl_pool_uploader(opt_runner, path_input_args=ticker_id_lookup, pool_kwargs=pool_kwargs)


async def upload_options_prices(o_tickers: dict):
    """This function uploads options prices data to the database

    Args:
        o_tickers: dict(o_ticker_id: OptionsTicker tuple)"""
    opt_price_runner = OptionsPricesRunner()
    pool_kwargs = {"childconcurrency": 3}
    await etl_pool_uploader(opt_price_runner, path_input_args=o_tickers, pool_kwargs=pool_kwargs)


async def upload_options_snapshots(o_tickers: dict[str, OptionTicker]):
    pass


async def upload_options_quotes(o_tickers: dict[str, OptionTicker]):
    pass
