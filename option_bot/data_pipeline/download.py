import asyncio
from argparse import Namespace
from datetime import datetime
from multiprocessing import cpu_count
from typing import Awaitable

import uvloop
from aiomultiprocess import Pool  # , set_start_method
from data_pipeline.exceptions import (
    InvalidArgs,
    ProjBaseException,
    ProjClientConnectionError,
    ProjClientResponseError,
    ProjIndexError,
    ProjTimeoutError,
)
from data_pipeline.polygon_utils import (
    HistoricalOptionsPrices,
    HistoricalStockPrices,
    OptionsContracts,
    PolygonPaginator,
    StockMetaData,
)
from db_tools.queries import (
    lookup_multi_ticker_ids,
    lookup_ticker_id,
    query_all_stock_tickers,
    query_options_tickers,
    ticker_imported,
    update_options_prices,
    update_options_tickers,
    update_stock_metadata,
    update_stock_prices,
)
from sentry_sdk import capture_exception

from option_bot.proj_constants import log, MAX_CONCURRENT_REQUESTS, POLYGON_BASE_URL


# set_start_method("fork")

CPUS = cpu_count() - 2


planned_exceptions = (
    InvalidArgs,
    ProjClientConnectionError,
    ProjBaseException,
    ProjClientResponseError,
    ProjIndexError,
    ProjTimeoutError,
)


pool_default_kwargs = {
    "processes": CPUS,
    "exception_handler": capture_exception,
    "loop_initializer": uvloop.new_event_loop,
    "childconcurrency": int(MAX_CONCURRENT_REQUESTS * 4 / CPUS),
    "queuecount": CPUS,
    "init_client_session": True,
    "session_base_url": POLYGON_BASE_URL,
}


def pool_kwarg_config(kwargs: dict) -> dict:
    """This function updates the kwargs for an aiomultiprocess.Pool from the defaults."""
    pool_kwargs = pool_default_kwargs.copy()
    pool_kwargs.update(kwargs)
    return pool_kwargs


async def etl_pool_uploader(
    paginator: PolygonPaginator, upload_func: Awaitable, record_size: int, expected_args: int, pool_kwargs: dict = {}
):
    """This function will create a process pool to concurrently upload the downloaded json to the db

    Args:
        upload_func: function to upload the data to the db, matching the endpoint/data type being queried
        record_size: number of fields per record, used to estimate the batch size
        expected_args: number of arguments expected to be passed to the pool. Based on files in directory

    """
    pass
    # uploader = Uploader(upload_func, expected_args, record_size)
    # async with Pool(**pool_kwargs) as pool:
    # if result is not None:
    #     log.info(f"Processing {paginator.paginator_type} results for arg {url_args[result_ix]}")
    #     clean_data = paginator.clean_data(result)
    #     await uploader.process_clean_data(clean_data)
    # else:
    #     uploader.update_expected_records()
    #     log.warning(
    #         f"no {paginator.paginator_type} results for arg {url_args[result_ix]}"
    #        )  # depends on results returned in order


async def api_pool_downloader(
    paginator: PolygonPaginator,
    args_data: list = None,
    pool_kwargs: dict = {},
):
    """This function creates a process pool to download data from the polygon api and store it in json files.
    It is the base module co-routine for all our data pulls.
    It generates the urls to be queried, creates and runs a process pool to perform the I/O queries.
    The results for each request are returned via PoolResults generator.

    Args:
        paginator: PolygonPaginator object, specific to the endpoint being queried


    """
    log.info("generating urls to be queried")
    url_args = paginator.generate_request_args(args_data)

    log.info("fetching data from polygon api")
    pool_kwargs = pool_kwarg_config(pool_kwargs)
    async with Pool(**pool_kwargs) as pool:
        await pool.starmap(paginator.download_data, url_args)

    log.info(f"finished downloading data for {paginator.paginator_type}. Process pool closed")


async def download_stock_metadata(tickers: list[str], all_: bool = True):
    """This function downloads stock metadata from polygon and stores it in /data/StockMetaData/*.json."""

    if all_:
        log.info("pulling ticker metadata for all tickers")
    else:
        log.info(f"pulling ticker metadata for tickers: {tickers}")
    meta = StockMetaData(tickers, all_)
    pool_kwargs = {"processes": 1, "childconcurrency": 1, "queuecount": 1}
    await api_pool_downloader(meta, pool_kwargs=pool_kwargs)


async def fetch_stock_prices(ticker: str, start_date: str, end_date: str, ticker_id: int | None = None):
    if not ticker_id:
        ticker_id = await lookup_ticker_id(ticker, stock=True)
    # TODO: Add exception handling here with a sleep function incase metadata has yet to populate
    log.info(f"pulling ticker prices for ticker: {ticker}")
    prices = HistoricalStockPrices(ticker, ticker_id, start_date, end_date)
    await prices.fetch()
    for batch in prices.clean_data_generator:
        await update_stock_prices(batch)
    await ticker_imported(ticker_id)
    log.info(f"{ticker} successfully imported")


async def test_query():
    results = (await query_options_tickers(["SPY"], all_=True),)
    return results  # 9912 is SPY id


async def fetch_options_contracts(
    tickers: list[str] = None,
    ticker_id_lookup: dict | None = None,
    months_hist: int = 24,
):
    # NOTE: if refreshing, just pull the current month, months_hist = 1
    if not tickers and not ticker_id_lookup:
        raise InvalidArgs("Must provide either tickers or ticker_id_lookup")
    elif not tickers:
        tickers = list(ticker_id_lookup.keys())
    elif not ticker_id_lookup:
        tickers_w_ids = await lookup_multi_ticker_ids(tickers, stock=True)
        ticker_id_lookup = {x[0]: x[1] for x in tickers_w_ids}
    options = OptionsContracts(tickers, ticker_id_lookup, months_hist)
    await api_pool_downloader(options)


async def fetch_options_prices(tickers: list[str], month_hist: int = 24, all_: bool = True):
    o_tickers = await generate_o_ticker_lookup(tickers, all_=all_)
    # batch_counter = 0
    pool_kwargs = {"childconcurrency": 400, "maxtasksperchild": 50000}
    # batches = chunks(o_tickers, size=250000)
    # for batch in batches:
    # log.info(f"fetching options prices batch {batch_counter}")
    op_prices = HistoricalOptionsPrices(month_hist=month_hist)
    await api_pool_downloader(paginator=op_prices, pool_kwargs=pool_kwargs, args_data=o_tickers)
    # log.info(f"finished downloading options prices for batch {batch_counter}")
    # batch_counter += 1


def chunks(data: list, size=250000):
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def download_all_tickers(args: Namespace):
    log.info("fetching all stock ticker metadata")
    # ticker_lookup = await import_all_ticker_metadata()
    # args_list = [
    #     (
    #         list(x.keys())[0],  # "ticker":
    #         list(x.values())[0],  # "ticker_id":
    #         args.startdate,  # "start_date":
    #         args.enddate,  # "end_date":
    #         args.monthhist,  # "months_hist":
    #     )
    #     for x in ticker_lookup
    # ]

    log.info("importing all stock prices and options contract metadata")
    pass

    log.info("fetching options contracts prices")
    pass


async def main():
    # ticker_lookup = await import_all_ticker_metadata()
    # ticker_lookup = {list(x.keys())[0]: list(x.values())[0] for x in ticker_lookup}
    # await fetch_options_contracts(ticker_id_lookup=ticker_lookup)
    await fetch_options_prices(["SPY"], all_=True)


if __name__ == "__main__":
    asyncio.run(main())
    # fetch_options_contracts(["SPY", "HOOD", "IVV"]))
