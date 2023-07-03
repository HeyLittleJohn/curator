import asyncio
from datetime import datetime

from aiomultiprocess import Pool
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
    ticker_imported,
    update_stock_prices,
)

from option_bot.proj_constants import log, POLYGON_BASE_URL
from option_bot.utils import pool_kwarg_config


planned_exceptions = (
    InvalidArgs,
    ProjClientConnectionError,
    ProjBaseException,
    ProjClientResponseError,
    ProjIndexError,
    ProjTimeoutError,
)


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
    pool_kwargs = dict(**pool_kwargs, **{"init_client_session": True, "session_base_url": POLYGON_BASE_URL})
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


# TODO: update this
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


async def download_options_contracts(
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


async def download_options_prices(o_tickers: list[tuple[str, int, datetime, str]], month_hist: int = 24):
    """This function downloads options prices from polygon and stores it as local json.

    Args:
        o_tickers: list of OptionTicker tuples
        month_hist: number of months of history to pull
    """
    pool_kwargs = {"childconcurrency": 300, "maxtasksperchild": 50000}
    op_prices = HistoricalOptionsPrices(month_hist=month_hist)
    await api_pool_downloader(paginator=op_prices, pool_kwargs=pool_kwargs, args_data=o_tickers)


async def main():
    # ticker_lookup = await import_all_ticker_metadata()
    # ticker_lookup = {list(x.keys())[0]: list(x.values())[0] for x in ticker_lookup}
    # await fetch_options_contracts(ticker_id_lookup=ticker_lookup)
    await download_options_prices(["SPY"], all_=True)


if __name__ == "__main__":
    asyncio.run(main())
    # fetch_options_contracts(["SPY", "HOOD", "IVV"]))
