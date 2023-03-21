import asyncio
from datetime import datetime
from math import floor
from multiprocessing import cpu_count

from aiomultiprocess import Pool
from sentry_sdk import capture_exception

from option_bot.db_manager import (
    delete_stock_ticker,
    lookup_ticker_id,
    query_all_stock_tickers,
    query_options_tickers,
    ticker_imported,
    update_options_prices,
    update_options_tickers,
    update_stock_metadata,
    update_stock_prices,
)
from option_bot.polygon_utils import (
    HistoricalOptionsPrices,
    HistoricalStockPrices,
    OptionsContracts,
    StockMetaData,
)
from option_bot.proj_constants import log


CPUS = cpu_count()
# CPUS -= 1  # so as to not jam the computer


async def add_tickers_to_universe(kwargs_list):

    cpus_per_stock = floor(CPUS / len(kwargs_list))
    remaining_cpus = CPUS - cpus_per_stock * len(kwargs_list)
    for i, ticker_dict in enumerate(kwargs_list):
        ticker_dict["cpus"] = cpus_per_stock
        if i + 1 <= remaining_cpus:
            ticker_dict["cpus"] += 1

    async with Pool(processes=CPUS, exception_handler=capture_exception) as pool:
        async for _ in pool.starmap(ticker_import_process, kwargs_list):
            continue


async def ticker_import_process(
    ticker: str, start_date: datetime, end_date: datetime, opt_price_days: int, months_hist: int, cpus: int
):
    """Process of fetching stock, contract, and pricing data for individual stock tickers.

    Make sure it is properly asyncronous
    """
    log.info(
        f"ticker import inputs: ticker: {ticker}, st_date: {start_date}, end_date: {end_date}, \
months_hist: {months_hist}, cpus: {cpus}"
    )
    await fetch_stock_metadata(ticker)
    await fetch_stock_prices(ticker, start_date, end_date)
    await fetch_options_contracts(ticker, months_hist, cpus)
    await fetch_options_prices(ticker, cpus)
    log.info(f"ticker : {ticker} successfully imported")


async def remove_tickers_from_universe(tickers: list[str]):
    for ticker in tickers:
        log.info(f"deleting ticker {ticker}")
        await delete_stock_ticker(ticker)
        log.info(f"ticker {ticker} successfully deleted")


async def import_all_tickers(args):
    await fetch_stock_metadata(all_=True)
    ticker_results = await query_all_stock_tickers()
    ticker_lookup = [{x[1]: x[0]} for x in ticker_results]
    await fetch_options_contracts(ticker="all_", all_=True, all_ticker_id_lookup=ticker_lookup)


async def fetch_stock_metadata(ticker: str = "", all_: bool = True):
    if all_:
        log.info("pulling ticker metadata for all tickers")
    else:
        log.info(f"pulling ticker metadata for ticker: {ticker}")
    meta = StockMetaData(ticker, all_)
    await meta.fetch()
    batch_counter = 0
    for batch in meta.clean_data_generator:
        await update_stock_metadata(batch)
        log.info("ticker metadata uploaded for ticker: {}".format(ticker if not all_ else f"all_batch:{batch_counter}"))
        batch_counter += 1


async def fetch_stock_prices(
    ticker: str, start_date: str, end_date: str, all_: bool = False, ticker_id: int | None = None
):
    if not ticker_id:
        ticker_id = await lookup_ticker_id(ticker, stock=True)
    # TODO: Add exception handling here with a sleep function incase metadata has yet to populate
    prices = HistoricalStockPrices(ticker, ticker_id, start_date, end_date)
    await prices.fetch()
    for batch in prices.clean_data_generator:
        await update_stock_prices(batch)
    await ticker_imported(ticker_id)


async def test_query():
    results = await query_all_stock_tickers()  # 9912 is SPY id
    print(results)


async def fetch_options_contracts(
    ticker: str,
    months_hist: int = 24,
    cpu_count: int = 1,
    all_=False,
    ticker_id: int | None = None,
    all_ticker_id_lookup: dict | None = None,
):
    # NOTE: if refreshing, just pull the current month, months_hist = 1
    if not ticker_id:
        ticker_id = await lookup_ticker_id(ticker, stock=True)
    options = OptionsContracts(ticker, ticker_id, months_hist, cpu_count, all_, all_ticker_id_lookup)
    await options.fetch()
    batch_counter = 0
    for batch in options.clean_data_generator:
        await update_options_tickers(batch)
        log.info("options-tickers uploaded for ticker: {}".format(ticker if not all_ else f"all_batch:{batch_counter}"))
        batch_counter += 1


async def fetch_options_prices(ticker: str, cpu_count: int = 1):
    o_tickers = await query_options_tickers(ticker)  # NOTE: may need to adjust to not pull all columns from table
    # TODO: Add exception handling here with a sleep function incase metadata has yet to populate
    o_prices = HistoricalOptionsPrices(o_tickers, cpu_count)
    await o_prices.fetch()
    for batch in o_prices.clean_data_generator:
        await update_options_prices(batch)


if __name__ == "__main__":
    asyncio.run(test_query())
